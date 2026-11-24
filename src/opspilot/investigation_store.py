"""Persist investigation checkpoints and serialize runners for each task."""

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, TypedDict
from uuid import UUID, uuid4

from opspilot.core.diagnosis import DiagnosisPlan, DiagnosisReport, InvestigationDecision, ToolName
from opspilot.core.investigation import InvestigationCheckpoint
from opspilot.task_store import StoredReport, initialize_reports, insert_report, open_read_only

_MAX_ATTEMPTS = 2


class InvestigationRequest(TypedDict):
    service_url: str
    health_timeout_seconds: float
    management_url: str
    queue: str
    vhost: str
    queue_timeout_seconds: float
    consumer_health_url: str | None
    processing_url: str | None
    processing_record: str | None
    message_id: str | None


class _AttemptFields(TypedDict):
    step_index: int
    attempt: int
    tool: ToolName
    source: str
    started_at: str
    outcome: Literal["started", "interrupted", "observed", "missing", "timeout"]


class InvestigationAttempt(_AttemptFields, total=False):
    finished_at: str
    reason: str


class InvestigationTask(TypedDict):
    task_id: str
    created_at: str
    status: Literal["unfinished", "completed"]
    previous_task_id: str | None
    request: InvestigationRequest
    plan: DiagnosisPlan
    checkpoint: InvestigationCheckpoint
    attempts: list[InvestigationAttempt]


class InvestigationBusyError(RuntimeError):
    """A different runner holds the task's execution lock."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_task(
    database: Path, request: InvestigationRequest, plan: DiagnosisPlan, *, previous_task_id: str | None = None,
) -> InvestigationTask:
    task: InvestigationTask = {
        "task_id": str(uuid4()), "created_at": _utc_now(), "status": "unfinished",
        "previous_task_id": previous_task_id, "request": request, "plan": plan, "attempts": [],
        "checkpoint": {"decisions": [], "evidence": [], "tool_calls": [], "elapsed_ms": 0.0},
    }
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection, connection:
        initialize_reports(connection)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS investigation_tasks ("
            "task_id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO investigation_tasks (task_id, payload) VALUES (?, ?)",
            (task["task_id"], json.dumps(task)),
        )
    return task


def load_task(database: Path, task_id: str) -> InvestigationTask | None:
    if not database.exists():
        return None
    with closing(open_read_only(database)) as connection:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'investigation_tasks'",
        ).fetchone()
        if table is None:
            return None
        row = connection.execute(
            "SELECT payload FROM investigation_tasks WHERE task_id = ?", (task_id,),
        ).fetchone()
    return None if row is None else json.loads(row[0])


@contextmanager
def lock_task(database: Path, task_id: str) -> Iterator[None]:
    if str(UUID(task_id)) != task_id:
        raise ValueError("Invalid investigation task ID.")
    resolved = database.resolve()
    directory = resolved.with_name(f"{resolved.name}.locks")
    directory.mkdir(parents=True, exist_ok=True)
    # A separate SQLite database holds only the cross-process lock. A dead
    # runner releases it automatically while the task journal stays readable.
    with closing(sqlite3.connect(directory / f"{task_id}.sqlite3", timeout=0)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            if "locked" in str(error).lower():
                raise InvestigationBusyError(f"Investigation {task_id} already has an active runner.") from error
            raise
        yield


def _update_task(connection: sqlite3.Connection, task: InvestigationTask) -> None:
    result = connection.execute(
        "UPDATE investigation_tasks SET payload = ? WHERE task_id = ?", (json.dumps(task), task["task_id"]),
    )
    if result.rowcount != 1:
        raise RuntimeError(f"Investigation task {task['task_id']} disappeared.")


class TaskJournal:
    def __init__(self, database: Path, task: InvestigationTask) -> None:
        self.database = database
        self.task = task
        last = self._started_attempt()
        if last is not None:
            last.update(outcome="interrupted", finished_at=_utc_now())
            self._save()

    @property
    def checkpoint(self) -> InvestigationCheckpoint:
        return self.task["checkpoint"]

    def _started_attempt(self) -> InvestigationAttempt | None:
        attempts = self.task["attempts"]
        return attempts[-1] if attempts and attempts[-1]["outcome"] == "started" else None

    def _save(self) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            _update_task(connection, self.task)

    def begin_read(self, decision: InvestigationDecision, source: str, elapsed_ms: float) -> bool:
        step = len(self.checkpoint["evidence"])
        attempt = 1 + sum(item["step_index"] == step for item in self.task["attempts"])
        if attempt > _MAX_ATTEMPTS:
            return False
        self.checkpoint["elapsed_ms"] = elapsed_ms
        self.task["attempts"].append({
            "step_index": step, "attempt": attempt, "tool": decision["tool"], "source": source,
            "started_at": _utc_now(), "outcome": "started",
        })
        self._save()
        return True

    def finish_read(self, checkpoint: InvestigationCheckpoint) -> None:
        last = self._started_attempt()
        if last is not None:
            evidence = checkpoint["evidence"][-1]
            last.update(outcome=evidence["outcome"], finished_at=_utc_now())
            if "reason" in evidence:
                last["reason"] = evidence["reason"]
        self.task["checkpoint"] = checkpoint
        self._save()

    def complete(self, report: DiagnosisReport) -> StoredReport:
        self.task["status"] = "completed"
        self.checkpoint["elapsed_ms"] = report["investigation"]["elapsed_ms"]
        with closing(sqlite3.connect(self.database)) as connection, connection:
            stored = insert_report(connection, self.task["task_id"], report)
            _update_task(connection, self.task)
        return stored
