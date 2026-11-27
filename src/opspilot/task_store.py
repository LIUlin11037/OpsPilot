"""Local SQLite storage for completed diagnosis reports."""

import json
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from opspilot.adapters.voting_snapshot import is_voting_trace
from opspilot.core.diagnosis import DiagnosisReport, DiagnosisStatus, TargetTraceSummary


class StoredReport(DiagnosisReport):
    task_id: str


class ReportSummaryFields(TypedDict):
    task_id: str
    question: str
    status: DiagnosisStatus


class ReportSummary(ReportSummaryFields, total=False):
    target_trace: TargetTraceSummary
    service_url: str
    queue_source: str
    requested_message_id: str


def open_read_only(database: Path) -> sqlite3.Connection:
    database_uri = f"{database.resolve().as_uri()}?mode=ro"
    return sqlite3.connect(database_uri, uri=True)


def initialize_reports(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS reports ("
        "task_id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL)"
    )


def insert_report(connection: sqlite3.Connection, task_id: str, report: DiagnosisReport) -> StoredReport:
    initialize_reports(connection)
    connection.execute(
        "INSERT INTO reports (task_id, payload) VALUES (?, ?)", (task_id, json.dumps(report)),
    )
    return {**report, "task_id": task_id}


def save_report(database: Path, report: DiagnosisReport) -> StoredReport:
    task_id = str(uuid4())
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection, connection:
        return insert_report(connection, task_id, report)


def load_report(database: Path, task_id: str) -> StoredReport | None:
    if not database.exists():
        return None

    with closing(open_read_only(database)) as connection:
        row = connection.execute(
            "SELECT payload FROM reports WHERE task_id = ?", (task_id,)
        ).fetchone()

    if row is None:
        return None
    report: DiagnosisReport = json.loads(row[0])
    return {**report, "task_id": task_id}


def list_reports(
    database: Path, *, limit: int, delivery_id: int | None = None,
    environment: str | None = None, service_url: str | None = None, queue_source: str | None = None,
    voter_id: str | None = None, message_id: str | None = None,
) -> list[ReportSummary]:
    if not database.exists():
        return []

    filter_trace = delivery_id is not None or voter_id is not None or environment is not None
    has_filters = filter_trace or service_url is not None or queue_source is not None or message_id is not None
    summaries: list[ReportSummary] = []
    with closing(open_read_only(database)) as connection:
        # The original report table has a rowid and no save timestamp.
        # Filters apply to stored payloads, so the limit counts matching reports.
        rows = connection.execute(
            "SELECT task_id, payload FROM reports ORDER BY rowid DESC LIMIT ?",
            (-1 if has_filters else limit,),
        )
        for task_id, payload in rows:
            report: DiagnosisReport = json.loads(payload)
            if message_id is not None and report.get("requested_message_id") != message_id:
                continue
            summary: ReportSummary = {
                "task_id": task_id, "question": report["question"], "status": report["status"],
            }
            if filter_trace:
                trace = report.get("target_trace")
                if trace is None or (environment is not None and trace["environment"] != environment):
                    continue
                if delivery_id is not None and (trace["entity_id"] != delivery_id or is_voting_trace(trace)):
                    continue
                if voter_id is not None and (str(trace["entity_id"]) != voter_id or not is_voting_trace(trace)):
                    continue
                summary["target_trace"] = trace
            if service_url is not None:
                if not any(
                    check["tool"] == "inspect_service_health" and check["source"] == service_url
                    for check in report["evidence"]
                ):
                    continue
                summary["service_url"] = service_url
            if queue_source is not None:
                if not any(
                    check["tool"] == "inspect_queue" and check["source"] == queue_source
                    for check in report["evidence"]
                ):
                    continue
                summary["queue_source"] = queue_source
            if message_id is not None:
                summary["requested_message_id"] = message_id
            summaries.append(summary)
            if len(summaries) == limit:
                break
    return summaries

def load_reports_for_export(database: Path, task_ids: Sequence[str]) -> list[StoredReport]:
    if not database.is_file():
        raise ValueError(f"Source report database does not exist: {database}")
    placeholders = ", ".join("?" for _ in task_ids)
    with closing(open_read_only(database)) as connection:
        rows = connection.execute(
            f"SELECT task_id, payload FROM reports WHERE task_id IN ({placeholders}) ORDER BY rowid",
            tuple(task_ids),
        ).fetchall()
    found = {task_id for task_id, _ in rows}
    missing = [task_id for task_id in task_ids if task_id not in found]
    if missing:
        raise ValueError(f"No reports found for task IDs: {', '.join(missing)}")
    return [{**json.loads(payload), "task_id": task_id} for task_id, payload in rows]
