"""Local SQLite storage for completed diagnosis reports."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from opspilot.core.diagnosis import DiagnosisReport, DiagnosisStatus, TargetTraceSummary


class StoredReport(DiagnosisReport):
    task_id: str


class ReportSummaryFields(TypedDict):
    task_id: str
    question: str
    status: DiagnosisStatus


class ReportSummary(ReportSummaryFields, total=False):
    target_trace: TargetTraceSummary


def _open_read_only(database: Path) -> sqlite3.Connection:
    database_uri = f"{database.resolve().as_uri()}?mode=ro"
    return sqlite3.connect(database_uri, uri=True)


def save_report(database: Path, report: DiagnosisReport) -> StoredReport:
    task_id = str(uuid4())
    payload = json.dumps(report)
    database.parent.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS reports ("
            "task_id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO reports (task_id, payload) VALUES (?, ?)", (task_id, payload)
        )

    return {**report, "task_id": task_id}


def load_report(database: Path, task_id: str) -> StoredReport | None:
    if not database.exists():
        return None

    with closing(_open_read_only(database)) as connection:
        row = connection.execute(
            "SELECT payload FROM reports WHERE task_id = ?", (task_id,)
        ).fetchone()

    if row is None:
        return None
    report: DiagnosisReport = json.loads(row[0])
    return {**report, "task_id": task_id}


def list_reports(
    database: Path, *, limit: int, delivery_id: int | None = None, environment: str | None = None
) -> list[ReportSummary]:
    if not database.exists():
        return []

    filter_trace = delivery_id is not None or environment is not None
    summaries: list[ReportSummary] = []
    with closing(_open_read_only(database)) as connection:
        # The original report table has a rowid and no save timestamp.
        # Trace filters apply to stored payloads, so the limit counts matching reports.
        rows = connection.execute(
            "SELECT task_id, payload FROM reports ORDER BY rowid DESC LIMIT ?",
            (-1 if filter_trace else limit,),
        )
        for task_id, payload in rows:
            report: DiagnosisReport = json.loads(payload)
            summary: ReportSummary = {
                "task_id": task_id, "question": report["question"], "status": report["status"],
            }
            if filter_trace:
                trace = report.get("target_trace")
                if (
                    trace is None
                    or (delivery_id is not None and trace["entity_id"] != delivery_id)
                    or (environment is not None and trace["environment"] != environment)
                ):
                    continue
                summary["target_trace"] = trace
            summaries.append(summary)
            if len(summaries) == limit:
                break
    return summaries
