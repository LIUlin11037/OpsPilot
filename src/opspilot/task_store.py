"""Local SQLite storage for completed diagnosis reports."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from opspilot.core.diagnosis import DiagnosisReport


class StoredReport(DiagnosisReport):
    task_id: str


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

    database_uri = f"{database.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(database_uri, uri=True)) as connection:
        row = connection.execute(
            "SELECT payload FROM reports WHERE task_id = ?", (task_id,)
        ).fetchone()

    if row is None:
        return None
    report: DiagnosisReport = json.loads(row[0])
    return {**report, "task_id": task_id}
