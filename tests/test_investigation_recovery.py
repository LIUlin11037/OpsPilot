import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.core.investigation import InvestigationTool, run_investigation
from opspilot.investigation_store import TaskJournal, create_task, load_task, lock_task
from opspilot.task_store import load_report


@pytest.fixture
def directory():
    with TemporaryDirectory(prefix="opspilot-recovery-") as name:
        yield Path(name)


def _task(database):
    return create_task(
        database,
        {
            "service_url": "http://localhost/health", "health_timeout_seconds": 5,
            "management_url": "http://localhost", "queue": "orders", "vhost": "/", "queue_timeout_seconds": 5,
            "consumer_health_url": None, "processing_url": None, "processing_record": None, "message_id": None,
        },
        {
            "question": "Why is the order delayed?",
            "routing": {"workflow": "investigation", "matched_terms": []},
            "scope": {}, "checks": [], "budget": {"max_steps": 1, "max_seconds": 15},
        },
    )


def test_abandoned_read_has_at_most_one_retry(directory):
    database = directory / "reports.sqlite3"
    task = _task(database)
    task_id = task["task_id"]
    decision = {"tool": "inspect_service_health", "reason": "configured_service", "evidence_indexes": []}
    source = task["request"]["service_url"]
    for _ in range(2):
        with lock_task(database, task_id):
            journal = TaskJournal(database, load_task(database, task_id))
            assert journal.begin_read(decision, source, 0.5)
            # Drop the runner without saving the read result.

    def unexpected_read(_):
        pytest.fail("A read was started after exhausting both attempts")

    with lock_task(database, task_id):
        journal = TaskJournal(database, load_task(database, task_id))
        report = run_investigation(
            task["plan"]["question"], InvestigationTool(source, unexpected_read),
            InvestigationTool("http://localhost/queue", unexpected_read), task["plan"]["budget"], journal=journal,
        )
        stored = journal.complete(report)
    assert stored["task_id"] == task_id
    assert stored["evidence"][0]["reason"] == "interruption_retry_exhausted"
    assert stored["investigation"]["elapsed_ms"] >= 0.5
    final = load_task(database, task_id)
    assert final["status"] == "completed"
    assert [attempt["outcome"] for attempt in final["attempts"]] == ["interrupted", "interrupted"]
    assert len(final["checkpoint"]["evidence"]) == 1


def test_report_and_completion_are_committed_together(directory, monkeypatch):
    import opspilot.investigation_store as store

    database = directory / "reports.sqlite3"
    task = _task(database)
    task_id = task["task_id"]
    report = {
        "question": task["plan"]["question"], "status": "undetermined", "conclusion": "",
        "processing_correlation": None, "evidence": [],
        "investigation": {"elapsed_ms": 0},
    }

    def fail_update(*args):
        raise sqlite3.OperationalError("simulated persistence failure")

    with lock_task(database, task_id):
        journal = TaskJournal(database, task)
        monkeypatch.setattr(store, "_update_task", fail_update)
        with pytest.raises(sqlite3.OperationalError):
            journal.complete(report)
    assert load_report(database, task_id) is None
    assert load_task(database, task_id)["status"] == "unfinished"


def test_old_report_can_be_read_without_task_table(directory):
    database = directory / "legacy.sqlite3"
    legacy = {
        "question": "Is the queue healthy?", "status": "no_backlog_observed",
        "conclusion": "No pending messages observed.", "processing_correlation": None, "evidence": [],
    }
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE reports (task_id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL)")
        connection.execute("INSERT INTO reports VALUES (?, ?)", ("legacy-id", json.dumps(legacy)))
    original = database.read_bytes()
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(database), "report", "legacy-id"],
        capture_output=True, encoding="utf-8", timeout=25,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {**legacy, "task_id": "legacy-id"}
    assert load_task(database, "legacy-id") is None
    assert database.read_bytes() == original


def test_recovery_demo_survives_real_process_interruption(directory):
    example = Path(__file__).resolve().parents[1] / "examples" / "investigation_recovery_demo.py"
    result = subprocess.run(
        [sys.executable, str(example), "--output-dir", str(directory)],
        capture_output=True, encoding="utf-8", timeout=120,
    )
    assert result.returncode == 0, result.stderr
    evidence_path, = directory.glob("run-*/acceptance.json")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["before_interruption"]["status"] == "unfinished"
    assert evidence["resumed_report"]["task_id"] == evidence["task_id"]
    assert evidence["recheck_report"]["previous_task_id"] == evidence["task_id"]
