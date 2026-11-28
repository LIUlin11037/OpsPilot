import json
import subprocess
import sys

from opspilot.task_store import save_report
from test_incident_comparison import incident_report


def run(database, command, *args):
    return subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(database), command, *args],
        cwd=database.parent, capture_output=True, encoding="utf-8", timeout=25,
    )


def test_saved_comparison_keeps_http_separate_from_history_and_database_unchanged(tmp_path):
    database = tmp_path / "reports.sqlite3"
    baseline = save_report(database, incident_report("before"))
    recovered = incident_report("after", healthy=True)
    recovered["evidence"][2].update(threshold=0.2, outcome="missing", reason="metric_missing")
    after = save_report(database, recovered)
    disk = database.read_bytes()
    ids = baseline["task_id"], after["task_id"]

    result = run(database, "compare-incident", *ids)
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["window_relation"] == "same_window"
    assert (data["before"]["task_id"], data["after"]["task_id"]) == ids
    assert data["before"]["current_http"]["evidence"]["http_status"] == 503
    assert data["after"]["current_http"]["evidence"]["http_status"] == 200
    assert data["after"]["window"]["window_status"] == "anomaly_observed"
    assert data["observations"][1]["limitations"] == ["after_unavailable", "different_configuration"]

    markdown = run(database, "compare-incident", *ids, "--format", "markdown")
    assert markdown.returncode == 0, markdown.stderr
    assert all(value in markdown.stdout for value in ids)
    assert "Same historical window" in markdown.stdout
    assert "503" in markdown.stdout and "200" in markdown.stdout
    assert "Historical status" in markdown.stdout and "anomaly\\_observed" in markdown.stdout
    assert "metric\\_missing" in markdown.stdout and "Different configurations" in markdown.stdout
    assert "Evidence 2" in markdown.stdout and "No trace follow-ups recorded" in markdown.stdout

    legacy = run(database, "compare-http", *ids)
    assert legacy.returncode == 0, legacy.stderr
    assert "metric_comparisons" in json.loads(legacy.stdout)
    assert database.read_bytes() == disk


def test_command_identifies_missing_and_non_historical_inputs(tmp_path):
    database = tmp_path / "reports.sqlite3"
    report = incident_report("regular-http")
    del report["incident"]
    regular = save_report(database, report)["task_id"]
    historical = save_report(database, incident_report("history"))["task_id"]

    missing = run(database, "compare-incident", historical, "missing-id")
    assert missing.returncode == 1 and "missing-id" in missing.stderr
    invalid = run(database, "compare-incident", historical, regular)
    assert invalid.returncode == 2 and regular in invalid.stderr and "historical" in invalid.stderr
