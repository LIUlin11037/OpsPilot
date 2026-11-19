import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.hirehub_snapshot import HireHubSnapshotAdapter
from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import DiagnosisReport, run_diagnosis
from opspilot.core.target_trace import run_target_trace
from opspilot.task_store import save_report

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/hirehub-notification-snapshot.json"


@pytest.fixture
def report_db() -> Iterator[Path]:
    with TemporaryDirectory(prefix="opspilot-markdown-test-") as directory:
        yield Path(directory) / "reports.sqlite3"


def _read_report(database: Path, task_id: str, *arguments: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(database), "report", task_id, *arguments],
        cwd=database.parent, capture_output=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_report_markdown_keeps_queue_details_and_default_json(
    report_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = "消息已入队 📨 | **检查**\n# 额外内容 <img>"
    report = run_diagnosis(
        question, SimulatedQueueConnector(0, True), SimulatedProcessingAdapter()
    )
    stored = save_report(report_db, report)
    task_id = stored["task_id"]
    before = report_db.read_bytes()
    monkeypatch.setenv("PYTHONIOENCODING", "ascii")

    output = _read_report(report_db, task_id, "--format", "markdown")

    assert output.startswith("# OpsPilot diagnosis report\n")
    assert f"- **Task ID** {task_id}" in output
    assert r"消息已入队 📨 \| \*\*检查\*\* # 额外内容 &lt;img&gt;" in output
    assert "\n# 额外内容" not in output
    assert "Pending messages 0" in output
    assert "simulated:queue" in output
    assert report["evidence"][0]["observed_at"] in output
    assert "## Tool calls" in output
    assert json.loads(_read_report(report_db, task_id)) == stored
    assert report_db.read_bytes() == before


@pytest.mark.parametrize("notification_outcome", ["observed", "missing"])
def test_trace_markdown_preserves_zero_rows_and_unavailable_checks(
    report_db: Path, notification_outcome: str
) -> None:
    snapshot = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    snapshot["notification"]["outcome"] = notification_outcome
    path = report_db.parent / "snapshot.json"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    report = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(path))
    stored = save_report(report_db, report)

    output = _read_report(report_db, stored["task_id"], "--format", "markdown")

    assert "## Target trace" in output
    assert "- **Message ID** sample-delivery-event-1001" in output
    assert "- **job branch** observed" in output
    assert "Registered consumers 0" in output
    notification = next(line for line in output.splitlines() if line.startswith("| notification |"))
    assert r"hirehub\_notification.mq\_consume\_log" in notification
    assert "snapshot.json#notification" in notification
    if notification_outcome == "observed":
        assert r"- **notification branch** not\_observed" in output
        assert "| observed |" in notification
        assert "Matching rows 0" in notification
    else:
        assert "- **notification branch** unknown" in output
        assert "| missing |" in notification
        assert r"Reason source\_unavailable" in notification
        assert "Matching rows" not in notification


def test_markdown_supports_reports_saved_before_tool_call_data(report_db: Path) -> None:
    early_report: DiagnosisReport = {
        "question": "Check queue",
        "status": "undetermined",
        "conclusion": "Queue inspection timed out. Queue depth is unknown.",
        "processing_correlation": None,
        "evidence": [{
            "tool": "inspect_queue", "outcome": "timeout", "source": "legacy|source",
            "observed_at": "2026-11-19T06:00:00+00:00",
        }],
    }
    stored = save_report(report_db, early_report)

    output = _read_report(report_db, stored["task_id"], "--format", "markdown")

    assert early_report["conclusion"] in output
    assert r"| inspect\_queue | timeout |  |  | legacy\|source | 2026-11-19T06:00:00+00:00 |" in output
    assert "## Tool calls" not in output
