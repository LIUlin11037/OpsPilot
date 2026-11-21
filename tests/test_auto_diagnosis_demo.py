import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from opspilot.task_store import list_reports


def test_demo_compares_http_health_service_logs_and_queue_backlog() -> None:
    script = Path(__file__).resolve().parents[1] / "examples" / "auto_diagnosis_demo.py"
    with TemporaryDirectory(prefix="opspilot-demo-test-") as output:
        result = subprocess.run(
            [sys.executable, str(script), "--output-dir", output],
            cwd=output, capture_output=True, encoding="utf-8", timeout=120,
        )
        assert result.returncode == 0, result.stderr
        run_directory, = Path(output).iterdir()
        health = (run_directory / "health-comparison.md").read_text(encoding="utf-8")
        queue = (run_directory / "queue-comparison.md").read_text(encoding="utf-8")
        reports = list_reports(run_directory / "reports.sqlite3", limit=5)

    assert len(reports) == 4
    assert "HTTP status 503 | Outcome observed, Health healthy, HTTP status 204 |" in health
    assert "## Service log changes" in health
    log_change, = (line for line in health.splitlines() if "| Outcome observed, Error-level entries" in line)
    _, before, after = log_change.split(" | ")
    assert before.startswith("Outcome observed, Error-level entries 1,")
    assert after.startswith("Outcome observed, Error-level entries 0,")
    assert "| Pending messages | 3 | 0 |" in queue
