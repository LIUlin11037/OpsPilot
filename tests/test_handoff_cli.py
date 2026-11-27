import json
import subprocess
import sys

from opspilot.task_store import save_report


def test_handoff_cli_reuses_report_listing_and_comparison_on_receiver(tmp_path):
    source = tmp_path / "source.sqlite3"
    receiver = tmp_path / "receiver.sqlite3"
    package = tmp_path / "incident.zip"
    reports = [
        save_report(source, {
            "question": question,
            "status": status,
            "conclusion": "Recorded evidence.",
            "processing_correlation": None,
            "evidence": [{
                "tool": "inspect_service_health",
                "outcome": "observed",
                "source": "http://127.0.0.1:18771/orders/",
                "observed_at": "2026-11-26T11:23:54+08:00",
                "health": health,
                "http_status": code,
            }],
        })
        for question, status, health, code in (
            ("Fault", "undetermined", "unhealthy", 504),
            ("Recovery", "no_failure_observed", "healthy", 200),
        )
    ]
    ids = [report["task_id"] for report in reports]

    def cli(db, *args):
        result = subprocess.run(
            [sys.executable, "-m", "opspilot", "--db", str(db), *args],
            cwd=tmp_path, capture_output=True, encoding="utf-8", timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    initial = source.read_bytes()
    assert json.loads(cli(source, "export-reports", "--output", str(package), *ids)) == {
        "count": 2, "task_ids": ids,
    }
    assert json.loads(cli(receiver, "import-reports", str(package))) == {"count": 2, "task_ids": ids}
    assert [json.loads(cli(receiver, "report", task_id)) for task_id in ids] == reports
    assert [row["task_id"] for row in json.loads(cli(receiver, "reports"))] == ids[::-1]
    comparison = cli(receiver, "compare-http", *ids)
    assert comparison == cli(source, "compare-http", *ids)
    change, = json.loads(comparison)["changes"]
    assert (change["before"]["http_status"], change["after"]["http_status"]) == (504, 200)
    assert source.read_bytes() == initial
