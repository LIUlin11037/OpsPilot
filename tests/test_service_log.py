import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.connectors.jsonl_service_log import JsonlServiceLogConnector


def _cli(directory: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory,
        capture_output=True, encoding="utf-8", timeout=20,
    )


def _write_log(path: Path, entries: list[tuple[datetime, str]]) -> None:
    path.write_text("\n".join(
        json.dumps({"timestamp": timestamp.isoformat(), "level": level, "message": "raw-log-marker"})
        for timestamp, level in entries
    ), encoding="utf-8")


def test_service_log_counts_only_error_levels_within_the_inclusive_window() -> None:
    end = datetime(2026, 11, 21, 12, tzinfo=timezone.utc)
    start = end - timedelta(minutes=5)
    with TemporaryDirectory(prefix="opspilot-log-window-") as directory:
        path = Path(directory) / "service.jsonl"
        _write_log(path, [
            (start - timedelta(seconds=1), "ERROR"),
            (start.astimezone(timezone(timedelta(hours=8))), "error"),
            (end - timedelta(minutes=1), "WARN"),
            (end, "CRITICAL"),
            (end + timedelta(seconds=1), "FATAL"),
        ])
        evidence = JsonlServiceLogConnector(path).inspect_recent_errors(end.isoformat())

    assert evidence["source"] == path.as_uri()
    assert evidence["outcome"] == "observed"
    assert evidence["error_count"] == 2
    assert evidence["window_start"] == start.isoformat()
    assert evidence["window_end"] == end.isoformat()
    assert "raw-log-marker" not in json.dumps(evidence)


def test_service_log_preserves_an_unavailable_result_for_an_oversized_export() -> None:
    with TemporaryDirectory(prefix="opspilot-log-size-") as directory:
        path = Path(directory) / "service.jsonl"
        path.write_bytes(b" " * (1024 * 1024 + 1))
        evidence = JsonlServiceLogConnector(path).inspect_recent_errors("2026-11-21T12:00:00Z")

    assert evidence["outcome"] == "missing"
    assert evidence["reason"] == "log_too_large"
    assert "error_count" not in evidence


@pytest.mark.parametrize(
    ("service_status", "log_contents", "count", "reason", "automatic"),
    [
        (503, "entry", 1, None, True),
        (503, "", 0, None, False),
        (503, "{invalid", None, "invalid_log_record", False),
        (503, None, None, "log_not_found", False),
        (204, "{invalid", None, None, True),
        (403, "{invalid", None, None, False),
    ],
)
def test_http_cli_uses_the_configured_log_after_an_observed_service_failure(
    processing_service, service_status, log_contents, count, reason, automatic,
) -> None:
    with TemporaryDirectory(prefix="opspilot-health-logs-") as directory:
        config_dir = Path(directory) / "config"
        config_dir.mkdir()
        config = config_dir / "target.json"
        log = config_dir / "service export.jsonl"
        if log_contents == "entry":
            _write_log(log, [(datetime.now(timezone.utc) - timedelta(minutes=1), "ERROR")])
        elif log_contents is not None:
            log.write_text(log_contents, encoding="utf-8")

        def respond(path):
            return (service_status if path == "/health" else 204), b""

        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            settings = {
                "service_url": f"{base}/health", "dependency_urls": [f"{base}/dependency"],
                "service_log_path": log.name,
            }
            config.write_text(json.dumps({"health": settings} if automatic else settings), encoding="utf-8")
            command = "diagnose-auto" if automatic else "diagnose-http"
            result = _cli(directory, command, "--config", str(config), "Check HTTP health")

        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        tools = ["inspect_service_health"]
        if service_status == 503:
            tools.append("inspect_service_log")
            service, service_log, dependency = report["evidence"]
            assert service_log["source"] == log.as_uri()
            assert service_log["window_end"] == service["observed_at"]
            assert service_log.get("error_count") == count
            assert service_log.get("reason") == reason
            assert service_log["outcome"] == ("observed" if reason is None else "missing")
            assert dependency["http_status"] == 204
            assert report["status"] == "undetermined"
            assert "raw-log-marker" not in result.stdout
        if service_status != 204:
            tools.append("inspect_dependency_health")
        assert requests == (["/health"] if service_status == 204 else ["/health", "/dependency"])
        assert [call["tool"] for call in report["tool_calls"]] == tools
        if automatic:
            assert report["planned_checks"][1] == {
                "tool": "inspect_service_log", "when": "service_unhealthy", "count": 1,
            }
        if count == 1:
            rendered = _cli(directory, "report", report["task_id"], "--format", "markdown")
            assert rendered.returncode == 0, rendered.stderr
            assert "Error-level entries 1" in rendered.stdout
            assert "raw-log-marker" not in rendered.stdout
