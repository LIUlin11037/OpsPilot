import json
import subprocess
import sys

import pytest

from opspilot.core.incident_window import parse_incident_window


@pytest.mark.parametrize(("start", "end", "reason"), [
    (None, "2024-01-01T00:02:00Z", "supplied together"),
    ("2024-01-01T00:00:00", "2024-01-01T00:02:00Z", "--incident-start"),
    ("2024-01-01T00:00:00Z", "2024-01-01T00:02:00.5Z", "--incident-end"),
    ("2024-01-01T00:00:00Z", "2024-01-01T00:01:04Z", "65 and 900"),
    ("2024-01-01T00:00:00Z", "2023-12-31T00:00:00Z", "65 and 900"),
    ("2024-01-01T00:00:00Z", "2024-01-01T00:15:01Z", "65 and 900"),
    ("2099-01-01T00:00:00Z", "2099-01-01T00:02:00Z", "current time"),
])
def test_incident_window_rejects_ambiguous_or_out_of_bounds_time(start, end, reason):
    with pytest.raises(ValueError, match=reason):
        parse_incident_window(start, end)


def test_historical_preview_scopes_both_sources_without_reading_them(tmp_path):
    target = tmp_path / "target.json"
    target.write_text(json.dumps({
        "health": {"service_url": "http://127.0.0.1:1/ready", "service_log_path": "not-present.jsonl"},
        "prometheus": {
            "base_url": "http://127.0.0.1:1",
            "service": "orders",
            "step_seconds": 120,
            "queries": {"error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.1}},
        },
    }), encoding="utf-8")
    result = subprocess.run([
        sys.executable, "-m", "opspilot", "diagnose-auto", "--config", str(target), "--plan",
        "--incident-start", "2024-01-01T08:00:00+08:00",
        "--incident-end", "2024-01-01T00:02:00Z", "Review the incident",
    ], cwd=tmp_path, capture_output=True, encoding="utf-8", timeout=20)
    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert plan["routing"]["selected_by"] == "incident_window"
    assert plan["scope"] == {
        "service_url": "http://127.0.0.1:1/ready", "service": "orders",
        "incident_start": "2024-01-01T00:00:00+00:00", "incident_end": "2024-01-01T00:02:00+00:00",
    }
    probe, log, metrics = plan["checks"]
    assert probe["tool"] == "inspect_service_health"
    assert log["service"] == metrics["service"] == "orders"
    assert log["when"] == metrics["when"] == "always"
    assert log["sources"] == [(tmp_path / "not-present.jsonl").as_uri()]
    assert metrics["evaluation_start"] == "2024-01-01T00:01:00+00:00"
    assert metrics["step_seconds"] == 60
    assert not (tmp_path / ".opspilot").exists()
