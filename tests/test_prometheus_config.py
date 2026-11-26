import json
import subprocess
import sys

import pytest

from opspilot.prometheus_config import parse_prometheus_config


def _config():
    return {
        "base_url": "http://127.0.0.1:1/prometheus",
        "service": "orders",
        "queries": {
            "error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.05},
            "latency_p95": {"metric": "http_request_duration_seconds_bucket", "unit": "seconds", "threshold": 0.4},
        },
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("window_seconds", True), ("window_seconds", 901), ("step_seconds", 301),
        ("timeout_seconds", float("inf")), ("service", 'orders",job="other'),
        ("base_url", "https://token@example.test"), ("base_url", "http://example.test:0"),
        ("queries", {}), ("queries", {"arbitrary": {}}),
    ],
)
def test_prometheus_configuration_rejects_invalid_fields(field, value):
    with pytest.raises(ValueError, match=rf"prometheus\.{field}"):
        parse_prometheus_config({**_config(), field: value})


@pytest.mark.parametrize("force", [False, True])
def test_metrics_plan_is_offline_and_previews_service_queries_and_conditions(tmp_path, force):
    target = tmp_path / "target.json"
    metrics = _config()
    target.write_text(json.dumps({
        "health": {"service_url": "http://127.0.0.1:1/ready"}, "prometheus": metrics,
    }), encoding="utf-8")
    options = ["--check-metrics"] if force else []
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "diagnose-auto", "--config", str(target), "--plan", *options, "HTTP health"],
        cwd=tmp_path, capture_output=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert plan["checks"][1] == {
        "tool": "inspect_service_metrics", "when": "always" if force else "service_unhealthy_or_unknown",
        "count": 2, "sources": ["http://127.0.0.1:1/prometheus/api/v1/query_range"], "service": "orders",
        "queries": [{"name": name, **query} for name, query in metrics["queries"].items()],
        "window_seconds": 300, "step_seconds": 15, "timeout_seconds": 5,
    }
    assert not (tmp_path / ".opspilot").exists()


def test_offline_plan_reports_invalid_metrics_window(tmp_path):
    target = tmp_path / "target.json"
    target.write_text(json.dumps({
        "health": {"service_url": "http://127.0.0.1:1/ready"},
        "prometheus": {**_config(), "window_seconds": 1000},
    }), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "diagnose-auto", "--config", str(target), "--plan", "HTTP health"],
        cwd=tmp_path, capture_output=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == 2
    assert "prometheus.window_seconds" in result.stderr
