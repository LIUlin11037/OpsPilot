import json
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit


def test_cli_keeps_historical_evidence_when_current_health_and_sources_change(
    processing_service, tmp_path, monkeypatch,
):
    monkeypatch.delenv("OPSPILOT_PROMETHEUS_TOKEN", raising=False)
    health_status, peak = 503, "0.6"

    def respond(path):
        if path == "/ready":
            return health_status, b""
        end = int(parse_qs(urlsplit(path).query)["end"][0])
        return 200, json.dumps({
            "status": "success", "data": {"resultType": "matrix", "result": [{"values": [[end, peak]]}]},
        }).encode()

    def cli(*args):
        result = subprocess.run(
            [sys.executable, "-m", "opspilot", *args], cwd=tmp_path,
            capture_output=True, encoding="utf-8", timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    log = tmp_path / "shared.jsonl"
    target = tmp_path / "target.json"
    args = (
        "diagnose-auto", "--config", str(target),
        "--incident-start", "2024-01-01T00:00:00Z", "--incident-end", "2024-01-01T00:02:00Z", "Review the incident",
    )
    with processing_service(respond) as (endpoint, requests):
        base = endpoint.removesuffix("/processing")
        target.write_text(json.dumps({
            "health": {"service_url": f"{base}/ready", "service_log_path": "shared.jsonl"},
            "prometheus": {
                "base_url": base, "service": "orders", "step_seconds": 60,
                "queries": {"error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.1}},
            },
        }), encoding="utf-8")
        log.write_text('\n'.join(json.dumps(row) for row in (
            {"service": "orders", "timestamp": "2024-01-01T00:01:00Z", "level": "ERROR"},
            {"service": "orders-neighbor", "timestamp": "2024-01-01T00:01:00Z", "level": "ERROR"},
        )), encoding="utf-8")
        ongoing = json.loads(cli(*args))
        assert ongoing["incident"]["current_health"] == "unhealthy"
        assert ongoing["incident"]["current_evidence_index"] == 0
        assert ongoing["incident"]["anomaly_evidence_indexes"] == [1, 2]
        assert ongoing["incident"]["stop_reason"] == "checks_complete"

        health_status = 200
        recovered = json.loads(cli(*args))
        assert recovered["incident"]["current_health"] == "healthy"
        assert recovered["incident"]["window_status"] == "anomaly_observed"
        assert recovered["evidence"][1]["error_count"] == 1
        assert recovered["evidence"][2]["trigger"] == "incident_window"
        before = parse_qs(urlsplit(requests[1]).query)
        after = parse_qs(urlsplit(requests[3]).query)
        assert all(before[name] == after[name] for name in ("start", "end", "step", "query"))

        log.rename(tmp_path / "archived.jsonl")
        incomplete = json.loads(cli(*args))
        assert incomplete["incident"]["stop_reason"] == "sources_incomplete"
        assert incomplete["incident"]["anomaly_evidence_indexes"] == [2]
        assert incomplete["incident"]["unavailable_evidence_indexes"] == [1]
        assert incomplete["evidence"][1]["reason"] == "log_not_found"
        assert json.loads(cli("report", incomplete["task_id"])) == incomplete
        markdown = cli("report", incomplete["task_id"], "--format", "markdown")
        assert "## Incident window" in markdown and "| Index | Check |" in markdown

        peak = "0"
        unknown = json.loads(cli(*args))
        assert unknown["incident"]["window_status"] == "undetermined"
        assert unknown["incident"]["anomaly_evidence_indexes"] == []

        log.write_text('{"service":"orders","timestamp":"2024-01-01T00:01:00Z","level":"INFO"}', encoding="utf-8")
        cleared = json.loads(cli(*args))
        assert cleared["incident"]["window_status"] == "no_anomaly_observed"
        assert cleared["incident"]["unavailable_evidence_indexes"] == []
