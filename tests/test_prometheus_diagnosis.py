import json
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

from opspilot.report_comparison import compare_health_reports


def test_diagnose_auto_preserves_http_and_compares_scoped_metrics(processing_service, tmp_path, monkeypatch):
    health_status, metrics_status, sample = 200, 200, "0"
    monkeypatch.delenv("OPSPILOT_PROMETHEUS_TOKEN", raising=False)

    def respond(path):
        if path == "/ready":
            return health_status, b""
        end = int(parse_qs(urlsplit(path).query)["end"][0])
        return metrics_status, json.dumps({
            "status": "success", "data": {"resultType": "matrix", "result": [{"values": [[end, sample]]}]},
        }).encode()

    def cli(*args):
        result = subprocess.run(
            [sys.executable, "-m", "opspilot", *args], cwd=tmp_path,
            capture_output=True, encoding="utf-8", timeout=20,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    with processing_service(respond) as (processing_url, requests):
        base = processing_url.removesuffix("/processing")
        target = tmp_path / "target.json"
        target.write_text(json.dumps({
            "health": {"service_url": f"{base}/ready"},
            "prometheus": {
                "base_url": base, "service": "orders",
                "queries": {"error_rate": {"metric": "requests_total", "unit": "ratio", "threshold": 0.1}},
            },
        }), encoding="utf-8")
        args = ("diagnose-auto", "--config", str(target), "HTTP health")

        skipped = json.loads(cli(*args))
        assert requests == ["/ready"]
        assert skipped["skipped_checks"][0]["query_name"] == "error_rate"
        assert "error\\_rate" in cli("report", skipped["task_id"], "--format", "markdown")

        health_status, sample = 503, "0.5"
        failed = json.loads(cli(*args))
        assert failed["status"] == "undetermined"
        assert failed["evidence"][1]["trigger"] == "service_unhealthy"
        assert failed["evidence"][1]["threshold_exceeded"] is True

        health_status, sample = 200, "0"
        recovered = json.loads(cli(*args, "--check-metrics"))
        assert recovered["evidence"][1]["trigger"] == "explicit_recheck"
        assert recovered["status"] == "no_failure_observed"
        assert recovered["evidence"][1]["latest_value"] == 0

        metrics_status = 403
        unavailable = json.loads(cli(*args, "--check-metrics"))
        assert unavailable["evidence"][0]["http_status"] == 200
        assert unavailable["evidence"][1]["reason"] == "access_denied"
        assert unavailable["status"] == "undetermined"
        assert json.loads(cli("report", unavailable["task_id"])) == unavailable

    compare = ("compare-http", failed["task_id"], recovered["task_id"])
    pair, = json.loads(cli(*compare))["metric_comparisons"]
    assert pair["before"]["peak_value"] == 0.5
    assert pair["after"]["peak_value"] == 0
    assert "## Metric observations" in cli(*compare, "--format", "markdown")
    changed = {**recovered, "evidence": [
        recovered["evidence"][0], {**recovered["evidence"][1], "unit": "seconds"},
    ]}
    assert "metric_comparisons" not in compare_health_reports(failed, changed)
