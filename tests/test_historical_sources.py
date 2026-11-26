import json
from urllib.parse import parse_qs, urlsplit

from opspilot.connectors.jsonl_service_log import JsonlServiceLogConnector
from opspilot.connectors.prometheus import PrometheusConnector
from opspilot.core.incident_window import parse_incident_window
from opspilot.prometheus_config import parse_prometheus_config


def test_historical_logs_count_only_the_exact_service_and_window(tmp_path):
    path = tmp_path / "shared.jsonl"
    window = parse_incident_window("2024-01-01T00:00:00Z", "2024-01-01T00:02:00Z")
    rows = [
        {"timestamp": "2023-12-31T23:59:59Z", "service": "orders", "level": "ERROR"},
        {"timestamp": "2024-01-01T00:00:00Z", "service": "orders", "level": "ERROR"},
        {"timestamp": "2024-01-01T00:01:00Z", "service": "orders-v2", "level": "ERROR"},
        {"timestamp": "2024-01-01T00:01:10Z", "service": "orders", "level": "INFO"},
        {"timestamp": "2024-01-01T00:02:00Z", "service": "orders", "level": "FATAL"},
        {"timestamp": "2024-01-01T00:02:01Z", "service": "orders", "level": "ERROR"},
        {"timestamp": "2024-01-01T00:01:10Z", "level": "ERROR"},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    log = JsonlServiceLogConnector(path)
    observed = log.inspect_window(window, "orders")
    assert observed["outcome"] == "observed"
    assert observed["error_count"] == 2
    assert observed["matched_entries"] == 3
    assert observed["unscoped_entries"] == 1
    assert observed["service"] == "orders"
    assert observed["window_start"] == window.start.isoformat()
    assert observed["window_end"] == window.end.isoformat()
    other = log.inspect_window(window, "missing")
    assert other["outcome"] == "observed"
    assert other["error_count"] == other["matched_entries"] == 0
    assert other["unscoped_entries"] == 1


def test_historical_metric_evaluation_stays_inside_the_incident_window(processing_service):
    window = parse_incident_window("2024-01-01T00:00:00Z", "2024-01-01T00:02:00Z")
    expected_start = int(window.evaluation_start.timestamp())
    expected_end = int(window.end.timestamp())
    samples = [[expected_start, "0.8"], [expected_end, "0"]]
    body = json.dumps({
        "status": "success", "data": {"resultType": "matrix", "result": [{"values": samples}]},
    }).encode()
    with processing_service(lambda path: (200, body)) as (url, requests):
        config = parse_prometheus_config({
            "base_url": url.removesuffix("/processing"), "service": "orders", "step_seconds": 120,
            "queries": {"error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.1}},
        })
        observed = PrometheusConnector(config).inspect(
            config.queries[0], "2026-11-26T10:00:00+00:00", window=window,
        )
    params = parse_qs(urlsplit(requests[0]).query)
    assert params["start"] == [str(expected_start)]
    assert params["end"] == [str(expected_end)]
    assert params["step"] == ["60"]
    assert 'job="orders"' in params["query"][0] and "[1m]" in params["query"][0]
    assert observed["outcome"] == "observed"
    assert observed["peak_value"] == 0.8
    assert observed["latest_value"] == 0
    assert observed["sample_count"] == 2
    assert observed["window_start"] == window.start.isoformat()
    assert observed["evaluation_start"] == window.evaluation_start.isoformat()
    assert observed["window_end"] == window.end.isoformat()
