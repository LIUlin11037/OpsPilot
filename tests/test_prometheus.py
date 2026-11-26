import json
from datetime import datetime
from time import sleep
from urllib.parse import parse_qs, urlsplit

import pytest

from opspilot.connectors.prometheus import PrometheusConnector
from opspilot.prometheus_config import parse_prometheus_config

REFERENCE = "2026-11-26T08:00:00+00:00"
END = int(datetime.fromisoformat(REFERENCE).timestamp())


def _connector(base_url, name="error_rate", timeout_seconds=2):
    metric = "http_requests_total" if name == "error_rate" else "http_request_duration_seconds_bucket"
    unit = "ratio" if name == "error_rate" else "seconds"
    config = parse_prometheus_config({
        "base_url": base_url, "service": "orders", "window_seconds": 60, "step_seconds": 10,
        "timeout_seconds": timeout_seconds,
        "queries": {name: {"metric": metric, "unit": unit, "threshold": 0.2}},
    })
    return PrometheusConnector(config), config.queries[0]


def _matrix(*series):
    return json.dumps({
        "status": "success", "data": {"resultType": "matrix", "result": [{"values": s} for s in series]},
    }).encode()


@pytest.mark.parametrize("name", ["error_rate", "latency_p95"])
def test_prometheus_reads_named_service_and_preserves_real_zero(processing_service, name):
    with processing_service(lambda path: (200, _matrix([[END - 10, "0.25"], [END, "0"]]))) as (base, requests):
        connector, query = _connector(base.removesuffix("/processing"), name)
        observation = connector.inspect(query, REFERENCE)
    assert observation["outcome"] == "observed"
    assert observation["latest_value"] == 0.0
    assert observation["peak_value"] == 0.25
    assert observation["sample_count"] == 2
    assert observation["latest_sample_at"] == REFERENCE
    request = urlsplit(requests[0])
    assert request.path == "/api/v1/query_range"
    params = parse_qs(request.query)
    assert params["end"] == [str(END)]
    assert params["start"] == [str(END - 60)]
    assert params["step"] == ["10"]
    assert params["limit"] == ["2"]
    assert 'job="orders"' in params["query"][0]
    assert ('status=~"5.."' if name == "error_rate" else "histogram_quantile(0.95") in params["query"][0]


@pytest.mark.parametrize(("status", "body", "outcome", "reason"), [
    (200, _matrix(), "missing", "no_data"),
    (200, _matrix([[END, "NaN"]]), "missing", "no_data"),
    (200, _matrix([[END - 30, "0"]]), "missing", "stale_samples"),
    (401, b"", "missing", "authentication_failed"),
    (403, b"", "missing", "access_denied"),
    (503, b'{"status":"error","errorType":"timeout"}', "timeout", "query_timeout"),
    (200, _matrix([[END, "0"]], [[END, "0"]]), "missing", "query_limit_exceeded"),
    (200, _matrix([[END - 7 + index, "0"] for index in range(8)]), "missing", "query_limit_exceeded"),
    (200, b" " * (256 * 1024 + 1), "missing", "query_limit_exceeded"),
    (200, _matrix([[END + 1, "0"]]), "missing", "invalid_response"),
    (200, _matrix([[END, "+Inf"]]), "missing", "invalid_response"),
])
def test_prometheus_categorizes_unavailable_evidence(processing_service, status, body, outcome, reason):
    with processing_service(lambda path: (status, body)) as (base, _):
        connector, query = _connector(base.removesuffix("/processing"))
        observation = connector.inspect(query, REFERENCE)
    assert observation["outcome"] == outcome
    assert observation["reason"] == reason
    assert "latest_value" not in observation


def test_prometheus_respects_remaining_timeout(processing_service):
    def respond(path):
        sleep(0.15)
        return 200, _matrix()

    with processing_service(respond) as (base, requests):
        connector, query = _connector(base.removesuffix("/processing"))
        observation = connector.inspect(query, REFERENCE, timeout=0.05)
        exhausted = connector.inspect(query, REFERENCE, timeout=0.0)
    assert observation["outcome"] == exhausted["outcome"] == "timeout"
    assert observation["reason"] == "request_timeout"
    assert exhausted["reason"] == "time_budget_exhausted"
    assert len(requests) == 1
