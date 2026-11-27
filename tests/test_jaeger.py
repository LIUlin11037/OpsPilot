import json
from http.client import HTTPConnection

import pytest

from opspilot.connectors import jaeger
from opspilot.connectors.jaeger import JaegerConnector
from opspilot.jaeger_config import parse_jaeger_config
from opspilot.target_config import load_target_config


TRACE_ID = "abcdef0123456789abcdef0123456789"
OTHER_ID = "0123456789abcdef0123456789abcdef"
START = "1795764380279320000"
END = "1795764380280320000"


def recorded_span(span_id="0102030405060708", *, trace_id=TRACE_ID, **extra):
    return {
        "traceId": trace_id, "spanId": span_id, "name": "GET /orders", "kind": 2,
        "startTimeUnixNano": START, "endTimeUnixNano": END, "status": {"code": 2, "message": "private status"},
        "attributes": [{"key": "request.body", "value": {"stringValue": "private payload"}}],
        **extra,
    }


def frame(*spans, service="orders"):
    return json.dumps({"result": {"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
        "scopeSpans": [{"scope": {}, "spans": list(spans)}],
    }]}}).encode() + b"\n"


def query(processing_service, status, body, token=None):
    with processing_service(lambda _: (status, body)) as (endpoint, requests):
        base = endpoint.removesuffix("/processing") + "/trace-proxy/"
        connector = JaegerConnector(parse_jaeger_config({"base_url": base}), token)
        read = connector.inspect_trace(TRACE_ID.upper())
    assert requests == ["/trace-proxy/api/v3/traces/" + TRACE_ID]
    assert read["source"] == base + "api/v3/traces/" + TRACE_ID
    return read


def test_reads_readonly_v3_frames_and_keeps_only_span_evidence(processing_service, monkeypatch):
    methods = []

    class ObservedConnection(HTTPConnection):
        def request(self, method, url, body=None, headers=None, *, encode_chunked=False):
            methods.append((method, headers.get("Authorization")))
            return super().request(method, url, body, headers, encode_chunked=encode_chunked)

    monkeypatch.setattr(jaeger, "HTTPConnection", ObservedConnection)
    second = recorded_span("1112131415161718", parentSpanId="0102030405060708", status={})
    read = query(processing_service, 200, frame(recorded_span()) + frame(second, service="downstream"), "test-token")

    assert methods == [("GET", "Bearer test-token")]
    assert read["outcome"] == "observed" and read["gaps"] == []
    one, two = read["spans"]
    assert one == {
        "trace_id": TRACE_ID, "span_id": "0102030405060708", "parent_span_id": None,
        "service": "orders", "operation": "GET /orders", "kind": "server",
        "start_time_unix_nano": int(START), "end_time_unix_nano": int(END), "status": "error",
    }
    assert two["parent_span_id"] == one["span_id"] and two["status"] == "unset"
    assert "private" not in json.dumps(read) and "test-token" not in json.dumps(read)


@pytest.mark.parametrize("status, body, outcome, reason", [
    (404, b'{"error":{"httpCode":404,"message":"No traces found"}}', "observed", "trace_not_found"),
    (404, b"proxy route missing", "missing", "unexpected_http_status"),
    (200, b'{"result":{}}\n', "observed", "trace_not_found"),
    (401, b"private error", "missing", "authentication_failed"),
    (403, b"private error", "missing", "access_denied"),
])
def test_distinguishes_no_match_from_unavailable_source(processing_service, status, body, outcome, reason):
    read = query(processing_service, status, body)
    assert read["outcome"] == outcome and read["reason"] == reason and read["spans"] == []


def test_foreign_and_malformed_frames_preserve_verified_evidence(processing_service):
    body = frame(recorded_span(), recorded_span(trace_id=OTHER_ID)) + b'{"invalid":\n'
    read = query(processing_service, 200, body)
    assert read["outcome"] == "missing" and read["reason"] == "invalid_response"
    assert read["gaps"] == ["foreign_trace_id"]
    assert [span["trace_id"] for span in read["spans"]] == [TRACE_ID]


def test_capped_span_count_retains_valid_spans_and_reports_the_gap(processing_service, monkeypatch):
    monkeypatch.setattr(jaeger, "MAX_SPANS", 1)
    body = frame(recorded_span(), recorded_span("1112131415161718"))
    read = query(processing_service, 200, body)
    assert read["reason"] == "span_limit" and read["outcome"] == "missing"
    assert [span["span_id"] for span in read["spans"]] == ["0102030405060708"]


def test_response_bytes_are_bounded(processing_service, monkeypatch):
    monkeypatch.setattr(jaeger, "MAX_RESPONSE_BYTES", 20)
    read = query(processing_service, 200, frame(recorded_span()))
    assert read["outcome"] == "missing" and read["reason"] == "response_limit" and not read["spans"]


def test_timeout_does_not_turn_into_an_empty_lookup(monkeypatch):
    class TimedOutConnection(HTTPConnection):
        def request(self, *args, **kwargs):
            raise TimeoutError

    monkeypatch.setattr(jaeger, "HTTPConnection", TimedOutConnection)
    connector = JaegerConnector(parse_jaeger_config({"base_url": "http://127.0.0.1:16686"}))
    read = connector.inspect_trace(TRACE_ID)
    assert read["outcome"] == "timeout" and read["reason"] == "request_timeout"


@pytest.mark.parametrize("section, field", [
    ({"base_url": "http://user:pass@localhost:16686"}, "jaeger.base_url"),
    ({"base_url": "https://example.invalid/path?search=x"}, "jaeger.base_url"),
    ({"base_url": "http://localhost:0"}, "jaeger.base_url"),
    ({"base_url": "http://localhost", "timeout_seconds": 16}, "jaeger.timeout_seconds"),
    ({"base_url": "http://localhost", "token": "dummy"}, "unsupported"),
])
def test_rejects_unbounded_or_credential_bearing_trace_config(section, field):
    with pytest.raises(ValueError, match=field):
        parse_jaeger_config(section)


def test_jaeger_only_target_config_is_valid(tmp_path):
    path = tmp_path / "target.json"
    path.write_text('{"jaeger":{"base_url":"https://trace.example.invalid"}}', encoding="utf-8")
    config = load_target_config(path).jaeger
    assert config is not None and config.timeout_seconds == 5
