import json
from time import sleep
from urllib.parse import parse_qs, urlsplit

import pytest

from opspilot.adapters.http_processing import HttpProcessingAdapter

RECORD = {
    "queue": "订单 /#",
    "vhost": "/staging +",
    "message_id": "msg+雪&7",
    "result": "failed",
    "attempted_at": "2026-11-19T03:00:00Z",
}


def test_http_processing_encodes_query_and_selects_exact_record(processing_service) -> None:
    other = {**RECORD, "message_id": "newer", "attempted_at": "2026-11-19T03:01:00Z"}
    body = json.dumps([RECORD, other]).encode()
    with processing_service(lambda _path: (200, body)) as (endpoint, requests):
        adapter = HttpProcessingAdapter(endpoint, RECORD["queue"], RECORD["vhost"])
        evidence = adapter.inspect_processing(message_id=RECORD["message_id"])

    (request,) = requests
    assert urlsplit(request).path == "/processing"
    assert parse_qs(urlsplit(request).query) == {
        key: [RECORD[key]] for key in ("queue", "vhost", "message_id")
    }
    assert evidence["outcome"] == "observed"
    assert evidence["message_id"] == RECORD["message_id"]
    assert evidence["result"] == "failed"
    assert evidence["source"] == endpoint.removesuffix("/processing") + request


def test_http_processing_omits_message_id_for_latest_lookup(processing_service) -> None:
    body = json.dumps(RECORD).encode()
    with processing_service(lambda _path: (200, body)) as (endpoint, requests):
        adapter = HttpProcessingAdapter(endpoint, RECORD["queue"], RECORD["vhost"])
        evidence = adapter.inspect_processing()

    (request,) = requests
    assert parse_qs(urlsplit(request).query) == {
        key: [RECORD[key]] for key in ("queue", "vhost")
    }
    assert evidence["outcome"] == "observed"
    assert evidence["message_id"] == RECORD["message_id"]


@pytest.mark.parametrize(
    ("status", "body", "message_id", "reason"),
    [
        (401, b"", None, "authentication_failed"),
        (403, b"", None, "access_denied"),
        (404, b"", None, "record_not_found"),
        (404, b"", "msg-101", "message_not_found"),
        (503, b"", None, "http_error"),
        (200, b"not JSON", None, "invalid_record"),
        (200, b"[]", "msg-101", "message_not_found"),
        (200, json.dumps({**RECORD, "queue": "another"}).encode(), None, "target_mismatch"),
    ],
)
def test_http_processing_retains_unavailable_reasons(
    processing_service, status: int, body: bytes, message_id: str | None, reason: str
) -> None:
    with processing_service(lambda _path: (status, body)) as (endpoint, _requests):
        adapter = HttpProcessingAdapter(endpoint, RECORD["queue"], RECORD["vhost"])
        evidence = adapter.inspect_processing(message_id=message_id)

    assert evidence["outcome"] == "missing"
    assert evidence["reason"] == reason
    assert evidence["http_status"] == status
    assert "result" not in evidence


def test_http_processing_reports_socket_timeout(processing_service) -> None:
    def delayed(_path: str) -> tuple[int, bytes]:
        sleep(0.4)
        return 200, b"[]"

    with processing_service(delayed) as (endpoint, _requests):
        adapter = HttpProcessingAdapter(endpoint, "orders", "/", timeout_seconds=0.1)
        evidence = adapter.inspect_processing()

    assert evidence["outcome"] == "timeout"
    assert evidence["reason"] == "request_timeout"


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://localhost/processing",
        "http://user:pass@localhost/processing",
        "http://localhost/processing?other=value",
    ],
)
def test_http_processing_requires_a_plain_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError, match="Use an HTTP"):
        HttpProcessingAdapter(endpoint, "orders", "/")
