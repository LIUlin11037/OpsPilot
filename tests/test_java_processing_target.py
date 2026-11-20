from contextlib import closing
from http.client import HTTPConnection
from urllib.parse import urlsplit

import pytest

from opspilot.adapters.http_processing import HttpProcessingAdapter


def test_java_target_implements_processing_contract(
    pytestconfig: pytest.Config, java_processing_service
) -> None:
    if not pytestconfig.getoption("--java-target"):
        pytest.skip("Pass --java-target to run the Java processing target.")

    queue = 'orders + "retry"'
    vhost = "/sandbox & diagnostics"
    with java_processing_service(queue, vhost) as endpoint:
        adapter = HttpProcessingAdapter(endpoint, queue, vhost)
        latest = adapter.inspect_processing()
        requested = adapter.inspect_processing(message_id="order-msg-101")
        missing = adapter.inspect_processing(message_id="order-msg-999")
        other_queue = HttpProcessingAdapter(endpoint, "another", vhost)
        wrong_target = other_queue.inspect_processing()

        url = urlsplit(endpoint)
        with closing(HTTPConnection(url.hostname, url.port, timeout=2)) as connection:
            connection.request("POST", url.path)
            with connection.getresponse() as response:
                assert response.status == 405
                assert response.headers.get("Allow") == "GET"

    assert latest["outcome"] == "observed"
    assert latest["message_id"] == "order-msg-102"
    assert requested["outcome"] == "observed"
    assert requested["queue"] == queue
    assert requested["vhost"] == vhost
    assert requested["message_id"] == "order-msg-101"
    assert requested["result"] == "failed"
    assert requested["attempted_at"] == "2026-11-19T03:00:00+00:00"
    assert missing["outcome"] == "observed"
    assert missing["row_count"] == 0
    assert missing["reason"] == "message_not_found"
    assert wrong_target["outcome"] == "missing"
    assert wrong_target["http_status"] == 404
