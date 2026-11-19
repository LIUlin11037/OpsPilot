import json
import os
import subprocess
import sys
from base64 import b64encode
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread

import pytest

from opspilot.connectors.rabbitmq import RabbitMQQueueConnector


@contextmanager
def _management_api(
    body: bytes,
    *,
    status: int = 200,
    stall: bool = False,
) -> Iterator[tuple[str, list[tuple[str, str | None]]]]:
    requests: list[tuple[str, str | None]] = []
    release = Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.path, self.headers.get("Authorization")))
            if stall:
                release.wait()
                return
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if status == 302:
                self.send_header("Location", "/another-endpoint")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests
        finally:
            release.set()
            server.shutdown()
            worker.join()


def _cli(*arguments: str, cwd: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *arguments],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=30,
        env={
            **os.environ,
            "OPSPILOT_RABBITMQ_USER": "reader",
            "OPSPILOT_RABBITMQ_PASSWORD": "test-pass",
        },
    )


@pytest.mark.parametrize(
    ("pending", "unacknowledged", "registered"), [(0, 0, 0), (3, 4, 2)]
)
def test_reads_metrics_using_encoded_queue_path(
    pending: int, unacknowledged: int, registered: int
) -> None:
    body = json.dumps(
        {
            "messages": pending + unacknowledged,
            "messages_ready": pending,
            "messages_unacknowledged": unacknowledged,
            "consumers": registered,
        }
    ).encode()

    with _management_api(body) as (url, requests):
        evidence = RabbitMQQueueConnector(
            f"{url}/management/", "orders/retry #1", "reader", "test-pass"
        ).inspect_queue()

    path = "/management/api/queues/%2F/orders%2Fretry%20%231"
    authorization = b64encode(b"reader:test-pass").decode("ascii")
    assert requests == [(path, f"Basic {authorization}")]
    assert evidence["outcome"] == "observed"
    assert evidence["pending_messages"] == pending
    assert evidence["unacknowledged_messages"] == unacknowledged
    assert evidence["registered_consumers"] == registered
    assert evidence["sampled_pending_message_ids"] == []
    assert evidence["source"] == f"{url}{path}"
    assert datetime.fromisoformat(evidence["observed_at"]).utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (302, "http_error"),
        (401, "authentication_failed"),
        (403, "access_denied"),
        (404, "queue_not_found"),
        (503, "http_error"),
    ],
)
def test_http_errors_are_unavailable_evidence(status: int, reason: str) -> None:
    with _management_api(b'{"messages_ready": 0}', status=status) as (url, requests):
        evidence = RabbitMQQueueConnector(url, "orders", "reader", "test-pass")
        result = evidence.inspect_queue()

    assert len(requests) == 1
    assert result["outcome"] == "missing"
    assert result["reason"] == reason
    assert result["http_status"] == status
    assert "pending_messages" not in result


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"not json", "invalid_response"),
        (b"[]", "invalid_response"),
        (b"{}", "metric_missing"),
        (b'{"messages_ready": null}', "metric_missing"),
        (b'{"messages_ready": false}', "invalid_response"),
        (b'{"messages_ready": -1}', "invalid_response"),
    ],
)
def test_missing_or_malformed_counts_stay_unknown(body: bytes, reason: str) -> None:
    with _management_api(body) as (url, _requests):
        result = RabbitMQQueueConnector(url, "orders", "reader", "test-pass").inspect_queue()

    assert result["outcome"] == "missing"
    assert result["reason"] == reason
    assert "pending_messages" not in result


@pytest.mark.parametrize("registered", [None, -1, True, "2"])
def test_unusable_consumer_count_preserves_pending_count(registered: object) -> None:
    body = json.dumps({"messages_ready": 7, "consumers": registered}).encode()
    with _management_api(body) as (url, _requests):
        result = RabbitMQQueueConnector(url, "orders", "reader", "test-pass").inspect_queue()

    assert result["outcome"] == "observed"
    assert result["pending_messages"] == 7
    assert result["registered_consumers"] is None


@pytest.mark.parametrize("unacknowledged", [None, -1, True, "2"])
def test_unusable_acknowledgement_count_preserves_other_metrics(
    unacknowledged: object,
) -> None:
    body = json.dumps(
        {"messages_ready": 7, "messages_unacknowledged": unacknowledged, "consumers": 2}
    ).encode()
    with _management_api(body) as (url, _requests):
        result = RabbitMQQueueConnector(url, "orders", "reader", "test-pass").inspect_queue()

    assert result["outcome"] == "observed"
    assert result["pending_messages"] == 7
    assert result["registered_consumers"] == 2
    assert result["unacknowledged_messages"] is None


def test_missing_ready_count_preserves_other_metrics() -> None:
    with _management_api(b'{"consumers": 2, "messages_unacknowledged": 4}') as (
        url,
        _requests,
    ):
        result = RabbitMQQueueConnector(url, "orders", "reader", "test-pass").inspect_queue()

    assert result["outcome"] == "missing"
    assert result["reason"] == "metric_missing"
    assert result["registered_consumers"] == 2
    assert result["unacknowledged_messages"] == 4
    assert "pending_messages" not in result


def test_socket_timeout_is_reported_separately() -> None:
    with _management_api(b"", stall=True) as (url, _requests):
        connector = RabbitMQQueueConnector(
            url, "orders", "reader", "test-pass", timeout_seconds=0.2
        )
        result = connector.inspect_queue()

    assert result["outcome"] == "timeout"
    assert result["reason"] == "request_timeout"
    assert result["http_status"] is None
    assert "pending_messages" not in result


@pytest.mark.parametrize(
    ("status", "exit_code", "outcome"),
    [(200, 0, "observed"), (403, 1, "missing")],
)
def test_inspect_queue_cli(status: int, exit_code: int, outcome: str) -> None:
    with _management_api(b'{"messages_ready": 7}', status=status) as (url, requests):
        result = _cli("inspect-queue", "--management-url", url, "--queue", "orders")

    assert result.returncode == exit_code
    assert not result.stderr
    evidence = json.loads(result.stdout)
    assert evidence["outcome"] == outcome
    assert evidence["source"] == f"{url}/api/queues/%2F/orders"
    assert "test-pass" not in result.stdout
    assert len(requests) == 1


@pytest.mark.parametrize(
    ("http_status", "pending", "unacknowledged", "registered", "diagnosis_status", "outcome"),
    [
        (200, 0, 0, 0, "no_backlog_observed", "observed"),
        (200, 0, 3, 2, "no_backlog_observed", "observed"),
        (200, 7, 0, 0, "undetermined", "observed"),
        (200, 7, 3, 2, "undetermined", "observed"),
        (200, 7, None, None, "undetermined", "observed"),
        (200, None, 4, 0, "undetermined", "missing"),
        (403, 0, None, None, "undetermined", "missing"),
    ],
)
def test_rabbitmq_diagnosis_saves_queue_evidence(
    http_status: int,
    pending: int | None,
    unacknowledged: int | None,
    registered: int | None,
    diagnosis_status: str,
    outcome: str,
) -> None:
    metrics = {"messages_ready": pending}
    if unacknowledged is not None:
        metrics["messages_unacknowledged"] = unacknowledged
    if registered is not None:
        metrics["consumers"] = registered
    body = json.dumps(metrics).encode()
    with TemporaryDirectory(prefix="opspilot-rabbitmq-test-") as directory:
        with _management_api(body, status=http_status) as (url, requests):
            result = _cli(
                "diagnose",
                "--management-url",
                url,
                "--queue",
                "orders",
                "Check backlog",
                cwd=directory,
            )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        saved = _cli("report", report["task_id"], cwd=directory)

    assert saved.returncode == 0, saved.stderr
    assert json.loads(saved.stdout) == report
    assert len(requests) == 1
    assert report["status"] == diagnosis_status
    assert report["processing_correlation"] is None
    evidence, = report["evidence"]
    call, = report["tool_calls"]
    assert call["tool"] == evidence["tool"] == "inspect_queue"
    assert call["outcome"] == evidence["outcome"] == outcome
    assert evidence["registered_consumers"] == registered
    assert evidence["unacknowledged_messages"] == unacknowledged
    if outcome == "observed":
        assert evidence["pending_messages"] == pending
        if unacknowledged is None:
            assert "unacknowledged message count is unavailable" in report["conclusion"]
        else:
            assert (
                f"delivered messages awaiting acknowledgement is {unacknowledged}"
                in report["conclusion"]
            )
    else:
        assert evidence["reason"] == ("metric_missing" if http_status == 200 else "access_denied")
        assert "pending_messages" not in evidence
        if http_status == 200:
            assert "pending message count is unavailable" in report["conclusion"]
            assert (
                f"delivered messages awaiting acknowledgement is {unacknowledged}"
                in report["conclusion"]
            )
            assert f"registered consumer count of {registered}" in report["conclusion"]

    if pending is not None and pending > 0:
        if registered is None:
            assert "registered consumer count is unavailable" in report["conclusion"]
        else:
            assert f"registered consumer count of {registered}" in report["conclusion"]


@pytest.mark.parametrize(
    ("http_status", "pending", "unacknowledged", "expected_outcomes"),
    [
        (200, 0, 0, ["observed"]),
        (403, 0, 3, ["missing"]),
        (200, 3, 0, ["observed", "missing"]),
        (200, 0, 3, ["observed", "observed"]),
        (200, None, 3, ["missing", "observed"]),
        (200, None, 0, ["missing"]),
    ],
)
def test_rabbitmq_diagnosis_reads_record_only_for_observed_queue_activity(
    http_status: int, pending: int | None, unacknowledged: int, expected_outcomes: list[str]
) -> None:
    body = json.dumps(
        {
            "messages_ready": pending,
            "messages_unacknowledged": unacknowledged,
            "consumers": 0,
        }
    ).encode()

    with TemporaryDirectory(prefix="opspilot-rabbitmq-test-") as directory:
        record = Path(directory) / "processing.json"
        if unacknowledged > 0:
            record.write_text(
                json.dumps(
                    {
                        "queue": "orders",
                        "vhost": "/",
                        "message_id": "msg-101",
                        "result": "succeeded",
                        "attempted_at": "2026-11-19T03:00:00Z",
                    }
                ),
                encoding="utf-8",
            )
        with _management_api(body, status=http_status) as (url, requests):
            result = _cli(
                "diagnose",
                "--management-url",
                url,
                "--queue",
                "orders",
                "--processing-record",
                str(record),
                "Investigate deliveries",
                cwd=directory,
            )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert len(requests) == 1
    assert [call["outcome"] for call in report["tool_calls"]] == expected_outcomes
    if http_status != 200:
        assert report["processing_correlation"] is None
    elif pending is not None and pending > 0:
        assert report["processing_correlation"] == "unknown"
        assert report["evidence"][1]["reason"] == "record_not_found"
    elif unacknowledged > 0:
        assert report["status"] == ("undetermined" if pending is None else "no_backlog_observed")
        assert report["processing_correlation"] == "unknown"
        assert report["evidence"][1]["result"] == "succeeded"
        assert "The latest processing attempt succeeded." in report["conclusion"]
        if pending is None:
            assert "pending message count is unavailable" in report["conclusion"]
            assert f"delivered messages awaiting acknowledgement is {unacknowledged}" in report["conclusion"]
            assert "registered consumer count of 0" in report["conclusion"]
    else:
        assert report["processing_correlation"] is None


@pytest.mark.parametrize(
    ("http_status", "message_id", "status"),
    [
        (200, "msg-101", "no_backlog_observed"),
        (403, "msg-101", "undetermined"),
        (200, "msg-999", "no_backlog_observed"),
    ],
)
def test_requested_message_is_checked_independently_of_queue_activity(
    http_status: int, message_id: str, status: str
) -> None:
    record = {
        "queue": "orders",
        "vhost": "/",
        "message_id": "msg-101",
        "result": "succeeded",
        "attempted_at": "2026-11-19T03:00:00Z",
    }
    other = {**record, "message_id": "msg-102", "attempted_at": "2026-11-19T03:01:00Z"}
    with TemporaryDirectory(prefix="opspilot-rabbitmq-test-") as directory:
        path = Path(directory) / "processing.json"
        path.write_text(json.dumps([record, other]), encoding="utf-8")
        body = b'{"messages_ready": 0, "messages_unacknowledged": 0}'
        with _management_api(body, status=http_status) as (url, _requests):
            result = _cli(
                "diagnose",
                "--management-url",
                url,
                "--queue",
                "orders",
                "--processing-record",
                str(path),
                "--message-id",
                message_id,
                "Check this delivery",
                cwd=directory,
            )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        stored = _cli("report", report["task_id"], cwd=directory)

    assert stored.returncode == 0, stored.stderr
    assert json.loads(stored.stdout) == report
    assert report["requested_message_id"] == message_id
    assert report["status"] == status
    assert report["processing_correlation"] == "unknown"
    assert [call["tool"] for call in report["tool_calls"]] == [
        "inspect_queue",
        "inspect_processing",
    ]
    processing = report["evidence"][1]
    if message_id == "msg-101":
        assert processing["message_id"] == message_id
        assert "for the requested message succeeded." in report["conclusion"]
    else:
        assert processing["outcome"] == "missing"
        assert processing["reason"] == "message_not_found"
        assert "lookup for the requested message returned no data." in report["conclusion"]
