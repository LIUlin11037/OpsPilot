import json
import os
import subprocess
import sys
from base64 import b64encode
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


@pytest.mark.parametrize("pending", [0, 3])
def test_reads_ready_count_using_encoded_queue_path(pending: int) -> None:
    body = json.dumps({"messages": 12, "messages_ready": pending}).encode()

    with _management_api(body) as (url, requests):
        evidence = RabbitMQQueueConnector(
            f"{url}/management/", "orders/retry #1", "reader", "test-pass"
        ).inspect_queue()

    path = "/management/api/queues/%2F/orders%2Fretry%20%231"
    authorization = b64encode(b"reader:test-pass").decode("ascii")
    assert requests == [(path, f"Basic {authorization}")]
    assert evidence["outcome"] == "observed"
    assert evidence["pending_messages"] == pending
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
    ("http_status", "pending", "diagnosis_status", "outcome"),
    [
        (200, 0, "no_backlog_observed", "observed"),
        (200, 7, "undetermined", "observed"),
        (403, 0, "undetermined", "missing"),
    ],
)
def test_rabbitmq_diagnosis_saves_queue_evidence(
    http_status: int, pending: int, diagnosis_status: str, outcome: str
) -> None:
    body = json.dumps({"messages_ready": pending}).encode()
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
    if outcome == "observed":
        assert evidence["pending_messages"] == pending
    else:
        assert evidence["reason"] == "access_denied"
        assert "pending_messages" not in evidence
