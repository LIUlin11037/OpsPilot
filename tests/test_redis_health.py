import json
import subprocess
import sys
from contextlib import contextmanager
from socketserver import StreamRequestHandler, ThreadingTCPServer
from tempfile import TemporaryDirectory
from threading import Thread
from time import sleep

import pytest

from opspilot.connectors.redis_health import RedisHealthConnector

PING_FRAME = b"*1\r\n$4\r\nPING\r\n"


@contextmanager
def _redis_response(reply: bytes, *, delay: float = 0):
    requests: list[bytes] = []

    class Handler(StreamRequestHandler):
        def handle(self) -> None:
            self.connection.settimeout(2)
            requests.append(self.rfile.read(len(PING_FRAME)))
            if delay:
                sleep(delay)
            try:
                self.wfile.write(reply)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

    with ThreadingTCPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": .02})
        worker.start()
        try:
            yield f"redis://127.0.0.1:{server.server_address[1]}", requests
        finally:
            server.shutdown()
            worker.join()


@pytest.mark.parametrize(
    ("service_status", "reply", "dependency_health", "reason"),
    [
        (204, b"+PONG\r\n", None, None),
        (503, b"+PONG\r\n", "healthy", None),
        (503, b"-NOAUTH private fixture text\r\n", "unknown", "authentication_failed"),
        (503, b"+OK\r\n", "unknown", "invalid_redis_response"),
    ],
)
def test_http_diagnosis_probes_redis_only_after_service_anomaly(
    processing_service, service_status, reply, dependency_health, reason
) -> None:
    with TemporaryDirectory(prefix="opspilot-redis-health-test-") as directory:
        with processing_service(lambda path: (service_status, b"")) as (url, http_requests):
            with _redis_response(reply) as (redis_url, redis_requests):
                result = subprocess.run(
                    [
                        sys.executable, "-m", "opspilot", "diagnose-http", "--service-url", url,
                        "--redis-dependency-url", redis_url, "Check service and Redis",
                    ],
                    cwd=directory, capture_output=True, text=True, encoding="utf-8", timeout=20,
                )
    assert result.returncode == 0, result.stderr
    assert "private fixture text" not in result.stdout
    report = json.loads(result.stdout)
    assert http_requests == ["/processing"]
    service, *dependencies = report["evidence"]
    assert service["http_status"] == service_status
    if dependency_health is None:
        assert not redis_requests and not dependencies
        assert report["status"] == "no_failure_observed"
    else:
        assert redis_requests == [PING_FRAME]
        (dependency,) = dependencies
        assert dependency["tool"] == "inspect_dependency_health"
        assert dependency["source"] == redis_url
        assert dependency["health"] == dependency_health
        assert dependency.get("reason") == reason
        assert "http_status" not in dependency
        assert report["status"] == "undetermined"


def test_redis_dependency_timeout_is_unavailable() -> None:
    with _redis_response(b"+PONG\r\n", delay=.2) as (url, requests):
        evidence = RedisHealthConnector(url, timeout_seconds=.05).inspect_health()
    assert requests == [PING_FRAME]
    assert evidence["outcome"] == "timeout"
    assert evidence["health"] == "unknown"
    assert evidence["reason"] == "request_timeout"


def test_redis_dependency_rejects_credentials_in_url() -> None:
    with pytest.raises(ValueError, match="redis://HOST"):
        RedisHealthConnector("redis://reader:secret@127.0.0.1:6379")
