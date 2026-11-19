from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--rabbitmq-live",
        action="store_true",
        help="Run acceptance tests against the configured test RabbitMQ broker.",
    )


@contextmanager
def _serve_processing(
    respond: Callable[[str], tuple[int, bytes]],
) -> Iterator[tuple[str, list[str]]]:
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            status, body = respond(self.path)
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format: str, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/processing", requests
        finally:
            server.shutdown()
            worker.join()


@pytest.fixture
def processing_service() -> Callable[
    [Callable[[str], tuple[int, bytes]]], AbstractContextManager[tuple[str, list[str]]]
]:
    return _serve_processing
