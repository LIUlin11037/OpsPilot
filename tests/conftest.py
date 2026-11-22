from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from shutil import which
from subprocess import PIPE, STDOUT, Popen, TimeoutExpired
from threading import Thread

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--rabbitmq-live",
        action="store_true",
        help="Run acceptance tests against the configured test RabbitMQ broker.",
    )
    parser.addoption(
        "--java-target",
        action="store_true",
        help="Run the Java processing target contract test and use Java in live acceptance.",
    )
    parser.addoption(
        "--hirehub-live",
        action="store_true",
        help="Run read-only acceptance against a prepared HireHub test delivery.",
    )
    parser.addoption(
        "--voting-live",
        type=Path,
        metavar="COMPOSE_FILE",
        help="Run isolated voting acceptance using the upstream docker-compose.images.yml.",
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


@contextmanager
def _serve_java_processing(queue: str, vhost: str) -> Iterator[str]:
    java = which("java")
    if java is None:
        pytest.fail("Put JDK 17 or newer on PATH to run the Java target tests.")
    examples = Path(__file__).resolve().parents[1] / "examples"
    source = examples / "java-processing-target" / "ProcessingTarget.java"
    command = [java, "--add-modules", "jdk.httpserver", str(source), "0", queue, vhost]

    with Popen(command, stdout=PIPE, stderr=STDOUT, encoding="utf-8", errors="replace") as process:
        assert process.stdout is not None
        output = process.stdout

        def read_endpoint() -> str:
            startup_output: deque[str] = deque(maxlen=8)
            for line in output:
                endpoint = line.strip()
                if endpoint.startswith("http://127.0.0.1:") and endpoint.endswith("/processing"):
                    return endpoint
                startup_output.append(line)
            pytest.fail(f"Java target stopped before startup.\n{''.join(startup_output)}")

        with ThreadPoolExecutor(max_workers=1) as worker:
            try:
                try:
                    endpoint = worker.submit(read_endpoint).result(timeout=20)
                except TimeoutError:
                    pytest.fail("Java target did not announce its endpoint within 20 seconds.")
                yield endpoint
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)


@pytest.fixture
def java_processing_service() -> Callable[[str, str], AbstractContextManager[str]]:
    return _serve_java_processing
