"""Run the regular OpsPilot CLI against the bundled offline regression cases."""

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit


CASES = Path(__file__).with_name("cases.json")
HOST = "127.0.0.1"
SERVICE = "orders"
QUEUE_PATH = "/api/queues/%2F/orders"
QUERY_PATH = "/api/v1/query_range"
WINDOW_SECONDS = 30
STEP_SECONDS = 5
METRIC_CONFIG = {
    "error_rate": {"metric": "traefik_service_requests_total", "unit": "ratio", "threshold": 0.1},
    "latency_p95": {"metric": "traefik_service_request_duration_seconds_bucket", "unit": "seconds", "threshold": 0.2},
}
ABSENT = object()

# Use the normal CLI entry in a child process. Reject any connection beyond this case's fixture.
RUN_CLI = """
import runpy
import sys

host, port = '127.0.0.1', int(sys.argv.pop(1))

def only_fixture(event, args):
    if event == 'socket.getaddrinfo' and args[:2] != (host, port):
        raise RuntimeError('Regression command attempted to resolve a non-fixture endpoint.')
    if event in ('socket.connect', 'socket.sendto') and args[1] != (host, port):
        raise RuntimeError('Regression command attempted to connect outside its fixture.')

sys.addaudithook(only_fixture)
runpy.run_module('opspilot', run_name='__main__', alter_sys=True)
"""


class FixtureServer(ThreadingHTTPServer):
    def __init__(self, inputs: dict[str, Any]) -> None:
        self.inputs = inputs
        self.unexpected: list[str] = []
        super().__init__((HOST, 0), FixtureHandler)

    def reply(self, request: str) -> tuple[int, object | None]:
        url = urlsplit(request)
        if request == f"/{SERVICE}/" and "http_status" in self.inputs:
            return self.inputs["http_status"], None
        if request == QUEUE_PATH and "queue" in self.inputs:
            return 200, self.inputs["queue"]
        if url.path != QUERY_PATH or "metrics" not in self.inputs:
            raise ValueError(f"Unexpected fixture request {request}")

        params = parse_qs(url.query)
        query, = params["query"]
        end, = map(int, params["end"])
        start, = map(int, params["start"])
        step, = map(int, params["step"])
        if f'job="{SERVICE}"' not in query or step != STEP_SECONDS:
            raise ValueError(f"Unexpected metric scope or step in {request}")
        if "window" in self.inputs:
            window_start, window_end = map(datetime.fromisoformat, self.inputs["window"])
            interval = (int(window_start.timestamp()) + 60, int(window_end.timestamp()))
        else:
            interval = (end - WINDOW_SECONDS, end)
        if (start, end) != interval:
            raise ValueError(f"Unexpected metric window in {request}")

        names = [name for name in self.inputs["metrics"] if f'{METRIC_CONFIG[name]["metric"]}{{' in query]
        if len(names) != 1:
            raise ValueError(f"Unexpected metric query in {request}")
        samples = self.inputs["metrics"][names[0]]
        if samples == "query_timeout":
            return 503, {"status": "error", "errorType": "timeout"}
        return 200, {
            "status": "success", "data": {
                "resultType": "matrix",
                "result": [{"values": [[end + offset, value] for offset, value in samples]}],
            },
        }


class FixtureHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        server = cast(FixtureServer, self.server)
        try:
            code, payload = server.reply(self.path)
        except (KeyError, TypeError, ValueError) as error:
            server.unexpected.append(str(error))
            code, payload = 400, None
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        pass


@contextmanager
def fixture_server(inputs: dict[str, Any]) -> Iterator[FixtureServer]:
    with FixtureServer(inputs) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


def build_diagnosis_args(case: dict[str, Any], base: str, directory: Path) -> list[str]:
    inputs = case["inputs"]
    options: list[str] = []
    if "queue" in inputs:
        config = {"rabbitmq": {
            "management_url": base, "queue": SERVICE, "vhost": "/", "processing_record": "processing.json",
        }}
        (directory / "processing.json").write_text(json.dumps(inputs["processing"]), encoding="utf-8")
        options.extend(("--workflow", "queue", "--message-id", inputs["message_id"]))
    else:
        health = {"service_url": f"{base}/{SERVICE}/"}
        config = {"health": health}
        if "metrics" in inputs:
            config["prometheus"] = {
                "base_url": base, "service": SERVICE, "window_seconds": WINDOW_SECONDS, "step_seconds": STEP_SECONDS,
                "queries": {name: METRIC_CONFIG[name] for name in inputs["metrics"]},
            }
        if "window" in inputs:
            health["service_log_path"] = "service.jsonl"
            log = "".join(json.dumps(row) + "\n" for row in inputs["logs"])
            (directory / "service.jsonl").write_text(log, encoding="utf-8")
            start, end = inputs["window"]
            options.extend(("--incident-start", start, "--incident-end", end))
        else:
            options.extend(("--workflow", "http_health"))
    (directory / "target.json").write_text(json.dumps(config), encoding="utf-8")
    return ["diagnose-auto", "--config", "target.json", *options, "--", case["question"]]


def cli(directory: Path, port: int, *args: str) -> dict[str, Any]:
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("OPSPILOT_", "LANGCHAIN_", "LANGSMITH_"))
    }
    environment.update(
        OPSPILOT_RABBITMQ_USER="fixture", OPSPILOT_RABBITMQ_PASSWORD="fixture", PYTHONIOENCODING="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-c", RUN_CLI, str(port), "--db", "reports.sqlite3", *args],
        cwd=directory, env=environment, capture_output=True, encoding="utf-8", timeout=20,
    )
    if result.returncode:
        raise ValueError(f"{args[0]} exited {result.returncode}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def check(field: str, expected: object, actual: object) -> dict[str, Any]:
    numeric = type(expected) in (int, float) and type(actual) in (int, float)
    matching_type = type(expected) is type(actual) or numeric
    return {
        "field": field, "expected": None if expected is ABSENT else expected,
        "actual": None if actual is ABSENT else actual, "expected_present": expected is not ABSENT,
        "actual_present": actual is not ABSENT, "passed": matching_type and expected == actual,
    }


def compare(field: str, expected: object, actual: object) -> Iterator[dict[str, Any]]:
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key, value in expected.items():
            child = f"{field}.{key}" if field else key
            yield from compare(child, value, actual.get(key, ABSENT))
    elif isinstance(expected, list) and isinstance(actual, list):
        yield check(f"{field}.length", len(expected), len(actual))
        for index, item in enumerate(expected):
            value = actual[index] if index < len(actual) else ABSENT
            yield from compare(f"{field}.{index}", item, value)
    else:
        yield check(field, expected, actual)


def field_value(report: object, path: str) -> object:
    for key in path.split("."):
        if isinstance(report, dict):
            report = report.get(key, ABSENT)
        elif isinstance(report, list) and key.isdecimal() and int(key) < len(report):
            report = report[int(key)]
        else:
            return ABSENT
    return report


def run_case(case: dict[str, Any], directory: Path) -> dict[str, Any]:
    name = {"id": case["id"], "origin": case.get("origin")}
    try:
        with fixture_server(case["inputs"]) as server:
            port = server.server_port
            args = build_diagnosis_args(case, f"http://{HOST}:{port}", directory)
            diagnosed = cli(directory, port, *args)
            saved = cli(directory, port, "report", diagnosed["task_id"])
            if server.unexpected:
                raise ValueError("; ".join(server.unexpected))
        checks = [check("report_round_trip", True, saved == diagnosed), *compare("", case["expected"], saved)]
        checks.extend(check(path, ABSENT, field_value(saved, path)) for path in case.get("absent", ()))
    except (KeyError, OSError, TypeError, ValueError, subprocess.TimeoutExpired) as error:
        return {**name, "passed": False, "error": str(error)}
    return {**name, "passed": all(item["passed"] for item in checks), "checks": checks}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, default=CASES, help="Use an alternate case file.")
    args = parser.parse_args(argv)
    try:
        cases = json.loads(args.cases.read_text(encoding="utf-8"))
        if not isinstance(cases, list) or not cases or any(
            not isinstance(case, dict) or not isinstance(case.get("id"), str) or not case["id"] for case in cases
        ):
            raise ValueError("Cases must be a nonempty list of objects with string IDs.")
        if len({case["id"] for case in cases}) != len(cases):
            raise ValueError("Case IDs must be unique.")
    except (OSError, ValueError) as error:
        parser.error(str(error))

    results = []
    for case in cases:
        with TemporaryDirectory(prefix="opspilot-regression-") as directory:
            results.append(run_case(case, Path(directory)))
    passed = all(result["passed"] for result in results)
    print(json.dumps({"passed": passed, "cases": results}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
