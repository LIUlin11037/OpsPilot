"""Run an isolated fault and recovery demo through the OpsPilot CLI."""

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import mkdtemp
from threading import Thread
from time import sleep
from urllib.parse import parse_qs, urlsplit

QUEUE_NAME = "orders"
QUEUE_PATH = "/api/queues/%2F/orders"


class DemoServer(ThreadingHTTPServer):
    recovered = False
    queue_delay = 0.0


class DemoHandler(BaseHTTPRequestHandler):
    server: DemoServer

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        recovered = self.server.recovered
        if url.path in ("/service/health", "/consumer/health"):
            self._respond(204 if recovered else 503)
        elif url.path == QUEUE_PATH:
            if self.server.queue_delay:
                sleep(self.server.queue_delay)
            self._respond(200, {
                "messages_ready": 0 if recovered else 3,
                "messages_unacknowledged": 0,
                "consumers": 1,
            })
        elif url.path == "/processing":
            query = parse_qs(url.query)
            scope = {"queue": [QUEUE_NAME], "vhost": ["/"]}
            message = "demo-order-101"
            if query not in (scope, {**scope, "message_id": [message]}):
                self._respond(404)
                return
            records = [{
                "queue": QUEUE_NAME, "vhost": "/", "message_id": "demo-order-101",
                "result": "succeeded" if recovered else "failed",
                "attempted_at": "2026-11-19T03:00:00Z",
            }]
            if "message_id" in query:
                records.append({**records[0], "message_id": "another-order", "result": "succeeded",
                                "attempted_at": "2026-11-19T04:00:00Z"})
            self._respond(200, records)
        else:
            self._respond(404)

    def _respond(self, status: int, data: object = None) -> None:
        body = b"" if data is None else json.dumps(data).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)
        except ConnectionError:
            pass

    def log_message(self, format: str, *args: object) -> None:
        pass


def run_demo(directory: Path, *, investigation: bool = False) -> None:
    database = directory / "reports.sqlite3"
    config = directory / "target.json"
    service_log = directory / "service.jsonl"
    env = {
        **os.environ,
        "OPSPILOT_RABBITMQ_USER": "demo-reader",
        "OPSPILOT_RABBITMQ_PASSWORD": "demo-local-only",
    }

    def run_cli(*args: str) -> str:
        result = subprocess.run(
            [sys.executable, "-m", "opspilot", "--db", str(database), *args],
            cwd=directory, env=env, capture_output=True, encoding="utf-8", timeout=30,
        )
        if result.returncode:
            raise RuntimeError(f"{args[0]} failed: {result.stderr.strip()}")
        return result.stdout

    with DemoServer(("127.0.0.1", 0), DemoHandler) as server:
        base_url = f"http://127.0.0.1:{server.server_port}"
        service_url = f"{base_url}/service/health"
        consumer_url = f"{base_url}/consumer/health"
        config.write_text(json.dumps({
            "health": {
                "service_url": service_url, "dependency_urls": [consumer_url],
                "service_log_path": service_log.name,
            },
            "rabbitmq": {
                "management_url": base_url, "queue": QUEUE_NAME,
                "consumer_health_url": consumer_url, "processing_url": f"{base_url}/processing",
                **({"timeout_seconds": 0.5} if investigation else {}),
            },
        }, indent=2) + "\n", encoding="utf-8")
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            if investigation:
                _run_investigation_demo(directory, config, server, run_cli)
                return
            for recovered in (False, True):
                server.recovered = recovered
                if recovered:
                    service_log.write_text("", encoding="utf-8")
                else:
                    entry = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": "ERROR"}
                    service_log.write_text(json.dumps(entry) + "\n", encoding="utf-8")
                recheck_options = ("--check-service-log", "--check-dependencies") if recovered else ()
                run_cli("diagnose-auto", "--config", str(config), *recheck_options, "Check HTTP health")
                consumer_options = ("--check-consumer-health",) if recovered else ()
                run_cli("diagnose-auto", "--config", str(config), *consumer_options, "Check the queue backlog")
        finally:
            server.shutdown()
            thread.join()

    health = run_cli(
        "compare-http-latest", "--service-url", service_url,
        "--include-service-log", "--format", "markdown",
    )
    queue = run_cli(
        "compare-queue-latest", "--queue-source", base_url + QUEUE_PATH,
        "--include-consumer-health", "--format", "markdown",
    )
    (directory / "health-comparison.md").write_text(health, encoding="utf-8")
    (directory / "queue-comparison.md").write_text(queue, encoding="utf-8")


def _run_investigation_demo(
    directory: Path, config: Path, server: DemoServer, run_cli: Callable[..., str],
) -> None:
    args = (
        "diagnose-auto", "--workflow", "investigation", "--config", str(config),
        "--message-id", "demo-order-101", "Why is this order delayed?",
    )
    (directory / "plan.json").write_text(run_cli(*args, "--plan"), encoding="utf-8")
    for name, healthy, delay, options, expected_stop in (
        ("healthy", True, 0.0, (), "service_healthy"),
        ("fault", False, 0.0, (), "checks_complete"),
        ("partial-timeout", False, 1.5, (), "checks_complete"),
        ("step-budget", False, 0.0, ("--max-steps", "1"), "step_budget_exhausted"),
        ("time-budget", False, 1.5, ("--max-seconds", "0.25"), "time_budget_exhausted"),
    ):
        server.recovered = healthy
        server.queue_delay = delay
        output = run_cli(*args, *options)
        report = json.loads(output)
        stop = report["investigation"]["stop_reason"]
        if stop != expected_stop:
            raise RuntimeError(f"{name} stopped with {stop}, expected {expected_stop}.")
        if json.loads(run_cli("report", report["task_id"])) != report:
            raise RuntimeError(f"{name} report readback changed the saved evidence.")
        markdown = run_cli("report", report["task_id"], "--format", "markdown")
        (directory / f"{name}.json").write_text(output, encoding="utf-8")
        (directory / f"{name}.md").write_text(markdown, encoding="utf-8")
        print(f"{name}: {report['task_id']} ({stop})", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path(".opspilot/demo"))
    parser.add_argument("--investigation", action="store_true", help="Run the cross-source investigation demo.")
    args = parser.parse_args()

    root = args.output_dir.absolute()
    root.mkdir(parents=True, exist_ok=True)
    directory = Path(mkdtemp(prefix="run-", dir=root))
    print(f"Demo output directory: {directory}", flush=True)
    try:
        run_demo(directory, investigation=args.investigation)
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"Demo failed: {error}\n")
    files = (
        ("reports.sqlite3", "plan.json") if args.investigation
        else ("reports.sqlite3", "health-comparison.md", "queue-comparison.md")
    )
    for name in files:
        print(directory / name)


if __name__ == "__main__":
    main()
