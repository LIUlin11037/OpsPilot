"""Interrupt and resume a saved investigation using isolated loopback sources."""

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, SimpleQueue
from tempfile import mkdtemp
from threading import Event, Thread

from auto_diagnosis_demo import QUEUE_NAME, QUEUE_PATH, DemoHandler, DemoServer


class RecoveryServer(DemoServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), RecoveryHandler)
        self.queue_gates: SimpleQueue[tuple[Event, Event]] = SimpleQueue()
        self.requests: list[str] = []


class RecoveryHandler(DemoHandler):
    server: RecoveryServer

    def do_GET(self) -> None:
        self.server.requests.append(self.path)
        if self.path == QUEUE_PATH:
            try:
                entered, release = self.server.queue_gates.get_nowait()
            except Empty:
                pass
            else:
                entered.set()
                release.wait(timeout=40)
        super().do_GET()


@contextmanager
def _hold_queue(server: RecoveryServer) -> Iterator[tuple[Event, Event]]:
    entered, release = Event(), Event()
    server.queue_gates.put((entered, release))
    try:
        yield entered, release
    finally:
        release.set()


@contextmanager
def _running(command: list[str], env: dict[str, str]) -> Iterator[subprocess.Popen[str]]:
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def run_demo(directory: Path) -> dict[str, object]:
    database = directory / "reports.sqlite3"
    target = directory / "target.json"
    env = {**os.environ, "OPSPILOT_RABBITMQ_USER": "demo-reader", "OPSPILOT_RABBITMQ_PASSWORD": "demo-local-only"}
    command = [sys.executable, "-m", "opspilot", "--db", str(database)]

    def cli(*args: str, expected_code: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run([*command, *args], env=env, capture_output=True, encoding="utf-8", timeout=30)
        if result.returncode != expected_code:
            raise RuntimeError(f"{args[0]} returned {result.returncode}: {result.stderr.strip()}")
        return result

    with RecoveryServer() as server:
        url = f"http://127.0.0.1:{server.server_port}"
        target.write_text(json.dumps({
            "health": {"service_url": f"{url}/service/health"},
            "rabbitmq": {"management_url": url, "queue": QUEUE_NAME, "timeout_seconds": 35},
        }, indent=2), encoding="utf-8")
        runner = Thread(target=server.serve_forever, daemon=True)
        runner.start()
        try:
            with _hold_queue(server) as (entered, release), _running([
                *command, "diagnose-auto", "--workflow", "investigation", "--config", str(target),
                "--max-seconds", "45", "Why is this order delayed?",
            ], env) as original:
                assert entered.wait(timeout=20), "The first runner did not reach the queue."
                assert original.stderr is not None
                line = original.stderr.readline()
                assert line.startswith("OpsPilot investigation task: "), line
                task_id = line.split()[-1]
                interrupted = json.loads(cli("task", task_id).stdout)
                assert interrupted["status"] == "unfinished"
                assert interrupted["checkpoint"]["evidence"][0]["http_status"] == 503
                assert [attempt["outcome"] for attempt in interrupted["attempts"]] == ["observed", "started"]
                original.kill()
                original.wait(timeout=10)
                release.set()

            # Resume must use the original saved configuration.
            target.rename(directory / "original-target.json")
            with _hold_queue(server) as (entered, release), _running([*command, "resume", task_id], env) as resumed:
                assert entered.wait(timeout=20), "The resume runner did not reach the queue."
                competing = cli("resume", task_id, expected_code=1)
                assert "already has an active runner" in competing.stderr
                release.set()
                stdout, stderr = resumed.communicate(timeout=20)
                assert resumed.returncode == 0, stderr
            report = json.loads(stdout)
            assert report["task_id"] == task_id
            assert report["evidence"][0] == interrupted["checkpoint"]["evidence"][0]
            assert [item["tool"] for item in report["evidence"]] == ["inspect_service_health", "inspect_queue"]
            final = json.loads(cli("task", task_id).stdout)
            assert final["status"] == "completed"
            assert [attempt["outcome"] for attempt in final["attempts"]] == ["observed", "interrupted", "observed"]
            assert json.loads(cli("resume", task_id).stdout) == report
            assert json.loads(cli("report", task_id).stdout) == report
            assert server.requests == ["/service/health", QUEUE_PATH, QUEUE_PATH]

            server.recovered = True
            fresh = json.loads(cli("recheck", task_id).stdout)
            assert fresh["previous_task_id"] == task_id
            assert fresh["task_id"] != task_id and fresh["evidence"][0]["http_status"] == 204
            assert fresh["evidence"][0]["observed_at"] > report["evidence"][0]["observed_at"]
            assert json.loads(cli("report", task_id).stdout) == report
            assert server.requests == ["/service/health", QUEUE_PATH, QUEUE_PATH, "/service/health"]
        finally:
            server.shutdown()
            runner.join()

    acceptance = {
        "task_id": task_id, "recheck_task_id": fresh["task_id"], "requests": server.requests,
        "before_interruption": interrupted, "completed_task": final,
        "resumed_report": report, "recheck_report": fresh, "competing_resume": competing.stderr.strip(),
    }
    (directory / "acceptance.json").write_text(json.dumps(acceptance, indent=2), encoding="utf-8")
    return acceptance


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path(".opspilot/recovery-demo"))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    directory = Path(mkdtemp(prefix="run-", dir=args.output_dir.absolute()))
    acceptance = run_demo(directory)
    print(f"Resumed {acceptance['task_id']}, rechecked as {acceptance['recheck_task_id']}")
    print(directory / "acceptance.json")


if __name__ == "__main__":
    main()
