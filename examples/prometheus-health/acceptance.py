"""Exercise three phases against the dedicated Prometheus Compose example."""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
SERVICE = "http://127.0.0.1:18761"
COMPOSE = ("docker", "compose", "-p", "opspilot-v11-prometheus", "-f", str(HERE / "compose.yaml"))
# Age out the one-minute rates and all 30 seconds of the range query.
PHASE_SECONDS = 96


def set_fault(enabled: bool) -> None:
    action = "fault.touch()" if enabled else "fault.unlink(missing_ok=True)"
    subprocess.run([
        *COMPOSE, "exec", "-T", "service", "python", "-c",
        f"from pathlib import Path; fault = Path('/tmp/opspilot-fault'); {action}",
    ], check=True, timeout=20)


def status(path: str) -> int:
    try:
        with urlopen(f"{SERVICE}/{path}", timeout=3) as response:
            return response.status
    except HTTPError as error:
        with error:
            return error.code


def exercise(expected: int) -> None:
    deadline = monotonic() + PHASE_SECONDS
    while monotonic() < deadline:
        observed = status("work")
        if observed != expected:
            raise RuntimeError(f"Test traffic expected HTTP {expected}, received {observed}.")
        sleep(0.2)


def run_cli(output: Path, *arguments: str) -> str:
    fixture_env = dict(os.environ)
    fixture_env.pop("OPSPILOT_PROMETHEUS_TOKEN", None)
    return subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(output / "reports.sqlite3"), *arguments],
        env=fixture_env, capture_output=True, text=True, encoding="utf-8", check=True, timeout=25,
    ).stdout


def raw_range(read: dict, start: int, end: int) -> dict:
    params = {
        "query": read["expression"], "start": start, "end": end, "step": read["step_seconds"],
    }
    with urlopen(f"{read['source']}?{urlencode(params)}", timeout=5) as response:
        return json.load(response)


def verify_raw(output: Path, phase: str, read: dict, fault: bool) -> None:
    start = int(datetime.fromisoformat(read["window_start"]).timestamp())
    end = int(datetime.fromisoformat(read["window_end"]).timestamp())
    raw = raw_range(read, start, end)
    values = [float(value) for _, value in raw["data"]["result"][0]["values"]]
    if (
        read["outcome"] != "observed" or len(values) != read["sample_count"]
        or values[-1] != read["latest_value"] or max(values) != read["peak_value"]
        or read["threshold_exceeded"] != fault
    ):
        raise RuntimeError(f"{phase} metric differs from the raw query or the expected phase: {read}")
    (output / f"{phase}-{read['query_name']}-raw.json").write_text(json.dumps(raw, indent=2), encoding="utf-8")


def measure_recovery(output: Path) -> None:
    run = json.loads((output / "run.json").read_text(encoding="utf-8"))
    recovery = datetime.fromisoformat(run["ready_observed_at"]["recovery"])
    report = json.loads((output / "recovery.json").read_text(encoding="utf-8"))
    timing = {"ready_observed_at": recovery.isoformat(), "metrics": {}}
    for read in report["evidence"]:
        if read["tool"] != "inspect_service_metrics":
            continue
        end = int(datetime.fromisoformat(read["window_end"]).timestamp())
        raw = raw_range(read, int(recovery.timestamp()), end)
        values = raw["data"]["result"][0]["values"]
        above = [i for i, (_, value) in enumerate(values) if float(value) > read["threshold"]]
        if not above or above[-1] + 1 == len(values):
            raise RuntimeError(f"{read['query_name']} lacks a measured transition below its threshold.")
        settled = datetime.fromtimestamp(values[above[-1] + 1][0], recovery.tzinfo)
        timing["metrics"][read["query_name"]] = {
            "settled_at": settled.isoformat(), "seconds_after_ready": (settled - recovery).total_seconds(),
        }
        (output / f"recovery-{read['query_name']}-transition.json").write_text(
            json.dumps(raw, indent=2), encoding="utf-8",
        )
    (output / "recovery-timing.json").write_text(json.dumps(timing, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Directory for a new acceptance run.")
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True)
    target = output / "target.json"
    target.write_bytes((HERE / "target.json").read_bytes())
    reports = {}
    ready_observations = {}
    try:
        for phase, expected in (("baseline", 200), ("fault", 503), ("recovery", 200)):
            fault = expected == 503
            set_fault(fault)
            if status("ready") != expected:
                raise RuntimeError(f"The {phase} health endpoint did not enter the commanded state.")
            ready_observations[phase] = datetime.now().astimezone().isoformat()
            print(f"{phase} health is {expected}; collecting load and letting the query window fill.", flush=True)
            exercise(expected)
            options = () if fault else ("--check-metrics",)
            stored = json.loads(run_cli(output, "diagnose-auto", "--config", str(target), *options, f"HTTP health {phase}"))
            task_id = stored["task_id"]
            raw_report = run_cli(output, "report", task_id)
            if json.loads(raw_report) != stored or stored["evidence"][0]["http_status"] != expected:
                raise RuntimeError(f"The {phase} SQLite report does not match the live diagnosis.")
            metrics = [item for item in stored["evidence"] if item["tool"] == "inspect_service_metrics"]
            if {item["query_name"] for item in metrics} != {"latency_p95", "error_rate"}:
                raise RuntimeError(f"The {phase} report lacks the expected metric observations.")
            for metric in metrics:
                verify_raw(output, phase, metric, fault)
            reports[phase] = task_id
            (output / f"{phase}.json").write_text(raw_report, encoding="utf-8")
            markdown = run_cli(output, "report", task_id, "--format", "markdown")
            (output / f"{phase}.md").write_text(markdown, encoding="utf-8")
            print(f"{phase}: {task_id}", flush=True)
    finally:
        set_fault(False)
        if status("ready") != 200:
            raise RuntimeError("The example service failed to return to the healthy state.")

    for before, after in (("baseline", "fault"), ("fault", "recovery")):
        args = ("compare-http", reports[before], reports[after])
        comparison = run_cli(output, *args)
        if len(json.loads(comparison)["metric_comparisons"]) != 2:
            raise RuntimeError("The report comparison must pair both metric observations.")
        (output / f"{before}-{after}.json").write_text(comparison, encoding="utf-8")
        markdown = run_cli(output, *args, "--format", "markdown")
        (output / f"{before}-{after}.md").write_text(markdown, encoding="utf-8")
    (output / "run.json").write_text(
        json.dumps({"tasks": reports, "ready_observed_at": ready_observations}, indent=2), encoding="utf-8",
    )
    measure_recovery(output)


if __name__ == "__main__":
    main()
