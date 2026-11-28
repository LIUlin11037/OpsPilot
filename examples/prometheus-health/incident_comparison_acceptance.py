"""Capture four incident reports and compare them after stopping the sample services."""

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep

from acceptance import HERE, raw_range, run_cli, status


COMPOSE = ("docker", "compose", "-p", "opspilot-v18-window", "-f", str(HERE / "compose.yaml"))
WINDOW_SECONDS = 75


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def set_fault(enabled):
    change = "fault.touch()" if enabled else "fault.unlink(missing_ok=True)"
    subprocess.run([
        *COMPOSE, "exec", "-T", "service", "python", "-c",
        f"from pathlib import Path; fault = Path('/tmp/opspilot-fault'); {change}",
    ], check=True, timeout=20)


def capture_window(directory, expected):
    start = datetime.now(timezone.utc).replace(microsecond=0)
    end = start + timedelta(seconds=WINDOW_SECONDS)
    while datetime.now(timezone.utc) < end:
        observed = status("work")
        if observed != expected:
            raise RuntimeError(f"Expected fixture HTTP {expected}, got {observed}.")
        sleep(0.2)
    result = subprocess.run(
        [*COMPOSE, "logs", "--no-log-prefix", "--no-color", "service"], capture_output=True,
        check=True, timeout=20, encoding="utf-8",
    )
    (directory / "orders.jsonl").write_text(result.stdout, encoding="utf-8")
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    errors = sum(
        row["level"] == "ERROR" and row["service"] == "orders"
        and start <= datetime.fromisoformat(row["timestamp"]) <= end for row in rows
    )
    assert bool(errors) == (expected == 503)
    return start, end, errors


def capture_report(directory, name, window, http_status, *, missing_log=False):
    start, end, errors = window
    output = run_cli(directory, "diagnose-auto", "--config", str(directory / "target.json"),
                     "--incident-start", start.isoformat(), "--incident-end", end.isoformat(), "Check orders")
    report = json.loads(output)
    observations = report["evidence"]
    assert observations[0]["http_status"] == http_status
    log = observations[1]
    if missing_log:
        assert log["outcome"] == "missing" and log["reason"] == "log_not_found"
    else:
        assert log["outcome"] == "observed" and log["error_count"] == errors
    for metric in observations[2:]:
        evaluation_start = int(datetime.fromisoformat(metric["evaluation_start"]).timestamp())
        raw = raw_range(metric, evaluation_start, int(end.timestamp()))
        values = [float(value) for _, value in raw["data"]["result"][0]["values"]]
        assert len(values) == metric["sample_count"] and max(values) == metric["peak_value"]
        assert metric["threshold_exceeded"] == bool(errors)
        write_json(directory / f"{name}-{metric['query_name']}-raw.json", raw)
    task_id = report["task_id"]
    assert json.loads(run_cli(directory, "report", task_id)) == report
    (directory / f"{name}.json").write_text(output, encoding="utf-8")
    (directory / f"{name}.md").write_text(run_cli(directory, "report", task_id, "--format", "markdown"), encoding="utf-8")
    print(f"Saved {name} task {task_id}", flush=True)
    return task_id


def capture(directory):
    directory.mkdir(parents=True, exist_ok=False)
    config = json.loads((HERE / "target.json").read_text(encoding="utf-8"))
    config["health"]["service_log_path"] = "orders.jsonl"
    write_json(directory / "target.json", config)
    set_fault(True)
    try:
        fault = capture_window(directory, 503)
        ongoing_id = capture_report(directory, "fault", fault, 503)
    finally:
        set_fault(False)
    recheck_id = capture_report(directory, "same-window-recheck", fault, 200)
    later = capture_window(directory, 200)
    healthy_id = capture_report(directory, "later-healthy", later, 200)
    log = directory / "orders.jsonl"
    archived = log.rename(directory / "orders-held-aside.jsonl")
    try:
        missing_id = capture_report(directory, "missing-log", later, 200, missing_log=True)
    finally:
        archived.rename(log)
    write_json(directory / "tasks.json", {
        "fault": ongoing_id, "same-window-recheck": recheck_id, "later-healthy": healthy_id, "missing-log": missing_id,
    })


def compare(directory):
    tasks = json.loads((directory / "tasks.json").read_text(encoding="utf-8"))
    database = directory / "reports.sqlite3"
    original = database.read_bytes()
    comparisons = {}
    for name in ("same-window-recheck", "later-healthy", "missing-log"):
        args = "compare-incident", tasks["fault"], tasks[name]
        result = json.loads(run_cli(directory, *args))
        assert (result["before"]["task_id"], result["after"]["task_id"]) == args[1:]
        assert result["before"]["current_http"]["evidence"]["http_status"] == 503
        assert result["after"]["current_http"]["evidence"]["http_status"] == 200
        assert result["before"]["window"]["window_status"] == "anomaly_observed"
        expected = {"same-window-recheck": "anomaly_observed", "later-healthy": "no_anomaly_observed",
                    "missing-log": "undetermined"}[name]
        assert result["after"]["window"]["window_status"] == expected
        relation = "same_window" if name == "same-window-recheck" else "after_window_later"
        assert result["window_relation"] == relation
        log, *metrics = result["observations"]
        assert all(not pair["limitations"] for pair in metrics) and len(metrics) == 2
        if name == "missing-log":
            assert log["limitations"] == ["after_unavailable"] and log["after"]["evidence"]["reason"] == "log_not_found"
            assert result["after"]["window"]["unavailable_evidence_indexes"] == [log["after"]["evidence_index"]]
        else:
            assert not log["limitations"]
            observed = log["after"]["evidence"]["error_count"]
            assert observed == (log["before"]["evidence"]["error_count"] if name == "same-window-recheck" else 0)
        markdown = run_cli(directory, *args, "--format", "markdown")
        assert all(task_id in markdown for task_id in args[1:])
        write_json(directory / f"fault-to-{name}.json", result)
        (directory / f"fault-to-{name}.md").write_text(markdown, encoding="utf-8")
        comparisons[name] = {"window_relation": relation, "window_status": expected, "log_limitations": log["limitations"]}
    assert database.read_bytes() == original
    summary = {"tasks": tasks, "comparisons": comparisons}
    write_json(directory / "verification.json", summary)
    print(json.dumps(summary, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("capture", "compare"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    phase = capture if args.phase == "capture" else compare
    phase(args.directory.resolve())


if __name__ == "__main__":
    main()
