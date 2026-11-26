"""Replay a single incident window before and after recovery on the Compose example."""

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep

from acceptance import COMPOSE, HERE, raw_range, run_cli, set_fault, status


def collect_logs(output: Path, start: datetime, end: datetime) -> int:
    result = subprocess.run(
        [*COMPOSE, "logs", "--no-log-prefix", "--no-color", "service"],
        capture_output=True, text=True, encoding="utf-8", check=True, timeout=20,
    )
    entries = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    expected = sum(
        entry["level"] == "ERROR" and start <= datetime.fromisoformat(entry["timestamp"]) <= end
        for entry in entries if entry["service"] == "orders"
    )
    for service, stamp in (
        ("orders-neighbor", start + timedelta(seconds=65)),
        ("orders", start - timedelta(seconds=1)),
        ("orders", end + timedelta(seconds=1)),
    ):
        entries.append({"timestamp": stamp.isoformat(), "service": service, "level": "ERROR"})
    (output / "shared.jsonl").write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8",
    )
    if not expected:
        raise RuntimeError("The service did not log requests during the selected fault window.")
    return expected


def run_phase(
    output: Path, target: Path, start: datetime, end: datetime, phase: str, expected_http: int, expected_logs: int,
) -> dict:
    options = (
        "diagnose-auto", "--config", str(target),
        "--incident-start", start.isoformat(), "--incident-end", end.isoformat(), "Review the incident window",
    )
    stored = json.loads(run_cli(output, *options))
    task_id = stored["task_id"]
    details = stored["incident"]
    evidence = stored["evidence"]
    log = evidence[1]
    if (
        details["window_start"] != start.isoformat() or details["window_end"] != end.isoformat()
        or details["service"] != "orders" or details["current_evidence_index"] != 0
        or evidence[0]["http_status"] != expected_http or details["window_status"] != "anomaly_observed"
        or details["window_evidence_indexes"] != [1, 2, 3]
    ):
        raise RuntimeError(f"Unexpected incident summary for {phase}: {details}")
    if phase == "missing-log":
        if details["unavailable_evidence_indexes"] != [1] or log["reason"] != "log_not_found":
            raise RuntimeError("The missing log was not recorded independently.")
        if details["stop_reason"] != "sources_incomplete" or details["anomaly_evidence_indexes"] != [2, 3]:
            raise RuntimeError("Available metrics were not preserved after losing the log.")
    elif (
        log["error_count"] != expected_logs or log["service"] != "orders"
        or details["anomaly_evidence_indexes"] != [1, 2, 3] or details["stop_reason"] != "checks_complete"
    ):
        raise RuntimeError("The scoped log result or anomaly attribution was wrong.")

    for metric in evidence[2:]:
        evaluation_start = datetime.fromisoformat(metric["evaluation_start"])
        if evaluation_start != start + timedelta(minutes=1) or metric["service"] != "orders":
            raise RuntimeError("The metric evaluation exceeded the incident scope.")
        raw = raw_range(metric, int(evaluation_start.timestamp()), int(end.timestamp()))
        values = [float(value) for _, value in raw["data"]["result"][0]["values"]]
        if not metric["threshold_exceeded"] or max(values) != metric["peak_value"]:
            raise RuntimeError("The reported metric anomaly disagreed with Prometheus.")
        (output / f"{phase}-{metric['query_name']}-raw.json").write_text(json.dumps(raw, indent=2), encoding="utf-8")
    report = run_cli(output, "report", task_id)
    if json.loads(report) != stored:
        raise RuntimeError("SQLite returned different incident evidence.")
    (output / f"{phase}.json").write_text(report, encoding="utf-8")
    (output / f"{phase}.md").write_text(run_cli(output, "report", task_id, "--format", "markdown"), encoding="utf-8")
    print(f"{phase}: {task_id}, HTTP {expected_http}", flush=True)
    return stored


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Directory for a new acceptance run.")
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True)
    target = output / "target.json"
    config = json.loads((HERE / "target.json").read_text(encoding="utf-8"))
    config["health"]["service_log_path"] = "shared.jsonl"
    target.write_text(json.dumps(config, indent=2), encoding="utf-8")
    try:
        set_fault(True)
        start = datetime.now(timezone.utc).replace(microsecond=0)
        end = start + timedelta(seconds=80)
        print(f"Generating fault traffic from {start.isoformat()} through {end.isoformat()}", flush=True)
        while datetime.now(timezone.utc) < end:
            if status("work") != 503:
                raise RuntimeError("The fixture did not return the intended fault response.")
            sleep(0.2)
        expected_logs = collect_logs(output, start, end)
        ongoing = run_phase(output, target, start, end, "ongoing", 503, expected_logs)
    finally:
        set_fault(False)
    recovered = run_phase(output, target, start, end, "recovered", 200, expected_logs)
    config["health"]["service_log_path"] = "missing.jsonl"
    missing_target = output / "missing-log-target.json"
    missing_target.write_text(json.dumps(config, indent=2), encoding="utf-8")
    missing_log = run_phase(output, missing_target, start, end, "missing-log", 200, expected_logs)
    if ongoing["evidence"][1]["error_count"] != recovered["evidence"][1]["error_count"]:
        raise RuntimeError("Historical log evidence changed after the endpoint recovered.")
    args = ("compare-http", ongoing["task_id"], recovered["task_id"], "--include-service-log")
    comparison = json.loads(run_cli(output, *args))
    if len(comparison["metric_comparisons"]) != 2 or len(comparison["changes"]) != 1:
        raise RuntimeError("The comparison did not pair both metrics and the HTTP transition.")
    (output / "ongoing-to-recovered.json").write_text(json.dumps(comparison, indent=2), encoding="utf-8")
    (output / "ongoing-to-recovered.md").write_text(
        run_cli(output, *args, "--format", "markdown"), encoding="utf-8",
    )
    (output / "run.json").write_text(json.dumps({
        "start": start.isoformat(), "end": end.isoformat(), "expected_error_count": expected_logs,
        "tasks": {name: report["task_id"] for name, report in (
            ("ongoing", ongoing), ("recovered", recovered), ("missing-log", missing_log),
        )},
    }, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
