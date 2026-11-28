"""Capture native Caddy evidence, then compare saved incidents offline."""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from map_access_logs import map_access_logs


HERE = Path(__file__).resolve().parent
COMPOSE = ("docker", "compose", "-p", "opspilot-v19-caddy", "-f", str(HERE / "compose.yaml"))
GATEWAY = "http://127.0.0.1:18771/work"
PROMETHEUS = "http://127.0.0.1:18772"
JAEGER = "http://127.0.0.1:18773"
SERVICE = "caddy-gateway"
WINDOW_SECONDS = 75
COUNTER = re.compile(r'caddy_http_request_duration_seconds_count\{code="([0-9]{3})",handler="subroute",method="GET",server="srv0"\} (\d+)')


def docker(*args):
    return subprocess.run([*COMPOSE, *args], capture_output=True, encoding="utf-8", check=True, timeout=20).stdout


def get(url):
    try:
        with urlopen(url, timeout=5) as reply:
            return reply.status, reply.read()
    except HTTPError as reply:
        with reply:
            return reply.code, reply.read()


def cli(directory, *args):
    environment = {key: value for key, value in os.environ.items() if key not in {
        "OPSPILOT_PROMETHEUS_TOKEN", "OPSPILOT_JAEGER_TOKEN",
    }}
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", "reports.sqlite3", *args], cwd=directory,
        capture_output=True, encoding="utf-8", env=environment, timeout=30,
    )
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def change_upstream(*, fault):
    config = "/etc/caddy/fault.Caddyfile" if fault else "/etc/caddy/Caddyfile"
    docker("exec", "-T", "upstream", "caddy", "reload", "--config", config, "--adapter", "caddyfile")


def counter(status):
    exposition = docker("exec", "-T", "gateway", "wget", "-qO-", "http://127.0.0.1:9180/metrics")
    values = [int(match[2]) for line in exposition.splitlines() if (match := COUNTER.fullmatch(line))
              and int(match[1]) == status]
    assert len(values) <= 1
    return values[0] if values else 0


def traffic(directory, expected):
    before = counter(expected)
    now = datetime.now(timezone.utc)
    start = now.replace(microsecond=0) + timedelta(seconds=1)
    end = start + timedelta(seconds=WINDOW_SECONDS)
    sleep((start - now).total_seconds())
    count = 0
    while datetime.now(timezone.utc) < end - timedelta(seconds=0.5):
        status, _ = get(GATEWAY)
        if status != expected:
            raise RuntimeError(f"Gateway returned HTTP {status}; expected {expected}.")
        count += 1
        sleep(0.3)
    while datetime.now(timezone.utc) < end:
        sleep(0.15)
    after = counter(expected)
    raw = docker("exec", "-T", "gateway", "cat", "/data/gateway-access.jsonl")
    raw_path = directory / "gateway-raw.jsonl"
    raw_path.write_text(raw, encoding="utf-8")
    map_access_logs(raw_path, directory / "gateway.jsonl", SERVICE)
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    scoped = [row for row in rows if start.timestamp() <= row["ts"] <= end.timestamp()]
    assert count == after - before == len(scoped), "Native counters, requests and access records disagree"
    assert all(row["status"] == expected for row in scoped)
    return start, end, scoped


def verify_trace(directory, name, candidate, followup, evidence):
    trace_id = candidate["trace_id"]
    status, raw = get(f"{JAEGER}/api/v3/traces/{trace_id}")
    assert status == 200
    (directory / f"{name}-{trace_id}-jaeger.jsonl").write_bytes(raw)
    spans = {}
    for line in raw.splitlines():
        for batch in json.loads(line)["result"]["resourceSpans"]:
            service, = [item["value"]["stringValue"] for item in batch["resource"]["attributes"]
                        if item["key"] == "service.name"]
            for scope in batch["scopeSpans"]:
                for span in scope["spans"]:
                    assert span["traceId"] == trace_id
                    spans[span["spanId"]] = (service, span.get("parentSpanId") or None, span["status"]["code"])
    lookup = evidence[followup["lookup_evidence_index"]]
    assert lookup["source"] == f"{JAEGER}/api/v3/traces/{trace_id}" and lookup["trace_id"] == trace_id
    trace = followup["trace"]
    observed = [evidence[index] for index in trace["span_evidence_indexes"]]
    assert trace["coverage"] == "linked" and len(observed) == len(spans) == 2
    for span in observed:
        assert spans[span["span_id"]] == (span["service"], span["parent_span_id"], 2)
        assert span["status"] == "error" and span["trace_id"] == trace_id
    parents = {span["service"]: span for span in observed}
    assert parents["caddy-upstream"]["parent_span_id"] == parents[SERVICE]["span_id"]
    errors = [evidence[index]["service"] for index in trace["leaf_error_evidence_indexes"]]
    assert errors == ["caddy-upstream"]


def capture_report(directory, name, window, current_status, *, fault=False, missing=False):
    start, end, rows = window
    args = ("diagnose-auto", "--config", "target.json", "--incident-start", start.isoformat(),
            "--incident-end", end.isoformat(), "--follow-traces", "Investigate Caddy gateway")
    plan = json.loads(cli(directory, *args, "--plan"))
    assert plan["scope"]["service"] == SERVICE and plan["checks"][-1]["count"] == 3
    write_json(directory / f"{name}-plan.json", plan)
    output = cli(directory, *args)
    report = json.loads(output)
    health, log, metric, *_ = report["evidence"]
    assert health["http_status"] == current_status
    assert metric["outcome"] == "observed" and metric["threshold_exceeded"] == fault
    params = urlencode({
        "query": metric["expression"], "start": int(datetime.fromisoformat(metric["evaluation_start"]).timestamp()),
        "end": int(end.timestamp()), "step": metric["step_seconds"],
    })
    status, raw = get(f"{PROMETHEUS}/api/v1/query_range?{params}")
    assert status == 200
    values = [float(value) for _, value in json.loads(raw)["data"]["result"][0]["values"]]
    assert len(values) == metric["sample_count"] and max(values) == metric["peak_value"]
    (directory / f"{name}-prometheus.json").write_bytes(raw)
    traces = report["incident_traces"]
    if missing:
        assert log["reason"] == "log_not_found" and traces["stop_reason"] == "logs_unavailable"
    else:
        assert log["error_count"] == (len(rows) if fault else 0)
        if fault:
            expected = [row["traceID"] for row in sorted(rows, key=lambda row: row["ts"], reverse=True)[:3]]
            candidates = log["trace_candidates"]["candidates"]
            assert [candidate["trace_id"] for candidate in candidates] == expected
            assert len(traces["followups"]) == 3
            for followup in traces["followups"]:
                verify_trace(directory, name, candidates[followup["candidate_index"]], followup, report["evidence"])
        else:
            assert traces["stop_reason"] == "no_valid_trace_ids" and not traces["followups"]
    task_id = report["task_id"]
    assert json.loads(cli(directory, "report", task_id)) == report
    (directory / f"{name}.json").write_text(output, encoding="utf-8")
    (directory / f"{name}.md").write_text(cli(directory, "report", task_id, "--format", "markdown"), encoding="utf-8")
    print(f"Saved {name} task {task_id}", flush=True)
    return task_id


def capture(directory):
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "target.json").write_bytes((HERE / "target.json").read_bytes())
    change_upstream(fault=True)
    try:
        fault = traffic(directory, 503)
        before = capture_report(directory, "fault", fault, 503, fault=True)
    finally:
        change_upstream(fault=False)
    recheck = capture_report(directory, "same-window", fault, 200, fault=True)
    later = traffic(directory, 200)
    healthy = capture_report(directory, "later-healthy", later, 200)
    log = directory / "gateway.jsonl"
    temporarily_hidden = log.rename(directory / "gateway-preserved.jsonl")
    try:
        missing = capture_report(directory, "missing-log", later, 200, missing=True)
    finally:
        temporarily_hidden.rename(log)
    write_json(directory / "tasks.json", {
        "fault": before, "same-window": recheck, "later-healthy": healthy, "missing-log": missing,
    })
    write_json(directory / "handoff.json", {
        "incident_start": fault[0].isoformat(), "incident_end": fault[1].isoformat(),
        "gateway": GATEWAY, "prometheus": PROMETHEUS, "jaeger": JAEGER, "service": SERVICE,
    })


def compare(directory):
    tasks = json.loads((directory / "tasks.json").read_text(encoding="utf-8"))
    before = (directory / "reports.sqlite3").read_bytes()
    results = {}
    for name, status in (("same-window", "anomaly_observed"), ("later-healthy", "no_anomaly_observed"),
                         ("missing-log", "undetermined")):
        args = "compare-incident", tasks["fault"], tasks[name]
        delta = json.loads(cli(directory, *args))
        assert delta["before"]["current_http"]["evidence"]["http_status"] == 503
        assert delta["after"]["current_http"]["evidence"]["http_status"] == 200
        assert delta["after"]["window"]["window_status"] == status
        assert delta["window_relation"] == ("same_window" if name == "same-window" else "after_window_later")
        log, metric = delta["observations"]
        assert not metric["limitations"]
        assert log["limitations"] == (["after_unavailable"] if name == "missing-log" else [])
        trace_counts = [len(delta[side]["trace_followups"]["entries"]) for side in ("before", "after")]
        assert trace_counts == [3, 3 if name == "same-window" else 0]
        results[name] = {"status": status, "trace_counts": trace_counts, "log_limitations": log["limitations"]}
        write_json(directory / f"compare-{name}.json", delta)
        markdown = cli(directory, *args, "--format", "markdown")
        assert all(task_id in markdown for task_id in args[1:])
        (directory / f"compare-{name}.md").write_text(markdown, encoding="utf-8")
    assert (directory / "reports.sqlite3").read_bytes() == before
    receipt = {"tasks": tasks, "comparisons": results}
    write_json(directory / "verification.json", receipt)
    print(json.dumps(receipt, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("capture", "compare"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    action = capture if args.phase == "capture" else compare
    action(args.directory.resolve())


if __name__ == "__main__":
    main()
