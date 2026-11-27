"""Correlate an isolated HTTP failure with scoped logs and a real Jaeger query."""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from time import time
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from acceptance import SampleHandler, cli, exchange, gateway_service, orders_service, serve, wait_for_trace


def timestamp(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace("+00:00", "Z")


def observation_service(query, directory, lookups):
    class Observations(SampleHandler):
        def do_GET(self):
            path = urlsplit(self.path)
            if path.path == "/ready":
                self.reply(200, {"status": "ready"})
            elif path.path == "/api/v1/query_range":
                end = int(parse_qs(path.query)["end"][0])
                self.reply(200, {"status": "success", "data": {
                    "resultType": "matrix", "result": [{"values": [[end, "0.5"]]}],
                }})
            elif path.path.startswith("/api/v3/traces/"):
                status, body = exchange(f"{query}{self.path}")
                lookups.append({"method": self.command, "path": self.path, "http_status": status})
                (directory / f"lookup-{len(lookups)}-raw.jsonl").write_bytes(body)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.reply(404, {"error": "unexpected sample endpoint"})

    return Observations


def write_target(directory, observations):
    config = {
        "health": {"service_url": f"{observations}/ready", "service_log_path": "service.jsonl"},
        "prometheus": {
            "base_url": observations, "service": "gateway", "step_seconds": 5,
            "queries": {"error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.1}},
        },
        "jaeger": {"base_url": observations},
    }
    (directory / "target.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


def write_window_logs(directory, end, matched, missing):
    rows = [
        (end - 15, "gateway", "ERROR", matched.upper()),
        (end - 5, "gateway", "ERROR", missing),
        (end, "gateway", "ERROR", matched),
        (end, "neighbor", "ERROR", uuid4().hex),
        (end + 1, "gateway", "ERROR", uuid4().hex),
        (end, "gateway", "INFO", uuid4().hex),
    ]
    body = "".join(json.dumps({
        "timestamp": timestamp(seconds), "service": service, "level": level, "trace_id": trace_id,
    }) + "\n" for seconds, service, level, trace_id in rows)
    (directory / "service.jsonl").write_text(body, encoding="utf-8")


def verify(directory, gateway, query, observations, lookups):
    status, raw = exchange(f"{gateway}/downstream-fault")
    end = int(time())
    start = end - 90
    request = json.loads(raw)
    assert (status, request["orders_http_status"]) == (502, 503)
    matched, missing = request["trace_id"], uuid4().hex
    expected_spans = {request[key] for key in ("gateway_span_id", "client_span_id", "orders_span_id")}
    _, spans = wait_for_trace(query, matched, expected_spans)
    write_target(directory, observations)
    write_window_logs(directory, end, matched, missing)
    args = ("diagnose-auto", "--config", "target.json", "--incident-start", timestamp(start),
            "--incident-end", timestamp(end), "--follow-traces", "Check the gateway incident")
    plan = cli(directory, *args, "--plan")
    check = json.loads(plan)["checks"][-1]
    assert check["count"] == 3 and check["total_timeout_seconds"] == 10
    assert not lookups and not (directory / "reports.sqlite3").exists()

    output = cli(directory, *args)
    report = json.loads(output)
    expected_queries = [f"/api/v3/traces/{trace_id}" for trace_id in (matched, missing)]
    assert [(item["method"], item["path"]) for item in lookups] == [("GET", path) for path in expected_queries]
    assert lookups[0]["http_status"] == 200
    details = report["incident_traces"]
    first, second = details["followups"]
    evidence = report["evidence"]
    selection = evidence[details["log_evidence_index"]]["trace_candidates"]
    assert [item["trace_id"] for item in selection["candidates"]] == [matched, missing]
    assert [item["line_number"] for item in selection["candidates"]] == [3, 2] and selection["duplicate_rows"] == 1
    linked = first["trace"]
    assert linked["coverage"] == "linked" and second["trace"]["coverage"] == "not_found"
    assert evidence[second["lookup_evidence_index"]]["reason"] == "trace_not_found"
    assert {evidence[index]["span_id"] for index in linked["span_evidence_indexes"]} == expected_spans
    for index in linked["span_evidence_indexes"]:
        span = evidence[index]
        assert all(span[key] == value for key, value in spans[span["span_id"]].items())
    assert [evidence[index]["span_id"] for index in linked["leaf_error_evidence_indexes"]] == [request["orders_span_id"]]

    task_id = report["task_id"]
    assert json.loads(cli(directory, "report", task_id)) == report
    markdown = cli(directory, "report", task_id, "--format", "markdown")
    assert all(marker in markdown for marker in ("## Request trace follow-ups", matched, missing, *expected_spans))
    (directory / "plan.json").write_text(plan, encoding="utf-8")
    (directory / "report.json").write_text(output, encoding="utf-8")
    (directory / "report.md").write_text(markdown, encoding="utf-8")
    (directory / "observed-jaeger-gets.json").write_text(json.dumps(lookups, indent=2) + "\n", encoding="utf-8")
    return {
        "task_id": task_id, "matched_trace_id": matched, "missing_trace_id": missing,
        "gateway_http_status": status, "orders_http_status": request["orders_http_status"],
        "linked_spans": len(spans), "orders_error_evidence_indexes": linked["leaf_error_evidence_indexes"],
        "missing_lookup_evidence_index": second["lookup_evidence_index"], "jaeger_gets": lookups,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-url", required=True)
    parser.add_argument("--otlp-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    query, collector = args.query_url.rstrip("/"), args.otlp_url.rstrip("/")
    os.environ.pop("OPSPILOT_PROMETHEUS_TOKEN", None)
    lookups = []
    with serve(orders_service(collector)) as orders, serve(gateway_service(collector, orders)) as gateway, \
            serve(observation_service(query, directory, lookups)) as observations:
        result = verify(directory, gateway, query, observations, lookups)
    output = json.dumps(result, indent=2) + "\n"
    (directory / "verification.json").write_text(output, encoding="utf-8")
    print(output, end="")


if __name__ == "__main__":
    main()
