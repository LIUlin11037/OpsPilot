"""Verify OpsPilot against Jaeger using two isolated HTTP services."""

import argparse
import json
import os
import subprocess
import sys
from contextlib import closing, contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from time import monotonic, sleep, time_ns
from urllib.parse import urlsplit
from uuid import uuid4


HOST = "127.0.0.1"
SCENARIOS = ("normal", "downstream-fault", "incomplete")


def exchange(url, method="GET", data=None, headers=None):
    address = urlsplit(url)
    if address.scheme != "http" or address.hostname != HOST or address.username is not None:
        raise ValueError("The sample uses HTTP endpoints on 127.0.0.1.")
    with closing(HTTPConnection(HOST, address.port, timeout=4)) as connection:
        connection.request(method, address.path, data, headers or {})
        with connection.getresponse() as reply:
            return reply.status, reply.read()


def make_span(trace_id, span_id, name, start, kind, http_status, parent=None):
    value = {
        "traceId": trace_id, "spanId": span_id, "name": name, "kind": kind,
        "startTimeUnixNano": str(start), "endTimeUnixNano": str(time_ns()),
        "status": {"code": 2 if http_status >= 500 else 0},
    }
    if parent is not None:
        value["parentSpanId"] = parent
    return value


def record(collector, service, *spans):
    body = json.dumps({"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
        "scopeSpans": [{"spans": spans}],
    }]}).encode()
    code, response = exchange(f"{collector}/v1/traces", "POST", body, {"Content-Type": "application/json"})
    if code != 200 or json.loads(response).get("partialSuccess", {}).get("rejectedSpans"):
        raise RuntimeError(f"Jaeger rejected the isolated sample spans with HTTP {code}.")


class SampleHandler(BaseHTTPRequestHandler):
    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def orders_service(collector):
    class Orders(SampleHandler):
        def do_GET(self):
            start = time_ns()
            header = self.headers.get("traceparent", "").split("-")
            if len(header) != 4 or self.path[1:] not in SCENARIOS:
                self.reply(400, {"error": "unexpected sample request"})
                return
            _, trace_id, client_span_id, _ = header
            span_id = uuid4().hex[:16]
            status = 503 if self.path == "/downstream-fault" else 200
            span = make_span(trace_id, span_id, "GET /orders", start, 2, status, client_span_id)
            record(collector, "orders", span)
            self.reply(status, {"span_id": span_id})

    return Orders


def gateway_service(collector, orders_url):
    class Gateway(SampleHandler):
        def do_GET(self):
            start = time_ns()
            if self.path[1:] not in SCENARIOS:
                self.reply(404, {"error": "unknown sample"})
                return
            trace_id = uuid4().hex
            ingress_id, client_id = uuid4().hex[:16], uuid4().hex[:16]
            client_start = time_ns()
            status, body = exchange(f"{orders_url}{self.path}", headers={"traceparent": f"00-{trace_id}-{client_id}-01"})
            downstream_id = json.loads(body)["span_id"]
            client = make_span(trace_id, client_id, "Call orders", client_start, 3, status, ingress_id)
            gateway_status = 502 if status >= 500 else 200
            ingress = make_span(trace_id, ingress_id, "GET /checkout", start, 2, gateway_status)
            if self.path == "/incomplete":
                record(collector, "gateway", ingress)
            else:
                record(collector, "gateway", ingress, client)
            self.reply(gateway_status, {
                "trace_id": trace_id, "gateway_span_id": ingress_id, "client_span_id": client_id,
                "orders_span_id": downstream_id, "orders_http_status": status,
            })

    return Gateway


@contextmanager
def serve(handler):
    with ThreadingHTTPServer((HOST, 0), handler) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
        thread.start()
        try:
            yield f"http://{HOST}:{server.server_port}"
        finally:
            server.shutdown()
            thread.join()


def observed_spans(body):
    spans = {}
    for line in body.splitlines():
        for resource in json.loads(line)["result"]["resourceSpans"]:
            service, = [a["value"]["stringValue"] for a in resource["resource"]["attributes"] if a["key"] == "service.name"]
            for scope in resource["scopeSpans"]:
                for span in scope["spans"]:
                    spans[span["spanId"]] = {
                        "trace_id": span["traceId"], "parent_span_id": span.get("parentSpanId") or None,
                        "service": service, "operation": span["name"], "kind": {2: "server", 3: "client"}[span["kind"]],
                        "start_time_unix_nano": int(span["startTimeUnixNano"]),
                        "end_time_unix_nano": int(span["endTimeUnixNano"]),
                        "status": {0: "unset", 2: "error"}[span.get("status", {}).get("code", 0)],
                    }
    return spans


def wait_for_trace(query, trace_id, expected_ids):
    deadline = monotonic() + 12
    endpoint = f"{query}/api/v3/traces/{trace_id}"
    while monotonic() < deadline:
        code, raw = exchange(endpoint)
        if code == 200:
            spans = observed_spans(raw)
            if set(spans) == expected_ids:
                return raw, spans
        elif code != 404:
            raise RuntimeError(f"Jaeger trace lookup returned HTTP {code}.")
        sleep(0.15)
    raise RuntimeError(f"The expected spans were not available for {trace_id}.")


def cli(directory, *args):
    environment = {key: value for key, value in os.environ.items() if key != "OPSPILOT_JAEGER_TOKEN"}
    environment["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", "reports.sqlite3", *args], cwd=directory, env=environment,
        capture_output=True, timeout=20, encoding="utf-8",
    )
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def verify_case(scenario, gateway, query, directory):
    code, body = exchange(f"{gateway}/{scenario}")
    info = json.loads(body)
    trace_id = info["trace_id"]
    client_id = info["client_span_id"]
    expected_ids = {info["gateway_span_id"], info["orders_span_id"]}
    assert (code, info["orders_http_status"]) == ((502, 503) if scenario == "downstream-fault" else (200, 200))
    if scenario != "incomplete":
        expected_ids.add(client_id)
    raw, spans = wait_for_trace(query, trace_id, expected_ids)
    assert spans[info["orders_span_id"]]["parent_span_id"] == client_id
    assert {span["service"] for span in spans.values()} == {"gateway", "orders"}
    (directory / f"{scenario}-raw.jsonl").write_bytes(raw)

    result = cli(directory, "diagnose-auto", "--config", "target.json", "--trace-id", trace_id, f"Inspect {scenario}")
    report = json.loads(result)
    assert json.loads(cli(directory, "report", report["task_id"])) == report
    view = report["request_trace"]
    assert view["trace_id"] == trace_id
    assert set(report["evidence"][i]["span_id"] for i in view["span_evidence_indexes"]) == expected_ids
    for index in view["span_evidence_indexes"]:
        evidence = report["evidence"][index]
        assert all(evidence[key] == value for key, value in spans[evidence["span_id"]].items())

    downstream = [report["evidence"][i]["span_id"] for i in view["leaf_error_evidence_indexes"]]
    assert downstream == ([info["orders_span_id"]] if scenario == "downstream-fault" else [])
    if scenario == "downstream-fault":
        assert {report["evidence"][i]["span_id"] for i in view["upstream_error_evidence_indexes"]} == {
            info["gateway_span_id"], client_id,
        }
    assert view["coverage"] == ("incomplete" if scenario == "incomplete" else "linked")
    assert view["missing_parent_span_ids"] == ([client_id] if scenario == "incomplete" else [])
    assert report["status"] == ("no_failure_observed" if scenario == "normal" else "undetermined")

    markdown = cli(directory, "report", report["task_id"], "--format", "markdown")
    assert all(marker in markdown for marker in (trace_id, "## Request trace", "| Index | Check |", *expected_ids))
    (directory / f"{scenario}-report.json").write_text(result, encoding="utf-8")
    (directory / f"{scenario}-report.md").write_text(markdown, encoding="utf-8")
    return {"scenario": scenario, "http_status": code, "orders_http_status": info["orders_http_status"],
            "trace_id": trace_id, "task_id": report["task_id"], "span_count": len(spans), "coverage": view["coverage"],
            "leaf_error_evidence_indexes": view["leaf_error_evidence_indexes"],
            "upstream_error_evidence_indexes": view["upstream_error_evidence_indexes"], "gaps": view["gaps"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-url", required=True)
    parser.add_argument("--otlp-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    directory.mkdir(parents=True)
    query, collector = args.query_url.rstrip("/"), args.otlp_url.rstrip("/")
    (directory / "target.json").write_text(json.dumps({"jaeger": {"base_url": query}}), encoding="utf-8")
    with serve(orders_service(collector)) as orders, serve(gateway_service(collector, orders)) as gateway:
        results = [verify_case(scenario, gateway, query, directory) for scenario in SCENARIOS]
    result = json.dumps(results, indent=2) + "\n"
    (directory / "verification.json").write_text(result, encoding="utf-8")
    print(result, end="")


if __name__ == "__main__":
    main()
