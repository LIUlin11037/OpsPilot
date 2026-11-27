import json
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

import pytest


A, B, C, D = (letter * 32 for letter in "abcd")
QUERY = ("diagnose-auto", "--config", "target.json", "--incident-start", "2024-01-01T00:00:00Z",
         "--incident-end", "2024-01-01T00:01:30Z")


def run(tmp_path, *args):
    return subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", "reports.sqlite3", *args], cwd=tmp_path,
        capture_output=True, encoding="utf-8", timeout=25,
    )


def output(result):
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def respond(path):
    if path == "/ready":
        return 200, b""
    if path.startswith("/api/v1/query_range?"):
        end = int(parse_qs(urlsplit(path).query)["end"][0])
        return 200, json.dumps({"status": "success", "data": {
            "resultType": "matrix", "result": [{"values": [[end, "0.5"]]}],
        }}).encode()
    if path == f"/api/v3/traces/{B}":
        return 404, b'{"error":{"httpCode":404,"message":"No traces found"}}'
    if path == f"/api/v3/traces/{A}":
        resource_spans = []
        for service, span_id, parent, start in (
            ("orders", "1" * 16, "", "1704067280000000000"),
            ("inventory", "2" * 16, "1" * 16, "1704067280100000000"),
        ):
            resource_spans.append({
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
                "scopeSpans": [{"spans": [{
                    "traceId": A, "spanId": span_id, "parentSpanId": parent, "name": f"GET /{service}", "kind": 2,
                    "startTimeUnixNano": start, "endTimeUnixNano": "1704067280200000000", "status": {"code": 2},
                }]}],
            })
        return 200, json.dumps({"result": {"resourceSpans": resource_spans}}).encode()
    return 400, b"Unexpected fixture request"


def write_target(directory, base, *, jaeger=True):
    config = {"health": {"service_url": f"{base}/ready", "service_log_path": "service.jsonl"}, "prometheus": {
        "base_url": base, "service": "orders", "step_seconds": 5,
        "queries": {"error_rate": {"metric": "http_requests_total", "unit": "ratio", "threshold": 0.1}},
    }}
    if jaeger:
        config["jaeger"] = {"base_url": base, "timeout_seconds": 13}
    (directory / "target.json").write_text(json.dumps(config), encoding="utf-8")


def write_rows(directory):
    rows = [
        ("00:01:01", "orders", A.upper()), ("00:01:22", "orders", B), ("00:01:24", "orders", A),
        ("00:01:27", "neighbor", C), ("00:01:31", "orders", D),
    ]
    (directory / "service.jsonl").write_text("\n".join(json.dumps({
        "timestamp": f"2024-01-01T{stamp}Z", "service": service, "level": "ERROR", "trace_id": trace_id,
    }) for stamp, service, trace_id in rows), encoding="utf-8")


def test_opt_in_preview_and_saved_report_correlate_only_scoped_requests(tmp_path, processing_service, monkeypatch):
    monkeypatch.delenv("OPSPILOT_JAEGER_TOKEN", raising=False)
    monkeypatch.delenv("OPSPILOT_PROMETHEUS_TOKEN", raising=False)
    with processing_service(respond) as (endpoint, requests):
        write_target(tmp_path, endpoint.removesuffix("/processing"))
        plan = output(run(tmp_path, *QUERY, "--follow-traces", "--plan", "Check the window"))
        assert not requests and not (tmp_path / "reports.sqlite3").exists()
        check = plan["checks"][-1]
        assert check["when"] == "scoped_error_trace_id" and check["count"] == 3 and check["method"] == "GET"
        assert check["total_timeout_seconds"] == 10 and check["timeout_seconds"] == 10
        assert plan["scope"]["service"] == "orders"

        write_rows(tmp_path)
        legacy = output(run(tmp_path, *QUERY, "Check the window"))
        assert all("/api/v3/traces/" not in path for path in requests)
        assert "incident_traces" not in legacy and "trace_candidates" not in legacy["evidence"][1]
        requests.clear()

        report = output(run(tmp_path, *QUERY, "--follow-traces", "Check the window"))
        assert [path for path in requests if "/api/v3/traces/" in path] == [f"/api/v3/traces/{A}", f"/api/v3/traces/{B}"]
        assert report["incident"] == legacy["incident"] and report["status"] == legacy["status"]
        details = report["incident_traces"]
        assert details["log_evidence_index"] == 1
        first, missing = details["followups"]
        assert first["lookup_evidence_index"] == 3 and first["trace"]["leaf_error_evidence_indexes"] == [5]
        assert missing["lookup_evidence_index"] == 6 and missing["trace"]["coverage"] == "not_found"
        assert report["evidence"][6]["reason"] == "trace_not_found"
        assert [item["line_number"] for item in report["evidence"][1]["trace_candidates"]["candidates"]] == [3, 2]

    assert output(run(tmp_path, "report", report["task_id"])) == report
    markdown = run(tmp_path, "report", report["task_id"], "--format", "markdown")
    assert markdown.returncode == 0, markdown.stderr
    assert "## Request trace follow-ups" in markdown.stdout and A in markdown.stdout and B in markdown.stdout
    assert "| 3 | 2 |" in markdown.stdout


@pytest.mark.parametrize("args, message", [
    (("diagnose-auto", "--follow-traces"), "requires an incident window"),
    (QUERY + ("--follow-traces",), "requires a jaeger section"),
    (("diagnose-auto", "--trace-id", A, "--follow-traces"), "cannot be combined"),
])
def test_followup_configuration_requires_exact_opt_in_scope(tmp_path, args, message):
    write_target(tmp_path, "http://127.0.0.1:12345", jaeger=False)
    result = run(tmp_path, *args, "Check the window")
    assert result.returncode == 2 and message in result.stderr
    assert not (tmp_path / "reports.sqlite3").exists()
