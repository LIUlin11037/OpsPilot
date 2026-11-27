import json
import subprocess
import sys

import pytest

from opspilot.task_store import save_report


TRACE_ID = "abcdef0123456789abcdef0123456789"


def cli(tmp_path, *args):
    return subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", "reports.sqlite3", *args],
        cwd=tmp_path, capture_output=True, encoding="utf-8", timeout=20,
    )


def output(result):
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_request_trace_preview_is_offline_and_saved_report_has_evidence(tmp_path, processing_service, monkeypatch):
    monkeypatch.setenv("OPSPILOT_JAEGER_TOKEN", "local-test-token")
    resource_spans = []
    for service, span_id, parent in (
        ("gateway", "1111222233334444", ""),
        ("orders", "aaaabbbbccccdddd", "1111222233334444"),
    ):
        resource_spans.append({
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
            "scopeSpans": [{"spans": [{
                "traceId": TRACE_ID, "spanId": span_id, "parentSpanId": parent, "kind": 2,
                "name": f"GET /{service}", "startTimeUnixNano": "1795764380279320000",
                "endTimeUnixNano": "1795764380280320000", "status": {"code": 2},
            }]}],
        })
    body = json.dumps({"result": {"resourceSpans": resource_spans}}).encode()
    with processing_service(lambda _: (200, body)) as (endpoint, requests):
        config = tmp_path / "target.json"
        config.write_text(json.dumps({"jaeger": {"base_url": endpoint.removesuffix("/processing")}}), encoding="utf-8")
        args = ("diagnose-auto", "--config", "target.json", "--trace-id", TRACE_ID.upper())

        plan = output(cli(tmp_path, *args, "--plan", "Inspect this request"))
        assert not requests and not (tmp_path / "reports.sqlite3").exists()
        assert plan["scope"]["trace_id"] == TRACE_ID
        check, = plan["checks"]
        assert check["method"] == "GET" and check["count"] == 1
        assert check["timeout_seconds"] == 5 and check["max_spans"] == 1024 and check["max_response_bytes"] == 4 * 1024 * 1024
        assert "local-test-token" not in json.dumps(plan)

        report = output(cli(tmp_path, *args, "Inspect this request"))
        assert requests == [f"/api/v3/traces/{TRACE_ID}"]
        assert json.dumps(report).find("local-test-token") == -1
        assert report["request_trace"]["leaf_error_evidence_indexes"] == [2]
        assert report["request_trace"]["upstream_error_evidence_indexes"] == [1]

    assert output(cli(tmp_path, "report", report["task_id"])) == report
    markdown = cli(tmp_path, "report", report["task_id"], "--format", "markdown")
    assert markdown.returncode == 0, markdown.stderr
    assert "## Request trace" in markdown.stdout and TRACE_ID in markdown.stdout
    assert "| Index | Check |" in markdown.stdout and "aaaabbbbccccdddd" in markdown.stdout
    assert "GET /orders" in markdown.stdout and "| 2 |" in markdown.stdout


@pytest.mark.parametrize("flags, fragment", [
    (("--order-id", "a"), "not allowed with argument --trace-id"),
    (("--incident-start", "2024-01-01T00:00:00Z"), "cannot be combined"),
    (("--check-metrics",), "cannot be combined"),
])
def test_trace_id_rejects_conflicting_diagnosis_scope(tmp_path, flags, fragment):
    result = cli(tmp_path, "diagnose-auto", "--trace-id", TRACE_ID, *flags, "Check the request")
    assert result.returncode == 2 and fragment in result.stderr
    assert not (tmp_path / "reports.sqlite3").exists()


def test_prior_format_report_still_renders_without_a_request_trace(tmp_path):
    stored = save_report(tmp_path / "reports.sqlite3", {
        "question": "Earlier health check", "conclusion": "No observed error.", "status": "no_failure_observed",
        "processing_correlation": None, "evidence": [],
    })
    result = cli(tmp_path, "report", stored["task_id"], "--format", "markdown")
    assert result.returncode == 0
    assert "Earlier health check" in result.stdout and "## Request trace" not in result.stdout
