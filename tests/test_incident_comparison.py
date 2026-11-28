from copy import deepcopy

import pytest

from opspilot.incident_comparison import compare_incident_reports


TRACE_ID = "a" * 32
START, END = "2026-01-01T00:00:00Z", "2026-01-01T00:01:30Z"
OBSERVED = "2026-01-01T00:04:00Z"


def incident_report(task_id, *, start=START, end=END, healthy=False):
    log = {
        "tool": "inspect_service_log", "source": "file:///logs/orders.jsonl", "service": "orders",
        "observed_at": OBSERVED, "outcome": "observed", "window_start": start, "window_end": end,
        "error_count": 3, "unscoped_entries": 0,
    }
    metric = {
        "tool": "inspect_service_metrics", "source": "http://127.0.0.1:9090/api/v1/query_range", "service": "orders",
        "query_name": "error_rate", "expression": 'rate(errors{job="orders"}[1m])', "unit": "ratio", "threshold": 0.1,
        "step_seconds": 5, "window_start": start, "window_end": end, "observed_at": OBSERVED,
        "outcome": "observed", "threshold_exceeded": True, "sample_count": 7, "peak_value": 0.8, "latest_value": 0.5,
    }
    current = {
        "tool": "inspect_service_health", "source": "http://127.0.0.1:8080/ready", "observed_at": OBSERVED,
        "outcome": "observed", "health": "healthy" if healthy else "unhealthy", "http_status": 200 if healthy else 503,
    }
    return {
        "task_id": task_id, "question": "Investigate orders", "status": "undetermined",
        "conclusion": "Historical anomaly observed.", "processing_correlation": None,
        "evidence": [current, log, metric],
        "incident": {
            "service": "orders", "window_start": start, "window_end": end, "window_status": "anomaly_observed",
            "current_health": current["health"], "current_evidence_index": 0, "window_evidence_indexes": [1, 2],
            "anomaly_evidence_indexes": [1, 2], "unavailable_evidence_indexes": [], "stop_reason": "checks_complete",
        },
    }


@pytest.mark.parametrize("start,end,relation", [
    ("2026-01-01T08:00:00+08:00", "2026-01-01T08:01:30+08:00", "same_window"),
    ("2026-01-01T00:01:30Z", "2026-01-01T00:03:00Z", "overlap"),
    ("2026-01-01T00:01:31Z", "2026-01-01T00:03:01Z", "after_window_later"),
    ("2025-12-31T23:58:29Z", "2025-12-31T23:59:59Z", "after_window_earlier"),
])
def test_reports_preserve_live_state_and_observe_window_relation(start, end, relation):
    before, after = incident_report("before"), incident_report("after", start=start, end=end, healthy=True)
    result = compare_incident_reports(before, after)
    assert result["window_relation"] == relation
    assert result["before"]["current_http"]["evidence"]["http_status"] == 503
    assert result["after"]["current_http"]["evidence"]["http_status"] == 200
    assert result["before"]["window"]["anomaly_evidence_indexes"] == [1, 2]
    assert result["after"]["window"]["window_status"] == "anomaly_observed"
    assert all(not pair["limitations"] for pair in result["observations"])


@pytest.mark.parametrize("field,value,diagnostic", [
    ("service", "neighbor", "same incident service"),
    ("current_evidence_index", -1, "after"),
    ("current_evidence_index", 1, "after"),
    ("window_evidence_indexes", [1, 1, 2], "after"),
    ("window_start", "2026-01-01T00:00:00", "after"),
])
def test_rejects_other_scopes_or_invalid_references(field, value, diagnostic):
    after = incident_report("after")
    after["incident"][field] = value
    with pytest.raises(ValueError, match=diagnostic):
        compare_incident_reports(incident_report("before"), after)


def test_rejects_non_incident_and_different_probe():
    before, after = incident_report("before"), incident_report("after")
    after["evidence"][0]["source"] += "/other"
    with pytest.raises(ValueError, match="same HTTP probe"):
        compare_incident_reports(before, after)
    del after["incident"]
    with pytest.raises(ValueError, match="after.*historical"):
        compare_incident_reports(before, after)


def test_keeps_evidence_when_comparison_is_limited():
    before, after = incident_report("before"), incident_report("after", end="2026-01-01T00:02:00Z")
    after["evidence"][1]["unscoped_entries"] = 3
    after["evidence"][2].update(outcome="timeout", reason="query_timeout", threshold=0.2)
    after["evidence"].append({**after["evidence"][2], "query_name": "latency_p95"})
    after["incident"]["window_evidence_indexes"].append(3)
    log, error_rate, latency = compare_incident_reports(before, after)["observations"]
    assert log["limitations"] == ["after_scope_incomplete", "different_window_length"]
    assert log["before"]["evidence"]["error_count"] == 3
    assert error_rate["limitations"] == ["after_unavailable", "different_configuration"]
    assert error_rate["after"]["evidence"]["reason"] == "query_timeout"
    assert latency["before"] is None and "before_missing" in latency["limitations"]


def test_different_log_source_and_mismatched_window_keep_saved_values_visible():
    before, after = incident_report("before"), incident_report("after")
    after["evidence"][1].update(source="file:///logs/alternate.jsonl", outcome="missing", reason="log_not_found")
    after["evidence"][2]["window_end"] = "2026-01-01T00:01:29Z"
    log, metric = compare_incident_reports(before, after)["observations"]
    assert log["limitations"] == ["after_unavailable", "different_configuration"]
    assert metric["limitations"] == ["after_scope_incomplete"]


def test_trace_context_uses_only_its_own_candidates_and_evidence():
    before, after = incident_report("before"), incident_report("after")
    before["evidence"][1]["trace_candidates"] = {
        "candidates": [{"trace_id": TRACE_ID, "timestamp": START, "line_number": 7, "level": "ERROR"}],
        "missing_ids": 0, "invalid_ids": 0, "duplicate_rows": 0, "over_limit": 0,
    }
    before["evidence"].append({
        "tool": "inspect_request_trace", "source": "http://127.0.0.1:16686/api/v3/traces/" + TRACE_ID,
        "observed_at": OBSERVED, "trace_id": TRACE_ID, "outcome": "observed", "span_count": 0, "reason": "trace_not_found",
    })
    before["incident_traces"] = {
        "log_evidence_index": 1, "max_lookups": 3, "budget_seconds": 10, "stop_reason": "lookups_complete",
        "followups": [{"candidate_index": 0, "lookup_evidence_index": 3, "trace": {
            "trace_id": TRACE_ID, "coverage": "not_found", "span_evidence_indexes": [], "leaf_error_evidence_indexes": [],
            "upstream_error_evidence_indexes": [], "missing_parent_span_ids": [], "gaps": [],
        }}],
    }
    original = deepcopy(before)
    result = compare_incident_reports(before, after)
    assert result["after"]["trace_followups"] is None
    entry, = result["before"]["trace_followups"]["entries"]
    assert entry["candidate"]["line_number"] == 7 and entry["candidate"]["trace_id"] == TRACE_ID
    assert entry["lookup_reason"] == "trace_not_found" and entry["followup"]["lookup_evidence_index"] == 3
    assert before == original
