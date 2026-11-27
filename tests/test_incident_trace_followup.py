from threading import Event

import pytest

from opspilot.connectors import jaeger
from opspilot.connectors.jaeger import JaegerConnector
from opspilot.core import incident_traces
from opspilot.core.incident_report import summarize_incident
from opspilot.core.incident_traces import attach_incident_traces
from opspilot.core.incident_window import parse_incident_window
from opspilot.jaeger_config import JaegerConfig


WINDOW = parse_incident_window("2024-01-01T00:00:00Z", "2024-01-01T00:01:30Z")
A, B, C = "a" * 32, "b" * 32, "c" * 32
OBSERVED = "2024-01-01T00:03:00+00:00"


def incident(ids=(), log_available=True):
    log = {
        "tool": "inspect_service_log", "source": "file:///service.jsonl", "observed_at": OBSERVED,
        "outcome": "observed" if log_available else "missing", "error_count": 5,
    }
    if log_available:
        log["trace_candidates"] = {
            "candidates": [{"trace_id": trace_id, "line_number": i, "timestamp": OBSERVED, "level": "ERROR"}
                           for i, trace_id in enumerate(ids, start=1)],
            "missing_ids": 0, "invalid_ids": 0, "duplicate_rows": 0, "over_limit": 0,
        }
    return summarize_incident({
        "question": "Check this window", "status": "no_failure_observed", "conclusion": "", "processing_correlation": None,
        "evidence": [
            {"tool": "inspect_service_health", "health": "healthy", "source": "http://localhost/ready",
             "observed_at": OBSERVED, "outcome": "observed"},
            log,
            {"tool": "inspect_service_metrics", "source": "http://localhost/metrics", "observed_at": OBSERVED,
             "outcome": "observed", "threshold_exceeded": False},
        ],
    }, WINDOW, "orders")


def read(trace_id, *, outcome="observed", reason=None, spans=True, parent=None):
    result = {
        "source": f"http://localhost/api/v3/traces/{trace_id}", "observed_at": OBSERVED,
        "outcome": outcome, "gaps": [], "spans": [{
            "trace_id": trace_id, "span_id": "1" * 16, "parent_span_id": parent, "service": "orders",
            "operation": "GET /orders", "kind": "server", "status": "error", "start_time_unix_nano": 1,
            "end_time_unix_nano": 2,
        }] if spans else [],
    }
    if reason is not None:
        result["reason"] = reason
    return result


@pytest.mark.parametrize("available, stop_reason", [(True, "no_valid_trace_ids"), (False, "logs_unavailable")])
def test_no_candidate_and_log_failure_have_distinct_reasons(available, stop_reason):
    report = incident(log_available=available)
    evidence = report["evidence"].copy()

    def unexpected(*args):
        pytest.fail("A log with no verified candidates must not trigger a read")

    result = attach_incident_traces(report, unexpected)
    assert result["incident_traces"]["stop_reason"] == stop_reason
    assert result["incident_traces"]["followups"] == []
    assert result["incident_traces"]["log_evidence_index"] == 1
    assert result["evidence"] == evidence


def test_keeps_original_status_and_remaps_each_trace_and_action_index(monkeypatch):
    monkeypatch.setattr(incident_traces, "monotonic", lambda: 0.0)
    report = incident((A, B, C))
    original = {key: report[key] for key in ("incident", "status", "conclusion")}
    prefix = report["evidence"].copy()
    responses = {
        A: read(A), B: read(B, outcome="missing", reason="access_denied", spans=False),
        C: read(C, parent="2" * 16),
    }
    queries = []

    def lookup(trace_id, remaining):
        queries.append((trace_id, remaining))
        return responses[trace_id]

    result = attach_incident_traces(report, lookup)
    assert queries == [(A, 10), (B, 10), (C, 10)]
    assert {key: result[key] for key in original} == original
    assert result["evidence"][:3] == prefix
    summary = result["incident_traces"]
    assert summary["stop_reason"] == "lookups_complete" and summary["log_evidence_index"] == 1
    first, denied, partial = summary["followups"]
    assert [item["candidate_index"] for item in summary["followups"]] == [0, 1, 2]
    assert [item["lookup_evidence_index"] for item in summary["followups"]] == [3, 5, 6]
    assert first["trace"]["leaf_error_evidence_indexes"] == [4]
    assert denied["trace"]["coverage"] == "unavailable" and "access_denied" in denied["trace"]["gaps"]
    assert partial["trace"]["coverage"] == "incomplete" and partial["trace"]["span_evidence_indexes"] == [7]
    assert result["evidence"][4]["trace_id"] == A and result["evidence"][7]["trace_id"] == C
    assert any(item["evidence_index"] == 5 for item in result["next_checks"])
    assert len(result["tool_calls"]) == 3


def test_shared_budget_skips_later_candidates_and_preserves_earlier_results(monkeypatch):
    clock = iter((0, 0, 4, 11))
    monkeypatch.setattr(incident_traces, "monotonic", lambda: next(clock))
    queries = []

    def lookup(trace_id, remaining):
        queries.append((trace_id, remaining))
        return read(trace_id) if trace_id == A else read(trace_id, outcome="timeout", reason="request_timeout", spans=False)

    report = attach_incident_traces(incident((A, B, C)), lookup)
    assert queries == [(A, 10), (B, 6)]
    summary = report["incident_traces"]
    assert summary["stop_reason"] == "budget_exhausted"
    assert summary["followups"][2] == {"candidate_index": 2, "skipped_reason": "budget_exhausted"}
    assert summary["followups"][0]["trace"]["span_evidence_indexes"] == [4]
    assert report["evidence"][5]["outcome"] == "timeout"
    assert len(report["tool_calls"]) == 2


def test_stalled_jaeger_connection_cannot_delay_report_or_send_a_late_get(monkeypatch):
    entered = Event()
    release = Event()
    closed = Event()
    get_sent = Event()

    class StalledConnection:
        sock = None

        def __init__(self, *args, **kwargs):
            pass

        def connect(self):
            entered.set()
            release.wait(timeout=2)

        def request(self, *args, **kwargs):
            get_sent.set()
            raise OSError("Unexpected request after an expired connection deadline")

        def close(self):
            closed.set()

    monkeypatch.setattr(jaeger, "HTTPConnection", StalledConnection)
    monkeypatch.setattr(incident_traces, "TRACE_FOLLOWUP_BUDGET_SECONDS", 0.15)

    def lookup(trace_id, remaining):
        return JaegerConnector(JaegerConfig("http://jaeger.example", remaining)).inspect_trace(trace_id)

    try:
        report = attach_incident_traces(incident((A, B)), lookup)
        assert entered.is_set() and not closed.is_set(), "Report waited for stalled connection establishment"
        assert report["evidence"][3]["reason"] == "request_timeout"
        assert report["incident_traces"]["followups"][1] == {"candidate_index": 1, "skipped_reason": "budget_exhausted"}
    finally:
        release.set()
        assert closed.wait(timeout=2)
    assert not get_sent.is_set()
