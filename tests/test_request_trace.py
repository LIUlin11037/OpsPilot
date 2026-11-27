import pytest

from opspilot.core.request_trace import run_request_trace
from opspilot.core.trace_evidence import normalize_trace_id


TRACE_ID = "abcdef0123456789abcdef0123456789"
OTHER_TRACE_ID = "0123456789abcdef0123456789abcdef"
SOURCE = "http://127.0.0.1:16686/api/v3/traces/" + TRACE_ID
OBSERVED_AT = "2026-11-27T04:00:00+00:00"


def span(identifier, *, parent=None, status="ok", service="gateway", trace_id=TRACE_ID):
    return {
        "trace_id": trace_id,
        "span_id": f"{identifier:016x}",
        "parent_span_id": None if parent is None else f"{parent:016x}",
        "service": service,
        "operation": f"call {identifier}",
        "kind": "server",
        "start_time_unix_nano": 1000 + identifier * 10,
        "end_time_unix_nano": 2000 + identifier * 10,
        "status": status,
    }


def diagnose(*spans, outcome="observed", reason=None, gaps=()):
    read = {"source": SOURCE, "observed_at": OBSERVED_AT, "outcome": outcome,
            "spans": list(spans), "gaps": list(gaps)}
    if reason is not None:
        read["reason"] = reason
    calls = []

    def lookup(trace_id):
        calls.append(trace_id)
        return read

    report = run_request_trace("Inspect this request", TRACE_ID.upper(), lookup)
    assert calls == [TRACE_ID]
    return report


@pytest.mark.parametrize("text, expected", [
    (TRACE_ID.upper(), TRACE_ID),
    ("0123456789ABCDEF", "00000000000000000123456789abcdef"),
])
def test_trace_id_accepts_canonical_and_legacy_hex(text, expected):
    assert normalize_trace_id(text) == expected


@pytest.mark.parametrize("text", ["0" * 16, "0" * 32, "Z" * 32, "abc", TRACE_ID + "/", " " + TRACE_ID])
def test_trace_id_rejects_invalid_or_zero_ids(text):
    with pytest.raises(ValueError, match="Trace ID"):
        normalize_trace_id(text)


def test_linked_trace_records_the_lookup_and_ordered_span_evidence():
    report = diagnose(span(2, parent=1, service="orders", status="unset"), span(1))
    summary = report["request_trace"]
    assert report["status"] == "no_failure_observed"
    assert summary["coverage"] == "linked"
    assert summary["span_evidence_indexes"] == [1, 2]
    assert report["evidence"][0]["outcome"] == "observed"
    assert report["evidence"][2]["parent_span_id"] == report["evidence"][1]["span_id"]
    assert report["evidence"][2]["status"] == "unset"
    assert all(item["source"] == SOURCE and item["observed_at"] == OBSERVED_AT for item in report["evidence"])
    assert len(report["tool_calls"]) == 1


def test_downstream_and_parallel_errors_keep_independent_evidence():
    report = diagnose(
        span(1, status="error"), span(2, parent=1, status="error", service="orders"),
        span(3, parent=1, status="error", service="payments"),
    )
    summary = report["request_trace"]
    assert report["status"] == "undetermined"
    assert summary["leaf_error_evidence_indexes"] == [2, 3]
    assert summary["upstream_error_evidence_indexes"] == [1]
    assert "Multiple" in report["conclusion"]


def test_successful_middle_span_does_not_support_error_propagation():
    report = diagnose(span(1, status="error"), span(2, parent=1), span(3, parent=2, status="error"))
    assert report["request_trace"]["leaf_error_evidence_indexes"] == [1, 3]
    assert report["request_trace"]["upstream_error_evidence_indexes"] == []


def test_ignores_foreign_spans_and_keeps_unlinked_matching_observations():
    report = diagnose(span(2, parent=1, status="error"), span(1, trace_id=OTHER_TRACE_ID))
    summary = report["request_trace"]
    assert summary["coverage"] == "incomplete"
    assert summary["gaps"] == ["foreign_trace_id", "missing_parent"]
    assert summary["missing_parent_span_ids"] == [f"{1:016x}"]
    assert summary["span_evidence_indexes"] == [1]
    assert summary["leaf_error_evidence_indexes"] == [1]
    assert report["evidence"][1]["trace_id"] == TRACE_ID


def test_cycle_and_conflicting_ids_do_not_create_an_upstream_explanation():
    report = diagnose(
        span(1, parent=2, status="error"), span(2, parent=1, status="error"),
        span(3), span(3, status="error"), span(4),
    )
    summary = report["request_trace"]
    assert summary["coverage"] == "incomplete"
    assert summary["upstream_error_evidence_indexes"] == []
    assert summary["leaf_error_evidence_indexes"] == [1, 2]
    assert "cyclic_parent" in summary["gaps"]
    assert "conflicting_span_id" in summary["gaps"]
    assert f"{3:016x}" not in [item.get("span_id") for item in report["evidence"]]


@pytest.mark.parametrize("outcome, reason, coverage", [
    ("observed", "trace_not_found", "not_found"),
    ("missing", "access_denied", "unavailable"),
    ("timeout", "request_timeout", "unavailable"),
])
def test_no_trace_and_failed_reads_are_distinct(outcome, reason, coverage):
    report = diagnose(outcome=outcome, reason=reason)
    assert report["status"] == "undetermined"
    assert report["request_trace"]["coverage"] == coverage
    assert report["evidence"][0]["outcome"] == outcome
    assert report["evidence"][0]["reason"] == reason


def test_partial_response_retains_observed_spans_and_can_never_claim_linked():
    report = diagnose(span(1, status="error"), outcome="timeout", reason="request_timeout")
    assert report["request_trace"]["coverage"] == "incomplete"
    assert report["request_trace"]["leaf_error_evidence_indexes"] == [1]
    assert report["evidence"][0]["outcome"] == "timeout"
    assert report["evidence"][1]["outcome"] == "observed"
