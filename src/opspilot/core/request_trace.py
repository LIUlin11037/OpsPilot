"""Summarize a single request using recorded spans and their parent links."""

from collections.abc import Callable
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import DiagnosisReport, Evidence, ToolCall, measure_tool_call, suggest_next_checks
from opspilot.core.trace_evidence import (
    RequestSpanEvidence,
    RequestTraceRead,
    RequestTraceSummary,
    TraceCoverage,
    TraceLookupEvidence,
    TraceSpan,
    normalize_trace_id,
)


class _RequestTraceState(TypedDict, total=False):
    read: RequestTraceRead
    duration_ms: float
    report: DiagnosisReport


def _matching_spans(read: RequestTraceRead, trace_id: str, gaps: set[str]) -> list[TraceSpan]:
    by_id: dict[str, TraceSpan] = {}
    conflicts: set[str] = set()
    for span in read["spans"]:
        if span["trace_id"] != trace_id:
            gaps.add("foreign_trace_id")
            continue
        span_id = span["span_id"]
        if span_id in by_id and by_id[span_id] != span:
            conflicts.add(span_id)
        by_id[span_id] = span
    if conflicts:
        gaps.add("conflicting_span_id")
        for span_id in conflicts:
            del by_id[span_id]
    return sorted(by_id.values(), key=lambda item: (
        item["start_time_unix_nano"] is None, item["start_time_unix_nano"] or 0, item["span_id"],
    ))


def _cyclic_spans(parents: dict[str, str | None]) -> set[str]:
    checked: set[str] = set()
    loops: set[str] = set()
    for span_id in parents:
        cursor: str | None = span_id
        path: dict[str, int] = {}
        while cursor is not None and cursor in parents and cursor not in checked and cursor not in path:
            path[cursor] = len(path)
            cursor = parents[cursor]
        if cursor is not None and cursor in path:
            loops.update(list(path)[path[cursor]:])
        checked.update(path)
    return loops


def _summarize(trace_id: str, read: RequestTraceRead, spans: list[TraceSpan], gaps: set[str]) -> RequestTraceSummary:
    parents = {span["span_id"]: span["parent_span_id"] for span in spans}
    errors = {span["span_id"] for span in spans if span["status"] == "error"}
    missing = {parent for parent in parents.values() if parent is not None and parent not in parents}
    cyclic = _cyclic_spans(parents)
    if missing:
        gaps.add("missing_parent")
    if cyclic:
        gaps.add("cyclic_parent")
    if sum(parent is None for parent in parents.values()) > 1:
        gaps.add("multiple_roots")
    if any(
        span["service"] is None or span["operation"] is None or span["status"] == "unknown"
        or span["start_time_unix_nano"] is None or span["end_time_unix_nano"] is None
        for span in spans
    ):
        gaps.add("span_metadata_incomplete")
    if read["outcome"] != "observed":
        gaps.add(read.get("reason", "read_unavailable"))

    # Report adjacent error links as possible propagation. A successful or unknown intermediate span
    # provides no such support, and cycle edges cannot establish upstream or downstream order.
    upstream = {parent for span_id, parent in parents.items() if (
        parent is not None and parent in errors and span_id in errors
        and span_id not in cyclic and parent not in cyclic
    )}
    indexes = {span["span_id"]: index for index, span in enumerate(spans, start=1)}
    coverage: TraceCoverage
    if spans:
        coverage = "incomplete" if gaps else "linked"
    else:
        coverage = "unavailable" if gaps else "not_found"
    return {
        "trace_id": trace_id, "coverage": coverage, "span_evidence_indexes": list(indexes.values()),
        "leaf_error_evidence_indexes": sorted(indexes[span_id] for span_id in errors - upstream),
        "upstream_error_evidence_indexes": sorted(indexes[span_id] for span_id in upstream),
        "missing_parent_span_ids": sorted(missing), "gaps": sorted(gaps),
    }


def _conclusion(summary: RequestTraceSummary) -> str:
    coverage = summary["coverage"]
    errors = summary["leaf_error_evidence_indexes"]
    upstream = summary["upstream_error_evidence_indexes"]
    if coverage == "not_found":
        return "The lookup returned no matching spans. The request status is unknown."
    if coverage == "unavailable":
        return "Matching spans could not be established. The request status is unknown."
    details = []
    if len(errors) > 1:
        details.append(f"Multiple recorded errors have no confirmed downstream error link: evidence {errors}.")
    elif errors:
        details.append(f"The trace records an error at evidence {errors[0]}.")
    if upstream:
        details.append(f"Errors at evidence {upstream} lie on a downstream error path and may reflect propagation.")
    if coverage == "incomplete":
        details.append("The recorded trace has evidence gaps.")
    if errors or coverage == "incomplete":
        details.append("The root cause is undetermined.")
    else:
        details.append("No failure was observed in the recorded spans.")
    return " ".join(details)


def build_request_trace_report(
    question: str, trace_id: str, read: RequestTraceRead, duration_ms: float,
) -> DiagnosisReport:
    trace_id = normalize_trace_id(trace_id)
    gaps = set(read["gaps"])
    spans = _matching_spans(read, trace_id, gaps)
    summary = _summarize(trace_id, read, spans, gaps)
    lookup: TraceLookupEvidence = {
        "tool": "inspect_request_trace", "outcome": read["outcome"], "source": read["source"],
        "observed_at": read["observed_at"], "trace_id": trace_id, "span_count": len(spans),
    }
    if "reason" in read:
        lookup["reason"] = read["reason"]
    evidence: list[Evidence] = [lookup]
    for span in spans:
        item: RequestSpanEvidence = {
            **span, "tool": "inspect_request_trace", "outcome": "observed",
            "source": read["source"], "observed_at": read["observed_at"],
        }
        evidence.append(item)
    call: ToolCall = {
        "tool": "inspect_request_trace", "outcome": read["outcome"], "source": read["source"], "duration_ms": duration_ms,
    }
    healthy = summary["coverage"] == "linked" and not summary["leaf_error_evidence_indexes"]
    return {
        "question": question, "status": "no_failure_observed" if healthy else "undetermined",
        "conclusion": _conclusion(summary), "processing_correlation": None, "request_trace": summary,
        "evidence": evidence, "tool_calls": [call], "next_checks": suggest_next_checks(evidence),
    }


def run_request_trace(
    question: str, trace_id: str, source: Callable[[str], RequestTraceRead],
) -> DiagnosisReport:
    trace_id = normalize_trace_id(trace_id)

    def inspect(_state: _RequestTraceState) -> _RequestTraceState:
        read, duration = measure_tool_call(lambda: source(trace_id))
        return {"read": read, "duration_ms": duration}

    def report(state: _RequestTraceState) -> _RequestTraceState:
        return {"report": build_request_trace_report(question, trace_id, state["read"], state["duration_ms"])}

    graph = StateGraph(_RequestTraceState)
    graph.add_node("inspect_request_trace", inspect)
    graph.add_node("report", report)
    graph.add_edge(START, "inspect_request_trace")
    graph.add_edge("inspect_request_trace", "report")
    graph.add_edge("report", END)
    return graph.compile().invoke({})["report"]
