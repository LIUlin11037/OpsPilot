"""Follow a bounded number of request IDs supported by incident log evidence."""

from collections.abc import Callable
from time import monotonic
from typing import cast

from opspilot.core.diagnosis import DiagnosisReport, ServiceLogEvidence, measure_tool_call
from opspilot.core.request_trace import build_request_trace_report
from opspilot.core.trace_evidence import (
    MAX_INCIDENT_TRACE_LOOKUPS, IncidentTraceDetails, RequestTraceRead, RequestTraceSummary,
)

TRACE_FOLLOWUP_BUDGET_SECONDS = 10


def _shift_trace(summary: RequestTraceSummary, offset: int) -> RequestTraceSummary:
    return {
        **summary,
        "span_evidence_indexes": [offset + index for index in summary["span_evidence_indexes"]],
        "leaf_error_evidence_indexes": [offset + index for index in summary["leaf_error_evidence_indexes"]],
        "upstream_error_evidence_indexes": [offset + index for index in summary["upstream_error_evidence_indexes"]],
    }


def attach_incident_traces(
    report: DiagnosisReport, lookup: Callable[[str, float], RequestTraceRead],
) -> DiagnosisReport:
    evidence = report["evidence"]
    log_index = next((
        index for index in report["incident"]["window_evidence_indexes"]
        if evidence[index]["tool"] == "inspect_service_log"
    ), None)
    details: IncidentTraceDetails = {
        "log_evidence_index": log_index, "max_lookups": MAX_INCIDENT_TRACE_LOOKUPS,
        "budget_seconds": TRACE_FOLLOWUP_BUDGET_SECONDS, "stop_reason": "logs_unavailable", "followups": [],
    }
    report["incident_traces"] = details
    if log_index is None:
        return report
    log = cast(ServiceLogEvidence, evidence[log_index])
    if log["outcome"] != "observed":
        return report
    selection = log.get("trace_candidates")
    if not selection or not selection["candidates"]:
        details["stop_reason"] = "no_valid_trace_ids"
        return report

    details["stop_reason"] = "lookups_complete"
    deadline = monotonic() + TRACE_FOLLOWUP_BUDGET_SECONDS
    for candidate_index, candidate in enumerate(selection["candidates"][:MAX_INCIDENT_TRACE_LOOKUPS]):
        remaining = deadline - monotonic()
        if remaining <= 0:
            details["stop_reason"] = "budget_exhausted"
            details["followups"].append({"candidate_index": candidate_index, "skipped_reason": "budget_exhausted"})
            continue
        trace_id = candidate["trace_id"]
        read, elapsed = measure_tool_call(lambda: lookup(trace_id, remaining))
        request = build_request_trace_report(report["question"], trace_id, read, elapsed)
        offset = len(evidence)
        evidence.extend(request["evidence"])
        report.setdefault("tool_calls", []).extend(request["tool_calls"])
        report.setdefault("next_checks", []).extend(
            {**check, "evidence_index": offset + check["evidence_index"]} for check in request["next_checks"]
        )
        details["followups"].append({
            "candidate_index": candidate_index, "lookup_evidence_index": offset,
            "trace": _shift_trace(request["request_trace"], offset),
        })
    return report
