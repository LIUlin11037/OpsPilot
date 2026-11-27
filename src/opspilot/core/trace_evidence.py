"""Normalized evidence passed from trace sources to the diagnosis workflow."""

import re
from typing import Literal, TypedDict

_TRACE_ID = re.compile(r"(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})")
TraceCoverage = Literal["linked", "incomplete", "not_found", "unavailable"]


def normalize_trace_id(value: str) -> str:
    if not _TRACE_ID.fullmatch(value) or int(value, 16) == 0:
        raise ValueError("Trace ID must contain 16 or 32 hexadecimal digits and be nonzero.")
    return value.lower().zfill(32)


class TraceSpan(TypedDict):
    trace_id: str
    span_id: str
    parent_span_id: str | None
    service: str | None
    operation: str | None
    kind: str
    start_time_unix_nano: int | None
    end_time_unix_nano: int | None
    status: Literal["ok", "error", "unset", "unknown"]


class _RequestTraceReadFields(TypedDict):
    source: str
    observed_at: str
    outcome: Literal["observed", "missing", "timeout"]
    spans: list[TraceSpan]
    gaps: list[str]


class RequestTraceRead(_RequestTraceReadFields, total=False):
    reason: str


class _TraceLookupEvidenceFields(TypedDict):
    tool: Literal["inspect_request_trace"]
    source: str
    observed_at: str
    trace_id: str
    outcome: Literal["observed", "missing", "timeout"]
    span_count: int


class TraceLookupEvidence(_TraceLookupEvidenceFields, total=False):
    reason: str


class RequestSpanEvidence(TraceSpan):
    tool: Literal["inspect_request_trace"]
    source: str
    observed_at: str
    outcome: Literal["observed"]


class RequestTraceSummary(TypedDict):
    trace_id: str
    coverage: TraceCoverage
    span_evidence_indexes: list[int]
    leaf_error_evidence_indexes: list[int]
    upstream_error_evidence_indexes: list[int]
    missing_parent_span_ids: list[str]
    gaps: list[str]
