"""Compare saved incident windows without reading their original sources."""

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Literal, TypedDict, cast

from opspilot.core.diagnosis import DiagnosisStatus, Evidence, HealthEvidence, MetricEvidence, ServiceLogEvidence
from opspilot.core.incident_report import IncidentDetails
from opspilot.core.trace_evidence import FollowedRequest, TraceLogCandidate
from opspilot.task_store import StoredReport


WindowRelation = Literal["same_window", "overlap", "after_window_later", "after_window_earlier"]
WindowBounds = tuple[datetime, datetime]
ObservationKey = tuple[Literal["log", "metric"], str]
_METRIC_IDENTITY = ("source", "service", "expression", "unit", "step_seconds", "threshold")
_LOG_IDENTITY = ("source", "service")


class IndexedHealth(TypedDict):
    evidence_index: int
    evidence: HealthEvidence


class IndexedWindowEvidence(TypedDict):
    evidence_index: int
    evidence: MetricEvidence | ServiceLogEvidence


class TraceEntry(TypedDict):
    candidate: TraceLogCandidate
    followup: FollowedRequest
    lookup_reason: str | None


class TraceContext(TypedDict):
    log_evidence_index: int | None
    stop_reason: str
    entries: list[TraceEntry]


class IncidentObservation(TypedDict):
    task_id: str
    status: DiagnosisStatus
    current_http: IndexedHealth
    window: IncidentDetails
    trace_followups: TraceContext | None


class WindowEvidenceComparison(TypedDict):
    kind: Literal["log", "metric"]
    name: str
    before: IndexedWindowEvidence | None
    after: IndexedWindowEvidence | None
    limitations: list[str]


class IncidentComparison(TypedDict):
    service: str
    service_url: str
    window_relation: WindowRelation
    before: IncidentObservation
    after: IncidentObservation
    observations: list[WindowEvidenceComparison]


def _utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected a timestamp.")
    stamp = datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value)
    if stamp.utcoffset() is None:
        raise ValueError("Expected a timestamp with a time zone.")
    return stamp.astimezone(timezone.utc)


def _bounds(item: Mapping[str, object]) -> WindowBounds:
    start, end = _utc(item.get("window_start")), _utc(item.get("window_end"))
    if start >= end:
        raise ValueError("Expected a positive incident window.")
    return start, end


def _relation(before: WindowBounds, after: WindowBounds) -> WindowRelation:
    if before == after:
        return "same_window"
    if before[1] < after[0]:
        return "after_window_later"
    if after[1] < before[0]:
        return "after_window_earlier"
    return "overlap"


def _at(report: StoredReport, index: int) -> Evidence:
    if type(index) is not int or not 0 <= index < len(report["evidence"]):
        raise ValueError(f"Task {report['task_id']} has an invalid evidence index.")
    return report["evidence"][index]


def _trace_evidence(report: StoredReport, index: int, trace_id: str, *, span: bool) -> Evidence:
    item = _at(report, index)
    marker = "span_id" if span else "span_count"
    if item["tool"] != "inspect_request_trace" or item.get("trace_id") != trace_id or marker not in item:
        raise ValueError(f"Task {report['task_id']} has invalid trace evidence for {trace_id}.")
    return item


def _trace_entry(report: StoredReport, followup: FollowedRequest, candidate: TraceLogCandidate) -> TraceEntry:
    trace = followup.get("trace")
    lookup_index = followup.get("lookup_evidence_index")
    trace_id = candidate["trace_id"]
    reason = None
    if trace is not None:
        if trace["trace_id"] != trace_id or lookup_index is None:
            raise ValueError(f"Task {report['task_id']} has a trace follow-up that does not match its candidate.")
        lookup = _trace_evidence(report, lookup_index, trace_id, span=False)
        reason = lookup.get("reason")
        spans = set(trace["span_evidence_indexes"])
        errors = set(trace["leaf_error_evidence_indexes"]) | set(trace["upstream_error_evidence_indexes"])
        if not errors <= spans:
            raise ValueError(f"Task {report['task_id']} has trace error references outside its spans.")
        for index in spans:
            _trace_evidence(report, index, trace_id, span=True)
    elif lookup_index is not None or "skipped_reason" not in followup:
        raise ValueError(f"Task {report['task_id']} has an invalid skipped trace follow-up.")
    return {"candidate": candidate, "followup": followup, "lookup_reason": reason}


def _traces(report: StoredReport) -> TraceContext | None:
    details = report.get("incident_traces")
    if details is None:
        return None
    log_index = details["log_evidence_index"]
    entries: list[TraceEntry] = []
    if details["followups"]:
        log = _at(report, log_index)
        if log["tool"] != "inspect_service_log" or log_index not in report["incident"]["window_evidence_indexes"]:
            raise ValueError(f"Task {report['task_id']} has invalid trace log evidence.")
        candidates = cast(ServiceLogEvidence, log).get("trace_candidates", {}).get("candidates", [])
        for followup in details["followups"]:
            index = followup["candidate_index"]
            if type(index) is not int or not 0 <= index < len(candidates):
                raise ValueError(f"Task {report['task_id']} has an invalid trace candidate index.")
            entries.append(_trace_entry(report, followup, candidates[index]))
    return {"log_evidence_index": log_index, "stop_reason": details["stop_reason"], "entries": entries}


def _incident(report: StoredReport) -> IncidentObservation:
    summary = report.get("incident")
    task_id = report["task_id"]
    if summary is None:
        raise ValueError(f"Task {task_id} is not a historical incident investigation.")
    current_index = summary["current_evidence_index"]
    current = _at(report, current_index)
    if current["tool"] != "inspect_service_health":
        raise ValueError(f"Task {task_id} has invalid HTTP probe evidence.")
    return {
        "task_id": task_id, "status": report["status"], "window": summary,
        "current_http": {"evidence_index": current_index, "evidence": cast(HealthEvidence, current)},
        "trace_followups": _traces(report),
    }


def _observations(report: StoredReport, summary: IncidentDetails) -> dict[ObservationKey, IndexedWindowEvidence]:
    collected: dict[ObservationKey, IndexedWindowEvidence] = {}
    for index in summary["window_evidence_indexes"]:
        item = _at(report, index)
        key: ObservationKey
        if item["tool"] == "inspect_service_log":
            key = "log", "service_log"
        elif item["tool"] == "inspect_service_metrics":
            key = "metric", cast(MetricEvidence, item)["query_name"]
        else:
            raise ValueError(f"Task {report['task_id']} has a non-historical window evidence index.")
        if key in collected:
            raise ValueError(f"Task {report['task_id']} has duplicate historical observations for {key[1]}.")
        collected[key] = {"evidence_index": index, "evidence": cast(MetricEvidence | ServiceLogEvidence, item)}
    return collected


def _covers_scope(item: Mapping[str, object], service: str, bounds: WindowBounds) -> bool:
    if item.get("service") != service or item.get("unscoped_entries", 0):
        return False
    try:
        return _bounds(item) == bounds
    except (ValueError, OverflowError):
        return False


def _limitations(
    key: ObservationKey, service: str, before: IndexedWindowEvidence | None, after: IndexedWindowEvidence | None,
    before_bounds: WindowBounds, after_bounds: WindowBounds,
) -> list[str]:
    limitations = []
    fields = _LOG_IDENTITY if key[0] == "log" else _METRIC_IDENTITY
    for side, observation, bounds in (("before", before, before_bounds), ("after", after, after_bounds)):
        if observation is None:
            limitations.append(f"{side}_missing")
            continue
        evidence = observation["evidence"]
        if evidence["outcome"] != "observed":
            limitations.append(f"{side}_unavailable")
        if not _covers_scope(evidence, service, bounds) or any(evidence.get(field) is None for field in fields):
            limitations.append(f"{side}_scope_incomplete")
    if before is not None and after is not None:
        if any(before["evidence"].get(field) != after["evidence"].get(field) for field in fields):
            limitations.append("different_configuration")
        if key[0] == "log" and before_bounds[1] - before_bounds[0] != after_bounds[1] - after_bounds[0]:
            limitations.append("different_window_length")
    return limitations


def compare_incident_reports(before: StoredReport, after: StoredReport) -> IncidentComparison:
    first, second = _incident(before), _incident(after)
    service = first["window"]["service"]
    if not service or service != second["window"]["service"]:
        raise ValueError("Reports must refer to the same incident service.")
    endpoint = first["current_http"]["evidence"]["source"]
    if not endpoint or endpoint != second["current_http"]["evidence"]["source"]:
        raise ValueError("Reports must use the same HTTP probe URL.")
    windows = []
    for side in (first, second):
        try:
            windows.append(_bounds(side["window"]))
        except (ValueError, OverflowError) as error:
            raise ValueError(f"Task {side['task_id']} has an invalid incident window.") from error
    before_bounds, after_bounds = windows
    baseline = _observations(before, first["window"])
    followup = _observations(after, second["window"])
    observations: list[WindowEvidenceComparison] = []
    for key in sorted(baseline.keys() | followup.keys()):
        earlier, later = baseline.get(key), followup.get(key)
        observations.append({
            "kind": key[0], "name": key[1], "before": earlier, "after": later,
            "limitations": _limitations(key, service, earlier, later, before_bounds, after_bounds),
        })
    return {
        "service": service, "service_url": endpoint, "window_relation": _relation(before_bounds, after_bounds),
        "before": first, "after": second, "observations": observations,
    }
