"""Compare observations from saved diagnosis reports."""

from typing import Literal, TypedDict

from opspilot.core.diagnosis import Evidence, ObservationOutcome, TargetTraceSummary
from opspilot.task_store import StoredReport

ComparisonValue = str | int | None


class TraceScope(TypedDict):
    environment: str
    vhost: str
    entity_id: int
    message_id: str


class TraceChange(TypedDict):
    field: str
    before: ComparisonValue
    after: ComparisonValue


class TraceComparison(TypedDict):
    before_task_id: str
    after_task_id: str
    target: TraceScope
    changes: list[TraceChange]


HealthRole = Literal["service", "dependency"]


class EndpointObservation(TypedDict):
    health: str
    outcome: ObservationOutcome
    http_status: int | None
    reason: str | None


class EndpointChange(TypedDict):
    role: HealthRole
    source: str
    occurrence: int
    before: EndpointObservation | None
    after: EndpointObservation | None


class HealthComparison(TypedDict):
    before_task_id: str
    after_task_id: str
    service_url: str
    changes: list[EndpointChange]


def _scope(trace: TargetTraceSummary) -> TraceScope:
    environment, vhost, message_id = trace["environment"], trace["vhost"], trace["message_id"]
    if not environment or not vhost or not message_id:
        raise ValueError("Both reports must resolve a message ID in a named environment and vhost.")
    return {
        "environment": environment,
        "vhost": vhost,
        "entity_id": trace["entity_id"],
        "message_id": message_id,
    }


def _values(trace: TargetTraceSummary, evidence: list[Evidence]) -> dict[str, ComparisonValue]:
    values: dict[str, ComparisonValue] = {
        f"branches.{branch}": status for branch, status in trace["branches"].items()
    }
    queue = next((check for check in evidence if check.get("checkpoint") == "queue"), None)
    if queue is None:
        return values
    outcome = queue["outcome"]
    values["queue.outcome"] = outcome
    if outcome == "observed":
        values["queue.pending_messages"] = queue.get("pending_messages")
    partial_response = (
        outcome == "missing"
        and queue.get("http_status") == 200
        and queue.get("reason") in ("metric_missing", "invalid_response")
    )
    if outcome == "observed" or partial_response:
        values["queue.unacknowledged_messages"] = queue.get("unacknowledged_messages")
        values["queue.registered_consumers"] = queue.get("registered_consumers")
    return values


def compare_trace_reports(before: StoredReport, after: StoredReport) -> TraceComparison:
    before_trace = before.get("target_trace")
    after_trace = after.get("target_trace")
    if before_trace is None or after_trace is None:
        raise ValueError("Both task IDs must refer to HireHub trace reports.")
    scope = _scope(before_trace)
    if scope != _scope(after_trace):
        raise ValueError("Reports must match on environment, vhost, delivery ID, and message ID.")

    before_values = _values(before_trace, before["evidence"])
    after_values = _values(after_trace, after["evidence"])
    changes: list[TraceChange] = []
    for field in sorted(before_values.keys() | after_values.keys()):
        previous = before_values.get(field)
        current = after_values.get(field)
        if previous != current:
            changes.append({"field": field, "before": previous, "after": current})
    return {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "target": scope,
        "changes": changes,
    }


def _health_observations(
    report: StoredReport,
) -> tuple[str, dict[tuple[HealthRole, str, int], EndpointObservation]]:
    observations: dict[tuple[HealthRole, str, int], EndpointObservation] = {}
    counts: dict[tuple[HealthRole, str], int] = {}
    services: list[str] = []
    for evidence in report["evidence"]:
        tool = evidence["tool"]
        if tool not in ("inspect_service_health", "inspect_dependency_health"):
            continue
        source = evidence["source"]
        role: HealthRole = "service" if tool == "inspect_service_health" else "dependency"
        if role == "service":
            services.append(source)
        key = (role, source)
        occurrence = counts.get(key, 0) + 1
        counts[key] = occurrence
        observations[(role, source, occurrence)] = {
            "health": evidence["health"],
            "outcome": evidence["outcome"],
            "http_status": evidence.get("http_status"),
            "reason": evidence.get("reason"),
        }
    if len(services) != 1:
        raise ValueError("Both task IDs must refer to HTTP health reports with one service endpoint.")
    return services[0], observations


def compare_health_reports(before: StoredReport, after: StoredReport) -> HealthComparison:
    service, before_values = _health_observations(before)
    after_service, after_values = _health_observations(after)
    if service != after_service:
        raise ValueError("Reports must match on the service endpoint URL.")

    changes: list[EndpointChange] = []
    for role, source, occurrence in before_values | after_values:
        key = (role, source, occurrence)
        previous = before_values.get(key)
        current = after_values.get(key)
        if current != previous:
            changes.append({
                "role": role, "source": source, "occurrence": occurrence,
                "before": previous, "after": current,
            })
    return {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "service_url": service,
        "changes": changes,
    }
