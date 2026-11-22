"""Compare observations from saved diagnosis reports."""

from typing import Literal, TypedDict

from opspilot.adapters.voting_snapshot import is_voting_trace
from opspilot.core.diagnosis import DiagnosisStatus, Evidence, ObservationOutcome, TargetTraceSummary
from opspilot.task_store import StoredReport

ComparisonValue = str | int | None


class TraceScope(TypedDict):
    environment: str
    vhost: str
    entity_id: int | str
    message_id: str


class FieldChange(TypedDict):
    field: str
    before: ComparisonValue
    after: ComparisonValue


class TraceComparison(TypedDict):
    before_task_id: str
    after_task_id: str
    target: TraceScope
    changes: list[FieldChange]


class VotingScope(TypedDict):
    environment: str
    voter_id: str


class VotingComparison(TypedDict):
    before_task_id: str
    after_task_id: str
    target: VotingScope
    changes: list[FieldChange]


HealthRole = Literal["service", "dependency", "consumer"]
_HTTP_HEALTH_ROLES: dict[str, HealthRole] = {
    "inspect_service_health": "service",
    "inspect_dependency_health": "dependency",
}
_CONSUMER_HEALTH_ROLES: dict[str, HealthRole] = {"inspect_consumer_endpoint": "consumer"}


class EndpointObservationFields(TypedDict):
    health: str
    outcome: ObservationOutcome
    http_status: int | None
    reason: str | None


class EndpointObservation(EndpointObservationFields, total=False):
    mysql_error_code: int


EndpointObservations = dict[tuple[HealthRole, str, int], EndpointObservation]


class EndpointChange(TypedDict):
    role: HealthRole
    source: str
    occurrence: int
    before: EndpointObservation | None
    after: EndpointObservation | None


class EndpointIssue(TypedDict):
    role: HealthRole
    source: str
    occurrence: int
    observation: EndpointObservation


class QueueComparisonFields(TypedDict):
    before_task_id: str
    after_task_id: str
    queue_source: str
    changes: list[FieldChange]


class QueueComparison(QueueComparisonFields, total=False):
    consumer_health_changes: list[EndpointChange]


class ServiceLogObservation(TypedDict):
    outcome: ObservationOutcome
    error_count: int | None
    reason: str | None
    window_start: str
    window_end: str


class ServiceLogChange(TypedDict):
    source: str
    before: ServiceLogObservation | None
    after: ServiceLogObservation | None


class HealthComparisonFields(TypedDict):
    before_task_id: str
    after_task_id: str
    before_status: DiagnosisStatus
    after_status: DiagnosisStatus
    service_url: str
    changes: list[EndpointChange]


class HealthComparison(HealthComparisonFields, total=False):
    unchanged_endpoint_issues: list[EndpointIssue]
    service_log_changes: list[ServiceLogChange]


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


def _queue_values(queue: Evidence) -> dict[str, ComparisonValue]:
    outcome = queue["outcome"]
    values: dict[str, ComparisonValue] = {"queue.outcome": outcome}
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


def _trace_values(trace: TargetTraceSummary, evidence: list[Evidence]) -> dict[str, ComparisonValue]:
    values: dict[str, ComparisonValue] = {
        f"branches.{branch}": status for branch, status in trace["branches"].items()
    }
    queue = next((check for check in evidence if check.get("checkpoint") == "queue"), None)
    if queue is not None:
        values.update(_queue_values(queue))
    return values


def _changed_fields(
    before: dict[str, ComparisonValue], after: dict[str, ComparisonValue]
) -> list[FieldChange]:
    changes: list[FieldChange] = []
    for field in sorted(before.keys() | after.keys()):
        previous = before.get(field)
        current = after.get(field)
        if previous != current:
            changes.append({"field": field, "before": previous, "after": current})
    return changes


def compare_trace_reports(before: StoredReport, after: StoredReport) -> TraceComparison:
    before_trace = before.get("target_trace")
    after_trace = after.get("target_trace")
    if before_trace is None or after_trace is None:
        raise ValueError("Both task IDs must refer to HireHub trace reports.")
    scope = _scope(before_trace)
    if scope != _scope(after_trace):
        raise ValueError("Reports must match on environment, vhost, delivery ID, and message ID.")

    return {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "target": scope,
        "changes": _changed_fields(
            _trace_values(before_trace, before["evidence"]),
            _trace_values(after_trace, after["evidence"]),
        ),
    }


def _voting_scope(trace: TargetTraceSummary) -> VotingScope:
    environment = trace["environment"]
    if not environment or not is_voting_trace(trace):
        raise ValueError("Both task IDs must refer to voting reports with evidence scoped to a named environment.")
    return {"environment": environment, "voter_id": str(trace["entity_id"])}


def _voting_values(trace: TargetTraceSummary, evidence: list[Evidence]) -> dict[str, ComparisonValue]:
    values = _trace_values(trace, evidence)
    for item in evidence:
        if item["tool"] != "inspect_target_trace":
            continue
        branch = item.get("branch")
        if branch not in trace["branches"]:
            continue
        values[f"{branch}.outcome"] = item["outcome"]
        values[f"{branch}.row_count"] = item.get("row_count")
        values[f"{branch}.reason"] = item.get("reason")
    return values


def compare_voting_reports(before: StoredReport, after: StoredReport) -> VotingComparison:
    before_trace = before.get("target_trace")
    after_trace = after.get("target_trace")
    if before_trace is None or after_trace is None:
        raise ValueError("Both task IDs must refer to voting reports with evidence scoped to a named environment.")
    scope = _voting_scope(before_trace)
    if scope != _voting_scope(after_trace):
        raise ValueError("Reports must match on environment and exact voter ID.")

    return {
        "before_task_id": before["task_id"], "after_task_id": after["task_id"], "target": scope,
        "changes": _changed_fields(
            _voting_values(before_trace, before["evidence"]),
            _voting_values(after_trace, after["evidence"]),
        ),
    }


def _queue_observation(report: StoredReport) -> tuple[str, dict[str, ComparisonValue]]:
    queues = [check for check in report["evidence"] if check["tool"] == "inspect_queue"]
    if len(queues) != 1:
        raise ValueError("Both task IDs must refer to diagnosis reports with one queue observation.")
    queue, = queues
    values = _queue_values(queue)
    values["queue.reason"] = queue.get("reason")
    values["queue.http_status"] = queue.get("http_status")
    return queue["source"], values


def compare_queue_reports(
    before: StoredReport, after: StoredReport, *, include_consumer_health: bool = False,
) -> QueueComparison:
    source, before_values = _queue_observation(before)
    after_source, after_values = _queue_observation(after)
    if source != after_source:
        raise ValueError("Reports must match on the queue evidence source.")
    comparison: QueueComparison = {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "queue_source": source,
        "changes": _changed_fields(before_values, after_values),
    }
    if include_consumer_health:
        comparison["consumer_health_changes"] = _endpoint_changes(
            _endpoint_observations(before["evidence"], _CONSUMER_HEALTH_ROLES),
            _endpoint_observations(after["evidence"], _CONSUMER_HEALTH_ROLES),
        )
    return comparison


def _endpoint_observations(
    evidence: list[Evidence], roles: dict[str, HealthRole],
) -> EndpointObservations:
    observations: EndpointObservations = {}
    counts: dict[tuple[HealthRole, str], int] = {}
    for item in evidence:
        role = roles.get(item["tool"])
        if role is None:
            continue
        source = item["source"]
        key = (role, source)
        occurrence = counts.get(key, 0) + 1
        counts[key] = occurrence
        observation: EndpointObservation = {
            "health": item["health"],
            "outcome": item["outcome"],
            "http_status": item.get("http_status"),
            "reason": item.get("reason"),
        }
        if item.get("mysql_error_code") is not None:
            observation["mysql_error_code"] = item["mysql_error_code"]
        observations[(role, source, occurrence)] = observation
    return observations


def _health_observations(report: StoredReport) -> tuple[str, EndpointObservations]:
    observations = _endpoint_observations(report["evidence"], _HTTP_HEALTH_ROLES)
    services = [source for role, source, _ in observations if role == "service"]
    if len(services) != 1:
        raise ValueError("Both task IDs must refer to HTTP health reports with one service endpoint.")
    return services[0], observations


def _endpoint_changes(before: EndpointObservations, after: EndpointObservations) -> list[EndpointChange]:
    changes: list[EndpointChange] = []
    for role, source, occurrence in before | after:
        key = (role, source, occurrence)
        previous = before.get(key)
        current = after.get(key)
        if current != previous:
            changes.append({
                "role": role, "source": source, "occurrence": occurrence,
                "before": previous, "after": current,
            })
    return changes


def _service_logs(evidence: list[Evidence]) -> dict[str, ServiceLogObservation]:
    observations: dict[str, ServiceLogObservation] = {}
    for item in evidence:
        if item["tool"] != "inspect_service_log":
            continue
        source = item["source"]
        if source in observations:
            raise ValueError("A report contains multiple log observations for the same source.")
        observations[source] = {
            "outcome": item["outcome"], "error_count": item.get("error_count"),
            "reason": item.get("reason"), "window_start": item["window_start"],
            "window_end": item["window_end"],
        }
    return observations


def _log_values(
    observation: ServiceLogObservation | None,
) -> tuple[ObservationOutcome, int | None, str | None] | None:
    if observation is None:
        return None
    return observation["outcome"], observation["error_count"], observation["reason"]


def _service_log_changes(before: list[Evidence], after: list[Evidence]) -> list[ServiceLogChange]:
    previous = _service_logs(before)
    current = _service_logs(after)
    changes: list[ServiceLogChange] = []
    for source in previous | current:
        first = previous.get(source)
        second = current.get(source)
        if _log_values(first) != _log_values(second):
            changes.append({"source": source, "before": first, "after": second})
    return changes


def compare_health_reports(
    before: StoredReport, after: StoredReport, *, include_service_log: bool = False,
) -> HealthComparison:
    service, before_values = _health_observations(before)
    after_service, after_values = _health_observations(after)
    if service != after_service:
        raise ValueError("Reports must match on the service endpoint URL.")

    comparison: HealthComparison = {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "before_status": before["status"],
        "after_status": after["status"],
        "service_url": service,
        "changes": _endpoint_changes(before_values, after_values),
    }
    unchanged_endpoint_issues: list[EndpointIssue] = [
        {"role": role, "source": source, "occurrence": occurrence, "observation": observation}
        for (role, source, occurrence), observation in after_values.items()
        if observation["health"] != "healthy" and before_values.get((role, source, occurrence)) == observation
    ]
    if unchanged_endpoint_issues:
        comparison["unchanged_endpoint_issues"] = unchanged_endpoint_issues
    if include_service_log:
        comparison["service_log_changes"] = _service_log_changes(before["evidence"], after["evidence"])
    return comparison
