"""Compare saved observations for the same message trace."""

from typing import TypedDict

from opspilot.core.diagnosis import Evidence, TargetTraceSummary
from opspilot.task_store import StoredReport

_QUEUE_FIELDS = ("outcome", "pending_messages", "unacknowledged_messages", "registered_consumers")
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
    for field in _QUEUE_FIELDS:
        values[f"queue.{field}"] = (
            queue.get(field)
            if queue is not None and (field == "outcome" or queue["outcome"] == "observed")
            else None
        )
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
