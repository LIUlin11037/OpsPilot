"""Render saved diagnoses and report comparisons as Markdown."""

import re
from collections.abc import Mapping
from html import escape

from opspilot.report_comparison import (
    EndpointChange,
    EndpointObservation,
    HealthComparison,
    QueueComparison,
)
from opspilot.task_store import StoredReport

_MARKDOWN = re.compile(r"([\\`*_\[\]|~])")
_EVIDENCE_FIELDS = {
    "health": "Health",
    "status": "State",
    "pending_messages": "Pending messages",
    "unacknowledged_messages": "Unacknowledged messages",
    "registered_consumers": "Registered consumers",
    "running": "Consumer running",
    "result": "Processing result",
    "message_id": "Message ID",
    "sampled_pending_message_ids": "Sampled pending IDs",
    "consumer": "Consumer",
    "row_count": "Matching rows",
    "expected_recipient_listed": "Expected recipient listed",
    "attempted_at": "Attempted at",
    "recorded_at": "Recorded at",
    "sent_at": "Sent at",
    "acknowledged_at": "Acknowledged at",
    "duration_ms": "Read duration (ms)",
    "reason": "Reason",
    "mysql_error_code": "MySQL error code",
    "http_status": "HTTP status",
}


def _value(value: object) -> str:
    if value is None:
        return "Unavailable"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, list):
        return ", ".join(_value(item) for item in value) or "Empty"
    text = escape(" ".join(str(value).split()), quote=False)
    return _MARKDOWN.sub(r"\\\1", text)


def _evidence_details(item: Mapping[str, object]) -> str:
    return ", ".join(
        f"{label} {_value(item[key])}" for key, label in _EVIDENCE_FIELDS.items() if key in item
    )


def _endpoint_observation(observation: EndpointObservation | None) -> str:
    if observation is None:
        return "No observation recorded"
    details = _evidence_details({key: value for key, value in observation.items() if value is not None})
    return f"Outcome {_value(observation['outcome'])}, {details}"


def _endpoint_change_lines(changes: list[EndpointChange]) -> list[str]:
    if not changes:
        return ["No changes in recorded endpoint observations."]
    lines = [
        "| Role | Endpoint | Occurrence | Baseline | Follow-up |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for change in changes:
        lines.append(
            f"| {change['role'].capitalize()} | {_value(change['source'])} | "
            f"{change['occurrence']} | {_endpoint_observation(change['before'])} | "
            f"{_endpoint_observation(change['after'])} |"
        )
    return lines


def render_queue_comparison_markdown(comparison: QueueComparison) -> str:
    lines = [
        "# OpsPilot queue report comparison",
        "",
        f"- **Queue source** {_value(comparison['queue_source'])}",
        f"- **Baseline task ID** {_value(comparison['before_task_id'])}",
        f"- **Follow-up task ID** {_value(comparison['after_task_id'])}",
        "",
        "## Queue observation changes",
        "",
    ]
    if comparison["changes"]:
        lines.extend([
            "| Observation | Baseline | Follow-up |",
            "| --- | --- | --- |",
        ])
        for change in comparison["changes"]:
            field = change["field"].removeprefix("queue.")
            label = "Outcome" if field == "outcome" else _EVIDENCE_FIELDS.get(field, field)
            lines.append(
                f"| {_value(label)} | {_value(change['before'])} | {_value(change['after'])} |"
            )
    else:
        lines.append("No changes in recorded queue observations.")
    if "consumer_health_changes" in comparison:
        lines.extend(["", "## Consumer endpoint changes", ""])
        lines.extend(_endpoint_change_lines(comparison["consumer_health_changes"]))
    return "\n".join(lines) + "\n"


def render_health_comparison_markdown(comparison: HealthComparison) -> str:
    lines = [
        "# OpsPilot health report comparison",
        "",
        f"- **Service URL** {_value(comparison['service_url'])}",
        f"- **Baseline task ID** {_value(comparison['before_task_id'])}",
        f"- **Follow-up task ID** {_value(comparison['after_task_id'])}",
        "",
    ]
    lines.extend(_endpoint_change_lines(comparison["changes"]))
    return "\n".join(lines) + "\n"


def render_markdown(report: StoredReport) -> str:
    lines = [
        "# OpsPilot diagnosis report",
        "",
        f"- **Task ID** {_value(report['task_id'])}",
        f"- **Question** {_value(report['question'])}",
        f"- **Status** {_value(report['status'])}",
        f"- **Conclusion** {_value(report['conclusion'])}",
    ]
    correlation = report.get("processing_correlation")
    routing = report.get("routing")
    if routing is not None:
        lines.extend([
            f"- **Selected workflow** {_value(routing['workflow'])}",
            f"- **Matched terms** {_value(routing['matched_terms'])}",
        ])
    if correlation is not None:
        lines.append(f"- **Message correlation** {_value(correlation)}")
    if "requested_message_id" in report:
        lines.append(f"- **Requested message ID** {_value(report['requested_message_id'])}")

    trace = report.get("target_trace")
    if trace is not None:
        lines.extend([
            "", "## Target trace", "",
            f"- **Environment** {_value(trace['environment'])}",
            f"- **Virtual host** {_value(trace['vhost'])}",
            f"- **Entity ID** {_value(trace['entity_id'])}",
            f"- **Message ID** {_value(trace['message_id'])}",
        ])
        lines.extend(
            f"- **{_value(branch)} branch** {_value(status)}"
            for branch, status in trace["branches"].items()
        )

    if report.get("next_checks"):
        lines.extend(["", "## Suggested read-only checks", ""])
        for check in report["next_checks"]:
            item = report["evidence"][check["evidence_index"]]
            tool = item["tool"]
            if tool == "inspect_service_health":
                label = f"Service endpoint {item['source']}"
            elif tool == "inspect_dependency_health":
                label = f"Dependency endpoint {item['source']}"
            elif tool == "inspect_consumer_endpoint":
                label = f"Consumer endpoint {item['source']}"
            else:
                label = item.get("checkpoint", tool)
            lines.append(f"- **{_value(label)}** {_value(check['description'])}")

    lines.extend(["", "## Evidence", ""])
    if report["evidence"]:
        lines.extend([
            "| Check | Outcome | Details | Origin | Source | Observed at |",
            "| --- | --- | --- | --- | --- | --- |",
        ])
        for item in report["evidence"]:
            check = item.get("checkpoint", item["tool"])
            lines.append(
                f"| {_value(check)} | {_value(item['outcome'])} | {_evidence_details(item)} | "
                f"{_value(item.get('origin', ''))} | {_value(item['source'])} | "
                f"{_value(item['observed_at'])} |"
            )
    else:
        lines.append("No evidence was recorded.")

    if report.get("tool_calls"):
        lines.extend([
            "", "## Tool calls", "",
            "| Tool | Outcome | Duration (ms) |",
            "| --- | --- | ---: |",
        ])
        for call in report["tool_calls"]:
            lines.append(
                f"| {_value(call['tool'])} | {_value(call['outcome'])} | {call['duration_ms']:.3f} |"
            )
    return "\n".join(lines) + "\n"
