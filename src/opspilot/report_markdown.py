"""Render saved diagnoses and report comparisons as Markdown."""

import re
from collections.abc import Mapping
from html import escape

from opspilot.report_comparison import EndpointObservation, HealthComparison
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


def render_health_comparison_markdown(comparison: HealthComparison) -> str:
    lines = [
        "# OpsPilot health report comparison",
        "",
        f"- **Service URL** {_value(comparison['service_url'])}",
        f"- **Baseline task ID** {_value(comparison['before_task_id'])}",
        f"- **Follow-up task ID** {_value(comparison['after_task_id'])}",
        "",
    ]
    if comparison["changes"]:
        lines.extend([
            "| Role | Endpoint | Occurrence | Baseline | Follow-up |",
            "| --- | --- | ---: | --- | --- |",
        ])
        for change in comparison["changes"]:
            lines.append(
                f"| {change['role'].capitalize()} | {_value(change['source'])} | "
                f"{change['occurrence']} | {_endpoint_observation(change['before'])} | "
                f"{_endpoint_observation(change['after'])} |"
            )
    else:
        lines.append("No changes in recorded endpoint observations.")
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
            label = item.get("checkpoint", item["tool"])
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
