"""Render saved diagnoses and report comparisons as Markdown."""

import re
from collections import Counter
from collections.abc import Mapping
from html import escape

from opspilot.core.diagnosis import PlannedCheck, ToolCall
from opspilot.report_comparison import (
    EndpointChange,
    HealthComparison,
    QueueComparison,
    VotingComparison,
)
from opspilot.task_store import StoredReport

_MARKDOWN = re.compile(r"([\\`*_\[\]|~])")
_ROUTE_BASES = {
    "message_id": "Requested message ID",
    "delivery_id": "Requested delivery ID",
    "voter_id": "Requested voter ID",
    "workflow": "Explicit workflow",
}
_SCOPE_LABELS = {
    "service_url": "Service URL",
    "queue_source": "Queue source",
    "queue": "Queue",
    "vhost": "Virtual host",
    "message_id": "Message ID",
    "delivery_id": "Delivery ID",
    "voter_id": "Voter ID",
    "snapshot_source": "Snapshot source",
}
_PLAN_CONDITIONS = {
    "always": "Always",
    "service_unhealthy": "Service unhealthy",
    "service_unhealthy_or_unknown": "Service unhealthy or unknown",
    "service_unhealthy_or_unknown_or_log_errors": "Service unhealthy or unknown, or service log errors observed",
    "queue_activity": "Pending or unacknowledged messages observed",
}
_VOTING_FIELDS = {
    "branches.queued_vote": "Redis queue status",
    "branches.stored_vote": "PostgreSQL record status",
    "queued_vote.outcome": "Redis query outcome",
    "queued_vote.row_count": "Redis matching rows",
    "queued_vote.reason": "Redis reason",
    "stored_vote.outcome": "PostgreSQL query outcome",
    "stored_vote.row_count": "PostgreSQL matching rows",
    "stored_vote.reason": "PostgreSQL reason",
}
_EVIDENCE_FIELDS = {
    "health": "Health",
    "error_count": "Error-level entries",
    "window_start": "Window start",
    "window_end": "Window end",
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


def _comparison_observation(observation: Mapping[str, object] | None) -> str:
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
            f"{change['occurrence']} | {_comparison_observation(change['before'])} | "
            f"{_comparison_observation(change['after'])} |"
        )
    return lines


def render_voting_comparison_markdown(comparison: VotingComparison) -> str:
    target = comparison["target"]
    lines = [
        "# OpsPilot voting report comparison",
        "",
        f"- **Environment** {_value(target['environment'])}",
        f"- **Voter ID** {_value(target['voter_id'])}",
        f"- **Baseline task ID** {_value(comparison['before_task_id'])}",
        f"- **Follow-up task ID** {_value(comparison['after_task_id'])}",
        "",
    ]
    if not comparison["changes"]:
        lines.append("No changes in recorded voting observations.")
    else:
        lines.extend(["| Observation | Baseline | Follow-up |", "| --- | --- | --- |"])
        for change in comparison["changes"]:
            label = _VOTING_FIELDS.get(change["field"], change["field"])
            lines.append(f"| {_value(label)} | {_value(change['before'])} | {_value(change['after'])} |")
    return "\n".join(lines) + "\n"


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
        f"- **Baseline diagnosis status** {_value(comparison['before_status'])}",
        f"- **Follow-up diagnosis status** {_value(comparison['after_status'])}",
        "",
    ]
    lines.extend(_endpoint_change_lines(comparison["changes"]))
    if "unchanged_endpoint_issues" in comparison:
        lines.extend([
            "", "## Unchanged endpoint issues", "",
            "| Role | Endpoint | Occurrence | Observation in both reports |",
            "| --- | --- | ---: | --- |",
        ])
        for issue in comparison["unchanged_endpoint_issues"]:
            lines.append(
                f"| {issue['role'].capitalize()} | {_value(issue['source'])} | {issue['occurrence']} | "
                f"{_comparison_observation(issue['observation'])} |"
            )
    if "service_log_changes" in comparison:
        lines.extend(["", "## Service log changes", ""])
        changes = comparison["service_log_changes"]
        if changes:
            lines.extend(["| Source | Baseline | Follow-up |", "| --- | --- | --- |"])
            for change in changes:
                lines.append(
                    f"| {_value(change['source'])} | {_comparison_observation(change['before'])} | "
                    f"{_comparison_observation(change['after'])} |"
                )
        else:
            lines.append("No changes in recorded service log observations.")
    return "\n".join(lines) + "\n"


def _planned_check_lines(checks: list[PlannedCheck], calls: list[ToolCall] | None) -> list[str]:
    has_sources = any(check.get("sources") for check in checks)
    source_header = "Source | " if has_sources else ""
    source_separator = "--- | " if has_sources else ""
    lines = [
        "", "## Planned checks", "",
        f"| Tool | {source_header}When | Configured calls | Recorded calls |",
        f"| --- | {source_separator}--- | ---: | ---: |",
    ]
    recorded_counts = Counter(call["tool"] for call in calls or ())
    recorded_sources = Counter((call["tool"], call["source"]) for call in calls or () if "source" in call)
    unscoped_tools = {call["tool"] for call in calls or () if "source" not in call}
    for check in checks:
        tool = check["tool"]
        condition = _value(_PLAN_CONDITIONS.get(check["when"], check["when"]))
        sources = check.get("sources")
        if sources:
            for source, count in Counter(sources).items():
                recorded = recorded_sources[tool, source] if calls is not None and tool not in unscoped_tools else None
                lines.append(f"| {_value(tool)} | {_value(source)} | {condition} | {count} | {_value(recorded)} |")
        else:
            source_cell = " | " if has_sources else ""
            recorded = recorded_counts[tool] if calls is not None else None
            lines.append(f"| {_value(tool)} | {source_cell}{condition} | {check['count']} | {_value(recorded)} |")
    return lines


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
        lines.append(f"- **Selected workflow** {_value(routing['workflow'])}")
        basis = _ROUTE_BASES.get(routing.get("selected_by", ""))
        if basis is not None:
            lines.append(f"- **Selection basis** {basis}")
        if routing["matched_terms"]:
            lines.append(f"- **Matched terms** {_value(routing['matched_terms'])}")
    if correlation is not None:
        lines.append(f"- **Message correlation** {_value(correlation)}")
    if "requested_message_id" in report:
        lines.append(f"- **Requested message ID** {_value(report['requested_message_id'])}")

    planned_scope = report.get("planned_scope")
    if planned_scope:
        lines.extend(["", "## Planned scope", ""])
        lines.extend(
            f"- **{_value(_SCOPE_LABELS.get(key, key))}** {_value(value)}"
            for key, value in planned_scope.items()
        )

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

    calls = report.get("tool_calls")
    if report.get("planned_checks"):
        lines.extend(_planned_check_lines(report["planned_checks"], calls))

    if calls:
        has_sources = any("source" in call for call in calls)
        source_header = "Source | " if has_sources else ""
        source_separator = "--- | " if has_sources else ""
        lines.extend([
            "", "## Tool calls", "",
            f"| Tool | {source_header}Outcome | Duration (ms) |",
            f"| --- | {source_separator}--- | ---: |",
        ])
        for call in calls:
            source = f"{_value(call.get('source'))} | " if has_sources else ""
            lines.append(
                f"| {_value(call['tool'])} | {source}{_value(call['outcome'])} | {call['duration_ms']:.3f} |"
            )
    return "\n".join(lines) + "\n"
