"""Render saved diagnoses and report comparisons as Markdown."""

import re
from collections import Counter
from collections.abc import Mapping
from html import escape

from opspilot.core.diagnosis import CheckCondition, PlannedCheck, ToolCall
from opspilot.report_comparison import (
    EndpointChange,
    EndpointIssue,
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
    "entity_id": "Requested entity ID",
    "workflow": "Explicit workflow",
    "check_consumer_health": "--check-consumer-health",
    "check_dependencies": "--check-dependencies",
    "check_service_log": "--check-service-log",
}
_SCOPE_LABELS = {
    "service_url": "Service URL",
    "queue_source": "Queue source",
    "queue": "Queue",
    "vhost": "Virtual host",
    "message_id": "Message ID",
    "delivery_id": "Delivery ID",
    "voter_id": "Voter ID",
    "entity_id": "Entity ID",
    "environment": "Environment",
    "snapshot_source": "Snapshot source",
}
_PLAN_CONDITIONS = {
    "always": "Always",
    "service_unhealthy": "Service unhealthy",
    "service_unhealthy_or_unknown": "Service unhealthy or unknown",
    "service_unhealthy_or_unknown_or_log_errors": "Service unhealthy or unknown, or service log errors observed",
    "queue_activity": "Pending or unacknowledged messages observed",
    "queue_activity_or_unavailable": "Queue activity observed or queue read unavailable",
    "exact_message_after_service_issue": "Exact message ID supplied and service unhealthy or unknown",
}
_INVESTIGATION_REASONS = {
    "configured_service": "Configured starting endpoint",
    "service_unhealthy": "Service health check reported an issue",
    "service_health_unknown": "Service health could not be established",
    "queue_activity": "Queue activity was observed",
    "queue_read_unavailable": "Queue read was unavailable",
    "exact_message_after_service_issue": "Exact message ID supplied and service unhealthy or unknown",
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


def _comparison_observation(
    observation: Mapping[str, object] | None, skipped_when: CheckCondition | None = None,
) -> str:
    if observation is None:
        if skipped_when is not None:
            condition = _value(_PLAN_CONDITIONS.get(skipped_when, skipped_when))
            return f"Skipped because the condition was unmet ({condition})"
        return "No observation recorded"
    details = _evidence_details({key: value for key, value in observation.items() if value is not None})
    outcome = f"Outcome {_value(observation['outcome'])}"
    return f"{outcome}, {details}" if details else outcome


def _endpoint_change_lines(changes: list[EndpointChange]) -> list[str]:
    if not changes:
        return ["No changes in recorded endpoint observations."]
    lines = [
        "| Role | Endpoint | Occurrence | Baseline | Follow-up |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for change in changes:
        before = _comparison_observation(change["before"], change.get("before_skipped_when"))
        after = _comparison_observation(change["after"], change.get("after_skipped_when"))
        lines.append(
            f"| {change['role'].capitalize()} | {_value(change['source'])} | "
            f"{change['occurrence']} | {before} | {after} |"
        )
    return lines


def _endpoint_issue_lines(issues: list[EndpointIssue]) -> list[str]:
    if not issues:
        return []
    lines = [
        "", "## Unchanged endpoint issues", "",
        "| Role | Endpoint | Occurrence | Observation in both reports |",
        "| --- | --- | ---: | --- |",
    ]
    for issue in issues:
        lines.append(
            f"| {issue['role'].capitalize()} | {_value(issue['source'])} | {issue['occurrence']} | "
            f"{_comparison_observation(issue['observation'])} |"
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
    lines.extend(_endpoint_issue_lines(comparison.get("unchanged_endpoint_issues", [])))
    processing = comparison.get("message_processing")
    if processing is not None:
        lines.extend([
            "",
            "## Message processing",
            "",
            f"- **Message ID** {_value(processing['message_id'])}",
            f"- **Processing source** {_value(processing['source'])}",
            "",
            "| Baseline | Follow-up |",
            "| --- | --- |",
            f"| {_comparison_observation(processing['before'])} | {_comparison_observation(processing['after'])} |",
        ])
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
    lines.extend(_endpoint_issue_lines(comparison.get("unchanged_endpoint_issues", [])))
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
    if "previous_task_id" in report:
        lines.append(f"- **Previous task ID** {_value(report['previous_task_id'])}")
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

    investigation = report.get("investigation")
    if investigation is not None:
        budget = investigation["budget"]
        lines.extend([
            "", "## Investigation", "",
            f"- **Stop reason** {_value(investigation['stop_reason'])}",
            f"- **Budget** {budget['max_steps']} reads, {budget['max_seconds']:g} seconds",
            f"- **Elapsed** {investigation['elapsed_ms']:.3f} ms",
        ])
        if investigation["conflicting_evidence_indexes"]:
            indexes = _value(investigation["conflicting_evidence_indexes"])
            lines.append(f"- **Different signals (evidence indexes)** {indexes}")
        if investigation["decisions"]:
            lines.extend([
                "", "| Read | Decision | Basis (zero-based evidence indexes) |",
                "| --- | --- | --- |",
            ])
            for decision in investigation["decisions"]:
                reason = _INVESTIGATION_REASONS.get(decision["reason"], decision["reason"])
                indexes = ", ".join(str(index) for index in decision["evidence_indexes"])
                lines.append(f"| {_value(decision['tool'])} | {_value(reason)} | {indexes} |")

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

    skipped_checks = report.get("skipped_checks")
    if skipped_checks:
        has_sources = any("source" in check for check in skipped_checks)
        source_header = " Source |" if has_sources else ""
        source_separator = " --- |" if has_sources else ""
        lines.extend([
            "", "## Skipped checks", "",
            f"| Tool | Unmet condition |{source_header}",
            f"| --- | --- |{source_separator}",
        ])
        for check in skipped_checks:
            condition = _PLAN_CONDITIONS.get(check["when"], check["when"])
            source = f" {_value(check.get('source', ''))} |" if has_sources else ""
            lines.append(f"| {_value(check['tool'])} | {_value(condition)} |{source}")

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
