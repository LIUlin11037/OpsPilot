"""Evidence-driven workflow for the queue backlog case."""

from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial
from operator import add
from time import perf_counter_ns
from typing import Annotated, Literal, Protocol, TypedDict, TypeVar

from langgraph.graph import END, START, StateGraph

UnavailableOutcome = Literal["timeout", "missing"]
ObservationOutcome = Literal["observed", UnavailableOutcome]
ToolName = Literal[
    "inspect_queue", "inspect_consumer", "inspect_processing", "inspect_target_trace",
    "inspect_service_health", "inspect_dependency_health", "inspect_consumer_endpoint",
    "inspect_service_log",
]
ProcessingResult = Literal["succeeded", "failed"]
ProcessingCorrelation = Literal["matched", "not_in_sample", "unknown"]
DiagnosisStatus = Literal[
    "no_backlog_observed", "no_failure_observed", "consumer_stopped", "undetermined"
]


class Observation(TypedDict):
    source: str
    observed_at: str


class QueueEvidenceFields(Observation):
    tool: Literal["inspect_queue"]
    outcome: Literal["observed"]
    pending_messages: int
    sampled_pending_message_ids: list[str]


class QueueEvidence(QueueEvidenceFields, total=False):
    registered_consumers: int | None
    unacknowledged_messages: int | None


class ConsumerEvidence(Observation):
    tool: Literal["inspect_consumer"]
    outcome: Literal["observed"]
    running: bool


class ProcessingEvidence(Observation):
    tool: Literal["inspect_processing"]
    outcome: Literal["observed"]
    result: ProcessingResult
    message_id: str | None


class EmptyProcessingEvidenceFields(Observation):
    tool: Literal["inspect_processing"]
    outcome: Literal["observed"]
    row_count: Literal[0]
    reason: Literal["record_not_found", "message_not_found"]


class EmptyProcessingEvidence(EmptyProcessingEvidenceFields, total=False):
    http_status: int


class UnavailableEvidence(Observation):
    tool: ToolName
    outcome: UnavailableOutcome


class UnavailableQueueEvidence(UnavailableEvidence, total=False):
    """Queue reads may preserve independently observed counts."""

    registered_consumers: int | None
    unacknowledged_messages: int | None


class HealthReadFields(Observation):
    outcome: ObservationOutcome
    health: Literal["healthy", "unhealthy", "unknown"]


class HealthRead(HealthReadFields, total=False):
    http_status: int
    mysql_error_code: int
    reason: str


class HealthEvidence(HealthRead):
    tool: Literal["inspect_service_health", "inspect_dependency_health", "inspect_consumer_endpoint"]


class ServiceLogReadFields(Observation):
    outcome: ObservationOutcome
    window_start: str
    window_end: str


class ServiceLogRead(ServiceLogReadFields, total=False):
    error_count: int
    reason: str


class ServiceLogEvidence(ServiceLogRead):
    tool: Literal["inspect_service_log"]


class TraceEvidenceFields(Observation):
    tool: Literal["inspect_target_trace"]
    checkpoint: str
    outcome: ObservationOutcome
    origin: str


class TraceEvidence(TraceEvidenceFields, total=False):
    duration_ms: float
    branch: str
    consumer: str
    status: str
    row_count: int
    message_id: str
    expected_recipient_listed: bool | None
    recorded_at: str
    sent_at: str
    acknowledged_at: str
    reason: str
    mysql_error_code: int
    http_status: int
    pending_messages: int | None
    unacknowledged_messages: int | None
    registered_consumers: int | None


ProcessingRead = ProcessingEvidence | EmptyProcessingEvidence | UnavailableEvidence


Evidence = (
    QueueEvidence | ConsumerEvidence | ProcessingRead |
    TraceEvidence | HealthEvidence | ServiceLogEvidence
)


class ToolCall(TypedDict):
    tool: ToolName
    outcome: ObservationOutcome
    duration_ms: float


class PlannedCheck(TypedDict):
    tool: ToolName
    when: Literal[
        "always", "service_unhealthy", "service_unhealthy_or_unknown",
        "service_unhealthy_or_unknown_or_log_errors", "queue_activity",
    ]
    count: int


class QueueTool(Protocol):
    def inspect_queue(self) -> QueueEvidence | UnavailableQueueEvidence: ...


class HealthTool(Protocol):
    @property
    def endpoint(self) -> str: ...

    def inspect_health(self) -> HealthRead: ...


def read_health(tool: HealthTool) -> HealthRead:
    try:
        return tool.inspect_health()
    except OSError as error:
        timed_out = isinstance(error, TimeoutError)
        return {
            "source": tool.endpoint,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "timeout" if timed_out else "missing",
            "health": "unknown",
            "reason": "request_timeout" if timed_out else "transport_error",
        }


class DiagnosticTools(QueueTool, Protocol):
    def inspect_consumer(self) -> ConsumerEvidence | UnavailableEvidence: ...


class ProcessingTool(Protocol):
    def inspect_processing(self) -> ProcessingRead: ...


class TargetProcessingTool(ProcessingTool, Protocol):
    def inspect_processing(
        self, *, message_id: str | None = None
    ) -> ProcessingRead: ...


class DiagnosisReportFields(TypedDict):
    question: str
    status: DiagnosisStatus
    conclusion: str
    processing_correlation: ProcessingCorrelation | None
    evidence: list[Evidence]


BranchStatus = Literal["observed", "not_observed", "unknown"]


class TargetTraceSummary(TypedDict):
    environment: str | None
    vhost: str | None
    entity_id: int | str
    message_id: str | None
    branches: dict[str, BranchStatus]


NextCheckAction = Literal[
    "retry_read",
    "verify_read_access",
    "verify_source",
    "verify_lookup",
    "inspect_endpoint_logs",
    "inspect_consumer_health",
    "inspect_processing_logs",
]


class NextCheck(TypedDict):
    action: NextCheckAction
    evidence_index: int
    description: str


DiagnosisWorkflow = Literal["http_health", "queue", "target_trace"]


class DiagnosisRouteFields(TypedDict):
    workflow: DiagnosisWorkflow
    matched_terms: list[str]


class DiagnosisRoute(DiagnosisRouteFields, total=False):
    selected_by: Literal["message_id", "delivery_id", "workflow"]


class DiagnosisReport(DiagnosisReportFields, total=False):
    tool_calls: list[ToolCall]
    requested_message_id: str
    target_trace: TargetTraceSummary
    next_checks: list[NextCheck]
    routing: DiagnosisRoute
    planned_checks: list[PlannedCheck]


_NEXT_CHECK_DESCRIPTIONS: dict[NextCheckAction, str] = {
    "retry_read": "Collect a fresh read for the same scope and compare it with this observation.",
    "verify_read_access": "Verify that the configured read-only account can access this source.",
    "verify_source": "Verify the configured source and its availability before repeating the read.",
    "verify_lookup": "Verify the lookup keys and expected scope, then inspect the matching records.",
    "inspect_endpoint_logs": "Read request and error logs for this endpoint around the observation time.",
    "inspect_consumer_health": "Review consumer service health and read recent subscription logs.",
    "inspect_processing_logs": "Read logs for the observed processing attempt to verify its error and message ID.",
}


def _next_check_action(evidence: Evidence) -> NextCheckAction | None:
    if evidence["outcome"] == "timeout":
        return "retry_read"
    if evidence["outcome"] == "missing":
        if evidence.get("reason") in ("access_denied", "authentication_failed"):
            return "verify_read_access"
        return "verify_source"
    if evidence.get("health") == "unhealthy":
        return "inspect_endpoint_logs"
    if evidence.get("reason") == "ambiguous_rows":
        return "verify_lookup"
    if evidence.get("row_count") == 0:
        return "retry_read" if "branch" in evidence else "verify_lookup"
    pending = evidence.get("pending_messages")
    if evidence.get("running") is False or (
        isinstance(pending, int) and pending > 0 and evidence.get("registered_consumers") == 0
    ):
        return "inspect_consumer_health"
    if evidence.get("result") == "failed":
        return "inspect_processing_logs"
    return None


def suggest_next_checks(evidence: list[Evidence]) -> list[NextCheck]:
    checks: list[NextCheck] = []
    for index, item in enumerate(evidence):
        action = _next_check_action(item)
        if action is not None:
            checks.append({
                "action": action,
                "evidence_index": index,
                "description": _NEXT_CHECK_DESCRIPTIONS[action],
            })
    return checks


class DiagnosisInput(TypedDict):
    question: str


class DiagnosisState(DiagnosisInput, total=False):
    requested_message_id: str
    queue: QueueEvidence | UnavailableQueueEvidence
    consumer: ConsumerEvidence | UnavailableEvidence
    consumer_health: HealthEvidence
    processing: ProcessingRead
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


def _after_queue(state: DiagnosisState) -> Literal["inspect_consumer", "report"]:
    queue = state["queue"]
    if queue["outcome"] == "observed" and queue["pending_messages"] > 0:
        return "inspect_consumer"
    return "report"


def _has_queue_activity(state: DiagnosisState) -> bool:
    queue = state["queue"]
    if queue["outcome"] == "observed" and queue["pending_messages"] > 0:
        return True
    return queue["outcome"] != "timeout" and (queue.get("unacknowledged_messages") or 0) > 0


def _after_consumer(state: DiagnosisState) -> Literal["inspect_processing", "report"]:
    consumer = state["consumer"]
    if consumer["outcome"] == "observed" and consumer["running"]:
        return "inspect_processing"
    return "report"


def _describe_failure(outcome: UnavailableOutcome) -> str:
    return "timed out" if outcome == "timeout" else "returned no data"


def _correlate_processing(
    queue: QueueEvidence | UnavailableQueueEvidence,
    processing: ProcessingRead,
) -> ProcessingCorrelation:
    if queue["outcome"] != "observed" or processing["outcome"] != "observed":
        return "unknown"

    sampled_ids = queue.get("sampled_pending_message_ids")
    message_id = processing.get("message_id")
    if not sampled_ids or not message_id:
        return "unknown"
    if message_id in sampled_ids:
        return "matched"
    return "not_in_sample"


def _conclude_queue_snapshot(
    queue: QueueEvidence | UnavailableQueueEvidence,
) -> tuple[DiagnosisStatus, str]:
    acknowledgement = ""
    unacknowledged = queue.get("unacknowledged_messages")
    if unacknowledged is not None:
        acknowledgement = (
            f" The count of delivered messages awaiting acknowledgement is {unacknowledged}."
        )
    elif queue["outcome"] == "observed" and "unacknowledged_messages" in queue:
        acknowledgement = " The unacknowledged message count is unavailable."

    registered = queue.get("registered_consumers")
    registration = (
        f" The broker reports a registered consumer count of {registered}."
        if registered is not None else ""
    )
    if queue["outcome"] != "observed":
        if queue["outcome"] == "missing" and (unacknowledged is not None or registered is not None):
            return "undetermined", (
                f"The pending message count is unavailable.{acknowledgement}{registration}"
            )
        failure = _describe_failure(queue["outcome"])
        return "undetermined", f"Queue inspection {failure}. Queue depth is unknown."

    pending = queue["pending_messages"]
    if pending <= 0:
        return (
            "no_backlog_observed",
            f"No pending messages were observed in the queue.{acknowledgement}",
        )

    registration = registration or " The registered consumer count is unavailable."
    return "undetermined", (
        f"The broker reports a pending message count of {pending}.{acknowledgement}"
        f"{registration} The cause of the backlog is undetermined."
    )


def _processing_summary(
    processing: ProcessingRead,
    correlation: ProcessingCorrelation | None,
    *,
    targeted: bool = False,
) -> str:
    if processing["outcome"] != "observed":
        failure = _describe_failure(processing["outcome"])
        if targeted:
            return f"Processing lookup for the requested message {failure}."
        return f"Processing inspection {failure}."

    if processing.get("row_count") == 0:
        scope = " for the requested message" if targeted else ""
        return f"Processing lookup{scope} succeeded and returned zero matching records."
    subject = "The latest processing attempt"
    if targeted:
        subject = f"{subject} for the requested message"
    detail = f"{subject} {processing['result']}."
    if correlation == "matched":
        return f"{detail} Its message ID matches a sampled pending message."
    if correlation == "not_in_sample":
        return f"{detail} Its message ID was absent from the pending-message sample."
    return f"{detail} Message correlation is unknown."


def _conclude_without_consumer(
    state: DiagnosisState, correlation: ProcessingCorrelation | None
) -> tuple[DiagnosisStatus, str]:
    status, conclusion = _conclude_queue_snapshot(state["queue"])
    health = state.get("consumer_health")
    if health is not None:
        conclusion += f" Consumer endpoint health is {health['health']}."

    processing = state.get("processing")
    if processing is not None:
        detail = _processing_summary(
            processing, correlation, targeted="requested_message_id" in state
        )
        conclusion = f"{conclusion} {detail}"
    return status, conclusion


def _conclude(
    state: DiagnosisState, correlation: ProcessingCorrelation | None
) -> tuple[DiagnosisStatus, str]:
    consumer = state.get("consumer")
    if consumer is None:
        return _conclude_without_consumer(state, correlation)

    if consumer["outcome"] != "observed":
        failure = _describe_failure(consumer["outcome"])
        return "undetermined", (
            f"The queue has pending messages. Consumer inspection {failure}, "
            "so the cause of the backlog is undetermined."
        )

    if not consumer["running"]:
        return (
            "consumer_stopped",
            "The queue has pending messages and the consumer is stopped.",
        )

    detail = _processing_summary(state["processing"], correlation)
    return "undetermined", (
        "The queue has pending messages and the consumer is running. "
        f"{detail} The cause of the backlog is undetermined."
    )


def _write_report(state: DiagnosisState) -> dict[str, DiagnosisReport]:
    queue = state["queue"]
    processing = state.get("processing")
    correlation = (
        _correlate_processing(queue, processing) if processing is not None else None
    )
    status, conclusion = _conclude(state, correlation)
    report: DiagnosisReport = {
        "question": state["question"],
        "status": status,
        "conclusion": conclusion,
        "processing_correlation": correlation,
        "evidence": [queue],
        "tool_calls": state["tool_calls"],
    }
    if "requested_message_id" in state:
        report["requested_message_id"] = state["requested_message_id"]
    consumer = state.get("consumer")
    if consumer is not None:
        report["evidence"].append(consumer)
    if "consumer_health" in state:
        report["evidence"].append(state["consumer_health"])
    if processing is not None:
        report["evidence"].append(processing)
    report["next_checks"] = suggest_next_checks(report["evidence"])
    return {"report": report}


ToolResult = TypeVar("ToolResult")


def measure_tool_call(tool: Callable[[], ToolResult]) -> tuple[ToolResult, float]:
    started_ns = perf_counter_ns()
    result = tool()
    return result, (perf_counter_ns() - started_ns) / 1_000_000


def _inspect(
    state_field: Literal["queue", "consumer", "consumer_health", "processing"],
    tool: Callable[[], Evidence],
    _state: DiagnosisState,
) -> dict[str, Evidence | list[ToolCall]]:
    evidence, duration_ms = measure_tool_call(tool)
    call: ToolCall = {
        "tool": evidence["tool"],
        "outcome": evidence["outcome"],
        "duration_ms": duration_ms,
    }
    return {state_field: evidence, "tool_calls": [call]}


def _read_consumer_health(tool: HealthTool) -> HealthEvidence:
    return {**read_health(tool), "tool": "inspect_consumer_endpoint"}


def _queue_graph(tool: QueueTool) -> StateGraph:
    graph = StateGraph(DiagnosisState)
    graph.add_node("inspect_queue", partial(_inspect, "queue", tool.inspect_queue))
    graph.add_node("report", _write_report)
    graph.add_edge(START, "inspect_queue")
    graph.add_edge("report", END)
    return graph


def _add_processing_node(
    graph: StateGraph, inspect: Callable[[], ProcessingRead]
) -> None:
    graph.add_node(
        "inspect_processing", partial(_inspect, "processing", inspect)
    )
    graph.add_edge("inspect_processing", "report")


def run_queue_diagnosis(
    question: str,
    tool: QueueTool,
    processing_tool: TargetProcessingTool | None = None,
    *,
    message_id: str | None = None,
    consumer_health_tool: HealthTool | None = None,
) -> DiagnosisReport:
    if message_id is not None and (not message_id.strip() or processing_tool is None):
        raise ValueError("A nonempty message ID requires a configured processing source.")

    graph = _queue_graph(tool)
    state: DiagnosisState = {"question": question}
    processing_step = "report"
    if processing_tool is not None:
        inspect_processing = processing_tool.inspect_processing
        if message_id is not None:
            state["requested_message_id"] = message_id
            inspect_processing = partial(inspect_processing, message_id=message_id)
        _add_processing_node(graph, inspect_processing)
        processing_step = "inspect_processing"

    active_step = processing_step
    if consumer_health_tool is not None:
        graph.add_node(
            "inspect_consumer_endpoint",
            partial(_inspect, "consumer_health", partial(_read_consumer_health, consumer_health_tool)),
        )
        graph.add_edge("inspect_consumer_endpoint", processing_step)
        active_step = "inspect_consumer_endpoint"

    inactive_step = processing_step if message_id is not None else "report"
    if active_step == inactive_step:
        graph.add_edge("inspect_queue", active_step)
    else:
        graph.add_conditional_edges(
            "inspect_queue", _has_queue_activity, {True: active_step, False: inactive_step},
        )
    return graph.compile().invoke(state)["report"]


def run_diagnosis(
    question: str, tools: DiagnosticTools, processing_tool: ProcessingTool
) -> DiagnosisReport:
    graph = _queue_graph(tools)
    graph.add_node(
        "inspect_consumer", partial(_inspect, "consumer", tools.inspect_consumer)
    )
    _add_processing_node(graph, processing_tool.inspect_processing)
    graph.add_conditional_edges("inspect_queue", _after_queue)
    graph.add_conditional_edges("inspect_consumer", _after_consumer)
    return graph.compile().invoke({"question": question})["report"]
