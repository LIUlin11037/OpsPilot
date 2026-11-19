"""Evidence-driven workflow for the queue backlog case."""

from collections.abc import Callable
from functools import partial
from operator import add
from time import perf_counter_ns
from typing import Annotated, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

UnavailableOutcome = Literal["timeout", "missing"]
ToolName = Literal["inspect_queue", "inspect_consumer", "inspect_processing"]
ProcessingResult = Literal["succeeded", "failed"]
ProcessingCorrelation = Literal["matched", "not_in_sample", "unknown"]
DiagnosisStatus = Literal["no_backlog_observed", "consumer_stopped", "undetermined"]


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


class UnavailableEvidence(Observation):
    tool: ToolName
    outcome: UnavailableOutcome


Evidence = QueueEvidence | ConsumerEvidence | ProcessingEvidence | UnavailableEvidence


class ToolCall(TypedDict):
    tool: ToolName
    outcome: Literal["observed", UnavailableOutcome]
    duration_ms: float


class QueueTool(Protocol):
    def inspect_queue(self) -> QueueEvidence | UnavailableEvidence: ...


class DiagnosticTools(QueueTool, Protocol):
    def inspect_consumer(self) -> ConsumerEvidence | UnavailableEvidence: ...


class ProcessingTool(Protocol):
    def inspect_processing(self) -> ProcessingEvidence | UnavailableEvidence: ...


class TargetProcessingTool(ProcessingTool, Protocol):
    def inspect_processing(
        self, *, message_id: str | None = None
    ) -> ProcessingEvidence | UnavailableEvidence: ...


class DiagnosisReportFields(TypedDict):
    question: str
    status: DiagnosisStatus
    conclusion: str
    processing_correlation: ProcessingCorrelation | None
    evidence: list[Evidence]


class DiagnosisReport(DiagnosisReportFields, total=False):
    tool_calls: list[ToolCall]
    requested_message_id: str


class DiagnosisInput(TypedDict):
    question: str


class DiagnosisState(DiagnosisInput, total=False):
    requested_message_id: str
    queue: QueueEvidence | UnavailableEvidence
    consumer: ConsumerEvidence | UnavailableEvidence
    processing: ProcessingEvidence | UnavailableEvidence
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


def _after_queue(state: DiagnosisState) -> Literal["inspect_consumer", "report"]:
    queue = state["queue"]
    if queue["outcome"] == "observed" and queue["pending_messages"] > 0:
        return "inspect_consumer"
    return "report"


def _after_queue_with_processing(
    state: DiagnosisState,
) -> Literal["inspect_processing", "report"]:
    queue = state["queue"]
    if queue["outcome"] == "observed":
        unacknowledged = queue.get("unacknowledged_messages") or 0
        if queue["pending_messages"] > 0 or unacknowledged > 0:
            return "inspect_processing"
    return "report"


def _after_consumer(state: DiagnosisState) -> Literal["inspect_processing", "report"]:
    consumer = state["consumer"]
    if consumer["outcome"] == "observed" and consumer["running"]:
        return "inspect_processing"
    return "report"


def _describe_failure(outcome: UnavailableOutcome) -> str:
    return "timed out" if outcome == "timeout" else "returned no data"


def _correlate_processing(
    queue: QueueEvidence | UnavailableEvidence,
    processing: ProcessingEvidence | UnavailableEvidence,
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


def _conclude_queue_snapshot(queue: QueueEvidence) -> tuple[DiagnosisStatus, str]:
    acknowledgement = ""
    if "unacknowledged_messages" in queue:
        unacknowledged = queue["unacknowledged_messages"]
        if unacknowledged is None:
            acknowledgement = " The unacknowledged message count is unavailable."
        else:
            acknowledgement = (
                " The count of delivered messages awaiting acknowledgement is "
                f"{unacknowledged}."
            )

    pending = queue["pending_messages"]
    if pending <= 0:
        return (
            "no_backlog_observed",
            f"No pending messages were observed in the queue.{acknowledgement}",
        )

    registered = queue.get("registered_consumers")
    if registered is None:
        registration = "The registered consumer count is unavailable."
    else:
        registration = f"The broker reports a registered consumer count of {registered}."
    return "undetermined", (
        f"The broker reports a pending message count of {pending}.{acknowledgement} "
        f"{registration} The cause of the backlog is undetermined."
    )


def _processing_summary(
    processing: ProcessingEvidence | UnavailableEvidence,
    correlation: ProcessingCorrelation | None,
    *,
    targeted: bool = False,
) -> str:
    if processing["outcome"] != "observed":
        failure = _describe_failure(processing["outcome"])
        if targeted:
            return f"Processing lookup for the requested message {failure}."
        return f"Processing inspection {failure}."

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
    queue = state["queue"]
    if queue["outcome"] == "observed":
        status, conclusion = _conclude_queue_snapshot(queue)
    else:
        failure = _describe_failure(queue["outcome"])
        status = "undetermined"
        conclusion = f"Queue inspection {failure}. Queue depth is unknown."

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
    if processing is not None:
        report["evidence"].append(processing)
    return {"report": report}


def _inspect(
    state_field: Literal["queue", "consumer", "processing"],
    tool: Callable[[], Evidence],
    _state: DiagnosisState,
) -> dict[str, Evidence | list[ToolCall]]:
    started_ns = perf_counter_ns()
    evidence = tool()
    duration_ms = (perf_counter_ns() - started_ns) / 1_000_000
    call: ToolCall = {
        "tool": evidence["tool"],
        "outcome": evidence["outcome"],
        "duration_ms": duration_ms,
    }
    return {state_field: evidence, "tool_calls": [call]}


def _queue_graph(tool: QueueTool) -> StateGraph:
    graph = StateGraph(DiagnosisState)
    graph.add_node("inspect_queue", partial(_inspect, "queue", tool.inspect_queue))
    graph.add_node("report", _write_report)
    graph.add_edge(START, "inspect_queue")
    graph.add_edge("report", END)
    return graph


def _add_processing_node(
    graph: StateGraph, inspect: Callable[[], ProcessingEvidence | UnavailableEvidence]
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
) -> DiagnosisReport:
    if message_id is not None and (not message_id.strip() or processing_tool is None):
        raise ValueError("A nonempty message ID requires a configured processing record.")

    graph = _queue_graph(tool)
    state: DiagnosisState = {"question": question}
    if processing_tool is None:
        graph.add_edge("inspect_queue", "report")
    elif message_id is None:
        _add_processing_node(graph, processing_tool.inspect_processing)
        graph.add_conditional_edges("inspect_queue", _after_queue_with_processing)
    else:
        state["requested_message_id"] = message_id
        _add_processing_node(
            graph, partial(processing_tool.inspect_processing, message_id=message_id)
        )
        graph.add_edge("inspect_queue", "inspect_processing")
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
