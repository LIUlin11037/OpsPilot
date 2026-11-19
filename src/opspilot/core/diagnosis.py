"""Evidence-driven workflow for the queue backlog case."""

from typing import Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

UnavailableOutcome = Literal["timeout", "missing"]
ProcessingResult = Literal["succeeded", "failed"]
ProcessingCorrelation = Literal["matched", "not_in_sample", "unknown"]
DiagnosisStatus = Literal["no_backlog_observed", "consumer_stopped", "undetermined"]


class Observation(TypedDict):
    source: str
    observed_at: str


class QueueEvidence(Observation):
    tool: Literal["inspect_queue"]
    outcome: Literal["observed"]
    pending_messages: int
    sampled_pending_message_ids: list[str]


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
    tool: Literal["inspect_queue", "inspect_consumer", "inspect_processing"]
    outcome: UnavailableOutcome


class DiagnosticTools(Protocol):
    def inspect_queue(self) -> QueueEvidence | UnavailableEvidence: ...

    def inspect_consumer(self) -> ConsumerEvidence | UnavailableEvidence: ...


class ProcessingTool(Protocol):
    def inspect_processing(self) -> ProcessingEvidence | UnavailableEvidence: ...


class DiagnosisReport(TypedDict):
    question: str
    status: DiagnosisStatus
    conclusion: str
    processing_correlation: ProcessingCorrelation | None
    evidence: list[
        QueueEvidence | ConsumerEvidence | ProcessingEvidence | UnavailableEvidence
    ]


class DiagnosisInput(TypedDict):
    question: str


class DiagnosisState(DiagnosisInput, total=False):
    queue: QueueEvidence | UnavailableEvidence
    consumer: ConsumerEvidence | UnavailableEvidence
    processing: ProcessingEvidence | UnavailableEvidence
    report: DiagnosisReport


def _after_queue(state: DiagnosisState) -> Literal["inspect_consumer", "report"]:
    queue = state["queue"]
    if queue["outcome"] == "observed" and queue["pending_messages"] > 0:
        return "inspect_consumer"
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


def _conclude(
    state: DiagnosisState, correlation: ProcessingCorrelation | None
) -> tuple[DiagnosisStatus, str]:
    queue = state["queue"]
    if queue["outcome"] != "observed":
        failure = _describe_failure(queue["outcome"])
        return "undetermined", f"Queue inspection {failure}. Queue depth is unknown."

    consumer = state.get("consumer")
    if consumer is None:
        return "no_backlog_observed", "No pending messages were observed in the queue."

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

    processing = state["processing"]
    if processing["outcome"] == "observed":
        detail = f"The latest processing attempt {processing['result']}."
        if correlation == "matched":
            detail += " Its message ID matches a sampled pending message."
        elif correlation == "not_in_sample":
            detail += " Its message ID was absent from the pending-message sample."
        else:
            detail += " Message correlation is unknown."
    else:
        failure = _describe_failure(processing["outcome"])
        detail = f"Processing inspection {failure}."

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
    }
    consumer = state.get("consumer")
    if consumer is not None:
        report["evidence"].append(consumer)
    if processing is not None:
        report["evidence"].append(processing)
    return {"report": report}


def run_diagnosis(
    question: str, tools: DiagnosticTools, processing_tool: ProcessingTool
) -> DiagnosisReport:
    def inspect_queue(
        _state: DiagnosisState,
    ) -> dict[str, QueueEvidence | UnavailableEvidence]:
        return {"queue": tools.inspect_queue()}

    def inspect_consumer(
        _state: DiagnosisState,
    ) -> dict[str, ConsumerEvidence | UnavailableEvidence]:
        return {"consumer": tools.inspect_consumer()}

    def inspect_processing(
        _state: DiagnosisState,
    ) -> dict[str, ProcessingEvidence | UnavailableEvidence]:
        return {"processing": processing_tool.inspect_processing()}

    graph = StateGraph(DiagnosisState)
    graph.add_node("inspect_queue", inspect_queue)
    graph.add_node("inspect_consumer", inspect_consumer)
    graph.add_node("inspect_processing", inspect_processing)
    graph.add_node("report", _write_report)
    graph.add_edge(START, "inspect_queue")
    graph.add_conditional_edges("inspect_queue", _after_queue)
    graph.add_conditional_edges("inspect_consumer", _after_consumer)
    graph.add_edge("inspect_processing", "report")
    graph.add_edge("report", END)
    return graph.compile().invoke({"question": question})["report"]
