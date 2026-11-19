"""Evidence-driven workflow for the queue backlog case."""

from typing import Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph


class Observation(TypedDict):
    source: str
    observed_at: str


class QueueEvidence(Observation):
    tool: Literal["inspect_queue"]
    pending_messages: int


class ConsumerEvidence(Observation):
    tool: Literal["inspect_consumer"]
    running: bool


class DiagnosticTools(Protocol):
    def inspect_queue(self) -> QueueEvidence: ...

    def inspect_consumer(self) -> ConsumerEvidence: ...


class DiagnosisReport(TypedDict):
    question: str
    status: Literal["no_backlog_observed", "consumer_stopped", "undetermined"]
    conclusion: str
    evidence: list[QueueEvidence | ConsumerEvidence]


class DiagnosisInput(TypedDict):
    question: str


class DiagnosisState(DiagnosisInput, total=False):
    queue: QueueEvidence
    consumer: ConsumerEvidence
    report: DiagnosisReport


def _after_queue(state: DiagnosisState) -> Literal["inspect_consumer", "report"]:
    if state["queue"]["pending_messages"] > 0:
        return "inspect_consumer"
    return "report"


def _write_report(state: DiagnosisState) -> dict[str, DiagnosisReport]:
    report: DiagnosisReport = {
        "question": state["question"],
        "status": "no_backlog_observed",
        "conclusion": "No pending messages were observed in the queue.",
        "evidence": [state["queue"]],
    }
    consumer = state.get("consumer")
    if consumer is not None:
        report["evidence"].append(consumer)
        if consumer["running"]:
            report["status"] = "undetermined"
            report["conclusion"] = (
                "The queue has pending messages and the consumer is running. "
                "The cause of the backlog is undetermined."
            )
        else:
            report["status"] = "consumer_stopped"
            report["conclusion"] = (
                "The queue has pending messages and the consumer is stopped."
            )
    return {"report": report}


def run_diagnosis(question: str, tools: DiagnosticTools) -> DiagnosisReport:
    def inspect_queue(_state: DiagnosisState) -> dict[str, QueueEvidence]:
        return {"queue": tools.inspect_queue()}

    def inspect_consumer(_state: DiagnosisState) -> dict[str, ConsumerEvidence]:
        return {"consumer": tools.inspect_consumer()}

    graph = StateGraph(DiagnosisState)
    graph.add_node("inspect_queue", inspect_queue)
    graph.add_node("inspect_consumer", inspect_consumer)
    graph.add_node("report", _write_report)
    graph.add_edge(START, "inspect_queue")
    graph.add_conditional_edges("inspect_queue", _after_queue)
    graph.add_edge("inspect_consumer", "report")
    graph.add_edge("report", END)
    return graph.compile().invoke({"question": question})["report"]
