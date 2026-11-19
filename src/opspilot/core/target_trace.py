"""Summarize message-trace evidence from a read-only target adapter."""

from typing import Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    BranchStatus,
    DiagnosisReport,
    ObservationOutcome,
    ToolCall,
    TraceEvidence,
    measure_tool_call,
)


class TargetTraceRead(TypedDict):
    outcome: ObservationOutcome
    environment: str | None
    vhost: str | None
    message_id: str | None
    checks: list[TraceEvidence]


class TargetTraceSource(Protocol):
    @property
    def branches(self) -> tuple[str, ...]: ...

    def inspect_trace(self, entity_id: int) -> TargetTraceRead: ...


class _TraceState(TypedDict, total=False):
    question: str
    entity_id: int
    trace: TargetTraceRead
    call: ToolCall
    report: DiagnosisReport


def _branch_status(evidence: TraceEvidence) -> BranchStatus:
    row_count = evidence.get("row_count")
    if evidence["outcome"] != "observed" or row_count is None:
        return "unknown"
    if row_count == 0:
        return "not_observed"
    return "observed"


def run_target_trace(
    question: str, entity_id: int, source: TargetTraceSource
) -> DiagnosisReport:
    def inspect_trace(state: _TraceState) -> _TraceState:
        trace, duration_ms = measure_tool_call(
            lambda: source.inspect_trace(state["entity_id"])
        )
        return {
            "trace": trace,
            "call": {
                "tool": "inspect_target_trace",
                "outcome": trace["outcome"],
                "duration_ms": duration_ms,
            },
        }

    def write_report(state: _TraceState) -> _TraceState:
        trace = state["trace"]
        branches: dict[str, BranchStatus] = {name: "unknown" for name in source.branches}
        for evidence in trace["checks"]:
            if "branch" in evidence:
                branches[evidence["branch"]] = _branch_status(evidence)

        if trace["message_id"] is None:
            summary = ["The target trace did not establish a unique message ID."]
        else:
            summary = ["The target trace resolved a unique message ID."]
        for branch, status in branches.items():
            if status == "observed":
                summary.append(f"{branch} has an observed processing record.")
            elif status == "not_observed":
                summary.append(
                    f"{branch} lookup succeeded and returned zero processing records."
                )
            else:
                summary.append(f"{branch} processing status is unknown.")
        summary.append("The root cause is undetermined.")

        return {
            "report": {
                "question": state["question"],
                "status": "undetermined",
                "conclusion": " ".join(summary),
                "processing_correlation": "unknown" if trace["message_id"] else None,
                "target_trace": {
                    "environment": trace["environment"],
                    "vhost": trace["vhost"],
                    "entity_id": state["entity_id"],
                    "message_id": trace["message_id"],
                    "branches": branches,
                },
                "evidence": list(trace["checks"]),
                "tool_calls": [state["call"]],
            }
        }

    graph = StateGraph(_TraceState)
    graph.add_node("inspect_target_trace", inspect_trace)
    graph.add_node("report", write_report)
    graph.add_edge(START, "inspect_target_trace")
    graph.add_edge("inspect_target_trace", "report")
    graph.add_edge("report", END)
    return graph.compile().invoke({"question": question, "entity_id": entity_id})["report"]
