"""Summarize message-trace evidence from a read-only target adapter."""

from typing import Protocol, TypedDict, TypeVar

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    BranchStatus,
    DiagnosisReport,
    Evidence,
    ObservationOutcome,
    ToolCall,
    TraceEvidence,
    measure_tool_call,
    suggest_next_checks,
)


class TargetTraceRead(TypedDict):
    outcome: ObservationOutcome
    environment: str | None
    vhost: str | None
    message_id: str | None
    checks: list[TraceEvidence]


_EntityId = TypeVar("_EntityId", bound=int | str, contravariant=True)


class TargetTraceSource(Protocol[_EntityId]):
    @property
    def branches(self) -> tuple[str, ...]: ...

    def inspect_trace(self, entity_id: _EntityId) -> TargetTraceRead: ...


class _TraceState(TypedDict, total=False):
    question: str
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
    question: str, entity_id: _EntityId, source: TargetTraceSource[_EntityId]
) -> DiagnosisReport:
    def inspect_trace(_state: _TraceState) -> _TraceState:
        trace, duration_ms = measure_tool_call(lambda: source.inspect_trace(entity_id))
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
        evidence: list[Evidence] = list(trace["checks"])

        return {
            "report": {
                "question": state["question"],
                "status": "undetermined",
                "conclusion": " ".join(summary),
                "processing_correlation": "unknown" if trace["message_id"] else None,
                "target_trace": {
                    "environment": trace["environment"],
                    "vhost": trace["vhost"],
                    "entity_id": entity_id,
                    "message_id": trace["message_id"],
                    "branches": branches,
                },
                "evidence": evidence,
                "tool_calls": [state["call"]],
                "next_checks": suggest_next_checks(evidence),
            }
        }

    graph = StateGraph(_TraceState)
    graph.add_node("inspect_target_trace", inspect_trace)
    graph.add_node("report", write_report)
    graph.add_edge(START, "inspect_target_trace")
    graph.add_edge("inspect_target_trace", "report")
    graph.add_edge("report", END)
    return graph.compile().invoke({"question": question})["report"]
