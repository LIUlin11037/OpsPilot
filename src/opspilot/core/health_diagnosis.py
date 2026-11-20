"""Select read-only health checks from endpoint observations."""

from functools import partial
from operator import add
from typing import Annotated, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    DiagnosisReport,
    Evidence,
    HealthEvidence,
    HealthRead,
    ToolCall,
    measure_tool_call,
    suggest_next_checks,
)


class HealthTool(Protocol):
    def inspect_health(self) -> HealthRead: ...


class _HealthState(TypedDict, total=False):
    question: str
    service: HealthEvidence
    dependency: HealthEvidence
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


def _inspect(
    role: Literal["service", "dependency"], tool: HealthTool, _state: _HealthState
) -> _HealthState:
    read, duration_ms = measure_tool_call(tool.inspect_health)
    name = "inspect_service_health" if role == "service" else "inspect_dependency_health"
    evidence: HealthEvidence = {**read, "tool": name}
    call: ToolCall = {"tool": name, "outcome": read["outcome"], "duration_ms": duration_ms}
    if role == "service":
        return {"service": evidence, "tool_calls": [call]}
    return {"dependency": evidence, "tool_calls": [call]}


def _after_service(state: _HealthState) -> Literal["check_dependency", "report"]:
    return "report" if state["service"]["health"] == "healthy" else "check_dependency"


def _write_report(state: _HealthState) -> _HealthState:
    service = state["service"]
    evidence: list[Evidence] = [service]
    summary = [f"Service endpoint health is {service['health']}."]
    dependency = state.get("dependency")
    if dependency is not None:
        evidence.append(dependency)
        summary.append(f"Dependency endpoint health is {dependency['health']}.")
    healthy = service["health"] == "healthy"
    if not healthy:
        summary.append("The cause is undetermined.")
    return {"report": {
        "question": state["question"],
        "status": "no_failure_observed" if healthy else "undetermined",
        "conclusion": " ".join(summary),
        "processing_correlation": None,
        "evidence": evidence,
        "tool_calls": state["tool_calls"],
        "next_checks": suggest_next_checks(evidence),
    }}


def run_health_diagnosis(
    question: str, service: HealthTool, dependency: HealthTool | None = None
) -> DiagnosisReport:
    graph = StateGraph(_HealthState)
    graph.add_node("check_service", partial(_inspect, "service", service))
    graph.add_node("report", _write_report)
    graph.add_edge(START, "check_service")
    graph.add_edge("report", END)
    if dependency is None:
        graph.add_edge("check_service", "report")
    else:
        graph.add_node("check_dependency", partial(_inspect, "dependency", dependency))
        graph.add_conditional_edges("check_service", _after_service)
        graph.add_edge("check_dependency", "report")
    return graph.compile().invoke({"question": question})["report"]
