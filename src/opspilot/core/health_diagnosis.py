"""Select read-only health checks from endpoint observations."""

from collections.abc import Sequence
from functools import partial
from operator import add
from typing import Annotated, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    DiagnosisReport,
    Evidence,
    HealthEvidence,
    HealthTool,
    ToolCall,
    measure_tool_call,
    read_health,
    suggest_next_checks,
)


class _HealthState(TypedDict, total=False):
    question: str
    service: HealthEvidence
    dependencies: list[HealthEvidence]
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


def _inspect(
    tool: HealthTool, name: Literal["inspect_service_health", "inspect_dependency_health"]
) -> tuple[HealthEvidence, ToolCall]:
    read, duration_ms = measure_tool_call(partial(read_health, tool))
    evidence: HealthEvidence = {**read, "tool": name}
    call: ToolCall = {"tool": name, "outcome": read["outcome"], "duration_ms": duration_ms}
    return evidence, call


def _check_service(tool: HealthTool, _state: _HealthState) -> _HealthState:
    evidence, call = _inspect(tool, "inspect_service_health")
    return {"service": evidence, "tool_calls": [call]}


def _check_dependencies(tools: Sequence[HealthTool], _state: _HealthState) -> _HealthState:
    dependencies: list[HealthEvidence] = []
    calls: list[ToolCall] = []
    for tool in tools:
        evidence, call = _inspect(tool, "inspect_dependency_health")
        dependencies.append(evidence)
        calls.append(call)
    return {"dependencies": dependencies, "tool_calls": calls}


def _after_service(state: _HealthState) -> Literal["check_dependencies", "report"]:
    return "report" if state["service"]["health"] == "healthy" else "check_dependencies"


def _write_report(state: _HealthState) -> _HealthState:
    service = state["service"]
    dependencies = state.get("dependencies", [])
    evidence: list[Evidence] = [service, *dependencies]
    summary = [f"Service endpoint health is {service['health']}."]
    for dependency in dependencies:
        label = "Dependency endpoint"
        if len(dependencies) > 1:
            label = f"{label} {dependency['source']}"
        summary.append(f"{label} health is {dependency['health']}.")
    healthy = service["health"] == "healthy" and all(
        dependency["health"] == "healthy" for dependency in dependencies
    )
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
    question: str, service: HealthTool,
    dependency: HealthTool | Sequence[HealthTool] | None = None,
    *,
    check_dependencies: bool = False,
) -> DiagnosisReport:
    if isinstance(dependency, Sequence):
        dependency_tools = tuple(dependency)
    else:
        dependency_tools = () if dependency is None else (dependency,)

    graph = StateGraph(_HealthState)
    graph.add_node("check_service", partial(_check_service, service))
    graph.add_node("report", _write_report)
    graph.add_edge(START, "check_service")
    graph.add_edge("report", END)
    if not dependency_tools:
        graph.add_edge("check_service", "report")
    else:
        graph.add_node("check_dependencies", partial(_check_dependencies, dependency_tools))
        if check_dependencies:
            graph.add_edge("check_service", "check_dependencies")
        else:
            graph.add_conditional_edges("check_service", _after_service)
        graph.add_edge("check_dependencies", "report")
    return graph.compile().invoke({"question": question})["report"]
