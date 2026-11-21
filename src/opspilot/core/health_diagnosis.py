"""Select read-only health checks from endpoint observations."""

from collections.abc import Sequence
from functools import partial
from operator import add
from typing import Annotated, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    DiagnosisReport,
    Evidence,
    HealthEvidence,
    HealthTool,
    ServiceLogEvidence,
    ServiceLogRead,
    ToolCall,
    measure_tool_call,
    read_health,
    suggest_next_checks,
)


class _HealthState(TypedDict, total=False):
    question: str
    service: HealthEvidence
    dependencies: list[HealthEvidence]
    service_log: ServiceLogEvidence
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


class ServiceLogTool(Protocol):
    def inspect_recent_errors(self, reference_time: str) -> ServiceLogRead: ...


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


def _check_service_log(tool: ServiceLogTool, state: _HealthState) -> _HealthState:
    read, duration_ms = measure_tool_call(
        partial(tool.inspect_recent_errors, state["service"]["observed_at"])
    )
    evidence: ServiceLogEvidence = {**read, "tool": "inspect_service_log"}
    call: ToolCall = {
        "tool": "inspect_service_log", "outcome": read["outcome"], "duration_ms": duration_ms,
    }
    return {"service_log": evidence, "tool_calls": [call]}


def _after_service(
    state: _HealthState, *, has_log: bool, check_service_log: bool,
    has_dependencies: bool, check_dependencies: bool,
) -> Literal["check_service_log", "check_dependencies", "report"]:
    if has_log and (check_service_log or state["service"]["health"] == "unhealthy"):
        return "check_service_log"
    return _after_log(state, has_dependencies=has_dependencies, check_dependencies=check_dependencies)


def _after_log(
    state: _HealthState, *, has_dependencies: bool, check_dependencies: bool,
) -> Literal["check_dependencies", "report"]:
    service_log = state.get("service_log")
    has_log_errors = service_log is not None and service_log.get("error_count", 0) > 0
    if has_dependencies and (check_dependencies or state["service"]["health"] != "healthy" or has_log_errors):
        return "check_dependencies"
    return "report"


def _write_report(state: _HealthState) -> _HealthState:
    service = state["service"]
    dependencies = state.get("dependencies", [])
    evidence: list[Evidence] = [service]
    summary = [f"Service endpoint health is {service['health']}."]
    service_log = state.get("service_log")
    if service_log is not None:
        evidence.append(service_log)
        if "error_count" in service_log:
            summary.append(f"Service log recorded {service_log['error_count']} error-level entries in the review window.")
        else:
            summary.append("Service log evidence was unavailable.")
    evidence.extend(dependencies)
    for dependency in dependencies:
        label = "Dependency endpoint"
        if len(dependencies) > 1:
            label = f"{label} {dependency['source']}"
        summary.append(f"{label} health is {dependency['health']}.")
    endpoints_healthy = service["health"] == "healthy" and all(
        dependency["health"] == "healthy" for dependency in dependencies
    )
    logs_clear = service_log is None or service_log.get("error_count") == 0
    healthy = endpoints_healthy and logs_clear
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
    service_log: ServiceLogTool | None = None,
    check_service_log: bool = False,
) -> DiagnosisReport:
    if check_service_log and service_log is None:
        raise ValueError("An explicit service log check requires a configured log source.")
    if isinstance(dependency, Sequence):
        dependency_tools = tuple(dependency)
    else:
        dependency_tools = () if dependency is None else (dependency,)

    graph = StateGraph(_HealthState)
    graph.add_node("check_service", partial(_check_service, service))
    graph.add_node("report", _write_report)
    graph.add_edge(START, "check_service")
    graph.add_edge("report", END)
    routes = {"report": "report"}
    if dependency_tools:
        graph.add_node("check_dependencies", partial(_check_dependencies, dependency_tools))
        graph.add_edge("check_dependencies", "report")
        routes["check_dependencies"] = "check_dependencies"
    if service_log is not None:
        graph.add_node("check_service_log", partial(_check_service_log, service_log))
        graph.add_conditional_edges(
            "check_service_log",
            partial(
                _after_log, has_dependencies=bool(dependency_tools),
                check_dependencies=check_dependencies,
            ),
            dict(routes),
        )
        routes["check_service_log"] = "check_service_log"
    graph.add_conditional_edges(
        "check_service",
        partial(
            _after_service, has_log=service_log is not None, has_dependencies=bool(dependency_tools),
            check_dependencies=check_dependencies, check_service_log=check_service_log,
        ),
        routes,
    )
    return graph.compile().invoke({"question": question})["report"]
