"""Select read-only health checks from endpoint observations."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from operator import add
from time import monotonic
from typing import Annotated, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    CheckCondition,
    DiagnosisReport,
    Evidence,
    HealthEvidence,
    HealthTool,
    MetricEvidence,
    ServiceLogEvidence,
    ServiceLogRead,
    SkippedCheck,
    ToolCall,
    measure_tool_call,
    read_health,
    suggest_next_checks,
    tool_call_from_evidence,
)
from opspilot.core.metrics import MetricRead


class _HealthState(TypedDict, total=False):
    question: str
    service: HealthEvidence
    dependencies: list[HealthEvidence]
    service_log: ServiceLogEvidence
    metrics: list[MetricEvidence]
    tool_calls: Annotated[list[ToolCall], add]
    report: DiagnosisReport


class ServiceLogTool(Protocol):
    def inspect_recent_errors(self, reference_time: str) -> ServiceLogRead: ...


@dataclass(frozen=True)
class MetricCheck:
    endpoint: str
    name: str
    read: Callable[[str, float], MetricRead]


def _inspect(
    tool: HealthTool, name: Literal["inspect_service_health", "inspect_dependency_health"]
) -> tuple[HealthEvidence, ToolCall]:
    read, duration_ms = measure_tool_call(partial(read_health, tool))
    evidence: HealthEvidence = {**read, "tool": name}
    return evidence, tool_call_from_evidence(evidence, duration_ms)


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


def _check_service_log(read_log: Callable[[str], ServiceLogRead], state: _HealthState) -> _HealthState:
    read, duration_ms = measure_tool_call(
        partial(read_log, state["service"]["observed_at"])
    )
    evidence: ServiceLogEvidence = {**read, "tool": "inspect_service_log"}
    return {"service_log": evidence, "tool_calls": [tool_call_from_evidence(evidence, duration_ms)]}


def _check_metrics(
    state: _HealthState, *, tools: tuple[MetricCheck, ...], force: bool, budget_seconds: float,
) -> _HealthState:
    service = state["service"]
    if not force and service["health"] == "healthy":
        return {}
    deadline = monotonic() + budget_seconds
    trigger = "explicit_recheck" if force else f"service_{service['health']}"
    evidence: list[MetricEvidence] = []
    calls: list[ToolCall] = []
    for tool in tools:
        read, duration = measure_tool_call(
            partial(tool.read, service["observed_at"], max(0.0, deadline - monotonic())),
        )
        metric: MetricEvidence = {**read, "tool": "inspect_service_metrics", "trigger": trigger}
        if "peak_value" in metric:
            metric["threshold_exceeded"] = metric["peak_value"] > metric["threshold"]
        evidence.append(metric)
        calls.append(tool_call_from_evidence(metric, duration))
    return {"metrics": evidence, "tool_calls": calls}


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


def _write_report(
    state: _HealthState, *, conditional_log: bool, conditional_dependencies: tuple[HealthTool, ...],
    conditional_metrics: tuple[MetricCheck, ...],
) -> _HealthState:
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
    metrics = state.get("metrics", [])
    evidence.extend(metrics)
    for metric in metrics:
        name = f"{metric['query_name']} for {metric['service']}"
        if metric["outcome"] == "observed":
            relation = "above" if metric["threshold_exceeded"] else "within"
            summary.append(
                f"Metric {name} peaked at {metric['peak_value']:g} {metric['unit']}, "
                f"{relation} its configured threshold in the review window."
            )
        else:
            summary.append(f"Metric {name} was unavailable.")
    metrics_clear = all(metric.get("threshold_exceeded") is False for metric in metrics)
    healthy = endpoints_healthy and logs_clear and metrics_clear
    if not healthy:
        summary.append("The cause is undetermined.")
    report: DiagnosisReport = {
        "question": state["question"],
        "status": "no_failure_observed" if healthy else "undetermined",
        "conclusion": " ".join(summary),
        "processing_correlation": None,
        "evidence": evidence,
        "tool_calls": state["tool_calls"],
        "next_checks": suggest_next_checks(evidence),
    }
    skipped: list[SkippedCheck] = []
    if conditional_log and service_log is None:
        skipped.append({"tool": "inspect_service_log", "when": "service_unhealthy"})
    if conditional_dependencies and "dependencies" not in state:
        condition: CheckCondition = (
            "service_unhealthy_or_unknown_or_log_errors" if service_log is not None
            else "service_unhealthy_or_unknown"
        )
        for dependency in conditional_dependencies:
            skipped.append({"tool": "inspect_dependency_health", "when": condition, "source": dependency.endpoint})
    if "metrics" not in state:
        skipped.extend({
            "tool": "inspect_service_metrics", "when": "service_unhealthy_or_unknown",
            "source": metric.endpoint, "query_name": metric.name,
        } for metric in conditional_metrics)
    if skipped:
        report["skipped_checks"] = skipped
    return {"report": report}


def run_health_diagnosis(
    question: str, service: HealthTool,
    dependency: HealthTool | Sequence[HealthTool] | None = None,
    *,
    check_dependencies: bool = False,
    service_log: ServiceLogTool | Callable[[str], ServiceLogRead] | None = None,
    check_service_log: bool = False,
    metric_checks: tuple[MetricCheck, ...] = (),
    check_metrics: bool = False,
    metric_budget_seconds: float = 5.0,
) -> DiagnosisReport:
    if check_service_log and service_log is None:
        raise ValueError("An explicit service log check requires a configured log source.")
    if isinstance(dependency, Sequence):
        dependency_tools = tuple(dependency)
    else:
        dependency_tools = () if dependency is None else (dependency,)
    if check_dependencies and not dependency_tools:
        raise ValueError("An explicit dependency check requires at least one dependency tool.")

    graph = StateGraph(_HealthState)
    graph.add_node("check_service", partial(_check_service, service))
    graph.add_node("report", partial(
        _write_report, conditional_log=service_log is not None and not check_service_log,
        conditional_dependencies=() if check_dependencies else dependency_tools,
        conditional_metrics=() if check_metrics else metric_checks,
    ))
    graph.add_edge(START, "check_service")
    graph.add_edge("report", END)
    after_checks = "report"
    if metric_checks:
        graph.add_node("check_metrics", partial(
            _check_metrics, tools=metric_checks, force=check_metrics, budget_seconds=metric_budget_seconds,
        ))
        graph.add_edge("check_metrics", "report")
        after_checks = "check_metrics"
    routes = {"report": after_checks}
    if dependency_tools:
        graph.add_node("check_dependencies", partial(_check_dependencies, dependency_tools))
        graph.add_edge("check_dependencies", after_checks)
        routes["check_dependencies"] = "check_dependencies"
    if service_log is not None:
        read_log = service_log if callable(service_log) else service_log.inspect_recent_errors
        graph.add_node("check_service_log", partial(_check_service_log, read_log))
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
