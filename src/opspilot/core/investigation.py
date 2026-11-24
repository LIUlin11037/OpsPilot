"""Follow evidence across configured sources with a shared step and time budget."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from opspilot.core.diagnosis import (
    DiagnosisReport,
    Evidence,
    InvestigationBudget,
    InvestigationDecision,
    InvestigationStopReason,
    ToolCall,
    ToolName,
    UnavailableEvidence,
    measure_tool_call,
    suggest_next_checks,
    tool_call_from_evidence,
)


@dataclass(frozen=True)
class InvestigationTool:
    source: str
    read: Callable[[float], Evidence]


class _FailedRead(UnavailableEvidence):
    reason: str


class _State(TypedDict, total=False):
    started_at: float
    decision: InvestigationDecision
    decisions: list[InvestigationDecision]
    evidence: list[Evidence]
    tool_calls: list[ToolCall]
    report: DiagnosisReport


def unavailable_read(tool: ToolName, source: str, reason: str, *, timed_out: bool = False) -> _FailedRead:
    return {
        "tool": tool, "source": source, "observed_at": datetime.now(timezone.utc).isoformat(),
        "outcome": "timeout" if timed_out else "missing", "reason": reason,
    }


def _next_read(state: _State, tools: dict[ToolName, InvestigationTool]) -> InvestigationDecision | None:
    evidence = state["evidence"]
    if not evidence:
        return {"tool": "inspect_service_health", "reason": "configured_service", "evidence_indexes": []}
    health = evidence[0].get("health")
    if health == "healthy":
        return None
    if len(evidence) == 1:
        reason = "service_unhealthy" if health == "unhealthy" else "service_health_unknown"
        return {"tool": "inspect_queue", "reason": reason, "evidence_indexes": [0]}

    queue = evidence[1]
    if "inspect_consumer_endpoint" in tools and len(evidence) == 2:
        if (queue.get("pending_messages") or 0) > 0 or (queue.get("unacknowledged_messages") or 0) > 0:
            return {"tool": "inspect_consumer_endpoint", "reason": "queue_activity", "evidence_indexes": [1]}
        if queue["outcome"] != "observed":
            return {"tool": "inspect_consumer_endpoint", "reason": "queue_read_unavailable", "evidence_indexes": [1]}
    if "inspect_processing" in tools and evidence[-1]["tool"] != "inspect_processing":
        return {"tool": "inspect_processing", "reason": "exact_message_after_service_issue", "evidence_indexes": [0]}
    return None


def _conflicting_evidence(evidence: list[Evidence]) -> list[int]:
    if len(evidence) < 3 or evidence[2]["tool"] != "inspect_consumer_endpoint":
        return []
    queue, endpoint = evidence[1:3]
    consumers = queue.get("registered_consumers")
    health = endpoint.get("health")
    if consumers is not None and health in ("healthy", "unhealthy"):
        if (consumers > 0) != (health == "healthy"):
            return [1, 2]
    return []


def run_investigation(
    question: str,
    service: InvestigationTool,
    queue: InvestigationTool,
    budget: InvestigationBudget,
    *,
    consumer: InvestigationTool | None = None,
    processing: InvestigationTool | None = None,
    message_id: str | None = None,
) -> DiagnosisReport:
    tools: dict[ToolName, InvestigationTool] = {"inspect_service_health": service, "inspect_queue": queue}
    if consumer is not None:
        tools["inspect_consumer_endpoint"] = consumer
    if processing is not None:
        if not message_id or not message_id.strip():
            raise ValueError("An investigation processing source requires an exact message ID.")
        tools["inspect_processing"] = processing

    def report(
        state: _State, elapsed_ms: float, stop_reason: InvestigationStopReason,
    ) -> DiagnosisReport:
        evidence = state["evidence"]
        healthy = bool(evidence and evidence[0].get("health") == "healthy")
        conflict = _conflicting_evidence(evidence)
        summary = (
            "No failure was observed at the service health endpoint."
            if healthy else "The root cause is undetermined."
        )
        if conflict:
            summary += " Queue registration and consumer endpoint health provide different signals."
        return {
            "question": question, "status": "no_failure_observed" if healthy else "undetermined",
            "conclusion": summary, "processing_correlation": None,
            "evidence": evidence, "tool_calls": state["tool_calls"], "next_checks": suggest_next_checks(evidence),
            "investigation": {
                "budget": budget, "decisions": state["decisions"], "stop_reason": stop_reason,
                "elapsed_ms": elapsed_ms, "conflicting_evidence_indexes": conflict,
            },
        }

    def select(state: _State) -> _State:
        elapsed = perf_counter() - state["started_at"]
        decision = _next_read(state, tools)
        if elapsed >= budget["max_seconds"]:
            return {"report": report(state, elapsed * 1000, "time_budget_exhausted")}
        if decision is None:
            reason = "service_healthy" if state["evidence"][0].get("health") == "healthy" else "checks_complete"
            return {"report": report(state, elapsed * 1000, reason)}
        if len(state["evidence"]) >= budget["max_steps"]:
            return {"report": report(state, elapsed * 1000, "step_budget_exhausted")}
        return {"decision": decision}

    def read(state: _State) -> _State:
        elapsed = perf_counter() - state["started_at"]
        remaining = budget["max_seconds"] - elapsed
        if remaining <= 0:
            return {"report": report(state, elapsed * 1000, "time_budget_exhausted")}
        decision = state["decision"]
        name = decision["tool"]
        tool = tools[name]

        def observe() -> Evidence:
            try:
                result = tool.read(remaining)
            except TimeoutError:
                return unavailable_read(name, tool.source, "request_timeout", timed_out=True)
            except OSError:
                return unavailable_read(name, tool.source, "transport_error")
            except ValueError:
                return unavailable_read(name, tool.source, "invalid_request")
            if name == "inspect_processing" and "result" in result and result.get("message_id") != message_id:
                return unavailable_read(name, tool.source, "message_id_mismatch")
            return result

        evidence, duration = measure_tool_call(observe)
        return {
            "evidence": [*state["evidence"], evidence],
            "decisions": [*state["decisions"], decision],
            "tool_calls": [*state["tool_calls"], tool_call_from_evidence(evidence, duration)],
        }

    def after_select(state: _State) -> Literal["read", "__end__"]:
        return END if "report" in state else "read"

    def after_read(state: _State) -> Literal["select", "__end__"]:
        return END if "report" in state else "select"

    graph = StateGraph(_State)
    graph.add_node("select", select)
    graph.add_node("read", read)
    graph.add_edge(START, "select")
    graph.add_conditional_edges("select", after_select)
    graph.add_conditional_edges("read", after_read)
    compiled = graph.compile()
    return compiled.invoke({"started_at": perf_counter(), "decisions": [], "evidence": [], "tool_calls": []})["report"]
