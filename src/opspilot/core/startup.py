"""Minimal LangGraph startup check."""

from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph


class StartupState(TypedDict):
    status: Literal["starting", "ready"]


def mark_ready(_state: StartupState) -> StartupState:
    return {"status": "ready"}


def run_startup() -> StartupState:
    graph = StateGraph(StartupState)
    graph.add_node("mark_ready", mark_ready)
    graph.add_edge(START, "mark_ready")
    graph.add_edge("mark_ready", END)
    return graph.compile().invoke({"status": "starting"})
