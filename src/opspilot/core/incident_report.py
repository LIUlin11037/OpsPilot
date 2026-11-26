"""Associate current HTTP observations with evidence from a historical window."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from opspilot.core.incident_window import IncidentWindow

if TYPE_CHECKING:
    from opspilot.core.diagnosis import DiagnosisReport


class IncidentDetails(TypedDict):
    service: str
    window_start: str
    window_end: str
    current_health: Literal["healthy", "unhealthy", "unknown"]
    current_evidence_index: int
    window_status: Literal["anomaly_observed", "no_anomaly_observed", "undetermined"]
    window_evidence_indexes: list[int]
    anomaly_evidence_indexes: list[int]
    unavailable_evidence_indexes: list[int]
    stop_reason: Literal["checks_complete", "sources_incomplete", "time_budget_exhausted"]


def summarize_incident(report: DiagnosisReport, window: IncidentWindow, service: str) -> DiagnosisReport:
    evidence = report["evidence"]
    current_health = evidence[0]["health"]
    window_indexes = [
        index for index, item in enumerate(evidence)
        if item["tool"] in ("inspect_service_log", "inspect_service_metrics")
    ]
    unavailable = [
        index for index, item in enumerate(evidence)
        if item["outcome"] != "observed" or item.get("unscoped_entries", 0) > 0
    ]
    anomalies = [
        index for index in window_indexes if evidence[index]["outcome"] == "observed"
        and (evidence[index].get("error_count", 0) > 0 or evidence[index].get("threshold_exceeded") is True)
    ]
    incomplete_window = any(index in unavailable for index in window_indexes)
    window_status = "undetermined" if incomplete_window else "no_anomaly_observed"
    if anomalies:
        window_status = "anomaly_observed"
    reason = "sources_incomplete" if unavailable else "checks_complete"
    if any(evidence[index].get("reason") == "time_budget_exhausted" for index in unavailable):
        reason = "time_budget_exhausted"
    for index in window_indexes:
        item = evidence[index]
        if item["tool"] == "inspect_service_metrics":
            item["trigger"] = "incident_window"
    report["incident"] = {
        "service": service, "window_start": window.start.isoformat(), "window_end": window.end.isoformat(),
        "current_health": current_health, "current_evidence_index": 0,
        "window_status": window_status, "window_evidence_indexes": window_indexes,
        "anomaly_evidence_indexes": anomalies, "unavailable_evidence_indexes": unavailable,
        "stop_reason": reason,
    }
    summary = f"Current service endpoint health is {current_health}."
    if anomalies:
        summary += f" Scoped logs or metrics show an anomaly for {service} in the incident window."
    elif incomplete_window:
        summary += f" Evidence for {service} in the incident window is incomplete."
    else:
        summary += f" No anomaly was observed for {service} in the scoped incident window."
    if anomalies and incomplete_window:
        summary += " Some historical sources were unavailable or could not be fully attributed."
    summary += " The evidence describes the service. Request-level attribution requires an exact matching identifier."
    report["conclusion"] = summary
    if anomalies or unavailable:
        report["status"] = "undetermined"
    return report
