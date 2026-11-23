"""Select a workflow from an explicit choice, a message ID, a requested check, or symptoms."""

import re
from typing import Literal
from unicodedata import normalize

from opspilot.core.diagnosis import DiagnosisRoute, DiagnosisWorkflow, RequestedCheck

_SYMPTOMS: dict[DiagnosisWorkflow, re.Pattern[str]] = {
    "queue": re.compile(
        r"队列|积压|待投递|待确认|消费者"
        r"|(?<![a-z0-9_])(?:rabbitmq|queues?|backlog|unacknowledged|consumers?)(?![a-z0-9_])",
        re.IGNORECASE,
    ),
    "http_health": re.compile(
        r"健康检查|健康端点|服务(?:不可用|宕机)|接口(?:异常|不可用)"
        r"|(?:返回|报错|状态码)\s*5(?:[0-9]{2}|xx)(?![a-z0-9_])"
        r"|(?<![a-z0-9_])(?:https?|health(?:[ -]?check)?|5xx)(?![a-z0-9_])",
        re.IGNORECASE,
    ),
}
_CHECK_WORKFLOWS: dict[RequestedCheck, DiagnosisWorkflow] = {
    "check_consumer_health": "queue",
    "check_dependencies": "http_health",
    "check_service_log": "http_health",
}


def select_diagnosis_route(
    question: str, *, message_lookup: bool = False, workflow: Literal["queue", "http_health"] | None = None,
    requested_check: RequestedCheck | None = None,
) -> DiagnosisRoute:
    description = normalize("NFKC", question)
    matches = {
        workflow: list(dict.fromkeys(match.group().casefold() for match in pattern.finditer(description)))
        for workflow, pattern in _SYMPTOMS.items()
    }
    if workflow is not None:
        if message_lookup and workflow != "queue":
            raise ValueError("--message-id requires --workflow queue.")
        return {"workflow": workflow, "matched_terms": matches[workflow], "selected_by": "workflow"}
    if message_lookup:
        if matches["http_health"]:
            raise ValueError(
                "HTTP health symptoms conflict with --message-id. Focus on the message or use --workflow queue."
            )
        return {
            "workflow": "queue", "matched_terms": matches["queue"], "selected_by": "message_id",
        }
    routes = [workflow for workflow, terms in matches.items() if terms]
    if requested_check is not None and len(routes) != 1:
        selected = _CHECK_WORKFLOWS[requested_check]
        return {"workflow": selected, "matched_terms": matches[selected], "selected_by": requested_check}
    if not routes:
        raise ValueError("Describe a queue symptom or an HTTP health symptom, or pass --workflow.")
    if len(routes) > 1:
        raise ValueError("Both queue and HTTP symptoms matched. Focus on one symptom or pass --workflow.")
    selected, = routes
    return {"workflow": selected, "matched_terms": matches[selected]}
