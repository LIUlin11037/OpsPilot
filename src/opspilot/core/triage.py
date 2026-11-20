"""Select a supported diagnostic workflow from explicit symptom terms."""

import re
from unicodedata import normalize

from opspilot.core.diagnosis import DiagnosisRoute, DiagnosisWorkflow

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


def select_diagnosis_route(question: str) -> DiagnosisRoute:
    description = normalize("NFKC", question)
    matches = {
        workflow: list(dict.fromkeys(match.group().casefold() for match in pattern.finditer(description)))
        for workflow, pattern in _SYMPTOMS.items()
    }
    routes = [workflow for workflow, terms in matches.items() if terms]
    if not routes:
        raise ValueError("Describe a queue symptom or an HTTP health symptom, or choose an explicit command.")
    if len(routes) > 1:
        raise ValueError("Both queue and HTTP symptoms matched. Focus on one symptom or choose an explicit command.")
    workflow, = routes
    return {"workflow": workflow, "matched_terms": matches[workflow]}
