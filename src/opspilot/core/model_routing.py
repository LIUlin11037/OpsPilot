"""Validate a routing suggestion against the application's registered choices."""

import json
from dataclasses import dataclass
from typing import Literal

AdvisableWorkflow = Literal["http_health", "queue"]
MAX_QUESTION_CHARS = 2048
REQUEST_FIELDS = ("question", "candidate_workflows")
PROMPT_VERSION = 1


@dataclass(frozen=True)
class RoutingSuggestion:
    workflow: AdvisableWorkflow | None
    reason: str
    elapsed_ms: float
    input_tokens: int | None = None
    output_tokens: int | None = None


def validate_suggestion(
    content: object, candidates: tuple[AdvisableWorkflow, ...],
) -> tuple[AdvisableWorkflow | None, str]:
    if not isinstance(content, str):
        return None, "invalid_response"
    try:
        value = json.loads(content)
    except (ValueError, RecursionError):
        return None, "invalid_response"
    if not isinstance(value, dict) or value.keys() != {"workflow"}:
        return None, "invalid_response"
    workflow = value["workflow"]
    if workflow is None:
        return None, "abstained"
    if workflow not in candidates:
        return None, "out_of_scope"
    return workflow, "suggested"
