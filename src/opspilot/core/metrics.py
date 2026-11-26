"""Read contract for scoped, time-windowed service metrics."""

from typing import Literal, TypedDict


class _MetricReadFields(TypedDict):
    source: str
    observed_at: str
    service: str
    query_name: str
    expression: str
    unit: str
    threshold: float
    window_start: str
    window_end: str
    step_seconds: int
    outcome: Literal["observed", "missing", "timeout"]


class MetricRead(_MetricReadFields, total=False):
    sample_count: int
    latest_sample_at: str
    latest_value: float
    peak_value: float
    reason: str
    http_status: int
