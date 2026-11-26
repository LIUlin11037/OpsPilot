"""Validate the service and query limits of a Prometheus target."""

import re
from dataclasses import dataclass
from math import isfinite
from typing import Literal
from urllib.parse import urlsplit

MetricKind = Literal["error_rate", "latency_p95"]
MetricUnit = Literal["ratio", "seconds"]
_UNITS: dict[MetricKind, MetricUnit] = {"error_rate": "ratio", "latency_p95": "seconds"}
_METRIC_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")


@dataclass(frozen=True)
class MetricQuery:
    name: MetricKind
    metric: str
    unit: MetricUnit
    threshold: float


@dataclass(frozen=True)
class PrometheusConfig:
    base_url: str
    service: str
    queries: tuple[MetricQuery, ...]
    window_seconds: int
    step_seconds: int
    timeout_seconds: int

    @property
    def endpoint(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1/query_range"


def _object(value: object, field: str, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object.")
    unsupported = value.keys() - fields
    if unsupported:
        raise ValueError(f"{field} contains unsupported fields: {', '.join(sorted(unsupported))}.")
    return value


def _bounded_integer(value: object, field: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer between {minimum} and {maximum}.")
    return value


def _query(name: MetricKind, value: object, unit: MetricUnit) -> MetricQuery:
    field = f"prometheus.queries.{name}"
    data = _object(value, field, {"metric", "unit", "threshold"})
    metric = data.get("metric")
    if not isinstance(metric, str) or not _METRIC_NAME.fullmatch(metric):
        raise ValueError(f"{field}.metric must be a Prometheus metric name.")
    if data.get("unit") != unit:
        raise ValueError(f"{field}.unit must be {unit!r}.")
    threshold = data.get("threshold")
    try:
        valid = type(threshold) in (int, float) and isfinite(threshold) and threshold >= 0
    except OverflowError:
        valid = False
    if not valid or (unit == "ratio" and threshold > 1):
        interval = "between 0 and 1" if unit == "ratio" else "nonnegative"
        raise ValueError(f"{field}.threshold must be a finite number {interval}.")
    return MetricQuery(name, metric, unit, float(threshold))


def parse_prometheus_config(value: object) -> PrometheusConfig:
    data = _object(value, "prometheus", {
        "base_url", "service", "queries", "window_seconds", "step_seconds", "timeout_seconds",
    })
    base_url = data.get("base_url")
    if not isinstance(base_url, str) or any(char.isspace() or ord(char) < 32 for char in base_url):
        raise ValueError("prometheus.base_url must be an HTTP(S) URL.")
    try:
        url = urlsplit(base_url)
        valid_url = (
            url.scheme in ("http", "https") and url.hostname and url.port != 0
            and url.username is None and "?" not in base_url and "#" not in base_url
        )
    except ValueError:
        valid_url = False
    if not valid_url:
        raise ValueError("prometheus.base_url must be an HTTP(S) URL without credentials, a query or a fragment.")

    service = data.get("service")
    if not isinstance(service, str) or not re.fullmatch(r"[a-zA-Z0-9_.:/-]{1,128}", service):
        raise ValueError("prometheus.service must be a nonempty Prometheus job label value.")
    window = _bounded_integer(data.get("window_seconds", 300), "prometheus.window_seconds", 15, 900)
    step = _bounded_integer(data.get("step_seconds", 15), "prometheus.step_seconds", 5, window)
    timeout = _bounded_integer(data.get("timeout_seconds", 5), "prometheus.timeout_seconds", 1, 10)
    queries = _object(data.get("queries"), "prometheus.queries", set(_UNITS))
    if not queries:
        raise ValueError("prometheus.queries requires error_rate, latency_p95, or both.")
    return PrometheusConfig(base_url, service, tuple(
        _query(name, queries[name], unit) for name, unit in _UNITS.items() if name in queries
    ), window, step, timeout)
