"""Fetch bounded, read-only Prometheus range results for configured metrics."""

import json
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from math import isfinite
from time import monotonic
from urllib.parse import quote, urlencode, urlsplit

from opspilot.core.incident_window import IncidentWindow, METRIC_RATE_SECONDS
from opspilot.core.metrics import MetricRead
from opspilot.prometheus_config import MetricQuery, PrometheusConfig

_MAX_RESPONSE_BYTES = 256 * 1024
# Both queries aggregate to one series. Ask for two to detect extra series.
_SERIES_LIMIT = 2
_ACCESS_REASONS = {401: "authentication_failed", 403: "access_denied"}


def _expression(service: str, query: MetricQuery) -> str:
    scope = f'job="{service}"'
    period = f"{METRIC_RATE_SECONDS // 60}m"
    if query.name == "error_rate":
        total = f"sum(rate({query.metric}{{{scope}}}[{period}]))"
        errors = f'sum(rate({query.metric}{{{scope},status=~"5.."}}[{period}]))'
        return f"(({errors} or (0 * {total})) / {total}) and on() ({total} > 0)"
    buckets = f"sum by (le) (rate({query.metric}{{{scope}}}[{period}]))"
    count = f'sum(rate({query.metric}{{{scope},le="+Inf"}}[{period}]))'
    return f"histogram_quantile(0.95, {buckets}) and on() ({count} > 0)"


def _timestamp(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


class _QueryFailure(Exception):
    def __init__(self, reason: str, status: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status = status


@dataclass(frozen=True)
class PrometheusConnector:
    config: PrometheusConfig
    token: str | None = field(default=None, repr=False)

    def _query_range(self, parameters: dict[str, str | int], timeout: float) -> object:
        url = urlsplit(self.config.endpoint)
        connection_type = HTTPSConnection if url.scheme == "https" else HTTPConnection
        path = quote(url.path, safe="/%:@!$&'()*+,;=") + "?" + urlencode(parameters)
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        deadline = monotonic() + timeout
        with closing(connection_type(url.hostname, url.port, timeout=timeout)) as connection:
            connection.request("GET", path, headers=headers)
            sock = connection.sock
            with connection.getresponse() as response:
                if response.status in _ACCESS_REASONS:
                    raise _QueryFailure(_ACCESS_REASONS[response.status], response.status)
                if response.status not in (200, 400, 422, 503):
                    raise _QueryFailure("query_failed", response.status)
                if response.length is not None and response.length > _MAX_RESPONSE_BYTES:
                    raise _QueryFailure("query_limit_exceeded")
                body = bytearray()
                while not response.isclosed() and len(body) <= _MAX_RESPONSE_BYTES:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError
                    if sock is not None:
                        sock.settimeout(remaining)
                    chunk = response.read1(min(16384, _MAX_RESPONSE_BYTES + 1 - len(body)))
                    if not chunk:
                        break
                    body.extend(chunk)
                if len(body) > _MAX_RESPONSE_BYTES:
                    raise _QueryFailure("query_limit_exceeded")
                if response.length:
                    raise _QueryFailure("invalid_response", response.status)
                try:
                    payload = json.loads(body)
                except (UnicodeError, ValueError, RecursionError) as error:
                    raise _QueryFailure("invalid_response", response.status) from error
                if response.status != 200 or (isinstance(payload, dict) and payload.get("status") == "error"):
                    reason = (
                        "query_timeout" if isinstance(payload, dict) and payload.get("errorType") == "timeout"
                        else "query_failed"
                    )
                    raise _QueryFailure(reason, response.status)
                return payload

    def inspect(
        self, query: MetricQuery, reference_time: str, timeout: float | None = None,
        *, window: IncidentWindow | None = None,
    ) -> MetricRead:
        config = self.config
        if window is None:
            end = int(datetime.fromisoformat(reference_time).timestamp())
            start = window_start = end - config.window_seconds
            step = config.step_seconds
        else:
            end = int(window.end.timestamp())
            start = int(window.evaluation_start.timestamp())
            window_start = int(window.start.timestamp())
            step = window.metric_step(config.step_seconds)
        expression = _expression(config.service, query)
        timeout = min(config.timeout_seconds, timeout) if timeout is not None else config.timeout_seconds
        result: MetricRead = {
            "source": config.endpoint, "observed_at": reference_time, "service": config.service,
            "query_name": query.name, "expression": expression, "unit": query.unit, "threshold": query.threshold,
            "window_start": _timestamp(window_start), "window_end": _timestamp(end), "step_seconds": step,
            "outcome": "missing",
        }
        if window is not None:
            result["evaluation_start"] = _timestamp(start)
        params = {
            "query": expression, "start": start, "end": end, "step": step,
            "timeout": f"{max(1, int(timeout * 1000))}ms", "limit": _SERIES_LIMIT,
        }
        try:
            if timeout <= 0:
                raise _QueryFailure("time_budget_exhausted")
            payload = self._query_range(params, timeout)
            samples = _samples(payload, start, end, step, query.unit)
            result["sample_count"] = len(samples)
            if not samples:
                raise _QueryFailure("no_data")
            latest_time, latest_value = samples[-1]
            result["latest_sample_at"] = _timestamp(latest_time)
            if latest_time < end - 2 * step:
                raise _QueryFailure("stale_samples")
            result.update(outcome="observed", latest_value=latest_value, peak_value=max(value for _, value in samples))
        except TimeoutError:
            result.update(outcome="timeout", reason="request_timeout")
        except _QueryFailure as error:
            if error.reason in ("query_timeout", "time_budget_exhausted"):
                result["outcome"] = "timeout"
            result["reason"] = error.reason
            if error.status is not None:
                result["http_status"] = error.status
        except (OSError, HTTPException):
            result["reason"] = "transport_error"
        except ValueError:
            result["reason"] = "invalid_request"
        result["observed_at"] = datetime.now(timezone.utc).isoformat()
        return result


def _samples(payload: object, start: int, end: int, step: int, unit: str) -> list[tuple[float, float]]:
    if not isinstance(payload, dict) or payload.get("status") != "success":
        raise _QueryFailure("invalid_response")
    if payload.get("warnings"):
        raise _QueryFailure("partial_response")
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "matrix" or not isinstance(data.get("result"), list):
        raise _QueryFailure("invalid_response")
    series = data["result"]
    if not series:
        return []
    if len(series) > 1:
        raise _QueryFailure("query_limit_exceeded")
    item = series[0]
    if not isinstance(item, dict) or item.get("histograms") or not isinstance(item.get("values"), list):
        raise _QueryFailure("invalid_response")
    if len(item["values"]) > (end - start) // step + 1:
        raise _QueryFailure("query_limit_exceeded")
    samples: list[tuple[float, float]] = []
    previous = start - 1
    for sample in item["values"]:
        if (
            not isinstance(sample, list) or len(sample) != 2 or type(sample[0]) not in (int, float)
            or not max(start, previous) <= sample[0] <= end or sample[0] == previous
            or not isinstance(sample[1], str)
        ):
            raise _QueryFailure("invalid_response")
        timestamp, raw_value = sample
        previous = timestamp
        try:
            value = float(raw_value)
        except ValueError as error:
            raise _QueryFailure("invalid_response") from error
        if raw_value == "NaN":
            continue
        if not isfinite(value) or value < 0 or (unit == "ratio" and value > 1):
            raise _QueryFailure("invalid_response")
        samples.append((timestamp, value))
    return samples
