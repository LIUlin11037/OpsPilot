"""Read one trace from Jaeger's public v3 HTTP query interface."""

import json
import re
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from ipaddress import ip_address
from time import monotonic
from typing import Literal, cast
from urllib.parse import quote, urlsplit

from opspilot.core.trace_evidence import RequestTraceRead, TraceSpan, normalize_trace_id
from opspilot.jaeger_config import MAX_RESPONSE_BYTES, MAX_SPANS, JaegerConfig

_SPAN_ID = re.compile(r"[0-9a-fA-F]{16}")
_KIND_NAMES = ("unspecified", "internal", "server", "client", "producer", "consumer")
_HTTP_ERRORS = {401: "authentication_failed", 403: "access_denied", 408: "request_timeout", 504: "request_timeout"}
_GRPC_ERRORS = {4: "request_timeout", 5: "trace_not_found", 7: "access_denied", 16: "authentication_failed"}


class _ReadFailure(Exception):
    """A stable reason for a failed or incomplete read."""


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _span_id(value: object) -> str | None:
    if isinstance(value, str) and _SPAN_ID.fullmatch(value) and int(value, 16):
        return value.lower()
    return None


def _nanos(value: object) -> int | None:
    if isinstance(value, str) and value.isascii() and value.isdecimal() and len(value) <= 20:
        value = int(value)
    return value if type(value) is int and 0 < value < 2**64 else None


def _status(value: object) -> Literal["ok", "error", "unset", "unknown"]:
    code = value.get("code", 0) if isinstance(value, dict) else None
    if type(code) is int and code in (0, 1, 2):
        return ("unset", "ok", "error")[code]
    if isinstance(code, str):
        return {"STATUS_CODE_UNSET": "unset", "STATUS_CODE_OK": "ok", "STATUS_CODE_ERROR": "error"}.get(code, "unknown")
    return "unknown"


def _kind(value: object) -> str:
    if type(value) is int and 0 <= value < len(_KIND_NAMES):
        return _KIND_NAMES[value]
    if isinstance(value, str) and value.startswith("SPAN_KIND_"):
        name = value.removeprefix("SPAN_KIND_").lower()
        if name in _KIND_NAMES:
            return name
    return "unspecified"


def _service(resource: object) -> str | None:
    attributes = resource.get("attributes") if isinstance(resource, dict) else None
    if isinstance(attributes, list):
        names = {
            _text(item["value"].get("stringValue")) for item in attributes
            if isinstance(item, dict) and item.get("key") == "service.name" and isinstance(item.get("value"), dict)
        }
        if len(names) == 1:
            return names.pop()
    return None


def _array(container: object, key: str) -> list:
    if not isinstance(container, dict) or not isinstance(container.get(key, []), list):
        raise _ReadFailure("invalid_response")
    return container.get(key, [])


def _span_rows(frame: object) -> Iterator[tuple[str | None, object]]:
    if not isinstance(frame, dict):
        raise _ReadFailure("invalid_response")
    error = frame.get("error")
    if isinstance(error, dict):
        http = error.get("httpCode")
        grpc = error.get("grpcCode", error.get("code"))
        if type(http) is int and http == 404:
            raise _ReadFailure("trace_not_found")
        if type(http) is int and http in _HTTP_ERRORS:
            raise _ReadFailure(_HTTP_ERRORS[http])
        raise _ReadFailure(_GRPC_ERRORS.get(grpc, "query_failed") if type(grpc) is int else "query_failed")
    for resource in _array(frame.get("result"), "resourceSpans"):
        scopes = _array(resource, "scopeSpans")
        service = _service(resource.get("resource"))
        for scope in scopes:
            for span in _array(scope, "spans"):
                yield service, span


def _span_evidence(raw: object, service: str | None, trace_id: str, gaps: set[str]) -> TraceSpan | None:
    if not isinstance(raw, dict) or not isinstance(raw.get("traceId"), str):
        gaps.add("invalid_span")
        return None
    try:
        candidate = normalize_trace_id(raw["traceId"])
    except ValueError:
        gaps.add("invalid_span")
        return None
    if candidate != trace_id:
        gaps.add("foreign_trace_id")
        return None
    identifier = _span_id(raw.get("spanId"))
    if identifier is None:
        gaps.add("invalid_span")
        return None
    parent_value = raw.get("parentSpanId")
    parent = _span_id(parent_value)
    if parent is None and parent_value not in (None, ""):
        gaps.add("invalid_parent_span_id")
    start = _nanos(raw.get("startTimeUnixNano"))
    end = _nanos(raw.get("endTimeUnixNano"))
    if start is not None and end is not None and end < start:
        gaps.add("invalid_span_time")
    return {
        "trace_id": trace_id, "span_id": identifier, "parent_span_id": parent, "service": service,
        "operation": _text(raw.get("name")), "kind": _kind(raw.get("kind")), "status": _status(raw.get("status", {})),
        "start_time_unix_nano": start, "end_time_unix_nano": end,
    }


def _parse_spans(body: bytearray, trace_id: str, spans: list[TraceSpan], gaps: set[str], *, partial: bool) -> None:
    lines = body.split(b"\n")
    if partial:
        # A read error leaves the final line's completeness unknown.
        lines.pop()
    count = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except (ValueError, RecursionError) as error:
            raise _ReadFailure("invalid_response") from error
        for service, raw in _span_rows(payload):
            count += 1
            if count > MAX_SPANS:
                raise _ReadFailure("span_limit")
            span = _span_evidence(raw, service, trace_id, gaps)
            if span is not None:
                spans.append(span)


def _loopback(host: str) -> bool:
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


@dataclass(frozen=True)
class JaegerConnector:
    config: JaegerConfig
    token: str | None = field(default=None, repr=False)

    def endpoint(self, trace_id: str) -> str:
        return f"{self.config.base_url}/api/v3/traces/{normalize_trace_id(trace_id)}"

    def _get(self, endpoint: str, body: bytearray) -> int:
        url = urlsplit(endpoint)
        host = cast(str, url.hostname)
        connection_type = HTTPSConnection if url.scheme == "https" else HTTPConnection
        headers = {"Accept": "application/json"}
        if self.token:
            if url.scheme != "https" and not _loopback(host):
                raise _ReadFailure("insecure_auth_endpoint")
            headers["Authorization"] = f"Bearer {self.token}"
        timeout = self.config.timeout_seconds
        deadline = monotonic() + timeout
        path = quote(url.path, safe="/%:@!$&'()*+,;=")
        with closing(connection_type(host, url.port, timeout=timeout)) as connection:
            connection.request("GET", path, headers=headers)
            sock = connection.sock
            if sock is not None:
                sock.settimeout(max(0.001, deadline - monotonic()))
            with connection.getresponse() as response:
                if response.status not in (200, 404):
                    return response.status
                if response.length is not None and response.length > MAX_RESPONSE_BYTES:
                    raise _ReadFailure("response_limit")
                while not response.isclosed():
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError
                    if sock is not None:
                        sock.settimeout(remaining)
                    chunk = response.read1(min(16384, MAX_RESPONSE_BYTES + 1 - len(body)))
                    if not chunk:
                        break
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        del body[MAX_RESPONSE_BYTES:]
                        raise _ReadFailure("response_limit")
                if response.length:
                    raise _ReadFailure("invalid_response")
                return response.status

    def inspect_trace(self, trace_id: str) -> RequestTraceRead:
        trace_id = normalize_trace_id(trace_id)
        endpoint = self.endpoint(trace_id)
        body = bytearray()
        spans: list[TraceSpan] = []
        gaps: set[str] = set()
        reason = None
        status = None
        try:
            status = self._get(endpoint, body)
        except TimeoutError:
            reason = "request_timeout"
        except (OSError, HTTPException):
            reason = "transport_error"
        except _ReadFailure as error:
            reason = str(error)
        except ValueError:
            reason = "invalid_request"

        if status is not None and status not in (200, 404):
            reason = _HTTP_ERRORS.get(status, "unexpected_http_status")
        else:
            try:
                _parse_spans(body, trace_id, spans, gaps, partial=reason is not None)
            except _ReadFailure as error:
                if reason is None:
                    reason = str(error)
        if status == 404 and reason != "trace_not_found":
            reason = "unexpected_http_status"
            spans.clear()
        if not spans and reason is None:
            reason = "trace_not_found" if body.strip() and not gaps else "invalid_response"
        if spans and reason == "trace_not_found":
            reason = "invalid_response"

        outcome: Literal["observed", "missing", "timeout"] = "observed"
        if reason == "request_timeout":
            outcome = "timeout"
        elif reason is not None and reason != "trace_not_found":
            outcome = "missing"
        read: RequestTraceRead = {
            "source": endpoint, "observed_at": datetime.now(timezone.utc).isoformat(), "outcome": outcome,
            "spans": spans, "gaps": sorted(gaps),
        }
        if reason is not None:
            read["reason"] = reason
        return read
