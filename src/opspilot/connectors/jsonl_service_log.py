"""Read counts from a bounded local JSON Lines service log export."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from opspilot.core.diagnosis import ServiceLogRead
from opspilot.core.incident_window import IncidentWindow
from opspilot.core.trace_evidence import (
    MAX_INCIDENT_TRACE_LOOKUPS, TraceLogCandidate, TraceLogSelection, normalize_trace_id,
)

_MAX_LOG_BYTES = 1024 * 1024
_ERROR_LEVELS = {"ERROR", "CRITICAL", "FATAL"}
_LOOKBACK = timedelta(minutes=5)


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected an ISO 8601 timestamp.")
    stamp = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    if stamp.utcoffset() is None:
        raise ValueError("Expected a timestamp with a time zone.")
    return stamp.astimezone(timezone.utc)


def _log_counts(
    content: bytes, start: datetime, end: datetime, service: str | None, *, include_trace_candidates: bool,
) -> tuple[int, int, int, TraceLogSelection | None]:
    errors = matched = unscoped = 0
    selection: TraceLogSelection | None = None
    candidates: dict[str, tuple[datetime, TraceLogCandidate]] = {}
    if include_trace_candidates:
        selection = {"candidates": [], "missing_ids": 0, "invalid_ids": 0, "duplicate_rows": 0, "over_limit": 0}
    for line_number, line in enumerate(content.decode("utf-8-sig").split("\n"), start=1):
        if not line.strip():
            continue
        entry = json.loads(line)
        if not isinstance(entry, dict):
            raise ValueError("Expected a JSON Lines object.")
        entry_service = entry.get("service")
        if service is not None and isinstance(entry_service, str) and entry_service.strip() and entry_service != service:
            continue
        stamp = _timestamp(entry.get("timestamp"))
        if service is not None:
            if not start <= stamp <= end:
                continue
            if entry_service != service:
                unscoped += 1
                continue
        level = entry.get("level")
        if not isinstance(level, str) or not level.strip():
            raise ValueError("Expected a nonempty log level.")
        if not start <= stamp <= end:
            continue
        matched += 1
        level = level.strip().upper()
        if level not in _ERROR_LEVELS:
            continue
        errors += 1
        if selection is None:
            continue
        trace = entry.get("trace_id")
        if trace is None:
            selection["missing_ids"] += 1
            continue
        try:
            if not isinstance(trace, str):
                raise ValueError
            trace_id = normalize_trace_id(trace)
        except ValueError:
            selection["invalid_ids"] += 1
            continue
        previous = candidates.get(trace_id)
        if previous is not None:
            selection["duplicate_rows"] += 1
            if stamp <= previous[0]:
                continue
        candidates[trace_id] = stamp, {
            "trace_id": trace_id, "timestamp": stamp.isoformat(), "line_number": line_number, "level": level,
        }
    if selection is not None:
        ordered = sorted(candidates.values(), key=lambda item: (item[0], -item[1]["line_number"]), reverse=True)
        selection["candidates"] = [item for _, item in ordered[:MAX_INCIDENT_TRACE_LOOKUPS]]
        selection["over_limit"] = len(ordered) - len(selection["candidates"])
    return errors, matched, unscoped, selection


class ScopedServiceLogRead(ServiceLogRead, total=False):
    service: str
    matched_entries: int
    unscoped_entries: int


@dataclass(frozen=True)
class JsonlServiceLogConnector:
    path: Path

    def inspect_recent_errors(self, reference_time: str) -> ServiceLogRead:
        end = _timestamp(reference_time)
        return self._inspect(end - _LOOKBACK, end)

    def inspect_window(
        self, window: IncidentWindow, service: str, *, include_trace_candidates: bool = False,
    ) -> ScopedServiceLogRead:
        return self._inspect(window.start, window.end, service=service, include_trace_candidates=include_trace_candidates)

    def _inspect(
        self, start: datetime, end: datetime, *, service: str | None = None, include_trace_candidates: bool = False,
    ) -> ScopedServiceLogRead:
        result: ScopedServiceLogRead = {
            "source": self.path.absolute().as_uri(),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "outcome": "missing",
        }
        if service is not None:
            result["service"] = service
        try:
            with self.path.open("rb") as file:
                content = file.read(_MAX_LOG_BYTES + 1)
            if len(content) > _MAX_LOG_BYTES:
                result["reason"] = "log_too_large"
                return result
            errors, matched, unscoped, selection = _log_counts(
                content, start, end, service, include_trace_candidates=include_trace_candidates,
            )
            result["error_count"] = errors
            if service is not None:
                result.update(matched_entries=matched, unscoped_entries=unscoped)
            if selection is not None:
                result["trace_candidates"] = selection
        except FileNotFoundError:
            result["reason"] = "log_not_found"
        except OSError:
            result["reason"] = "log_unreadable"
        except (ValueError, OverflowError, RecursionError):
            result["reason"] = "invalid_log_record"
        else:
            result["outcome"] = "observed"
        return result
