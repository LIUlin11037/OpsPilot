"""Read counts from a bounded local JSON Lines service log export."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from opspilot.core.diagnosis import ServiceLogRead
from opspilot.core.incident_window import IncidentWindow

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
    content: bytes, start: datetime, end: datetime, service: str | None,
) -> tuple[int, int, int]:
    errors = matched = unscoped = 0
    for line in content.decode("utf-8-sig").split("\n"):
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
        if start <= stamp <= end:
            matched += 1
            if level.strip().upper() in _ERROR_LEVELS:
                errors += 1
    return errors, matched, unscoped


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

    def inspect_window(self, window: IncidentWindow, service: str) -> ScopedServiceLogRead:
        return self._inspect(window.start, window.end, service=service)

    def _inspect(self, start: datetime, end: datetime, *, service: str | None = None) -> ScopedServiceLogRead:
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
            errors, matched, unscoped = _log_counts(content, start, end, service)
            result["error_count"] = errors
            if service is not None:
                result.update(matched_entries=matched, unscoped_entries=unscoped)
        except FileNotFoundError:
            result["reason"] = "log_not_found"
        except OSError:
            result["reason"] = "log_unreadable"
        except (ValueError, OverflowError, RecursionError):
            result["reason"] = "invalid_log_record"
        else:
            result["outcome"] = "observed"
        return result
