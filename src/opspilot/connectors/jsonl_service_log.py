"""Read counts from a bounded local JSON Lines service log export."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from opspilot.core.diagnosis import ServiceLogRead

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


def _count_errors(content: bytes, start: datetime, end: datetime) -> int:
    errors = 0
    for line in content.decode("utf-8-sig").split("\n"):
        if not line.strip():
            continue
        entry = json.loads(line)
        if not isinstance(entry, dict):
            raise ValueError("Expected a JSON Lines object.")
        stamp = _timestamp(entry.get("timestamp"))
        level = entry.get("level")
        if not isinstance(level, str) or not level.strip():
            raise ValueError("Expected a nonempty log level.")
        if start <= stamp <= end and level.strip().upper() in _ERROR_LEVELS:
            errors += 1
    return errors


@dataclass(frozen=True)
class JsonlServiceLogConnector:
    path: Path

    def inspect_recent_errors(self, reference_time: str) -> ServiceLogRead:
        end = _timestamp(reference_time)
        start = end - _LOOKBACK
        result: ServiceLogRead = {
            "source": self.path.absolute().as_uri(),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "outcome": "missing",
        }
        try:
            with self.path.open("rb") as file:
                content = file.read(_MAX_LOG_BYTES + 1)
            if len(content) > _MAX_LOG_BYTES:
                result["reason"] = "log_too_large"
                return result
            result["error_count"] = _count_errors(content, start, end)
        except FileNotFoundError:
            result["reason"] = "log_not_found"
        except OSError:
            result["reason"] = "log_unreadable"
        except (ValueError, OverflowError, RecursionError):
            result["reason"] = "invalid_log_record"
        else:
            result["outcome"] = "observed"
        return result
