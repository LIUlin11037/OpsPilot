"""Read processing records exported by a target."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from opspilot.core.diagnosis import (
    ProcessingEvidence,
    ProcessingResult,
    UnavailableEvidence,
)

RecordFailure = Literal[
    "record_not_found",
    "record_unreadable",
    "invalid_record",
    "target_mismatch",
    "message_not_found",
]


class JsonProcessingEvidence(ProcessingEvidence):
    queue: str
    vhost: str
    attempted_at: str


class UnavailableRecordEvidence(UnavailableEvidence):
    reason: RecordFailure


@dataclass(frozen=True)
class _ProcessingRecord:
    queue: str
    vhost: str
    message_id: str
    result: ProcessingResult
    attempted_at: datetime


def _parse_record(raw: object) -> _ProcessingRecord | None:
    if not isinstance(raw, dict):
        return None

    queue = raw.get("queue")
    vhost = raw.get("vhost")
    message_id = raw.get("message_id")
    result = raw.get("result")
    timestamp = raw.get("attempted_at")
    if (
        not isinstance(queue, str)
        or not queue
        or not isinstance(vhost, str)
        or not vhost
        or not isinstance(message_id, str)
        or not message_id.strip()
        or result not in ("succeeded", "failed")
        or not isinstance(timestamp, str)
    ):
        return None

    if timestamp.endswith("Z"):
        timestamp = f"{timestamp[:-1]}+00:00"
    try:
        attempted_at = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if attempted_at.utcoffset() is None:
        return None
    return _ProcessingRecord(queue, vhost, message_id, result, attempted_at)


@dataclass(frozen=True)
class JsonProcessingAdapter:
    path: Path
    queue: str
    vhost: str

    def inspect_processing(
        self, *, message_id: str | None = None
    ) -> JsonProcessingEvidence | UnavailableRecordEvidence:
        source = self.path.absolute().as_uri()

        def unavailable(reason: RecordFailure) -> UnavailableRecordEvidence:
            return {
                "tool": "inspect_processing",
                "source": source,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "outcome": "missing",
                "reason": reason,
            }

        try:
            payload = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except FileNotFoundError:
            return unavailable("record_not_found")
        except OSError:
            return unavailable("record_unreadable")
        except (UnicodeError, json.JSONDecodeError):
            return unavailable("invalid_record")

        entries = payload if isinstance(payload, list) else [payload]
        scoped: list[_ProcessingRecord] = []
        for entry in entries:
            parsed = _parse_record(entry)
            if parsed is None:
                return unavailable("invalid_record")
            if parsed.queue == self.queue and parsed.vhost == self.vhost:
                scoped.append(parsed)

        if entries and not scoped:
            return unavailable("target_mismatch")
        matches = scoped
        if message_id is not None:
            matches = [record for record in scoped if record.message_id == message_id]
        if not matches:
            reason = "record_not_found" if message_id is None else "message_not_found"
            return unavailable(reason)

        latest = max(reversed(matches), key=lambda record: record.attempted_at)
        return {
            "tool": "inspect_processing",
            "source": source,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "observed",
            "result": latest.result,
            "message_id": latest.message_id,
            "queue": latest.queue,
            "vhost": latest.vhost,
            "attempted_at": latest.attempted_at.isoformat(),
        }
