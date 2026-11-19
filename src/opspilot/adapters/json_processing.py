"""Read the most recent processing record exported by a target."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from opspilot.core.diagnosis import ProcessingEvidence, UnavailableEvidence

RecordFailure = Literal[
    "record_not_found", "record_unreadable", "invalid_record", "target_mismatch"
]


class JsonProcessingEvidence(ProcessingEvidence):
    queue: str
    vhost: str
    attempted_at: str


class UnavailableRecordEvidence(UnavailableEvidence):
    reason: RecordFailure


@dataclass(frozen=True)
class JsonProcessingAdapter:
    path: Path
    queue: str
    vhost: str

    def inspect_processing(self) -> JsonProcessingEvidence | UnavailableRecordEvidence:
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
            record = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except FileNotFoundError:
            return unavailable("record_not_found")
        except OSError:
            return unavailable("record_unreadable")
        except (UnicodeError, json.JSONDecodeError):
            return unavailable("invalid_record")

        if not isinstance(record, dict):
            return unavailable("invalid_record")
        queue = record.get("queue")
        vhost = record.get("vhost")
        if (
            not isinstance(queue, str)
            or not queue
            or not isinstance(vhost, str)
            or not vhost
        ):
            return unavailable("invalid_record")
        if queue != self.queue or vhost != self.vhost:
            return unavailable("target_mismatch")

        result = record.get("result")
        message_id = record.get("message_id")
        timestamp = record.get("attempted_at")
        if (
            result not in ("succeeded", "failed")
            or not isinstance(message_id, str)
            or not message_id.strip()
            or not isinstance(timestamp, str)
        ):
            return unavailable("invalid_record")

        if timestamp.endswith("Z"):
            timestamp = f"{timestamp[:-1]}+00:00"
        try:
            attempted_at = datetime.fromisoformat(timestamp)
        except ValueError:
            return unavailable("invalid_record")
        if attempted_at.utcoffset() is None:
            return unavailable("invalid_record")

        return {
            "tool": "inspect_processing",
            "source": source,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "observed",
            "result": result,
            "message_id": message_id,
            "queue": queue,
            "vhost": vhost,
            "attempted_at": attempted_at.isoformat(),
        }
