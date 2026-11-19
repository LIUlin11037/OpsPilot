"""Read processing records exported by a target."""

import json
from dataclasses import dataclass
from pathlib import Path

from opspilot.adapters.processing_records import (
    ProcessingRecordEvidence,
    UnavailableProcessingEvidence,
    select_processing_evidence,
    unavailable_processing,
)


@dataclass(frozen=True)
class JsonProcessingAdapter:
    path: Path
    queue: str
    vhost: str

    def inspect_processing(
        self, *, message_id: str | None = None
    ) -> ProcessingRecordEvidence | UnavailableProcessingEvidence:
        source = self.path.absolute().as_uri()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except FileNotFoundError:
            return unavailable_processing(source, "record_not_found")
        except OSError:
            return unavailable_processing(source, "record_unreadable")
        except (UnicodeError, json.JSONDecodeError):
            return unavailable_processing(source, "invalid_record")

        return select_processing_evidence(
            payload, source=source, queue=self.queue, vhost=self.vhost, message_id=message_id
        )
