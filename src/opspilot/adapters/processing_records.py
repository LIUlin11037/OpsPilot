"""Validate and select a target processing record independently of transport."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from opspilot.core.diagnosis import ProcessingEvidence, ProcessingResult, UnavailableEvidence

ProcessingFailure = Literal[
    "record_not_found",
    "record_unreadable",
    "invalid_record",
    "target_mismatch",
    "message_not_found",
    "authentication_failed",
    "access_denied",
    "http_error",
    "request_timeout",
    "transport_error",
]


class ProcessingRecordEvidence(ProcessingEvidence):
    queue: str
    vhost: str
    attempted_at: str


class UnavailableProcessingFields(UnavailableEvidence):
    reason: ProcessingFailure


class UnavailableProcessingEvidence(UnavailableProcessingFields, total=False):
    http_status: int


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


def unavailable_processing(
    source: str, reason: ProcessingFailure, *, http_status: int | None = None
) -> UnavailableProcessingEvidence:
    evidence: UnavailableProcessingEvidence = {
        "tool": "inspect_processing",
        "source": source,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "outcome": "timeout" if reason == "request_timeout" else "missing",
        "reason": reason,
    }
    if http_status is not None:
        evidence["http_status"] = http_status
    return evidence


def select_processing_evidence(
    payload: object,
    *,
    source: str,
    queue: str,
    vhost: str,
    message_id: str | None = None,
) -> ProcessingRecordEvidence | UnavailableProcessingEvidence:
    entries = payload if isinstance(payload, list) else [payload]
    scoped: list[_ProcessingRecord] = []
    for entry in entries:
        parsed = _parse_record(entry)
        if parsed is None:
            return unavailable_processing(source, "invalid_record")
        if parsed.queue == queue and parsed.vhost == vhost:
            scoped.append(parsed)

    if entries and not scoped:
        return unavailable_processing(source, "target_mismatch")
    matches = scoped
    if message_id is not None:
        matches = [record for record in scoped if record.message_id == message_id]
    if not matches:
        reason = "record_not_found" if message_id is None else "message_not_found"
        return unavailable_processing(source, reason)

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
