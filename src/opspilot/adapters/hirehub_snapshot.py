"""Read projected HireHub evidence from a local snapshot."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from opspilot.core.diagnosis import TraceEvidence
from opspilot.core.target_trace import TargetTraceRead

NOTIFICATION_QUEUE = "hirehub.notification.msg"
CONSUMERS = {"job": "job-delivery-count", "notification": "notification"}
_ORIGINS = {
    "delivery": "hirehub_delivery.delivery",
    "outbox": "hirehub_delivery.local_message",
    "job": "hirehub_job.mq_consume_log",
    "notification": "hirehub_notification.mq_consume_log",
    "queue": f"RabbitMQ/{NOTIFICATION_QUEUE}",
}


def _timestamp(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timestamp.utcoffset() is None:
        return None
    return timestamp.isoformat()


def _nonnegative_count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _mark_missing(evidence: TraceEvidence, reason: str) -> None:
    evidence["outcome"] = "missing"
    evidence["reason"] = reason
    evidence.pop("row_count", None)


@dataclass(frozen=True)
class _Snapshot:
    data: dict[str, Any]
    source: str
    captured_at: str

    def section(self, name: str) -> tuple[TraceEvidence, dict[str, Any]]:
        evidence: TraceEvidence = {
            "tool": "inspect_target_trace",
            "checkpoint": name,
            "origin": _ORIGINS[name],
            "source": f"{self.source}#{name}",
            "observed_at": self.captured_at,
            "outcome": "missing",
        }
        section = self.data.get(name)
        if section is None:
            evidence["reason"] = "section_not_captured"
            return evidence, {}
        if not isinstance(section, dict):
            evidence["reason"] = "invalid_section"
            return evidence, {}

        observed_at = _timestamp(section.get("observed_at", self.captured_at))
        outcome = section.get("outcome")
        if observed_at is None or outcome not in ("observed", "timeout", "missing"):
            evidence["reason"] = "invalid_section"
            return evidence, {}

        evidence["observed_at"] = observed_at
        evidence["outcome"] = outcome
        if outcome != "observed":
            evidence["reason"] = (
                "source_timeout" if outcome == "timeout" else "source_unavailable"
            )
        return evidence, section

    def rows(
        self, name: str, *, message_id: str | None = None
    ) -> tuple[TraceEvidence, list[dict[str, Any]]]:
        evidence, section = self.section(name)
        consumer = CONSUMERS.get(name)
        if message_id is not None:
            evidence["branch"] = name
            evidence["message_id"] = message_id
        if consumer is not None:
            evidence["consumer"] = consumer
        if evidence["outcome"] != "observed":
            return evidence, []

        if message_id is not None and section.get("message_id") != message_id:
            _mark_missing(evidence, "message_id_mismatch")
            return evidence, []
        if consumer is not None and section.get("consumer") != consumer:
            _mark_missing(evidence, "consumer_mismatch")
            return evidence, []
        rows = section.get("rows")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            _mark_missing(evidence, "invalid_rows")
            return evidence, []
        if message_id is not None and any(
            row.get("message_id") != message_id for row in rows
        ):
            _mark_missing(evidence, "message_id_mismatch")
            return evidence, []
        if consumer is not None and any(row.get("consumer") != consumer for row in rows):
            _mark_missing(evidence, "consumer_mismatch")
            return evidence, []
        evidence["row_count"] = len(rows)
        return evidence, rows

    def queue(self) -> TraceEvidence:
        evidence, section = self.section("queue")
        if evidence["outcome"] != "observed":
            return evidence
        if section.get("name") != NOTIFICATION_QUEUE:
            _mark_missing(evidence, "queue_mismatch")
            return evidence

        evidence["pending_messages"] = _nonnegative_count(section.get("pending_messages"))
        evidence["unacknowledged_messages"] = _nonnegative_count(
            section.get("unacknowledged_messages")
        )
        evidence["registered_consumers"] = _nonnegative_count(
            section.get("registered_consumers")
        )
        if all(
            evidence.get(key) is None
            for key in ("pending_messages", "unacknowledged_messages", "registered_consumers")
        ):
            _mark_missing(evidence, "metrics_missing")
        return evidence


@dataclass(frozen=True)
class HireHubSnapshotAdapter:
    path: Path
    branches = tuple(CONSUMERS)

    def inspect_trace(self, entity_id: int) -> TargetTraceRead:
        source = self.path.absolute().as_uri()

        def unavailable(reason: str) -> TargetTraceRead:
            return {
                "outcome": "missing",
                "environment": None,
                "vhost": None,
                "message_id": None,
                "checks": [
                    {
                        "tool": "inspect_target_trace",
                        "checkpoint": "snapshot",
                        "origin": "snapshot_file",
                        "source": source,
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                        "outcome": "missing",
                        "reason": reason,
                    }
                ],
            }

        try:
            data = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except OSError:
            return unavailable("snapshot_unreadable")
        except (UnicodeError, json.JSONDecodeError):
            return unavailable("invalid_snapshot")
        if not isinstance(data, dict):
            return unavailable("invalid_snapshot")

        environment = data.get("environment")
        vhost = data.get("vhost")
        captured_at = _timestamp(data.get("captured_at"))
        snapshot_id = data.get("delivery_id")
        if (
            not isinstance(environment, str)
            or not environment
            or not isinstance(vhost, str)
            or not vhost
            or captured_at is None
            or type(snapshot_id) is not int
            or snapshot_id <= 0
        ):
            return unavailable("invalid_snapshot")
        if snapshot_id != entity_id:
            return unavailable("delivery_id_mismatch")

        snapshot = _Snapshot(data, source, captured_at)
        delivery, rows = snapshot.rows("delivery")
        trace: TargetTraceRead = {
            "outcome": "observed",
            "environment": environment,
            "vhost": vhost,
            "message_id": None,
            "checks": [delivery],
        }
        if len(rows) != 1:
            if len(rows) > 1:
                delivery["reason"] = "ambiguous_rows"
            return trace
        status = rows[0].get("status")
        delivery_id = rows[0].get("delivery_id")
        if (
            type(delivery_id) is not int
            or delivery_id != entity_id
            or not isinstance(status, str)
            or not status
        ):
            _mark_missing(delivery, "invalid_delivery")
            return trace
        delivery["status"] = status

        outbox, rows = snapshot.rows("outbox")
        trace["checks"].append(outbox)
        if len(rows) != 1:
            if len(rows) > 1:
                outbox["reason"] = "ambiguous_rows"
            return trace
        event = rows[0]
        message_id = event.get("message_id")
        status = event.get("status")
        delivery_id = event.get("delivery_id")
        if (
            type(delivery_id) is not int
            or delivery_id != entity_id
            or event.get("biz_type") != "DELIVERY_CREATED"
            or event.get("routing_key") != "delivery.created"
            or not isinstance(message_id, str)
            or not message_id.strip()
            or status not in ("PENDING", "SENT", "CONFIRMED", "FAILED")
        ):
            _mark_missing(outbox, "invalid_creation_event")
            return trace
        recipient = event.get("expected_recipient_listed")
        outbox["message_id"] = message_id
        outbox["status"] = status
        outbox["expected_recipient_listed"] = recipient if type(recipient) is bool else None
        for checkpoint_time in ("sent_at", "acknowledged_at"):
            timestamp = _timestamp(event.get(checkpoint_time))
            if timestamp is not None:
                outbox[checkpoint_time] = timestamp
        trace["message_id"] = message_id

        for branch in self.branches:
            evidence, rows = snapshot.rows(branch, message_id=message_id)
            if len(rows) == 1:
                recorded_at = _timestamp(rows[0].get("recorded_at"))
                if recorded_at is not None:
                    evidence["recorded_at"] = recorded_at
            trace["checks"].append(evidence)
        trace["checks"].append(snapshot.queue())
        return trace
