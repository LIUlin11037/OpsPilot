"""Validate and correlate projected HireHub observations."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from math import inf
from pathlib import Path
from typing import Any, get_args

from opspilot.connectors.mysql import MySQLFailureReason
from opspilot.connectors.rabbitmq import FailureReason as RabbitMQFailureReason
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
_MYSQL_FAILURE_REASONS = get_args(MySQLFailureReason)
_RABBITMQ_FAILURE_REASONS = get_args(RabbitMQFailureReason)


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
    file_source: str | None
    captured_at: str

    def section(self, name: str) -> tuple[TraceEvidence, dict[str, Any]]:
        section = self.data.get(name)
        origin = _ORIGINS[name]
        if self.file_source is not None:
            source = f"{self.file_source}#{name}"
        elif name == "queue":
            source = section.get("source", origin) if isinstance(section, dict) else origin
        else:
            source = f"MySQL/{origin}"
        evidence: TraceEvidence = {
            "tool": "inspect_target_trace",
            "checkpoint": name,
            "origin": origin,
            "source": source,
            "observed_at": self.captured_at,
            "outcome": "missing",
        }
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
        duration_ms = section.get("duration_ms")
        if type(duration_ms) in (int, float) and 0 <= duration_ms < inf:
            evidence["duration_ms"] = duration_ms
        if outcome != "observed":
            timed_out = outcome == "timeout"
            allowed = _RABBITMQ_FAILURE_REASONS if name == "queue" else _MYSQL_FAILURE_REASONS
            reason = section.get("reason")
            if (
                isinstance(reason, str)
                and reason in allowed
                and (reason == "request_timeout") == timed_out
            ):
                evidence["reason"] = reason
            else:
                evidence["reason"] = "source_timeout" if timed_out else "source_unavailable"
            if name == "queue":
                status = _nonnegative_count(section.get("http_status"))
                if status is not None and 100 <= status <= 599:
                    evidence["http_status"] = status
            else:
                code = _nonnegative_count(section.get("mysql_error_code"))
                if code is not None and 0 < code <= 65535:
                    evidence["mysql_error_code"] = code
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
        observed = evidence["outcome"] == "observed"
        if section.get("name") != NOTIFICATION_QUEUE:
            if observed:
                _mark_missing(evidence, "queue_mismatch")
            return evidence
        partial_response = (
            evidence["outcome"] == "missing"
            and evidence.get("http_status") == 200
            and evidence.get("reason") in ("metric_missing", "invalid_response")
        )
        if not observed and not partial_response:
            return evidence

        unacknowledged = _nonnegative_count(section.get("unacknowledged_messages"))
        registered = _nonnegative_count(section.get("registered_consumers"))
        if observed or unacknowledged is not None:
            evidence["unacknowledged_messages"] = unacknowledged
        if observed or registered is not None:
            evidence["registered_consumers"] = registered
        if observed:
            pending = _nonnegative_count(section.get("pending_messages"))
            evidence["pending_messages"] = pending
            if pending is None and unacknowledged is None and registered is None:
                _mark_missing(evidence, "metrics_missing")
        return evidence


@dataclass(frozen=True)
class HireHubSnapshotAdapter:
    path: Path
    branches = tuple(CONSUMERS)

    def inspect_trace(self, entity_id: int) -> TargetTraceRead:
        source = self.path.absolute().as_uri()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except OSError:
            return self._unavailable("snapshot_unreadable", source)
        except (UnicodeError, json.JSONDecodeError):
            return self._unavailable("invalid_snapshot", source)
        return self.inspect_data(data, entity_id, file_source=source)

    @staticmethod
    def _unavailable(reason: str, file_source: str | None) -> TargetTraceRead:
        return {
            "outcome": "missing",
            "environment": None,
            "vhost": None,
            "message_id": None,
            "checks": [
                {
                    "tool": "inspect_target_trace",
                    "checkpoint": "snapshot",
                    "origin": "snapshot_file" if file_source else "hirehub_capture",
                    "source": file_source or "HireHub live capture",
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "outcome": "missing",
                    "reason": reason,
                }
            ],
        }

    @classmethod
    def inspect_data(
        cls, data: object, entity_id: int, *, file_source: str | None = None
    ) -> TargetTraceRead:
        if not isinstance(data, dict):
            return cls._unavailable("invalid_snapshot", file_source)

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
            return cls._unavailable("invalid_snapshot", file_source)
        if snapshot_id != entity_id:
            return cls._unavailable("delivery_id_mismatch", file_source)

        snapshot = _Snapshot(data, file_source, captured_at)
        trace = cls._correlate(snapshot, entity_id, environment, vhost)
        if "queue" in data or file_source is None or trace["message_id"] is not None:
            trace["checks"].append(snapshot.queue())
        if file_source is None and not any(check["outcome"] == "observed" for check in trace["checks"]):
            trace["outcome"] = trace["checks"][0]["outcome"]
        return trace

    @classmethod
    def _correlate(
        cls, snapshot: _Snapshot, entity_id: int, environment: str, vhost: str
    ) -> TargetTraceRead:
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

        for branch in cls.branches:
            evidence, rows = snapshot.rows(branch, message_id=message_id)
            if len(rows) == 1:
                recorded_at = _timestamp(rows[0].get("recorded_at"))
                if recorded_at is not None:
                    evidence["recorded_at"] = recorded_at
            trace["checks"].append(evidence)
        return trace
