"""Capture the minimal HireHub evidence consumed by the snapshot adapter."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from opspilot.adapters.hirehub_snapshot import CONSUMERS, HireHubSnapshotAdapter
from opspilot.connectors.mysql import MySQLConnector
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.core.diagnosis import measure_tool_call
from opspilot.core.target_trace import TargetTraceRead

_DELIVERY = "SELECT id AS delivery_id, status FROM hirehub_delivery.delivery WHERE id = %s"
_OUTBOX = """
    SELECT biz_id AS delivery_id, biz_type, routing_key, message_id,
           CASE status WHEN 0 THEN 'PENDING' WHEN 1 THEN 'SENT'
                       WHEN 2 THEN 'CONFIRMED' WHEN 3 THEN 'FAILED' END AS status,
           CASE WHEN JSON_VALID(payload)
                THEN JSON_CONTAINS(payload, JSON_ARRAY(%s), '$.receiverIds')
           END AS expected_recipient_listed,
           send_time AS sent_at, ack_time AS acknowledged_at
    FROM hirehub_delivery.local_message
    WHERE biz_id = %s AND biz_type = 'DELIVERY_CREATED' AND routing_key = 'delivery.created'
    LIMIT 2
"""
_CONSUME_LOGS = {
    "job": "hirehub_job.mq_consume_log",
    "notification": "hirehub_notification.mq_consume_log",
}


def _read_mysql_section(
    mysql: MySQLConnector, query: str, parameters: tuple[int | str, ...]
) -> dict[str, Any]:
    read, duration_ms = measure_tool_call(lambda: mysql.select(query, parameters))
    section: dict[str, Any] = {
        "outcome": read.outcome,
        "observed_at": read.observed_at,
        "duration_ms": duration_ms,
    }
    if read.outcome == "observed":
        section["rows"] = [
            {
                field: value.replace(tzinfo=timezone.utc).isoformat()
                if isinstance(value, datetime)
                else value
                for field, value in row.items()
            }
            for row in read.rows
        ]
    else:
        if read.reason is not None:
            section["reason"] = read.reason
        if read.mysql_error_code is not None:
            section["mysql_error_code"] = read.mysql_error_code
    return section


def capture_hirehub_snapshot(
    mysql: MySQLConnector,
    rabbitmq: RabbitMQQueueConnector,
    *,
    environment: str,
    delivery_id: int,
    recipient_id: int,
) -> dict[str, Any]:
    delivery = _read_mysql_section(mysql, _DELIVERY, (delivery_id,))
    snapshot = {
        "environment": environment,
        "vhost": rabbitmq.vhost,
        "delivery_id": delivery_id,
        "delivery": delivery,
    }
    if len(delivery.get("rows", [])) == 1:
        snapshot["outbox"] = outbox = _read_mysql_section(mysql, _OUTBOX, (recipient_id, delivery_id))
        rows = outbox.get("rows", [])
        for row in rows:
            match = row["expected_recipient_listed"]
            row["expected_recipient_listed"] = None if match is None else bool(match)
        if len(rows) == 1 and rows[0]["status"] is not None:
            message_id = rows[0]["message_id"]
            if isinstance(message_id, str) and message_id.strip():
                for branch, consumer in CONSUMERS.items():
                    query = (
                        "SELECT message_id, consumer, create_time AS recorded_at "
                        f"FROM {_CONSUME_LOGS[branch]} WHERE message_id = %s AND consumer = %s"
                    )
                    section = _read_mysql_section(mysql, query, (message_id, consumer))
                    snapshot[branch] = dict(section, message_id=message_id, consumer=consumer)

    queue, duration_ms = measure_tool_call(rabbitmq.inspect_queue)
    queue_section = {
        "name": rabbitmq.queue,
        "source": queue["source"],
        "outcome": queue["outcome"],
        "observed_at": queue["observed_at"],
        "duration_ms": duration_ms,
        "pending_messages": queue.get("pending_messages"),
        "unacknowledged_messages": queue.get("unacknowledged_messages"),
        "registered_consumers": queue.get("registered_consumers"),
    }
    if queue["outcome"] != "observed":
        queue_section["reason"] = queue["reason"]
        if queue["http_status"] is not None:
            queue_section["http_status"] = queue["http_status"]
    snapshot["queue"] = queue_section
    snapshot["captured_at"] = datetime.now(timezone.utc).isoformat()
    return snapshot


@dataclass(frozen=True)
class HireHubLiveAdapter:
    mysql: MySQLConnector
    rabbitmq: RabbitMQQueueConnector
    environment: str
    recipient_id: int
    branches = HireHubSnapshotAdapter.branches

    def capture(self, delivery_id: int) -> dict[str, Any]:
        return capture_hirehub_snapshot(
            self.mysql, self.rabbitmq, environment=self.environment, delivery_id=delivery_id,
            recipient_id=self.recipient_id,
        )

    def inspect_trace(self, entity_id: int) -> TargetTraceRead:
        return HireHubSnapshotAdapter.inspect_data(self.capture(entity_id), entity_id)
