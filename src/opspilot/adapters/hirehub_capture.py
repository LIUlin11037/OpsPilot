"""Capture the minimal HireHub evidence consumed by the snapshot adapter."""

from datetime import datetime, timezone
from typing import Any

from opspilot.adapters.hirehub_snapshot import CONSUMERS
from opspilot.connectors.mysql import MySQLConnector, MySQLRead
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector

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


def _section(read: MySQLRead) -> dict[str, Any]:
    section: dict[str, Any] = {"outcome": read.outcome, "observed_at": read.observed_at}
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
    return section


def capture_hirehub_snapshot(
    mysql: MySQLConnector,
    rabbitmq: RabbitMQQueueConnector,
    *,
    environment: str,
    delivery_id: int,
    recipient_id: int,
) -> dict[str, Any]:
    delivery = mysql.select(_DELIVERY, (delivery_id,))
    snapshot = {
        "environment": environment,
        "vhost": rabbitmq.vhost,
        "delivery_id": delivery_id,
        "delivery": _section(delivery),
    }
    if len(delivery.rows) == 1:
        snapshot["outbox"] = outbox = _section(mysql.select(_OUTBOX, (recipient_id, delivery_id)))
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
                    read = mysql.select(query, (message_id, consumer))
                    snapshot[branch] = dict(_section(read), message_id=message_id, consumer=consumer)

    queue = rabbitmq.inspect_queue()
    snapshot["queue"] = {
        "name": rabbitmq.queue,
        "outcome": queue["outcome"],
        "observed_at": queue["observed_at"],
        "pending_messages": queue.get("pending_messages"),
        "unacknowledged_messages": queue.get("unacknowledged_messages"),
        "registered_consumers": queue.get("registered_consumers"),
    }
    snapshot["captured_at"] = datetime.now(timezone.utc).isoformat()
    return snapshot
