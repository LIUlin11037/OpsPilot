import json
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.__main__ import main
from opspilot.adapters.hirehub_snapshot import HireHubSnapshotAdapter
from opspilot.connectors.mysql import MySQLConnector, MySQLRead
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.core.target_trace import run_target_trace

OBSERVED_AT = "2026-11-19T06:00:00+00:00"
MESSAGE_ID = "message-for-delivery-1001"


@pytest.mark.parametrize(
    ("notification_outcome", "branch_status"),
    [("observed", "not_observed"), ("missing", "unknown"), ("timeout", "unknown")],
)
def test_capture_cli_produces_diagnosable_partial_evidence(
    monkeypatch, capsys, notification_outcome, branch_status
) -> None:
    queries = []

    def select(self, query, parameters):
        assert (self.host, self.port, self.username, self.password) == (
            "127.0.0.1", 13306, "mysql-reader", "mysql-secret"
        )
        queries.append(parameters)
        assert query.lstrip().startswith("SELECT ")
        if "hirehub_delivery.delivery" in query:
            return MySQLRead("observed", OBSERVED_AT, ({"delivery_id": 1001, "status": "PENDING"},))
        if "hirehub_delivery.local_message" in query:
            return MySQLRead("observed", OBSERVED_AT, ({
                "delivery_id": 1001, "biz_type": "DELIVERY_CREATED", "routing_key": "delivery.created",
                "message_id": MESSAGE_ID, "status": "CONFIRMED", "expected_recipient_listed": 0,
                "sent_at": datetime(2026, 11, 19, 5, 59), "acknowledged_at": None,
            },))
        if "hirehub_job.mq_consume_log" in query:
            return MySQLRead("observed", OBSERVED_AT, ({
                "message_id": MESSAGE_ID, "consumer": "job-delivery-count",
                "recorded_at": datetime(2026, 11, 19, 5, 59),
            },))
        assert "hirehub_notification.mq_consume_log" in query
        return MySQLRead(notification_outcome, OBSERVED_AT)

    def inspect_queue(self):
        assert (self.queue, self.vhost) == ("hirehub.notification.msg", "/test")
        return {
            "outcome": "observed", "observed_at": OBSERVED_AT, "pending_messages": 1,
            "unacknowledged_messages": 0, "registered_consumers": 0,
        }

    monkeypatch.setattr(MySQLConnector, "select", select)
    monkeypatch.setattr(RabbitMQQueueConnector, "inspect_queue", inspect_queue)
    monkeypatch.setenv("OPSPILOT_MYSQL_USER", "mysql-reader")
    monkeypatch.setenv("OPSPILOT_MYSQL_PASSWORD", "mysql-secret")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "rabbit-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "rabbit-secret")
    monkeypatch.setattr(sys, "argv", [
        "opspilot", "capture-hirehub", "--environment", "isolated", "--delivery-id", "1001",
        "--recipient-id", "4002", "--mysql-port", "13306", "--management-url",
        "http://127.0.0.1:15673", "--vhost", "/test",
    ])

    main()

    assert queries == [(1001,), (4002, 1001), (MESSAGE_ID, "job-delivery-count"),
                       (MESSAGE_ID, "notification")]
    output = capsys.readouterr().out
    assert "mysql-secret" not in output and "rabbit-secret" not in output
    assert "payload" not in output and "recipient_id" not in output
    with TemporaryDirectory(prefix="opspilot-hirehub-capture-test-") as directory:
        path = Path(directory) / "snapshot.json"
        path.write_text(output, encoding="utf-8")
        report = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(path))

    assert report["target_trace"]["branches"] == {"job": "observed", "notification": branch_status}
    checks = {item["checkpoint"]: item for item in report["evidence"]}
    assert checks["outbox"]["expected_recipient_listed"] is False
    assert checks["outbox"]["sent_at"] == "2026-11-19T05:59:00+00:00"
    assert ("row_count" in checks["notification"]) == (notification_outcome == "observed")
    assert checks["queue"]["registered_consumers"] == 0
    assert json.loads(output)["environment"] == "isolated"
