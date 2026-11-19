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
from opspilot.core import diagnosis
from opspilot.core.target_trace import run_target_trace

OBSERVED_AT = "2026-11-19T06:00:00+00:00"
MESSAGE_ID = "message-for-delivery-1001"
QUEUE_SOURCE = "http://127.0.0.1:15673/api/queues/%2Ftest/hirehub.notification.msg"
LIVE_OPTIONS = [
    "--environment", "isolated", "--delivery-id", "1001", "--recipient-id", "4002",
    "--mysql-port", "13306", "--management-url", "http://127.0.0.1:15673", "--vhost", "/test",
]


@pytest.mark.parametrize("command", ["capture-hirehub", "diagnose-hirehub"])
@pytest.mark.parametrize(
    ("notification_outcome", "branch_status"),
    [("observed", "not_observed"), ("missing", "unknown"), ("timeout", "unknown")],
)
def test_hirehub_cli_live_reads_produce_diagnosable_evidence(
    monkeypatch, capsys, command, notification_outcome, branch_status
) -> None:
    queries = []
    clock_ns = 0

    def select(self, query, parameters):
        nonlocal clock_ns
        assert (self.host, self.port, self.username, self.password) == (
            "127.0.0.1", 13306, "mysql-reader", "mysql-secret"
        )
        queries.append(parameters)
        clock_ns += len(queries) * 1_000_000
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
        if notification_outcome == "missing":
            return MySQLRead(
                "missing", OBSERVED_AT, reason="access_denied", mysql_error_code=1142
            )
        if notification_outcome == "timeout":
            return MySQLRead("timeout", OBSERVED_AT, reason="request_timeout")
        return MySQLRead(notification_outcome, OBSERVED_AT)

    def inspect_queue(self):
        nonlocal clock_ns
        clock_ns += 5_000_000
        assert (self.queue, self.vhost) == ("hirehub.notification.msg", "/test")
        return {
            "outcome": "observed", "source": QUEUE_SOURCE, "observed_at": OBSERVED_AT,
            "pending_messages": 1,
            "unacknowledged_messages": 0, "registered_consumers": 0,
        }

    monkeypatch.setattr(MySQLConnector, "select", select)
    monkeypatch.setattr(RabbitMQQueueConnector, "inspect_queue", inspect_queue)
    monkeypatch.setattr(diagnosis, "perf_counter_ns", lambda: clock_ns)
    monkeypatch.setenv("OPSPILOT_MYSQL_USER", "mysql-reader")
    monkeypatch.setenv("OPSPILOT_MYSQL_PASSWORD", "mysql-secret")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "rabbit-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "rabbit-secret")
    with TemporaryDirectory(prefix="opspilot-hirehub-capture-test-") as directory:
        database = Path(directory) / "reports.sqlite3"
        path = Path(directory) / "snapshot.json"
        invocation = ["opspilot", "--db", str(database), command, *LIVE_OPTIONS]
        if command == "diagnose-hirehub":
            invocation.append("Check notification")
        monkeypatch.setattr(sys, "argv", invocation)

        main()

        output = capsys.readouterr().out
        assert "mysql-secret" not in output and "rabbit-secret" not in output
        assert "payload" not in output and "recipient_id" not in output
        if command == "capture-hirehub":
            assert json.loads(output)["environment"] == "isolated"
            path.write_text(output, encoding="utf-8")
            report = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(path))
        else:
            report = json.loads(output)
            monkeypatch.setattr(
                sys, "argv", ["opspilot", "--db", str(database), "report", report["task_id"]]
            )
            main()
            assert json.loads(capsys.readouterr().out) == report
            assert not path.exists()

    assert queries == [(1001,), (4002, 1001), (MESSAGE_ID, "job-delivery-count"),
                       (MESSAGE_ID, "notification")]
    assert report["target_trace"]["branches"] == {"job": "observed", "notification": branch_status}
    checks = {item["checkpoint"]: item for item in report["evidence"]}
    assert {name: check["duration_ms"] for name, check in checks.items()} == {
        "delivery": 1, "outbox": 2, "job": 3, "notification": 4, "queue": 5,
    }
    assert report["tool_calls"][0]["duration_ms"] == (
        15 if command == "diagnose-hirehub" else 0
    )
    assert checks["outbox"]["expected_recipient_listed"] is False
    assert checks["outbox"]["sent_at"] == "2026-11-19T05:59:00+00:00"
    assert ("row_count" in checks["notification"]) == (notification_outcome == "observed")
    if notification_outcome == "missing":
        assert checks["notification"]["reason"] == "access_denied"
        assert checks["notification"]["mysql_error_code"] == 1142
    if notification_outcome == "timeout":
        assert checks["notification"]["reason"] == "request_timeout"
    assert [
        (check["action"], report["evidence"][check["evidence_index"]]["checkpoint"])
        for check in report["next_checks"]
    ] == [
        ("verify_read_access" if notification_outcome == "missing" else "retry_read", "notification"),
        ("inspect_consumer_health", "queue"),
    ]
    assert checks["queue"]["registered_consumers"] == 0
    if command == "diagnose-hirehub":
        assert checks["notification"]["source"] == "MySQL/hirehub_notification.mq_consume_log"
        assert checks["queue"]["source"] == QUEUE_SOURCE
    else:
        assert checks["queue"]["source"] == f"{path.as_uri()}#queue"


@pytest.mark.parametrize("queue_denied", [False, True])
@pytest.mark.parametrize("replay", [False, True])
def test_diagnosis_preserves_queue_when_delivery_lookup_times_out(
    monkeypatch, capsys, queue_denied, replay
) -> None:
    def select(self, query, parameters):
        assert "hirehub_delivery.delivery" in query and parameters == (1001,)
        return MySQLRead("timeout", OBSERVED_AT)

    monkeypatch.setattr(MySQLConnector, "select", select)
    queue_result = {"source": QUEUE_SOURCE, "observed_at": OBSERVED_AT}
    if queue_denied:
        queue_result.update(outcome="missing", reason="access_denied", http_status=403)
    else:
        queue_result.update(outcome="observed", pending_messages=2)
    monkeypatch.setattr(RabbitMQQueueConnector, "inspect_queue", lambda self: queue_result)
    for name in (
        "OPSPILOT_MYSQL_USER", "OPSPILOT_MYSQL_PASSWORD",
        "OPSPILOT_RABBITMQ_USER", "OPSPILOT_RABBITMQ_PASSWORD",
    ):
        monkeypatch.setenv(name, "test")
    with TemporaryDirectory(prefix="opspilot-hirehub-partial-test-") as directory:
        database = Path(directory) / "reports.sqlite3"
        options = LIVE_OPTIONS
        captured = None
        if replay:
            monkeypatch.setattr(sys, "argv", ["opspilot", "capture-hirehub", *LIVE_OPTIONS])
            main()
            output = capsys.readouterr().out
            captured = json.loads(output)
            snapshot = Path(directory) / "partial.json"
            snapshot.write_text(output, encoding="utf-8")
            options = ["--snapshot", str(snapshot), "--delivery-id", "1001"]
        monkeypatch.setattr(sys, "argv", [
            "opspilot", "--db", str(database), "diagnose-hirehub", *options, "Check notification",
        ])
        main()
        report = json.loads(capsys.readouterr().out)

    assert report["target_trace"]["message_id"] is None
    assert report["processing_correlation"] is None
    assert report["target_trace"]["branches"] == {"job": "unknown", "notification": "unknown"}
    assert [check["checkpoint"] for check in report["evidence"]] == ["delivery", "queue"]
    assert report["evidence"][0]["outcome"] == "timeout"
    assert "message_id" not in report["evidence"][1]
    if captured is not None:
        assert report["evidence"][1]["duration_ms"] == captured["queue"]["duration_ms"]
        assert report["evidence"][1]["source"] == f"{snapshot.as_uri()}#queue"
    if queue_denied:
        assert report["evidence"][1]["reason"] == "access_denied"
        assert report["evidence"][1]["http_status"] == 403
    else:
        assert report["evidence"][1]["pending_messages"] == 2


@pytest.mark.parametrize("options", [
    ["--snapshot", "capture.json", "--management-url", "http://127.0.0.1:15673"],
    ["--snapshot", "capture.json", "--mysql-port", "13306"],
    ["--management-url", "http://127.0.0.1:15673", "--environment", "isolated"],
])
def test_diagnose_rejects_invalid_source_options(monkeypatch, options) -> None:
    monkeypatch.setattr(sys, "argv", [
        "opspilot", "diagnose-hirehub", "--delivery-id", "1001", *options, "Check notification",
    ])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
