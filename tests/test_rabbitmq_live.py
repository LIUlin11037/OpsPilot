"""Opt-in acceptance tests for the RabbitMQ CLI and saved reports."""

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from time import monotonic, sleep

import pytest

from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.core.diagnosis import Evidence

MESSAGE_COUNT = 3


@dataclass(frozen=True)
class RabbitMQTestSettings:
    amqp_url: str = field(repr=False)
    management_url: str
    username: str
    password: str = field(repr=False)


@pytest.fixture(scope="module")
def rabbitmq_settings(pytestconfig: pytest.Config) -> RabbitMQTestSettings:
    if not pytestconfig.getoption("--rabbitmq-live"):
        pytest.skip("Pass --rabbitmq-live to run against a test RabbitMQ broker.")

    names = (
        "OPSPILOT_TEST_RABBITMQ_AMQP_URL",
        "OPSPILOT_TEST_RABBITMQ_MANAGEMENT_URL",
        "OPSPILOT_RABBITMQ_USER",
        "OPSPILOT_RABBITMQ_PASSWORD",
    )
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.fail(f"Set {', '.join(missing)} for the RabbitMQ acceptance tests.")
    return RabbitMQTestSettings(*(os.environ[name] for name in names))


def _run_cli(directory: str, *arguments: str) -> dict:
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", *arguments],
        cwd=directory,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return json.loads(result.stdout)


def _snapshot(evidence: Evidence) -> tuple[int, int | None, int | None] | None:
    if evidence["tool"] != "inspect_queue" or evidence["outcome"] != "observed":
        return None
    return (
        evidence["pending_messages"],
        evidence.get("unacknowledged_messages"),
        evidence.get("registered_consumers"),
    )


def _wait_for_snapshot(
    connector: RabbitMQQueueConnector, expected: tuple[int, int, int]
) -> None:
    deadline = monotonic() + 20
    while True:
        evidence = connector.inspect_queue()
        if _snapshot(evidence) == expected:
            return
        if monotonic() >= deadline:
            pytest.fail(f"Expected queue metrics {expected}; last evidence was {evidence}.")
        sleep(0.25)


@pytest.mark.parametrize(
    ("prefetch", "diagnosis_status"),
    [(1, "undetermined"), (3, "no_backlog_observed")],
    ids=["ready-and-unacknowledged", "unacknowledged-only"],
)
def test_live_queue_lifecycle(
    rabbitmq_settings: RabbitMQTestSettings, prefetch: int, diagnosis_status: str
) -> None:
    try:
        import pika
    except ImportError:
        pytest.fail('Install the live test extra with pip install -e ".[dev,rabbitmq-test]".')

    settings = rabbitmq_settings
    try:
        parameters = pika.URLParameters(settings.amqp_url)
    except ValueError:
        pytest.fail("OPSPILOT_TEST_RABBITMQ_AMQP_URL must be a valid AMQP URL.", pytrace=False)
    parameters.socket_timeout = 3
    parameters.stack_timeout = 5
    parameters.blocked_connection_timeout = 10

    with pika.Connection(parameters) as connection:
        channel = connection.channel()
        queue = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        name = queue.method.queue
        connector = RabbitMQQueueConnector(
            settings.management_url,
            name,
            settings.username,
            settings.password,
            vhost=parameters.virtual_host,
            timeout_seconds=2,
        )
        queue_options = (
            "--management-url",
            settings.management_url,
            "--vhost",
            parameters.virtual_host,
            "--queue",
            name,
        )

        with TemporaryDirectory(prefix="opspilot-rabbitmq-live-") as directory:
            _wait_for_snapshot(connector, (0, 0, 0))
            empty = _run_cli(directory, "inspect-queue", *queue_options)
            assert _snapshot(empty) == (0, 0, 0)

            for index in range(MESSAGE_COUNT):
                channel.basic_publish(
                    exchange="",
                    routing_key=name,
                    body=f"fixture {index}".encode(),
                    mandatory=True,
                )

            ready_snapshot = (MESSAGE_COUNT, 0, 0)
            _wait_for_snapshot(connector, ready_snapshot)
            backlog = _run_cli(directory, "diagnose", *queue_options, "Check ready messages")
            assert backlog["status"] == "undetermined"
            assert _snapshot(backlog["evidence"][0]) == ready_snapshot

            channel.basic_qos(prefetch_count=prefetch)
            channel.basic_consume(
                queue=name, on_message_callback=lambda *_delivery: None, auto_ack=False
            )
            delivered_snapshot = (MESSAGE_COUNT - prefetch, prefetch, 1)
            _wait_for_snapshot(connector, delivered_snapshot)
            delivered = _run_cli(
                directory,
                "diagnose",
                *queue_options,
                "Check deliveries awaiting acknowledgement",
            )
            assert delivered["status"] == diagnosis_status
            evidence, = delivered["evidence"]
            assert _snapshot(evidence) == delivered_snapshot
            assert f"awaiting acknowledgement is {prefetch}" in delivered["conclusion"]
            call, = delivered["tool_calls"]
            assert call["tool"] == "inspect_queue"
            assert call["outcome"] == "observed"
            assert call["duration_ms"] >= 0
            saved = _run_cli(directory, "report", delivered["task_id"])
            assert saved == delivered
