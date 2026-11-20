"""Opt-in acceptance tests for the RabbitMQ CLI and saved reports."""

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from urllib.parse import parse_qs, urlsplit

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


@pytest.fixture
def live_processing_target(pytestconfig, processing_service, java_processing_service):
    if pytestconfig.getoption("--java-target"):
        return java_processing_service

    @contextmanager
    def serve(queue: str, vhost: str) -> Iterator[str]:
        records = [
            {
                "queue": queue,
                "vhost": vhost,
                "message_id": "order-msg-101",
                "result": "failed",
                "attempted_at": "2026-11-19T03:00:00Z",
            },
            {
                "queue": queue,
                "vhost": vhost,
                "message_id": "order-msg-102",
                "result": "succeeded",
                "attempted_at": "2026-11-19T03:01:00Z",
            },
        ]

        def read_records(path: str) -> tuple[int, bytes]:
            url = urlsplit(path)
            if url.path != "/processing":
                return 404, b"[]"
            query = parse_qs(url.query)
            matches = [
                record
                for record in records
                if all(
                    query.get(field) == [record[field]]
                    for field in ("queue", "vhost", "message_id")
                )
            ]
            return (200, json.dumps(matches).encode()) if matches else (404, b"[]")

        with processing_service(read_records) as (endpoint, requests):
            yield endpoint
        assert len(requests) == 1

    return serve


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
    rabbitmq_settings: RabbitMQTestSettings,
    prefetch: int,
    diagnosis_status: str,
    live_processing_target,
    processing_service,
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

        health_status = 503
        with (
            TemporaryDirectory(prefix="opspilot-rabbitmq-live-") as directory,
            processing_service(lambda _path: (health_status, b"")) as (fixture_url, health_requests),
        ):
            health_url = fixture_url.removesuffix("/processing") + "/health"
            diagnosis_options = (*queue_options, "--consumer-health-url", health_url)
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
            backlog = _run_cli(directory, "diagnose", *diagnosis_options, "Check ready messages")
            assert backlog["status"] == "undetermined"
            queue_evidence, health = backlog["evidence"]
            assert _snapshot(queue_evidence) == ready_snapshot
            assert health["tool"] == "inspect_consumer_endpoint"
            assert health["source"] == health_url
            assert health["health"] == "unhealthy" and health["http_status"] == 503
            assert any(
                check["action"] == "inspect_endpoint_logs" and check["evidence_index"] == 1
                for check in backlog["next_checks"]
            )

            health_status = 204
            channel.basic_qos(prefetch_count=prefetch)
            channel.basic_consume(
                queue=name, on_message_callback=lambda *_delivery: None, auto_ack=False
            )
            delivered_snapshot = (MESSAGE_COUNT - prefetch, prefetch, 1)
            _wait_for_snapshot(connector, delivered_snapshot)
            with live_processing_target(name, parameters.virtual_host) as endpoint:
                delivered = _run_cli(
                    directory,
                    "diagnose",
                    *diagnosis_options,
                    "--processing-url",
                    endpoint,
                    "--message-id",
                    "order-msg-101",
                    "Check deliveries awaiting acknowledgement",
                )
            assert delivered["status"] == diagnosis_status
            assert delivered["requested_message_id"] == "order-msg-101"
            queue_evidence, health, processing = delivered["evidence"]
            assert _snapshot(queue_evidence) == delivered_snapshot
            assert health["source"] == health_url
            assert health["health"] == "healthy" and health["http_status"] == 204
            assert f"awaiting acknowledgement is {prefetch}" in delivered["conclusion"]
            assert delivered["processing_correlation"] == "unknown"
            assert processing["message_id"] == "order-msg-101"
            assert processing["result"] == "failed"
            assert processing["attempted_at"] == "2026-11-19T03:00:00+00:00"
            source = urlsplit(processing["source"])
            assert source._replace(query="").geturl() == endpoint
            assert parse_qs(source.query) == {
                "queue": [name],
                "vhost": [parameters.virtual_host],
                "message_id": ["order-msg-101"],
            }
            assert [call["tool"] for call in delivered["tool_calls"]] == [
                "inspect_queue",
                "inspect_consumer_endpoint",
                "inspect_processing",
            ]
            assert all(call["outcome"] == "observed" for call in delivered["tool_calls"])
            assert all(call["duration_ms"] >= 0 for call in delivered["tool_calls"])
            saved = _run_cli(directory, "report", delivered["task_id"])
            assert saved == delivered

            for remaining in range(MESSAGE_COUNT, 0, -prefetch):
                in_flight = min(remaining, prefetch)
                _wait_for_snapshot(connector, (remaining - in_flight, in_flight, 1))
                channel.basic_ack(delivery_tag=0, multiple=True)
            _wait_for_snapshot(connector, (0, 0, 1))
            drained = _run_cli(directory, "diagnose", *diagnosis_options, "Check drained queue")
            assert drained["status"] == "no_backlog_observed"
            assert len(drained["evidence"]) == 1
            assert _snapshot(drained["evidence"][0]) == (0, 0, 1)
            assert health_requests == ["/health", "/health"]
