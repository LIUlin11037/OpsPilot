import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit

import pytest

from opspilot.target_config import load_target_config


def _cli(directory: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory,
        capture_output=True, encoding="utf-8", timeout=20,
    )


@pytest.mark.parametrize("target_file", [False, True])
@pytest.mark.parametrize(
    ("question", "workflow", "terms", "expected_path", "tool"),
    [
        ("服务返回 503", "http_health", ["返回 503"], "/health", "inspect_service_health"),
        ("RabbitMQ 队列积压", "queue", ["rabbitmq", "队列", "积压"],
         "/api/queues/%2F/orders", "inspect_queue"),
    ],
)
def test_auto_diagnosis_runs_and_saves_the_selected_workflow(
    processing_service, monkeypatch, question, workflow, terms, expected_path, tool, target_file,
) -> None:
    queue_metrics = b'{"messages_ready": 3, "messages_unacknowledged": 0, "consumers": 1}'

    def respond(path):
        if path in ("/health", "/dependency"):
            return (503 if path == "/health" else 204), b""
        return 200, queue_metrics

    if workflow == "queue":
        monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
        monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    else:
        monkeypatch.delenv("OPSPILOT_RABBITMQ_USER", raising=False)
        monkeypatch.delenv("OPSPILOT_RABBITMQ_PASSWORD", raising=False)

    with TemporaryDirectory(prefix="opspilot-auto-diagnosis-") as directory:
        expected_requests = [expected_path]
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            health = {"service_url": f"{base}/health"}
            if target_file:
                config = Path(directory) / "target.json"
                config.write_text(json.dumps({
                    "health": {**health, "dependency_urls": [f"{base}/dependency"]},
                    "rabbitmq": {"management_url": base, "queue": "orders", "vhost": "staging /"},
                }), encoding="utf-8-sig")
                options = ("--config", str(config))
                if workflow == "http_health":
                    expected_requests.append("/dependency")
                else:
                    expected_requests = ["/api/queues/staging%20%2F/orders"]
            else:
                config = Path(directory) / "health.json"
                config.write_text(json.dumps(health), encoding="utf-8")
                options = ("--health-config", str(config), "--management-url", base, "--queue", "orders")
            result = _cli(directory, "diagnose-auto", *options, question)

        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["routing"] == {"workflow": workflow, "matched_terms": terms}
        assert report["question"] == question
        assert report["tool_calls"][0]["tool"] == tool
        assert requests == expected_requests

        stored = _cli(directory, "report", report["task_id"])
        assert stored.returncode == 0 and json.loads(stored.stdout) == report
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selected workflow**" in markdown.stdout
        assert f"**Matched terms** {', '.join(terms)}" in markdown.stdout


@pytest.mark.parametrize(
    ("options", "question", "error"),
    [
        ([], "Something is wrong", "Describe a queue symptom or an HTTP health symptom"),
        ([], "队列积压，HTTP 5xx", "Both queue and HTTP symptoms matched"),
        (["--health-config", "unused.json"], "Queue backlog", "Supply --management-url and --queue"),
        ([], "HTTP health check", "Supply --health-config"),
        (["--config", "unused.json", "--health-config", "health.json"],
         "HTTP health check", "--config cannot be combined with direct target options"),
        (["--message-id", "msg-101"], "Check this delivery", "--message-id requires --config"),
        (["--config", "unused.json", "--message-id", " "],
         "Check this delivery", "--message-id must not be empty"),
        (["--config", "unused.json", "--message-id", "msg-101"],
         "HTTP health check", "HTTP health symptoms conflict with --message-id"),
    ],
)
def test_auto_diagnosis_requires_clear_and_configured_symptoms(options, question, error) -> None:
    with TemporaryDirectory(prefix="opspilot-auto-diagnosis-") as directory:
        result = _cli(directory, "diagnose-auto", *options, question)

        assert result.returncode == 2
        assert error in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


def test_auto_diagnosis_requires_the_selected_section_in_target_file() -> None:
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(
            '{"rabbitmq": {"management_url": "http://localhost:15672", "queue": "orders"}}',
            encoding="utf-8",
        )
        result = _cli(directory, "diagnose-auto", "--config", str(config), "HTTP health check")

        assert result.returncode == 2
        assert "a health section in --config" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize(
    ("pending", "unacknowledged", "processing_source"),
    [(3, 0, "processing_url"), (None, 2, "processing_record"), (0, 0, "processing_url")],
)
def test_target_queue_uses_configured_checks_only_when_queue_activity_is_observed(
    processing_service, monkeypatch, pending, unacknowledged, processing_source,
) -> None:
    metrics = json.dumps({
        "messages_ready": pending, "messages_unacknowledged": unacknowledged, "consumers": 1,
    }).encode()
    record = json.dumps({
        "queue": "orders", "vhost": "staging", "message_id": "msg-101",
        "result": "succeeded", "attempted_at": "2026-11-19T03:00:00Z",
    })
    queue_path = "/api/queues/staging/orders"

    def respond(path):
        if path == queue_path:
            return 200, metrics
        if path == "/consumer-health":
            return 503, b""
        return 200, record.encode()

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config_dir = Path(directory) / "configs"
        config_dir.mkdir()
        config = config_dir / "target.json"
        record_path = config_dir / "processing record.json"
        record_path.write_text(record, encoding="utf-8")
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            processing_value = endpoint if processing_source == "processing_url" else record_path.name
            config.write_text(json.dumps({
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "vhost": "staging",
                    "consumer_health_url": f"{base}/consumer-health",
                    processing_source: processing_value,
                },
            }), encoding="utf-8")
            result = _cli(directory, "diagnose-auto", "--config", str(config), "队列积压")

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["routing"]["workflow"] == "queue"
    if pending == 0 and unacknowledged == 0:
        assert requests == [queue_path]
        assert [call["tool"] for call in report["tool_calls"]] == ["inspect_queue"]
        return

    queue, health, processing = report["evidence"]
    assert queue["unacknowledged_messages"] == unacknowledged
    assert health["tool"] == "inspect_consumer_endpoint" and health["http_status"] == 503
    assert processing["tool"] == "inspect_processing" and processing["message_id"] == "msg-101"
    assert [call["tool"] for call in report["tool_calls"]] == [
        "inspect_queue", "inspect_consumer_endpoint", "inspect_processing",
    ]
    if processing_source == "processing_url":
        queue_request, health_request, processing_request = requests
        assert (queue_request, health_request) == (queue_path, "/consumer-health")
        url = urlsplit(processing_request)
        assert url.path == "/processing"
        assert parse_qs(url.query) == {"queue": ["orders"], "vhost": ["staging"]}
    else:
        assert requests == [queue_path, "/consumer-health"]
        assert processing["source"] == record_path.as_uri()


@pytest.mark.parametrize("queue_status", [200, 403])
def test_auto_diagnosis_can_lookup_exact_message_independently_of_queue_activity(
    processing_service, monkeypatch, queue_status,
) -> None:
    message_id = "Order+42 /雪"
    question = "这条消息处理了吗？"
    queue_path = "/api/queues/%2F/orders"
    metrics = b'{"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1}'
    records = json.dumps([
        {
            "queue": "orders", "vhost": "/", "message_id": message_id,
            "result": "failed", "attempted_at": "2026-11-19T03:00:00Z",
        },
        {
            "queue": "orders", "vhost": "/", "message_id": "another-message",
            "result": "succeeded", "attempted_at": "2026-11-19T03:01:00Z",
        },
    ]).encode()

    def respond(path):
        if path == queue_path:
            return queue_status, metrics
        return 200, records

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-target-message-") as directory:
        config = Path(directory) / "target.json"
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            config.write_text(json.dumps({
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "processing_url": endpoint,
                    "consumer_health_url": f"{base}/health",
                },
            }), encoding="utf-8")
            result = _cli(
                directory, "diagnose-auto", "--config", str(config), "--message-id", message_id, question,
            )

        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["routing"] == {"workflow": "queue", "matched_terms": [], "selected_by": "message_id"}
        assert report["requested_message_id"] == message_id
        queue, processing = report["evidence"]
        assert queue["outcome"] == ("observed" if queue_status == 200 else "missing")
        assert processing["message_id"] == message_id and processing["result"] == "failed"
        queue_request, processing_request = requests
        assert queue_request == queue_path
        url = urlsplit(processing_request)
        assert url.path == "/processing"
        assert parse_qs(url.query) == {"queue": ["orders"], "vhost": ["/"], "message_id": [message_id]}
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selection basis** Requested message ID" in markdown.stdout
        assert f"**Requested message ID** {message_id}" in markdown.stdout


def test_auto_message_lookup_requires_processing_source() -> None:
    with TemporaryDirectory(prefix="opspilot-target-message-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(
            '{"rabbitmq": {"management_url": "http://localhost:15672", "queue": "orders"}}',
            encoding="utf-8",
        )
        result = _cli(
            directory, "diagnose-auto", "--config", str(config), "--message-id", "msg-101", "Check delivery",
        )

        assert result.returncode == 2
        assert "--message-id requires rabbitmq.processing_url or rabbitmq.processing_record" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize(
    ("data", "error"),
    [
        ({}, "requires a health or rabbitmq section"),
        ({"health": None}, "health must be a JSON object"),
        ({"rabbitmq": {"management_url": "http://localhost"}}, "rabbitmq.queue must be a nonempty string"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "password": "fixture"}},
         "unsupported fields"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "timeout_seconds": True}},
         "timeout_seconds must be a finite positive number"),
        ({"rabbitmq": {
            "management_url": "http://localhost", "queue": "orders",
            "processing_url": "http://localhost/processing", "processing_record": "processing.json",
        }}, "processing_url and rabbitmq.processing_record are mutually exclusive"),
    ],
)
def test_target_config_rejects_invalid_fields(data, error) -> None:
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ValueError, match=error):
            load_target_config(config)
