import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

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
    ("data", "error"),
    [
        ({}, "requires a health or rabbitmq section"),
        ({"health": None}, "health must be a JSON object"),
        ({"rabbitmq": {"management_url": "http://localhost"}}, "rabbitmq.queue must be a nonempty string"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "password": "fixture"}},
         "unsupported fields"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "timeout_seconds": True}},
         "timeout_seconds must be a finite positive number"),
    ],
)
def test_target_config_rejects_invalid_fields(data, error) -> None:
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ValueError, match=error):
            load_target_config(config)
