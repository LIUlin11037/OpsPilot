import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit

import pytest

from opspilot.core import investigation
from opspilot.core.investigation import InvestigationTool, run_investigation

QUESTION = "Why is this operation delayed?"
STAMP = "2026-11-24T09:00:00Z"
QUEUE = "/api/queues/%2F/orders"
MESSAGE = "order-01/A"


def _cli(directory, *args, credentials=True, expected_code=0):
    environment = {**os.environ}
    for name, value in (
        ("OPSPILOT_RABBITMQ_USER", "fixture-reader"),
        ("OPSPILOT_RABBITMQ_PASSWORD", "fixture-secret"),
    ):
        if credentials:
            environment[name] = value
        else:
            environment.pop(name, None)
    process = subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory, env=environment,
        capture_output=True, encoding="utf-8", timeout=25,
    )
    assert process.returncode == expected_code, process.stderr
    return process


def _config(directory, base, *, consumer=True, processing=True):
    queue = {"management_url": base, "queue": "orders"}
    if consumer:
        queue["consumer_health_url"] = f"{base}/consumer"
    if processing:
        queue["processing_url"] = f"{base}/processing"
    config = {
        "health": {"service_url": f"{base}/health", "dependency_urls": [f"{base}/unrelated"]},
        "rabbitmq": queue,
    }
    (Path(directory) / "target.json").write_text(json.dumps(config), encoding="utf-8")


def test_investigation_cli_same_question_drives_two_paths_and_saves_report(processing_service):
    service_healthy = True

    def reply(path):
        request = urlsplit(path)
        if request.path == "/health":
            return (204 if service_healthy else 503), b""
        if request.path == QUEUE:
            return 200, b'{"messages_ready":3,"messages_unacknowledged":1,"consumers":1}'
        if request.path == "/consumer":
            return 503, b""
        assert request.path == "/processing"
        assert parse_qs(request.query) == {"queue": ["orders"], "vhost": ["/"], "message_id": [MESSAGE]}
        return 200, json.dumps([
            {"queue": "orders", "vhost": "/", "message_id": MESSAGE, "result": "failed", "attempted_at": STAMP},
            {"queue": "orders", "vhost": "/", "message_id": "another-order", "result": "succeeded",
             "attempted_at": "2026-11-24T09:05:00Z"},
        ]).encode()

    with TemporaryDirectory(prefix="opspilot-investigation-") as directory, processing_service(reply) as (url, calls):
        base = url.removesuffix("/processing")
        _config(directory, base)
        args = ("diagnose-auto", "--config", "target.json", "--workflow", "investigation", "--message-id", MESSAGE, QUESTION)
        plan = json.loads(_cli(directory, *args, "--plan", credentials=False).stdout)
        assert not calls and not (Path(directory) / ".opspilot").exists()
        assert plan["budget"] == {"max_steps": 4, "max_seconds": 15}
        assert [item["sources"][0] for item in plan["checks"]] == [
            f"{base}/health", f"{base}{QUEUE}", f"{base}/consumer",
            f"{base}/processing?queue=orders&vhost=%2F&message_id=order-01%2FA",
        ]
        healthy = json.loads(_cli(directory, *args, credentials=False).stdout)
        assert healthy["investigation"]["stop_reason"] == "service_healthy"
        assert healthy["status"] == "no_failure_observed"
        assert calls == ["/health"]

        calls.clear()
        service_healthy = False
        fault = json.loads(_cli(directory, *args).stdout)
        assert [urlsplit(call).path for call in calls] == ["/health", QUEUE, "/consumer", "/processing"]
        assert fault["planned_checks"] == plan["checks"]
        assert fault["status"] == "undetermined"
        assert fault["processing_correlation"] is None
        assert fault["evidence"][3]["message_id"] == MESSAGE
        assert fault["evidence"][3]["result"] == "failed"
        details = fault["investigation"]
        assert details["stop_reason"] == "checks_complete"
        assert details["conflicting_evidence_indexes"] == [1, 2]
        assert [item["evidence_indexes"] for item in details["decisions"]] == [[], [0], [1], [0]]
        assert json.loads(_cli(directory, "report", fault["task_id"]).stdout) == fault
        markdown = _cli(directory, "report", fault["task_id"], "--format", "markdown").stdout
        assert "## Investigation" in markdown
        assert "**Different signals (evidence indexes)** 1, 2" in markdown


def test_investigation_credentials_failure_preserves_service_and_uses_configured_consumer(processing_service):
    with TemporaryDirectory(prefix="opspilot-investigation-") as directory, processing_service(
        lambda path: (503 if path == "/health" else 204, b""),
    ) as (url, calls):
        _config(directory, url.removesuffix("/processing"))
        report = json.loads(_cli(
            directory, "diagnose-auto", "--config", "target.json", "--workflow", "investigation", QUESTION,
            credentials=False,
        ).stdout)
    assert calls == ["/health", "/consumer"]
    assert report["evidence"][0]["http_status"] == 503
    assert report["evidence"][1]["reason"] == "credentials_unconfigured"
    assert report["investigation"]["decisions"][2]["reason"] == "queue_read_unavailable"


def test_investigation_keeps_partial_results_when_a_source_times_out():
    queue_source = "fixture://queue"

    def queue_timeout(_):
        raise TimeoutError()

    report = run_investigation(
        QUESTION, _service(), InvestigationTool(queue_source, queue_timeout),
        {"max_steps": 4, "max_seconds": 10}, consumer=_consumer(),
    )
    assert [item["outcome"] for item in report["evidence"]] == ["observed", "timeout", "observed"]
    assert report["evidence"][1]["source"] == queue_source
    assert report["investigation"]["decisions"][2]["evidence_indexes"] == [1]
    assert report["investigation"]["stop_reason"] == "checks_complete"


def _service():
    return InvestigationTool("fixture://service", lambda _: {
        "tool": "inspect_service_health", "health": "unhealthy", "outcome": "observed",
        "source": "fixture://service", "observed_at": STAMP, "http_status": 503,
    })


def _consumer():
    return InvestigationTool("fixture://consumer", lambda _: {
        "tool": "inspect_consumer_endpoint", "health": "healthy", "outcome": "observed",
        "source": "fixture://consumer", "observed_at": STAMP, "http_status": 204,
    })


@pytest.mark.parametrize(("max_steps", "reason"), [(1, "step_budget_exhausted"), (4, "time_budget_exhausted")])
def test_investigation_preserves_evidence_and_starts_no_reads_after_budget(monkeypatch, max_steps, reason):
    clock = [0.0]
    monkeypatch.setattr(investigation, "perf_counter", lambda: clock[0])

    def service(remaining):
        assert remaining == 1
        clock[0] = 0.5 if max_steps == 1 else 1
        return _service().read(remaining)

    def unexpected_read(_):
        pytest.fail("read started after budget expired")

    report = run_investigation(
        QUESTION, InvestigationTool("fixture://service", service), InvestigationTool("fixture://queue", unexpected_read),
        {"max_steps": max_steps, "max_seconds": 1},
    )
    assert len(report["evidence"]) == len(report["tool_calls"]) == 1
    assert report["investigation"]["stop_reason"] == reason
    assert report["evidence"][0]["http_status"] == 503


def test_investigation_does_not_read_unconfigured_sources(processing_service):
    def reply(path):
        if path == "/health":
            return 503, b""
        assert path == QUEUE
        return 200, b'{"messages_ready":1,"consumers":1}'

    with TemporaryDirectory(prefix="opspilot-investigation-") as directory, processing_service(reply) as (url, calls):
        _config(directory, url.removesuffix("/processing"), consumer=False, processing=False)
        args = ("diagnose-auto", "--config", "target.json", "--workflow", "investigation", QUESTION)
        report = json.loads(_cli(directory, *args).stdout)
        rejected = _cli(directory, *args, "--message-id", MESSAGE, "--plan", expected_code=2)
        assert "requires rabbitmq.processing_url or rabbitmq.processing_record" in rejected.stderr
    assert calls == ["/health", QUEUE]
    assert len(report["evidence"]) == 2
    assert report["investigation"]["stop_reason"] == "checks_complete"
