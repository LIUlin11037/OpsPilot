import json
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import DiagnosisReport, run_diagnosis


def _diagnose_from_cli(scenario: str) -> DiagnosisReport:
    question = "消息已入队但未消费"
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "diagnose", "--scenario", scenario, question],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    report = json.loads(result.stdout)

    assert report["question"] == question
    for evidence in report["evidence"]:
        timestamp = datetime.fromisoformat(evidence["observed_at"])
        assert timestamp.utcoffset() == timedelta(0)
    return report


@pytest.mark.parametrize(
    ("scenario", "status", "pending_messages"),
    [
        ("normal", "no_backlog_observed", 0),
        ("consumer-stopped", "consumer_stopped", 3),
    ],
)
def test_cli_diagnoses_simulated_queue(
    scenario: str, status: str, pending_messages: int
) -> None:
    report = _diagnose_from_cli(scenario)

    assert report["status"] == status
    assert report["conclusion"]
    queue, *remaining = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["source"] == "simulated:queue"
    assert queue["outcome"] == "observed"
    assert queue["pending_messages"] == pending_messages
    if scenario == "normal":
        assert remaining == []
    else:
        (consumer,) = remaining
        assert consumer["tool"] == "inspect_consumer"
        assert consumer["source"] == "simulated:consumer"
        assert consumer["outcome"] == "observed"
        assert consumer["running"] is False


def test_queue_timeout_leaves_queue_depth_unknown() -> None:
    report = _diagnose_from_cli("queue-timeout")

    assert report["status"] == "undetermined"
    assert "Queue inspection timed out" in report["conclusion"]
    (queue,) = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["source"] == "simulated:queue"
    assert queue["outcome"] == "timeout"
    assert "pending_messages" not in queue


def test_missing_consumer_data_preserves_observed_backlog() -> None:
    report = _diagnose_from_cli("consumer-data-missing")

    assert report["status"] == "undetermined"
    assert "Consumer inspection returned no data" in report["conclusion"]
    queue, consumer = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["outcome"] == "observed"
    assert queue["pending_messages"] == 3
    assert consumer["tool"] == "inspect_consumer"
    assert consumer["source"] == "simulated:consumer"
    assert consumer["outcome"] == "missing"
    assert "running" not in consumer


def test_backlog_with_running_consumer_has_undetermined_cause() -> None:
    tools = SimulatedQueueConnector(pending_messages=3, consumer_running=True)

    report = run_diagnosis("Messages are stuck in the queue", tools)

    assert report["status"] == "undetermined"
    assert [evidence["tool"] for evidence in report["evidence"]] == [
        "inspect_queue",
        "inspect_consumer",
    ]
