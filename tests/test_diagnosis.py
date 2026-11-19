import json
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import run_diagnosis


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
    assert report["status"] == status
    assert report["conclusion"]
    queue, *remaining = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["source"] == "simulated:queue"
    assert queue["pending_messages"] == pending_messages
    if scenario == "normal":
        assert remaining == []
    else:
        (consumer,) = remaining
        assert consumer["tool"] == "inspect_consumer"
        assert consumer["source"] == "simulated:consumer"
        assert consumer["running"] is False

    for evidence in report["evidence"]:
        timestamp = datetime.fromisoformat(evidence["observed_at"])
        assert timestamp.utcoffset() == timedelta(0)


def test_backlog_with_running_consumer_has_undetermined_cause() -> None:
    tools = SimulatedQueueConnector(pending_messages=3, consumer_running=True)

    report = run_diagnosis("Messages are stuck in the queue", tools)

    assert report["status"] == "undetermined"
    assert [evidence["tool"] for evidence in report["evidence"]] == [
        "inspect_queue",
        "inspect_consumer",
    ]
