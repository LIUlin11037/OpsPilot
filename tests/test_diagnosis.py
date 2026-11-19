import json
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import (
    DiagnosisReport,
    ProcessingResult,
    UnavailableOutcome,
    run_diagnosis,
)


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
    assert report["processing_correlation"] is None
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


@pytest.mark.parametrize(
    ("scenario", "correlation", "message_id", "explanation"),
    [
        (
            "processing-failed",
            "matched",
            "msg-101",
            "Its message ID matches a sampled pending message.",
        ),
        (
            "processing-failed-outside-sample",
            "not_in_sample",
            "msg-099",
            "Its message ID was absent from the pending-message sample.",
        ),
    ],
)
def test_cli_correlates_failed_processing_with_sample(
    scenario: str, correlation: str, message_id: str, explanation: str
) -> None:
    report = _diagnose_from_cli(scenario)

    assert report["status"] == "undetermined"
    assert report["processing_correlation"] == correlation
    assert "The latest processing attempt failed." in report["conclusion"]
    assert explanation in report["conclusion"]
    queue, consumer, processing = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["outcome"] == "observed"
    assert queue["pending_messages"] == 3
    assert queue["sampled_pending_message_ids"] == ["msg-101", "msg-102"]
    assert consumer["tool"] == "inspect_consumer"
    assert consumer["outcome"] == "observed"
    assert consumer["running"] is True
    assert processing["tool"] == "inspect_processing"
    assert processing["source"] == "simulated:processing"
    assert processing["outcome"] == "observed"
    assert processing["result"] == "failed"
    assert processing["message_id"] == message_id


@pytest.mark.parametrize(
    ("latest_result", "outcome", "explanation"),
    [
        ("succeeded", "observed", "The latest processing attempt succeeded."),
        ("missing", "missing", "Processing inspection returned no data."),
        ("timeout", "timeout", "Processing inspection timed out."),
    ],
)
def test_processing_evidence_preserves_uncertainty(
    latest_result: ProcessingResult | UnavailableOutcome,
    outcome: str,
    explanation: str,
) -> None:
    tools = SimulatedQueueConnector(pending_messages=3, consumer_running=True)
    adapter = SimulatedProcessingAdapter(latest_result)

    report = run_diagnosis("Messages are stuck in the queue", tools, adapter)

    assert report["status"] == "undetermined"
    assert report["processing_correlation"] == "unknown"
    assert explanation in report["conclusion"]
    assert [evidence["tool"] for evidence in report["evidence"]] == [
        "inspect_queue",
        "inspect_consumer",
        "inspect_processing",
    ]
    processing = report["evidence"][-1]
    assert processing["outcome"] == outcome
    if processing["outcome"] != "observed":
        assert "result" not in processing


@pytest.mark.parametrize(
    ("sampled_ids", "message_id"),
    [((), "msg-101"), (("msg-101",), None)],
)
def test_correlation_is_unknown_when_an_id_is_unavailable(
    sampled_ids: tuple[str, ...], message_id: str | None
) -> None:
    tools = SimulatedQueueConnector(
        pending_messages=3,
        consumer_running=True,
        sampled_pending_message_ids=sampled_ids,
    )
    adapter = SimulatedProcessingAdapter(latest_result="failed", message_id=message_id)

    report = run_diagnosis("Messages are stuck in the queue", tools, adapter)

    assert report["status"] == "undetermined"
    assert report["processing_correlation"] == "unknown"
    assert "Message correlation is unknown." in report["conclusion"]
