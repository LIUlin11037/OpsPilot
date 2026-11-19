import json
import subprocess
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import pytest

from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import (
    DiagnosisReport,
    ProcessingResult,
    UnavailableOutcome,
    run_diagnosis,
)
from opspilot.task_store import StoredReport, save_report


@pytest.fixture
def cli_directory() -> Iterator[Path]:
    with TemporaryDirectory(prefix="opspilot-test-") as directory:
        yield Path(directory)


def _run_cli(
    cwd: Path, *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *arguments],
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _diagnose_from_cli(scenario: str, directory: Path) -> StoredReport:
    question = "消息已入队但未消费"
    result = _run_cli(directory, "diagnose", "--scenario", scenario, question)
    report = json.loads(result.stdout)

    assert report["question"] == question
    for evidence in report["evidence"]:
        timestamp = datetime.fromisoformat(evidence["observed_at"])
        assert timestamp.utcoffset() == timedelta(0)
    assert [(call["tool"], call["outcome"]) for call in report["tool_calls"]] == [
        (evidence["tool"], evidence["outcome"]) for evidence in report["evidence"]
    ]
    assert all(call["duration_ms"] >= 0 for call in report["tool_calls"])
    return report


@pytest.mark.parametrize(
    ("scenario", "status", "pending_messages"),
    [
        ("normal", "no_backlog_observed", 0),
        ("consumer-stopped", "consumer_stopped", 3),
    ],
)
def test_cli_diagnoses_simulated_queue(
    scenario: str, status: str, pending_messages: int, cli_directory: Path
) -> None:
    report = _diagnose_from_cli(scenario, cli_directory)

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


def test_queue_timeout_leaves_queue_depth_unknown(cli_directory: Path) -> None:
    report = _diagnose_from_cli("queue-timeout", cli_directory)

    assert report["status"] == "undetermined"
    assert "Queue inspection timed out" in report["conclusion"]
    (queue,) = report["evidence"]
    assert queue["tool"] == "inspect_queue"
    assert queue["source"] == "simulated:queue"
    assert queue["outcome"] == "timeout"
    assert "pending_messages" not in queue


def test_missing_consumer_data_preserves_observed_backlog(cli_directory: Path) -> None:
    report = _diagnose_from_cli("consumer-data-missing", cli_directory)

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
    scenario: str,
    correlation: str,
    message_id: str,
    explanation: str,
    cli_directory: Path,
) -> None:
    report = _diagnose_from_cli(scenario, cli_directory)

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


def test_tool_call_durations_are_measured_individually(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = iter((0, 1_000_000, 4_000_000, 6_500_000, 7_000_000, 10_000_000))
    monkeypatch.setattr("opspilot.core.diagnosis.perf_counter_ns", lambda: next(ticks))
    tools = SimulatedQueueConnector(pending_messages=3, consumer_running=True)
    adapter = SimulatedProcessingAdapter(latest_result="timeout")

    report = run_diagnosis("Messages are stuck in the queue", tools, adapter)

    assert report["tool_calls"] == [
        {"tool": "inspect_queue", "outcome": "observed", "duration_ms": 1.0},
        {"tool": "inspect_consumer", "outcome": "observed", "duration_ms": 2.5},
        {"tool": "inspect_processing", "outcome": "timeout", "duration_ms": 3.0},
    ]


def test_reports_are_retrievable_across_cli_processes(cli_directory: Path) -> None:
    database = cli_directory / "reports # local" / "diagnoses.sqlite3"
    database_option = ("--db", str(database))
    first = json.loads(
        _run_cli(
            cli_directory,
            *database_option,
            "diagnose",
            "--scenario",
            "normal",
            "Check queue",
        ).stdout
    )
    second = json.loads(
        _run_cli(
            cli_directory,
            *database_option,
            "diagnose",
            "--scenario",
            "processing-failed",
            "消费者在运行，为什么队列还有积压？",
        ).stdout
    )

    assert database.is_file()
    assert first["task_id"] != second["task_id"]
    for report in (first, second):
        assert UUID(report["task_id"]).version == 4
        result = _run_cli(cli_directory, *database_option, "report", report["task_id"])
        assert json.loads(result.stdout) == report


def test_early_report_can_be_read_without_tool_call_data(cli_directory: Path) -> None:
    database = cli_directory / "reports.sqlite3"
    legacy: DiagnosisReport = {
        "question": "Check queue",
        "status": "no_backlog_observed",
        "conclusion": "No pending messages were observed in the queue.",
        "processing_correlation": None,
        "evidence": [SimulatedQueueConnector(0, True).inspect_queue()],
    }
    stored = save_report(database, legacy)

    result = _run_cli(cli_directory, "--db", str(database), "report", stored["task_id"])

    assert json.loads(result.stdout) == stored


def test_report_lookup_with_no_database_does_not_create_one(cli_directory: Path) -> None:
    task_id = "f0f34b73-e48f-4ea5-b689-02d2edf3e301"

    result = _run_cli(cli_directory, "report", task_id, check=False)

    assert result.returncode == 1
    assert not result.stdout
    assert f"no report found for task {task_id}" in result.stderr
    assert not (cli_directory / ".opspilot").exists()
