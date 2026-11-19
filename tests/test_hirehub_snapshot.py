import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.hirehub_snapshot import HireHubSnapshotAdapter
from opspilot.core.target_trace import run_target_trace

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/hirehub-notification-snapshot.json"
MESSAGE_ID = "sample-delivery-event-1001"


def test_snapshot_ignores_invalid_durations_but_keeps_zero() -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    data["delivery"]["duration_ms"] = -1
    data["outbox"]["duration_ms"] = True
    data["job"]["duration_ms"] = float("inf")
    data["notification"]["duration_ms"] = "10"
    data["queue"]["duration_ms"] = 0

    trace = HireHubSnapshotAdapter.inspect_data(data, 1001)

    assert {
        item["checkpoint"]: item["duration_ms"]
        for item in trace["checks"] if "duration_ms" in item
    } == {"queue": 0}


@pytest.mark.parametrize("correct_queue", [True, False])
def test_snapshot_only_keeps_valid_partial_counts_for_the_expected_queue(correct_queue: bool) -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    data["queue"].update(
        outcome="missing", reason="invalid_response", http_status=200,
        pending_messages=2, registered_consumers=0, unacknowledged_messages=True,
    )
    if not correct_queue:
        data["queue"]["name"] = "another.queue"

    trace = HireHubSnapshotAdapter.inspect_data(data, 1001)
    queue = trace["checks"][-1]

    assert queue["checkpoint"] == "queue"
    assert queue["outcome"] == "missing"
    assert "pending_messages" not in queue
    assert "unacknowledged_messages" not in queue
    if correct_queue:
        assert queue["registered_consumers"] == 0
    else:
        assert "registered_consumers" not in queue


def test_hirehub_cli_saves_a_branch_with_no_processing_rows() -> None:
    with TemporaryDirectory(prefix="opspilot-hirehub-test-") as directory:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "opspilot",
                "diagnose-hirehub",
                "--snapshot",
                str(EXAMPLE),
                "--delivery-id",
                "1001",
                "投递成功，但 HR 未收到站内通知",
            ],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        saved = subprocess.run(
            [sys.executable, "-m", "opspilot", "report", report["task_id"]],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=20,
        )
    assert saved.returncode == 0, saved.stderr
    assert json.loads(saved.stdout) == report
    assert report["status"] == "undetermined"
    assert report["processing_correlation"] == "unknown"
    assert report["target_trace"] == {
        "environment": "hirehub-acceptance-sample",
        "vhost": "/",
        "entity_id": 1001,
        "message_id": MESSAGE_ID,
        "branches": {"job": "observed", "notification": "not_observed"},
    }
    checks = {item["checkpoint"]: item for item in report["evidence"]}
    assert all("duration_ms" not in evidence for evidence in checks.values())
    assert checks["delivery"]["status"] == "PENDING"
    assert checks["outbox"]["status"] == "CONFIRMED"
    assert checks["outbox"]["expected_recipient_listed"] is True
    assert checks["outbox"]["acknowledged_at"] == "2026-11-19T03:09:02+00:00"
    assert checks["job"]["message_id"] == MESSAGE_ID
    assert checks["job"]["origin"] == "hirehub_job.mq_consume_log"
    assert checks["job"]["row_count"] == 1
    assert checks["notification"]["origin"] == "hirehub_notification.mq_consume_log"
    assert checks["notification"]["outcome"] == "observed"
    assert checks["notification"]["row_count"] == 0
    assert checks["notification"]["observed_at"] == "2026-11-19T03:10:00+00:00"
    assert checks["queue"]["pending_messages"] == 1
    assert checks["queue"]["registered_consumers"] == 0
    assert "notification lookup succeeded and returned zero processing records" in (
        report["conclusion"]
    )
    call, = report["tool_calls"]
    assert call["tool"] == "inspect_target_trace"
    assert call["outcome"] == "observed"
    assert call["duration_ms"] >= 0


@pytest.mark.parametrize(
    ("changes", "outcome", "reason"),
    [
        ({"outcome": "timeout"}, "timeout", "source_timeout"),
        ({"outcome": "missing"}, "missing", "source_unavailable"),
        ({"outcome": "missing", "reason": "unexpected raw error"}, "missing", "source_unavailable"),
        ({"outcome": "timeout", "reason": "access_denied"}, "timeout", "source_timeout"),
        ({"message_id": "a-different-message"}, "missing", "message_id_mismatch"),
        ({"consumer": "job-delivery-count"}, "missing", "consumer_mismatch"),
        ({"rows": None}, "missing", "invalid_rows"),
    ],
)
def test_unavailable_notification_check_does_not_become_zero_rows(
    changes: dict, outcome: str, reason: str
) -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    data["notification"].update(changes)
    with TemporaryDirectory(prefix="opspilot-hirehub-test-") as directory:
        path = Path(directory) / "snapshot.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        report = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(path))

    assert report["status"] == "undetermined"
    assert report["target_trace"]["branches"] == {"job": "observed", "notification": "unknown"}
    evidence = next(item for item in report["evidence"] if item["checkpoint"] == "notification")
    assert evidence["message_id"] == MESSAGE_ID
    assert evidence["outcome"] == outcome
    assert evidence["reason"] == reason
    assert "row_count" not in evidence
    assert "notification processing status is unknown" in report["conclusion"]


@pytest.mark.parametrize("queue_captured", [True, False])
def test_ambiguous_outbox_stops_the_message_trace(queue_captured: bool) -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    outbox = data["outbox"]["rows"]
    outbox.append({**outbox[0], "message_id": "another-creation-event"})
    if not queue_captured:
        data.pop("queue")
    with TemporaryDirectory(prefix="opspilot-hirehub-test-") as directory:
        path = Path(directory) / "snapshot.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        report = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(path))

    assert report["target_trace"]["message_id"] is None
    assert report["target_trace"]["branches"] == {"job": "unknown", "notification": "unknown"}
    checkpoints = ["delivery", "outbox"]
    if queue_captured:
        checkpoints.append("queue")
        assert report["evidence"][2]["pending_messages"] == 1
    assert [item["checkpoint"] for item in report["evidence"]] == checkpoints
    assert report["evidence"][1]["reason"] == "ambiguous_rows"
    follow_up = report["next_checks"][0]
    assert (follow_up["action"], follow_up["evidence_index"]) == ("verify_lookup", 1)


def test_snapshot_scope_must_match_the_requested_delivery() -> None:
    report = run_target_trace("Check notification", 1002, HireHubSnapshotAdapter(EXAMPLE))

    assert report["target_trace"]["entity_id"] == 1002
    assert report["target_trace"]["message_id"] is None
    evidence, = report["evidence"]
    assert evidence["checkpoint"] == "snapshot"
    assert evidence["reason"] == "delivery_id_mismatch"
