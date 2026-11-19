import json
import subprocess
import sys
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.hirehub_snapshot import HireHubSnapshotAdapter
from opspilot.core.target_trace import run_target_trace
from opspilot.report_comparison import compare_trace_reports
from opspilot.task_store import StoredReport, list_reports, save_report

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/hirehub-notification-snapshot.json"


@pytest.fixture
def saved_pair() -> Iterator[tuple[Path, StoredReport, StoredReport]]:
    with TemporaryDirectory(prefix="opspilot-comparison-test-") as directory:
        path = Path(directory)
        database = path / "reports.sqlite3"
        snapshot = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        baseline = path / "baseline.json"
        baseline.write_text(json.dumps(snapshot), encoding="utf-8")
        before = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(baseline))
        snapshot["notification"]["rows"] = [{
            "message_id": snapshot["notification"]["message_id"],
            "consumer": "notification",
            "recorded_at": "2026-11-19T03:11:00Z",
        }]
        snapshot["queue"].update(pending_messages=0, registered_consumers=1)
        follow_up = path / "follow-up.json"
        follow_up.write_text(json.dumps(snapshot), encoding="utf-8")
        after = run_target_trace("Check notification again", 1001, HireHubSnapshotAdapter(follow_up))
        yield database, save_report(database, before), save_report(database, after)


def test_compare_hirehub_reports_returns_the_changed_observations(saved_pair) -> None:
    database, before, after = saved_pair
    original = database.read_bytes()
    result = subprocess.run(
        [
            sys.executable, "-m", "opspilot", "--db", str(database), "compare-hirehub",
            before["task_id"], after["task_id"],
        ],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "target": {
            "environment": "hirehub-acceptance-sample",
            "vhost": "/",
            "entity_id": 1001,
            "message_id": "sample-delivery-event-1001",
        },
        "changes": [
            {"field": "branches.notification", "before": "not_observed", "after": "observed"},
            {"field": "queue.pending_messages", "before": 1, "after": 0},
            {"field": "queue.registered_consumers", "before": 0, "after": 1},
        ],
    }
    assert database.read_bytes() == original


def test_compare_keeps_unavailable_queue_distinct_from_observed_zero(saved_pair) -> None:
    _, _, after = saved_pair
    unavailable = deepcopy(after)
    queue = next(check for check in unavailable["evidence"] if check.get("checkpoint") == "queue")
    queue["outcome"] = "timeout"

    assert compare_trace_reports(after, unavailable)["changes"] == [
        {"field": "queue.outcome", "before": "observed", "after": "timeout"},
        {"field": "queue.pending_messages", "before": 0, "after": None},
        {"field": "queue.registered_consumers", "before": 1, "after": None},
        {"field": "queue.unacknowledged_messages", "before": 0, "after": None},
    ]
    assert compare_trace_reports(after, after)["changes"] == []


def test_compare_preserves_valid_counts_from_a_partial_queue_response(saved_pair) -> None:
    _, _, before = saved_pair
    partial = deepcopy(before)
    queue = next(check for check in partial["evidence"] if check.get("checkpoint") == "queue")
    queue.update(
        outcome="missing", reason="metric_missing", http_status=200,
        unacknowledged_messages=4, registered_consumers=0,
    )

    assert compare_trace_reports(before, partial)["changes"] == [
        {"field": "queue.outcome", "before": "observed", "after": "missing"},
        {"field": "queue.pending_messages", "before": 0, "after": None},
        {"field": "queue.registered_consumers", "before": 1, "after": 0},
        {"field": "queue.unacknowledged_messages", "before": 0, "after": 4},
    ]


@pytest.mark.parametrize(
    ("field", "value"), [
        ("environment", "other-environment"), ("vhost", "/other"),
        ("entity_id", 1002), ("message_id", "different-event"),
    ]
)
def test_compare_rejects_a_different_trace_scope(saved_pair, field, value) -> None:
    _, before, after = saved_pair
    after["target_trace"][field] = value

    with pytest.raises(ValueError, match="Reports must match"):
        compare_trace_reports(before, after)


def test_compare_requires_resolved_trace_identity(saved_pair) -> None:
    _, before, after = saved_pair
    after["target_trace"]["message_id"] = None
    with pytest.raises(ValueError, match="must resolve a message ID"):
        compare_trace_reports(before, after)

    after.pop("target_trace")
    with pytest.raises(ValueError, match="must refer to HireHub trace reports"):
        compare_trace_reports(before, after)


def test_reports_filters_hirehub_history_before_applying_limit(saved_pair) -> None:
    database, before, after = saved_pair
    other_environment = deepcopy(after)
    other_environment["target_trace"]["environment"] = "other-lab"
    foreign = save_report(database, other_environment)
    other_delivery = deepcopy(after)
    other_delivery["target_trace"]["entity_id"] = 1002
    save_report(database, other_delivery)
    save_report(database, {
        "question": "Check queue",
        "status": "undetermined",
        "conclusion": "Queue depth is unknown.",
        "processing_correlation": None,
        "evidence": [],
    })
    original = database.read_bytes()

    result = subprocess.run(
        [
            sys.executable, "-m", "opspilot", "--db", str(database), "reports",
            "--delivery-id", "1001", "--environment", "hirehub-acceptance-sample", "--limit", "2",
        ],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )

    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    assert [row["task_id"] for row in rows] == [after["task_id"], before["task_id"]]
    assert [row["target_trace"] for row in rows] == [after["target_trace"], before["target_trace"]]
    assert [row["task_id"] for row in list_reports(database, limit=1, delivery_id=1001)] == [
        foreign["task_id"]
    ]
    assert list_reports(database, limit=20, environment="missing-lab") == []
    assert database.read_bytes() == original
