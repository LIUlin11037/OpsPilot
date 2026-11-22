import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.voting_snapshot import VotingSnapshotAdapter
from opspilot.core.target_trace import run_target_trace
from opspilot.report_comparison import compare_voting_reports
from opspilot.task_store import save_report

VOTER_ID = 102431
TIMESTAMP = "2026-11-22T05:00:00Z"


def _snapshot(queued: dict[str, object], stored: dict[str, object], voter_id: int | str = VOTER_ID) -> str:
    return json.dumps({
        "environment": "voting-test", "voter_id": voter_id, "captured_at": TIMESTAMP,
        "queued_vote": {"observed_at": TIMESTAMP, **queued},
        "stored_vote": {"observed_at": TIMESTAMP, **stored},
    })


@pytest.mark.parametrize("voter_id", [VOTER_ID, "38b48362514db77", "001234"])
def test_voting_cli_saves_a_target_trace_for_a_queued_vote(voter_id) -> None:
    with TemporaryDirectory(prefix="opspilot-voting-") as directory:
        snapshot = Path(directory) / "voting.json"
        snapshot.write_text(_snapshot(
            {"outcome": "observed", "row_count": 1}, {"outcome": "observed", "row_count": 0}, voter_id,
        ), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-m", "opspilot", "diagnose-voting", "--snapshot", str(snapshot),
             "--voter-id", str(voter_id), "Was the vote stored?"],
            cwd=directory, capture_output=True, encoding="utf-8", timeout=20,
        )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["target_trace"] == {
            "environment": "voting-test", "vhost": None, "entity_id": voter_id, "message_id": None,
            "branches": {"queued_vote": "observed", "stored_vote": "not_observed"},
        }
        assert report["evidence"][0]["source"] == f"{snapshot.as_uri()}#queued_vote"
        rendered = subprocess.run(
            [sys.executable, "-m", "opspilot", "report", report["task_id"], "--format", "markdown"],
            cwd=directory, capture_output=True, encoding="utf-8", timeout=20,
        )
        assert rendered.returncode == 0, rendered.stderr
        assert f"**Entity ID** {voter_id}" in rendered.stdout
        assert r"**stored\_vote branch** not\_observed" in rendered.stdout


@pytest.mark.parametrize(("stored", "expected", "reason"), [
    ({"outcome": "observed", "row_count": 1}, "observed", None),
    ({"outcome": "timeout"}, "unknown", "request_timeout"),
    ({"outcome": "observed", "row_count": False}, "unknown", "invalid_observation"),
])
def test_voting_keeps_zero_matches_distinct_from_unavailable_evidence(stored, expected, reason) -> None:
    with TemporaryDirectory(prefix="opspilot-voting-") as directory:
        snapshot = Path(directory) / "voting.json"
        snapshot.write_text(_snapshot({"outcome": "observed", "row_count": 0}, stored), encoding="utf-8-sig")
        report = run_target_trace("Was the vote stored?", VOTER_ID, VotingSnapshotAdapter(snapshot))

    assert report["target_trace"]["branches"] == {"queued_vote": "not_observed", "stored_vote": expected}
    assert report["evidence"][1].get("reason") == reason
    assert report["evidence"][1].get("row_count") == (1 if reason is None else None)


@pytest.mark.parametrize(("snapshot_id", "requested_id"), [
    (VOTER_ID, VOTER_ID + 1), ("001234", 1234), ("abcd", "ABCD"),
])
def test_voting_snapshot_does_not_apply_to_another_voter(snapshot_id, requested_id) -> None:
    with TemporaryDirectory(prefix="opspilot-voting-") as directory:
        snapshot = Path(directory) / "voting.json"
        snapshot.write_text(_snapshot(
            {"outcome": "observed", "row_count": 0}, {"outcome": "observed", "row_count": 1}, snapshot_id,
        ), encoding="utf-8")
        trace = VotingSnapshotAdapter(snapshot).inspect_trace(requested_id)

    assert trace["outcome"] == "missing"
    assert trace["checks"][0]["reason"] == "voter_id_mismatch"


@pytest.fixture
def saved_voting_pair():
    with TemporaryDirectory(prefix="opspilot-voting-comparison-") as directory:
        snapshot = Path(directory) / "voting.json"
        database = Path(directory) / "reports.sqlite3"
        reports = []
        for queued_count, stored_count in ((1, 0), (0, 1)):
            snapshot.write_text(_snapshot(
                {"outcome": "observed", "row_count": queued_count},
                {"outcome": "observed", "row_count": stored_count}, "001abc",
            ), encoding="utf-8")
            report = run_target_trace("Was the vote stored?", "001abc", VotingSnapshotAdapter(snapshot))
            reports.append(save_report(database, report))
        yield database, *reports


def test_compare_voting_shows_the_saved_transition_in_json_and_markdown(saved_voting_pair) -> None:
    database, before, after = saved_voting_pair
    original = database.read_bytes()
    command = [sys.executable, "-m", "opspilot", "--db", str(database), "compare-voting",
               before["task_id"], after["task_id"]]
    result = subprocess.run(command, capture_output=True, encoding="utf-8", timeout=20)
    assert result.returncode == 0, result.stderr
    comparison = json.loads(result.stdout)
    assert comparison == {
        "before_task_id": before["task_id"], "after_task_id": after["task_id"],
        "target": {"environment": "voting-test", "voter_id": "001abc"},
        "changes": [
            {"field": "branches.queued_vote", "before": "observed", "after": "not_observed"},
            {"field": "branches.stored_vote", "before": "not_observed", "after": "observed"},
            {"field": "queued_vote.row_count", "before": 1, "after": 0},
            {"field": "stored_vote.row_count", "before": 0, "after": 1},
        ],
    }

    markdown = subprocess.run([*command, "--format", "markdown"], capture_output=True, encoding="utf-8", timeout=20)
    assert markdown.returncode == 0, markdown.stderr
    assert "**Voter ID** 001abc" in markdown.stdout
    assert "| Redis matching rows | 1 | 0 |" in markdown.stdout
    assert "| PostgreSQL matching rows | 0 | 1 |" in markdown.stdout
    assert database.read_bytes() == original


@pytest.mark.parametrize(("field", "value"), [
    ("environment", "other-environment"), ("entity_id", "1abc"), ("entity_id", "001ABC"),
])
def test_compare_voting_rejects_reports_from_another_scope(saved_voting_pair, field, value) -> None:
    _, before, after = saved_voting_pair
    after["target_trace"][field] = value
    with pytest.raises(ValueError, match="match on environment and exact voter ID"):
        compare_voting_reports(before, after)


def test_compare_voting_preserves_the_difference_between_zero_and_timeout(saved_voting_pair) -> None:
    _, before, after = saved_voting_pair
    timed_out = deepcopy(after)
    timed_out["target_trace"]["branches"]["stored_vote"] = "unknown"
    observation = timed_out["evidence"][1]
    observation["outcome"] = "timeout"
    observation["reason"] = "request_timeout"
    del observation["row_count"]

    changes = compare_voting_reports(before, timed_out)["changes"]
    assert {"field": "stored_vote.row_count", "before": 0, "after": None} in changes
    assert {"field": "stored_vote.outcome", "before": "observed", "after": "timeout"} in changes
    assert {"field": "stored_vote.reason", "before": None, "after": "request_timeout"} in changes
