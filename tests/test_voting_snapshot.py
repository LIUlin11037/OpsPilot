import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.voting_snapshot import VotingSnapshotAdapter
from opspilot.core.target_trace import run_target_trace

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
