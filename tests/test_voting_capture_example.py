import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from examples import capture_voting_snapshot
from opspilot.adapters.voting_snapshot import VotingSnapshotAdapter

COMPOSE = ["docker", "compose", "--project-name", "example"]
VOTER_ID = "001abc"


def test_capture_uses_read_only_commands_and_round_trips_the_snapshot(monkeypatch) -> None:
    def run(command, *, input, **options):
        assert command[:len(COMPOSE)] == COMPOSE
        assert options["check"] and options["timeout"] > 0
        if "redis-cli" in command:
            assert command[-5:-1] == ["--json", "LRANGE", "votes", "0"]
            return subprocess.CompletedProcess(command, 0, json.dumps([
                json.dumps({"voter_id": VOTER_ID, "vote": "raw-vote"}),
                json.dumps({"voter_id": "001ABC", "vote": "raw-vote"}),
                json.dumps({"voter_id": "1abc", "vote": "raw-vote"}),
            ]))
        assert "PGOPTIONS=-c default_transaction_read_only=on" in command
        assert f"--set=voter_id={VOTER_ID}" in command
        assert input == "SELECT count(*) FROM public.votes WHERE id = :'voter_id';\n"
        return subprocess.CompletedProcess(command, 0, "0\n")

    monkeypatch.setattr(capture_voting_snapshot.subprocess, "run", run)
    snapshot = capture_voting_snapshot.capture_snapshot(COMPOSE, "voting-test", VOTER_ID)
    payload = json.dumps(snapshot)
    assert "raw-vote" not in payload

    with TemporaryDirectory(prefix="voting-capture-") as directory:
        path = Path(directory) / "snapshot.json"
        path.write_text(payload, encoding="utf-8")
        queued, stored = VotingSnapshotAdapter(path).inspect_trace(VOTER_ID)["checks"]
    assert (queued["outcome"], queued["row_count"]) == ("observed", 1)
    assert (stored["outcome"], stored["row_count"]) == ("observed", 0)


@pytest.mark.parametrize("redis_outcome", ["timeout", "missing"])
def test_capture_records_incomplete_redis_reads_and_continues_to_postgres(monkeypatch, redis_outcome) -> None:
    def run(command, *, timeout, **_options):
        if "redis-cli" not in command:
            return subprocess.CompletedProcess(command, 0, "1\n")
        if redis_outcome == "timeout":
            raise subprocess.TimeoutExpired(command, timeout)
        return subprocess.CompletedProcess(command, 0, json.dumps(["{}"] * 1001))

    monkeypatch.setattr(capture_voting_snapshot.subprocess, "run", run)
    snapshot = capture_voting_snapshot.capture_snapshot(COMPOSE, "voting-test", VOTER_ID)
    assert snapshot["queued_vote"]["outcome"] == redis_outcome
    assert "row_count" not in snapshot["queued_vote"]
    assert snapshot["stored_vote"]["outcome"] == "observed"
    assert snapshot["stored_vote"]["row_count"] == 1
