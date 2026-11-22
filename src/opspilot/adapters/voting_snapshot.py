"""Project read-only observations of a voter in Docker's Example Voting App."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from opspilot.core.diagnosis import ObservationOutcome, TraceEvidence
from opspilot.core.target_trace import TargetTraceRead

_ORIGINS = {"queued_vote": "Redis/votes", "stored_vote": "PostgreSQL/public.votes"}


def _timestamp(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        stamp = datetime.fromisoformat(value)
        return stamp.isoformat() if stamp.utcoffset() is not None else None
    except ValueError:
        return None


def _observation(snapshot: dict[str, object], branch: str, source: str, captured_at: str) -> TraceEvidence:
    evidence: TraceEvidence = {
        "tool": "inspect_target_trace", "checkpoint": branch, "branch": branch,
        "origin": _ORIGINS[branch], "source": f"{source}#{branch}",
        "observed_at": captured_at, "outcome": "missing", "reason": "invalid_observation",
    }
    if branch not in snapshot:
        evidence["reason"] = "section_not_captured"
        return evidence
    section = snapshot[branch]
    if not isinstance(section, dict):
        return evidence
    observed_at = _timestamp(section.get("observed_at"))
    if observed_at is None:
        return evidence
    evidence["observed_at"] = observed_at
    outcome = section.get("outcome")
    count = section.get("row_count")
    if outcome == "observed" and type(count) is int and count >= 0:
        evidence["outcome"] = "observed"
        evidence["row_count"] = count
        del evidence["reason"]
    elif outcome == "timeout":
        evidence["outcome"] = "timeout"
        evidence["reason"] = "request_timeout"
    elif outcome == "missing":
        evidence["reason"] = "source_unavailable"
    return evidence


@dataclass(frozen=True)
class VotingSnapshotAdapter:
    path: Path
    branches = tuple(_ORIGINS)

    def inspect_trace(self, entity_id: int) -> TargetTraceRead:
        source = self.path.absolute().as_uri()
        try:
            snapshot = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except OSError:
            return self._unavailable(source, "snapshot_unreadable")
        except (ValueError, RecursionError):
            return self._unavailable(source, "invalid_snapshot")
        if not isinstance(snapshot, dict):
            return self._unavailable(source, "invalid_snapshot")
        environment = snapshot.get("environment")
        captured_at = _timestamp(snapshot.get("captured_at"))
        voter_id = snapshot.get("voter_id")
        if not isinstance(environment, str) or not environment.strip() or captured_at is None:
            return self._unavailable(source, "invalid_snapshot")
        if type(voter_id) is not int or voter_id <= 0:
            return self._unavailable(source, "invalid_snapshot")
        if voter_id != entity_id:
            return self._unavailable(source, "voter_id_mismatch")

        checks = [_observation(snapshot, branch, source, captured_at) for branch in self.branches]
        outcomes = {check["outcome"] for check in checks}
        outcome: ObservationOutcome = "missing"
        if "observed" in outcomes:
            outcome = "observed"
        elif "timeout" in outcomes:
            outcome = "timeout"
        return {
            "environment": environment, "vhost": None, "message_id": None,
            "checks": checks, "outcome": outcome,
        }

    @staticmethod
    def _unavailable(source: str, reason: str) -> TargetTraceRead:
        return {
            "environment": None, "vhost": None, "message_id": None, "outcome": "missing",
            "checks": [{
                "tool": "inspect_target_trace", "checkpoint": "snapshot", "origin": "snapshot_file",
                "source": source, "observed_at": datetime.now(timezone.utc).isoformat(),
                "outcome": "missing", "reason": reason,
            }],
        }
