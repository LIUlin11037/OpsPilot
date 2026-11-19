"""Read-only business processing results from the simulated target."""

from dataclasses import dataclass
from datetime import datetime, timezone

from opspilot.core.diagnosis import (
    Observation,
    ProcessingEvidence,
    ProcessingResult,
    UnavailableEvidence,
    UnavailableOutcome,
)


@dataclass(frozen=True)
class SimulatedProcessingAdapter:
    latest_result: ProcessingResult | UnavailableOutcome = "missing"
    message_id: str | None = None

    def inspect_processing(self) -> ProcessingEvidence | UnavailableEvidence:
        observation: Observation = {
            "source": "simulated:processing",
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        result = self.latest_result
        if result == "timeout" or result == "missing":
            return {**observation, "tool": "inspect_processing", "outcome": result}

        return {
            **observation,
            "tool": "inspect_processing",
            "outcome": "observed",
            "result": result,
            "message_id": self.message_id,
        }
