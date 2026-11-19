"""Read-only observations from an in-process queue simulation."""

from dataclasses import dataclass
from datetime import datetime, timezone

from opspilot.core.diagnosis import (
    ConsumerEvidence,
    Observation,
    QueueEvidence,
    UnavailableEvidence,
    UnavailableOutcome,
    UnavailableQueueEvidence,
)


@dataclass(frozen=True)
class SimulatedQueueConnector:
    pending_messages: int | UnavailableOutcome
    consumer_running: bool | UnavailableOutcome
    sampled_pending_message_ids: tuple[str, ...] = ()

    def inspect_queue(self) -> QueueEvidence | UnavailableQueueEvidence:
        observation: Observation = {
            "source": "simulated:queue",
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        pending_messages = self.pending_messages
        if isinstance(pending_messages, str):
            return {**observation, "tool": "inspect_queue", "outcome": pending_messages}

        return {
            **observation,
            "tool": "inspect_queue",
            "outcome": "observed",
            "pending_messages": pending_messages,
            "sampled_pending_message_ids": list(self.sampled_pending_message_ids),
        }

    def inspect_consumer(self) -> ConsumerEvidence | UnavailableEvidence:
        observation: Observation = {
            "source": "simulated:consumer",
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        running = self.consumer_running
        if isinstance(running, str):
            return {**observation, "tool": "inspect_consumer", "outcome": running}

        return {
            **observation,
            "tool": "inspect_consumer",
            "outcome": "observed",
            "running": running,
        }
