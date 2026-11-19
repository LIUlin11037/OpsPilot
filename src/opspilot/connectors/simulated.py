"""Read-only observations from an in-process queue simulation."""

from dataclasses import dataclass
from datetime import datetime, timezone

from opspilot.core.diagnosis import ConsumerEvidence, QueueEvidence


@dataclass(frozen=True)
class SimulatedQueueConnector:
    pending_messages: int
    consumer_running: bool

    def inspect_queue(self) -> QueueEvidence:
        return {
            "tool": "inspect_queue",
            "source": "simulated:queue",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "pending_messages": self.pending_messages,
        }

    def inspect_consumer(self) -> ConsumerEvidence:
        return {
            "tool": "inspect_consumer",
            "source": "simulated:consumer",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "running": self.consumer_running,
        }
