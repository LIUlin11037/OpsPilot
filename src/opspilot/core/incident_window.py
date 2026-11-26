"""A bounded interval shared by historical diagnostic observations."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

METRIC_RATE_SECONDS = 60


@dataclass(frozen=True)
class IncidentWindow:
    start: datetime
    end: datetime

    @property
    def duration_seconds(self) -> int:
        return int((self.end - self.start).total_seconds())

    @property
    def evaluation_start(self) -> datetime:
        return self.start + timedelta(seconds=METRIC_RATE_SECONDS)

    def metric_step(self, configured_seconds: int) -> int:
        return min(configured_seconds, self.duration_seconds - METRIC_RATE_SECONDS)


def _timestamp(value: str, option: str) -> datetime:
    try:
        stamp = datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value)
        if stamp.utcoffset() is None or stamp.microsecond:
            raise ValueError
        return stamp.astimezone(timezone.utc)
    except (ValueError, OverflowError) as error:
        raise ValueError(f"{option} requires a whole-second ISO 8601 timestamp with a time zone.") from error


def parse_incident_window(start: str | None, end: str | None) -> IncidentWindow:
    if start is None or end is None:
        raise ValueError("--incident-start and --incident-end must be supplied together.")
    window = IncidentWindow(_timestamp(start, "--incident-start"), _timestamp(end, "--incident-end"))
    if not METRIC_RATE_SECONDS + 5 <= window.duration_seconds <= 900:
        raise ValueError("The incident window must be between 65 and 900 seconds.")
    if window.end > datetime.now(timezone.utc):
        raise ValueError("--incident-end must be at or before the current time.")
    return window
