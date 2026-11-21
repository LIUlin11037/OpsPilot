"""Read UTF-8 JSON configuration objects and their timeout values."""

import json
from math import isfinite
from pathlib import Path


def read_config_object(path: Path, label: str) -> dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except OSError as error:
        raise ValueError(f"Cannot read the {label.lower()} configuration file: {error.strerror}.") from error
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} configuration must be valid UTF-8 JSON.") from error
    if not isinstance(data, dict):
        raise ValueError(f"{label} configuration must be a JSON object.")
    return data


def parse_timeout_seconds(value: object) -> float:
    message = "timeout_seconds must be a finite positive number."
    if type(value) not in (float, int):
        raise ValueError(message)
    try:
        seconds = float(value)
    except OverflowError as error:
        raise ValueError(message) from error
    if not isfinite(seconds) or seconds <= 0:
        raise ValueError(message)
    return seconds
