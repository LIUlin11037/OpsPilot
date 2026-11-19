import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.json_processing import JsonProcessingAdapter

RECORD = {
    "queue": "orders",
    "vhost": "/",
    "message_id": "msg-101",
    "result": "failed",
    "attempted_at": "2026-11-19T03:00:00Z",
}


def test_reads_utf8_json_processing_record() -> None:
    with TemporaryDirectory(prefix="opspilot-record-test-") as directory:
        path = Path(directory) / "processing.json"
        path.write_text(json.dumps(RECORD), encoding="utf-8-sig")
        evidence = JsonProcessingAdapter(path, "orders", "/").inspect_processing()

    assert evidence["outcome"] == "observed"
    assert evidence["source"] == path.as_uri()
    assert evidence["message_id"] == "msg-101"
    assert evidence["result"] == "failed"
    assert evidence["attempted_at"] == "2026-11-19T03:00:00+00:00"


@pytest.mark.parametrize(
    ("contents", "reason"),
    [
        (None, "record_not_found"),
        ("{", "invalid_record"),
        (json.dumps({**RECORD, "result": "unknown"}), "invalid_record"),
        (json.dumps({**RECORD, "attempted_at": "2026-11-19T03:00:00"}), "invalid_record"),
        (json.dumps({**RECORD, "queue": "other"}), "target_mismatch"),
        (json.dumps({**RECORD, "vhost": "other"}), "target_mismatch"),
    ],
)
def test_unusable_processing_record_is_evidence(
    contents: str | None, reason: str
) -> None:
    with TemporaryDirectory(prefix="opspilot-record-test-") as directory:
        path = Path(directory) / "processing.json"
        if contents is not None:
            path.write_text(contents, encoding="utf-8")
        evidence = JsonProcessingAdapter(path, "orders", "/").inspect_processing()

    assert evidence["outcome"] == "missing"
    assert evidence["reason"] == reason
    assert "result" not in evidence
