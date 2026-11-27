import hashlib
import json
import sqlite3
from contextlib import closing
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from opspilot.handoff import export_reports
from opspilot.task_store import insert_report, list_reports, load_report, save_report


def _import(package, database):
    from opspilot.handoff import import_reports

    return import_reports(package, database)


@pytest.fixture
def packaged_reports(tmp_path):
    database = tmp_path / "source.sqlite3"
    reports = [
        save_report(database, {
            "question": question,
            "status": "undetermined",
            "conclusion": "Recorded evidence.",
            "processing_correlation": None,
            "evidence": [{
                "tool": "inspect_service_health",
                "outcome": "observed",
                "source": "http://127.0.0.1:18771/orders/",
                "observed_at": "2026-11-26T11:23:54+08:00",
                "health": "unhealthy",
                "http_status": 504,
            }],
        })
        for question in ("original fault", "original recovery")
    ]
    package = tmp_path / "source.zip"
    export_reports(database, package, [r["task_id"] for r in reports])
    return package, reports


def test_import_preserves_original_reports_and_insertion_order(tmp_path, packaged_reports):
    package, reports = packaged_reports
    database = tmp_path / "receiver" / "reports.sqlite3"
    ids = [r["task_id"] for r in reports]

    assert _import(package, database) == ids
    assert [load_report(database, task_id) for task_id in ids] == reports
    assert [r["task_id"] for r in list_reports(database, limit=5)] == ids[::-1]


@pytest.mark.parametrize(("kind", "message"), [
    ("tampered", "checksum"),
    ("version", "version"),
    ("duplicate_member", "duplicate"),
    ("extra_member", "entries"),
    ("oversized", "size limit"),
    ("duplicate_id", "duplicate"),
    ("invalid_report", "Invalid report"),
])
def test_invalid_archives_leave_destination_absent(tmp_path, packaged_reports, kind, message):
    original, _ = packaged_reports
    package = tmp_path / "edited.zip"
    destination = tmp_path / "absent.sqlite3"
    with ZipFile(original) as source:
        contents = {name: source.read(name) for name in source.namelist()}

    manifest = json.loads(contents["manifest.json"])
    if kind == "tampered":
        contents["reports/1.md"] = b"Different evidence text"
    elif kind == "version":
        manifest["version"] = 99
    elif kind == "extra_member":
        contents["../outside.md"] = b"Unexpected file"
    elif kind == "oversized":
        contents["reports/1.md"] = b"x" * (32 * 1024 * 1024)
    elif kind == "duplicate_id":
        manifest["reports"][1]["task_id"] = manifest["reports"][0]["task_id"]
    elif kind == "invalid_report":
        report = json.loads(contents["reports/2.json"])
        report["evidence"] = "not an evidence list"
        contents["reports/2.json"] = json.dumps(report).encode()
        manifest["reports"][1]["json_sha256"] = hashlib.sha256(contents["reports/2.json"]).hexdigest()
    contents["manifest.json"] = json.dumps(manifest).encode()
    with ZipFile(package, "w", compression=ZIP_DEFLATED) as archive:
        for name, data in contents.items():
            archive.writestr(name, data)
        if kind == "duplicate_member":
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr("reports/1.md", contents["reports/1.md"])

    with pytest.raises(ValueError, match=message):
        _import(package, destination)

    assert not destination.exists()
    assert not (tmp_path.parent / "outside.md").exists()


def test_existing_task_conflict_rejects_all_rows(tmp_path, packaged_reports):
    package, reports = packaged_reports
    database = tmp_path / "receiver.sqlite3"
    conflict = reports[1]
    with closing(sqlite3.connect(database)) as conn, conn:
        insert_report(conn, conflict["task_id"], {
            "question": "already on the receiving machine",
            "status": "undetermined",
            "conclusion": "Existing.",
            "processing_correlation": None,
            "evidence": [],
        })
    original = database.read_bytes()

    with pytest.raises(ValueError, match=conflict["task_id"]):
        _import(package, database)

    assert load_report(database, reports[0]["task_id"]) is None
    assert database.read_bytes() == original
