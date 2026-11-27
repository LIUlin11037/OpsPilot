import hashlib
import json
from zipfile import ZipFile

import pytest

from opspilot.task_store import save_report


def _export(database, output, task_ids):
    from opspilot.handoff import export_reports

    return export_reports(database, output, task_ids)


@pytest.fixture
def source(tmp_path):
    database = tmp_path / "source.sqlite3"
    reports = [
        save_report(database, {
            "question": question,
            "status": "no_failure_observed",
            "conclusion": "Recorded evidence.",
            "processing_correlation": None,
            "evidence": [],
        })
        for question in ("故障基准", "Unselected private report", "恢复复查")
    ]
    return database, reports


def test_export_contains_selected_reports_in_source_order(tmp_path, source):
    database, reports = source
    output = tmp_path / "handoff.zip"
    original = database.read_bytes()
    expected = [reports[0], reports[2]]

    published = _export(database, output, [reports[2]["task_id"], reports[0]["task_id"]])

    assert published == [item["task_id"] for item in expected]
    with ZipFile(output) as archive:
        assert set(archive.namelist()) == {
            "manifest.json", "reports/1.json", "reports/1.md", "reports/2.json", "reports/2.md",
        }
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["version"] == 1
        assert [item["task_id"] for item in manifest["reports"]] == published
        for index, (entry, report) in enumerate(zip(manifest["reports"], expected), start=1):
            raw_json = archive.read(f"reports/{index}.json")
            raw_md = archive.read(f"reports/{index}.md")
            assert json.loads(raw_json) == report
            assert report["question"] in raw_md.decode("utf-8")
            assert report["task_id"] in raw_md.decode("utf-8")
            assert entry["json_sha256"] == hashlib.sha256(raw_json).hexdigest()
            assert entry["markdown_sha256"] == hashlib.sha256(raw_md).hexdigest()
    assert database.read_bytes() == original


@pytest.mark.parametrize("kind", ["duplicate", "missing", "existing_output"])
def test_rejected_exports_preserve_the_source_and_output(tmp_path, source, kind):
    database, (report, *_) = source
    original = database.read_bytes()
    output = tmp_path / "handoff.zip"
    task_ids = [report["task_id"]]
    if kind == "duplicate":
        task_ids += task_ids
    elif kind == "missing":
        task_ids.append("absent-task")
    else:
        output.write_bytes(b"Existing bundle")

    with pytest.raises(ValueError, match={
        "duplicate": "duplicate", "missing": "absent-task", "existing_output": "already exists",
    }[kind]):
        _export(database, output, task_ids)

    if kind == "existing_output":
        assert output.read_bytes() == b"Existing bundle"
    else:
        assert not output.exists()
    assert database.read_bytes() == original
