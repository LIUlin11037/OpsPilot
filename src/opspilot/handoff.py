"""Export and import explicitly selected saved reports in a local ZIP."""

import hashlib
import json
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from math import isfinite
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from zipfile import ZIP_DEFLATED, ZIP_STORED, BadZipFile, ZipFile, ZipInfo
from zlib import error as ZlibError

from opspilot.report_markdown import render_markdown
from opspilot.task_store import StoredReport, load_reports_for_export, store_imported_reports

_PACKAGE_VERSION = 1
_MAX_REPORTS = 100
_MAX_BYTES = 32 * 1024 * 1024
_MANIFEST = "manifest.json"


def _report_names(index: int) -> tuple[str, str]:
    return f"reports/{index}.json", f"reports/{index}.md"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@contextmanager
def _publish_new_file(destination: Path) -> Iterator[Path]:
    if destination.exists() or destination.is_symlink():
        raise ValueError(f"Output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".opspilot-", dir=destination.parent) as directory:
        staging = Path(directory) / "output"
        yield staging
        try:
            os.link(staging, destination)
        except FileExistsError as error:
            raise ValueError(f"Output already exists: {destination}") from error


def export_reports(database: Path, destination: Path, task_ids: Sequence[str]) -> list[str]:
    if not 1 <= len(task_ids) <= _MAX_REPORTS:
        raise ValueError(f"Select between 1 and {_MAX_REPORTS} task IDs.")
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("Task IDs contain a duplicate.")
    reports = load_reports_for_export(database, task_ids)
    manifest: list[dict[str, str]] = []
    entries: dict[str, bytes] = {}
    total_size = 0
    for index, report in enumerate(reports, start=1):
        json_name, markdown_name = _report_names(index)
        raw_report = _json_bytes(report)
        markdown = render_markdown(report).encode("utf-8")
        total_size += len(raw_report) + len(markdown)
        if total_size > _MAX_BYTES:
            raise ValueError("Report package exceeds the 32 MiB uncompressed size limit.")
        entries[json_name] = raw_report
        entries[markdown_name] = markdown
        manifest.append({
            "task_id": report["task_id"], "json_sha256": _digest(raw_report), "markdown_sha256": _digest(markdown),
        })
    raw_manifest = _json_bytes({"version": _PACKAGE_VERSION, "reports": manifest})
    if total_size + len(raw_manifest) > _MAX_BYTES:
        raise ValueError("Report package exceeds the 32 MiB uncompressed size limit.")
    with _publish_new_file(destination) as staging:
        with ZipFile(staging, "w", compression=ZIP_DEFLATED) as archive:
            archive.writestr(_MANIFEST, raw_manifest)
            for name, content in entries.items():
                archive.writestr(name, content)
    return [report["task_id"] for report in reports]


def _finite_float(text: str) -> float:
    value = float(text)
    if not isfinite(value):
        raise ValueError("JSON contains a non-finite number.")
    return value


def _decode_json(data: bytes, name: str) -> object:
    try:
        return json.loads(data.decode("utf-8"), parse_float=_finite_float, parse_constant=_finite_float)
    except (ValueError, RecursionError) as error:
        raise ValueError(f"Invalid JSON in package entry {name}.") from error


def _read_entry(archive: ZipFile, entry: ZipInfo) -> bytes:
    with archive.open(entry) as stream:
        data = stream.read(entry.file_size + 1)
    if len(data) != entry.file_size:
        raise ValueError(f"Invalid size for package entry {entry.filename}.")
    return data


def _manifest_reports(value: object) -> list[dict[str, str]]:
    if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != _PACKAGE_VERSION:
        raise ValueError("Unsupported report package version.")
    reports = value.get("reports")
    if value.keys() != {"version", "reports"} or not isinstance(reports, list) or not 1 <= len(reports) <= _MAX_REPORTS:
        raise ValueError("Invalid report package manifest.")
    task_ids: set[str] = set()
    for item in reports:
        if (
            not isinstance(item, dict) or item.keys() != {"task_id", "json_sha256", "markdown_sha256"}
            or not all(isinstance(field, str) and field.strip() for field in item.values())
        ):
            raise ValueError("Invalid report package manifest entry.")
        if item["task_id"] in task_ids:
            raise ValueError(f"Report package contains a duplicate task ID: {item['task_id']}")
        task_ids.add(item["task_id"])
    return reports


def _validated_report(value: object, task_id: str) -> StoredReport:
    if (
        not isinstance(value, dict) or value.get("task_id") != task_id
        or any(not isinstance(value.get(field), str) for field in ("question", "conclusion", "status"))
        or "processing_correlation" not in value
        or value["processing_correlation"] is not None and not isinstance(value["processing_correlation"], str)
        or not isinstance(value.get("evidence"), list)
        or any(
            not isinstance(item, dict) or any(
                not isinstance(item.get(field), str) for field in ("tool", "source", "observed_at", "outcome")
            )
            for item in value["evidence"]
        )
    ):
        raise ValueError(f"Invalid report for task ID {task_id}.")
    report = cast(StoredReport, value)
    try:
        render_markdown(report)
    except (AttributeError, IndexError, KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid report for task ID {task_id}.") from error
    return report


def _read_package(package: Path) -> list[StoredReport]:
    if package.stat().st_size > _MAX_BYTES + 1024 * 1024:
        raise ValueError("Report package exceeds the size limit.")
    try:
        with ZipFile(package) as archive:
            records = archive.infolist()
            entries = {record.filename: record for record in records}
            if len(entries) != len(records):
                raise ValueError("Report package contains a duplicate ZIP entry.")
            if sum(record.file_size for record in records) > _MAX_BYTES:
                raise ValueError("Report package exceeds the 32 MiB uncompressed size limit.")
            if _MANIFEST not in entries or len(entries) > 1 + 2 * _MAX_REPORTS:
                raise ValueError("Invalid report package entries.")
            if any(record.compress_type not in (ZIP_DEFLATED, ZIP_STORED) or record.flag_bits & 1 for record in records):
                raise ValueError("Unsupported report package compression or encryption.")
            manifest = _manifest_reports(_decode_json(_read_entry(archive, entries[_MANIFEST]), _MANIFEST))
            names = [_report_names(index) for index in range(1, len(manifest) + 1)]
            if set(entries) != {_MANIFEST, *(name for pair in names for name in pair)}:
                raise ValueError("Report package entries do not match its manifest.")

            reports: list[StoredReport] = []
            for item, (json_name, markdown_name) in zip(manifest, names):
                raw_report = _read_entry(archive, entries[json_name])
                markdown = _read_entry(archive, entries[markdown_name])
                if _digest(raw_report) != item["json_sha256"] or _digest(markdown) != item["markdown_sha256"]:
                    raise ValueError(f"Report package checksum mismatch for task ID {item['task_id']}.")
                try:
                    markdown.decode("utf-8")
                except UnicodeError as error:
                    raise ValueError(f"Invalid UTF-8 in package entry {markdown_name}.") from error
                reports.append(_validated_report(_decode_json(raw_report, json_name), item["task_id"]))
            return reports
    except (BadZipFile, EOFError, RuntimeError, ZlibError) as error:
        raise ValueError("Invalid report ZIP.") from error


def import_reports(package: Path, database: Path) -> list[str]:
    reports = _read_package(package)
    if database.exists():
        store_imported_reports(database, reports)
    else:
        with _publish_new_file(database) as staging:
            staging.touch(exist_ok=False)
            store_imported_reports(staging, reports)
    return [report["task_id"] for report in reports]
