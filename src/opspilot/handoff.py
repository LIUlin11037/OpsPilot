"""Create a local ZIP containing only explicitly selected saved reports."""

import hashlib
import json
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

from opspilot.report_markdown import render_markdown
from opspilot.task_store import load_reports_for_export

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
