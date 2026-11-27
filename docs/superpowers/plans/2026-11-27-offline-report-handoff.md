# OpsPilot Offline Report Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for implementation in the current session. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export selected diagnostic reports and import them offline without changing task IDs or evidence.

**Architecture:** `handoff.py` coordinates the versioned ZIP format and validates all entries before storage. `task_store.py` owns source selection and transactional insertion. The CLI assembles these functions and delegates report reading, rendering and comparison to existing entry points.

**Tech Stack:** Python 3.10+, SQLite, standard-library ZIP, JSON and SHA-256, pytest.

**Spec:** [Offline report handoff design](../specs/2026-11-27-offline-report-handoff-design.md)

## Global Constraints

- Export only explicitly selected completed reports, in source storage order.
- Preserve task IDs, evidence contents, observation windows and reference indexes.
- Perform local I/O. Stage new files in their destination directory and publish exclusively. Existing databases use a single write transaction.
- Run `python -m opspilot` and `python -m pytest` before each local commit.

## Review Focus

- Requested IDs supplied out of order still preserve the source report order.
- Invalid requests and existing export paths leave source and destination intact.
- ZIP descriptors are bounded and match the manifest exactly, including duplicate entries and compressed payload sizes.
- A conflict on a later imported ID leaves an existing SQLite database intact, and invalid packages create no output database.
- A historical report retains the original evidence indexes and keeps working through ordinary CLI commands.

### Task 1. Write a versioned, selected-report ZIP

**Files**

- Modify `src/opspilot/task_store.py`.
- Create `src/opspilot/handoff.py` and `tests/test_handoff_export.py`.

**Interfaces**

- `load_reports_for_export(database: Path, task_ids: Sequence[str]) -> list[StoredReport]`
- `export_reports(database: Path, destination: Path, task_ids: Sequence[str]) -> list[str]`

- [ ] Add tests for selecting two of three reports in reversed argument order. Assert the manifest follows database order and includes matching JSON, Markdown and checksums.
- [ ] Run `python -m pytest tests/test_handoff_export.py -q` and verify the absent-interface failure.
- [ ] Implement a read-only selection and exclusive ZIP publication. Use `manifest.json`, format version `1`, and indexed entries `reports/1.json` and `reports/1.md`. Cap each package at 100 reports and 32 MiB uncompressed. Reject duplicate IDs, missing IDs and existing output files before publishing.
- [ ] Check source immutability, no output on failure, and the exact file set. Run startup and the full test suite, then commit the writer.

### Task 2. Validate the ZIP and import atomically

**Files**

- Modify `src/opspilot/handoff.py` and `src/opspilot/task_store.py`.
- Create `tests/test_handoff_import.py`.

**Interfaces**

- `import_reports(package: Path, database: Path) -> list[str]`
- `store_imported_reports(database: Path, reports: Sequence[StoredReport]) -> None`

- [ ] Add tests for corruption, unknown version, duplicate entries, extra files, oversized content, duplicate report IDs and an existing database conflict. Assert rejected packages leave destination reports untouched.
- [ ] Run `python -m pytest tests/test_handoff_import.py -q` and verify the absent-interface failure.
- [ ] Check member names and declared uncompressed size before reading. Bound the reads, verify digests, decode UTF-8 and validate the report fields used by the renderer. Use JSON as the imported report. Publish a new database after closing its transaction. Use a single transaction for an existing database.
- [ ] Expand the round-trip case to assert original report content and order. Run startup and the full test suite, then commit the importer.

### Task 3. Connect the CLI and verify a real offline handoff

**Files**

- Modify `src/opspilot/__main__.py` and `README.md`.
- Create `tests/test_handoff_cli.py` and `docs/v1.4-offline-handoff.md`.

- [ ] Add a CLI test exporting and importing into a new SQLite database. Assert `report`, `reports` and `compare-http` match source results by original ID.
- [ ] Run `python -m pytest tests/test_handoff_cli.py -q` and verify the unknown-command failure.
- [ ] Add `export-reports --output ZIP TASK_ID [...]` and `import-reports ZIP`, both using global `--db`. Return JSON with the count and original IDs and report errors through the CLI.
- [ ] Run the real Traefik archive handoff in an isolated directory using the fault, recovery and history IDs from `docs/v1.3-acceptance.md`. Check report equality, two matched metrics, HTTP 504 to 200, historical indexes `[1, 2, 3]`, and an unchanged source database digest.
- [ ] Record the commands and results. Run startup and the full test suite and commit the CLI and verification record.
