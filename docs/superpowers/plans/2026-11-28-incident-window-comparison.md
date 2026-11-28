# Incident Window Comparison Implementation Plan

> For agentic workers, execute the tasks inline and mark each verified step below.

OpsPilot compares two saved historical investigations using Python 3.10+, SQLite and pytest. A pure comparison module validates stored scope and pairs historical evidence. The existing CLI reads both reports and prints JSON or Markdown. The acceptance script generates reports using the existing Prometheus example and compares them with the observation services stopped. [Approved design](../specs/2026-11-28-incident-window-comparison-design.md)

## Global constraints

- Start from `746bdae` on a separate local branch. Preserve existing report schemas and commands, and use the SQLite read-only path.
- Match exact service and health URL. Only compare historical evidence under compatible configurations. Preserve unknown observations and keep each evidence index scoped to its task.
- Run `python -m opspilot` and `python -m pytest` before each local commit. Push only with separate authorization.

## Task 1. Compare stored incident evidence

- [x] Add `tests/test_incident_comparison.py` for same instants in different time zones, true gaps and inclusive overlap, reversed inputs, and rejection of unrelated or invalid reports. Verify the new entry point is absent first.
- [x] Implement `compare_incident_reports(before: StoredReport, after: StoredReport) -> IncidentComparison` in `src/opspilot/incident_comparison.py`. Return service and endpoint, ordered sides with task IDs and saved historical summaries, a window relation, and pairings with both indexed observations and reasons preventing comparison. Historical metrics pair by name and require matching source, service, expression, unit, threshold and step. Log counts require equivalent source and equal window length. A report's trace candidates and query outcomes remain attached to that report.
- [x] Cover changed thresholds and sources, missing or incomplete observations, differing window lengths, and available and disabled request tracing. Run the comparison tests and the full offline suite, then commit the comparison contract and design.

## Task 2. Publish the comparison through the CLI

- [x] Add CLI tests in `tests/test_incident_comparison_cli.py` for two saved tasks, stable JSON and Markdown, wrong or missing tasks, immutable input SQLite and existing command compatibility. Observe the unrecognized command before implementing it.
- [x] Register `compare-incident BEFORE_ID AFTER_ID --format json|markdown` in `src/opspilot/__main__.py`. Use the common report loader and error handling. Add `render_incident_comparison_markdown` in `src/opspilot/report_markdown.py`, with each observation reference labeled by its task.
- [x] Repeat startup, focused tests and full offline pytest, then commit the CLI and report rendering.

## Task 3. Compare reports from the isolated example

- [x] Add an acceptance script under `examples/prometheus-health/` to observe a fault window, revisit it after the current probe recovers, observe a later healthy window, and record a missing source. Validate evidence against sample HTTP, logs and raw Prometheus results. After stopping the sample services, check the expected comparison results by task ID.
- [x] Document reproduction and actual task IDs in `docs/v1.8-incident-window-comparison.md` and link README. Reuse archived v1.2 reports for old-format validation, run the v1.5 offline replay, and exercise v1.7 request follow-up through its saved reports and existing tests.
- [x] Run startup and full offline pytest. Review the entire branch and verify fixes before completing the local commits.
