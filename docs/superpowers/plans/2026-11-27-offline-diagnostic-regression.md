# OpsPilot Offline Diagnostic Regression Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run six representative diagnoses from the normal CLI and detect unsupported, missing or misattributed conclusions offline.

**Architecture:** An example runner serves fixed responses on loopback and writes case-specific config and JSON records into temporary directories. It runs `diagnose-auto` and `report`, then compares the returned reports with independently recorded expectations. Production diagnosis and report paths remain the entry points under evaluation.

**Tech Stack:** Python 3.10+, standard-library HTTP server, SQLite, JSON, pytest.

**Spec:** [Offline diagnostic regression design](../specs/2026-11-27-offline-diagnostic-regression-design.md)

## Global Constraints

- No model credential, external network or Docker needed. Use the existing rules and read-only evidence contracts.
- The fixtures and expectations must be available in Git. No reference to an untracked local report is required to run them.
- Commit locally in independent, reversible units after `python -m opspilot` and `python -m pytest` pass. Push, tag and release require separate authorization.

## Review Focus

- A clean working directory with no `.opspilot` or model config can run the example.
- Time-dependent task IDs, observation times and durations do not cause spurious verdict changes. Historical evidence still matches its declared window.
- A missing field and a JSON `null` value are distinguished. An extra or wrongly scoped evidence record is detectable.
- A harness or CLI failure returns a failure that names the case. A valid diagnosis with a mismatched assertion also makes the command fail.
- Historical replay and queue processing exercise the real log and message ID filters, including distractors.

### Task 1. Freeze representative observations and expectations

- [ ] Read the Traefik raw status, range response and converted log artifacts. Extract minimal samples carrying relative timestamps, HTTP statuses and scoped signal values. Record the associated raw measurements in `examples/diagnostic-regression/README.md`.
- [ ] Write `examples/diagnostic-regression/cases.json` as a list of objects with `id`, `origin`, `question`, `inputs`, and `expected`. Give health, fault, recovered-history, timeout, scope-isolation and unrelated-message separate IDs. Input fields use health status, metric samples relative to query end, log rows with fixed timestamps, a historical window or a queue snapshot with processing rows as applicable.
- [ ] Set `expected` to the relevant report fields as a partial object, with exact lengths and order for list values. Add `absent` field paths for any fields whose presence would falsely attribute a failure to the requested message. Document provenance for derived and synthetic rows.
- [ ] Check all six unique IDs, provenance and JSON validity before implementing the comparison. Run startup and full tests, then commit fixtures and design.

### Task 2. Run and compare the six cases

- [ ] Add `tests/test_diagnostic_regression.py` to run `python examples/diagnostic-regression/run.py` from a directory without the repository or model credentials. Assert all named cases pass and that two runs have the same JSON output. Add a mismatch test using `--cases` and a modified copy of the fixture. Assert case ID, exact field path, expected value, actual value and exit code 1. Assert unknown fields are distinguished from null and extra evidence fails the comparison.
- [ ] Run these tests against the absent runner and confirm the expected failure.
- [ ] Create `examples/diagnostic-regression/run.py` with `main(argv: Sequence[str] | None = None) -> int`. Default `--cases` to the bundled `cases.json`. For each case, start a scoped loopback fixture server, run the regular CLI with a case-specific config and database, read back by generated task ID, and compare stable report fields. A child command error includes the case and the original diagnostic error. Return 1 when any case fails. Print one JSON result with case IDs, status and the expectations and actual values checked.
- [ ] Run the new tests, startup and the full suite. Commit the runner and tests.

### Task 3. Verify the baseline and repair actual diagnosis discrepancies

- [ ] If a pre-annotated scenario reveals an actual diagnostic error, prove it with a focused failing regression at the affected layer, fix it and run startup and the full suite before a scoped commit. Record the observed discrepancy and evidence. If every case already passes, record that result.
- [ ] In a clean directory, run the documented command with model environment variables absent and socket connections limited to the loopback fixture. Verify all six cases and a controlled mismatch with exit 1. Verify that the committed baseline records genuine and synthetic provenance and can be reproduced.
- [ ] Link the commands from `README.md`. Record statuses, key evidence and limitations in `docs/v1.5-diagnostic-regression.md`. Run startup and the full suite before committing the acceptance record.
