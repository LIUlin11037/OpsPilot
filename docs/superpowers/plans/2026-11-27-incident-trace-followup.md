# OpsPilot Incident Trace Follow-up Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Follow the latest three distinct, explicitly attributed error-log Trace IDs during an opt-in incident investigation.

**Architecture:** The existing JSONL connector filters scoped error rows and returns bounded candidates. Core reuses the request-trace report builder, budgets Jaeger calls, and keeps evidence references in the overall incident report. The CLI assembles the connectors and the renderer displays request findings alongside the unchanged service-window findings.

**Tech Stack:** Python 3.10+, LangGraph, Python stdlib, SQLite, pytest. Jaeger 2.23 only in opt-in integration.

**Spec:** [Incident trace follow-up design](../specs/2026-11-27-incident-trace-followup-design.md)

## Global Constraints

- Use local v1.6 `44d1650`. Preserve the v1.5 offline regression and the standalone v1.6 `--trace-id` flow.
- Opt in with `--follow-traces` only for historical mode and a configured Jaeger query source. Use at most three distinct GETs and a shared 10-second follow-up budget. Keep service-window scopes and historical evidence derivation unchanged.
- Default pytest is offline. Verify `python -m opspilot` and `python -m pytest` before each small local commit. Push, merge, tag and release only with separate authorization.

## Review Focus

- Normalize and order ID candidates from exact service/time/error scope even when file order and timestamp order differ. Preserve the prior log result when the option is off.
- Invalid IDs and failures in later reads never delete earlier valid observations. Distinguish absence of valid candidates from log unavailability.
- Count and time bounds hold across timeouts and successful reads, and skipped candidates get a reason.
- Indexes in nested trace summaries, original log references and merged next checks resolve in both formats after SQLite round-trip.
- The live check compares the exact queries and returned span records to genuine traffic and intentional log distractors.

### Task 1. Read selected IDs from the existing scoped log evidence

- [ ] Add `tests/test_incident_trace_candidates.py` with shuffled and inclusive-boundary rows, casing or length aliases, tied times, out-of-scope rows, valid error counts, optional opt-in and unavailable logs. Verify the new `include_trace_candidates` keyword fails first.
- [ ] Define `TraceLogCandidate`, `TraceLogSelection` and `MAX_INCIDENT_TRACE_LOOKUPS = 3` in `src/opspilot/core/trace_evidence.py`. A candidate has `trace_id`, one-based `line_number`, `timestamp`, and `level`. A selection has `candidates`, `missing_ids`, `invalid_ids`, `duplicate_rows`, and `over_limit`.
- [ ] Add `trace_candidates: TraceLogSelection` to optional service-log evidence. Add keyword `include_trace_candidates: bool = False` to `JsonlServiceLogConnector.inspect_window`. Scan the bounded source once, keep the newest row per canonical ID and tie-break by earliest line, then cap the saved list.
- [ ] Verify the original and new log behavior, startup and full pytest, then commit the log contract, selection and design.

### Task 2. Budget follow-up reads and attach the evidence

- [ ] Add `tests/test_incident_trace_followup.py` for unavailable logs, no candidates, an observed trace followed by unavailable results and absent spans, a shared fake clock with exhausted budget, and correct references across two lookups. Verify the new module is absent.
- [ ] Factor the existing trace evidence rendering into a reusable builder in `src/opspilot/core/request_trace.py`. Preserve standalone behavior. Add `attach_incident_traces(report: DiagnosisReport, lookup: Callable[[str, float], RequestTraceRead]) -> DiagnosisReport` in `src/opspilot/core/incident_traces.py`, with remaining-time input and `TRACE_FOLLOWUP_BUDGET_SECONDS = 10`.
- [ ] Add optional `incident_traces` to the report with `log_evidence_index`, `stop_reason`, limits and ordered `followups`. Each item keeps its candidate index, an optional lookup evidence index, and either a remapped `RequestTraceSummary` or a skipped reason. Append actual tool calls and remap next-check indexes. Preserve the existing `incident` fields and service status.
- [ ] Verify the focused and full suites and startup, then commit the Core follow-up.

### Task 3. Add explicit CLI preview and persisted report formatting

- [ ] Add `tests/test_incident_trace_cli.py` using the existing local fixture for HTTP, Prometheus and Jaeger. Exercise preview without reads, validation, successive Jaeger results, global indexes, legacy opt-out and JSON/Markdown readback. Verify the CLI fails on the new flag before implementation.
- [ ] Wire `--follow-traces` through `src/opspilot/__main__.py`, passing remaining time with the existing `JaegerConnector` and configured timeout. Extend typed planned checks and `src/opspilot/report_markdown.py` with a request section for candidate rows, evidence indexes, gaps and skipped reasons.
- [ ] Verify startup, the focused and full suites, then commit the CLI and report integration.

### Task 4. Verify bounded correlation with real HTTP traffic and Jaeger

- [ ] Reuse v1.6 sample HTTP services to capture a real failed request. Build a bounded historical JSONL fixture with its Trace ID, a known missing ID, duplicate, adjacent service and outside-window rows. Provide a local Prometheus fixture for fixed historical samples and observe the actual Jaeger GETs used by OpsPilot.
- [ ] Verify the successful and missing trace lookups end to end, including a lookup-sequence receipt and SQLite JSON/Markdown readback. Save reproduction commands, task IDs and limits in `docs/v1.7-incident-trace-followup.md` and link README.
- [ ] Run startup, full pytest, explicit v1.5 replay and v1.6 standalone path. Commit sample and acceptance record. Review the whole branch and verify any required fixes before closing the goal.
