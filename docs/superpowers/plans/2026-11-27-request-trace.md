# OpsPilot Request Trace Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Diagnose one request by Trace ID from a configured Jaeger 2 query endpoint.

**Architecture:** The Core consumes a typed trace read and decides which span evidence can support a diagnosis. The Jaeger connector implements the bounded read and maps the v3 HTTP representation to that contract. The application adds an explicit route and stores the resulting report through the current SQLite entry point.

**Tech Stack:** Python 3.10+, LangGraph, stdlib HTTP and JSON, SQLite, pytest, Jaeger 2.23 for opt-in acceptance.

**Spec:** [Request trace design](../specs/2026-11-27-request-trace-design.md)

## Global Constraints

- Start from GitHub `main` merge `9fc7d6b`. Modify OpsPilot only. Use read-only queries outside the isolation fixture.
- Use one explicit nonzero 16- or 32-hex-digit Trace ID, one bounded Jaeger GET, and the existing diagnosis and report flow. Default tests require neither Jaeger nor model credentials.
- Run `python -m opspilot` and `python -m pytest` before each independently verifiable local commit. Run v1.5 offline regression before completion.

## Review Focus

- Missing or cyclic links, duplicate IDs and independent errors cannot manufacture a single cause. Tests cover both complete and disconnected graphs, including a failed ancestor separated by a successful span.
- No-match reads, unavailable sources and partly readable streams have distinct report behavior. Foreign-span metadata does not survive into the report.
- Requests are strictly scoped, preview stays offline, saved report indexes continue to identify the same records, and old reports render normally.
- Stream length, span count and time checks apply before a result can be presented as fully linked.
- Acceptance IDs come from actual requests and the observed parent relationships match the backend response.

### Task 1. Type, attribute and report the request evidence

- [x] Add `tests/test_request_trace.py` covering normalized IDs, linked healthy and ancestor/downstream errors, parallel branches, missing links, conflicts, cycles, foreign IDs, no match and unavailable or partial reads. Check topology and evidence indexes alongside status.
- [x] Run the focused tests and verify failure because the request trace workflow is absent.
- [x] Add `src/opspilot/core/trace_evidence.py` with `normalize_trace_id(value: str) -> str`, `TraceSpan`, `RequestTraceRead`, per-span and lookup evidence, and `RequestTraceSummary`. Use `source`, `observed_at`, `outcome`, `spans` and `gaps` in the read, and optional `reason`. Span IDs and parents are canonical hex strings; the optional parent uses null for a root.
- [x] Add `run_request_trace(question: str, trace_id: str, source: Callable[[str], RequestTraceRead]) -> DiagnosisReport` in `src/opspilot/core/request_trace.py`. Use one measured LangGraph read, preserve verified spans, and emit an ordered evidence list. Add its fields to `src/opspilot/core/diagnosis.py`.
- [x] Verify fresh startup, focused tests and the full suite, then commit core evidence and design.

### Task 2. Read Jaeger v3 within explicit limits

- [x] Add `tests/test_jaeger.py` with loopback HTTP fixtures for v3 result envelopes, multiple frames, no match, unauthorized, timeout, mismatched IDs, malformed frames with partial evidence, and byte and span caps. Check read method, route and credential handling.
- [x] Confirm the tests fail for the absent connector. Implement `src/opspilot/jaeger_config.py` with `JaegerConfig` and `parse_jaeger_config(value: object) -> JaegerConfig`. Integrate optional config with `src/opspilot/target_config.py` and expose the approved limits.
- [x] Implement `JaegerConnector(config: JaegerConfig, token: str | None = None)` and `.inspect_trace(trace_id: str) -> RequestTraceRead` in `src/opspilot/connectors/jaeger.py`. The `endpoint(trace_id: str) -> str` method performs local path construction. Map response envelopes and trace IDs before keeping necessary observation fields.
- [x] Verify the focused and full suites and startup, then commit the connector and target config.

### Task 3. Integrate preview, diagnosis and report readback

- [x] Add `tests/test_request_trace_cli.py` for offline `--plan`, conflicting flags, lookup plus original task ID readback, Markdown evidence and a prior-format report. Confirm the CLI cannot yet recognize `--trace-id`.
- [x] Modify `src/opspilot/__main__.py` and `src/opspilot/report_markdown.py`. Use `diagnose-auto --config ... --trace-id ...` and the existing `report` subcommand. Use `OPSPILOT_JAEGER_TOKEN` only for the connector. Record requested scope and limits in the preview and report.
- [x] Verify startup, focused cases and full tests, then commit the CLI and renderer.

### Task 4. Verify two isolated services through a real Jaeger backend

- [x] Prepare an opt-in example in `examples/jaeger-request-trace/` and a pinned Jaeger 2.23 backend. Issue actual HTTP requests through two services. Record requested Trace IDs for normal, downstream 5xx and omitted intermediate parent span cases.
- [x] Read the real trace query endpoint and compare stable span IDs, service, parents, status and timestamps against the saved report. Read the Markdown back by the three task IDs and confirm references. Update `README.md` and `docs/v1.6-request-trace.md` with the recorded IDs, provenance and reproduction commands.
- [x] Verify startup, the full suite and v1.5 offline regression, then commit the sample and acceptance record. Review the whole branch from `9fc7d6b` and verify any necessary corrections before closing the goal.
