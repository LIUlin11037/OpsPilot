# Third-party full onboarding implementation plan

> Work through the tasks inline and check off completed steps.

Validate the existing OpsPilot workflows against original Caddy images with native observability. The isolated Compose project provides the target and observations. The onboarding guide consumes only the already exported logs and query endpoints. [Approved design](../specs/2026-11-28-third-party-full-onboarding-design.md)

## Scope

- Base the work on `a3c4217`. Use Caddy `2.11.9-alpine`, Prometheus `3.13.5` and Jaeger `2.23.0` and record the run's image digests. Keep target source and HireHub intact, and expose the isolated project only on loopback.
- Log mapping retains timestamp, target identity, HTTP status and the source Trace ID. Map 5xx status to `ERROR`. Never supply a Trace ID inferred from timing or a similar request.
- Independently verify raw provenance, read-only onboarding, compatibility and evidence gaps before completing. Run `python -m opspilot` and `python -m pytest` before each small local commit. Push only by separate authorization.

## Task 1. Set up the real observability sources

- [ ] Add a test for `examples/caddy-onboarding/map_access_logs.py` covering source field mapping, missing Trace IDs, unrelated records and malformed input. Observe the missing mapper, then implement the converter from a gateway log snapshot to JSONL.
- [ ] Add a dedicated Compose example, Caddy gateway and upstream configurations, and a narrow Prometheus scrape configuration. Reuse the current target configuration contracts. Verify a real request generates one selected gateway observation, a native gateway access-log row and a linked trace in Jaeger.
- [ ] Run startup, focused tests and the regular offline suite, then commit the fixture and mapper with the approved design.

## Task 2. Reproduce the fault, recovery and missing evidence

- [ ] Add an operator script with capture and offline compare phases. In capture, exercise the target's native healthy and 503 response modes, take same-window and later-window reports, and lose the log source for one run. Preserve original observation data and verify exact Jaeger lookups by matching candidate IDs, span resources and parent relationships. Quantify the Prometheus request count against the generated traffic.
- [ ] Prepare an onboarding guide with only the read-only inputs needed by the first user. Stop the sample once and verify offline comparison and ZIP readback from existing reports. Reproduce any genuine product issue with a failing offline test before implementing its fix.
- [ ] Run startup, focused and full offline tests, then commit the acceptance script and guide.

## Task 3. Independent acceptance and final evidence

- [ ] Give a fresh-context executor the frozen guide, ready HTTP and query endpoints, raw log snapshot and fixed time window. Keep fault controls and previous conclusions out of the handoff. Measure time from handoff to first task-ID readback and save the receipt. Verify actual friction and whether source or OpsPilot code changes were required.
- [ ] Verify v1.5 and v1.8 regression paths and review the complete branch. Document actual digests, timing, task IDs, limits and reproduction steps, then run startup and full pytest and commit the results.
