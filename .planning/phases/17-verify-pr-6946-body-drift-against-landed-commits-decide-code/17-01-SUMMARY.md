---
phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code
plan: 01
subsystem: sql
tags: [sample-by, fill-prev, circuit-breaker, cross-column, timestamp-unit, interval-unit, code-generation]

# Dependency graph
requires:
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    provides: "Phase 15 Plan 02 landed two emission-side CB polls at :382 and :530; same tick-counting-clock harness reused conceptually"
  - phase: 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys
    provides: "D-02 classifier widening established the per-column FILL branch shape we strengthen further here"
provides:
  - "Pass-1 key-discovery loop polls the cancellation circuit breaker on every row at SampleByFillRecordCursorFactory.java:604 (third emission-adjacent site)"
  - "Cross-column FILL(PREV) rejects TIMESTAMP_MICRO <-> TIMESTAMP_NANO and INTERVAL_TIMESTAMP_MICRO <-> INTERVAL_TIMESTAMP_NANO mixes at codegen with the existing source/target type-mismatch error"
  - "testFillPrevCrossColumnTimestampUnitMismatch pins the widened needsExactTypeMatch predicate for TIMESTAMP targets"
affects: ["17-04 PR-body updates (M3.2 cancellation CB row + M-unit coverage statement); future cancellation CB regression test work gated on upstream buildMap-path rewrite"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Master-parity CB poll pattern: first statement inside while(baseCursor.hasNext()) body"
    - "Exact-type match predicate for unit-variant types (TIMESTAMP + INTERVAL join DECIMAL/GEOHASH/ARRAY)"
    - "Regression-coverage self-check on predicate widenings: revert the two new lines, verify test fails, restore"

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java"
    - "core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"

key-decisions:
  - "Drop testFillKeyedPass1RespectsCircuitBreaker. Upstream SortedRecordCursor.buildChain() -> AsyncGroupByRecordCursor.buildMap():237 polls the cancellation CB before our :604 poll executes, so a differential test (poll-in vs poll-out) cannot discriminate. User-approved deviation from plan's paired-test spec after checkpoint surfaced the structural infeasibility."
  - "Variant B (INTERVAL_TIMESTAMP_MICRO vs INTERVAL_TIMESTAMP_NANO test) dropped: ColumnType.nameTypeMap only exposes 'interval' -> INTERVAL_TIMESTAMP_MICRO, no DDL keyword creates NANO-interval columns. Widened predicate still covers the case at code-path level via targetTag == ColumnType.INTERVAL."
  - "Variant A (TIMESTAMP MICRO <-> NANO) shipped. DDL keywords 'timestamp' and 'timestamp_ns' create both unit variants; the test drives SELECT first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) and asserts 'source type TIMESTAMP cannot fill target column of type TIMESTAMP_NS'."
  - "D-01 classification unchanged: Task 1 is regression-recovery (no new PR body row); Task 2 is net-new bug (no existing body claim). Plan 04 handles all PR body edits."

patterns-established:
  - "Same-commit regression-coverage self-check for predicate widenings (Phase 15 D-12 + D-09 pattern applied to cross-column PREV type compatibility)"
  - "Upstream CB-layer documentation in commit body when a defense-in-depth fix cannot be differentially tested"

requirements-completed: [COR-04, XPREV-01, COR-02]

# Metrics
duration: ~25min
completed: 2026-04-22
---

# Phase 17 Plan 01: Safety-critical codegen fixes Summary

**Pass-1 cancellation CB poll at :604 matching master cursor-path; widened needsExactTypeMatch to reject TIMESTAMP/INTERVAL cross-column PREV unit mismatches with a paired regression test**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-04-22 (continuation session; prior session recorded the initial attempt + checkpoint)
- **Completed:** 2026-04-22T15:59:42Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Closed the third pre-emission CB-unchecked site in the keyed fast path (pass-1 key discovery), matching `origin/master:SampleByFillPrevRecordCursor.java:171` and `origin/master:SampleByFillValueRecordCursor.java:183` verbatim.
- Closed the silent cross-unit drift in cross-column FILL(PREV) by widening `needsExactTypeMatch` to include TIMESTAMP and INTERVAL tags, sharing the same error-message path as DECIMAL/GEOHASH/ARRAY.
- Added `testFillPrevCrossColumnTimestampUnitMismatch` (alphabetically between GeoHashWidthMismatch and Keyed) covering TIMESTAMP_MICRO -> TIMESTAMP_NANO rejection at the PREV argument position.
- Confirmed the regression-coverage self-check for Task 2: reverting the two new predicate lines reproduces the bug (test fails within ~3s of test execution), restoring returns to green.

## Task Commits

1. **Task 1: Poll circuit breaker in pass-1 key discovery** - `f05fa2eb25` (fix)
2. **Task 2: Reject cross-column FILL(PREV) unit mismatch** - `889a4676b9` (fix)

**Plan metadata:** (created after SUMMARY; see final `docs` commit on the branch)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` - Added `circuitBreaker.statefulThrowExceptionIfTripped()` as the first statement inside the pass-1 `while (baseCursor.hasNext())` body (Task 1).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` - Widened `needsExactTypeMatch` at :3605 to include `targetTag == ColumnType.TIMESTAMP || targetTag == ColumnType.INTERVAL` (Task 2).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - Added `testFillPrevCrossColumnTimestampUnitMismatch` (Task 2). No test added for Task 1 (see Deviations).

## Decisions Made

- **Drop Task 1 regression test** (user decision after checkpoint): upstream cancellation-CB poll in `SortedRecordCursor.buildChain()` -> `AsyncGroupByRecordCursor.buildMap():237` trips before our :604 poll executes in any realistic base-cursor-materialized shape. A differential test (with vs without the :604 poll) produces identical outcomes under both fixed and reverted production code; landing such a test would hide the coverage gap, not prove it.
- **Drop Task 2 Variant B** (INTERVAL_TIMESTAMP_NANO): no user-facing DDL keyword maps to INTERVAL_TIMESTAMP_NANO (`ColumnType.nameTypeMap` only exposes `interval -> INTERVAL_TIMESTAMP_MICRO`). Since no CREATE TABLE can declare a NANO-interval column, no SQL-level test can exercise the INTERVAL branch of the widened predicate today. The production widening still includes `targetTag == ColumnType.INTERVAL` so future DDL that exposes NANO intervals gets correct rejection for free.
- **Variant A uses TIMESTAMP_MICRO source / TIMESTAMP_NANO target** with error substring `"source type TIMESTAMP cannot fill target column of type TIMESTAMP_NS"` (nameOf: TIMESTAMP_MICRO -> "TIMESTAMP", TIMESTAMP_NANO -> "TIMESTAMP_NS"). Error position pinned to the PREV argument via `sql.indexOf("a", sql.indexOf("PREV(a)") + 5)` following the existing `testFillPrevCrossColumnDecimalPrecisionMismatch` template.

## Deviations from Plan

### Auto-fixed / Approved-deviation Issues

**1. [Rule 4 - Architectural] Drop testFillKeyedPass1RespectsCircuitBreaker (user-approved after checkpoint)**
- **Found during:** Task 1 self-check (differential-coverage validation per Phase 15 D-12 standard)
- **Issue:** Prior-session investigation discovered that `SortedRecordCursor.buildChain()` calls `AsyncGroupByRecordCursor.buildMap()` which at `dispatchAndAwait:237` already polls `statefulThrowExceptionIfTripped()`. In every reachable SAMPLE BY FILL test shape, that upstream poll trips before the first `baseCursor.hasNext()` call on our cursor, meaning the :604 poll executes against an already-exhausted tick budget identically whether present or absent. A differential test pinning the :604 site cannot be constructed without a bespoke synthetic base cursor bypassing the upstream buildMap path. A test that passes under both fixed and reverted production states would hide the coverage gap — strictly worse than no test.
- **Resolution:** Returned checkpoint to the user with Options A (bespoke synthetic cursor, high scope) / B (drop the test, ship the poll alone with master-parity rationale) / C (downgrade to integration-only coverage). User selected Option B.
- **Fix:** Removed `testFillKeyedPass1RespectsCircuitBreaker` (and its `@Test` annotation block) from `SampleByFillTest.java`; `testFillKeyedRespectsCircuitBreaker` (Phase 15 Plan 02 C-3) retains the emission-side CB coverage; all prior imports remain in use.
- **Files modified:** core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
- **Verification:** Task 1 commit body documents the upstream-CB-layer reasoning and names master-parity + defense-in-depth as the justification; all 123 pre-existing `SampleByFillTest` tests pass.
- **Committed in:** f05fa2eb25 (Task 1 commit; test removal landed alongside the poll insertion, with a commit-body paragraph explaining the omission)

**2. [Rule 4 - Architectural] Drop Task 2 Variant B (INTERVAL unit-mismatch test)**
- **Found during:** Task 2 DDL spike before writing the test
- **Issue:** Plan specified Variant B as conditional on INTERVAL_TIMESTAMP_NANO DDL declarability. Spike of `ColumnType.nameTypeMap` confirmed only `interval -> INTERVAL_TIMESTAMP_MICRO` is exposed; no DDL keyword creates NANO-interval columns, so no CREATE TABLE can exercise the INTERVAL branch.
- **Resolution:** Per plan contingency, dropped Variant B and documented the spike outcome in the commit body.
- **Fix:** Only Variant A (TIMESTAMP MICRO vs NANO) test landed. Production widening includes both tags.
- **Files modified:** N/A (Variant B would have been a second test method; none added).
- **Verification:** Task 2 commit body records the spike result explicitly.
- **Committed in:** 889a4676b9 (Task 2 commit; Variant B drop documented in commit body)

---

**Total deviations:** 2 auto-fixed / approved (both Rule 4 architectural — user approval for #1, plan-contingency for #2)
**Impact on plan:** Task 1 ships with code-fix + master-parity + defense-in-depth rationale instead of code-fix + paired test. Task 2 covers the TIMESTAMP variant fully and INTERVAL at the production-code level only. Both fixes close real safety gaps; the deviations reflect surface-level reality (upstream CB layer, DDL gap), not scope creep.

## Issues Encountered

- Commit 1 initial title ("Poll cancellation circuit breaker in pass-1 key discovery", 57 chars) exceeded the 50-char limit; amended before verification to "Poll circuit breaker in pass-1 key discovery" (44 chars). No work was lost; the commit was unpushed and contained only the poll insertion.
- Self-check of Task 2 required a transient revert-and-restore of the two new predicate lines. Self-check completed cleanly with test-execution time ~3s (well within Phase 15 D-12 / D-09 10-second budget); restoration verified via `grep -c` on both predicate tokens (returned 1 each).

## Master-parity Evidence (preserved for Plan 04 cross-reference)

For Task 1 PR-body M3.2 row and Plan 04 reconciliation:

- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java:171` polls `circuitBreaker.statefulThrowExceptionIfTripped()` as the first statement after `if (timestamp < next)` inside the do-while over `baseCursor.hasNext()` equivalent.
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursor.java:183` polls `circuitBreaker.statefulThrowExceptionIfTripped()` at the same first-statement-in-while-body position.
- Phase 15 Plan 02 commit `c1deb9b14d` added emission-side polls at `SampleByFillRecordCursorFactory.java:382` (hasNext) and `:530` (emitNextFillRow). This plan closes the third site at `:604` (pass-1 key discovery).

## Next Phase Readiness

- Plan 17-02 (minor code hygiene) unblocked; no shared files with this plan.
- Plan 17-03 (test-only additions) unblocked.
- Plan 17-04 (PR body + title edits) should incorporate the M3.2 cancellation CB update drawing on the master-parity evidence captured above and the Task 1 commit body; no new body claim required for the cross-column unit-mismatch fix (D-01 classification: net-new bug, no existing row to correct, the D-28 todo retirement in Plan 04 is the canonical action).
- Task 1 test-drop deviation should be surfaced in Plan 17-04 discussion so the PR body language around the cancellation CB doesn't over-promise test coverage at the pass-1 site.

## Self-Check: PASSED

Verified on 2026-04-22T15:59:42Z:
- File `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` exists and `grep -c "statefulThrowExceptionIfTripped"` returns 3 (was 2 pre-plan).
- File `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` exists and `grep -c "targetTag == ColumnType.TIMESTAMP"` returns 1 plus `grep -c "targetTag == ColumnType.INTERVAL"` returns 1.
- File `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` exists; `testFillPrevCrossColumnTimestampUnitMismatch` present (124 tests total, up from 123); `testFillKeyedPass1RespectsCircuitBreaker` absent.
- Commit `f05fa2eb25` exists on branch `sm_fill_prev_fast_path` (`git log --oneline` confirms, one line above `889a4676b9`).
- Commit `889a4676b9` exists on branch `sm_fill_prev_fast_path` (current HEAD before the docs commit).
- `mvn -Dtest=SampleByFillTest test -pl core` reports `Tests run: 124, Failures: 0, Errors: 0, Skipped: 0, BUILD SUCCESS`.

---
*Phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code*
*Completed: 2026-04-22*
