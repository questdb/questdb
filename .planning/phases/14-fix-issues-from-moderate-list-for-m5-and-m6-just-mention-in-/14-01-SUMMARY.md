---
phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
plan: 01
subsystem: sql
tags: [sample-by, fill, prev, codegen, java, questdb]

# Dependency graph
requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    provides: rowId-based FILL(PREV) snapshots, factoryColToUserFillIdx alias mapping, deleted retro-fallback machinery
  - phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
    provides: Tier 1 cross-column PREV gate, Defect 3 under-spec grammar, FillRecord dispatch order
provides:
  - "M-1 fix: bare FILL(PREV) classifies factory-order columns reorder-safely via factoryColToUserFillIdx"
  - "M-3 fix: FILL(PREV(colX)) with too few values no longer silently broadcasts; isBareBroadcastable demands LITERAL PREV"
  - "M-9 fix: cross-column PREV uses full-int equality for DECIMAL/GEOHASH/ARRAY targets"
  - "Mn-13 fix: success-path Misc.freeObjList(fillValues) frees residual non-transferred Function references"
  - "6 new regression tests in SampleByFillTest pinning M-1, M-3, M-9, D-17"
  - "D-16 ghost-test rename: testSampleByFromToIsAllowedForKeyedQueries with substantive aggregated assertion in SampleByTest + SampleByNanoTimestampTest"
affects: [14-02, 14-03, 14-04]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Shared alias-mapping pattern: factoryColToUserFillIdx built once per generateFill call, consumed by both bare and per-column FILL(PREV) branches"
    - "needsExactTypeMatch predicate for cross-column type equality: full-int equality for DECIMAL/GEOHASH/ARRAY, tag-level for other types"
    - "Test assertion of cartesian-product output via outer count_distinct wrap when the raw output is unwieldy for a literal assertion"

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java"

key-decisions:
  - "factoryColToUserFillIdx hoisted above the bare/per-column if/else so both branches share a single mapping build; isKeyColumn(factoryIdx, bottomUpCols, timestampIndex) call in the bare branch removed"
  - "Broadcast predicate tightened to require ExpressionNode.LITERAL type for PREV; NULL keyword stays broadcastable (LITERAL-only form); PREV(colX) FUNCTION-typed no longer broadcasts"
  - "Mn-13 cleanup lands immediately before the success-path SampleByFillRecordCursorFactory constructor; catch-block Misc.freeObjList(fillValues) unchanged (both paths null-safe via ownership transfer)"
  - "M-9 type check uses ColumnType.isDecimal (tag range DECIMAL8..DECIMAL; covers all six widths) + ColumnType.isGeoHash + ColumnType.ARRAY tag comparison"
  - "D-16 ghost test renamed to testSampleByFromToIsAllowedForKeyedQueries; cartesian-product output is 9 buckets x 479 keys = 4311 rows, so assertion uses an outer SELECT ts, count(*) rows, count_distinct(x) keys ... GROUP BY ts wrap to get a readable 9-row expectation"

patterns-established:
  - "Cross-column PREV type discrimination: tag-only equality is the default; full-int equality is opt-in per needsExactTypeMatch flag for types that encode discriminating subtype information in high type bits"
  - "Ghost-test cleanup: when the raw query output is too large for a literal assertion, wrap in an outer aggregate query that proves the keyed cartesian-product invariants (row count per bucket, distinct keys per bucket)"

requirements-completed: []

# Metrics
duration: 23min
completed: 2026-04-20
---

# Phase 14 Plan 01: Close four Moderate findings in SqlCodeGenerator.generateFill Summary

**Reorder-safe bare FILL(PREV), tightened broadcast, full-type cross-column check, and success-path fill-values cleanup in SqlCodeGenerator.generateFill, plus 6 regression tests and 2 ghost-test renames.**

## Performance

- **Duration:** 23 min
- **Started:** 2026-04-20T13:31:59Z
- **Completed:** 2026-04-20T13:55:00Z (approx)
- **Tasks:** 3
- **Files modified:** 4 (1 production, 3 test)

## Accomplishments

- M-1 fix: bare FILL(PREV) branch classifies factory-order columns via factoryColToUserFillIdx, fixing mis-classification after propagateTopDownColumns0 reorder
- M-3 fix: the under-specified-fill guard tightens the broadcast predicate to isBareBroadcastable, rejecting FILL(PREV(colX)) with too few values instead of silently broadcasting
- M-9 fix: cross-column PREV uses full-int equality for DECIMAL/GEOHASH/ARRAY targets, closing precision/width/dims leaks; other types retain tag-level equality
- Mn-13 fix: the success-path return frees residual fill-values via Misc.freeObjList immediately before the factory constructor, removing a native-memory slow-leak vector
- 6 new regression tests pin the four codegen fixes plus the D-17 Decimal zero-vs-null rendering distinction
- D-16 ghost tests (SampleByTest and SampleByNanoTimestampTest) renamed to testSampleByFromToIsAllowedForKeyedQueries with substantive aggregated assertions; Rnd/DEBUG_CAIRO_COPIER_TYPE preamble stripped

## Task Commits

Each task was committed atomically:

1. **Task 1: M-1 + M-3 + Mn-13 production fix in generateFill** - `65138282d5` (fix)
2. **Task 2: M-9 cross-column full-type match** - `0312ad9835` (fix)
3. **Task 3: Regression tests + D-16 ghost-test renames + D-17 decimal zero-vs-null** - `c2e02f228c` (test)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — three production edits: bare-FILL(PREV) uses factoryColToUserFillIdx; under-spec guard demands LITERAL PREV; success-path Misc.freeObjList(fillValues); cross-column PREV full-int equality for DECIMAL/GEOHASH/ARRAY.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 6 new @Test methods added in alphabetical insertion slots: testFillPrevCrossColumnArrayDimsMismatch, testFillPrevCrossColumnBroadcastRejection, testFillPrevCrossColumnDecimalPrecisionMismatch, testFillPrevCrossColumnGeoHashWidthMismatch, testFillPrevDecimalZeroVsNull, testFillPrevOuterProjectionReorder.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — ghost test renamed to testSampleByFromToIsAllowedForKeyedQueries with aggregated assertion; Rnd/DEBUG_CAIRO_COPIER_TYPE preamble removed.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — nano twin renamed and asserted (nanosecond timestamp format in expected output).

## Production changes (with line numbers)

All line numbers are after the edits landed.

- `SqlCodeGenerator.java:3397-3461` — factoryColToUserFillIdx construction (IntList walk over bottomUpCols) hoisted above the bare/per-column if/else so both branches share it.
- `SqlCodeGenerator.java:3464-3490` — bare-FILL(PREV) branch rewritten: for each factory column, decide FILL_CONSTANT (timestamp) / FILL_KEY (factoryColToUserFillIdx[col] < 0) / FILL_PREV_SELF (factoryColToUserFillIdx[col] >= 0). No call to isKeyColumn(factoryIdx, bottomUpCols, ...).
- `SqlCodeGenerator.java:3542-3569` — cross-column PREV type check replaced: needsExactTypeMatch for DECIMAL/GEOHASH/ARRAY; isTypeCompatible returns targetType == sourceType for those families, otherwise tag-level equality.
- `SqlCodeGenerator.java:3577-3594` — under-spec guard: isBareBroadcastable requires LITERAL type for PREV, keeps NULL broadcastable.
- `SqlCodeGenerator.java:3692` — success-path Misc.freeObjList(fillValues) inserted immediately before new SampleByFillRecordCursorFactory(...). Catch-block cleanup at :3715 unchanged.

## Test results

Full SAMPLE BY trio (SampleByFillTest + SampleByTest + SampleByNanoTimestampTest): **669 tests pass** (89 + 302 + 278), 0 failures, 0 errors.

Breakdown:
- SampleByFillTest: 83 existing + 6 new = 89 tests
- SampleByTest: 302 tests (one test renamed in place, no count change)
- SampleByNanoTimestampTest: 278 tests (one test renamed in place, no count change)

**Regression coverage self-check:** I temporarily reverted Task 2 during development and confirmed that all 3 M-9 tests (Decimal/GeoHash/Array mismatch) fail under the tag-only check. After restoring Task 2, all 3 pass. This proves the tests actually exercise the M-9 fix rather than coincidentally passing.

## Decisions Made

1. **`ColumnType.isDecimal` over explicit tag-range check.** `ColumnType.isDecimal(int)` at `ColumnType.java:510` uses `tag >= DECIMAL8 && tag <= DECIMAL` (where `DECIMAL = DECIMAL256 + 1 = 34`), so it covers DECIMAL8/16/32/64/128/256 plus the generic DECIMAL. `isDecimalType(int)` at :515 tests the raw int (`colType >= DECIMAL8 && colType <= DECIMAL256`) which misbehaves for encoded decimal types. Chose `isDecimal`.
2. **D-16 aggregated-shape assertion instead of literal expected output.** The specified query `SELECT ts, avg(x), first(x), last(x), x FROM fromto WHERE s != '5' SAMPLE BY 5d FROM '2017-12-20' TO '2018-01-31' FILL(42, 42, 42)` produces 9 buckets x 479 keys = 4311 rows. A literal assertion of that size is unreadable. Wrapped the query in `SELECT ts, count(*) rows, count_distinct(x) keys FROM (...) GROUP BY ts ORDER BY ts`, producing a 9-row expectation that proves every bucket emits the full cartesian product under FILL(42, 42, 42). This is a deviation from the plan's literal-assertion approach but preserves its intent (substantive assertion on the renamed ghost test).
3. **supportsRandomAccess=false in assertQueryNoLeakCheck 5-arg form.** The SAMPLE BY FILL factory returns supportsRandomAccess=false; the tests `testFillPrevOuterProjectionReorder` and `testFillPrevDecimalZeroVsNull` initially used the plan's `(..., true, true)` form, which failed with "supports random access expected:<true> but was:<false>". Corrected to `(..., false, false)` per the factory contract.

## Deviations from Plan

**1. [Rule 3 - Blocking] D-16 aggregated-shape assertion.**
- **Found during:** Task 3 (D-16 ghost-test rename probe)
- **Issue:** The plan directed `printSql` output to be captured one-time via PROBE and pasted verbatim into an `assertQueryNoLeakCheck` literal. The actual output is 4311 data rows (plus header), which would make the assertion unreadable and hide what's being verified.
- **Fix:** Wrapped the inner query in an outer `SELECT ts, count(*) rows, count_distinct(x) keys FROM (...) GROUP BY ts ORDER BY ts`. The 9-row result (one per bucket) proves: (a) the keyed FROM/TO query compiles, (b) all 9 buckets emit rows, (c) each bucket emits the full 479-key cartesian product under FILL(42, 42, 42). Under-count at any bucket would fail the assertion.
- **Files modified:** SampleByTest.java (testSampleByFromToIsAllowedForKeyedQueries), SampleByNanoTimestampTest.java (twin)
- **Verification:** Both renamed tests pass. PROBE captured the 9-row aggregated output; I then replaced printSql with assertSql using the exact captured output.
- **Committed in:** c2e02f228c (Task 3)

**2. [Rule 1 - Bug] supportsRandomAccess=false in new tests.**
- **Found during:** Task 3 (first attempt to run `testFillPrevOuterProjectionReorder` and `testFillPrevDecimalZeroVsNull`)
- **Issue:** Plan specified `assertQueryNoLeakCheck(expected, query, timestamp, true, true)` for both tests. SAMPLE BY FILL factories return supportsRandomAccess=false (design decision from Phase 2), so the assertion fails with "supports random access expected:<true> but was:<false>".
- **Fix:** Changed the supportsRandomAccess arg to `false` in both test calls; kept expectSize=false to match the factory contract.
- **Files modified:** SampleByFillTest.java
- **Verification:** Both tests now pass.
- **Committed in:** c2e02f228c (Task 3)

---

**Total deviations:** 2 auto-fixed (1 blocking plan infeasibility, 1 bug in plan-specified test parameters)
**Impact on plan:** Both deviations were necessary to land the tests without violating readability or the factory contract. Plan intent (substantive assertion on ghost tests; regression coverage for M-1/M-3/M-9/D-17) is fully honored.

## Issues Encountered

1. **printSql output is written to a StringSink, not stdout.** During the PROBE step, the default `printSql(sql)` call doesn't produce observable output in surefire logs. Worked around this by adding `System.out.println(sink.toString())` between PROBE_BEGIN and PROBE_END markers, capturing the output via `awk '/PROBE_BEGIN/,/PROBE_END/'` from the maven log. Removed the probe code before commit.
2. **The plan's outer-wrapped form initially errored with "not enough fill values" at position 251.** This turned out to be a red herring — my initial probe wrapped the FILL query in `SELECT ts, ..., min(avg), max(avg) FROM (...) GROUP BY ts ORDER BY ts` which introduced an extra aggregate path that confused the outer GROUP BY's FILL resolution. Simplified the outer wrap to just `SELECT ts, count(*) rows, count_distinct(x) keys` and the test executes cleanly.

## User Setup Required

None — no external service configuration required.

## Handoff to Plan 02

**Test insertion slots taken in `SampleByFillTest.java`:**
- `testFillPrevCrossColumnArrayDimsMismatch` (before `testFillPrevCrossColumnBadAlias`)
- `testFillPrevCrossColumnBroadcastRejection` (between `testFillPrevCrossColumnBadAlias` and `testFillPrevCrossColumnDecimalPrecisionMismatch`)
- `testFillPrevCrossColumnDecimalPrecisionMismatch` (between `testFillPrevCrossColumnBroadcastRejection` and `testFillPrevCrossColumnGeoHashWidthMismatch`)
- `testFillPrevCrossColumnGeoHashWidthMismatch` (before `testFillPrevCrossColumnKeyed`)
- `testFillPrevDecimalZeroVsNull` (between `testFillPrevDecimal256` and `testFillPrevGeoHash`)
- `testFillPrevOuterProjectionReorder` (between `testFillPrevOfVarcharKeyColumn` and `testFillPrevRejectBindVar`)

**Codebase state:**
- Bare FILL(PREV) classification uses factoryColToUserFillIdx. If Plan 02's cursor-cluster work affects FillRecord's key detection, the classification semantics are now documented and locked in by testFillPrevOuterProjectionReorder.
- The under-spec guard rejects FILL(PREV(colX)) with size < aggregates count. Plan 02 should not re-introduce broadcasting of FUNCTION-typed PREV.
- `Misc.freeObjList(fillValues)` runs on the success path. Plan 02's FillRecord getter additions do not need to add further cleanup in generateFill.
- Cross-column PREV full-type match shields DECIMAL/GEOHASH/ARRAY. Plan 02's getArray/getBin/getBinLen branches can rely on source and target types being compatible when mode >= 0.

**Regression test suite:**
- 669 tests pass (89 + 302 + 278) after Plan 01.
- Plan 02's FillRecord additions (M-2, M-8 getInterval) should preserve this count and add their own regression tests.

## Self-Check: PASSED

**Created files (none for Plan 01 — all edits modify existing files)**

**Modified files verified:**
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — FOUND (contains `isBareBroadcastable`, `needsExactTypeMatch`, `isTypeCompatible`, hoisted `factoryColToUserFillIdx`, success-path `Misc.freeObjList(fillValues)`)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — FOUND (contains 6 new @Test methods)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — FOUND (contains `testSampleByFromToIsAllowedForKeyedQueries`, no `DEBUG_CAIRO_COPIER_TYPE` in renamed body)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — FOUND (contains `testSampleByFromToIsAllowedForKeyedQueries`, nano twin)

**Commits verified:**
- `65138282d5` — FOUND (Task 1)
- `0312ad9835` — FOUND (Task 2)
- `c2e02f228c` — FOUND (Task 3)

---

*Phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-*
*Completed: 2026-04-20*
