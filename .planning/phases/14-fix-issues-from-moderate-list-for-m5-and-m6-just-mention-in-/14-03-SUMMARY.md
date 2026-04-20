---
phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
plan: 03
subsystem: sql
tags: [sample-by, fill, optimiser, timezone, to_utc, java, questdb]

# Dependency graph
requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    provides: rowId-based FILL(PREV) snapshots, chain.clear() on SortedRecordCursor reuse, fill cursor initialize() parsing of FROM/TO expressions
  - phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
    provides: sub-day detection (CommonUtils.isSubDayUnit), tsFloor TIME ZONE + FROM propagation via createToUtcCall at :8308
  - phase: 14-01
    provides: codegen cluster fixes (M-1, M-3, M-9, Mn-13) baseline for parallel wave
provides:
  - "M-4 fix: rewriteSampleBy wraps nested.setFillFrom and nested.setFillTo with createToUtcCall when isSubDay AND sampleByTimezoneName is present. The fill cursor's bucket grid now aligns with the GROUP BY tsFloor grid for sub-day + TIME ZONE + FROM queries."
  - "Three new @Test methods in SampleByFillTest lock in the fix: testFillSubDayTimezoneFromEmpty (Case A, empty base emits 24 shifted UTC fill rows), testFillSubDayTimezoneFromSparse (Case B, 3 leading NULLs aligned to local-time grid), testFillSubDayTimezoneFromDense (Case C, dense data no fill rows)."
affects: [14-04]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "hasSubDayTimezoneWrap boolean captures the sub-day + TIME ZONE guard once and drives both setFillFrom and setFillTo wraps in lockstep with the tsFloor FROM wrap at :8308"
    - "SQL clause order for SAMPLE BY with TIME ZONE + FROM/TO + FILL: SAMPLE BY <unit> FROM '...' TO '...' FILL(...) ALIGN TO CALENDAR TIME ZONE '...' (TIME ZONE comes last)"

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/SqlOptimiser.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"

key-decisions:
  - "hasSubDayTimezoneWrap precomputed once above the two setFill* calls so both branches share the exact same guard the tsFloor FROM wrap at :8308 uses. Avoids drift between the three call sites of createToUtcCall."
  - "Inline ternary wrap instead of a new helper method. The fix sits at one call site cluster, mirrors the inline style of the other three createToUtcCall call sites (tsFloor at :8308, FROM/TO fallback unwraps at :8462/:8465), and adding a helper would introduce noise for a single-concern fix."
  - "No cursor-side change. SqlCodeGenerator.generateFill continues to call functionParser.parseFunction(fillFromNode, ...), which evaluates the to_utc wrapper naturally at runtime and yields the shifted UTC instant. D-09 intent preserved verbatim."

patterns-established:
  - "Sub-day + TIME ZONE guard reuse: hasSubDayTimezoneWrap boolean is the canonical predicate when any downstream pass-through of FROM/TO needs to stay in lockstep with tsFloor. If a future change adds a third pass-through site (e.g., a fill-offset propagation), it should read hasSubDayTimezoneWrap rather than rederive the condition."

requirements-completed: []

# Metrics
duration: 20min
completed: 2026-04-20
---

# Phase 14 Plan 03: Close sub-day SAMPLE BY FILL grid divergence under TIME ZONE + FROM Summary

**rewriteSampleBy wraps setFillFrom/setFillTo with createToUtcCall for sub-day + TIME ZONE, aligning the fill cursor's bucket grid with the GROUP BY tsFloor grid; three new regression tests pin empty-base, sparse, and dense cases.**

## Performance

- **Duration:** 20 min
- **Started:** 2026-04-20T14:50:15Z
- **Completed:** 2026-04-20T15:10:35Z
- **Tasks:** 2
- **Files modified:** 2 (1 production, 1 test)

## Accomplishments

- M-4 fix: rewriteSampleBy now wraps both `nested.setFillFrom` and `nested.setFillTo` with `createToUtcCall(sampleByFrom/To, sampleByTimezoneName)` when `isSubDay && sampleByTimezoneName != null`. The fill cursor's bucket grid is now aligned with the GROUP BY tsFloor grid at :8308 for sub-day queries with a TIME ZONE and FROM/TO range.
- Three new @Test methods in SampleByFillTest lock in the post-fix behavior: testFillSubDayTimezoneFromEmpty emits 24 UTC fill rows at positions corresponding to London local 00:00..23:00 for an empty-base query; testFillSubDayTimezoneFromSparse emits 3 leading NULL fills aligned to the local-time grid before the single data row; testFillSubDayTimezoneFromDense locks the dense-data behavior that was already correct pre-fix because of the firstTs anchor.
- Regression-coverage self-check: temporarily setting `hasSubDayTimezoneWrap = false` in SqlOptimiser.java caused all three new tests to fail with pre-fix outputs, proving they exercise the actual fix path rather than coincidentally passing. Restored and verified post-check.

## Task Commits

Each task was committed atomically:

1. **Task 1: M-4 fix - wrap setFillFrom/setFillTo with createToUtcCall for sub-day + TIME ZONE** - `93a856332c` (fix)
2. **Task 2: M-4 regression tests - Cases A (empty), B (sparse), C (dense)** - `a1525b50af` (test)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` - Lines 8331-8344: replaced the raw `nested.setFillFrom(sampleByFrom); nested.setFillTo(sampleByTo);` pair with a `hasSubDayTimezoneWrap` boolean and two ternaries that wrap with `createToUtcCall` when the sub-day + TIME ZONE + non-null FROM/TO conditions all hold. An explanatory comment block above the boolean documents the intent, mirrors the tsFloor FROM wrap at :8308, and explains why day-granular queries skip the wrap.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - Three new @Test methods inserted alphabetically between `testFillPrevWithCalendarOffset` (line 2509) and `testFillToNullTimestamp` (now line 2649): `testFillSubDayTimezoneFromDense` (line 2532), `testFillSubDayTimezoneFromEmpty` (line 2565), `testFillSubDayTimezoneFromSparse` (line 2622). All three use `Europe/London` TIME ZONE during June 2024 (BST, UTC+1) and assert with `assertQueryNoLeakCheck(..., "ts", false, false)` to match the SAMPLE BY FILL factory's `supportsRandomAccess=false` contract.

## Production changes (with final line numbers)

- `SqlOptimiser.java:8331-8339` - Comment block documenting the fill-range wrap rationale, line-traced against the tsFloor FROM wrap at :8308.
- `SqlOptimiser.java:8340` - `final boolean hasSubDayTimezoneWrap = isSubDay && sampleByTimezoneName != null;` captures the guard once.
- `SqlOptimiser.java:8341-8342` - `nested.setFillFrom(hasSubDayTimezoneWrap && sampleByFrom != null ? createToUtcCall(sampleByFrom, sampleByTimezoneName) : sampleByFrom);`
- `SqlOptimiser.java:8343-8344` - `nested.setFillTo(hasSubDayTimezoneWrap && sampleByTo != null ? createToUtcCall(sampleByTo, sampleByTimezoneName) : sampleByTo);`
- `createToUtcCall` call sites total 4 post-fix: 1 at :8308 (tsFloor FROM, pre-existing), 2 at :8342/:8344 (new fill-range wraps), 1 at :8462/:8465 (pre-existing FROM/TO fallback unwraps in a different code path).

## Chosen TIME ZONE and NULL rendering

- **TIME ZONE:** `Europe/London`. During June 2024 London is on BST (UTC+1), so local 00:00 maps to UTC 23:00 of the previous day. This is a non-constant offset zone but the tests run on a single day within BST so the offset is effectively constant for the three test windows.
- **NULL rendering:** literal `null` (lower-case), confirmed against existing `testFillNullKeyed` and `testFillNullDstSparseData` precedents. The tests use this rendering verbatim in expected outputs.

## Test results

- **Task-1 only (baseline):** `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test` exits 0. 680 tests pass (100 + 302 + 278), 0 failures, 0 errors. No regression in existing day-granular or non-timezone tests.
- **SqlOptimiserTest:** `mvn -pl core -Dtest=SqlOptimiserTest test` exits 0. 172 tests pass.
- **Task 2 new tests only:** `mvn -pl core -Dtest='SampleByFillTest#testFillSubDayTimezoneFromEmpty+testFillSubDayTimezoneFromSparse+testFillSubDayTimezoneFromDense' test` exits 0. 3/0/0.
- **Plan-complete gate:** `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,SqlOptimiserTest' test` exits 0. 855 tests pass (103 + 302 + 278 + 172), 0 failures, 0 errors.

**Regression-coverage self-check:** I temporarily set `hasSubDayTimezoneWrap = false` in SqlOptimiser.java and re-ran the three new tests - all three failed with the pre-fix outputs. Restored `hasSubDayTimezoneWrap = isSubDay && sampleByTimezoneName != null` and they pass again. This proves the tests exercise the actual fix path.

## Decisions Made

1. **Inline ternary wrap rather than a new helper.** The fix sits at a single call-site cluster in rewriteSampleBy. The other three call sites of `createToUtcCall` (tsFloor FROM at :8308, the FROM/TO fallback unwraps at :8462/:8465) all use inline calls. Introducing a helper would add noise without a readability payoff. Decision aligns with plan guidance and locked D-08.
2. **Compute `hasSubDayTimezoneWrap` once.** Both ternaries share the same guard; hoisting it into a boolean avoids duplication and keeps the two wraps visibly in lockstep with each other and with the tsFloor FROM wrap at :8308. If a future change adds another fill-range pass-through, the boolean is the single source of truth.
3. **Chose `Europe/London` for all three tests.** This matches the plan's concrete Case A/B/C numbers verbatim and uses a BST (UTC+1) offset within June 2024 so local 00:00 cleanly maps to UTC 23:00 of the prior day. A constant-offset zone was considered (e.g., `Europe/Kiev` during winter) but the London BST scenario is what the plan's Case A description specifies and already exercises a non-zero offset.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's SQL test snippets placed `TIME ZONE` before `FILL(...)`, which QuestDB's parser rejects.**
- **Found during:** Task 2 first test run. All three new tests failed with `SqlException: unexpected token [TIME]` at the `TIME ZONE` clause.
- **Issue:** Plan 14-03 Task 2 wrote the test SQL as `SAMPLE BY 1h FROM '...' TO '...' TIME ZONE '...' FILL(NULL)`. QuestDB's actual SAMPLE BY clause order is `SAMPLE BY <unit> [FROM '...' [TO '...']] [FILL(...)] [ALIGN TO CALENDAR] [TIME ZONE '...']`: TIME ZONE must come last. Every existing `TIME ZONE` test in the file (testFillNullKeyedDST, testFillNullDstSparseData, testFillPrevNumericWithTimezone) uses the final-position form.
- **Fix:** Reordered the SQL clause in all three tests to `FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'`. `ALIGN TO CALENDAR` was already implicit in the plan's semantics but became explicit in the SQL to match the existing-test convention.
- **Files modified:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
- **Verification:** All three tests parse and execute; outputs match expected UTC grid positions exactly.
- **Committed in:** `a1525b50af` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 bug in plan-specified test SQL).
**Impact on plan:** The deviation is a syntactic correction to the plan's test SQL; the semantic intent (sub-day + TIME ZONE + FROM/TO + FILL with `Europe/London`) is preserved exactly. All three Case A/B/C scenarios exercise the Task 1 fix path as the plan intended.

## Issues Encountered

1. **Verifying clause ordering.** The plan's SQL used `TIME ZONE` mid-clause which the parser rejects. Confirmed the correct ordering by grepping existing `TIME ZONE` tests in SampleByFillTest, SampleByTest, and SampleByNanoTimestampTest - all consistently use `FILL(...) ALIGN TO CALENDAR TIME ZONE '...'` as the final clauses.
2. **TO-boundary exclusivity confirmation.** Before writing the tests I worried the plan's expected outputs might be off by one if the TO boundary's wrap shifted exclusivity semantics. In practice the TO wrap preserves the exclusive boundary (see `testFillValueNonKeyedFromTo` precedent at `SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00Z'` producing 5 rows 00:00..04:00). The plan's Case A (24 rows), Case B (4 rows), Case C (3 rows) all landed exactly as the plan predicted.

## User Setup Required

None - no external service configuration required.

## Handoff to Plan 04

**Test insertion slots taken in SampleByFillTest.java:**
- `testFillSubDayTimezoneFromDense` (line 2532, between `testFillPrevWithCalendarOffset` at :2509 and `testFillSubDayTimezoneFromEmpty` at :2565)
- `testFillSubDayTimezoneFromEmpty` (line 2565, between `testFillSubDayTimezoneFromDense` and `testFillSubDayTimezoneFromSparse` at :2622)
- `testFillSubDayTimezoneFromSparse` (line 2622, between `testFillSubDayTimezoneFromEmpty` and `testFillToNullTimestamp` at :2649)

Plan 04 will target `SortedRecordCursorFactory` (M-7 double-free defensive fix). The natural placement for Plan 04's regression test is `testSortedRecordCursorFactoryConstructorThrow` or a similarly named test in SampleByFillTest.java at the `testSort*` position in the alphabetical order. Plan 04 should verify no alphabetical-position collision with the three `testFillSubDay*` slots above.

**Codebase state:**
- Sub-day SAMPLE BY with TIME ZONE + FROM/TO now aligns the fill cursor grid with the tsFloor grid. Any future change to fill-range propagation should read the `hasSubDayTimezoneWrap` boolean rather than rederiving the condition from `isSubDay && sampleByTimezoneName != null` inline.
- Cursor-side (`SqlCodeGenerator.generateFill`) is unchanged; the `functionParser.parseFunction(fillFromNode, ...)` call evaluates the `to_utc` wrapper naturally at runtime.
- Plan 04's M-7 fix touches a different code path (SortedRecordCursorFactory constructor). No conflict expected with Plan 03's optimiser edit.

**Regression test suite:**
- 855 tests pass after Plan 14-03 (103 + 302 + 278 + 172). 0 failures, 0 errors.
- Plan 04's M-7 fix should preserve this count and add its own regression test per D-04.

## Self-Check: PASSED

**Created files (none for Plan 14-03 - all edits modify existing files)**

**Modified files verified:**
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` - FOUND (contains `hasSubDayTimezoneWrap`, `createToUtcCall(sampleByFrom, sampleByTimezoneName)` at :8342, `createToUtcCall(sampleByTo, sampleByTimezoneName)` at :8344; no raw `nested.setFillFrom(sampleByFrom)` / `nested.setFillTo(sampleByTo)` pass-through remains)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - FOUND (contains `testFillSubDayTimezoneFromDense`, `testFillSubDayTimezoneFromEmpty`, `testFillSubDayTimezoneFromSparse` @Test methods)

**Commits verified:**
- `93a856332c` - FOUND (Task 1: Align sub-day fill grid with TIME ZONE + FROM anchor)
- `a1525b50af` - FOUND (Task 2: Add regression tests for sub-day fill with TIME ZONE + FROM)

---

*Phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-*
*Completed: 2026-04-20*
