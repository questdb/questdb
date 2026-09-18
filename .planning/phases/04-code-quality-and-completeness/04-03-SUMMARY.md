---
phase: 04-code-quality-and-completeness
plan: 03
subsystem: window-function-tests
tags: [tests, window-functions, percentile, quantile, assertQueryNoLeakCheck]
dependency_graph:
  requires: [04-02]
  provides: [complete-window-test-coverage]
  affects: [PercentileWindowFunctionTest.java]
tech_stack:
  added: []
  patterns: [assertQueryNoLeakCheck-6-arg-overload]
key_files:
  modified:
    - core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java
decisions:
  - Used 6-arg assertQueryNoLeakCheck(expected, query, null, null, true, true) since no 2-arg overload exists
  - Single-percentile PARTITION BY tests already existed from earlier work; no new ones needed
  - Single-percentile ROWS frame and ORDER BY error tests already existed as RejectsNonWholePartitionFrame and RejectsOrderBy
metrics:
  duration: 9m 24s
  completed: 2026-04-13T16:22:26Z
  tasks_completed: 2
  tasks_total: 2
---

# Phase 04 Plan 03: Window Function Test Migration and Coverage Summary

Migrated all 26 assertSql() calls to assertQueryNoLeakCheck() with factory property validation, added 6 new test methods for quantile alias equivalence and multi-percentile error path coverage.

## Tasks Completed

### Task 1: Migrate assertSql to assertQueryNoLeakCheck

**Commit:** `77007bbb13`

Replaced all 26 `assertSql()` calls with `assertQueryNoLeakCheck()` using the 6-arg overload `(expected, query, null, null, true, true)` to validate both data correctness and factory properties (supportsRandomAccess, expectSize). The 2-arg convenience overload does not exist in AbstractCairoTest; the closest 4-arg overload defaults expectSize to false, which window function cursors do not support. The 8 existing `assertQueryNoLeakCheck` calls and 4 `assertException` calls remained unchanged.

**Verification:**
- 0 assertSql() calls remain
- 34 assertQueryNoLeakCheck() calls (26 migrated + 8 existing)
- 4 assertException() calls unchanged
- Compilation successful

### Task 2: Add quantile alias, ORDER BY error, and ROWS frame error tests

**Commit:** `dc5e6e3250`

Added 6 new test methods:

| Test Method | Purpose |
|-------------|---------|
| `testQuantileDiscAliasEquivalence` | Verifies quantile_disc window alias produces identical results to percentile_disc |
| `testQuantileContAliasEquivalence` | Verifies quantile_cont window alias produces identical results to percentile_cont |
| `testMultiPercentileDiscRejectsOrderBy` | ORDER BY rejection for multi-percentile disc |
| `testMultiPercentileContRejectsOrderBy` | ORDER BY rejection for multi-percentile cont |
| `testMultiPercentileDiscRejectsRowsFrame` | ROWS frame rejection for multi-percentile disc |
| `testMultiPercentileContRejectsRowsFrame` | ROWS frame rejection for multi-percentile cont |

**Pre-existing coverage confirmed:**
- Single-percentile PARTITION BY: `testPercentileDiscOverPartition`, `testPercentileContOverPartition`
- Single-percentile ORDER BY rejection: `testPercentileDiscRejectsOrderBy`, `testPercentileContRejectsOrderBy`
- Single-percentile ROWS frame rejection: `testPercentileDiscRejectsNonWholePartitionFrame`, `testPercentileContRejectsNonWholePartitionFrame`

**Verification:** All 41 tests pass (35 existing + 6 new), 0 failures, 0 errors.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Used 6-arg overload instead of 2-arg assertQueryNoLeakCheck**
- **Found during:** Task 1
- **Issue:** The plan suggested a 2-arg `assertQueryNoLeakCheck(expected, query)` overload, but AbstractCairoTest only provides 4-arg (with expectSize=false default) and 6-arg overloads. Window cursors require expectSize=true.
- **Fix:** Used 6-arg form `(expected, query, null, null, true, true)` matching the pattern from Phase 2 existing tests.
- **Files modified:** PercentileWindowFunctionTest.java
- **Commit:** 77007bbb13

**2. [Rule 3 - Blocking] Skipped redundant single-percentile ROWS/RANGE and PARTITION BY tests**
- **Found during:** Task 2
- **Issue:** The plan suggested adding single-percentile ROWS frame and PARTITION BY tests. These already exist as `testPercentileDiscRejectsNonWholePartitionFrame`, `testPercentileContRejectsNonWholePartitionFrame`, `testPercentileDiscOverPartition`, and `testPercentileContOverPartition`.
- **Fix:** Confirmed existing coverage satisfies the requirement. Only added multi-percentile variants which were genuinely missing.
- **Files modified:** None (existing tests sufficient)

**3. [Rule 3 - Blocking] Test execution in worktree blocked by native library loading**
- **Found during:** Task 2 verification
- **Issue:** The agent worktree at `.claude/worktrees/agent-a93fc63b` cannot load QuestDB native libraries (`Os.loadLib` fails with "cannot unpack null"), preventing test execution.
- **Fix:** Verified tests by temporarily copying the test file to the working `nw_percentile` worktree at `/Users/sminaev/qdbwt/abstract-crafting-sunrise` where native libraries load correctly. All 41 tests pass.

## Final Counts

| Metric | Count |
|--------|-------|
| assertSql() calls | 0 |
| assertQueryNoLeakCheck() calls | 36 |
| assertException() calls | 8 (4 existing + 4 new) |
| Total test methods | 41 |
| New test methods added | 6 |
| Tests passing | 41/41 |

## Self-Check: PASSED

```
FOUND: core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java
FOUND: 77007bbb13 (Task 1 commit)
FOUND: dc5e6e3250 (Task 2 commit)
```
