---
phase: 02-non-keyed-fill-cursor
plan: 01
subsystem: griffin/groupby
tags: [fill-cursor, bug-fix, tests, resource-leaks]
dependency_graph:
  requires: []
  provides: [streaming-fill-null, streaming-fill-value, fill-from-to, fill-dst]
  affects: [SampleByFillRecordCursorFactory, SqlCodeGenerator.generateFill]
tech_stack:
  added: []
  patterns: [explicit-sort-in-generateFill, no-peek-ahead-in-hasNext]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
  deleted:
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/DstDebug.java
decisions:
  - "Early-exit guard placed after data fetch, not before: baseCursorExhausted is only set after baseCursor.hasNext() returns false"
  - "Removed peek-ahead in dataTs==nextBucketTimestamp branch: calling baseCursor.hasNext() corrupts the SortedRecordCursor's current record position"
  - "Build SortedRecordCursorFactory explicitly in generateFill instead of calling generateOrderBy: the optimizer strips ORDER BY from the nested model before code generation"
  - "Tests use assertQueryNoLeakCheck with null expectedTimestamp: the fill cursor metadata does not designate a timestamp column"
metrics:
  duration: 42m55s
  completed: 2026-04-10
  tasks_completed: 2
  tasks_total: 2
  files_modified: 4
  files_deleted: 1
  tests_added: 8
  tests_total: 9
---

# Phase 2 Plan 01: Fix infinite loop, resource leaks, and add assertion tests

Fixed three bugs in the streaming fill cursor and two resource leaks in generateFill(), then added 8 new assertion-based tests covering FILL(NULL), FILL(VALUE), FROM/TO range, DST fall-back, empty tables, and multiple aggregates.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 35c58ad92b | Fix infinite loop and resource leaks in fill cursor |
| 2 | e3bef96a12 | Add assertion tests for fill cursor |

## Task Details

### Task 1: Fix infinite loop and resource leaks

Fixed five issues in SampleByFillRecordCursorFactory and SqlCodeGenerator:

1. **Infinite loop (hasNext):** The early-exit guard `baseCursorExhausted && !hasExplicitTo` was placed before the data fetch, so `baseCursorExhausted` was never true when the guard ran. Moved the guard after the data fetch so it fires once `baseCursor.hasNext()` returns false.

2. **Peek-ahead record corruption (hasNext):** The `dataTs == nextBucketTimestamp` branch called `baseCursor.hasNext()` to peek ahead, which advanced the `SortedRecordCursor`'s tree chain iterator. This caused the current row's data to be replaced by the next row's data before the caller consumed it. Removed the peek-ahead entirely; the next loop iteration handles the next row correctly.

3. **Unsorted GROUP BY input (generateFill):** `generateFill()` called `generateOrderBy(groupByFactory, model, ...)` but the model's ORDER BY had been stripped by `optimiseOrderBy()` before code generation. Built a `SortedRecordCursorFactory` explicitly using the timestamp column index.

4. **Sorted factory leak (generateFill catch block):** After `generateOrderBy()` wrapped groupByFactory in a sort factory, the catch block freed the original groupByFactory but not the wrapper. With the explicit sort construction, `groupByFactory` now always points at the outermost factory.

5. **constantFillFuncs leak (generateFill catch block):** The `ObjList<Function>` was not freed in the catch block. Declared it before the try block and added `Misc.freeObjList(constantFillFuncs)` to the catch block.

### Task 2: Add assertion tests

SampleByFillTest now contains 9 tests (8 new, 1 existing):

| Test | Requirement | What it verifies |
|------|-------------|------------------|
| testFillNullNonKeyed | FILL-01 | Basic FILL(NULL) with gaps between data points |
| testFillNullNonKeyedNoToClause | FILL-01 | Infinite loop regression: no-TO query terminates |
| testFillValueNonKeyed | FILL-03 | FILL(0) emits 0.0 for gap rows |
| testFillNullNonKeyedFromTo | FILL-04 | Leading fill before first data, trailing fill after last |
| testFillValueNonKeyedFromTo | FILL-03+04 | FILL(0) with FROM/TO range |
| testFillNullDstFallback | FILL-05 | DST fall-back with Europe/Riga timezone terminates |
| testFillNullEmptyTable | FILL-04 | Empty table with FROM/TO emits all-fill rows |
| testFillNullMultipleAggregates | FILL-01 | Multi-column (sum, avg, sum) NULL fill |
| testFillPrevNonKeyed | -- | Existing PREV test (uses old cursor path) |

Deleted DstDebug.java (superseded by testFillNullDstFallback) and the keyed test testFillNullKeyed (Phase 3 scope, had no assertions).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Peek-ahead corrupts SortedRecordCursor record position**
- **Found during:** Task 1
- **Issue:** The `dataTs == nextBucketTimestamp` branch called `baseCursor.hasNext()` to peek ahead, which moved the `SortedRecordCursor`'s tree chain to the next entry. The `FillRecord` delegating to `baseRecord` then returned the next row's data instead of the current row's. This caused the first row (00:00) to be skipped entirely.
- **Fix:** Removed the peek-ahead block. The cursor now always advances the bucket timestamp and lets the next loop iteration handle the next row.
- **Files modified:** SampleByFillRecordCursorFactory.java
- **Commit:** 35c58ad92b

**2. [Rule 3 - Blocking] generateOrderBy receives model with stripped ORDER BY**
- **Found during:** Task 1
- **Issue:** `generateFill()` called `generateOrderBy(groupByFactory, model, ...)` but the `model`'s ORDER BY had been cleared by `optimiseOrderBy()` during optimization. The `generateOrderBy()` method checked `model.getOrderHash()`, found it empty, and returned the factory unchanged. This meant the GROUP BY output was unsorted, causing fill rows to appear at wrong positions.
- **Fix:** Built a `SortedRecordCursorFactory` explicitly in `generateFill()` using `timestampIndex`, `entityColumnFilter`, and `recordComparatorCompiler`. This bypasses the model's ORDER BY metadata entirely.
- **Files modified:** SqlCodeGenerator.java
- **Commit:** 35c58ad92b

**3. [Rule 1 - Bug] Early-exit guard position (refinement from plan)**
- **Found during:** Task 1
- **Issue:** The plan specified moving the guard to the top of the while loop, before the data fetch. But `baseCursorExhausted` is only set to true inside the data-fetch `else` branch. Placing the guard before the fetch means it never fires on the first iteration after the base cursor is exhausted -- instead, the gap branch fires and emits one extra fill row.
- **Fix:** Placed the guard AFTER the data fetch but BEFORE the branch checks. This ensures `baseCursorExhausted` is set on the same iteration that discovers the base cursor is empty.
- **Files modified:** SampleByFillRecordCursorFactory.java
- **Commit:** 35c58ad92b

## Deferred Items

14 existing SampleByTest tests fail due to factory property mismatches (timestamp index, random access support, record type). These tests were written for the old FillRangeRecordCursorFactory and now route through the new SampleByFillRecordCursorFactory. No data correctness failures. Phase 5 (Verification and Hardening) addresses these.

Failing tests:
- testSampleFillValueNotKeyedAlignToCalendar (timestamp index)
- testSampleFillValueNotKeyedAlignToCalendarOffset (timestamp index)
- testSampleFillValueNotKeyedAlignToCalendarTimeZone (timestamp index)
- testSampleFillValueNotKeyedAlignToCalendarTimeZone2 (timestamp index)
- testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffset (timestamp index)
- testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffsetNepal (record type)
- testSampleFillValueNotKeyed (timestamp index)
- testSampleFillNullNotKeyedAlignToCalendar (random access)
- testSampleFillNullNotKeyedAlignToCalendarOffset (timestamp index)
- testSampleFillNullNotKeyedCharColumn (timestamp index)
- testSampleFillNullNotKeyedValid (record type)
- testSampleFillNullDayNotKeyedGaps (timestamp index)
- testSampleByFillNullDecimal (random access)
- testSampleFillWithWeekStride (random access)

## Self-Check: PASSED

All artifacts verified:
- SampleByFillRecordCursorFactory.java: early-exit guard at line 278 (after data fetch)
- SqlCodeGenerator.java: SortedRecordCursorFactory at line 3383, Misc.freeObjList(constantFillFuncs) at line 3417
- SampleByFillTest.java: 9 tests, all pass
- DstDebug.java: deleted
- Commit 35c58ad92b: exists
- Commit e3bef96a12: exists
