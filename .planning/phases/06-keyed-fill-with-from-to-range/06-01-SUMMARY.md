---
phase: 06-keyed-fill-with-from-to-range
plan: 01
subsystem: griffin/groupby
tags: [fill, keyed, from-to, sigsegv, tests]
dependency_graph:
  requires: [03-01]
  provides: [keyed-fill-from-to-correctness, sigsegv-fix]
  affects: [SampleByFillRecordCursorFactory, SampleByFillTest]
tech_stack:
  added: []
  patterns: [zero-key-early-return]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
decisions:
  - "Zero-key guard in initialize() returns early with maxTimestamp=Long.MIN_VALUE to produce empty result"
  - "Berlin at bucket 03:00 emitted before London because sorted base cursor orders alphabetically"
metrics:
  duration: 4m
  completed: 2026-04-10
---

# Phase 6 Plan 01: Fix SIGSEGV Crash + 8 Keyed FROM/TO Fill Tests Summary

Guard zero-key keyed fill initialize to prevent SIGSEGV, add 8 tests covering cartesian product emission, fill modes, and edge cases for keyed SAMPLE BY FILL with FROM/TO range.

## What Was Done

### Task 2: Fix SIGSEGV crash for zero-key keyed FROM/TO (executed first)

Added a `keyCount == 0` guard in `SampleByFillCursor.initialize()` immediately after pass 1 key discovery. When GROUP BY returns zero rows (e.g., FROM is after all data), the guard sets `baseCursorExhausted = true`, `maxTimestamp = Long.MIN_VALUE`, and `nextBucketTimestamp = Long.MAX_VALUE`, then returns early. This prevents `hasNext()` from reaching `emitNextFillRow()` with an empty keysMap, which caused a SIGSEGV via `Unsafe_GetInt` on the unpositioned `keysMapRecord`.

### Task 1: Add 8 keyed FROM/TO fill tests

Added 8 new test methods to `SampleByFillTest.java` in alphabetical order:

1. **testFillNullKeyedFromTo** -- 6 buckets x 2 keys with leading/trailing null fills
2. **testFillNullKeyedFromToAfterData** -- FROM after all data returns empty result (SIGSEGV trigger)
3. **testFillNullKeyedFromToBeforeDataToWithinData** -- FROM before data, TO within data, boundary excluded
4. **testFillNullKeyedFromToEmptyRange** -- FROM == TO produces zero rows
5. **testFillNullKeyedFromToKeyAppearsMidRange** -- 3 keys, Berlin discovered mid-range gets null from FROM
6. **testFillNullKeyedFromToMultipleAggregates** -- two aggregate columns with keyed FROM/TO
7. **testFillPrevKeyedFromTo** -- per-key FILL(PREV) independence across FROM/TO range
8. **testFillValueKeyedFromTo** -- FILL(0) constant values with keyed FROM/TO

All tests use `assertMemoryLeak()` and `assertSql()` with hardcoded expected output.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 2 | db80f1be48 | Guard zero-key case in keyed fill initialize |
| 1 | 340c700249 | Add 8 keyed FROM/TO fill tests |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed key order at bucket 03:00 in testFillNullKeyedFromToKeyAppearsMidRange**
- **Found during:** Task 1 test verification
- **Issue:** Expected output had London before Berlin at bucket 03:00, but the sorted base cursor emits Berlin before London alphabetically when both have data in the same bucket.
- **Fix:** Swapped Berlin and London in the expected output for bucket 03:00.
- **Files modified:** SampleByFillTest.java
- **Commit:** 340c700249

## Test Results

- Tests run: 23 (15 existing + 8 new)
- Failures: 0
- Errors: 0
- Skipped: 0
- All `assertMemoryLeak()` checks pass (no native memory leaks)

## Requirements Coverage

| Requirement | Tests |
|-------------|-------|
| KFTR-01 | testFillNullKeyedFromTo, testFillValueKeyedFromTo, testFillNullKeyedFromToKeyAppearsMidRange, testFillNullKeyedFromToMultipleAggregates |
| KFTR-02 | testFillNullKeyedFromTo, testFillNullKeyedFromToKeyAppearsMidRange, testFillNullKeyedFromToBeforeDataToWithinData |
| KFTR-03 | testFillNullKeyedFromTo |
| KFTR-04 | testFillPrevKeyedFromTo |
| KFTR-05 | testFillNullKeyedFromToMultipleAggregates |

## Self-Check: PASSED

- All source files exist on disk
- All commit hashes found in git log
- 23/23 tests pass
