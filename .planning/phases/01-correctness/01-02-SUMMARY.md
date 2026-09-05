---
phase: 01-correctness
plan: 02
subsystem: window-functions
tags: [correctness, sort-algorithm, validation, window-functions]
dependency_graph:
  requires: []
  provides: [constness-validation, correct-sort]
  affects: [percentile-disc-window, percentile-cont-window, multi-percentile-disc-window, multi-percentile-cont-window]
tech_stack:
  added: []
  patterns: [dutch-national-flag-partition, insertion-sort-fallback, compile-time-validation]
key_files:
  created:
    - core/src/test/java/io/questdb/test/griffin/engine/functions/window/PercentileWindowFunctionTest.java
  modified:
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java
decisions:
  - "Place INSERTION_SORT_THRESHOLD at factory class level so both inner classes share the constant"
  - "Use position 34 for constness error pointing at the CASE keyword in the non-constant expression"
  - "Threshold of 47 for insertion sort fallback matches GroupByDoubleList reference implementation"
metrics:
  duration: 14 minutes
  completed: "2026-04-13T12:44:00Z"
  tasks_completed: 2
  tasks_total: 2
---

# Phase 01 Plan 02: Window Percentile Constness Validation and Sort Fix Summary

Add compile-time constness validation for percentile arguments in all 4 window function factories and replace 8 broken Lomuto quickSort implementations with correct Dutch National Flag three-way partition sort that handles all-equal data in O(N).

## Tasks Completed

### Task 1: Add constness validation to all 4 window function factories (COR-04, D-05)
- **Commit:** b6df919d07
- Added `isConstant()` check in `newInstance()` for `PercentileDiscDoubleWindowFunctionFactory` and `PercentileContDoubleWindowFunctionFactory` with error message "percentile must be a constant"
- Added `isConstant()` check in `newInstance()` for `MultiPercentileDiscDoubleWindowFunctionFactory` and `MultiPercentileContDoubleWindowFunctionFactory` with error message "percentile array must be a constant"
- Created `PercentileWindowFunctionTest` with `testPercentileDiscNonConstantPercentile` and `testPercentileContNonConstantPercentile` error-path tests
- All checks run after `windowContext.validate()` and before any window function object creation

### Task 2: Replace broken Lomuto quickSort with correct dual-pivot sort (COR-02, D-01, D-02)
- **Commit:** 42395f0d7d
- Replaced all 8 Lomuto partition quickSort implementations across 4 factories (2 inner classes each)
- New sort uses Dutch National Flag (three-way) partitioning: elements < pivot, == pivot, > pivot
- Equal elements are skipped entirely, preventing O(N^2) degradation on all-equal data
- Added insertion sort fallback for subarrays smaller than 47 elements
- Removed all 8 standalone `partition()` methods (logic now inline in `quickSort`)
- Added 3 regression tests: `testPercentileDiscAllEqualValues`, `testPercentileContAllEqualValues`, `testPercentileDiscAllEqualValuesPartitioned` -- each with 10,000 identical values

## Verification Results

All 35 tests pass (5 new + 30 existing discovered tests):
- 2 constness error-path tests confirm SqlException on non-constant percentile
- 3 all-equal-values tests confirm correct results without O(N^2) degradation
- 30 existing tests confirm no regressions

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed partitioned test expected output ordering**
- **Found during:** Task 2 test creation
- **Issue:** The plan's expected output for `testPercentileDiscAllEqualValuesPartitioned` used DISTINCT which produced "Invalid/undetermined cursor size" assertion failures in `assertQueryNoLeakCheck`. The DISTINCT query changes cursor properties that the framework validates.
- **Fix:** Changed test to use LIMIT 5 instead of DISTINCT, checking first 5 rows of the partitioned window result. This correctly exercises the partitioned sort path with 10,000 all-equal values without DISTINCT cursor property issues.
- **Files modified:** PercentileWindowFunctionTest.java
- **Commit:** 42395f0d7d (included in Task 2 commit)

## Self-Check: PASSED

All 5 key files found on disk. Both task commits (b6df919d07, 42395f0d7d) verified in git log.
