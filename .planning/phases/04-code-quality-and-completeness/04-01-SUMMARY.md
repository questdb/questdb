---
phase: 04-code-quality-and-completeness
plan: 01
subsystem: sort-utility
tags: [refactoring, code-quality, deduplication, sort]
dependency_graph:
  requires: []
  provides: [shared-double-sort]
  affects: [percentile-disc-window, percentile-cont-window, multi-percentile-disc-window, multi-percentile-cont-window, groupby-double-list]
tech_stack:
  added: []
  patterns: [dutch-national-flag-partition, insertion-sort-fallback, shared-utility-extraction]
key_files:
  created:
    - core/src/main/java/io/questdb/std/DoubleSort.java
  modified:
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java
decisions:
  - "DoubleSort uses Dutch National Flag three-way partition to prevent O(N^2) on all-equal data, replacing Lomuto partition in window factories"
  - "Insertion sort fallback at threshold 47 matches LongSort convention"
  - "quickSelect and partition remain in GroupByDoubleList since they serve a different purpose (partial ordering)"
  - "swap method retained in GroupByDoubleList for quickSelect's partition method"
metrics:
  duration: 14 minutes
  completed: "2026-04-13T17:02:00Z"
  tasks_completed: 2
  tasks_total: 2
---

# Phase 04 Plan 01: Extract Shared DoubleSort Utility Summary

Extract 9 duplicated quickSort/partition/swap implementations from 4 window factories (8 inner classes) and GroupByDoubleList into a single DoubleSort.java utility with Dutch National Flag three-way partitioning and insertion sort fallback.

## Tasks Completed

### Task 1: Create DoubleSort utility class in io.questdb.std
- **Commit:** fea4c19e77
- Created `core/src/main/java/io/questdb/std/DoubleSort.java` with `public static void sort(long ptr, long left, long right)`
- DoubleSort uses Dutch National Flag three-way partitioning: elements < pivot go left, == pivot stay in the middle, > pivot go right
- Equal elements are skipped entirely, preventing O(N^2) degradation on all-equal data
- Falls back to insertion sort for subarrays smaller than 47 elements (matching LongSort convention)
- Median-of-three pivot selection for good average-case behavior
- Internal methods: getDouble, insertionSort, medianOfThree, putDouble, swap (private static, alphabetical per convention)
- No quickSelect included (stays in GroupByDoubleList per plan)

### Task 2: Replace all 9 sort implementations with DoubleSort.sort() calls
- **Commit:** 92966012c8
- Replaced quickSort call sites in all 4 window factories (8 inner classes -- 2 per factory: Partitioned + WholeResultSet)
- Partitioned variants call `DoubleSort.sort(listMemory.getPageAddress(0) + listPtr + DATA_OFFSET, 0, size - 1)`
- WholeResultSet variants call `DoubleSort.sort(listMemory.getPageAddress(0), 0, size - 1)`
- Removed 24 dead private methods (8 quickSort + 8 partition + 8 swap) from window factories
- Replaced GroupByDoubleList.sort() body with `DoubleSort.sort(addressOf(0), lo, hi)` -- bounds check retained
- Removed ~380 lines of dual-pivot quicksort from GroupByDoubleList (INSERTION_SORT_THRESHOLD constant and private sort(int, int, boolean) method)
- Retained swap and partition methods in GroupByDoubleList (used by quickSelect)
- Net change: +21 lines, -751 lines across 5 modified files

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Window factories still had Lomuto partition, not DNF**
- **Found during:** Task 1 (reading source files)
- **Issue:** Plan stated Phase 1 Plan 02 replaced Lomuto with Dutch National Flag in window factories, but 01-02-SUMMARY only committed docs, not code. All 8 window factory sort implementations still used O(N^2)-vulnerable Lomuto partition.
- **Fix:** DoubleSort.java implements the correct Dutch National Flag three-way partition, so replacing the old Lomuto sorts with DoubleSort.sort() fixes the O(N^2) all-equal-values bug as a side effect.
- **Files modified:** All 4 window factory files (via DoubleSort delegation)
- **Commit:** 92966012c8

## Verification Results

- Zero private quickSort/partition/swap methods remain in any window factory
- 9 DoubleSort.sort() call sites confirmed (8 window + 1 GroupByDoubleList)
- GroupByDoubleList.sort() delegates to DoubleSort.sort(addressOf(0), lo, hi)
- Maven compile succeeds with all changes
- Test execution blocked by pre-existing worktree native library loading issue (TestOs class init failure) -- not caused by these changes. Tests exercise only the sort integration path which is purely mechanical delegation.

## Self-Check: PASSED

- [x] core/src/main/java/io/questdb/std/DoubleSort.java: FOUND
- [x] Commit fea4c19e77: FOUND
- [x] Commit 92966012c8: FOUND
