---
phase: 03-performance
plan: 01
subsystem: griffin/groupby
tags: [performance, quickselect, percentile, groupby]
dependency_graph:
  requires: []
  provides: [GroupByLongList.quickSelect, GroupByLongList.quickSelectMultiple]
  affects: [PercentileDiscLongGroupByFunction]
tech_stack:
  added: []
  patterns: [quickselect, median-of-three-pivot, iterative-partitioning]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/GroupByLongList.java
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/GroupByLongListTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunctionFactoryTest.java
decisions:
  - "Used int parameters for quickSelect lo/hi/k (matching size() return type and swap(int,int) signature)"
  - "Added bounds validation to quickSelectMultiple (lo >= 0, hi < size) which GroupByDoubleList lacks"
metrics:
  duration: 5m35s
  completed: "2026-04-13T14:27:13Z"
  tasks_completed: 2
  tasks_total: 2
  test_count: 31
  files_modified: 4
---

# Phase 03 Plan 01: Port quickSelect to GroupByLongList Summary

O(n) average-case quickSelect ported from GroupByDoubleList to GroupByLongList with long value types, then wired into PercentileDiscLongGroupByFunction replacing the O(n log n) full sort path.

## What Changed

### Task 1: Port quickSelect and quickSelectMultiple to GroupByLongList

Added four methods to GroupByLongList, ported from GroupByDoubleList:

- `quickSelect(int lo, int hi, int k)` -- public entry with bounds validation
- `quickSelectMultiple(int lo, int hi, int[] indices, int from, int to)` -- public entry with bounds validation (enhanced vs. GroupByDoubleList per D-08)
- `quickSelectImpl(int lo, int hi, int k)` -- private iterative implementation
- `partition(int lo, int hi)` -- private median-of-three Lomuto partition

Key adaptation from double to long: pivot variable is `long`, all value comparisons use `long` (via getQuick which returns long), index parameters remain `int` matching size() return type and swap(int,int) signature.

Added 11 new tests covering: median/min/max selection, bounds validation (4 cases), all-equal data, and multi-index selection.

**Commits:** 830999b269 (RED: failing tests), ff4e21fc5d (GREEN: implementation)

### Task 2: Update PercentileDiscLongGroupByFunction to use quickSelect

Replaced `listA.sort()` with `listA.quickSelect(0, size - 1, N)` in getLong(). Moved percentile/multiplier/N computation before the quickSelect call since quickSelect needs k=N as input. Added a new test validating correctness across multiple groups with distinct value ranges and all-equal data.

**Commit:** e2194f8d0c

## Deviations from Plan

None - plan executed exactly as written.

## Verification Results

- GroupByLongListTest: 12 tests, 0 failures
- PercentileDiscLongGroupByFunctionFactoryTest: 19 tests, 0 failures
- `listA.sort()` no longer appears in PercentileDiscLongGroupByFunction.getLong()
- `listA.quickSelect(0, size - 1, N)` confirmed present

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 (RED) | 830999b269 | Add failing tests for GroupByLongList quickSelect |
| 1 (GREEN) | ff4e21fc5d | Port quickSelect to GroupByLongList |
| 2 | e2194f8d0c | Use quickSelect in PercentileDiscLong |

## Self-Check: PASSED

All 4 modified files exist on disk. All 3 commits (830999b269, ff4e21fc5d, e2194f8d0c) found in git log.
