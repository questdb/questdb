---
phase: 03-performance
plan: 02
subsystem: window-functions
tags: [performance, window, percentile, memory]
dependency_graph:
  requires: []
  provides: [capacity-tracked-window-percentile]
  affects: [PercentileDiscDoubleWindowFunctionFactory, PercentileContDoubleWindowFunctionFactory, MultiPercentileDiscDoubleWindowFunctionFactory, MultiPercentileContDoubleWindowFunctionFactory]
tech_stack:
  added: []
  patterns: [capacity-tracked-block-layout, amortized-O1-append, 2x-growth-policy]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java
    - core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java
decisions:
  - INITIAL_CAPACITY=16 elements (128 bytes data + 16 byte header) provides good balance for typical partition sizes
  - Named constants (CAPACITY_OFFSET, SIZE_OFFSET_BLOCK, DATA_OFFSET, INITIAL_CAPACITY) prevent raw offset arithmetic errors
metrics:
  duration: 8 minutes
  completed: "2026-04-13T14:37:44Z"
---

# Phase 03 Plan 02: Capacity-Tracked Window Percentile Append Summary

Replace O(N^2) copy-on-append with amortized O(1) capacity-tracked append-in-place across all 4 partitioned window percentile inner classes, using [capacity|size|data...] block layout with 2x growth.

## What Changed

All 4 partitioned window percentile inner classes now use a capacity-tracked block layout instead of copying the entire data block on every new row. The old code allocated a new block of `8 + (size+1)*8` bytes and copied all values on each append, causing O(N^2) total memory operations per partition. The new code pre-allocates blocks with capacity for 16 elements initially and doubles the capacity when full, achieving amortized O(1) per append and O(N) total.

### Block Layout Change

Old: `[size(8) | val_0(8) | val_1(8) | ...]`
New: `[capacity(8) | size(8) | val_0(8) | val_1(8) | ...]`

### Modified Classes

1. **PercentileDiscOverPartitionFunction** -- pass1(), preparePass2(), pass2()
2. **PercentileContOverPartitionFunction** -- pass1(), preparePass2(), pass2()
3. **MultiPercentileDiscOverPartitionFunction** -- pass1(), preparePass2()
4. **MultiPercentileContOverPartitionFunction** -- pass1(), preparePass2()

Each class received 4 named constants (`CAPACITY_OFFSET=0`, `SIZE_OFFSET_BLOCK=8`, `DATA_OFFSET=16`, `INITIAL_CAPACITY=16`) and updated offset arithmetic throughout.

### Whole-Result-Set Variants

The `*OverWholeResultSetFunction` inner classes already use O(1) sequential append (`listMemory.putDouble(size * 8, d)`) and require no changes (per D-05).

## Tasks Completed

| # | Task | Commit | Key Changes |
|---|------|--------|-------------|
| 1 | Capacity-tracked append for single-percentile variants | de4c34b584 | PercentileDiscOverPartitionFunction, PercentileContOverPartitionFunction |
| 2 | Capacity-tracked append for multi-percentile variants + regression test | a69d7311ee | MultiPercentileDiscOverPartitionFunction, MultiPercentileContOverPartitionFunction, large-partition test |

## Decisions Made

1. **INITIAL_CAPACITY=16**: Provides 4 capacity doublings before reaching 256, which covers most practical partition sizes without excessive initial allocation (144 bytes per partition).
2. **Named constants**: CAPACITY_OFFSET, SIZE_OFFSET_BLOCK, DATA_OFFSET, and INITIAL_CAPACITY replace all raw numeric offsets, mitigating the offset arithmetic error risk identified in Pitfall 1 of RESEARCH.md.

## Verification Results

- **PercentileWindowFunctionTest**: 35/35 tests pass (34 existing + 1 new large-partition test)
- **INITIAL_CAPACITY constant**: Present in all 4 factory files
- **Old O(N^2) pattern**: Zero occurrences of `appendAddressFor(8 + (size` across all window function files
- **Raw offset check**: Zero occurrences of `listPtr + 8` in any OverPartition inner class

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed test result ordering**
- **Found during:** Task 2
- **Issue:** The large-partition test expected `g=1` before `g=0` in ORDER BY g output, but DISTINCT + ORDER BY g naturally returns `g=0` first.
- **Fix:** Corrected expected test output to match actual `ORDER BY g` ascending order.
- **Files modified:** PercentileWindowFunctionTest.java
- **Commit:** a69d7311ee

## Self-Check: PASSED

- [x] PercentileDiscDoubleWindowFunctionFactory.java: FOUND
- [x] PercentileContDoubleWindowFunctionFactory.java: FOUND
- [x] MultiPercentileDiscDoubleWindowFunctionFactory.java: FOUND
- [x] MultiPercentileContDoubleWindowFunctionFactory.java: FOUND
- [x] PercentileWindowFunctionTest.java: FOUND
- [x] Commit de4c34b584: FOUND
- [x] Commit a69d7311ee: FOUND
