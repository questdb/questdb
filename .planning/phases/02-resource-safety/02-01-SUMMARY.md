---
phase: 02-resource-safety
plan: 01
subsystem: window-functions
tags: [resource-leak, zero-gc, memory-safety, window-functions]
dependency_graph:
  requires: []
  provides: [DirectArray-lifecycle-safety, zero-gc-preparePass2]
  affects: [MultiPercentileDiscDoubleWindowFunctionFactory, MultiPercentileContDoubleWindowFunctionFactory]
tech_stack:
  added: []
  patterns: [isResultValid-flag, Misc.free-assign-pattern, array-reuse-guard]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java
    - core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java
decisions:
  - Used isResultValid boolean flag instead of results != null to distinguish empty-data from stale-data in WholeResultSet variants
  - Only close() nulls the double[] results array; all other lifecycle methods clear the validity flag
  - Used supportsRandomAccess=true and expectSize=true in assertQueryNoLeakCheck to match window function cursor properties
metrics:
  duration: 5m 38s
  completed: "2026-04-13T13:57:41Z"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 3
---

# Phase 02 Plan 01: Fix DirectArray Lifecycle Leak and Zero-GC Violation Summary

Free DirectArray via `result = Misc.free(result)` in all lifecycle methods of 4 Multi* window percentile inner classes, and pre-allocate/reuse double[] in WholeResultSet preparePass2() with isResultValid flag for empty-data detection.

## Changes Made

### Task 1: Fix DirectArray lifecycle leak and double[] pre-allocation in all 4 inner classes

**Commit:** a77a06f05d

Added `result = Misc.free(result)` to reopen(), reset(), and toTop() in all 4 inner classes across both factory files (12 total occurrences). This prevents native memory leaks when lifecycle methods trigger without a matching close().

In the 2 WholeResultSet variants:
- Added `isResultValid` boolean field (placed alphabetically between `listMemory` and `percentilesFunc`)
- Modified preparePass2() to use `if (results == null || results.length < percentileCount)` guard instead of unconditional `new double[percentileCount]`, eliminating per-call heap allocation
- Modified preparePass2() to set `isResultValid = true` after computation and `isResultValid = false` on empty-data path
- Modified getArray() to check `isResultValid` instead of `results != null`
- Removed `results = null` from reopen/reset/toTop (only close() nulls it now)
- Added `results = null` to close() where it was missing

**Files modified:**
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java`

### Task 2: Add memory leak regression tests using assertQueryNoLeakCheck for reopen cycles

**Commit:** b5f2b70175

Added 4 new test methods that exercise the reopen path via assertQueryNoLeakCheck() inside assertMemoryLeak(). The assertQueryNoLeakCheck method calls factory.getCursor() twice, which triggers reopen() on the window function. The assertMemoryLeak wrapper detects any native memory leak from the first getCursor's DirectArray not being freed.

Tests added:
- `testMultiPercentileContOverPartitionReopen`
- `testMultiPercentileContOverWholeResultSetReopen`
- `testMultiPercentileDiscOverPartitionReopen`
- `testMultiPercentileDiscOverWholeResultSetReopen`

All 34 tests pass (30 existing + 4 new), 0 failures, 0 errors.

**File modified:**
- `core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed assertQueryNoLeakCheck call signature**
- **Found during:** Task 2
- **Issue:** The plan specified using the 4-arg overload `assertQueryNoLeakCheck(expected, query, null, null)` which defaults to `supportsRandomAccess=true, expectSize=false`. Window function cursors report a known size, causing "Invalid/undetermined cursor size expected but was 10" assertion failure.
- **Fix:** Used the 6-arg overload with `supportsRandomAccess=true, expectSize=true` to match the cursor's actual properties.
- **Files modified:** PercentileWindowFunctionTest.java
- **Commit:** b5f2b70175

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| `isResultValid` flag over `results != null` | Allows reusing the double[] array across lifecycle resets while correctly detecting empty-data path (size == 0) |
| Only close() nulls results array | Lifecycle methods (reopen/reset/toTop) should invalidate but not deallocate the Java array, enabling zero-GC reuse |
| `expectSize=true` in assertQueryNoLeakCheck | Window function cursors have deterministic row counts; the test framework validates this property alongside data correctness |

## Verification Results

1. `grep -rn "result = Misc.free(result)"` across both factory files: 12 occurrences (3 methods x 2 inner classes x 2 files)
2. `grep -rn "isResultValid"` across both factory files: field declaration + 6 usages in each WholeResultSet class (14 total)
3. `grep -n "results = null"` across both factory files: appears only in close() methods (2 occurrences)
4. `mvn -pl core -Dtest=PercentileWindowFunctionTest test`: 34 tests, 0 failures, 0 errors
5. Test count increased by 4 (30 existing + 4 new = 34)

## Self-Check: PASSED

All 3 modified files exist on disk. Both task commits (a77a06f05d, b5f2b70175) exist in git history. SUMMARY.md created at expected path.
