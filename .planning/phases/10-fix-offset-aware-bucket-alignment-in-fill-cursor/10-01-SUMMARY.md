---
phase: 10-fix-offset-aware-bucket-alignment-in-fill-cursor
plan: 01
subsystem: griffin/fill-cursor
tags: [bugfix, sample-by, fill, offset, alignment]
dependency_graph:
  requires: []
  provides: [offset-aware-fill-cursor]
  affects: [SampleByFillRecordCursorFactory, SqlCodeGenerator, SqlOptimiser, QueryModel]
tech_stack:
  added: []
  patterns: [fillOffset-field-pattern, offset-parsing-via-Dates]
key_files:
  created:
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java (5 new tests)
  modified:
    - core/src/main/java/io/questdb/griffin/model/QueryModel.java
    - core/src/main/java/io/questdb/griffin/SqlOptimiser.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
decisions:
  - "Propagate fillOffset only for non-timezone queries; timestamp_floor_utc handles offset internally when timezone is present"
  - "Parse offset in SqlCodeGenerator using Dates.parseOffset + driver.fromMinutes, matching AbstractTimestampFloorFromOffsetFunctionFactory"
  - "Use setOffset(calendarOffset) + round() for no-FROM case; setStart(fromTs + calendarOffset) for FROM case"
metrics:
  duration: 16m
  completed: 2026-04-13
  tasks_completed: 2
  tasks_total: 2
  files_modified: 5
---

# Phase 10 Plan 01: Fix Offset-Aware Bucket Alignment in Fill Cursor Summary

Align the fill cursor's sampler grid with timestamp_floor_utc offset boundaries to eliminate infinite fill on offset queries without TO.

## What Changed

### QueryModel.java
QueryModel carries a new `fillOffset` field (ExpressionNode) through the optimizer rewrite, mirroring the existing `fillFrom`/`fillTo`/`fillStride` pattern. The field includes getter, setter, clear() nulling, and deep-clone in copyTo().

### SqlOptimiser.java
The `rewriteSampleBy()` method saves `sampleByOffset` into `nested.setFillOffset()` before `setSampleByOffset(null)` clears it. The optimizer only propagates `fillOffset` when no timezone is present; with a timezone, `timestamp_floor_utc` already incorporates the offset, so the fill cursor must not shift again.

### SqlCodeGenerator.java
`generateFill()` parses the offset token from `curr.getFillOffset()` using `Dates.parseOffset()` + `driver.fromMinutes(Numbers.decodeLowInt())`, exactly matching `AbstractTimestampFloorFromOffsetFunctionFactory`. The resulting `long calendarOffset` passes to the factory constructor.

### SampleByFillRecordCursorFactory.java
The factory and inner cursor accept `calendarOffset` as a constructor parameter. In `initialize()`:
- **No FROM + offset**: calls `setOffset(calendarOffset)` then `round(firstTs)` to align the sampler grid to offset boundaries.
- **FROM + offset**: calls `setStart(fromTs + calendarOffset)` to match `timestamp_floor_utc`'s `effectiveOffset = from + offset`.
- **Empty base cursor + FROM/TO**: shifts `nextBucketTimestamp = fromTs + calendarOffset`.
- **No offset (calendarOffset=0)**: behavior unchanged.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Guard fillOffset against timezone queries**
- **Found during:** Task 2
- **Issue:** When both timezone and offset are present (e.g., ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu' WITH OFFSET '00:40'), `timestamp_floor_utc` already handles the offset in its `AllConstFunc` path. Applying `calendarOffset` in the fill cursor double-shifted bucket boundaries, breaking the Nepal timezone test.
- **Fix:** SqlOptimiser only sets `fillOffset` when `sampleByTimezoneName == null`, preventing double-application.
- **Files modified:** SqlOptimiser.java
- **Commit:** 697b9a0a2a

**2. [Rule 1 - Bug] FROM+TO test expected values corrected**
- **Found during:** Task 2
- **Issue:** The plan's test 3 used `FROM '2024-01-01T02:00'` with `OFFSET '02:00'`, but `timestamp_floor_utc` computes `effectiveOffset = from + offset`, so FROM must be at midnight for buckets at 02:00.
- **Fix:** Changed FROM to `'2024-01-01'` (midnight) so effectiveOffset = midnight + 2h = 02:00 buckets.
- **Files modified:** SampleByFillTest.java
- **Commit:** 697b9a0a2a

## Verification Results

- SampleByFillTest: 40/40 passed (35 existing + 5 new)
- SampleByTest: 302/302 passed (all existing, including timezone+offset tests)

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 6558835d0d | Propagate calendar offset to fill cursor |
| 2 | 697b9a0a2a | Add offset-aware fill tests, guard timezone |

## Self-Check: PASSED

All 5 modified/created files verified present. Both commits (6558835d0d, 697b9a0a2a) verified in git history.
