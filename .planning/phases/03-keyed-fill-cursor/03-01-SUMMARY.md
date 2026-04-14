---
phase: 03-keyed-fill-cursor
plan: 01
subsystem: griffin/engine/groupby
tags: [fill-cursor, keyed-fill, ordered-map, cartesian-product]
dependency_graph:
  requires: [02-01]
  provides: [keyed-fill-cursor, fill-key-mode, per-key-prev]
  affects: [SqlOptimiser, SqlCodeGenerator, SampleByFillRecordCursorFactory]
tech_stack:
  added: [OrderedMap, MapRecord, RecordSink]
  patterns: [two-pass-streaming, per-key-prev-in-MapValue, FILL_KEY-mode]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/SqlOptimiser.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
decisions:
  - "FILL_KEY = -3 constant distinguishes key columns from aggregates in fillModes array"
  - "OrderedMap stores key combinations with per-key prev in MapValue slots [keyIndex, hasPrev, prevCols...]"
  - "keyPosOffset compensates for value columns preceding key columns in MapRecord index space"
  - "symbolTableColIndices covers all map columns (value + key) for SYMBOL resolution"
  - "Non-keyed PREV preserved via simplePrev fallback when keysMap is null"
  - "PREV keyword skipped in functionParser to prevent Invalid column error"
metrics:
  duration: 112m
  completed: 2026-04-10
  tasks_completed: 2
  tasks_total: 2
  files_modified: 4
  tests_added: 6
  tests_total: 15
---

# Phase 3 Plan 01: Keyed Fill Cursor Summary

OrderedMap-based two-pass keyed fill cursor with cartesian product emission and per-key prev tracking via MapValue slots

## What Changed

### SqlOptimiser.java
- Removed `!isPrevKeyword(...)` clause from the fast-path gate at line 7880, allowing FILL(PREV) queries to reach the GROUP BY fast path
- Removed the `if (isKeyed) { return model; }` bail-out block that prevented keyed queries from proceeding through the rewrite

### SqlCodeGenerator.java
- Removed all three `guardAgainstFillWithKeyedGroupBy(model, keyTypes)` calls
- Added `isKeyColumn()` helper that checks `bottomUpColumns` AST for LITERAL type at non-timestamp positions
- Modified fillModes build loops to assign `FILL_KEY` to key columns; key columns do not consume a fill expression index
- Built `keyColIndices`, `mapKeyTypes`, `mapValueTypes`, `keySink`, and `symbolTableColIndices` for OrderedMap construction
- Added PREV keyword skip in the fillValues parsing loop to prevent functionParser from failing on PREV tokens

### SampleByFillRecordCursorFactory.java
- Added `FILL_KEY = -3` constant alongside existing `FILL_CONSTANT` and `FILL_PREV_SELF`
- Updated constructor to accept `keySink`, `mapKeyTypes`, `mapValueTypes`, `keyColIndices`, `symbolTableColIndices`
- Factory creates `OrderedMap` for keyed queries, passes it to the cursor
- `_close()` now frees the keysMap

### SampleByFillCursor (inner class)
- **Pass 1 (initialize)**: Iterates sorted base cursor, inserts key tuples into OrderedMap via keySink. Each new key gets a sequential index and hasPrev=0. Calls `baseCursor.toTop()`. Sets up `keysMapRecord.setSymbolTableResolver()`.
- **Pass 2 (hasNext)**: For each time bucket: emits data rows while marking keys present in `boolean[] keyPresent`, then emits fill rows for absent keys via `emitNextFillRow()` which iterates `keysMapCursor` in insertion order.
- `updatePerKeyPrev()` stores aggregate values as raw long bits in MapValue slots
- `prevValue()` reads per-key prev from MapValue during fill row emission
- `keyPosOffset` compensates for the MapRecord column layout (value columns precede key columns)
- Non-keyed queries still work via `simplePrev[]` fallback when `keysMap == null`

### FillRecord (inner class)
- Every `getXxx()` method now checks for `FILL_KEY` mode and delegates to `keysMapRecord` with the correct column offset
- `hasKeyPrev()` checks either MapValue (keyed) or `hasSimplePrev` (non-keyed)

### SampleByFillTest.java
- 6 new keyed fill tests added (total: 15 tests)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] PREV keyword parsed as column name**
- **Found during:** Task 1 verification
- **Issue:** FILL(PREV) queries now reach generateFill() but `functionParser.parseFunction()` treated PREV as a column reference, failing with "Invalid column: PREV"
- **Fix:** Skip functionParser for PREV tokens in the fillValues parsing loop, add NullConstant.NULL placeholder
- **Files modified:** SqlCodeGenerator.java
- **Commit:** bb8c6b7080

**2. [Rule 1 - Bug] MapRecord column index offset missing**
- **Found during:** Task 2 test execution
- **Issue:** FillRecord accessed key columns at MapRecord indices 0, 1, etc. but MapRecord stores value columns first (keyIndex, hasPrev, prevCols...) then key columns. The STRING key column accessed value column 0 (a LONG), causing NPE.
- **Fix:** Added `keyPosOffset` field computed as `2 + aggColumnCount`. Applied to all `outputColToKeyPos[]` entries.
- **Files modified:** SampleByFillRecordCursorFactory.java
- **Commit:** 4ede9d32cf

**3. [Rule 1 - Bug] symbolTableColIndices only covered key columns**
- **Found during:** Task 2 SYMBOL test execution
- **Issue:** `getSymA(columnIndex)` on MapRecord uses `symbolTableIndex.getQuick(columnIndex)` where columnIndex includes value column offset. The IntList only had entries for key columns (size = keyCount), causing IndexOutOfBoundsException at index 3 for a list of size 1.
- **Fix:** Built symbolTableColIndices with entries for ALL map columns: -1 for each value column, then SYMBOL resolution indices for key columns.
- **Files modified:** SqlCodeGenerator.java
- **Commit:** 4ede9d32cf

**4. [Rule 1 - Bug] Non-keyed bucket not advanced after data row**
- **Found during:** Task 1 verification
- **Issue:** The rewritten hasNext() did not advance nextBucketTimestamp after emitting a non-keyed data row, causing duplicate fill rows at the same timestamp.
- **Fix:** Added `nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp)` in the non-keyed branch of the `dataTs == nextBucketTimestamp` handler.
- **Files modified:** SampleByFillRecordCursorFactory.java
- **Commit:** bb8c6b7080

## Decisions Made

1. **keyPosOffset pattern**: Rather than changing the FillRecord to subtract the offset on every access, `outputColToKeyPos[]` stores the pre-computed offset-adjusted index. This avoids runtime arithmetic in the hot path.

2. **Non-keyed as separate path (not degenerate case)**: The CONTEXT.md suggested treating non-keyed as a degenerate case with an empty keysMap key. Instead, non-keyed queries use `keysMap == null` with `simplePrev[]` fallback. This avoids complications with empty RecordSink and preserves the simpler non-keyed code path.

3. **assertSql for keyed tests**: Keyed fill tests use `assertSql()` instead of `assertQueryNoLeakCheck()` because the factory properties (random access, expected timestamp) may differ from the cursor path. Data correctness is the priority.

## Self-Check: PASSED

All files exist. All commits verified.
