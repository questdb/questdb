# External Integrations

**Analysis Date:** 2026-04-09
**Scope:** How the SAMPLE BY fill subsystem integrates with other QuestDB components

## Overview: Two Execution Paths

SAMPLE BY queries with fill can execute on two distinct paths:

1. **GROUP BY fast path (rewritten):** The `SqlOptimiser` rewrites SAMPLE BY into GROUP BY + fill metadata. The `SqlCodeGenerator` then wraps the GROUP BY factory with a fill cursor. This path supports parallel (async) group-by execution.

2. **Cursor path (legacy):** When the rewrite does not apply (e.g., `FILL(LINEAR)`, `ALIGN TO FIRST OBSERVATION`, keyed PREV/NULL with FROM-TO, bind-variable FROM), the old `SampleByFillPrev*` / `SampleByFillNull*` / `SampleByFillValue*` cursor factories execute. These read raw table data and do aggregation + fill in a single pass.

## Integration Point 1: SqlOptimiser.rewriteSampleBy()

**File:** `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (line 7842)

**What it does:**
- Inspects the `QueryModel` for a SAMPLE BY clause.
- If eligible, converts it to GROUP BY on the nested model:
  - Replaces the timestamp column in SELECT with `timestamp_floor_from_offset_utc(interval, ts, from, offset, tz)`.
  - Adds `ORDER BY timestamp_alias ASC` on the nested model.
  - Stores fill metadata on the nested model: `setFillFrom()`, `setFillTo()`, `setFillStride()`, `setFillValues()`.
  - Clears `setSampleBy(null)` so the code generator treats it as GROUP BY.

**Eligibility conditions** (line 7875-7883):
- `sampleBy != null` and `timestamp != null`.
- `sampleByOffset != null` (ALIGN TO CALENDAR only; ALIGN TO FIRST OBSERVATION is excluded).
- Fill is not `PREV` or `LINEAR` (single fill keyword check).
- `sampleByUnit == null` (no dynamic period unit).
- `sampleByFrom` is not a bind variable or function/operation (must be constant literal or absent).

**Keyed query handling** (line 7972-8014):
- Detects key columns (non-timestamp, non-aggregate literals/functions).
- If keyed AND (fill is not NONE or FROM-TO is present): falls through to the old cursor path by returning the unmodified model.
- Non-keyed queries proceed with the rewrite.

**Model metadata flow:**
- `QueryModel.setFillStride(sampleBy)` -- e.g., `'1h'`.
- `QueryModel.setFillValues(sampleByFill)` -- list of fill expressions (e.g., `[null]`, `[prev]`, `[0, 0]`).
- `QueryModel.setFillFrom(sampleByFrom)` / `QueryModel.setFillTo(sampleByTo)` -- range bounds.
- These fields are read by `SqlCodeGenerator.generateFill()`.

## Integration Point 2: SqlOptimiser.rewriteSampleByFromTo()

**File:** `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (line 8263)

**What it does:**
- Runs before `rewriteSampleBy()` in the optimiser pipeline (line 11148).
- Converts FROM-TO bounds into a WHERE clause: `WHERE ts >= FROM AND ts < TO`.
- Handles timezone conversion: wraps FROM/TO in `to_utc(expr, tz)` if a non-UTC timezone is specified.
- This narrows down the scan to only the relevant partition range before GROUP BY executes.

## Integration Point 3: SqlCodeGenerator.generateSelectGroupBy()

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (line 7526)

**What it does:**
- Entry point for GROUP BY code generation.
- If the model still has a SAMPLE BY node (not rewritten), delegates to `generateSampleBy()` (old cursor path).
- Otherwise, builds a GROUP BY factory (keyed or not-keyed, parallel or single-threaded).
- Wraps the GROUP BY factory with `generateFill()` at three call sites:
  - Line 7719: Parallel keyed GROUP BY (`GroupByRecordCursorFactory` -- vectorized).
  - Line 7883: Parallel keyed GROUP BY (`AsyncGroupByRecordCursorFactory`).
  - Line 7943: Single-threaded keyed GROUP BY (`GroupByRecordCursorFactory`).
- Non-keyed queries do NOT call `generateFill()` because fill is not supported for non-keyed GROUP BY without SAMPLE BY rewrite (the rewrite converts them to keyed-by-timestamp).

**Guard:** `guardAgainstFillWithKeyedGroupBy()` (line 9880) throws `SqlException` if fill is used with more than 1 key column. Single-key (timestamp-only) is allowed.

## Integration Point 4: SqlCodeGenerator.generateFill()

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (line 3225)

**What it does:**
- Reads fill metadata from the `QueryModel`: `getFillStride()`, `getFillFrom()`, `getFillTo()`, `getFillValues()`.
- If no fill stride or fill values = `NONE`, returns the GROUP BY factory unwrapped.
- Parses fill values into `Function` instances via `functionParser.parseFunction()`.
- Determines `timestampIndex` in the GROUP BY output metadata by matching the `timestamp_floor` alias.
- Creates a `TimestampSampler` from the stride string.
- Builds per-column fill specification (`IntList fillModes`, `ObjList<Function> constantFillFuncs`):
  - `FILL_CONSTANT (-1)`: fill with a constant value or NULL.
  - `FILL_PREV_SELF (-2)`: fill from the previous row's own value.
  - Timestamp column always gets `FILL_CONSTANT` with `NullConstant.NULL`.
- Calls `generateOrderBy(groupByFactory, model, executionContext)` to sort the GROUP BY output.
- Creates a `RecordSink` via `RecordSinkFactory` for key column extraction.
- Returns `new SampleByFillRecordCursorFactory(...)`.

## Integration Point 5: generateOrderBy() and followedOrderByAdvice()

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (line 6473)

**What it does:**
- Checks `recordCursorFactory.followedOrderByAdvice()`. If `true`, skips sorting entirely.
- Otherwise, inspects `model.getOrderHash()` for ORDER BY columns and wraps the factory in a sort factory.
- The ORDER BY on the GROUP BY result is injected by `rewriteSampleBy()` (line 8200-8208): `ORDER BY timestamp_alias ASC`.

**followedOrderByAdvice() implementations:**
- `SampleByFillRecordCursorFactory.followedOrderByAdvice()` returns `true` (line 109) -- the new unified fill cursor expects already-sorted input and signals to outer ORDER BY nodes to skip redundant sorting.
- `FillRangeRecordCursorFactory` does NOT override (inherits default `false`) -- the old non-keyed fill cursor emits data rows first, then fill rows at the end (two-phase), so output is not sorted.
- `RecordCursorFactory` default returns `false`.

## Integration Point 6: SampleByFillRecordCursorFactory (New Unified Fill Cursor)

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`

**Architecture:**
- Wraps a sorted GROUP BY `RecordCursorFactory` as its base.
- Contains a `SampleByFillCursor` inner class implementing `NoRandomAccessRecordCursor`.
- Streaming single-pass design: walks sorted input, emits data rows at matching bucket timestamps and fill rows at gaps.
- `FillRecord` inner class delegates to `baseRecord` for data rows, and to fill logic (constant or prev) for gap rows.
- Uses `fillTimestampFunc` (`FillTimestampConstant`) to set the timestamp on fill rows.

**Current state (WIP):**
- `isNonKeyed` is hardcoded to `true` (line 228) -- keyed support is pending.
- Prev fill uses a flat `long[] simplePrev` array indexed by column, storing column values as raw long bits.
- DST handling: if `dataTs < nextBucketTimestamp`, emits the data row as-is (handles timestamp_floor_utc non-monotonicity in UTC space during DST fall-back).

**Lifecycle:**
- `getCursor()` calls `cursor.of(baseCursor, executionContext)` which inits fill functions and calls `toTop()`.
- `_close()` frees base factory, from/to functions, and constant fill functions.

## Integration Point 7: FillRangeRecordCursorFactory (Old Non-Keyed Fill)

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/FillRangeRecordCursorFactory.java`

**Architecture:**
- Two-phase design: Phase 1 scans base cursor, collects all timestamps in a `DirectLongList`. Phase 2 emits fill rows for gaps.
- Output is NOT ordered (data rows first, fill rows appended).
- Supports `FILL(VALUE)` and `FILL(NULL)` only (no PREV).
- Does not implement `followedOrderByAdvice()` (returns default `false`).
- Uses `DirectLongList.sortAsUnsigned()` if base output was unsorted.

**Note:** This factory is being replaced by `SampleByFillRecordCursorFactory` on the GROUP BY fast path.

## Integration Point 8: Old Cursor-Path Fill Factories

These are the legacy SAMPLE BY implementations that do aggregation + fill in a single pass over raw table data:

**Keyed:**
- `SampleByFillPrevRecordCursorFactory` / `SampleByFillPrevRecordCursor` (`griffin/engine/groupby/`) -- keyed FILL(PREV). Uses `Map` + `RecordSink` for per-key state.
- `SampleByFillNullRecordCursorFactory` -- keyed FILL(NULL).
- `SampleByFillValueRecordCursorFactory` / `SampleByFillValueRecordCursor` -- keyed FILL(value).

**Non-Keyed:**
- `SampleByFillPrevNotKeyedRecordCursor` / `SampleByFillPrevNotKeyedRecordCursorFactory`
- `SampleByFillNoneNotKeyedRecordCursor` / `SampleByFillNoneNotKeyedRecordCursorFactory`
- `SampleByFillValueNotKeyedRecordCursor` / `SampleByFillValueNotKeyedRecordCursorFactory`
- `SampleByFillNullNotKeyedRecordCursorFactory`

**Special:**
- `SampleByInterpolateRecordCursorFactory` -- FILL(LINEAR), always uses the cursor path.
- `SampleByFirstLastRecordCursorFactory` -- optimized for first()/last() with symbol key + index.

These factories inherit from:
- `AbstractSampleByFillRecordCursorFactory` (`griffin/engine/groupby/AbstractSampleByFillRecordCursorFactory.java`) -- sets up Map and RecordSink for keyed cursors.
- `AbstractSampleByRecordCursorFactory` -- base for all SAMPLE BY factories.
- `AbstractVirtualRecordSampleByCursor` / `AbstractSampleByCursor` -- base cursor classes managing timestamp sampling, timezone, FROM-TO range.

## Integration Point 9: Async Group By Factories (Upstream of Fill)

**Files:**
- `core/src/main/java/io/questdb/griffin/engine/table/AsyncGroupByRecordCursorFactory.java` -- parallel keyed GROUP BY. Distributes work across workers via page frame cursors.
- `core/src/main/java/io/questdb/griffin/engine/table/AsyncGroupByNotKeyedRecordCursorFactory.java` -- parallel non-keyed GROUP BY.
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByRecordCursorFactory.java` -- single-threaded vectorized keyed GROUP BY (Rosti-based).
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByNotKeyedRecordCursorFactory.java` -- single-threaded non-keyed GROUP BY.

These produce unsorted GROUP BY output that `generateFill()` wraps with sorting + fill.

## Pipeline Summary (GROUP BY Fast Path)

```
SQL: SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(NULL) FROM '2024-01-01' TO '2024-01-02'

1. SqlOptimiser.rewriteSampleByFromTo()
   - Adds WHERE ts >= '2024-01-01' AND ts < '2024-01-02'

2. SqlOptimiser.rewriteSampleBy()
   - Replaces SELECT ts -> SELECT timestamp_floor_from_offset_utc('1h', ts, '2024-01-01', '00:00', null) AS ts
   - Stores fillStride='1h', fillFrom='2024-01-01', fillTo='2024-01-02', fillValues=[null]
   - Adds ORDER BY ts ASC
   - Clears SAMPLE BY

3. SqlCodeGenerator.generateSelectGroupBy()
   - Builds AsyncGroupByRecordCursorFactory (or GroupByRecordCursorFactory)
   - Calls generateFill(model, groupByFactory, executionContext)

4. SqlCodeGenerator.generateFill()
   - Reads fillStride, fillFrom, fillTo, fillValues from model
   - Creates TimestampSampler for '1h'
   - Builds fillModes: [FILL_CONSTANT for ts, FILL_CONSTANT(null) for avg]
   - Calls generateOrderBy() to sort GROUP BY output by ts
   - Returns SampleByFillRecordCursorFactory wrapping sorted factory

5. At execution time:
   SampleByFillCursor streams sorted buckets, emitting fill rows for gaps
```

## Environment Configuration

**Required for fill subsystem:**
- No external env vars. All configuration is internal (`CairoConfiguration`).
- `TimestampSampler` and `TimestampDriver` are pure in-process abstractions.

## Key Interfaces for Fill Extension

When adding keyed fill support to `SampleByFillRecordCursorFactory`:
- `RecordSink` (`cairo/RecordSink.java`) -- needed to extract key columns for per-key prev tracking.
- `RecordSinkFactory.getInstance()` -- already called in `generateFill()` (line 3380-3381), but the `RecordSink` is not yet used by the cursor.
- `Map` interface (`cairo/map/Map.java`) -- may be needed for keyed prev state, or a flat array scheme can be used if keys are ordinal.

---

*Integration audit: 2026-04-09*
