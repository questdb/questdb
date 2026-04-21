# Architecture

**Analysis Date:** 2026-04-09

## Pattern Overview

**Overall:** Dual-path execution for SAMPLE BY with fill

SAMPLE BY queries with fill clauses (NULL, PREV, constant values) execute through one of two paths:

1. **Slow path (cursor-based):** `generateSampleBy()` in `SqlCodeGenerator` produces dedicated `SampleByFill*RecordCursor` objects that iterate raw data, maintain per-key Maps, and perform aggregation + fill inline. Used for ALIGN TO FIRST OBSERVATION, LINEAR fill, PREV fill (keyed), and other cases the optimizer cannot rewrite.

2. **Fast path (GROUP BY rewrite):** `rewriteSampleBy()` in `SqlOptimiser` transforms the SAMPLE BY model into a GROUP BY with `timestamp_floor_utc()`, then `generateFill()` in `SqlCodeGenerator` wraps the result in a fill cursor (`SampleByFillRecordCursorFactory` or `FillRangeRecordCursorFactory`). Supports parallel execution and vectorized aggregation.

**Key Characteristics:**
- The fast path fires early during optimization (before GROUP BY codegen) and produces a standard GROUP BY model with metadata (`fillStride`, `fillFrom`, `fillTo`, `fillValues`) stored on `QueryModel`.
- The slow path is the fallback; `generateSampleBy()` constructs fill cursors that read raw data and aggregate row-by-row using `Map`.
- Both paths share `TimestampSampler` for bucket calculation and `SampleByFillRecord` (slow) / `FillRecord` (fast) for row emission.

## Layers

**SQL Parser (model construction):**
- Purpose: Parses SAMPLE BY clause, fill mode, FROM/TO, offset, timezone into `QueryModel` fields.
- Location: `core/src/main/java/io/questdb/griffin/model/QueryModel.java`
- Key fields: `sampleBy`, `sampleByFill`, `sampleByOffset`, `sampleByFrom`, `sampleByTo`, `sampleByTimezoneName`, `sampleByUnit`
- Also stores rewritten fill fields: `fillStride`, `fillFrom`, `fillTo`, `fillValues` (set by optimizer during rewrite)

**SQL Optimizer (rewrite):**
- Purpose: Rewrites SAMPLE BY into GROUP BY + timestamp_floor_utc + ORDER BY. Transfers fill metadata to `QueryModel`.
- Location: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java`, method `rewriteSampleBy()` (line ~7842)
- Depends on: `QueryModel`, `ExpressionNode`, `FunctionFactoryCache`
- Used by: `SqlCodeGenerator.generateSelectGroupBy()`

**SQL Code Generator (factory construction):**
- Purpose: Builds `RecordCursorFactory` trees. Routes to either `generateSampleBy()` (slow) or `generateFill()` (fast).
- Location: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
- Key methods:
  - `generateSelectGroupBy()` (line ~7526) -- entry point; dispatches based on `model.getSampleBy()` presence
  - `generateSampleBy()` (line ~6776) -- builds slow-path cursor factories
  - `generateFill()` (line ~3225) -- wraps fast-path GROUP BY factory with fill cursor

**Fill Cursors (execution):**
- Purpose: Emit data rows + synthesized fill rows for time gaps.
- Location: `core/src/main/java/io/questdb/griffin/engine/groupby/`
- Fast-path cursors:
  - `SampleByFillRecordCursorFactory` -- unified fill cursor for the GROUP BY rewrite path (NEW)
  - `FillRangeRecordCursorFactory` -- earlier fill cursor for NULL/value fill only (no PREV support, two-pass with `DirectLongList`)
- Slow-path cursors:
  - `SampleByFillPrevRecordCursor` / `SampleByFillPrevRecordCursorFactory` (keyed PREV)
  - `SampleByFillPrevNotKeyedRecordCursor` / `SampleByFillPrevNotKeyedRecordCursorFactory` (non-keyed PREV)
  - `SampleByFillNullRecordCursorFactory` / `SampleByFillValueRecordCursorFactory` (keyed NULL/value)
  - `SampleByFillNoneRecordCursor` / `SampleByFillNoneRecordCursorFactory` (no fill)
  - `SampleByInterpolateRecordCursorFactory` (LINEAR fill)

**Timestamp Sampling:**
- Purpose: Calculates bucket boundaries (next/previous timestamp, rounding).
- Location: `core/src/main/java/io/questdb/griffin/engine/groupby/TimestampSampler.java` (interface)
- Implementations: `SimpleTimestampSampler` (fixed-size), `MonthTimestampMicrosSampler`, `YearTimestampMicrosSampler`, `WeekTimestampMicrosSampler` + nano variants
- Factory: `TimestampSamplerFactory.java`

## Data Flow

**Fast Path (SAMPLE BY rewritten to GROUP BY + fill):**

1. Parser creates `QueryModel` with `sampleBy`, `sampleByFill`, `sampleByOffset`, etc.
2. `SqlOptimiser.rewriteSampleBy()` checks eligibility (ALIGN TO CALENDAR, not LINEAR/PREV fill, not keyed with fill, no bind variable FROM, etc.).
3. Optimizer replaces the timestamp column expression with `timestamp_floor_utc('interval', ts, from, offset, timezone)`, adds GROUP BY + ORDER BY, clears `sampleBy`, and stores fill metadata (`fillStride`, `fillFrom`, `fillTo`, `fillValues`) on the nested `QueryModel`.
4. `SqlCodeGenerator.generateSelectGroupBy()` sees no `sampleBy` (cleared) and generates a standard `GroupByRecordCursorFactory` or `AsyncGroupByRecordCursorFactory` (parallel).
5. `generateFill()` detects `fillStride != null` on the model, constructs `TimestampSampler`, parses fill expressions, builds per-column `fillModes` and `constantFillFuncs` IntList/ObjList, sorts the GROUP BY output, and wraps it in `SampleByFillRecordCursorFactory`.
6. At execution time, `SampleByFillCursor.hasNext()` iterates the sorted base cursor, compares data timestamps to expected bucket timestamps, and either passes through data rows or emits fill rows with `fillRecord.isGapFilling = true`.

**Slow Path (dedicated SAMPLE BY cursors):**

1. Parser creates `QueryModel` with `sampleBy` populated.
2. Optimizer's `rewriteSampleBy()` skips the model (fails eligibility: ALIGN TO FIRST OBSERVATION, LINEAR, keyed with fill, PREV, etc.).
3. `SqlCodeGenerator.generateSelectGroupBy()` sees `sampleBy != null` and calls `generateSampleBy()`.
4. `generateSampleBy()` creates the base factory via `generateSubQuery()`, creates `TimestampSampler`, then dispatches to one of:
   - `SampleByFillPrevRecordCursorFactory` (PREV, keyed)
   - `SampleByFillPrevNotKeyedRecordCursorFactory` (PREV, non-keyed)
   - `SampleByFillNullRecordCursorFactory` (NULL, keyed)
   - `SampleByFillNoneRecordCursorFactory` (NONE, keyed)
   - `SampleByFillValueRecordCursorFactory` (constant values, keyed)
   - `SampleByInterpolateRecordCursorFactory` (LINEAR)
   - + NotKeyed variants for each
5. At execution time, the cursor iterates raw input, maintains a `Map` with composite keys (timestamp bucket + group-by keys), runs aggregation functions per row, and emits fill rows for missing buckets by walking between data buckets.

**Rewrite Eligibility (SqlOptimiser line 7875-7882):**

The fast path fires when ALL of these conditions hold:
- `sampleBy != null` (SAMPLE BY clause present)
- `timestamp != null` (designated timestamp exists)
- `sampleByOffset != null` (ALIGN TO CALENDAR, not FIRST OBSERVATION)
- Fill is NONE, NULL, or constant values (not PREV, not LINEAR)
- `sampleByUnit == null` (standard interval syntax, not `SAMPLE BY n UNIT`)
- `sampleByFrom` is a constant (not a bind variable, function, or operation)
- No keyed queries with fill (bails out at line 8001)
- No sub-query as the data source when keyed with fill (bails out at line 7978)

## Key Abstractions

**SampleByFillRecordCursorFactory (new unified fast-path fill cursor):**
- Purpose: Wraps sorted GROUP BY output, inserts fill rows for missing timestamp buckets. Supports NULL, PREV, and constant value fill.
- File: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
- Fill modes per column: `FILL_CONSTANT` (-1), `FILL_PREV_SELF` (-2), or index >= 0 (cross-column prev reference)
- Inner class `SampleByFillCursor` implements `NoRandomAccessRecordCursor`
- Inner class `FillRecord` delegates to base record (data) or returns fill values (gap)
- Reports `followedOrderByAdvice() = true` (output maintains timestamp order)

**FillRangeRecordCursorFactory (earlier fast-path fill cursor):**
- Purpose: Two-pass design for NULL/value fill only. Pass 1 emits all data rows and collects present timestamps in `DirectLongList`. Pass 2 walks timestamp range and emits fill-only rows for gaps.
- File: `core/src/main/java/io/questdb/griffin/engine/groupby/FillRangeRecordCursorFactory.java`
- Limitation: Does not support PREV fill. Output is NOT time-ordered (data rows first, then fill rows).

**AbstractSampleByFillRecordCursorFactory (slow-path base):**
- Purpose: Base for all slow-path keyed fill cursor factories. Creates `Map` for key tracking and `RecordSink` for key extraction.
- File: `core/src/main/java/io/questdb/griffin/engine/groupby/AbstractSampleByFillRecordCursorFactory.java`
- Hierarchy: `AbstractSampleByFillRecordCursorFactory` extends `AbstractSampleByRecordCursorFactory` extends `AbstractRecordCursorFactory`

**TimestampSampler:**
- Purpose: Abstracts timestamp bucketing (next, previous, round). Implementations handle fixed intervals vs. calendar intervals (month, year, week).
- File: `core/src/main/java/io/questdb/griffin/engine/groupby/TimestampSampler.java`
- `setStart(long timestamp)` anchors the bucket grid.
- `nextTimestamp(long timestamp)` advances to next bucket boundary.
- `round(long value)` snaps a timestamp to its bucket start.

**QueryModel (fill metadata):**
- Purpose: AST node for SELECT queries. Stores both original SAMPLE BY fields and rewritten fill fields.
- File: `core/src/main/java/io/questdb/griffin/model/QueryModel.java`
- Original fields: `sampleBy`, `sampleByFill`, `sampleByOffset`, `sampleByFrom`, `sampleByTo`, `sampleByTimezoneName`, `sampleByUnit`
- Rewritten fields (set by optimizer, consumed by codegen): `fillStride`, `fillFrom`, `fillTo`, `fillValues`

## Entry Points

**SQL compilation entry:**
- Location: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, `generateSelectGroupBy()` (line ~7526)
- Triggers: Any SELECT with GROUP BY or SAMPLE BY
- Responsibilities: Checks for `sampleBy` presence; if present, routes to `generateSampleBy()` (slow path). If absent (rewritten), generates GROUP BY factory then calls `generateFill()`.

**Optimizer rewrite entry:**
- Location: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java`, `rewriteSampleBy()` (line ~7842)
- Triggers: Called during optimizer's `optimise()` pass, recursively processes all nested/join/union models.
- Responsibilities: Converts eligible SAMPLE BY to GROUP BY + timestamp_floor_utc, sets fill metadata on model.

**Fill cursor construction:**
- Location: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, `generateFill()` (line ~3225)
- Triggers: Called after GROUP BY factory creation when `fillStride` is set on the model.
- Responsibilities: Parses fill expressions, creates `TimestampSampler`, builds `SampleByFillRecordCursorFactory`, wraps sorted GROUP BY output.

## Error Handling

**Strategy:** Exception-based with resource cleanup via try-catch-close patterns.

**Patterns:**
- `SqlException.$()` for user-facing errors with position information.
- `generateFill()` catches `Throwable`, frees all allocated objects (`fillValues`, `fillFromFunc`, `fillToFunc`, `groupByFactory`), and re-throws.
- `SampleByFillRecordCursorFactory.getCursor()` catches `Throwable`, closes cursor, and re-throws.
- `guardAgainstFillWithKeyedGroupBy()` throws `SqlException` when fill + multi-key GROUP BY is attempted.
- `guardAgainstFromToWithKeyedSampleBy()` throws `SqlException` when FROM-TO + keyed SAMPLE BY is attempted.

## Cross-Cutting Concerns

**DST Handling:** The slow path (`AbstractSampleByCursor`) tracks `nextDstUtc`, `topTzOffset`, and `TimeZoneRules` to handle DST transitions. The fast path relies on `timestamp_floor_utc()` to handle bucketing in the presence of timezones, with DST awareness via the timezone parameter. The `SampleByFillCursor` includes a fallback for non-monotonic UTC timestamps produced by DST fall-back.

**Keyed vs Non-Keyed:** The slow path has separate factories for keyed vs non-keyed (e.g., `SampleByFillPrevRecordCursorFactory` vs `SampleByFillPrevNotKeyedRecordCursorFactory`). The fast path's `SampleByFillRecordCursorFactory` currently treats all queries as non-keyed (`isNonKeyed = true`), with keyed support marked as pending. The `FillRangeRecordCursorFactory` also only handles non-keyed queries.

**Parallel Execution:** The fast path enables parallel GROUP BY (via `AsyncGroupByRecordCursorFactory` or vectorized `GroupByRecordCursorFactory`). The slow path is single-threaded.

**Validation:** `validateSampleByFillType` configuration flag controls whether fill value types are validated against column types during GROUP BY function assembly.

---

*Architecture analysis: 2026-04-09*
