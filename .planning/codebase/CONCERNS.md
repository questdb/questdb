# Codebase Concerns

**Analysis Date:** 2026-04-09

**Scope:** SAMPLE BY fill implementation on the GROUP BY fast path.

---

## DST Non-Monotonic Timestamps

**Severity:** High

- Issue: The `timestamp_floor_utc` function (`core/src/main/java/io/questdb/griffin/engine/functions/date/TimestampFloorFromOffsetUtcFunctionFactory.java`) produces UTC bucket keys that can be non-monotonic during DST fall-back transitions. During a fall-back (e.g., Europe/Berlin Oct 31 at 03:00 CEST -> 02:00 CET), two consecutive UTC input timestamps can floor to UTC outputs that go backward in time. The fill cursor in `SampleByFillRecordCursorFactory.SampleByFillCursor` (`core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`, line 253) uses `TimestampSampler.nextTimestamp()` to compute expected bucket boundaries as a simple arithmetic progression (e.g., +1h), which does not account for DST-induced non-monotonicity.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (lines 259-321, `hasNext()`)
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SimpleTimestampSampler.java` (lines 59-65, `nextTimestamp()`)
  - `core/src/main/java/io/questdb/griffin/engine/functions/date/AbstractTimestampFloorFromOffsetFunctionFactory.java` (lines 310-318, `floorWithTz()`)
  - `core/src/test/java/io/questdb/test/griffin/engine/groupby/DstDebug.java`
- Impact: When the sorted GROUP BY output contains a DST fall-back bucket whose UTC timestamp is less than the previous bucket, the fill cursor hits the `dataTs < nextBucketTimestamp` branch (line 304). The current handler emits the row as-is without advancing the bucket pointer, which produces a row that breaks the output ordering contract. Downstream consumers that rely on `followedOrderByAdvice() == true` (line 109) see non-monotonic timestamps.
- Fix approach: The `TimestampSampler` used by the fill cursor operates in pure UTC arithmetic. It needs timezone-aware bucket stepping that mirrors the `timestamp_floor_utc` logic, or the fill cursor must detect DST fall-back transitions and handle duplicate/backward bucket timestamps. A pragmatic intermediate fix: when `dataTs < nextBucketTimestamp`, set `nextBucketTimestamp = dataTs` and continue, so the cursor re-anchors to the actual data stream.

---

## Keyed Fill Not Implemented (Cartesian Product)

**Severity:** High

- Issue: SAMPLE BY with key columns (e.g., `SELECT ts, key, sum(val) FROM x SAMPLE BY 1h FILL(NULL)`) requires emitting fill rows for ALL known keys in EVERY time bucket where any key is missing. The current fill cursor hard-codes `isNonKeyed = true` (line 228) and does not maintain a key registry or perform cartesian product emission.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (line 227-228: `this.isNonKeyed = true`)
  - `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (lines 8001-8013: optimizer bails out for keyed queries)
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 9880-9900: `guardAgainstFillWithKeyedGroupBy` throws for multi-key GROUP BY)
- Impact: The optimizer (`rewriteSampleBy`, line 8001) currently drops out early for keyed queries, falling back to the old cursor-based SAMPLE BY path. This means keyed FILL queries do not benefit from the GROUP BY fast path. The `guardAgainstFillWithKeyedGroupBy` guard in the code generator also throws `SqlException` for multi-key GROUP BY with FILL.
- Fix approach: Requires a two-pass or map-based approach. Pass 1: iterate the sorted base cursor to discover all unique key combinations (using a `Map` from the QuestDB map framework). Pass 2: for each time bucket, emit data rows from the base cursor and fill rows for any missing keys from the key registry. The `RecordSink` parameter already accepted (but unused) in the constructor (line 89) was scaffolded for this purpose.

---

## FILL(PREV) on Fast Path Is Dead Code

**Severity:** Medium

- Issue: The `SampleByFillRecordCursorFactory` contains full FILL(PREV) support: `FILL_PREV_SELF` mode (line 62), `savePrevValues()` (line 391), `prevValue()` (line 416), `simplePrev` array (line 197), and `hasSimplePrev` flag (line 198). However, the optimizer gate in `SqlOptimiser.rewriteSampleBy()` (line 7880) explicitly excludes `FILL(PREV)` from the fast-path rewrite: `!isPrevKeyword(sampleByFill.getQuick(0).token)`. Since `setFillStride()` is only called inside the rewrite block (line 8167), FILL(PREV) queries never reach `generateFill()`, so the PREV code in the fill cursor is never executed.
- Files:
  - `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (line 7880)
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (lines 62, 183-198, 277-279, 309-311, 391-420)
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 3325-3345: PREV detection in `generateFill`)
  - `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (lines 34-52: `testFillPrevNonKeyed` exercises the OLD path)
- Impact: The `testFillPrevNonKeyed` test in `SampleByFillTest.java` passes but tests the old cursor-based path, not the new fill cursor. The PREV implementation in the new cursor is untested and potentially incorrect. The `simplePrev` array stores values as raw `long` bits via `readColumnAsLongBits()`, which only covers numeric types up to 64 bits. STRING, VARCHAR, SYMBOL, LONG256, DECIMAL128, DECIMAL256, BINARY, and ARRAY types cannot be stored as `long` bits.
- Fix approach: Enable PREV on the fast path by removing the `!isPrevKeyword()` guard in the optimizer, then add comprehensive tests. For non-numeric types, FILL(PREV) needs a `RecordChain` or per-column object storage instead of a flat `long[]`.

---

## Per-Key PREV Tracking Missing

**Severity:** Medium

- Issue: When FILL(PREV) is combined with key columns, each key needs independent prev state. The current cursor uses a single flat `simplePrev` array (line 197), meaning all keys share the same prev values. For a query like `SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV)`, the prev value for London would incorrectly be overwritten by the most-recently-seen city.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (lines 184-198: `prevValues`, `simplePrev` fields; line 227: `isNonKeyed = true`)
- Impact: Blocked by the keyed fill concern above. Once keyed fill is implemented, per-key prev tracking becomes required for correctness. The `prevValues` and `prevInitialized` arrays (lines 184-185) appear to have been scaffolded for this purpose (with comments referencing `keyIndex * columnCount + col`) but are unused.
- Fix approach: Use a `Map<key, long[]>` to maintain per-key prev state. Alternatively, store prev values in the key-enumeration map used for the cartesian product.

---

## Resource Leak: `sorted` Factory in `generateFill` Error Path

**Severity:** Medium

- Issue: In `SqlCodeGenerator.generateFill()` (line 3375), `generateOrderBy(groupByFactory, model, executionContext)` may wrap `groupByFactory` in a new sort factory. If an exception occurs after this call (e.g., during `RecordSinkFactory.getInstance()` at line 3380 or `SampleByFillRecordCursorFactory` construction at line 3384), the catch block (lines 3400-3406) frees `groupByFactory` but not the `sorted` wrapper. When `sorted != groupByFactory`, the sort factory leaks.
- Files:
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 3374-3406)
- Impact: Native memory leak on the error path. The sort factory wraps the GROUP BY factory and may hold a `RecordChain` or sort buffer with native memory.
- Fix approach: After line 3375, reassign `groupByFactory = sorted` so the catch block frees the correct reference. Alternatively, add a separate `Misc.free(sorted)` to the catch block when `sorted != groupByFactory`.

---

## Resource Leak: `constantFillFuncs` Not Freed on Error

**Severity:** Medium

- Issue: In `SqlCodeGenerator.generateFill()`, the `constantFillFuncs` list (line 3322) is populated with `Function` objects. The catch block (lines 3400-3406) frees `fillValues` but not `constantFillFuncs`. When `constantFillFuncs` contains functions other than `NullConstant.NULL` (which is a singleton), those function objects leak.
- Files:
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 3322, 3400-3406)
- Impact: Potential function object leak on error paths. In practice, `constantFillFuncs` mostly contains `NullConstant.NULL` or functions already in `fillValues`, so the leak is often benign.
- Fix approach: Add `Misc.freeObjList(constantFillFuncs)` to the catch block, but be careful about double-frees since some functions may be shared between `fillValues` and `constantFillFuncs`.

---

## Unused `recordSink` Parameter

**Severity:** Low

- Issue: The `SampleByFillRecordCursorFactory` constructor accepts a `RecordSink recordSink` parameter (line 89) but never stores or uses it. The `RecordSink` is constructed via `RecordSinkFactory.getInstance()` in `generateFill()` (line 3380-3382), which generates bytecode. The generated object is immediately discarded.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (line 89)
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 3377-3382)
- Impact: Wasted bytecode generation. No functional bug, but confusing for maintainers.
- Fix approach: Either remove the parameter until keyed fill is implemented, or store it for future use with clear documentation.

---

## `followedOrderByAdvice` Contract Violation with DST

**Severity:** High

- Issue: `SampleByFillRecordCursorFactory.followedOrderByAdvice()` unconditionally returns `true` (line 109). This tells the code generator's `generateOrderBy()` method (line 6478) that the output is already sorted, so no further sorting is needed. However, during DST fall-back transitions, the fill cursor can emit non-monotonic timestamps (see DST concern above), violating the ordering contract.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (line 109)
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 6478-6480: `generateOrderBy` skips sort when `followedOrderByAdvice()` is true)
- Impact: Any outer query that relies on ORDER BY (explicit or implicit) over the fill cursor's output will see unsorted results near DST transitions. This affects materialized views, LATEST ON queries, and joins that assume sorted input.
- Fix approach: Either make `followedOrderByAdvice()` conditional on whether a timezone is configured, or ensure the fill cursor truly produces monotonic output by handling DST correctly.

---

## Sort Overhead for Non-Timezone Queries

**Severity:** Low

- Issue: `generateFill()` always calls `generateOrderBy()` on the GROUP BY output (line 3375), adding a sort step. For non-timezone queries, `timestamp_floor_utc` produces monotonic output when the input is monotonic. The Async GROUP BY (`AsyncGroupByRecordCursorFactory`) produces unordered output, so the sort is necessary. However, for the single-threaded `GroupByRecordCursorFactory` with forward scan on a timestamp-partitioned table, the output is already sorted. The sort is redundant in that case.
- Files:
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (line 3375)
  - `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 6528-6551: `generateOrderBy` short-circuit logic)
- Impact: Performance overhead for simple non-keyed SAMPLE BY FILL queries on well-ordered data. The `generateOrderBy` method does check for pre-sorted data (lines 6528-6551), so it may elide the sort if the factory's scan direction matches. The overhead is only when this check fails.
- Fix approach: No immediate action needed. The `generateOrderBy` optimization already handles the common case. For Async GROUP BY, the sort is unavoidable.

---

## FILL(LINEAR) Look-Ahead Not Implemented

**Severity:** Low (future work)

- Issue: FILL(LINEAR) requires knowing both the left and right data points to interpolate. The current streaming fill cursor emits rows as it encounters gaps, with no ability to defer emission until the right endpoint is known. The optimizer already excludes LINEAR from the fast path (line 7880: `!isLinearKeyword(...)`).
- Files:
  - `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (line 7880)
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
- Impact: LINEAR fill is not available on the fast path. Queries fall back to the old cursor-based path.
- Fix approach: Requires buffering gap rows in a `RecordChain` until the right endpoint arrives, then retroactively computing interpolated values. Significantly more complex than PREV or constant fill.

---

## Two Execution Paths Must Produce Identical Results

**Severity:** Medium

- Issue: The old cursor-based SAMPLE BY path (e.g., `SampleByFillPrevRecordCursorFactory`, `SampleByFillNullRecordCursorFactory`, `SampleByFillValueRecordCursorFactory` in `core/src/main/java/io/questdb/griffin/engine/groupby/`) and the new GROUP BY fast path with `SampleByFillRecordCursorFactory` must produce identical output for all queries. The optimizer silently routes queries to one path or the other based on conditions in `rewriteSampleBy()` (line 7875). Any behavioral divergence is a correctness bug.
- Files:
  - Old path: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursorFactory.java`, `SampleByFillNullRecordCursorFactory.java`, `SampleByFillValueRecordCursorFactory.java`, `SampleByFillNoneRecordCursorFactory.java`
  - New path: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
  - Router: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (line 7875)
- Impact: Queries that differ only in incidental factors (e.g., presence of a non-UTC timezone, sub-query vs. direct table access) may produce different results if one path is correct and the other is not.
- Fix approach: Write parity tests that execute the same logical query through both paths and compare results. The `DstDebug.java` test does this manually for one scenario. Systematic parity testing is needed.

---

## Test Coverage Gaps

**Severity:** Medium

- What's not tested:
  - DST fall-back with FILL on the fast path (`DstDebug.java` is a manual debug tool, not an assertion-based test)
  - FILL(constant) with explicit FROM/TO range where data starts after FROM or ends before TO
  - FILL with multiple aggregate columns of different types
  - `toTop()` correctness (re-iteration of the fill cursor)
  - Error paths in `getCursor()` (line 119-127)
  - Keyed FILL (blocked at optimizer, but the guard error message should be tested)
  - `newSymbolTable()` — the cursor inherits the default that throws `UnsupportedOperationException`
- Files:
  - `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (72 lines, 3 tests)
  - `core/src/test/java/io/questdb/test/griffin/engine/groupby/DstDebug.java` (22 lines, 1 debug test)
- Risk: Regressions in edge cases go undetected. The `testFillPrevNonKeyed` test passes via the old path, creating a false sense of coverage for the new path.
- Priority: High — the fill cursor is WIP and needs comprehensive tests before the optimizer gates are widened.

---

## FillRecord Does Not Handle All Column Types for PREV

**Severity:** Medium

- Issue: `readColumnAsLongBits()` (line 399) converts column values to `long` bits for prev storage. This covers DOUBLE, FLOAT, INT, LONG, SHORT, BYTE, BOOLEAN, CHAR, IPv4, and geo types. It does NOT cover STRING, VARCHAR, SYMBOL, LONG128, LONG256, DECIMAL128, DECIMAL256, BINARY, or ARRAY types. When FILL(PREV) is applied to a query with these column types, `prevValue()` will return corrupted data.
- Files:
  - `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (lines 399-410: `readColumnAsLongBits`; lines 424-702: `FillRecord` methods)
- Impact: Currently no impact because FILL(PREV) does not reach this code path (see dead code concern above). When FILL(PREV) is enabled on the fast path, STRING/VARCHAR/SYMBOL columns in the SELECT list will produce incorrect fill values.
- Fix approach: For variable-length types (STRING, VARCHAR, BINARY), use a `RecordChain` to snapshot the entire prev row. For LONG256/DECIMAL types, use multiple `long` slots or dedicated storage. Consider whether FILL(PREV) for non-numeric types is even meaningful (the old path handles SYMBOL via map-based keyed cursors).

---

*Concerns audit: 2026-04-09*
