# Deferred Items - Phase 7

## Pre-existing: testSampleByFillNeedFix fails with PREV on SYMBOL key column

**File:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:5947`

**Issue:** The query in `testSampleByFillNeedFix` uses `sample by 1h fill(PREV, ...)` on a table with a SYMBOL key column (`candle_symbol`). The query has no explicit `ALIGN TO CALENDAR`, but the default configuration uses calendar alignment, so the fast-path rewrite triggers. The optimizer gate does not detect the SYMBOL key column as a PREV target (correctly -- FILL_KEY columns are not PREV targets). However, after the SAMPLE BY rewrite, the `isKeyColumn()` check in `generateFill()` does not recognize `candle_symbol` as a key column in the rewritten model structure (CTE with WHERE filter), so it gets assigned `FILL_PREV_SELF` instead of `FILL_KEY`. The safety-net type check then correctly rejects it.

**Pre-existing:** This test was already failing before Phase 7 changes with `UnsupportedOperationException` from `readColumnAsLongBits` (the old `default -> record.getLong(col)` branch). Phase 7 changes the error to a more descriptive `SqlException`.

**Root cause:** `isKeyColumn()` in `generateFill()` relies on `model.getBottomUpColumns()` matching the `groupByFactory.getMetadata()` column order. For complex query structures (CTEs, WHERE on key columns), the model may be wrapped, and the `bottomUpCols` structure may not align with the factory metadata.

**Fix approach:** Improve `isKeyColumn()` to handle wrapped models, or look up column kind from the GROUP BY factory metadata directly (checking if a column is an aggregate function vs. a pass-through key).
