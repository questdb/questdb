---
created: 2026-04-22T00:00:00.000Z
completed_at: 2026-04-22
completed_in: 17-01-PLAN.md
title: Reject cross-column FILL(PREV) across TIMESTAMP and INTERVAL unit mismatches
area: sql
files:
  - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3605-3611
  - core/src/main/java/io/questdb/cairo/ColumnType.java:149-150 (TIMESTAMP_MICRO / TIMESTAMP_NANO share tag)
  - core/src/main/java/io/questdb/cairo/ColumnType.java:145-146 (INTERVAL_TIMESTAMP_MICRO / _NANO share tag)
  - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:1012-1022 (FillRecord.getInterval)
  - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:1197-1208 (FillRecord.getTimestamp)
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java (add rejection test)
---

## Problem

Cross-column `FILL(PREV(col_ref))` type-compatibility check in `SqlCodeGenerator.generateFill` (around line 3605) uses tag equality (`targetTag == sourceTag`) for every type outside DECIMAL / GEOHASH / ARRAY. `ColumnType.tagOf(t)` returns `t & 0xFF`, so `TIMESTAMP_MICRO` (tag 8) and `TIMESTAMP_NANO` ((1<<18) | 8, same low byte) pass the check as compatible. Same for `INTERVAL_TIMESTAMP_MICRO` and `INTERVAL_TIMESTAMP_NANO`.

When the target column is NANO and the source column is MICRO (or vice versa), `FillRecord.getTimestamp` / `getInterval` copy the raw long from `prevRecord.getXxx(sourceCol)` without unit conversion, producing silent 1000x drift in query output.

Same class of bug as the FILL_CONSTANT TIMESTAMP unit drift that was fixed earlier on this branch (commit `9df205bac5 Fix TIMESTAMP fill constant unit drift`).

**Reachability.** Requires a SELECT list that mixes MICRO and NANO timestamp or interval columns (e.g., a native `TIMESTAMP_NS` aggregate alongside a `literal::TIMESTAMP` column) plus a `FILL(PREV(col))` that cross-references one from the other. Narrow but realistic, and fails silently — no crash, just wrong values.

Found during `/review-pr` on PR #6946.

## Solution

Extend `needsExactTypeMatch` in `SqlCodeGenerator.generateFill` to include TIMESTAMP and INTERVAL tags:

```java
final boolean needsExactTypeMatch =
        ColumnType.isDecimal(targetType)
                || ColumnType.isGeoHash(targetType)
                || targetTag == ColumnType.ARRAY
                || targetTag == ColumnType.TIMESTAMP
                || targetTag == ColumnType.INTERVAL;
```

This forces full-type equality for TIMESTAMP and INTERVAL cross-column PREV, rejecting MICRO↔NANO mixes at codegen with a clear error message pointing at the offending token.

Add a rejection test to `SampleByFillTest`, modeled on `testFillPrevCrossColumnDecimalPrecisionMismatch`, covering both TIMESTAMP_MICRO↔TIMESTAMP_NANO and INTERVAL_TIMESTAMP_MICRO↔INTERVAL_TIMESTAMP_NANO cases.

Optional follow-up: consider whether a runtime unit conversion (multiply/divide by 1000) is ever desired instead of rejection — probably not, since cross-column PREV is a rare enough feature that strict rejection is preferable to silent conversion.
