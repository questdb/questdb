# Phase 7: PREV Type-Safe Fast Path - Research

**Researched:** 2026-04-09
**Domain:** QuestDB fast-path fill cursor — type safety for PREV encoding
**Confidence:** HIGH

## Summary

The fast-path fill cursor (`SampleByFillRecordCursorFactory`) encodes all aggregate column values into `long` slots using `readColumnAsLongBits()`. The `default` branch of that switch calls `record.getLong(col)`, which throws `UnsupportedOperationException` for non-numeric types (SYMBOL, STRING, VARCHAR, BINARY, UUID, LONG256, ARRAY). The crash occurs in `savePrevValues` and `updatePerKeyPrev` because they iterate ALL aggregate columns whenever `hasPrevFill = true`, even if only one column uses PREV fill mode. A mixed query like `FILL(PREV, NULL)` where the DOUBLE column takes PREV and the SYMBOL column takes NULL still crashes, because `hasPrevFill = true` causes both columns to be snapshotted.

The fix has two independent parts. First, per-column snapshot tracking (PTSF-01): `savePrevValues` and `updatePerKeyPrev` should only read columns that are actually PREV sources (mode == FILL_PREV_SELF or mode >= 0), never KEY or CONSTANT columns. This makes mixed queries (PREV on numeric + NULL/CONSTANT on string) safe on the fast path. Second, an unsupported-type guard (PTSF-02, PTSF-04): when a PREV-source column has a type that cannot be round-tripped through a `long` (SYMBOL, STRING, VARCHAR, LONG256, BINARY, UUID, ARRAY, DECIMAL128, DECIMAL256), the optimizer must bail out of the GROUP BY rewrite, so the query falls through to the legacy cursor path.

**Primary recommendation:** Implement per-column snapshot tracking in `SampleByFillCursor` as the safety fix for mixed queries, and add an unsupported-type bail-out in `generateFill()` (not in `rewriteSampleBy()`) for PREV targets whose output column type is unsupported. The fallback from `generateFill()` must throw a `SqlException` (not silently skip fill), because by the time `generateFill()` runs, the GROUP BY factory is already compiled and there is no path back to the legacy cursor.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Per-column snapshot tracking (PTSF-01):** Add an `IntList prevSourceCols` or boolean mask `isPrevSource[col]`. `savePrevValues` / `updatePerKeyPrev` only iterate columns in this mask (columns where fillMode == FILL_PREV_SELF or fillMode >= 0).

**Source type support matrix (PTSF-02):**
- Supported (fast path): DOUBLE, FLOAT, INT, LONG, SHORT, BYTE, BOOLEAN, CHAR, TIMESTAMP, DATE, IPv4, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64
- Unsupported (legacy fallback): SYMBOL, STRING, VARCHAR, LONG256, BINARY, UUID, ARRAY, DECIMAL128, DECIMAL256

**Mixed-fill safety (PTSF-03):** Only PREV-mode columns are checked against the type matrix. A SYMBOL column with fill mode FILL_CONSTANT (NULL) does not trigger a fallback, even if `hasPrevFill = true` for other columns.

**Legacy fallback for unsupported types (PTSF-04):** If any PREV source column has an unsupported type, the optimizer gate in `rewriteSampleBy()` must not set `fillStride`, so the query uses the legacy cursor path.

**Nanosecond parity (PTSF-06):** Mirror all new microsecond tests with nanosecond equivalents in `SampleByNanoTimestampTest` or equivalent.

**Scope exclusions:**
- Keep existing behavior for numeric PREV (no changes to encoding or dispatch)
- Do not implement LINEAR
- FILL(NULL) and literal FILL(value) unchanged
- Apply to both keyed and non-keyed

### Claude's Discretion
- Exact field name for the prev-source mask (e.g., `prevSourceCols` as IntList, or boolean `isPrevSource[]`)
- Whether the mask is built in the constructor or passed as a constructor argument
- Exact error message text for the SqlException when `generateFill()` detects an unsupported PREV type

### Deferred Ideas (OUT OF SCOPE)
- PREV for string/symbol/varchar via RecordChain storage (future phase)
- LINEAR fill implementation
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PTSF-01 | Fast-path PREV snapshot only persists required source columns (not global save-all) | Verified: `savePrevValues` and `updatePerKeyPrev` iterate all columns — fix is to add a PREV-source mask |
| PTSF-02 | Explicit source type support matrix — numeric types on fast path, unsupported types fall back to legacy | Verified: `readColumnAsLongBits` default branch calls `getLong()` which throws `UnsupportedOperationException` for SYMBOL/STRING/VARCHAR/etc. |
| PTSF-03 | Mixed-fill queries (one PREV numeric + one non-PREV string/symbol) do not crash on fast path | Verified: crash occurs because `savePrevValues` iterates ALL columns when `hasPrevFill = true`; fix is PTSF-01 (only snapshot PREV-source columns) |
| PTSF-04 | prev(alias) referencing unsupported type triggers legacy path fallback | Verified: fallback requires optimizer gate since `generateFill()` runs after GROUP BY compilation — see architecture section |
| PTSF-05 | No behavior regressions for existing FILL(PREV) tests | Verified: all numeric PREV paths pass through the same switch cases; changes are additive |
| PTSF-06 | Nanosecond timestamp tests mirror microsecond equivalents | Verified: `SampleByNanoTimestampTest` exists; `testGeohashFillPrev` (line 531) shows the test pattern |
</phase_requirements>

---

## Standard Stack

No new libraries. All changes are in-tree Java.

| Class | File | Role |
|-------|------|------|
| `SampleByFillRecordCursorFactory` | `engine/groupby/SampleByFillRecordCursorFactory.java` | Unified fill cursor — contains `readColumnAsLongBits`, `savePrevValues`, `updatePerKeyPrev` |
| `SqlCodeGenerator` | `griffin/SqlCodeGenerator.java` | `generateFill()` — builds fill modes, calls factory constructor |
| `SqlOptimiser` | `griffin/SqlOptimiser.java` | `rewriteSampleBy()` — the optimizer gate that sets `fillStride` |
| `ColumnType` | `cairo/ColumnType.java` | Type constants (SYMBOL=12, STRING=11, VARCHAR=26, etc.) |
| `SampleByFillTest` | `test/...groupby/SampleByFillTest.java` | 27 existing fast-path fill tests |
| `SampleByNanoTimestampTest` | `test/...groupby/SampleByNanoTimestampTest.java` | Nanosecond equivalents |
| `SampleByTest` | `test/...groupby/SampleByTest.java` | 302 legacy cursor tests (regression baseline) |

---

## Architecture Patterns

### Where `readColumnAsLongBits` is called

`readColumnAsLongBits` is a `private static` method in `SampleByFillCursor` (the inner class). It is called from exactly two places: [VERIFIED: reading SampleByFillRecordCursorFactory.java lines 579, 602]

1. `savePrevValues(Record record)` — lines 575-581 — non-keyed path, called once per data row when `hasPrevFill = true` and `keysMap == null`. Iterates all columns 0..columnCount-1 skipping `timestampIndex`.

2. `updatePerKeyPrev(MapValue value, Record record)` — lines 596-605 — keyed path, called once per data row when `hasPrevFill = true` and `keysMap != null`. Iterates all columns whose `outputColToAggSlot[col] >= 0` (i.e., all non-timestamp, non-key columns).

Neither method filters by fill mode. Both call `readColumnAsLongBits(record, col, columnTypes[col])` on EVERY aggregate column, including those with `FILL_CONSTANT` (NULL) or `FILL_KEY` mode that are never actually PREV-used.

### `readColumnAsLongBits` switch — which types fall to `default`

[VERIFIED: reading SampleByFillRecordCursorFactory.java lines 583-594]

Explicit cases:
- `DOUBLE` (10) → `Double.doubleToRawLongBits`
- `FLOAT` (9) → `Float.floatToRawIntBits`
- `INT` (5), `IPv4` (22), `GEOINT` (16) → `record.getInt(col)`
- `SHORT` (4), `GEOSHORT` (15) → `record.getShort(col)`
- `BYTE` (3), `GEOBYTE` (14) → `record.getByte(col)`
- `BOOLEAN` (2) → `0` or `1`
- `CHAR` (1) → `record.getChar(col)`

`default → record.getLong(col)` fires for any type not listed above. This silently includes:
- `LONG` (6), `DATE` (7), `TIMESTAMP` (8) — safe, `getLong()` works for these
- `SYMBOL` (12), `STRING` (11), `VARCHAR` (26) — **unsafe**, `getLong()` throws `UnsupportedOperationException`
- `LONG256` (13), `BINARY` (18), `UUID` (19), `ARRAY` (27+) — **unsafe**
- `DECIMAL128` (32), `DECIMAL256` (33) — **unsafe**
- `GEOBYTE` (14), `GEOSHORT` (15), `GEOINT` (16), `GEOLONG` (17) — GEO types have `getGeoByte()` etc., but `readColumnAsLongBits` handles GEOBYTE/GEOSHORT/GEOINT via their `INT`/`SHORT`/`BYTE` explicit cases; `GEOLONG` falls to `default → getLong()` which happens to work since `getLong()` is defined for GEOLONG-backed records.

Note that DECIMAL8 (28), DECIMAL16 (29), DECIMAL32 (30), DECIMAL64 (31) — are not in the switch cases. DECIMAL16 and DECIMAL8 extend `short`/`byte` backed functions, DECIMAL32 extends `int`, DECIMAL64 extends `long`. Their `getLong()` may or may not be implemented depending on the concrete record type. These should be added as explicit cases in the switch.

### `hasPrevFill` flag — what triggers it

[VERIFIED: reading SqlCodeGenerator.java lines 3338-3406]

`hasPrevFill` is set to `true` in `generateFill()` when ANY column has `isPrevKeyword(fillExpr.token)` in its fill mode. This flag then controls whether `savePrevValues` / `updatePerKeyPrev` are called at all. The flag is coarse — it fires even if only one of ten columns is PREV.

The fix for PTSF-01 is to build a `prevSourceCols` list (IntList of output column indices that are PREV sources: `FILL_PREV_SELF` or `mode >= 0`) in `generateFill()`, pass it to the factory constructor, and have `savePrevValues`/`updatePerKeyPrev` iterate only those columns.

### `updatePerKeyPrev` — per-key slot layout

[VERIFIED: reading SampleByFillRecordCursorFactory.java lines 596-605 and 267-285]

The keyed map value layout is:
- Slot 0: `KEY_INDEX_SLOT` — long, key index
- Slot 1: `HAS_PREV_SLOT` — long, 0/1 for whether this key has a prev
- Slots 2..N: `PREV_START_SLOT + aggSlot` — one long per aggregate column

`outputColToAggSlot[col]` maps output column index → aggregate slot index. Non-key, non-timestamp columns get sequential `aggSlot` values (0-based). The total `aggColumnCount` = number of such columns. For per-column snapshot tracking, only the aggregate slots corresponding to PREV-source columns need be written/read; the rest can remain 0 (they're never read back since only PREV-mode columns use `prevValue(col)`).

There is no functional need to change the map value layout — we just need to skip the `putLong` call for non-PREV-source columns in `updatePerKeyPrev`.

### Optimizer gate — where PREV detection happens

[VERIFIED: reading SqlOptimiser.java lines 7884-7892, 8162-8163]

`rewriteSampleBy()` is the gate. It does not set `fillStride` if:
- `hasLinearFill(sampleByFill)` is true (lines 7889)
- Various other conditions (no timestamp, no offset, etc.)

Setting `fillStride` at line 8162 (`nested.setFillStride(sampleBy)`) is what triggers `generateFill()` later. If `fillStride` is never set, `generateFill()` returns `groupByFactory` immediately (line 3234).

The LINEAR bail-out pattern: detect in `hasLinearFill`, skip the entire rewrite. For PREV + unsupported types, the same pattern applies — add a new `hasPrevWithUnsupportedType(...)` helper to `rewriteSampleBy()` and if it returns true, return early without setting `fillStride`.

### Why the type check cannot be in `generateFill()` alone

`generateFill()` receives an already-compiled `groupByFactory`. Its metadata has full column types. BUT: `generateFill()` can only return `groupByFactory` unchanged (no fill) or return a `SampleByFillRecordCursorFactory` wrapping it. There is no path from `generateFill()` back to the legacy `SampleByFillPrevRecordCursorFactory` — that factory is built by a completely different code path (`generateGroupBy` with `groupByFunctions`, etc.). If `generateFill()` returns `groupByFactory` unchanged when PREV is present, the query produces results without fill — which is wrong (different output from legacy).

**Therefore, for PTSF-04 (true legacy fallback), the guard must be in `rewriteSampleBy()`.** For PTSF-03 (mixed-fill crash fix), the guard is in `savePrevValues`/`updatePerKeyPrev` (PTSF-01 per-column tracking).

### How to infer aggregate output types in `rewriteSampleBy()`

[VERIFIED: reading SqlOptimiser.java line 475 — precedent for column type lookup]

`rewriteSampleBy()` runs after `enumerateTableColumns()` (line 11140 < 11145). The nested model has its table columns enumerated in `model.getAliasToColumnMap()`. For a query column whose AST is a FUNCTION (aggregate), the return type equals the argument type for first-order aggregates like `first()`, `last()`, `min()`, `max()`. The argument is a LITERAL column reference.

Pattern (already used in SqlOptimiser.java line 474-475):
```java
QueryColumn argCol = nested.getAliasToColumnMap().get(funcArgNode.token);
if (argCol != null) {
    int argType = ColumnType.tagOf(argCol.getColumnType());
    // check argType against unsupported set
}
```

This covers the common cases: `first(str_col)`, `last(sym_col)`, `min(varchar_col)`. For complex expressions (expressions as arguments rather than plain column references), the type cannot be resolved at optimizer time — conservatively assume unknown (allowed to stay on fast path or bail out). The risk of false bail-outs for complex expressions is acceptable since unsupported types in aggregates are rare.

For the fill mode check: iterate `sampleByFill` to find PREV expressions, map them to columns by position (skipping key columns), then look up the corresponding aggregate function's argument type.

### Where `fillStride` absence causes legacy path

When `fillStride` is null, `generateFill()` returns `groupByFactory` at line 3234. But the question is: if the optimizer bailed out of `rewriteSampleBy()`, does the query reach `generateFill()` at all? No — without the rewrite, the query model remains a SAMPLE BY, and the legacy `generateSampleBy()` path in `SqlCodeGenerator` handles it, creating `SampleByFillPrevRecordCursorFactory` via the existing production code. The query plan will show `Sample By ... fill: prev` (as in `SampleByTest` line 5724).

### `prevValue(col)` — the read-back path

[VERIFIED: reading SampleByFillRecordCursorFactory.java lines 547-573]

`prevValue(col)` reads from:
- Keyed: `value.getLong(PREV_START_SLOT + aggSlot)` — reads the stored long bits
- Non-keyed: `simplePrev[col]` — reads the stored long bits

This is only called from `FillRecord.get*()` methods when `mode == FILL_PREV_SELF || mode >= 0` AND `hasKeyPrev()`. The crash is in the WRITE path (`savePrevValues`, `updatePerKeyPrev`), not the read path. The read path already handles nulls via `hasKeyPrev()`.

### `FillRecord.getStrA/getSymA/getVarcharA` — already correct for non-PREV mode

[VERIFIED: reading SampleByFillRecordCursorFactory.java lines 866-935]

For gap-fill rows, `getStrA(col)`, `getSymA(col)`, `getVarcharA(col)` check fill mode and return:
- `FILL_KEY`: from map record (correct)
- `FILL_CONSTANT`: from constant function (correct)
- Otherwise: return `null` (correct for NULL fill)

None of them call `prevValue()` for string/symbol types because those types are not in the supported PREV set. After PTSF-01 fixes the snapshot (write) side, the read side is already correct.

### Test locations and patterns

**`SampleByFillTest`** (27 tests) [VERIFIED: reading file]
- Uses `assertSql()` (plain data assertion)
- Tests are inside `assertMemoryLeak()` lambdas
- DDL with `execute()`, queries with `assertSql()`
- All tests use microsecond timestamps

**`SampleByNanoTimestampTest`** [VERIFIED: reading file]
- Has `FROM_TO_DDL` at line 73 — pre-built table creation with nanosecond timestamps including STRING, SYMBOL, VARCHAR, BOOLEAN, CHAR columns
- `testGeohashFillPrev` (line 531) — example of keyed FILL(PREV) test with SYMBOL key
- Tests use `assertQuery()` with `"k", false` (timestamp col name, not strictly ordered)
- Pattern for parity tests: copy microsecond test, replace `timestamp_sequence(...)` with `timestamp_sequence_ns(...)`, replace `TIMESTAMP` type with `TIMESTAMP_NS`

**`SampleByTest`** (302 tests) — legacy cursor baseline. Any PREV test involving SYMBOL/STRING key columns currently uses the legacy path (plan shows `Sample By`). After Phase 7, if those queries have PREV on an unsupported aggregate type, they should STILL use the legacy path (no regression).

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead |
|---------|-------------|-------------|
| Type tag extraction | `switch (colType)` on raw int | `ColumnType.tagOf(colType)` — strips dimension/flag bits |
| Checking if type is array | `colType >= ARRAY` | `ColumnType.isArray(colType)` |
| Column type lookup in model | Manual iteration of fields | `model.getAliasToColumnMap().get(name).getColumnType()` |
| ObjList cleanup | Manual null/close loop | `Misc.freeObjList()` |

---

## Common Pitfalls

### Pitfall 1: `ColumnType.tagOf()` vs raw type value
**What goes wrong:** ARRAY types encode element type and dimensions in the upper bits. `colType == ColumnType.ARRAY` is false for `DOUBLE[]`. Use `ColumnType.tagOf(colType) == ColumnType.ARRAY` or `ColumnType.isArray(colType)`.
**How to avoid:** Always call `tagOf()` before comparing to named constants in a switch or equality check.

### Pitfall 2: GEOLONG falls to `default → getLong()` — silently correct
**What goes wrong:** `readColumnAsLongBits` has explicit cases for GEOBYTE, GEOSHORT, GEOINT but NOT for GEOLONG. GEOLONG falls to `default → record.getLong(col)`. This happens to be correct because GEOLONG-backed records implement `getLong()`. But it's fragile — if the GEOLONG record type changes, it silently breaks.
**How to avoid:** Add explicit `case ColumnType.GEOLONG ->` to the switch to call `record.getGeoLong(col)`, for clarity and future safety.

### Pitfall 3: DECIMAL types — missing from `readColumnAsLongBits`
**What goes wrong:** DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64 (28-31) fall to `default → record.getLong(col)`. Whether this works depends on the concrete record's implementation of `getLong()`. The CONTEXT.md marks them as supported (yes, direct cast), but the switch doesn't have explicit cases.
**How to avoid:** Add explicit cases for `DECIMAL8`, `DECIMAL16`, `DECIMAL32`, `DECIMAL64` matching the accessor methods (`getDecimal8`, `getDecimal16`, `getDecimal32`, `getDecimal64`).

### Pitfall 4: `hasPrevFill` flag is coarse — crash on mixed queries
**What goes wrong:** If ANY column is PREV, `hasPrevFill = true` causes `savePrevValues` to snapshot ALL columns, including SYMBOL/STRING ones that are not PREV targets.
**How to avoid:** PTSF-01 fix — pass `prevSourceCols` mask to cursor, iterate only those columns.

### Pitfall 5: Optimizer type check only covers simple LITERAL arguments
**What goes wrong:** `first(name || '_suffix')` — the argument to `first()` is an expression, not a LITERAL. The optimizer can't resolve its type without parsing. If such a query has FILL(PREV) on `first(expr_returning_string)`, the optimizer won't detect the unsupported type and the query will reach `generateFill()`.
**How to avoid:** In `generateFill()`, add a secondary type check against `groupByFactory.getMetadata()` as a safety net. If an unsupported PREV type is detected there, throw a clear `SqlException` with a message like "FILL(PREV) is not supported for column type STRING — use a numeric aggregate". This is not a fallback to legacy (impossible at that stage), but it prevents the silent `UnsupportedOperationException` crash.

### Pitfall 6: MapValue slot layout — `aggColumnCount` must match `prevSourceCols`
**What goes wrong:** If `mapValueTypes` is sized by `aggColumnCount` (all non-key, non-timestamp columns) but per-column tracking only writes to PREV-source slots, the unused slots waste map memory but don't cause correctness issues. However if the slot count is reduced to only PREV-source columns, `outputColToAggSlot[]` mapping must be rebuilt to only count PREV-source aggregates.
**How to avoid:** Keep the current slot layout (one slot per aggregate column, regardless of fill mode). Only skip the `readColumnAsLongBits` call for non-PREV columns. This is the minimal diff and avoids restructuring the map value layout.

### Pitfall 7: `FILL_KEY` columns must not be snapshotted
**What goes wrong:** Key columns (FILL_KEY mode) have `outputColToAggSlot[col] == -1`, so they are already skipped by the `aggSlot >= 0` check in `updatePerKeyPrev`. The per-column tracking must also skip them.
**How to avoid:** Build `prevSourceCols` from columns where `fillModes.getQuick(col) == FILL_PREV_SELF || fillModes.getQuick(col) >= 0` (these are the only two PREV modes). Key columns have FILL_KEY (-3), which is excluded.

---

## Code Examples

### Current `readColumnAsLongBits` (the crash site)
```java
// Source: SampleByFillRecordCursorFactory.java line 583
private static long readColumnAsLongBits(Record record, int col, short type) {
    return switch (type) {
        case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
        case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
        case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> record.getInt(col);
        case ColumnType.SHORT, ColumnType.GEOSHORT -> record.getShort(col);
        case ColumnType.BYTE, ColumnType.GEOBYTE -> record.getByte(col);
        case ColumnType.BOOLEAN -> record.getBool(col) ? 1 : 0;
        case ColumnType.CHAR -> record.getChar(col);
        default -> record.getLong(col);  // ← UnsupportedOperationException for SYMBOL/STRING/VARCHAR
    };
}
```

Extended switch to add missing explicit cases:
```java
private static long readColumnAsLongBits(Record record, int col, short type) {
    return switch (type) {
        case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
        case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
        case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> record.getInt(col);
        case ColumnType.SHORT, ColumnType.GEOSHORT -> record.getShort(col);
        case ColumnType.BYTE, ColumnType.GEOBYTE -> record.getByte(col);
        case ColumnType.BOOLEAN -> record.getBool(col) ? 1 : 0;
        case ColumnType.CHAR -> record.getChar(col);
        case ColumnType.GEOLONG -> record.getGeoLong(col);
        case ColumnType.DECIMAL8 -> record.getDecimal8(col);
        case ColumnType.DECIMAL16 -> record.getDecimal16(col);
        case ColumnType.DECIMAL32 -> record.getDecimal32(col);
        case ColumnType.DECIMAL64 -> record.getDecimal64(col);
        default -> record.getLong(col);  // LONG, DATE, TIMESTAMP — these implement getLong()
    };
}
```

### Current `savePrevValues` — iterates all columns (the problem)
```java
// Source: SampleByFillRecordCursorFactory.java line 575
private void savePrevValues(Record record) {
    hasSimplePrev = true;
    for (int i = 0; i < columnCount; i++) {
        if (i == timestampIndex) continue;
        simplePrev[i] = readColumnAsLongBits(record, i, columnTypes[i]); // crashes if SYMBOL
    }
}
```

Fixed version (iterate only PREV-source columns):
```java
private void savePrevValues(Record record) {
    hasSimplePrev = true;
    for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
        int col = prevSourceCols.getQuick(i);
        simplePrev[col] = readColumnAsLongBits(record, col, columnTypes[col]);
    }
}
```

### Current `updatePerKeyPrev` — iterates all aggregate columns (the problem)
```java
// Source: SampleByFillRecordCursorFactory.java line 596
private void updatePerKeyPrev(MapValue value, Record record) {
    value.putLong(HAS_PREV_SLOT, 1L);
    for (int col = 0; col < columnCount; col++) {
        int aggSlot = outputColToAggSlot[col];
        if (aggSlot >= 0) {
            value.putLong(PREV_START_SLOT + aggSlot,
                    readColumnAsLongBits(record, col, columnTypes[col])); // crashes if SYMBOL
        }
    }
}
```

Fixed version (iterate only PREV-source columns):
```java
private void updatePerKeyPrev(MapValue value, Record record) {
    value.putLong(HAS_PREV_SLOT, 1L);
    for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
        int col = prevSourceCols.getQuick(i);
        int aggSlot = outputColToAggSlot[col];
        if (aggSlot >= 0) {
            value.putLong(PREV_START_SLOT + aggSlot,
                    readColumnAsLongBits(record, col, columnTypes[col]));
        }
    }
}
```

### Building `prevSourceCols` in `generateFill()`
```java
// In generateFill(), after building fillModes:
final IntList prevSourceCols = new IntList();
for (int col = 0; col < columnCount; col++) {
    int mode = fillModes.getQuick(col);
    if (mode == SampleByFillRecordCursorFactory.FILL_PREV_SELF || mode >= 0) {
        prevSourceCols.add(col);
    }
}
```

### Type support matrix check in `generateFill()`
```java
// Safety net in generateFill() — checks metadata types after GROUP BY compilation
for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
    int col = prevSourceCols.getQuick(i);
    int sourceCol = fillModes.getQuick(col) >= 0 ? fillModes.getQuick(col) : col;
    short tag = ColumnType.tagOf(groupByMetadata.getColumnType(sourceCol));
    if (!isFastPathPrevSupportedType(tag)) {
        throw SqlException.$(0, "FILL(PREV) is not supported for column '")
                .put(groupByMetadata.getColumnName(sourceCol))
                .put("' of type ")
                .put(ColumnType.nameOf(tag))
                .put(" — only numeric types support PREV fill on the fast path");
    }
}

private static boolean isFastPathPrevSupportedType(short tag) {
    return switch (tag) {
        case ColumnType.DOUBLE, ColumnType.FLOAT,
             ColumnType.INT, ColumnType.LONG, ColumnType.SHORT, ColumnType.BYTE,
             ColumnType.BOOLEAN, ColumnType.CHAR,
             ColumnType.TIMESTAMP, ColumnType.DATE,
             ColumnType.IPv4,
             ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG,
             ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 -> true;
        default -> false;
    };
}
```

### Optimizer type check in `rewriteSampleBy()` (for simple column references)
```java
// Pattern from SqlOptimiser.java line 474-475 — look up arg column type from nested model
// Call this before setFillStride() to bail if any PREV aggregate references an unsupported source type
private boolean hasPrevWithUnsupportedOutputType(
        QueryModel outerModel,
        QueryModel nested,
        ObjList<ExpressionNode> fillValues
) {
    // Look for PREV fill values and map them to output columns
    // For each PREV-targeted output column, check the argument type of the aggregate function
    // Returns true if ANY PREV target has unsupported type → bail out of rewrite
    ...
}
```

Note: this optimizer-side check is best-effort. It works for simple `first(col)` cases where the argument is a LITERAL. For expressions, skip (assume safe). The `generateFill()` safety net handles remaining cases with a SqlException.

### Nanosecond test pattern
```java
// Based on SampleByNanoTimestampTest.java line 531 (testGeohashFillPrev)
@Test
public void testFillPrevNumericNano() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (" +
                "SELECT x::DOUBLE val, timestamp_sequence_ns(" +
                "cast('2024-01-01T00:00:00' AS TIMESTAMP_NS), 3_600_000_000_000) ts " +
                "FROM long_sequence(3)) TIMESTAMP(ts)");
        assertSql(
                "sum\tts\n" +
                "1.0\t2024-01-01T00:00:00.000000000Z\n" +
                "1.0\t2024-01-01T01:00:00.000000000Z\n" +  // gap-filled with prev
                "2.0\t2024-01-01T02:00:00.000000000Z\n",
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
        );
    });
}
```

---

## Architecture Decision: Optimizer vs Code Generator

**Confirmed finding**: The CONTEXT.md instruction "check in `rewriteSampleBy()`" is architecturally correct for achieving true legacy fallback (query plan shows `Sample By`). Column types for aggregate OUTPUTS are not known at optimizer time in general, but for the common pattern (plain column reference as function argument), they CAN be inferred by looking up the argument column type from the nested model's alias map.

**Two-layer defense:**
1. **Optimizer layer** (`rewriteSampleBy()`): best-effort check — catches common cases like `first(str_col)` where the argument is a LITERAL. If unsupported type detected: don't set `fillStride` → legacy path.
2. **Code generator layer** (`generateFill()`): safety net — `groupByFactory.getMetadata()` has the exact output type. If unsupported type detected after GROUP BY compilation: throw `SqlException` with descriptive message.

Layer 2 handles cases Layer 1 misses (e.g., complex expressions, computed columns), converting silent crashes into user-visible errors.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `GEOLONG` falls to `default → getLong()` in `readColumnAsLongBits` and this works because GEOLONG records implement `getLong()` | Code Examples | Low — if wrong, GEOLONG PREV is already broken before this phase |
| A2 | DECIMAL8/16/32/64 types implement their respective `getDecimal8()`/etc. accessors on the concrete record type returned by GROUP BY | Code Examples | Low — these are first-class types with dedicated accessor methods |
| A3 | The optimizer's `nested.getAliasToColumnMap()` contains table column types after `enumerateTableColumns()` runs | Architecture | Medium — if the alias map isn't populated with types for computed columns, the optimizer check will miss some cases (Layer 2 fallback still catches them) |

---

## Open Questions

1. **GEOLONG in `readColumnAsLongBits` default branch**
   - What we know: GEOLONG falls to `default → record.getLong(col)`. The CONTEXT.md marks GEO types as supported.
   - What's unclear: Whether `getLong()` is implemented for all concrete record types that can back a GEOLONG-type aggregate output column.
   - Recommendation: Add explicit `case ColumnType.GEOLONG -> record.getGeoLong(col)` to be safe; the planner should include this in the switch case additions task.

2. **Optimizer check — `PREV(alias)` cross-column references**
   - What we know: `PREV(alias)` fill mode (mode >= 0) references a different column. The source column index is resolved in `generateFill()` at line 3395-3400.
   - What's unclear: How to detect unsupported cross-column PREV types in `rewriteSampleBy()` before the alias is resolved to a column index.
   - Recommendation: Skip the optimizer check for `PREV(alias)` expressions (where `fillExpr.type == FUNCTION && fillExpr.paramCount == 1`). The `generateFill()` safety net catches these.

3. **`simplePrev[]` array size after PTSF-01**
   - What we know: `simplePrev = new long[columnCount]` — indexed by output column index.
   - What's unclear: After switching to iterate only `prevSourceCols`, could `simplePrev` be downsized to `prevSourceCols.size()` slots (indexed by position in `prevSourceCols`)?
   - Recommendation: Keep `simplePrev[columnCount]` indexed by column index for simplicity. Downsizing requires changing `prevValue()` lookup logic and is not needed for correctness.

---

## Environment Availability

Step 2.6: SKIPPED (no external dependencies identified — pure Java source changes).

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 4 via Maven Surefire |
| Config file | `pom.xml` in `core/` module |
| Quick run command | `mvn -Dtest=SampleByFillTest test` |
| Full suite command | `mvn -Dtest=SampleByTest,SampleByFillTest,SampleByNanoTimestampTest test` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PTSF-01 | Only PREV-source columns snapshotted | unit (hidden by integration) | `mvn -Dtest=SampleByFillTest#testFillPrevMixed test` | ❌ Wave 0 |
| PTSF-02 | Numeric PREV types work on fast path | unit | `mvn -Dtest=SampleByFillTest test` | ✅ (existing) |
| PTSF-03 | Mixed PREV+NULL query does not crash | unit | `mvn -Dtest=SampleByFillTest#testFillPrevMixedWithSymbol test` | ❌ Wave 0 |
| PTSF-04 | PREV on STRING/SYMBOL falls back to legacy | unit | `mvn -Dtest=SampleByFillTest#testFillPrevSymbolLegacyFallback test` | ❌ Wave 0 |
| PTSF-05 | No regressions in existing FILL(PREV) tests | regression | `mvn -Dtest=SampleByTest,SampleByFillTest test` | ✅ |
| PTSF-06 | Nanosecond parity for PREV tests | unit | `mvn -Dtest=SampleByNanoTimestampTest test` | ✅ (partial) |

### Sampling Rate
- **Per task commit:** `mvn -Dtest=SampleByFillTest test`
- **Per wave merge:** `mvn -Dtest=SampleByTest,SampleByFillTest,SampleByNanoTimestampTest test`
- **Phase gate:** Full suite above green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `testFillPrevMixedWithSymbol` in `SampleByFillTest` — covers PTSF-03 (mixed PREV+NULL crash)
- [ ] `testFillPrevSymbolLegacyFallback` in `SampleByFillTest` — covers PTSF-04 (plan shows `Sample By`)
- [ ] `testFillPrevNanosecondParity` in `SampleByNanoTimestampTest` — covers PTSF-06

---

## Sources

### Primary (HIGH confidence)
- `[VERIFIED: reading SampleByFillRecordCursorFactory.java]` — direct code inspection of `readColumnAsLongBits`, `savePrevValues`, `updatePerKeyPrev`, `prevValue`, fill mode constants
- `[VERIFIED: reading SqlCodeGenerator.java lines 3225-3536]` — `generateFill()` full implementation, `prevSourceCols` build point, `hasPrevFill` flag
- `[VERIFIED: reading SqlOptimiser.java lines 7851-8170]` — `rewriteSampleBy()` full implementation, LINEAR bail-out pattern, `fillStride` set point
- `[VERIFIED: reading ColumnType.java lines 83-134]` — all type constants with numeric values
- `[VERIFIED: reading Record.java lines 302-303]` — `getLong()` default throws `UnsupportedOperationException`
- `[VERIFIED: reading SampleByFillTest.java]` — 27 tests, all microsecond, test patterns
- `[VERIFIED: reading SampleByNanoTimestampTest.java lines 531-563]` — `testGeohashFillPrev` test pattern for nanosecond PREV

### Secondary (MEDIUM confidence)
- `[ASSUMED]` Legacy path safety: the legacy `SampleByFillPrevRecordCursor` uses `GroupByFunction` typed state, never calls `getLong()` on non-numeric columns

---

## Metadata

**Confidence breakdown:**
- Crash mechanism: HIGH — directly observed in code
- Fix approach (PTSF-01): HIGH — straightforward mask filtering
- Optimizer gate (PTSF-04): MEDIUM — the mechanism is clear, but the exact detection of aggregate output types at optimizer time is limited to simple LITERAL arguments
- Test locations: HIGH — files verified, patterns observed

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable codebase, 30 day window)
