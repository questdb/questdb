# Phase 4 Context: Cross-Column Prev

## Domain Boundary

Add FILL(PREV(col_name)) syntax that fills a column using a different column's previous bucket value. Parser and optimizer changes only — the fill cursor already supports cross-column prev dispatch.

## Decisions

### Syntax: FILL(PREV(col_name))

**Decision**: `PREV(col_name)` inside a FILL clause references a named output column alias. The fill value for the current column comes from the referenced column's previous bucket value.

Example:
```sql
SELECT ts, sum(val) AS s, avg(val) AS a 
FROM t SAMPLE BY 1h FILL(PREV(a), NULL) ALIGN TO CALENDAR
```
Here `s` fills from `a`'s previous bucket value. `a` fills with NULL.

### Reference resolution: output alias

**Decision**: `col_name` in PREV(col_name) references the output column alias from the SELECT list, not raw table column names. Resolution happens in `generateFill()` via `metadata.getColumnIndex(alias)`.

### Fill mode encoding: column index >= 0

**Decision**: `fillModes[col] >= 0` means cross-column prev referencing column at that index. This is ALREADY supported by `prevValue()` in the fill cursor:
```java
if (mode >= 0) return simplePrev[mode]; // cross-column reference
```
No fill cursor changes needed.

### Scope: keyed and non-keyed

**Decision**: Cross-column prev works for both keyed and non-keyed queries. For keyed queries, the per-key MapValue prev lookup uses `getPerKeyPrev(value, mode)` which already supports cross-column via the aggregate slot mapping.

### Implementation scope

**What needs to change:**
1. **Parser/SQL model**: Detect `PREV(col_name)` as a function-call expression in fill values. Currently `isPrevKeyword()` only matches bare `PREV`.
2. **generateFill()**: When fill expression is `PREV(col_name)`, resolve `col_name` to column index, set `fillModes[col] = column_index`.
3. **Optimizer gate**: The gate at SqlOptimiser.java:7880 was already removed in Phase 3. No change needed.
4. **Tests**: Cross-column prev for non-keyed and keyed queries.

**What stays the same:**
- SampleByFillRecordCursorFactory — zero changes
- prevValue() dispatch — already handles mode >= 0
- Per-key MapValue prev — already stores all aggregate prev values

## Canonical References

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — generateFill() fill expression parsing
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — prevValue() dispatch (already supports >= 0)
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — optimizer gate (already removed)

## Deferred Ideas

- PREV(expression) — reference an arbitrary expression, not just a column alias
- PREV with type coercion — filling a DOUBLE column from an INT column's prev
