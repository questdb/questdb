# Phase 7 Context: PREV Type-Safe Fast Path

## Domain Boundary

Make fast-path `SAMPLE BY ... FILL(PREV/prev(alias))` type-safe by adding per-column snapshot tracking, a source type support matrix, and legacy fallback for unsupported types. No new features — safety and parity guardrails only.

## Decisions

### Problem statement

1. `readColumnAsLongBits(... default -> getLong())` is unsafe for symbol/string/varchar/array-like outputs. These types cannot be round-tripped through a `long` value.
2. Snapshot currently runs for ALL aggregates when ANY `PREV` column exists (`savePrevValues` / `updatePerKeyPrev` iterate all columns). Mixed queries can fail even if only one column uses PREV.
3. Legacy cursor path was safe because aggregate functions kept native typed state in map values — no long-bit encoding needed.

### Per-column snapshot tracking (PTSF-01)

**Decision**: Track which columns actually need prev snapshot. In `generateFill()`, build a list of PREV source columns (both `FILL_PREV_SELF` and cross-column `mode >= 0`). The cursor only snapshots those columns, not all aggregates.

**Implementation**: Add an `IntList prevSourceCols` or a boolean mask `isPrevSource[col]`. `savePrevValues` / `updatePerKeyPrev` only iterate columns in this mask.

### Source type support matrix (PTSF-02)

**Decision**: Define which column types support fast-path PREV:

| Type | Fast-path PREV | Encoding |
|------|---------------|----------|
| DOUBLE | Yes | `Double.doubleToRawLongBits` |
| FLOAT | Yes | `Float.floatToRawIntBits` |
| INT | Yes | direct cast |
| LONG | Yes | direct |
| SHORT | Yes | direct cast |
| BYTE | Yes | direct cast |
| BOOLEAN | Yes | 0/1 |
| CHAR | Yes | direct cast |
| TIMESTAMP | Yes | direct |
| DATE | Yes | direct |
| IPv4 | Yes | direct cast |
| GEOBYTE/SHORT/INT/LONG | Yes | direct cast |
| DECIMAL8/16/32/64 | Yes | direct cast |
| SYMBOL | No — falls back to legacy |
| STRING | No — falls back to legacy |
| VARCHAR | No — falls back to legacy |
| LONG256 | No — falls back to legacy |
| DECIMAL128/256 | No — falls back to legacy |
| BINARY | No — falls back to legacy |
| ARRAY | No — falls back to legacy |
| UUID | No — falls back to legacy |

### Legacy fallback for unsupported types (PTSF-04)

**Decision**: If any PREV source column (either `FILL_PREV_SELF` or `prev(alias)` target) has an unsupported type, skip the fast-path rewrite entirely. The optimizer gate at `rewriteSampleBy()` should check the output column types against the support matrix. If unsupported, don't set `fillStride` → query falls through to the cursor-based path.

**Implementation**: In `SqlOptimiser.rewriteSampleBy()`, after detecting PREV in fill values, check each aggregate column's type. If any PREV-targeted column has an unsupported type, bail out of the rewrite (same as LINEAR bail-out).

### Mixed-fill safety (PTSF-03)

**Decision**: A query like `SELECT ts, sum(val), first(name) FROM t SAMPLE BY 1h FILL(PREV, NULL)` has PREV on `sum(val)` (DOUBLE — supported) and NULL on `first(name)` (STRING — not using PREV). This should stay on the fast path because the PREV column IS numeric. The type check only applies to columns that actually use PREV as fill mode.

### Scope exclusions

- Keep existing behavior for numeric PREV (no changes to encoding or dispatch)
- Do not implement LINEAR
- FILL(NULL) and literal FILL(value) unchanged
- Apply to both keyed and non-keyed

### Nanosecond parity (PTSF-06)

**Decision**: Mirror all new microsecond tests with nanosecond equivalents in `SampleByNanoTimestampTest` or equivalent.

## Prior Decisions (carried forward)

- FILL_KEY = -3 for key columns
- OrderedMap for keyed fill with per-key prev in MapValue slots
- fillModes[col] >= 0 for cross-column prev (PREV(alias))
- followedOrderByAdvice = false (outer sort handles ordering)

## Canonical References

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:7880` — optimizer gate (hasLinearFill, PREV detection)
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — generateFill() fill mode building
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — readColumnAsLongBits, savePrevValues, updatePerKeyPrev
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — existing 302 tests
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 27 custom tests

## Deferred Ideas

- PREV for string/symbol/varchar via RecordChain storage (future phase)
- LINEAR fill implementation
