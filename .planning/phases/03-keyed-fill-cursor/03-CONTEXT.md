# Phase 3 Context: Keyed Fill Cursor

## Domain Boundary

Keyed SAMPLE BY FILL on the GROUP BY fast path. The fill cursor emits the cartesian product of all unique keys × all time buckets, with per-key prev tracking. Covers FILL(NULL), FILL(VALUE), and FILL(PREV) for keyed queries.

## Decisions

### Key column identification: FILL_KEY mode

**Decision**: Add `FILL_KEY = -3` to fillModes. Key columns get `FILL_KEY`. The FillRecord reads key columns from the keysMap cursor during gap filling, not from fill logic.

**Rationale**: Simplest approach — no new constructor parameter. The fill mode array already has one entry per column. Key columns are distinguished from aggregates by their fill mode.

**Implementation**: In `generateFill()`, detect key columns and assign `FILL_KEY` mode. Key columns are those in the GROUP BY key list that are NOT the timestamp floor function.

### Key discovery: OrderedMap with two-pass streaming

**Decision**: Use `MapFactory.createOrderedMap(config, keyTypes, valueTypes)` for key storage. OrderedMap maintains insertion order, guaranteeing KEY-04 (stable key order within each bucket).

**Pass 1**: Iterate sorted base cursor. For each row, insert key columns into the OrderedMap. New keys get a sequential index in the MapValue. Call `baseCursor.toTop()`.

**Pass 2**: Iterate again. For each bucket:
1. Read all data rows at this timestamp (consecutive in sorted order)
2. For each data row: find key in keysMap, mark as present, emit data row, update per-key prev
3. After all data rows: iterate keysMap cursor. For each key NOT marked present, emit fill row
4. Reset presence tracking, advance bucket

**Memory**: O(unique keys × columns). No row buffering.

### Per-key prev: MapValue slots

**Decision**: Store per-key previous aggregate values in the OrderedMap's value area. Each key's MapValue has one `long` slot per aggregate column (using `readColumnAsLongBits` to encode any numeric type as a long).

**Value layout**: `[keyIndex: long, hasPrev: long, prevCol0: long, prevCol1: long, ...]`

When a data row arrives for a key, update its MapValue with current aggregate values. When emitting a fill row for a missing key with FILL(PREV), read prev from MapValue.

### Fill row key column delegation

**Decision**: The FillRecord has two modes:
- **Data mode** (`isGapFilling = false`): all columns delegate to `baseRecord` (the sorted GROUP BY output)
- **Fill mode** (`isGapFilling = true`): 
  - FILL_KEY columns → read from keysMap cursor record (MapRecord)
  - Timestamp → fillTimestampFunc
  - Aggregates → fill mode dispatch (prev from MapValue / null / constant)

The keysMap cursor is positioned at the correct key during fill row emission.

### Presence tracking per bucket

**Decision**: Use a `boolean[]` array sized to `keyCount`, reset per bucket. When a data row arrives, look up its key index in the keysMap and set `keyPresent[keyIndex] = true`. After all data rows for the bucket, iterate keys and emit fill rows for `!keyPresent[i]`.

### Optimizer gate changes

**Decision**: Remove `!isPrevKeyword(...)` from line 7880 to allow FILL(PREV) through the fast path. Also remove the keyed bail-out at line 8001 (or modify it to only bail for LINEAR). Remove `guardAgainstFillWithKeyedGroupBy` restriction.

### Non-keyed as special case

**Decision**: When there are zero key columns, the keysMap has one entry (empty key). The cartesian product degenerates to one row per bucket — equivalent to Phase 2's non-keyed behavior. No separate code path needed.

## Prior Decisions (from Phase 2)

- DST not an issue for sub-day strides
- `followedOrderByAdvice=true` with inner sort
- Non-numeric PREV deferred (STRING, VARCHAR, SYMBOL, LONG256)

## Canonical References

- `core/src/main/java/io/questdb/cairo/map/OrderedMap.java` — insertion-order Map
- `core/src/main/java/io/questdb/cairo/map/MapFactory.java:39` — `createOrderedMap`
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — fill cursor
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — `generateFill()`
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:7880` — optimizer gate
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` — old cursor path (reference for keyed prev behavior)

## Deferred Ideas

- Non-numeric PREV (STRING, VARCHAR, SYMBOL, LONG256) — needs RecordChain or per-column object storage
- FILL(LINEAR) look-ahead with deferred emission
- Keyed fill with FROM/TO range (complex interaction)
- Day+ stride fill (Month, Year, Week)
