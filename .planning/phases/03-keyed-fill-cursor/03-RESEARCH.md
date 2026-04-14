# Phase 3: Keyed Fill Cursor - Research

**Researched:** 2026-04-09
**Domain:** QuestDB GROUP BY fast-path fill cursor, Map-based key discovery, two-pass streaming, per-key prev
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **FILL_KEY = -3 mode**: Key columns get `FILL_KEY` in the fillModes array. Timestamp gets `FILL_CONSTANT`. Aggregates get `FILL_PREV_SELF` / `FILL_CONSTANT`.
- **OrderedMap for key discovery**: `MapFactory.createOrderedMap(config, keyTypes, valueTypes)` — insertion-order guarantees stable iteration (KEY-04).
- **Two-pass streaming**: Pass 1 iterates base cursor, inserts key tuples into keysMap. `baseCursor.toTop()`. Pass 2 streams with fill and cartesian product emission.
- **Per-key prev in MapValue slots**: Value layout `[keyIndex: long, hasPrev: long, prevCol0: long, prevCol1: long, ...]`. Aggregate values encoded as raw long bits.
- **FillRecord dual mode**: `isGapFilling=false` delegates all to `baseRecord`. `isGapFilling=true`: FILL_KEY reads from keysMap MapRecord, timestamp from `fillTimestampFunc`, aggregates from fill dispatch.
- **boolean[] presence tracking**: Sized to `keyCount`, reset per bucket. `keyPresent[keyIndex] = true` on data row. After all data rows, emit fill for `!keyPresent[i]`.
- **Optimizer gate changes**: Remove `!isPrevKeyword(...)` from line 7880 in `SqlOptimiser.rewriteSampleBy()`. Remove keyed bail-out at line 8001. Remove (or bypass) `guardAgainstFillWithKeyedGroupBy` in `SqlCodeGenerator`.
- **Non-keyed as degenerate case**: Zero key columns → keysMap has one entry (empty key). Degenerates to one row per bucket (Phase 2 semantics). One code path.

### Claude's Discretion
- Exact MapValue slot layout (as long as the design fits `[keyIndex, hasPrev, prevCols...]`)
- Whether `generateFill` receives `keyColumnCount` as a new parameter or derives it from the model
- How to build the `keyColumnFilter` (ListColumnFilter vs selecting by position)
- Whether presence tracking uses `boolean[]` or `BitSet` — `boolean[]` is simpler and zero-GC

### Deferred Ideas (OUT OF SCOPE)
- Non-numeric PREV (STRING, VARCHAR, SYMBOL, LONG256) — needs RecordChain or per-column object storage
- FILL(LINEAR) look-ahead with deferred emission
- Keyed fill with FROM/TO range
- Day+ stride fill (Month, Year, Week)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| KEY-01 | Keyed fill emits the cartesian product of all unique keys x all buckets | Two-pass streaming: pass 1 discovers keys, pass 2 emits with fill for absent (key, bucket) pairs |
| KEY-02 | Missing (key, bucket) pairs filled according to fill mode (null/prev/value) | FILL_KEY delegates to MapRecord; FILL_PREV_SELF reads from MapValue per-key prev slots |
| KEY-03 | Per-key prev tracking — each key independently carries forward its own previous values | MapValue slots `[keyIndex, hasPrev, prevCol0...]` store per-key raw long bits |
| KEY-04 | Key order within a bucket is stable and consistent across all buckets | OrderedMap preserves insertion order; pass 2 iterates keysMap cursor in same order every bucket |
| KEY-05 | Key column values in fill rows match actual values discovered during pass 1 | FillRecord reads key columns from keysMap MapRecord (positioned at the correct key) |
</phase_requirements>

---

## Summary

Phase 3 extends `SampleByFillRecordCursorFactory` / `SampleByFillCursor` with an `OrderedMap`-based key discovery mechanism and cartesian product emission. The implementation follows a two-pass streaming design: pass 1 iterates the sorted GROUP BY output to collect unique key combinations into the OrderedMap, assigns each key a sequential index in the MapValue, then calls `baseCursor.toTop()`. Pass 2 streams the same sorted output again; for each time bucket the cursor (a) emits present data rows while marking presence, (b) emits fill rows for absent keys by iterating the keysMap cursor.

SYMBOL key columns stored in the keysMap need `setSymbolTableResolver()` called on the `MapRecord` during `of()` to enable string resolution via the base cursor's symbol table. The `guardAgainstFillWithKeyedGroupBy` guard in `SqlCodeGenerator` and the keyed bail-out in `SqlOptimiser.rewriteSampleBy()` must be removed so keyed queries reach the fast path.

**Primary recommendation**: Add `FILL_KEY = -3` constant, modify `generateFill()` to accept `keyColumnCount`, assign `FILL_KEY` to non-timestamp key columns, build the keysMap in `SampleByFillCursor.of()`, and implement the two-pass pass-2 bucket loop.

---

## Standard Stack

### Core
| Class | Location | Purpose |
|-------|----------|---------|
| `OrderedMap` | `cairo/map/OrderedMap.java` | Insertion-order hash map for key discovery |
| `MapFactory.createOrderedMap` | `cairo/map/MapFactory.java:57` | Two-arg overload with `keyTypes` + `valueTypes` |
| `ArrayColumnTypes` | `cairo/ArrayColumnTypes.java` | Builder for keyTypes and valueTypes passed to MapFactory |
| `RecordSink` | `cairo/RecordSink.java` | Copies key columns from Record into MapKey via `copy(record, mapKey)` |
| `RecordSinkFactory.getInstance` | `cairo/RecordSinkFactory.java` | Bytecode-generates or loops over selected columns |
| `MapKey` | `cairo/map/MapKey.java` | Key builder; `createValue()` / `findValue()` for insert or lookup |
| `MapValue` | `cairo/map/MapValue.java` | Value area; `getLong(slot)` / `putLong(slot, val)` for prev storage |
| `MapRecord` | `cairo/map/MapRecord.java` | `Record` view of key+value; `setSymbolTableResolver(cursor, indexList)` for SYMBOL resolution |
| `MapRecordCursor` | `cairo/map/MapRecordCursor.java` | Iterates keysMap in insertion order; `toTop()` resets |
| `ListColumnFilter` | `cairo/ListColumnFilter.java` | Selects specific column indices for RecordSink (key columns only) |
| `EntityColumnFilter` | `cairo/EntityColumnFilter.java` | Selects all columns 0..N-1; used for sort sink |

### Supporting
| Class | Purpose | When to Use |
|-------|---------|-------------|
| `SampleByFillRecordCursorFactory` | Existing factory to extend | Add keysMap, keyCount, recordSink, keyPresent[] |
| `SampleByFillCursor` (inner) | Existing cursor to extend | Replace `isNonKeyed=true` with real keyed logic |
| `FillRecord` (inner) | Existing record to extend | Add FILL_KEY branch that delegates to keysMap MapRecord |

**MapFactory creation:**
```java
// [VERIFIED: core/src/main/java/io/questdb/cairo/map/MapFactory.java:57-72]
Map keysMap = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
```

**MapKey insert pattern (pass 1):**
```java
// [VERIFIED: SampleByFillPrevRecordCursor.java:219-229 — initializeMap()]
MapKey key = keysMap.withKey();
keySink.copy(baseRecord, key);
MapValue value = key.createValue();
if (value.isNew()) {
    value.putLong(KEY_INDEX_SLOT, keyIndex++);
    value.putLong(HAS_PREV_SLOT, 0L);
    // leave prevCols at 0 / LONG_NULL
}
```

**MapRecord symbol table resolution:**
```java
// [VERIFIED: cairo/map/MapRecord.java:41 and DistinctRecordCursorFactory.java:230]
// Called once during of(), after keysMap is built:
MapRecord keysMapRecord = keysMap.getRecord();
keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColumnIndices);
// symbolTableColumnIndices: IntList mapping keysMap key col position -> baseCursor col index
```

---

## Architecture Patterns

### Recommended Change Scope

The change is confined to three files:

```
core/src/main/java/io/questdb/griffin/
├── SqlOptimiser.java          -- remove isPrevKeyword gate (line 7880), remove keyed bail-out (line 8001)
├── SqlCodeGenerator.java      -- remove guardAgainstFillWithKeyedGroupBy calls;
│                                  add keyColumnCount param to generateFill(); assign FILL_KEY
└── engine/groupby/
    └── SampleByFillRecordCursorFactory.java  -- add FILL_KEY constant, keysMap, keySink,
                                                  keyPresent[], two-pass logic in cursor
```

### Pattern 1: FILL_KEY Mode Constant

**What:** Add `FILL_KEY = -3` to the public constants in `SampleByFillRecordCursorFactory`.

**Purpose:** Distinguishes key columns from aggregate columns in the `fillModes` array. The `FillRecord` checks for `FILL_KEY` in gap-fill mode and delegates to the keysMap MapRecord.

```java
// [VERIFIED: SampleByFillRecordCursorFactory.java:61-63]
// Existing constants:
public static final int FILL_CONSTANT = -1;
public static final int FILL_PREV_SELF = -2;
// New:
public static final int FILL_KEY = -3;
```

### Pattern 2: Key Column Identification in generateFill()

**What:** `generateFill()` needs to know which output columns of the GROUP BY are key columns vs aggregates. The cleanest verified approach: add `int keyColumnCount` to the `generateFill()` signature. Call sites (lines 7733, 7889, 7957) already have `keyTypes.getColumnCount()` available.

**Challenge:** The GROUP BY output column order follows the SELECT clause, not "keys first." So we cannot simply say "first N columns are keys." We must match output columns against the GROUP BY key list from the model.

**Verified approach:** In `generateFill()`, scan `model.getBottomUpColumns()` and detect key columns by checking if the column's AST expression is in the GROUP BY list (i.e., `ast.type == LITERAL` and `!isTimestampFloor` and `!isGroupByFunction`). The non-timestamp key columns get `FILL_KEY`.

**Simpler verified approach (CONTEXT.md style):** Build a `Set<String>` of key column aliases from the model before calling `generateFill()`, pass it in, then in `generateFill()` match column names against this set.

**Simplest approach (no model scan):** Pass the `IntList keyColIndices` (the column indices in the GROUP BY output that are key columns, excluding timestamp). Built from the `listColumnFilterA` already available at call sites. This is the most direct mapping.

**Recommended:** Pass `IntList keyColIndices` (key column output indices, excluding timestamp) from the `generateSelectGroupBy` call sites into `generateFill`. Inside `generateFill`, for each index in `keyColIndices`, set `fillModes[col] = FILL_KEY`. For the timestamp index, set `FILL_CONSTANT`. For all other indices, set per the fill expression.

```java
// Inside generateFill(), building fillModes:
// [ASSUMED] — exact loop structure, but pattern matches existing fillModes build (lines 3335-3373)
for (int col = 0; col < columnCount; col++) {
    if (col == timestampIndex) {
        fillModes.add(FILL_CONSTANT);
        constantFillFuncs.add(NullConstant.NULL);
    } else if (isKeyColumn(col, keyColIndices)) {  // new check
        fillModes.add(FILL_KEY);
        constantFillFuncs.add(NullConstant.NULL);
    } else {
        // existing fill expression dispatch
    }
}
```

### Pattern 3: keysMap Construction (MapValue Layout)

**What:** The keysMap stores: key columns as the map key, and per-key state as the map value.

**Value layout:**
- Slot 0: `keyIndex` (long) — 0-based sequential insertion index
- Slot 1: `hasPrev` (long) — 0 = no prev yet, 1 = has prev
- Slots 2..2+aggCount-1: `prevCol_i` (long) — raw long bits of each aggregate column

**valueTypes construction:**
```java
// [VERIFIED pattern: ArrayColumnTypes.java]
ArrayColumnTypes valueTypes = new ArrayColumnTypes();
valueTypes.add(ColumnType.LONG);   // slot 0: keyIndex
valueTypes.add(ColumnType.LONG);   // slot 1: hasPrev
for (int i = 0; i < aggColumnCount; i++) {
    valueTypes.add(ColumnType.LONG); // slots 2..N: prevCol_i
}
```

**keyTypes construction:**
```java
// [VERIFIED pattern: AbstractSampleByFillRecordCursorFactory.java:59, MapFactory usage]
ArrayColumnTypes keyTypes = new ArrayColumnTypes();
for (int col : keyColIndices) {
    int colType = groupByMetadata.getColumnType(col);
    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
        keyTypes.add(ColumnType.INT); // SYMBOL stored as int ID in map
    } else {
        keyTypes.add(colType);
    }
}
```

**keySink construction:**
```java
// [VERIFIED: RecordSinkFactory.getInstance(), AbstractSampleByFillRecordCursorFactory.java:69]
// The RecordSink for key columns only (not all columns):
ListColumnFilter keyColFilter = new ListColumnFilter();
for (int col : keyColIndices) {
    keyColFilter.add(col + 1); // ListColumnFilter uses 1-based positive indices
}
RecordSink keySink = RecordSinkFactory.getInstance(
    configuration, asm, groupByMetadata, keyColFilter
);
// Note: no writeSymbolAsString — SYMBOL columns stored as putInt(r.getInt(col)) by default
```

### Pattern 4: Two-Pass Streaming in SampleByFillCursor.of()

**Pass 1 (key discovery):**
```java
// [VERIFIED pattern: SampleByFillPrevRecordCursor.java:216-234 — initializeMap()]
keysMap.clear();
long keyIdx = 0;
while (baseCursor.hasNext()) {
    MapKey key = keysMap.withKey();
    keySink.copy(baseRecord, key);
    MapValue value = key.createValue();
    if (value.isNew()) {
        value.putLong(KEY_INDEX_SLOT, keyIdx++);
        value.putLong(HAS_PREV_SLOT, 0L);
    }
}
keyCount = keyIdx;
keyPresent = new boolean[(int) keyCount]; // or resize existing
baseCursor.toTop();
// Set up keysMap MapRecord symbol table resolution:
keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColumnIndices);
keysMapCursor = keysMap.getCursor();
```

**Pass 2 bucket loop (in hasNext()):**

The cursor logic becomes:

1. While `nextBucketTimestamp < maxTimestamp`:
   - Collect all data rows with timestamp `== nextBucketTimestamp`:
     - For each: find key in keysMap via `keySink.copy + findValue()`, set `keyPresent[keyIndex] = true`, emit data row, update per-key prev in MapValue.
   - After all data rows for bucket:
     - Iterate keysMap cursor. For each key where `!keyPresent[keyIndex]`, position keysMapCursor at that key, set `isGapFilling = true`, emit fill row.
     - Reset `keyPresent[]` to false.
   - Advance `nextBucketTimestamp`.

**Iteration within a bucket:** The sorted base cursor is one-pass-forward only. We cannot go back. To collect all rows for a bucket, we need a peek-ahead pattern: check if the next pending row's timestamp equals the current bucket, emit it, then fetch next row. Stop when pending row's timestamp `> nextBucketTimestamp`.

**Critical insight from Phase 2:** Do NOT call `baseCursor.hasNext()` to peek ahead — it advances the record position and corrupts data before the caller reads it. The existing `hasPendingRow` / `pendingTs` pattern from Phase 2 handles this correctly. Extend it:

```java
// [VERIFIED: SampleByFillRecordCursorFactory.java:283-315 — existing hasNext() pattern]
// The two-pass bucket emission requires a sub-loop for data rows in the same bucket,
// followed by a fill emission loop. The existing single-step state machine must be
// replaced with a proper bucket-level state machine.
```

**State machine for keyed hasNext():**

```
States:
  INIT_BUCKET       — entering a new bucket
  EMIT_DATA_ROW     — caller is reading this data row (return true)
  FILL_KEYS_LOOP    — iterating keysMap for absent keys
  EMIT_FILL_ROW     — caller is reading this fill row (return true)
  NEXT_BUCKET       — done with this bucket, advance timestamp
  DONE              — no more buckets
```

This is cleaner than nested loops inside `hasNext()` since `hasNext()` must return one row at a time and be re-entrant.

### Pattern 5: FillRecord Key Column Delegation

**What:** When `isGapFilling = true` and `fillMode(col) == FILL_KEY`, the FillRecord reads from the keysMap MapRecord (which is positioned at the current key during fill row emission).

```java
// [ASSUMED] — exact method bodies, but pattern matches existing dispatch (lines 428-703)
@Override
public int getInt(int col) {
    if (!isGapFilling) return baseRecord.getInt(col);
    int mode = fillMode(col);
    if (mode == FILL_KEY) return keysMapRecord.getInt(keyColMapPosition(col));
    // ... existing FILL_PREV_SELF, FILL_CONSTANT dispatch
}

@Override
public CharSequence getSymA(int col) {
    if (!isGapFilling) return baseRecord.getSymA(col);
    int mode = fillMode(col);
    if (mode == FILL_KEY) return keysMapRecord.getSymA(keyColMapPosition(col));
    if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getSymbol(null);
    return null;
}
```

**keyColMapPosition(col):** Maps output column index to the key column's position within the map key area. Since the map key only contains key columns, this is the position of `col` within `keyColIndices`. Pre-compute as `int[] outputColToKeyPos` array at construction time.

### Pattern 6: Per-Key Prev Update and Read

**Update (when data row arrives for a key):**
```java
// [VERIFIED: readColumnAsLongBits() pattern at lines 400-411]
// MapValue found for current key (keySink.copy + findValue()):
MapValue value = findKey(baseRecord); // keySink.copy + findValue
value.putLong(HAS_PREV_SLOT, 1L);
int prevSlot = 2;
for (int col = 0; col < columnCount; col++) {
    if (fillMode(col) == FILL_PREV_SELF || fillMode(col) >= 0) {
        value.putLong(prevSlot++, readColumnAsLongBits(baseRecord, col, columnTypes[col]));
    }
}
```

**Read (when emitting fill row for an absent key):**
```java
// MapValue positioned by iterating keysMapCursor
MapValue value = keysMapRecord.getValue();
boolean hasPrev = value.getLong(HAS_PREV_SLOT) != 0;
// For each aggregate col needing PREV:
long prevBits = value.getLong(2 + aggSlotIndex);
// Dispatch same as existing prevValue(col) in FillRecord
```

### Anti-Patterns to Avoid

- **Not calling setSymbolTableResolver:** Without this, `keysMapRecord.getSymA(col)` returns null for SYMBOL key columns, breaking KEY-05 for SYMBOL keys.
- **Iterating keysMap cursor across multiple hasNext() calls without repositioning:** The `keysMapCursor` must be manually positioned (via `keysMap.getCursor()` + iteration) for each fill row emission. Store the cursor position as a `MapRecordCursor` field and advance it during fill key loop.
- **Calling baseCursor.hasNext() as peek-ahead inside hasNext():** Corrupts record position before caller reads it. Use `hasPendingRow` / `pendingTs` pattern from Phase 2.
- **Two-dimensional prevValues array:** `long[][]` allocates per key. Use MapValue slots instead (zero-GC, off-heap, reused across getCursor() calls after keysMap.reopen()).
- **boolean[] keyPresent sized to columnCount:** Must be sized to `keyCount` (number of unique keys from pass 1). Reset per-bucket by `Arrays.fill(keyPresent, false)` or a manual loop.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Insertion-order key iteration | Custom linked list of keys | `OrderedMap` (MapFactory.createOrderedMap) | Already implemented, off-heap, handles var-size keys |
| Off-heap prev storage per key | `HashMap<KeyTuple, long[]>` | `MapValue` slots in `OrderedMap` | Zero-GC, same lifetime as keysMap, no Java heap per key |
| Key column extraction from Record | Manual per-type switch | `RecordSink.copy(record, mapKey)` via `RecordSinkFactory.getInstance` | Handles all types including VAR-SIZE keys (STRING, VARCHAR) correctly |
| SYMBOL string resolution from map | Store full strings in map key | `MapRecord.setSymbolTableResolver(baseCursor, indexList)` | Stores int IDs, resolves to CharSequence on demand via base cursor |
| Presence tracking | `HashSet<Integer>` per bucket | `boolean[]` sized to `keyCount` | Zero-GC, O(1) reset, indices from MapValue slot 0 |

---

## Common Pitfalls

### Pitfall 1: SYMBOL Key Columns Not Resolved

**What goes wrong:** `FillRecord.getSymA(col)` returns `null` for SYMBOL key columns in fill rows.

**Why it happens:** `OrderedMap` stores SYMBOL as `int` (the symbol ID from `record.getInt(col)`), not as `CharSequence`. When iterating the keysMap cursor and calling `keysMapRecord.getSymA(col)`, it needs a `SymbolTableSource` to resolve the int back to a string. Without `setSymbolTableResolver()`, all SYMBOL columns return null.

**How to avoid:** Call `keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColumnIndices)` after pass 1 completes (before pass 2 begins). Build `symbolTableColumnIndices` as an `IntList` mapping each key column position in the map to the corresponding column index in the base cursor.

**Warning signs:** Fill rows have null SYMBOL keys. Non-fill (data) rows are correct. Test with `WHERE city IN ('London', 'Paris')` — if fill rows show null for city, this is the bug.

### Pitfall 2: keysMap Cursor Positioned Incorrectly During Fill Emission

**What goes wrong:** Fill rows emit the wrong key values — all fill rows get the same key (e.g., always the first key).

**Why it happens:** The keysMap `MapRecordCursor` is a single shared object. If the cursor is advanced to find the key's MapValue during data row processing (using `findValue()`), and then the fill row emitter tries to use the same cursor position, it may be at the wrong key.

**How to avoid:** Maintain two separate access patterns:
- For data row lookup: use `keysMap.withKey()` + `keySink.copy()` + `findValue()` (key lookup by content, not cursor position).
- For fill row iteration: use `keysMapCursor` (the MapRecordCursor from `keysMap.getCursor()`) to iterate in insertion order.

These two are independent — `withKey()` does not advance the cursor.

**Warning signs:** All fill rows have the same key values. Correct output only for the first or last key.

### Pitfall 3: Missing toTop() Between Passes

**What goes wrong:** Pass 2 produces no rows — `baseCursor.hasNext()` immediately returns false.

**Why it happens:** After pass 1 consumes the base cursor, the cursor position is at the end. Without `baseCursor.toTop()`, pass 2 has nothing to read.

**How to avoid:** Always call `baseCursor.toTop()` after pass 1 completes. Also reset `keysMapCursor` with `keysMap.getCursor()` (or `keysMapCursor.toTop()`) so fill row iteration starts from the first key.

**Warning signs:** Keyed query returns zero rows, or only fill rows (if the data rows are never emitted).

### Pitfall 4: keyPresent Array Sized or Reset Incorrectly

**What goes wrong:** Keys falsely marked as present (no fill emitted when it should be), or array index out of bounds.

**Why it happens:** (a) `keyPresent` sized to `columnCount` instead of `keyCount`. (b) `keyPresent` not reset after each bucket — keys present in bucket N are still marked in bucket N+1.

**How to avoid:** Size `keyPresent = new boolean[(int) keyCount]` after pass 1 sets `keyCount`. Reset at the end of each bucket's fill loop: `Arrays.fill(keyPresent, false)` or a targeted loop over `keyCount` entries.

**Warning signs:** First bucket correct, subsequent buckets emit wrong fill patterns. ArrayIndexOutOfBoundsException if sized to columnCount and keyCount > columnCount.

### Pitfall 5: keysMap Not Cleared in toTop()

**What goes wrong:** Second `getCursor()` call (re-using the factory) sees stale key set from the previous execution.

**Why it happens:** `toTop()` on the fill cursor resets the base cursor but must also clear the keysMap so pass 1 rebuilds it.

**How to avoid:** In `SampleByFillCursor.toTop()`, call `keysMap.clear()` and reset `isInitialized = false`. Pass 1 then re-runs at the start of the next `hasNext()`.

**Warning signs:** Keyed query gives wrong results on second execution (e.g., via `getCursor()` called twice).

### Pitfall 6: generateFill KeyColIndices Wrong After Sort

**What goes wrong:** FILL_KEY is assigned to wrong columns, breaking fill row emission.

**Why it happens:** After wrapping in `SortedRecordCursorFactory`, the metadata order is the same (sort doesn't change column order), but `keyColIndices` are built from the pre-sort metadata which is identical. This is not a bug — but if column indices are confused with key count, the wrong columns get FILL_KEY.

**How to avoid:** Build `keyColIndices` from the GROUP BY factory metadata BEFORE wrapping in `SortedRecordCursorFactory`. The sort factory preserves column order, so the indices remain valid.

---

## Code Examples

### Optimizer Gate — Line 7880 Change
```java
// [VERIFIED: SqlOptimiser.java:7875-7882]
// BEFORE (Phase 2 state — bars PREV and keyed queries):
&& (sampleByFillSize == 0 || (sampleByFillSize == 1 
    && !isPrevKeyword(sampleByFill.getQuick(0).token)     // <-- remove this check
    && !isLinearKeyword(sampleByFill.getQuick(0).token)))

// AFTER (Phase 3 — only bars LINEAR):
&& (sampleByFillSize == 0 || (sampleByFillSize == 1 
    && !isLinearKeyword(sampleByFill.getQuick(0).token)))
```

### Optimizer Gate — Line 8001 (Keyed Bail-Out)
```java
// [VERIFIED: SqlOptimiser.java:8001-8014]
// BEFORE: if (isKeyed) { return model; }  // falls through to slow path
// AFTER: remove the entire if (isKeyed) { ... return model; } block
// Keyed queries continue through the rewrite so they reach generateFill()
```

### guardAgainstFillWithKeyedGroupBy — Remove at Call Sites
```java
// [VERIFIED: SqlCodeGenerator.java:7733, 7889, 7957]
// BEFORE: guardAgainstFillWithKeyedGroupBy(model, keyTypes);
// AFTER: remove the call entirely at all three call sites
// The fill cursor now handles keyed GROUP BY correctly
```

### generateFill Signature Change
```java
// [ASSUMED] — method signature change, consistent with codebase style
// BEFORE:
private RecordCursorFactory generateFill(
    QueryModel model, RecordCursorFactory groupByFactory, 
    SqlExecutionContext executionContext)

// AFTER: add keyColIndices (key column indices in the GROUP BY output, excluding timestamp)
private RecordCursorFactory generateFill(
    QueryModel model, RecordCursorFactory groupByFactory,
    SqlExecutionContext executionContext,
    @Transient IntList keyColIndices)  // caller passes this; empty list = non-keyed
```

### SampleByFillRecordCursorFactory Constructor Extension
```java
// [ASSUMED] — extends existing constructor (SampleByFillRecordCursorFactory.java:75-106)
public SampleByFillRecordCursorFactory(
    CairoConfiguration configuration,
    RecordMetadata metadata,
    RecordCursorFactory base,
    Function fromFunc,
    Function toFunc,
    long samplingInterval,
    char samplingIntervalUnit,
    TimestampSampler timestampSampler,
    IntList fillModes,
    ObjList<Function> constantFills,
    int timestampIndex,
    int timestampType,
    boolean hasPrevFill,
    RecordSink keySink,         // existing param (was unused)
    ArrayColumnTypes keyTypes,  // new: to build keysMap
    ArrayColumnTypes valueTypes, // new: per-key prev slots
    IntList keyColIndices,       // new: which output cols are key cols
    IntList symbolTableColIndices // new: for setSymbolTableResolver
) {
    // ...
    Map keysMap = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
    this.cursor = new SampleByFillCursor(
        ..., keySink, keysMap, keyColIndices, symbolTableColIndices
    );
}
```

### MapRecord SYMBOL Resolution Setup
```java
// [VERIFIED: MapRecord.setSymbolTableResolver() at cairo/map/MapRecord.java:41]
// [VERIFIED: DistinctRecordCursorFactory.java:230 — usage pattern]
// Called once in SampleByFillCursor.of() after pass 1:
IntList symbolTableColIndices = new IntList(); // pre-built in constructor
for (int i = 0; i < keyColIndices.size(); i++) {
    int col = keyColIndices.getQuick(i);
    if (ColumnType.tagOf(groupByMetadata.getColumnType(col)) == ColumnType.SYMBOL) {
        symbolTableColIndices.add(col); // col index in base cursor
    } else {
        symbolTableColIndices.add(-1); // sentinel: not a SYMBOL col
    }
}
keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColIndices);
```

---

## Key API Facts

### OrderedMap API
[VERIFIED: `core/src/main/java/io/questdb/cairo/map/OrderedMap.java` and `Map.java`]

- `MapFactory.createOrderedMap(CairoConfiguration, ColumnTypes keyTypes, ColumnTypes valueTypes)` — two-arg overload at line 57 of MapFactory.java
- `Map.withKey()` returns a `MapKey` builder
- `MapKey.createValue()` inserts or finds an existing entry; `MapValue.isNew()` indicates insertion
- `MapKey.findValue()` finds without inserting; returns `null` if absent
- `Map.getCursor()` returns a `MapRecordCursor` that iterates in insertion order (for `OrderedMap`)
- `Map.clear()` — resets all entries, called in `toTop()`
- `Map.size()` — current entry count (equals `keyCount` after pass 1)
- `Map.close()` — releases off-heap memory

### MapValue API
[VERIFIED: `core/src/main/java/io/questdb/cairo/map/MapValue.java`]

- `putLong(int index, long value)` / `getLong(int index)` — slot access (slots indexed from 0)
- `isNew()` — true if just created by `createValue()`
- All slots are fixed-size longs; allocate `ColumnType.LONG` for each slot in `valueTypes`

### RecordSink Key-Column-Only Pattern
[VERIFIED: `AbstractSampleByFillRecordCursorFactory.java:69` and `RecordSinkFactory.java`]

```java
// Use ListColumnFilter to select only key columns (1-based positive indices):
ListColumnFilter filter = new ListColumnFilter();
for (int col : keyColIndices) {
    filter.add(col + 1); // positive = ascending in ListColumnFilter convention
}
RecordSink keySink = RecordSinkFactory.getInstance(
    configuration, asm, groupByMetadata, filter
    // no writeSymbolAsString → SYMBOL stored as int ID
);
```

### MapRecord SYMBOL Columns
[VERIFIED: `LoopingRecordSink.java:169-178` — SYMBOL stored as `putInt(r.getInt(col))` when writeSymbolAsString is null]
[VERIFIED: `MapRecord.java:41` — `setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex)` enables string resolution]

The `symbolTableIndex` IntList in `setSymbolTableResolver` maps each **key column position in the map** to the **column index in the base cursor** from which the symbol table is obtained. Non-SYMBOL key columns must map to -1 or a valid index (implementations check type). Build this at construction time from `keyColIndices` and the metadata.

---

## Optimizer Gate Changes — Detail

### Change 1: SqlOptimiser.java line 7880

[VERIFIED: SqlOptimiser.java:7875-7882]

The condition on line 7880 currently excludes PREV:
```java
&& !isPrevKeyword(sampleByFill.getQuick(0).token)
```
Remove this clause. After removal, FILL(PREV) queries reach `rewriteSampleBy()` and get rewritten to GROUP BY. The fill cursor handles PREV for both keyed and non-keyed queries.

This gate change is the Phase 3 enablement for FILL(PREV) on the fast path.

### Change 2: SqlOptimiser.java lines 8001-8014 (keyed bail-out)

[VERIFIED: SqlOptimiser.java:8001-8014]

```java
if (isKeyed) {
    // drop out early, since we don't handle keyed
    nested.setNestedModel(rewriteSampleBy(...));
    // ... unions/joins
    return model;
}
```

Remove the entire `if (isKeyed) { return model; }` block. Without this bail-out, keyed queries proceed through the rewrite and reach `generateFill()`.

### Change 3: SqlCodeGenerator.java — guardAgainstFillWithKeyedGroupBy

[VERIFIED: SqlCodeGenerator.java:9896-9916]

`guardAgainstFillWithKeyedGroupBy` throws `SqlException` when `keyTypes.getColumnCount() > 1` (i.e., more than just the timestamp key). Remove all three call sites at lines 7733, 7889, 7957. The method itself can be deleted or left as dead code.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `generateFill` signature change to add `keyColIndices` is the right approach for key column identification | Code Examples | Low — alternative is scanning model, which also works |
| A2 | `keyColIndices` (key output column indices) are stable after wrapping in `SortedRecordCursorFactory` | Architecture Patterns | Low — sort does not change column order; indices remain valid |
| A3 | The `symbolTableColIndices` IntList must map every key column position to either a base cursor column index (SYMBOL) or -1 (non-SYMBOL); this mapping is used by MapRecord implementations correctly | Key API Facts | Medium — need to verify specific MapRecord implementation checks for -1 |
| A4 | `keysMapCursor` (from `keysMap.getCursor()`) is independent from `withKey()` / `findValue()` operations | Architecture Patterns | Low — Map contract guarantees this; verified via `DistinctRecordCursorFactory` pattern |
| A5 | `SampleByFillRecordCursorFactory` constructor can accept `ArrayColumnTypes` for `keyTypes` / `valueTypes` and build the `keysMap` inline, freeing it in `_close()` | Code Examples | Low — same pattern as AbstractSampleByFillRecordCursorFactory |

---

## Open Questions (RESOLVED)

1. **symbolTableColIndices exact semantics for non-SYMBOL keys**
   - RESOLVED: Use -1 for non-SYMBOL columns in `symbolTableIndex`. The MapRecord implementations only call `resolver.getSymbolTable(symbolTableIndex.getQuick(i))` when `getSymA(col)` is invoked for a SYMBOL-typed column. Non-SYMBOL columns never reach that code path. The plan uses -1 for non-SYMBOL entries.

2. **keysMapCursor.toTop() vs keysMap.getCursor() for iteration**
   - RESOLVED: `OrderedMap.getCursor()` returns a singleton cursor (same object each time). Cache `keysMapCursor = keysMap.getCursor()` once after pass 1. Call `keysMapCursor.toTop()` at the start of each bucket's fill loop to reset iteration. The plan follows this pattern.

---

## Environment Availability

Step 2.6: SKIPPED (no external dependencies — all changes are in-process Java code)

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 5 (via Maven Surefire) |
| Config file | `core/pom.xml` |
| Quick run command | `mvn -pl core -Dtest=SampleByFillTest test` |
| Full suite command | `mvn -pl core test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command |
|--------|----------|-----------|-------------------|
| KEY-01 | Cartesian product emission for keyed FILL(NULL) | integration | `mvn -pl core -Dtest=SampleByFillTest test` |
| KEY-02 | Missing (key, bucket) pairs filled with correct mode | integration | `mvn -pl core -Dtest=SampleByFillTest test` |
| KEY-03 | Per-key PREV does not bleed between keys | integration | `mvn -pl core -Dtest=SampleByFillTest#testKeyedFillPrevIndependent test` (new test) |
| KEY-04 | Key order stable across all buckets | integration | `mvn -pl core -Dtest=SampleByFillTest#testKeyedKeyOrder test` (new test) |
| KEY-05 | Key column values in fill rows are correct | integration | `mvn -pl core -Dtest=SampleByFillTest test` |

### Sampling Rate
- **Per task commit:** `mvn -pl core -Dtest=SampleByFillTest test`
- **Per wave merge:** `mvn -pl core test`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- New test methods in `SampleByFillTest` for KEY-01 through KEY-05 (cartesian product, per-key PREV, stable key order)
- Existing framework is sufficient; no new test infrastructure needed

---

## Security Domain

Not applicable. This phase modifies query execution path internals (fill cursor logic and optimizer rewrites). No authentication, input validation from external sources, cryptography, or access control is involved.

---

## Sources

### Primary (HIGH confidence)
- `core/src/main/java/io/questdb/cairo/map/OrderedMap.java` — insertion-order map, verified API
- `core/src/main/java/io/questdb/cairo/map/MapFactory.java` — `createOrderedMap(config, keyTypes, valueTypes)` at line 57
- `core/src/main/java/io/questdb/cairo/map/MapKey.java` — `createValue()`, `findValue()`
- `core/src/main/java/io/questdb/cairo/map/MapValue.java` — `putLong()`, `getLong()`, `isNew()`
- `core/src/main/java/io/questdb/cairo/map/MapRecord.java` — `setSymbolTableResolver()` signature
- `core/src/main/java/io/questdb/cairo/RecordSink.java` — `copy(Record, RecordSinkSPI)` interface
- `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java` — `getInstance()` overloads
- `core/src/main/java/io/questdb/cairo/ArrayColumnTypes.java` — builder for column type lists
- `core/src/main/java/io/questdb/cairo/ListColumnFilter.java` — 1-based positive index convention
- `core/src/main/java/io/questdb/cairo/EntityColumnFilter.java` — `getColumnIndex(i)` returns `i+1`
- `core/src/main/java/io/questdb/cairo/LoopingRecordSink.java:169-178` — SYMBOL stored as `putInt(r.getInt(col))` by default
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — full Phase 2 implementation, all inner classes
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` — `initializeMap()` at lines 205-235 (pass 1 pattern)
- `core/src/main/java/io/questdb/griffin/engine/groupby/AbstractSampleByFillRecordCursorFactory.java` — `MapFactory.createOrderedMap` + `RecordSinkFactory.getInstance` combined pattern
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3225-3420` — full `generateFill()` method
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:9896-9921` — `guardAgainstFillWithKeyedGroupBy` and `guardAgainstFromToWithKeyedSampleBy`
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:7875-8014` — rewriteSampleBy() eligibility conditions and keyed bail-out
- `core/src/main/java/io/questdb/griffin/engine/groupby/DistinctRecordCursorFactory.java:228-233` — `setSymbolTableResolver(baseCursor, columnIndex)` usage pattern

### Secondary (MEDIUM confidence)
- `.planning/codebase/ARCHITECTURE.md` — dual-path execution, fast-path flow summary
- `.planning/codebase/INTEGRATIONS.md` — integration point 4 (generateFill), integration point 6 (SampleByFillRecordCursorFactory state)
- `.planning/codebase/CONVENTIONS.md` — zero-GC, ObjList, Misc.free patterns, boolean naming

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all APIs verified in source
- Architecture: HIGH — all patterns verified against actual source code
- Pitfalls: HIGH — most derived from verified code; A3 is the only uncertain claim requiring one additional method read
- Optimizer gate changes: HIGH — exact lines verified in SqlOptimiser.java and SqlCodeGenerator.java

**Research date:** 2026-04-09
**Valid until:** Stable (internal codebase; no external dependencies)
