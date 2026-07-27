# Column Type Conversion Internals

How `ALTER TABLE ... ALTER COLUMN ... TYPE` works across native and parquet partitions,
including the laziness model, column tops, and the contract that both paths must produce
identical results.

## Architecture

```
ALTER COLUMN TYPE
  ↓
ConvertOperatorImpl.convertColumn0()
  ├── Pre-pass: convert parquet→native when lazy decode is impossible
  │   (source or target is SYMBOL, or chained conversion with type mismatch)
  ├── For each NATIVE partition:
  │   └── ColumnTypeConverter dispatches by conversion category
  │       ├── Fixed→Fixed:  JNI native (ConvertersNative.fixedToFixed)
  │       ├── Fixed→Var:    Java loop, format each value to string
  │       ├── Var→Fixed:    Java loop, parse each string to value
  │       ├── Var→Var:      Java loop, transcode (UTF-16↔UTF-8)
  │       ├── →Symbol:      Java loop, resolveSymbol() builds symbol map
  │       └── Symbol→:      Java loop, map ID→string, then convert
  ├── For each PARQUET partition:
  │   └── Skip (parquet decoder handles on-the-fly conversion later)
  │       Column top is propagated from old column index to new column index
  └── Update metadata: new column type, replacingIndex chain
```

## Conversion Categories

### Fixed→Fixed (e.g., INT→LONG, SHORT→DOUBLE, DATE→TIMESTAMP)

**Native path**: `ConvertersNative.fixedToFixed()` via JNI. Maps source file, allocates
destination file, bulk-converts. Handles widening, narrowing, and null sentinels natively.

**Parquet path**: Rust decoder handles during `decode_page_dispatch()`. The decoder reads
the parquet physical type and decodes directly into the target fixed type. For same-width
reinterpretation (DATE↔TIMESTAMP), `post_convert()` applies scaling:
- Date→Timestamp: multiply i64 by 1000 (ms→μs), skip null sentinels
- Timestamp→Date: divide i64 by 1000 (μs→ms), skip null sentinels

**Boolean expansion**: Parquet stores BOOLEAN as bit-packed (1 bit/value). Rust unpacks to
1 byte/value. When target is wider (SHORT/INT/LONG/FLOAT/DOUBLE), `expand_bool<T>()` in
`post_convert()` walks backwards to avoid overwriting unread bytes.

### Var→Var (e.g., STRING→VARCHAR, VARCHAR→STRING)

**Native path**: Java loop in `ColumnTypeConverter`. Reads from source `.i`+`.d` files,
transcodes each value (UTF-16↔UTF-8), writes to destination `.i`+`.d` files.

**Parquet path**: Parquet stores strings as UTF-8 BYTE_ARRAY. Decoded to VARCHAR (UTF-8)
or STRING (UTF-16) depending on target. The Rust decoder handles the physical decode; Java
handles UTF-16↔UTF-8 transcoding if the parquet type and target type use different encodings.

### Fixed→Var (e.g., INT→STRING, DOUBLE→VARCHAR)

**Native path**: Java loop in `convertFixedToString()`/`convertFixedToVarchar()`. For each
row, reads fixed value, formats to string via `Fixed2VarConverter` (e.g., `stringFromInt`),
appends to destination var-size files.

**Parquet path**: **Deferred to Java**. Rust decodes in the source fixed type, then Java
performs the fixed→var conversion after parquet decode. The Rust layer cannot produce
variable-length output directly.

### Var→Fixed (e.g., STRING→INT, VARCHAR→LONG)

**Native path**: Java loop in `convertFromString()`/`convertFromVarchar()`. For each row,
reads string value, parses via `Var2FixedConverter` (e.g., `str2Int`), writes to destination
fixed file. Parse failures produce null sentinels.

**Parquet path**: **Deferred to Java**. Same as fixed→var — Rust decodes the source var type,
Java performs parsing post-decode.

### →Symbol (e.g., INT→SYMBOL, STRING→SYMBOL)

**Why Symbol is special**: SYMBOL columns store INT32 symbol IDs, not actual values. The IDs
index into a separate symbol map (`.o`, `.k`, `.v`, `.c` files). Building the symbol map
requires a `SymbolMapWriter` — this cannot happen inside the parquet decoder.

**Native path**: Java loop in `convertFixedToSymbol()`/`convertFromStringToSymbol()`. For
each row: format/read the source value as a string, call `symbolMapWriter.resolveSymbol()`
to get or create an ID, write the ID to the destination fixed column.

**Parquet path**: **Impossible lazily**. The `ConvertOperatorImpl` pre-pass converts parquet
partitions to native first. Then the normal native conversion runs. If the column doesn't
exist in the parquet file (added after the partition was converted to parquet), the
conversion is skipped — the column top covers those rows.

### Symbol→ (e.g., SYMBOL→STRING, SYMBOL→INT)

**Native path**: Java loop in `convertFromSymbol()`. Reads INT32 symbol ID, looks up the
string via `symbolMapReader`, then either writes the string directly (→STRING/VARCHAR) or
parses it to the target fixed type (→INT/LONG/etc.).

**Parquet path**: The `ConvertOperatorImpl` pre-pass converts parquet partitions to native
first (like →Symbol). The native conversion then reads symbol IDs and resolves them via the
symbol map files. Although parquet stores SYMBOL data as UTF-8 BYTE_ARRAY, the main
conversion loop skips parquet partitions entirely, so without the pre-pass the column would
remain unconverted and appear as NULL after a later parquet→native conversion.

## Column Tops

A **column top** is the first row number that has data for a column in a given partition.
If a column is added after a partition already exists, rows `[0, columnTop)` have no data
for that column — reads return NULL.

**Storage**: Column tops live in the `_cv` file (`ColumnVersionWriter`). Each entry is keyed
by `(partitionTimestamp, columnIndex)` and stores the column top value plus a column name txn.

**During type conversion**: `ALTER COLUMN TYPE` creates a new column index. The column top
must be **propagated** from the old column index to the new one:

```java
long colTop = columnVersionWriter.getColumnTop(pts, existingColIndex);
columnVersionWriter.upsertColumnTop(pts, newColumnIndex, colTop);
```

Without this, the new column would appear to have data from row 0, but the actual data
files only contain rows from `columnTop` onward — causing misalignment.

**For parquet partitions**: Column tops are propagated eagerly even though the data
conversion is lazy. This ensures that if the parquet partition is later converted to
native, the native reader finds data at the correct row offsets.

**Column doesn't exist in parquet**: If `parquetColType` is undefined (column was added
after the partition was converted to parquet), the column top equals the partition size —
all rows are NULL. No conversion needed.

## The Laziness Model

Parquet partitions store data in the type that was current when the partition was converted
to parquet. When the column type is later changed, **parquet is NOT re-encoded**.

### The replacingIndex Chain

Each `ALTER COLUMN TYPE` creates a new column in metadata with `replacingIndex` pointing to
the previous column. This forms a chain:

```
Column "price" (current, index=5, type=STRING)
  └── replacingIndex → index=3 (type=INT)
       └── replacingIndex → index=1 (type=DOUBLE, original)
```

When reading a parquet partition, Java looks up which column index the parquet file
actually contains. The chain head (the original writer index at the bottom of the
`replacingIndex` chain) is precomputed at metadata load time by
`TableUtils.getReplacingChainHead` and surfaced as `getOriginalWriterIndex()`, so the
lookup is a direct map probe rather than a per-query walk:

```java
// PageFrameMemoryPool.resolveParquetColumn
int parquetIdx = columnIdToParquetIdx.get(columnMapping.getWriterIndex(i));
if (parquetIdx < 0) {
    int origWriterIndex = columnMapping.getOriginalWriterIndex(i);
    if (origWriterIndex >= 0 && origWriterIndex != columnWriterIndex) {
        parquetIdx = columnIdToParquetIdx.get(origWriterIndex);
    }
}
```

### When Lazy Conversion Breaks

The pre-pass in `ConvertOperatorImpl` converts parquet to native in two cases:

**1. Target is SYMBOL**: Symbol maps cannot be built from parquet. Every parquet partition
with data for the column must become native first.

**2. Chained conversion with type mismatch**: If parquet stores type A, current metadata
says type B, and we're now converting to type C — the parquet decoder would convert A→C
directly. But the native path would convert B→C (it already did A→B in a prior ALTER).
These paths may produce different results (e.g., INT→STRING→DATE vs INT→DATE have different
semantics). Converting parquet to native first ensures B→C on both paths.

Symbol-as-source (SYMBOL → non-SYMBOL) is **not** a pre-pass trigger. The lazy decoder
handles it via `VARCHAR_SLICE`: `PageFrameMemoryPool.resolveParquetColumn` decodes the
parquet BYTE_ARRAY as VARCHAR_SLICE and flags the column for var→fixed/var→string
conversion in `PageFrameMemoryRecord`. No symbol-map lookup is needed because the parquet
column already stores the strings directly.

The check:
```java
boolean hasPriorConversion = tableWriter.getMetadata()
        .getColumnMetadata(existingColIndex).getReplacingIndex() >= 0;
boolean isTargetSymbol = ColumnType.isSymbol(newType);
if (hasPriorConversion || isTargetSymbol) {
    int parquetColType = tableWriter.getParquetColumnType(pi, existingColIndex);
    if (!ColumnType.isUndefined(parquetColType)
            && (isTargetSymbol
                || !isParquetStorageCompatible(parquetColType, existingType))) {
        tableWriter.convertPartitionParquetToNative(pts, false);
    }
}
```

## How Lazy Conversions Materialize During Queries

When a query reads a parquet partition whose column was type-changed after the partition
was converted to parquet, the conversion happens on-the-fly through `PageFrameMemoryPool`.

### Setup: Opening a Parquet Frame

`PageFrameMemoryPool.navigateTo()` calls `openParquet(frameIndex)` which:

1. Reads parquet file metadata via `PartitionDecoder.metadata()` (Rust call).
2. Builds a column ID map (`field_id` → parquet column index).
3. For each query column, calls `resolveParquetColumn()`:
   - Tries direct lookup by the column's current writer index.
   - If not found, falls back to the column's `getOriginalWriterIndex()` — the
     precomputed chain head from `TableUtils.getReplacingChainHead` — to find the
     parquet column under an older writer index.
   - Compares the parquet column's stored type against the current metadata type.
   - Records the conversion strategy in `sourceColumnTypes[col]`.

### Conversion Strategy Signals

`sourceColumnTypes[col]` encodes what conversion is needed per column:

| Value | Meaning | Example |
|-------|---------|---------|
| `-1` | No conversion needed | Column type matches parquet |
| `>= 0` | Fixed→Var conversion, value is source type tag | INT stored, current type is STRING |
| `< -1` | Var→Fixed conversion, value is negative source tag | VARCHAR stored, current type is LONG |

For **Symbol→Non-Symbol**: the parquet column (stored as BYTE_ARRAY) is decoded as
`VARCHAR_SLICE` and `sourceColumnTypes[col]` is set to `-ColumnType.VARCHAR`.

For **Fixed→Fixed** (e.g., INT→LONG, DATE→TIMESTAMP): no signal needed. The Rust decoder
handles the conversion during decode — data arrives in the target type already.

If any column needs conversion, `hasTypeCasts = true` is set on the frame.

### Decode

`ParquetBuffers.decode()` calls `parquetDecoder.decodeRowGroup()` (Rust JNI). The Rust
decoder receives parquet column indices paired with target decode types. For fixed→fixed
mismatches, Rust converts during decode (widening, narrowing, date/timestamp scaling via
`post_convert()`). For fixed→var and var→fixed, Rust decodes in the **source** type — Java
converts later.

Decoded data lives in off-heap `RowGroupBuffers` managed by an LRU buffer cache. Not
zero-copy from parquet — the decoder writes into these buffers.

### Per-Row Lazy Conversion at Record Access

`PageFrameMemoryRecord` accessor methods check `hasTypeCasts` on every call:

```java
// Example: getInt(col)
if (hasTypeCasts) {
    int srcTag = sourceColumnTypes.getQuick(col);
    if (srcTag < -1) {              // Var→Fixed: parse string to int
        return convertVarToInt(-srcTag, col);
    }
}
return Unsafe.getUnsafe().getInt(address + (rowIndex << 2));  // Direct read
```

```java
// Example: getStrA(col)
if (hasTypeCasts) {
    int srcTag = sourceColumnTypes.getQuick(col);
    if (srcTag >= 0) {              // Fixed→Var: format int as string
        return convertFixedToStr(srcTag, col, stringSinkA);
    }
}
// Direct varchar/string read
```

**Zero-GC**: Conversions use pre-allocated reusable `StringSink`/`Utf8StringSink` pools.
No allocations on the data path.

### O3 Merge with Type-Converted Parquet

When O3 (out-of-order) rows land inside a parquet partition that has a pending lazy
conversion, `O3PartitionJob` materialises the conversion at **write** time while merging the
rows in (a `MERGE` action interleaves them via `mergeRowGroup`; a non-overlapping row group
that still needs the new schema is re-encoded via `ParquetRowGroupMaterializer.materialize`).

That write path — the merge-action dispatch, the shared
`ParquetColumnTypeConverter.prepareSourceColumn` conversion and its allocations, and how
**deduplication** interacts with it — is documented
separately in `cairo/CLAUDE.md` ("Writing Parquet Partitions with Pending Column
Conversions"). This file (griffin) owns the conversion *semantics* and the *read* path; the
write path lives with `O3PartitionJob` / `TableWriter` in cairo.

## The Native/Parquet Contract

Both paths **must produce identical results** for the same conversion. This means:

1. **Same null handling**: Both use the same null sentinels (INT_NULL = Integer.MIN_VALUE,
   LONG_NULL = Long.MIN_VALUE, FLOAT/DOUBLE_NULL = NaN). Rust's `post_convert` checks
   `qdb_core::col_type::nulls::LONG` before scaling date/timestamp values.

2. **Same casting semantics**: Numeric widening/narrowing, date/timestamp scaling (×/÷1000),
   boolean expansion — all must agree between JNI native code and Rust decoder.

3. **Same parse/format rules**: When converting through strings (fixed→var→fixed), both
   paths use the same number formatting and parsing (via `Numbers.parseInt/parseLong` in
   Java and equivalent Rust logic).

4. **Same column top respect**: Both skip rows before columnTop. Native path maps the file
   starting at `skipRows = columnTop`. Parquet decoder reads all rows but the column top
   ensures correct alignment.

## NULL Sentinels by Type

| Type | Null Sentinel | Notes |
|------|---------------|-------|
| BYTE | none | 0 used as value; no dedicated null |
| SHORT | none | 0 used as value; no dedicated null |
| BOOLEAN | none | 0 = false, no null distinction in fixed storage |
| INT | `Integer.MIN_VALUE` | -2_147_483_648 |
| LONG | `Long.MIN_VALUE` | -9_223_372_036_854_775_808 |
| DATE | `Long.MIN_VALUE` | same as LONG |
| TIMESTAMP | `Long.MIN_VALUE` | same as LONG |
| FLOAT | `Float.NaN` | checked via `Numbers.isNull(float)` |
| DOUBLE | `Double.NaN` | checked via `Numbers.isNull(double)` |
| IPv4 | 0 | `Numbers.IPv4_NULL` |
| UUID | `Long.MIN_VALUE` for both hi and lo | two longs |
| STRING/VARCHAR | null reference | var-size null marker in `.i` file |
| SYMBOL | `SymbolTable.VALUE_IS_NULL` | -1 |

**BYTE/SHORT/BOOLEAN have no null sentinel.** Converting a nullable type (INT, LONG, etc.)
to BYTE/SHORT loses null information — nulls become 0. This is a known semantic gap.

## Parquet Schema Repetition for BOOLEAN/BYTE/SHORT/CHAR

`core/rust/qdbr/src/parquet_write/schema.rs` decides the parquet `Repetition` per column.
On master, BOOLEAN, BYTE, SHORT and CHAR were all written as `Required` — none of them have
an in-band null sentinel, and the file-level schema was kept stable across O3 merges.

Now every non-designated column is written `Optional`; only the designated timestamp stays
`Required` (it is never null). BOOLEAN, BYTE, SHORT and CHAR data values still cannot be null
(their `Nullable::is_null()` returns `false` unconditionally), so only column-top rows take
the null branch — those rows are marked with definition level 0.

**Why**: column type conversion. When `ALTER COLUMN TYPE` converts SHORT→INT (or BOOLEAN→INT)
lazily on a parquet partition, the column-top region for the source column must materialise as
INT_NULL on read. With `Required` repetition there is no way to distinguish column-top rows
from real zeros/`false` at the parquet layer; the lazy decoder would produce `0` in INT space
instead of `Integer.MIN_VALUE`, diverging from the native ALTER path which sees NULL via the
`.top` file. Making the schema `Optional` lets def-level=0 carry the column-top NULL signal
through to the decoder.

**Test impact**: parquet schema assertions for BOOLEAN/BYTE/SHORT/CHAR columns must use
`assertSchemaNullable` (maxDefinitionLevel=1), and Java-side reader values for column-top
rows of these types are 0/`false` while the parquet reader returns `null` — comparisons must
use `assertPrimitiveValue(..., 0)` / `assertPrimitiveValue(..., false)` rather than strict
`assertEquals`.

**Caveat**: `parquet_write/encoders/{plain,delta_binary_packed,rle_dictionary}` notnull
encoder paths assert `Repetition::Required` and panic if handed an Optional column. The
BYTE/SHORT/CHAR arms of `encode_int32_dispatch` are therefore routed to the
`encode_int_nullable` variants for every encoding (Plain, DeltaBinaryPacked, RleDictionary),
and `encode_boolean_dispatch` routes to `encode_boolean_nullable` (which emits def levels and
bit-packs only the non-null values). Each dispatch still falls back to the `Required`/notnull
encoder when a legacy file's preserved schema says `Required`. Schema and encoder dispatch
must stay in sync.

## Key Files

| File | Role |
|------|------|
| `ConvertOperatorImpl.java` | Orchestrator: pre-pass, partition dispatch, column top propagation |
| `ColumnTypeConverter.java` | All Java conversion loops (var-size, string, symbol) |
| `ConvertersNative.java` | JNI bridge to native fixed→fixed conversion |
| `ColumnVersionWriter.java` | Manages `_cv` file: column tops per (partition, column) |
| `O3PartitionJob.java` | Walks replacingIndex chain to map parquet columns to current metadata |
| `TableWriter.java` | `convertPartitionParquetToNative()`, `getParquetColumnType()` |
| `PageFrameMemoryPool.java` | Query path: opens parquet frames, resolves column mapping, sets up conversion strategy |
| `PageFrameMemoryRecord.java` | Query path: per-row lazy conversion at accessor level (zero-GC) |
| `row_groups.rs` | Rust: type dispatch, `post_convert()`, boolean expansion, date/timestamp scaling |
| `decode.rs` | Rust: physical parquet type → decoded values |
