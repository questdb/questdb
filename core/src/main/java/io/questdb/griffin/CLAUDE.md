# Column Type Conversion Internals

How `ALTER TABLE ... ALTER COLUMN ... TYPE` works across native and parquet partitions,
including the eager re-encode model, column tops, and the contract that both paths must produce
identical results.

## Architecture

```
ALTER COLUMN TYPE
  ↓
ConvertOperatorImpl.convertColumn0()
  ├── Pre-pass: convert parquet→native for the cases the re-encode can't apply
  │   (target SYMBOL; a chained conversion whose parquet no longer matches metadata)
  ├── For each NATIVE partition:
  │   └── ColumnTypeConverter dispatches by conversion category
  │       ├── Fixed→Fixed:  JNI native (ConvertersNative.fixedToFixed)
  │       ├── Fixed→Var:    Java loop, format each value to string
  │       ├── Var→Fixed:    Java loop, parse each string to value
  │       ├── Var→Var:      Java loop, transcode (UTF-16↔UTF-8)
  │       ├── →Symbol:      Java loop, resolveSymbol() builds symbol map
  │       └── Symbol→:      Java loop, map ID→string, then convert
  ├── For each PARQUET partition:
  │   └── Eagerly re-encode to the new type (rewriteParquetPartition), reusing the
  │       O3 row-group rewrite machinery; propagate the column top old→new index
  └── Update metadata: new column type, replacingIndex chain
```

## Conversion Categories

### Fixed→Fixed (e.g., INT→LONG, SHORT→DOUBLE, DATE→TIMESTAMP)

**Native path**: `ConvertersNative.fixedToFixed()` via JNI. Maps source file, allocates
destination file, bulk-converts. Handles widening, narrowing, and null sentinels natively.

**Parquet path**: applied during the eager re-encode. The Rust decoder reads
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

**Parquet path**: during the eager re-encode, parquet stores strings as UTF-8 BYTE_ARRAY,
decoded to VARCHAR (UTF-8) or STRING (UTF-16) depending on target. Rust does the physical
decode; Java transcodes UTF-16↔UTF-8 when the parquet and target encodings differ.

### Fixed→Var (e.g., INT→STRING, DOUBLE→VARCHAR)

**Native path**: Java loop in `convertFixedToString()`/`convertFixedToVarchar()`. For each
row, reads fixed value, formats to string via `Fixed2VarConverter` (e.g., `stringFromInt`),
appends to destination var-size files.

**Parquet path**: during the eager re-encode, Rust decodes the source fixed type, then Java
performs the fixed→var conversion before re-encoding. The Rust layer cannot produce
variable-length output directly.

### Var→Fixed (e.g., STRING→INT, VARCHAR→LONG)

**Native path**: Java loop in `convertFromString()`/`convertFromVarchar()`. For each row,
reads string value, parses via `Var2FixedConverter` (e.g., `str2Int`), writes to destination
fixed file. Parse failures produce null sentinels.

**Parquet path**: same as fixed→var — during the eager re-encode Rust decodes the source var
type and Java parses it to the target fixed type before re-encoding.

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

**Parquet path**: handled by the eager re-encode, not the pre-pass. Parquet stores SYMBOL
data as UTF-8 BYTE_ARRAY, so the re-encode decodes it as `VARCHAR_SLICE` and applies the
symbol→var/fixed conversion before re-encoding — no symbol-map lookup is needed.

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

**For parquet partitions**: the column top is propagated to the new column index alongside
the eager data re-encode, so a re-encoded partition (or a later parquet→native conversion)
finds data at the correct row offsets.

**Column doesn't exist in parquet**: If `parquetColType` is undefined (column was added
after the partition was converted to parquet), the column top equals the partition size —
all rows are NULL. No conversion needed.

## The Eager Re-encode Model

A parquet partition stores each column in its current type. When the column type changes,
`ALTER COLUMN TYPE` **eagerly re-encodes** the partition to the new type
(`TableWriter.rewriteParquetPartitionWithConversions` -> `O3PartitionJob.rewriteParquetPartition`),
so its on-disk bytes always match the metadata. The commit is deferred to the ALTER's
structure-version barrier, so the re-encode lands atomically with the new `_meta`.

### The replacingIndex Chain

Each `ALTER COLUMN TYPE` creates a new column in metadata with `replacingIndex` pointing to
the previous column. This forms a chain:

```
Column "price" (current, index=5, type=STRING)
  └── replacingIndex → index=3 (type=INT)
       └── replacingIndex → index=1 (type=DOUBLE, original)
```

The re-encode stamps each column into the parquet footer under its original writer index
(the chain head). When reading, Java looks up which column index the parquet file contains.
The chain head is precomputed at metadata load time by `TableUtils.getReplacingChainHead`
and surfaced as `getOriginalWriterIndex()`, so the lookup is a direct map probe rather than
a per-query walk:

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

### The parquet→native pre-pass

Before the eager re-encode, a pre-pass in `ConvertOperatorImpl` converts a parquet partition
to native when:

**Target is SYMBOL**: symbol maps cannot be built from parquet, so every parquet partition
with data for the column must become native first, then the native →Symbol conversion runs.

The pre-pass also carries a **chained-conversion** check (a prior conversion whose parquet
storage no longer matches metadata). Because the eager re-encode keeps a present column's
parquet type equal to metadata, that condition is not met for a re-encoded partition, so the
branch does not fire in practice; it stays as a guard.

Symbol-as-source (SYMBOL → non-SYMBOL) is **not** a pre-pass trigger — the eager re-encode
decodes the parquet BYTE_ARRAY as `VARCHAR_SLICE` and applies the var→fixed/var→string
conversion directly, since the parquet column already stores the strings.

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

## Reads After Conversion

Because the eager re-encode keeps a parquet partition's stored column type equal to the
current metadata type, reads are direct — there is no read-time conversion.

`PageFrameMemoryPool.openParquet()` builds a `field_id -> parquet column index` map, then
`resolveParquetColumn()` maps each query column:

1. Direct lookup by the column's current writer index.
2. If that misses, the column went through `ALTER COLUMN TYPE`, so the parquet stores it
   under its original writer index (the `replacingIndex` chain head, `getOriginalWriterIndex()`);
   retry under that id. The stored type already equals the current type — only the writer
   index differs — so the column is decoded directly at the current type.
   `resolveParquetColumn` asserts `sourceTag == targetTag` and throws if they ever diverge
   (they can't, unless a partition advanced its metadata type without being re-encoded).
3. A column absent from the parquet (added after the partition became parquet) stays at
   address 0 and reads NULL via the column-top path.

`ParquetBuffers.decode()` calls the Rust `decodeRowGroup()`; the decoded data lives in
off-heap `RowGroupBuffers` behind an LRU cache. `PageFrameMemoryRecord` reads those buffers
directly — no per-row casts.

## O3 Merge on a Converted Parquet Partition

`ALTER COLUMN TYPE` re-encodes parquet eagerly, so when O3 (out-of-order) rows later land in
a converted partition the parquet already stores the current type — the merge sees an
already-converted partition and applies no type conversion.

The re-encode itself reuses the O3 row-group rewrite machinery
(`rewriteParquetRowGroupWithConversions`, `chooseParquetDecodeType`, `prepareParquetSourceColumn`)
to decode the old parquet and write the new type. That write path — the merge-action
dispatch, the shared conversion and its allocations, and how **deduplication** interacts with
it — is documented in `cairo/CLAUDE.md` ("Writing Parquet Partitions with Pending Column
Conversions").

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
on a parquet partition, the column-top region for the source column must materialise as
INT_NULL. With `Required` repetition there is no way to distinguish column-top rows from real
zeros/`false` at the parquet layer; the decoder would produce `0` in INT space instead of
`Integer.MIN_VALUE`, diverging from the native ALTER path which sees NULL via the `.top` file.
Making the schema `Optional` lets def-level=0 carry the column-top NULL signal through the
re-encode's decode.

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
| `O3PartitionJob.java` | `rewriteParquetPartition()` (eager re-encode); walks the replacingIndex chain to map parquet columns to current metadata |
| `TableWriter.java` | `rewriteParquetPartitionWithConversions()`, `convertPartitionParquetToNative()`, `getParquetColumnType()` |
| `PageFrameMemoryPool.java` | Query path: opens parquet frames, resolves the column mapping (writer-index remap, no conversion) |
| `PageFrameMemoryRecord.java` | Query path: reads decoded parquet values directly |
| `row_groups.rs` | Rust: type dispatch, `post_convert()`, boolean expansion, date/timestamp scaling |
| `decode.rs` | Rust: physical parquet type → decoded values |