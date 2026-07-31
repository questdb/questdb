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

## INT Expression Width

An INT expression carries exactly one value: the value its four bytes hold. INT arithmetic wraps
modulo 2^32 in every context, exactly as LONG arithmetic wraps modulo 2^64.

> **Width is a property of the declared type and nothing else. To compute at 64 bits, widen an
> operand: `secs * 1_000_000L`, or `i::long * j`.**

`2000000000 + 2000000000` is an INT expression whose value is `-294967296`, and `::LONG`,
`::TIMESTAMP`, `::DOUBLE`, a store into a LONG column, `to_utc(...)`, a comparison against a LONG
column and an `UPDATE` into a TIMESTAMP column all read that same number, sign-extended where the
target is 64 bits. `IntFunction.getLong()` / `getTimestamp()` / `getDate()` are all
`Numbers.intToLong(getInt())`, and `Numbers.intToLong(INT_NULL) == LONG_NULL`, so an expression that
lands on `-2^31` reads as NULL at every width.

Three properties follow, and they are the reason to prefer this over any context-dependent rule:

- **`SELECT expr` shows exactly what every consumer received.** A user who gets a wrong timestamp
  can inspect the arithmetic and see the wrap that caused it, in one step.
- **Nullness stops depending on context.** `2147483647 + 1`, `-1073741824 * 2` and `~2147483647`
  read as NULL everywhere, `::long` included.
- **Every boundary reads a function at its declared type**, the engine's oldest and best-tested
  contract. Nothing is left for a boundary to ask, so nothing is left for it to get wrong.

### What this gives up

GitHub issue [#4752](https://github.com/questdb/questdb/issues/4752) is reopened by this rule.
`to_utc(1_720_468_802 * 1_000_000, tz)` returns a 1970 date. PR #4824 fixed it by giving the INT
operators a `getLong()` that recomputed at 64 bits, and PR #7021 extended that to more operators;
both are reverted here. The mitigation is the release note and the workaround the issue itself
named (`1_000_000L`), not anything in the engine. `IntWidthWrapTest` pins the whole matrix,
`MulIntFunctionFactoryTest#testTimestampIntOverflow` pins the repro, and `IntWidthContextTest` pins
the three contexts that reach 64 bits through overload resolution rather than through syntax.

A designated timestamp target is the loudest case: the wrapped product is negative, so the writer
refuses it with "designated timestamp before 1970-01-01 is not allowed" rather than storing a 1970
date. An ingest that relied on the widening errors instead of inserting.

**Throw on overflow** — the Postgres / DuckDB behaviour — is the follow-up this unblocks. It is the
same one-value architecture with a stricter value policy, and it fixes #4752 in *every* spelling
with one rule and no context-dependence.

### Constant folding

`FunctionParser.functionToConstant0`'s INT arm folds to an `IntConstant` holding the wrap, or to
`IntConstant.NULL` when `getInt()` carries the sentinel. The declared type of a constant expression
no longer depends on its value, so:

- `SELECT 1000000*1000000` is an INT column returning `-727379968`. Over pgwire the OID is `int4`.
- `CREATE TABLE t AS (SELECT 1000000*1000000 AS v)` creates an **INT** column storing the wrap.
- `INSERT INTO <existing INT column> SELECT 1000000*1000000` stores the wrap.

The literal, column and bind-variable spellings of one constant arithmetic therefore agree exactly,
which is what let the query fuzzer drop its int-overflow tolerance.

### The store path

`RecordToRowCopierUtils` (two bytecode generators) and `LoopingRecordToRowCopier` are purely
type-directed: an INT source reads `getInt()` for every target and `SqlUtil.implicitCastIntAsLong`
sign-extends it for a 64-bit one. There is no per-column width question, so no factory, metadata or
`ColumnTypes` view has to answer one.

### The JIT

`CompiledFilterIRSerializer` derives every type from the AST plus metadata and never reads the
function tree, so it has to reproduce the same rule by hand. The rule it implements is uniform:
**compute an arithmetic subtree at its own width, sign-extend only at the comparison boundary.**

`arithExprType` types a node by operand promotion alone, so an INT arithmetic subtree is `I4_TYPE`
however large its mathematical result; only a genuine 64-bit operand promotes it. `descend()` folds
a pure-constant subtree at that same type — the I4 arm reproduces the Java filter's per-op INT
wrapping, which differs from `(int) longVal` for a non-modular operator such as division.

Three compensations survive, and all three are about a constant's emitted WIDTH rather than about
arithmetic semantics:

- `markNarrowConstCmpWidenPair` / `maybeWidenCmpConstOperand` sign-extend a narrow-int leaf and the
  out-of-INT-range constant it is compared against (`WHERE i < 5_000_000_000`). The type observer
  sees only 4-byte columns, so the constant would otherwise emit as a lossy F4.
- `markFloatCmpConst` sends a constant with no exact float to the filter at double width, since a
  FLOAT column always compares at DOUBLE width in Java.
- `isNarrowIntCmpWideningConst` does the same for a narrow-int leaf compared against a
  floating-point constant (`WHERE i < 1.00000003`), which promotes to DOUBLE in Java through
  `IntFunction#getDouble`. Two independent roundings can diverge here, not one: the constant may
  have no exact float, **and** the column may be the side that rounds, since `(float)` holds every
  integer only up to 2^24 — `(float) 16777217` is `16777216`, so even an exactly-representable
  bound like `16777216.0` collides. The rule widens on `inexact || |c| >= 2^24`, and widens the
  LEAF too so the pair reaches the backend's ungated `(i64, f64)` arm. `maybeWidenCmpConstOperand`
  covers the arithmetic-subtree spelling (`i + 0 > 16777216.0`) by widening only the constant, so
  the subtree keeps wrapping at i32. Two things keep the cost off the vectorized path: the rule
  widens an inexact constant only when an integer (or the tolerance band round one) actually falls
  between the bound and the float the JIT would emit, so `i > 1.1` and `i > 0.1` keep the eight-lane
  loop; and `isWideLaneIntCmpFloatConstPair` makes the shapes that DO widen wide-lane eligible, so
  they run the four-lane loop rather than dropping to scalar. BYTE and SHORT leaves stay scalar -
  `avx2::sx_i64` widens an i32 lane and declines anything else - as does the arithmetic-subtree
  spelling, which is never sign-extended.
- `narrowKeptConstants` pins an integer constant operand of a NARROW arithmetic node to its own
  width, so `i32 * 2` stays `int32_mul` even when a LONG elsewhere in the predicate makes the
  observer type constants at I8.

An `IN` list re-serializes its key once per element but the key is one node with one emitted width,
so a single 64-bit pairing pulls the whole list to 64 bits: `markWidthSemantics` sign-extends the
key and every narrow-int leaf element together.

`forceScalarOnUnharmonisedNarrowArith` is the one place the JIT gives up performance for the rule.
SX_I64 is emitted per LEAF, so there is no way to sign-extend a narrow arithmetic subtree's RESULT
from the frontend, and the pairing reaches the backend as i32-against-i64 — which neither vectorized
mode reproduces correctly. `WHERE i * j > long_col` therefore runs scalar. Teaching the IR to emit
SX_I64 after an operator rather than after a leaf would recover it, and is the obvious follow-up.

Missing a width compensation does not merely lose rows — it can make the **scalar and vectorized
backends disagree with each other**, so the same query on the same data returns different rows
depending on whether the host has AVX2. `assertJitScalarAndVectorMatchJava` in
`CompiledFilterRegressionTest` runs a query with JIT off, then `FORCE_SCALAR`, then vectorized, and
is the guard for that class of bug.

`isFloatLeaf` deliberately does NOT accept an F8-promoting subtree such as `f + 0.0`. Widening only
the *bound* there is not enough, because the JIT also computes the arithmetic itself at f32 while
Java computes it at f64: for a value-preserving operand (`+ 0.0`, `* 1.0`) the two agree and
widening the bound fixes the comparison, but for `f - 0.1` the f32 and f64 sums already differ, so
widening the bound alone moves the divergence rather than removing it. Fixing that shape means
promoting the whole subtree to f64, not just its bound.

Promoting the subtree was attempted and reverted. Marking every constant operand of such a node does
move the arithmetic to f64, but three things have to move with it and none of them is local: an `IN`
key needs its ELEMENTS widened too, which `markNarrowConstCmpWidenNode`'s IN branch does not do for
an F8 key; a constant sub-expression operand (`f * (1.0 / 3.0)`) is not a constant by
`isWideLaneNumericConstant`, so the bound widens while the arithmetic does not; and `requiresWideLane`
has to be extended as well, or an exactly-representable bound drops the predicate out of the
vectorized loop entirely.

### Constant reassociation

`ExpressionNode.reassociateConstants` regroups a constant pair only when
`isReassociationSafe`. Integer pairs are excluded, because an intermediate can land on a NULL
sentinel. A **quoted** literal counts as widening for this purpose: overload resolution still
casts `'02'` to a number, so `l * '02' * 4` is integer arithmetic and regrouping it would
change the result exactly as `l * 2 * 4` would.

Concatenation is the one operator that escapes all of it. `isReassociationSafe` short-circuits
to `true` for `||`, because `concat(V)` renders each operand through that operand's own type
adapter and appends the characters to a sink — no operand's rendering depends on its
neighbours, and no overload resolution turns one into a number. `(A || B) || C` and
`A || (B || C)` therefore emit the same characters for every operand type. Without that
short-circuit the quoted-literal widening mark alone disables `||` regrouping outright, since
a concatenated constant is almost always a quoted string.

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
| `CompiledFilterIRSerializer.java` | `markNarrowConstCmpWidenPair`, `maybeWidenCmpConstOperand`, `narrowKeptConstants`, `isFloatLeaf` — the JIT's surviving constant-width compensations |
| `IntWidthWrapTest.java` (test) | The spelling matrix for the wrap-always rule, and the #4752 cost |
| `ExpressionNode.java` (griffin/model) | `cacheConstantFold()` / `isReassociationSafe()` — the constant-reassociation guard |
