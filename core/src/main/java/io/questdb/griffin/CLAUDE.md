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

An INT-typed *expression* can carry a value its 4 bytes cannot hold. `2000000000 + 2000000000`
is an INT expression whose `getInt()` wraps to `-294967296` and whose `getLong()` keeps
`4000000000`. Both answers are wanted: the wrapped one so that the literal, column and
bind-variable spellings of the same arithmetic agree, and the wide one so that an explicit
widening cast is not silently truncated.

That makes width a per-expression property rather than a property of the type, and the rule
that keeps it coherent is:

> **A read at 64-bit width returns the same value whether it is spelled as an explicit cast,
> an implicit promotion, or a store into a 64-bit column.**

### `Function#isIntWidthStable()`

The carrier of that property. It answers "does `getLong()` equal `getInt()` widened?" and
defaults to the conservative `false`.

| Reports | Who |
|---|---|
| `true` | `IntFunction` and everything built on it — columns, constants, casts, bind variables, memoizers |
| `false` | `LongWidthIntFunction` subclasses: the INT arithmetic and bitwise operators (`+ - * / %`, `& \| ^ ~`, unary minus, `abs`), and the conditionals that forward one of their branches (`CASE`, `COALESCE`, `NULLIF`) |
| `false` | a function implementing `Function` directly and deriving its two widths independently (`json_extract`) |

`ColumnTypes` and `RecordCursorFactory` expose the same question as
`isColumnIntWidthStable(int)`, so a consumer that holds a cursor rather than a function can
ask it. Both default to `true`.

### Which reads widen

| Read | Width | Why |
|---|---|---|
| `::LONG`, `::TIMESTAMP`, `::DATE` | long | the three 64-bit targets; `IntFunction.getLong/getTimestamp/getDate` all delegate to `getLong()` |
| implicit store into a LONG / TIMESTAMP / DATE column | long | must match the cast above |
| `::DOUBLE`, `::FLOAT`, `::DECIMAL*` | int | their `IntFunction` counterparts read `getInt()`, and QuestDB inserts no cast function for implicit numeric promotion, so `(i*j) + 0.0` wraps and the cast must agree |
| plain INT projection | int | the value a user sees for an INT column |

Adding a widening read means changing **three** places together: the `CastIntTo*` factory, the
matching `IntFunction` accessor, and `RecordToRowCopierUtils.widensIntSource`. Change one alone
and the store stops matching the cast.

### The store path

`RecordToRowCopierUtils` (two bytecode generators) and `LoopingRecordToRowCopier` read an INT
source with `getInt()` unless `ColumnTypes.isColumnIntWidthStable(i)` says otherwise. The flag
cannot be inferred from the type: a real stored INT column has only 4 bytes, and
`PageFrameMemoryRecord.getLong()` would read 8 bytes at `rowIndex << 3`. Only a function-backed
source can answer `false`.

- **INSERT ... VALUES** — the `VirtualRecord` is passed as the copier's `ColumnTypes`, and it
  answers from its own functions.
- **INSERT ... SELECT** and **CTAS** — `FactoryColumnTypes` pairs the cursor metadata with the
  factory's `isColumnIntWidthStable`.

A factory answers `false` only if its cursor hands the base record through — re-positioning it
by row id counts. Delegating today: limit, filter, column selection, light sort / top-K,
latest by, query progress, stale view check, the **master columns of a join** (`JoinRecord`
hands the master record straight through; the master is never value-materialised) — including
the **master columns of the serial and fast window joins** (`WindowJoinRecordCursorFactory`,
`WindowJoinFastRecordCursorFactory`), which delegate master columns to the master factory just like
`AbstractJoinRecordCursorFactory` and keep the default on the window-aggregate columns — the **base
columns of an extra-null-column pad**, **UNION ALL** and **UNION distinct**
(`UnionRecordCursorFactory`) — live leg pass-throughs (`UnionRecord`/`UnionCastRecord` delegate
`getInt`/`getLong` to the active leg) that delegate a column only when *both* legs are
width-unstable, because if either leg is a real INT column its `getLong()` would over-read the
4-byte slot, forcing the whole column to INT width — the **leg-A columns of EXCEPT / INTERSECT
and their ALL variants**, which emit only leg A's live record (`getRecord()` returns
`cursorA.getRecord()`, or — when a sibling column forces a cast — a `UnionCastRecord` pinned to leg A;
the maps hold only dedup/membership keys either way) so the answer is leg A's. Those set ops carry the
same cast-path caveat as UNION ALL: on the cast path an INT-to-INT column goes through
`IntColumn.getLong()`, which re-wraps, so a column reported unstable still stores the wrapped value
there — safe, never an over-read, but it makes the stored value depend on an unrelated sibling.
Also delegating: the
**base columns of the cached-window LIGHT factory** (`CachedWindowLightRecordCursorFactory`, where
`WindowLightRecord` reads a base column from the live base cursor via a non-negative `sourceMap`
code), and **`DistinctTimeSeries`**, whose cursor hands the base record straight through and uses
its `dataMap` only to detect adjacent duplicates, never to materialise the returned value.
Keeping the default `true`: full sort, group by — including the markout **HORIZON JOIN** family
(`HorizonJoin{,NotKeyed}RecordCursorFactory`, `MultiHorizonJoin{,NotKeyed}RecordCursorFactory`),
whose emitted record is a `VirtualRecord` over the aggregation map / `MapValue`, so the live join
record is only the aggregation *input* and every output column is a materialised map slot — the
async window/horizon joins (their master is a stored-column `PageFrameMemoryRecord`),
**distinct-over-map** (`DistinctRecordCursorFactory`, whose cursor copies each row into a 4-byte
map slot — not to be confused with the live `DistinctTimeSeriesRecordCursorFactory` above), the
**slave columns of a value-materialised join**, the **aggregate columns of a window join** and the
**narrow-chain (window-function) columns of the cached-window LIGHT factory** — all map- or
chain-backed, where the value has already been copied into a 4-byte slot, so the wrap has genuinely
happened and reading at long width would over-read. A per-column hybrid record
(`SortKeyMaterializing`, `CachedWindowLight`) must answer per column: `CachedWindowLight` does, via
`sourceMap` (base columns delegate, window columns keep the default); `SortKeyMaterializing` keeps
the default. (`DistinctTimeSeries` itself is reachable only with the distinct-to-GROUP BY rewrite
disabled — a test-only seam — since a running server always rewrites plain `SELECT DISTINCT` to
GROUP BY; the delegation is a factory-consistency guarantee, not a production store path.)

### Reading a function at two widths

A caller that needs both `getInt()` and `getLong()` of the same expression on one row — the
conditionals, `InLongFunctionFactory`'s split key — must not simply call both: a second read of
a non-deterministic function is a fresh draw. Guard with `isIntWidthStable()` first (one read
suffices), then `isRowStable()` (two reads are safe), and otherwise read once at long width.

When that last arm moves a *comparison* to long width, it must move both operands. Reading one
side at 64 bits and the other with a wrapping `getInt()` misses an equal pair — `nullif` handed
back the very value it excludes for `nullif(<row-unstable>, a + b)`. Widening the other operand
costs nothing when it is width stable, since `IntFunction.getLong()` is
`Numbers.intToLong(getInt())`.

### An alias is a column reference, and it must be transparent

A projection that references a column by name does not pass the referenced function through — it
creates a column function. `IntColumn` overrides only `getInt(rec)` and inherits
`getLong() = Numbers.intToLong(getInt())` while reporting `isIntWidthStable() == true`, so it
throws the wide half away. `a::LONG` over `SELECT i + j AS a` re-wrapped while `(i + j)::LONG`
widened, and the *stored* value depended on plan shape: with no sibling column the outer projection
is elided and the copier sees the arithmetic function (widens), with one it sees the column
reference (wraps).

`IntWideColumn` is the transparent variant — it reads the record at whichever width the caller
asks for. `FunctionParser.createColumn` emits it in place of `IntColumn` when the metadata reports
the referenced column width-unstable, which is exactly the condition under which `getLong()` on an
INT column is legal. `PriorityMetadata` is the metadata that can answer: it snapshots the base
factory's answers per column at construction, and reads the projection's own function list for a
reference to an earlier column of the same projection.

### `isColumnRowStable` — the other half of the answer

A column function is a **proxy**, so it must report the referenced expression's row stability, not
its own. The consumers above choose between one long-width read and two INT-width reads on that
answer, and two reads of a non-deterministic expression are two different draws.

`RecordCursorFactory#isColumnRowStable(int)` and `ColumnTypes#isColumnRowStable(int)` carry it.
**The two methods are a pair: a factory that overrides `isColumnIntWidthStable` must override this
one too.** Their defaults point in opposite directions — `true` for width (don't widen, never
over-read) and `false` for row (don't read twice) — so answering only the first reports a column
width-unstable *and* row-unstable, which changes what `nullif` / `coalesce` / `IN` return for it.
That would make the value depend on whether a delegating wrapper sits between the projection and
its base, i.e. re-create the plan-shape dependence the width rule exists to remove.

Each override mirrors its width sibling through the identical index mapping. Two differences:

- where the width sibling returns the constant `true` for a **materialised** column (join slave,
  window aggregates, extra-null padding, the cached-window narrow chain), so does this one — reading
  stored bytes twice gives the same value;
- **UNION / UNION ALL combine with AND where width combines with OR.** Width needs *either* leg safe
  to read at long width; row stability needs *both*, because either leg can be the active one.
  EXCEPT / INTERSECT emit only leg A, so both methods read leg A.

`IntWidthAnswerPairingTest` is the forcing function: it walks every compiled class and fails when a
`RecordCursorFactory` declares one of the two without the other. A per-shape test can only cover the
factories someone thought of, which is how the width enumeration was missed in the first place.

### The JIT

`CompiledFilterIRSerializer` faces the same split, because the Java filter evaluates at the
width the function factories pick while the IR types operands by their column widths. Two
compensations exist, and both must run for *any* predicate shape, not only float-bearing ones:

- `markNarrowConstCmpWidenPair` / `maybeWidenCmpConstOperand` widen an out-of-INT-range constant
  compared against a narrow-int operand. The first handles a bare leaf, the second an
  arithmetic subtree such as `-i`.
- `markFloatCmpConst` sends a constant with no exact float to the filter at double width, since
  a FLOAT column always compares at DOUBLE width in Java. It runs for a bare FLOAT column and for
  an arithmetic subtree that stays F4-typed.

`isFloatLeaf` deliberately does NOT accept an F8-promoting subtree such as `f + 0.0`. Widening
only the *bound* there is not enough, because the JIT also computes the arithmetic itself at f32
while Java computes it at f64: for a value-preserving operand (`+ 0.0`, `* 1.0`) the two agree
and widening the bound fixes the comparison, but for `f - 0.1` the f32 and f64 sums already
differ, so widening the bound alone moves the divergence rather than removing it. Fixing that
shape means promoting the whole subtree to f64, not just its bound.

Promoting the subtree was attempted and reverted. Marking every constant operand of such a node
does move the arithmetic to f64, but three things have to move with it and none of them is local:
an INT literal operand is also claimed by `i64WrapLeaves`, which outranks the widen mark in
`serializeConstant` and emits an `IMM I4` that the four-lane backend cannot pair with an f64
(AVX2 `convert()` has no `f64 x i32` arm, so the scalar and vectorized backends disagree); an `IN`
key needs its ELEMENTS widened too, which `markNarrowConstCmpWidenNode`'s IN branch does not do for
an F8 key; and a constant sub-expression operand (`f * (1.0 / 3.0)`) is not a constant by
`isWideLaneNumericConstant`, so the bound widens while the arithmetic does not. A correct promotion
has to handle all three together, and extend `requiresWideLane` as well, or an exactly-representable
bound drops the predicate out of the vectorized loop entirely.

Missing either one does not merely lose rows — it can make the **scalar and vectorized backends
disagree with each other**, so the same query on the same data returns different rows depending
on whether the host has AVX2. `assertJitScalarAndVectorMatchJava` in
`CompiledFilterRegressionTest` runs a query with JIT off, then `FORCE_SCALAR`, then vectorized,
and is the guard for that class of bug.

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
| `Function.java` (cairo/sql) | `isIntWidthStable()` / `isRowStable()` — the INT-width contract |
| `LongWidthIntFunction.java` | Base for the INT functions whose `getLong()` computes at long width |
| `RecordToRowCopierUtils.java` | `widensIntSource()` and the two bytecode generators' INT source arm |
| `LoopingRecordToRowCopier.java` | `intWidthUnstableColumns` snapshot for the wide-table copier |
| `FactoryColumnTypes.java` | Pairs cursor metadata with a factory's `isColumnIntWidthStable` for INSERT ... SELECT / CTAS |
| `IntWideColumn.java` | The transparent INT column reference — reads the record at the caller's width |
| `PriorityMetadata.java` | Answers both width questions for a projection: base factory snapshot + the projection's own functions |
| `IntWidthAnswerPairingTest.java` (test) | Forcing function: every factory answering one width question must answer the other |
| `CompiledFilterIRSerializer.java` | `isFloatLeaf`, `maybeWidenCmpConstOperand`, `markNarrowConstCmpWidenPair` — the JIT's width compensations |
| `ExpressionNode.java` (griffin/model) | `cacheConstantFold()` / `isReassociationSafe()` — the constant-reassociation guard |
