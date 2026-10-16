# Writing Parquet Partitions with Pending Column Conversions

How O3 (out-of-order) writes land on a **parquet** partition that carries a *lazy*
`ALTER COLUMN TYPE`, and the column conversions and native-memory allocations involved —
with and without deduplication.

This is the **write-path** counterpart to `griffin/CLAUDE.md`, which owns the conversion
*semantics* (per-type cast rules, null sentinels, the `replacingIndex` chain) and the
*read* path (`PageFrameMemoryPool` / `PageFrameMemoryRecord`). Read that first for the
"what each cast does" rules; this file covers "how the writer materialises them during O3".

## When this happens

A parquet partition stores each column in the type it had when the partition was converted
to parquet. A later `ALTER COLUMN TYPE` is **lazy** — `ConvertOperatorImpl` does *not*
re-encode parquet (it only re-encodes native partitions, and runs a pre-pass for the two
cases lazy decode cannot handle: target SYMBOL, or a chained conversion with a type
mismatch — see `griffin/CLAUDE.md`). So the parquet file keeps the *source* type, and the
conversion is deferred until something reads or rewrites the partition.

When O3 rows fall inside such a partition's time range, `O3PartitionJob` must merge them
into the parquet data, which forces the lazy conversion to materialise **at write time**.

There is **no conversion when O3 lands on a NATIVE partition**: `ALTER COLUMN TYPE`
materialises eagerly into native column files, so a native O3 merge always sees
already-target-typed columns and allocates zero conversion buffers.

## The write paths (merge actions)

`O3ParquetMergeStrategy.computeMergeActions()` turns the overlap between the sorted O3 batch
and the existing row groups into a list of `MergeAction`s. `O3PartitionJob.processParquetPartition()`
executes them:

| Action | Meaning | Handler | Conversions? |
|--------|---------|---------|--------------|
| `MERGE` | O3 rows interleave with an existing row group (possibly a coalesced run of groups sharing a boundary timestamp) | `mergeRowGroupPrepare()` then `mergeRowGroupWrite()` | yes — **dedup-aware** |
| `COPY_ROW_GROUP_SLICE` | a row group with no O3 overlap | rewrite: `copyRowGroupToRewrite()` (`ParquetRowGroupMaterializer.materialize()` for type-converted columns or a legacy file, `copyRowGroupWithNullColumns()` after another schema change, a raw `copyRowGroup()` otherwise); update: nothing, the row group stays in place | yes, when rewriting with converted columns |
| `COPY_O3` | O3 rows in a gap between/around row groups | fresh row group from O3 source buffers | no (O3 data is already target-typed) |

**Rewrite vs in-place update.** A partition is rewritten to a new `txn`-named directory
(rather than appended in place) when `isRewrite` is true:

```
isRewrite = hasSchemaChange            // missing / extra / type-converted columns
         || forceFullReencode          // legacy Required no-sentinel column present
         || rowGroupCount == 1         // any merge replaces the only row group
         || hasCoalescableTie          // a boundary-straddling timestamp run
         || unusedBytes/parquetSize > ratio  // too many dead bytes
         || unusedBytes > maxBytes;
hasSchemaChange = hasMissingColumns || hasExtraColumns || hasTypeConvertedColumns;
```

`hasTypeConvertedColumns` is set when a column maps into the parquet file through its
`getOriginalWriterIndex()` (the `replacingIndex` chain head) but its current writer index
differs — i.e. it went through `ALTER COLUMN TYPE`. So a pending conversion always forces a
rewrite; the in-place `copyRowGroupWithNullColumns()` path is taken only when the schema is
unchanged.

Both the row group merge and `ParquetRowGroupMaterializer.materialize()` share the same
`ParquetColumnTypeConverter` decode-selection and source-preparation methods. The materializer
is also the parquet-to-parquet rewrite path used by cold storage.

## Decode-type selection (`ParquetColumnTypeConverter.chooseDecodeType`)

Rust cannot produce every target representation directly, so the writer asks it to decode
into a type it *can* produce, then Java finishes the cast. Per column:

| source (parquet) → target (current) | Rust decodes as | finished by |
|---|---|---|
| fixed → same/other fixed | target fixed type | Rust (`post_convert`, widening/scaling) |
| var → same/other var | target var type | Rust physical decode (+ Java UTF transcode) |
| **fixed → var** (INT→VARCHAR) | **source fixed type** (no aux) | Java `convertFixedColumnTo{Varchar,String}` |
| **var → fixed** (VARCHAR→LONG) | **`VARCHAR_SLICE`** (or source var type for STRING) | Java `convertVarColumnToFixed` |
| **symbol → fixed** (SYMBOL→LONG) | **`VARCHAR_SLICE`** | Java `convertVarColumnToFixed` (srcType remapped to VARCHAR) |
| **symbol → var** (SYMBOL→VARCHAR) | **native VARCHAR/STRING** (not `VARCHAR_SLICE`) | nothing — pass-through |

`VARCHAR_SLICE` aux entries set bit 0 of the header, which the native VARCHAR reader would
misread as `HEADER_FLAG_INLINED`; that is why symbol→var must decode as native VARCHAR while
symbol→fixed must decode as `VARCHAR_SLICE` (its reader expects the 16-byte slice layout:
4-byte header + absolute data pointer at offset 8).

## Per-column source preparation (`ParquetColumnTypeConverter.prepareSourceColumn`)

Runs **once per active column** of a row group that the merge writes. In SKIP mode, a merge whose O3
rows the dedup all drops prepares only the designated timestamp and the dedup key columns. It turns
the decoded row group into a target-typed *source* the merge/copy can consume, writing
`outPtrs[slot4..+3] = {dataPtr, dataSize, auxPtr, auxSize}` and recording any buffer it
allocates in the caller's free-list `ownedBufs[slot4..+3]` (`slot4 = ai*4`).

| Column category | Allocates (using the owning context's memory tag) |
|---|---|
| fixed, no type change | **nothing** — points into the Rust decode buffer |
| var, no change / symbol→var | **nothing** — pass-through to the decode buffer |
| fixed→var, **VARCHAR** | aux `getAuxVectorSize(rows)` (= `rows*16`) + data `estimateVarcharDataSize(src, rows)` — *no* data buffer for BOOLEAN/BYTE/SHORT/CHAR sources (estimate is 0; values inline) |
| fixed→var, **STRING** | aux `(rows+1)*8` + data `estimateStringDataSize(src, rows)` (always > 0: 4-byte length prefix per row) |
| var→fixed / symbol→fixed | fixBuf `rows * sizeOf(target)` |
| column-top / missing, var | nullAux `getAuxVectorSize(rows)` + (if non-empty) nullData |
| column-top / missing, fixed | nullFix `rows * sizeOf(target)` |

The actual per-row transform is done by `convertFixedColumnToVarchar` /
`convertFixedColumnToString` / `convertVarColumnToFixed`, using the reusable
`Utf8StringSink` / `StringSink` / `Decimal*` scratch in `ParquetConversionContext` (zero per-row
GC). These converters must obey the same cast/parse/null rules as the native ALTER path and
the read path — see the Native/Parquet contract in `griffin/CLAUDE.md`.

`ownedBufs` is a flat `[addr, size, addr, size]` list; the free loops walk it as
**order-agnostic stride-2 `(addr,size)` pairs**, so the slot a buffer lands in does not encode
data-vs-aux semantics — do not rely on it.

## Row group merge pipeline

A `MERGE` runs in two stages, and `processParquetPartition()` sits between them:

```
mergeRowGroupPrepare (stage 1)
  build decode list  -> chooseDecodeType per column, track timestamp chunk index
                        (SKIP mode: the designated timestamp and the dedup keys only)
  decode row group   -> Rust into RowGroupBuffers (worker-owned, reused; SKIP mode:
                        the context's separate keyRowGroupBuffers)
  Phase 1a           -> prepareSourceColumn per decoded column -> srcPtrs (+ nullBufs owns the
                        converted/null buffers).  RUNS BEFORE the dedup compare.
  merge index        -> non-dedup: Vect.mergeTwoLongIndexesAsc
                        dedup:     malloc index, build dedup-compare addresses, mergeDedup, realloc
                     -> returns the number of O3 rows the dedup dropped
processParquetPartition
  SKIP mode, all O3 rows dropped -> the row groups stay as they are, no stage 2
  otherwise                      -> opens the output if it is not open yet (see Deduplication)
mergeRowGroupWrite (stage 2)
  SKIP mode          -> decode and Phase 1a for the columns that stage 1 left out
  even-split sizing  -> numChunks / maxChunkSize from (post-dedup) mergeRowCount
  Phase 1b           -> grow destination buffers in mergeDstBufs (reused across MERGE actions)
  Phase 2            -> O3CopyJob.mergeCopy into mergeDstBufs (allocates nothing) -> addRowGroup
finally (caller)     -> O3ParquetMergeContext.freeRowGroupMerge(): nullBufs + merge index
```

The critical ordering: **Phase 1a must precede the dedup compare.** The native dedup comparer
reads the source pointers directly; if it saw the raw cross-typed decode buffer it would
misread a fixed→var key's dangling/empty aux (SIGSEGV) or read a var/symbol→fixed key's
`VARCHAR_SLICE` bytes as fixed values (silent wrong dedup). Phase 1a, the dedup compare, the
Phase 1b sizing and the Phase 2 copy all read the *same* prepared pointers, so each crossing
conversion runs exactly once per row group.

## Deduplication

Dedup mode is gated by `tableWriter.isCommitDedupMode()`. It changes **only** the merge-index
step; the column conversions (Phase 1a), destination sizing (Phase 1b) and copy (Phase 2) are
byte-for-byte identical with and without dedup.

The table's dedup mode (`_meta` header, `TableUtils.DEDUP_MODE_*`) decides which row of a
duplicate group survives. UPSERT keeps the latest write, SKIP keeps the earliest one and drops
the rest. `tableWriter.isCommitDedupSkipMode()` passes the choice to every native dedup kernel
as `isSkipMode`: the sort-dedup of a commit or a WAL block (`Vect.dedupSortedTimestampIndex*`)
and the merge with partition rows (`Vect.mergeDedupTimestampWithLongIndex*`), for native and
parquet partitions alike. Both modes rely on the same invariant: equal-key rows reach the kernels
in commit order (stable sorts, lag rows before new rows, partition rows before O3 rows).

In SKIP mode the merge kernels emit every existing row once, in order, and an O3 row only when no
existing row matches it, and the O3 rows are unique on the keys after the commit's sort-dedup. So
a merge whose O3 rows are all duplicates reproduces the existing rows as they are:

- **Native partition:** `O3PartitionJob` skips the `checkDedupCommitIdenticalToPartition` column
  comparison and reports the partition as unchanged (`deduplication resulted in noop`).
- **Parquet partition:** stage 1 of a `MERGE` decodes only the designated timestamp and the dedup
  keys. When the dedup drops all O3 rows of the action, the row group stays in place in an
  in-place update, or the rewrite copies it like a `COPY_ROW_GROUP_SLICE`, with no stage 2.
  `processParquetPartition()` opens its output (the new `txn` directory of a rewrite, or the
  `data.parquet` and `_pm` of an update) at the first action that writes rows: a `MERGE` that
  keeps an O3 row, or a `COPY_O3`. On that first write a rewrite copies the row groups of the
  actions before it, in order. When no action writes, the partition stays as it is, and the
  partition sink reports it as unchanged as the native path does (`updatePartitionSink(...,
  newPartitionSize = -1, partitionMutates = 0)`). Other commits open the output upfront.

UPSERT takes the same shortcut on parquet partitions only for tables with a live BOOLEAN, BYTE or
SHORT column, the types whose column tops a merge would turn into 0/false (native turns an
identical replay into a noop or an append, which keeps every top). For those tables
`isUpsertIdenticalUpToPartitionMax()` runs before the output opens: per `MERGE` up to the
partition max it decodes the keys and dedups in UPSERT mode (`isKeyColumnsOnly = true`,
`isSkipMode = false`), stops unless every O3 row at or below the max is a duplicate, then decodes
the other columns and compares them with `FrameAlgebra.isColumnMergeIdentical()`, the native
comparer of `checkDedupCommitIdenticalToPartition`. A `COPY_O3` at or below the max ends the
check. When every compared row is identical, the commit runs in SKIP mode. Other tables, and
UPSERT commits that change a value, merge and write the row group even when the O3 rows carry
the values the partition has. The caller of `mergeRowGroupPrepare()` adds the duplicates to the
dedup counter, so the check does not count them twice.

| | Non-dedup | Dedup |
|---|---|---|
| call | `Vect.mergeTwoLongIndexesAsc` | `reallocMergeIndex` (malloc) → build addresses → `Vect.mergeDedupTimestampWithLongIndexIntKeys` → `reallocMergeIndex` (realloc) |
| index alloc | `malloc(mergeRowCount * 16)` | `malloc(mergeRowCount * 16)` then **realloc down** to `dedupRows * 16` |
| per-column work | none | build a dedup-compare address per non-timestamp dedup key |

(`TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2 = 16` — the (timestamp, rowId) pair.)

**The dedup-compare address build allocates no per-merge native memory.** For each
non-timestamp dedup-key column it writes the prepared source pointers (`srcPtrs`), the O3
pointers and the column top into `dedupColSinkAddr` — a `DedupColumnCommitAddresses` block the
`TableWriter` allocates **once per partition** (a `PagedDirectLongList`) and **reuses across
every merge action**. `setColValues` / `setColAddressValues` / `setO3DataAddressValues` are
pure `Unsafe.put*`; `clear()` is a `memset`. So dedup adds nothing beyond the merge index, and
because `dedupRows ≤ mergeRowCount` it tends to *shrink* the downstream footprint (smaller
`maxChunkSize` → smaller Phase 1b destination buffers).

For a var dedup key, the comparer needs a data-length bound; the writer computes it as
`getDataVectorSizeAt(srcAux, rows-1)` (the exact extent from the aux vector's last entry — the
same value Phase 1b uses), which is correct for both a converted buffer and a raw decode. The
native comparer only reads `var_data_len` inside debug `assert`s.

`ConvertOperatorImpl` has **no dedup-key pre-pass** — the merge path above handles a dedup-key
column whose conversion crosses the fixed↔var/symbol boundary while the partition stays lazy
parquet, so enabling dedup or altering a dedup key never eagerly rewrites partitions.

## Native-memory allocation and lifetime

| Buffer | Holds | Lifetime | Freed |
|---|---|---|---|
| `RowGroupBuffers` | Rust decode output | per-worker, reused | context close |
| `keyRowGroupBuffers` | SKIP stage-1 decode output (timestamp + dedup keys) | per-worker, reused | context close |
| `nullBufs` | **owned** conversion / null buffers (Phase 1a) | **per row group** | `O3ParquetMergeContext.freeRowGroupMerge()` in the caller's `finally` |
| `tmpBufs` | owned conversion / null buffers (rewrite path) | per row group | rewrite `finally` |
| merge index (`O3ParquetMergeContext.reallocMergeIndex`) | merge index | per row group, owned by the context | `freeRowGroupMerge()` (guarded `!= 0`) |
| `mergeDstBufs` | merge-copy destinations | **reused across MERGE actions** | caller, after the action loop |
| `dedupColSinkAddr` | dedup-compare address sink | per partition | `TableWriter` (`dedupColumnCommitAddresses`) |
| `srcPtrs`, `convertedPtrs` | **pointer copies** (not owned) | per-worker container; pointers valid per row group | nulled at `close` — never `freeNativePairs` |

`ParquetConversionContext` owns common decode/conversion scratch, while
`O3ParquetMergeContext` adds O3-only lists (`getNullBufs`, `getMergeDstBufs`, `getSrcPtrs`).
Each list is zero-filled per use. On abnormal worker shutdown `close()` walks the
**owning** lists (`mergeDstBufs`, `nullBufs`, `tmpBufs`)
with `freeNativePairs`; the pointer-copy lists (`srcPtrs`, `convertedPtrs`) are just dropped.

## Gotchas

- **Conversion before compare.** Any future code that reads dedup-key parquet data for a
  native compare must run after `prepareSourceColumn`. Moving the dedup compare ahead
  of Phase 1a reintroduces the SIGSEGV / silent-corruption bug.
- **One conversion site.** `chooseDecodeType` and `prepareSourceColumn` are
  shared by the merge and rewrite paths. Fix bugs in the helper, not in one caller.
- **Merge state survives a materializer run.** In SKIP mode a rewrite can copy the row groups before
  a `MERGE` through `ParquetRowGroupMaterializer` between the two stages of that merge. The
  materializer decodes into the shared `ParquetConversionContext` scratch (`getRowGroupBuffers`,
  the active-column lists, `convertedPtrs`, `tmpBufs`). So stage 1 keeps its state outside of it:
  `keyRowGroupBuffers`, the merge's own active-column lists, `srcPtrs`, `nullBufs` and the merge
  index. Stage 2 must not re-zero `nullBufs` or `srcPtrs`: it would drop the key columns' buffers.
- **`var_data_len` is a debug-assert bound**, so a tight `getDataVectorSizeAt` extent is fine;
  do not pass a stale `getChunkDataSize` for a converted buffer.
- **`dedup_column` is a packed struct.** Its `null_value` (read for rows in a column top) sits at
  an unaligned offset. Read it through `get_null_value<T>()`, which copies it out: dereferencing
  it in place as a 16-byte `T` (`__int128`, `int256`) lets the compiler emit aligned SIMD loads,
  which fault (seen with GCC 16 at `-O3`).
- **No conversion buffers on the native O3 path** — only parquet partitions with a lazy ALTER
  allocate them.

## Key Files

| File | Role |
|------|------|
| `O3PartitionJob.java` | `processParquetPartition` (action dispatch / rewrite decision / output opening), `mergeRowGroupPrepare` + `mergeRowGroupWrite` (dedup-aware merge), `copyRowGroupToRewrite`, `createMergeIndex` |
| `ParquetColumnTypeConverter.java` | shared decode-type selection, source preparation, and `convert*` / `estimate*` helpers |
| `ParquetConversionContext.java` | reusable worker-local decode/conversion scratch and native-resource lifecycle |
| `ParquetRowGroupMaterializer.java` | shared decode -> convert -> `PartitionUpdater.addRowGroup` pipeline for O3 and cold rewrites |
| `O3ParquetMergeStrategy.java` | `computeMergeActions`, the `MergeAction` types (`MERGE` / `COPY_ROW_GROUP_SLICE` / `COPY_O3`) |
| `O3ParquetMergeContext.java` | O3-specific extension of `ParquetConversionContext` (`getNullBufs`, `getMergeDstBufs`, merge ranges) |
| `DedupColumnCommitAddresses.java` | the dedup-compare address sink read by `Vect.mergeDedupTimestampWithLongIndexIntKeys` |
| `O3CopyJob.java` | `mergeCopy` — consumes prepared source + merge index into destination buffers |
| `TableWriter.java` | `isCommitDedupMode`, `getDedupCommitAddresses`, `convertPartitionParquetToNative`, `getParquetColumnType`, `TIMESTAMP_MERGE_ENTRY_BYTES` |
| `ConvertOperatorImpl.java` (griffin) | eager `ALTER COLUMN TYPE` for native partitions + the parquet→native pre-pass (target SYMBOL / chained mismatch only) |
| `row_groups.rs` / `decode.rs` (rust) | physical decode and `post_convert` (fixed→fixed scaling, boolean expansion) |
