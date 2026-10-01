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
| `MERGE` | O3 rows interleave with an existing row group (possibly a coalesced run of groups sharing a boundary timestamp) | `mergeRowGroup()` | yes — **dedup-aware** |
| `COPY_ROW_GROUP_SLICE` | a row group with no O3 overlap | `ParquetRowGroupMaterializer.materialize()` if `isRewrite`, else `copyRowGroupWithNullColumns()` | yes, when rewriting |
| `COPY_O3` | O3 rows in a gap between/around row groups | fresh row group from O3 source buffers | no (O3 data is already target-typed) |
| `DROP` | a row group fully inside a replace-commit range that brings no rows for it | update mode: `PartitionUpdater.removeRowGroup()` (the next footer omits the group, its bytes become unused); rewrite mode: not copied | no |

**Rewrite vs in-place update.** A partition is rewritten to a new `txn`-named directory
(rather than appended in place) when `isRewrite` is true:

```
isRewrite = hasSchemaChange            // missing / extra / type-converted columns
         || forceFullReencode          // legacy Required no-sentinel column present
         || rowGroupCount == 1         // any merge replaces the only row group
         || hasCoalescableTie          // a boundary-straddling timestamp run
         || projectedUnusedBytes/parquetSize > ratio  // too many dead bytes, after this commit
         || projectedUnusedBytes > maxBytes;
hasSchemaChange = hasMissingColumns || hasExtraColumns || hasTypeConvertedColumns;
projectedUnusedBytes = unusedBytes     // already dead (_pm footer)
         + sum(_pm compressed size of every row group a MERGE or DROP action consumes);
```

`ratio` / `maxBytes` are `cairo.partition.encoder.parquet.o3.rewrite.unused.ratio` (0.5) /
`.max.bytes` (1 GiB). The dead-bytes gate looks one commit ahead: an in-place MERGE appends
the re-encoded row group and leaves the old one as dead bytes, so a commit that re-encodes
most of the file (a whole-partition replace, O3 or a dedup merge into most row groups) is
rewritten to one clean file instead of roughly doubling it in place. This applies to every
parquet O3 commit, not only replace. The estimate sums `ParquetMetaFileReader.getRowGroupCompressedSize()`
(TOTAL_COMPRESSED over the group's column chunks), O(actions x columns), after replace filter merges with no row in the window have
been downgraded to COPY. It is a lower bound of what update mode actually adds to
`unusedBytes`: it omits the old parquet footer and the replaced groups' column/offset
indexes. Both kinds of bytes lie inside the current file, so the ratio stays below 1.0;
tests that force in-place updates with ratio 1.0 and maxBytes `Long.MAX_VALUE` are unaffected.

**DROP in update mode.** `removeRowGroup(rg)` takes the original index, like `updateRowGroup`.
parquet2's `ParquetFile::end()` applies it in original index space after the replacements
and before the insertions, whose final positions (`metadataPosition`, which a DROP does not
advance) already exclude the dropped group. The group's data, bloom and page-index bytes
count as unused, exactly as a replaced group's do, and feed the dead-bytes gate above.
MVCC holds as for any in-place update: only a new footer is appended, so the new parquet
size is a unique token and a reader pinned at the old size still resolves the old footer,
which lists the dropped group. The `_pm` update is origin-driven (`RowGroupOrigin`), so a
surviving group keeps its committed block and bloom filter wherever it lands, and the `_pm`
row-group count may shrink. Removing and replacing the same group in one pass is an error.
Dropping every group without an insert never reaches the updater: it is a partition removal.

**Index MVCC in update mode.** Update mode rebuilds every index `isRebuiltParquetIndex()`
selects (bitmap `.k`/`.v`, posting `.pk`/`.pv`, and the covering `.pci`/`.pc` sidecars) for the
new row layout. The committed files must stay untouched: a reader pinned at the old `_txn` still
reads them, and a failure before `_txn` commits must leave them valid. So the rebuild never
reuses the committed name:
- `O3PartitionJob.updateParquetIndexes` writes each index under the new column name txn `txn`
  (the current txn, the same value rewrite mode names its directory with) next to the committed
  files in the live partition directory. A committed name is always below the current txn.
- `TableWriter.o3ConsumePartitionUpdateSink` (in-place branch) calls
  `versionRebuiltParquetIndexes`, which moves `_cv` to that name for the same columns (top 0) and
  queues the old name. `_cv` then commits with the new parquet file size in `_txn`.
- `resealParquetCoveringForPartition` (finishO3Commit) resolves the name from `_cv` after that,
  so it builds the covering sidecars of the new version too.
- `housekeep()` hands the old names to a `PurgingOperator` after the commit
  (`purgeSupersededParquetIndexes`): deleted at once with no reader below the committed txn,
  otherwise `ColumnPurgeJob` deletes them once no reader is in the old version's txn range.
- A rollback reloads `_cv` and drops the queued purge. A retry at the same txn re-initialises the
  uncommitted `*.<txn>` files. Otherwise nothing ever names them: if the failed txn is skipped
  (`ALTER TABLE ... RESUME WAL FROM TXN`) or the writer commits something else, they stay in the
  live partition directory as orphans until the partition is rewritten or dropped, or until
  `VACUUM TABLE` purges them (their version is neither the `_cv` name nor at or above the reader
  txn). They cost disk space only: readers resolve the name from `_cv`.

Rewrite mode builds the indexes in the new directory under the unchanged name, and needs none of
this. Keep the worker and the writer on the shared predicate: a column the worker rebuilds but
`_cv` does not move is read from the stale committed files, and a column `_cv` moves but the
worker did not build has no files. Pinned by `ParquetInPlaceIndexPinnedReaderTest`.

**`_cv` invariant.** After any parquet O3 publish (rewrite or in-place), every live column's
`_cv` top is 0. update.rs `end()` zeroes the file column tops in both modes, and TableWriter's
publish branches (rewrite, in-place update and fresh FORMAT PARQUET partition) call
`zeroColumnTopsAfterParquetRewrite` so the two agree. A new publish branch must do the same, or
`_cv` consumers (CONVERT TO NATIVE, dedup, symbol index) treat present values as absent. The
in-place branch also moves every rebuilt index column to the new column name txn
(`versionRebuiltParquetIndexes`, see Index MVCC above); a new in-place publish branch must do
that too, or readers keep opening the committed index files for the new row layout.

`hasTypeConvertedColumns` is set when a column maps into the parquet file through its
`getOriginalWriterIndex()` (the `replacingIndex` chain head) but its current writer index
differs — i.e. it went through `ALTER COLUMN TYPE`. So a pending conversion always forces a
rewrite; the in-place `copyRowGroupWithNullColumns()` path is taken only when the schema is
unchanged.

Both `mergeRowGroup()` and `ParquetRowGroupMaterializer.materialize()` share the same
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

Runs **once per active column, unconditionally** (independent of dedup). It turns the decoded
row group into a target-typed *source* the merge/copy can consume, writing
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

## `mergeRowGroup` pipeline

```
build decode list  -> chooseDecodeType per column, track timestamp chunk index
decode row group   -> Rust into RowGroupBuffers (worker-owned, reused)
Phase 1a           -> prepareSourceColumn per column -> srcPtrs (+ nullBufs owns the
                      converted/null buffers).  RUNS BEFORE the dedup compare.
merge index        -> non-dedup: createMergeIndex
                      dedup:     malloc index, build dedup-compare addresses, mergeDedup, realloc
identical check    -> dedup only: every O3 row a duplicate and every non-key value equal
                      -> return 0 output row groups, nothing written (see Deduplication)
deferred copies    -> rewrite mode: copy the leading row groups an identical commit skipped
even-split sizing  -> numChunks / maxChunkSize from (post-dedup) mergeRowCount
Phase 1b           -> grow destination buffers in mergeDstBufs (reused across MERGE actions)
Phase 2            -> O3CopyJob.mergeCopy into mergeDstBufs (allocates nothing) -> addRowGroup
finally            -> free nullBufs + timestampMergeIndexAddr (per row group)
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

| | Non-dedup | Dedup |
|---|---|---|
| call | `createMergeIndex` | `Unsafe.malloc` → build addresses → `Vect.mergeDedupTimestampWithLongIndexIntKeys` → `Unsafe.realloc` |
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

**The dedup-key column top is in row-group units, never the `_cv` top.** The comparer reads
the existing side with row-group-local indexes (`col_index >= column_top`, `dedup_comparers.h`),
so `mergeRowGroup` passes `rowGroupSize` for a key column missing from the file (all NULL, the
native `columnTop > mergeDataHi` rule, data pointer never read) and `0` for a decoded one (the
decoder writes each def-level-0 row as the type's NULL value, and `prepareSourceColumn`
NULL-fills an all-NULL chunk, so `srcPtrs` holds every row; for BYTE/SHORT/CHAR/BOOLEAN, which
have no NULL sentinel, that value is 0, which is also the comparer's null for those types). The partition-level `_cv` top is wrong here: a key
added after CONVERT while the partition was last has `top == partition rows`, not the row-group
size (this used to trip an `-ea` assert and suspend the table). Where tops are not zeroed after an O3
publish (master before the in-place `_cv` zeroing, or any future publish path that skips it), a key
written into the file can also keep a stale full-partition top that would read its real values as
NULL and dedup them away.

### Identical dedup commits

A dedup commit that only re-sends rows the partition already holds (every O3 row duplicates an
existing row, and every non-key value is equal) leaves the parquet partition untouched, as the
native merge's `TableWriter.checkDedupCommitIdenticalToPartition` does for native partitions.

- **Per row group, inside `mergeRowGroup`.** After the dedup merge index is built, and only
  when `duplicateCount == o3RowCount` (the dedup merge emits one entry per existing row, so
  the counts match only when every O3 row replaced one), `isMergeIdenticalToRowGroup`
  compares each non-key column through the merge index with
  `FrameAlgebra.isColumnMergeIdentical`. That is the same native comparer the native check
  uses (fixed, VARCHAR, STRING, BINARY, ARRAY; NULLs included). It reads the Phase 1a
  prepared buffers, so it sees converted types and the NULL buffer of a column the file
  lacks, and it stops at the first difference. If every column is identical, `mergeRowGroup`
  returns 0 output row groups before Phase 1b: no encode, no write. The caller keeps the
  group(s) as a COPY would.
- **Cost when not identical.** Zero decode or allocation beyond the normal merge. The count
  test costs one comparison. When every O3 row is a duplicate but some value differs (an
  upsert), the comparison scans columns until the first differing value: at most one
  in-memory pass over the non-key columns of that row group, far below the encode it
  precedes.
- **Partition no-op.** When no action changed anything (every MERGE identical, no
  `COPY_O3`), `processParquetPartition` closes the updater without `end()`, so neither the
  file nor `_pm` changes, and publishes the native "deduplication resulted in noop" sink
  (`timestampMin = Long.MAX_VALUE`, `newSize = -1`, not mutated). Name txn, file size and
  unused bytes stay the same.
- **Rewrite mode** (single row group, dead-bytes gate, schema change, tie) defers
  the leading row groups' copies while nothing has changed (`pendingCopyRowGroups`). An
  identical commit then removes the new txn directory (`removePhantomPartitionDir`) without
  writing any of them. The first change writes them first: `copyPendingRowGroups` runs
  inside `mergeRowGroup` just before Phase 2, or before a `COPY_O3`. Deferral is off when
  copies must be materialized (`hasTypeConvertedColumns`, `forceFullReencode`): the
  materializer decodes through the worker buffers a running merge still reads. Those files
  copy eagerly, and an identical commit still abandons the rewrite, after the wasted copies.
- Only for dedup, non-replace commits (`isSkipIdenticalMerge`). Replace and plain O3 are
  unchanged.

`ConvertOperatorImpl` has **no dedup-key pre-pass** — the merge path above handles a dedup-key
column whose conversion crosses the fixed↔var/symbol boundary while the partition stays lazy
parquet, so enabling dedup or altering a dedup key never eagerly rewrites partitions.

## Replace commits

A replace commit (`WAL_DEDUP_MODE_REPLACE_RANGE`) passes its per-partition window
`[replaceLo, replaceHi]` to `computeMergeActions`. A row group the window intersects
without bringing O3 rows becomes a filter-only `MERGE` (empty O3 slice, `o3Hi < o3Lo`)
or, when fully covered, a `DROP`. `mergeRowGroup` builds its index with
`createReplaceMergeIndex` (existing rows before the window, O3 rows, existing rows after
it) instead of `createMergeIndex`/dedup; replace and dedup never combine. Before any
full decode, `processParquetPartition` decodes only the timestamp column of each
filter-only `MERGE` (`hasRowInReplaceRange`); when no row lies in `[replaceLo, replaceHi]`
(e.g. the window falls strictly between two rows) the action becomes a
`COPY_ROW_GROUP_SLICE`, so the row group is not re-encoded. A window that misses every
row with no O3 rows publishes a no-op (file size and name txn unchanged); one that drops
every row group with no O3 rows publishes a size-0 removal. Neither writes a file.

## Native-memory allocation and lifetime

| Buffer | Holds | Lifetime | Freed |
|---|---|---|---|
| `RowGroupBuffers` | Rust decode output | per-worker, reused | context close |
| `nullBufs` | **owned** conversion / null buffers (Phase 1a) | **per row group** | `mergeRowGroup` / rewrite `finally` |
| `tmpBufs` | owned conversion / null buffers (rewrite path) | per row group | rewrite `finally` |
| `timestampMergeIndexAddr` | merge index | per row group | `mergeRowGroup` `finally` (guarded `!= 0`) |
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
- **No `_cv` tops in `mergeRowGroup`.** Column tops there are row-group-local (see
  Deduplication). `tableWriter.getColumnTop(partitionTs, ...)` is a partition-level value.
- **One conversion site.** `chooseDecodeType` and `prepareSourceColumn` are
  shared by the merge and rewrite paths. Fix bugs in the helper, not in one caller.
- **`var_data_len` is a debug-assert bound**, so a tight `getDataVectorSizeAt` extent is fine;
  do not pass a stale `getChunkDataSize` for a converted buffer.
- **No conversion buffers on the native O3 path** — only parquet partitions with a lazy ALTER
  allocate them.

## Key Files

| File | Role |
|------|------|
| `O3PartitionJob.java` | `processParquetPartition` (action dispatch / rewrite decision), `mergeRowGroup` (dedup-aware merge), `createMergeIndex` |
| `ParquetColumnTypeConverter.java` | shared decode-type selection, source preparation, and `convert*` / `estimate*` helpers |
| `ParquetConversionContext.java` | reusable worker-local decode/conversion scratch and native-resource lifecycle |
| `ParquetRowGroupMaterializer.java` | shared decode -> convert -> `PartitionUpdater.addRowGroup` pipeline for O3 and cold rewrites |
| `O3ParquetMergeStrategy.java` | `computeMergeActions`, the `MergeAction` types (`MERGE` / `COPY_ROW_GROUP_SLICE` / `COPY_O3` / `DROP`) |
| `O3ParquetMergeContext.java` | O3-specific extension of `ParquetConversionContext` (`getNullBufs`, `getMergeDstBufs`, merge ranges) |
| `DedupColumnCommitAddresses.java` | the dedup-compare address sink read by `Vect.mergeDedupTimestampWithLongIndexIntKeys` |
| `O3CopyJob.java` | `mergeCopy` — consumes prepared source + merge index into destination buffers |
| `TableWriter.java` | `isCommitDedupMode`, `getDedupCommitAddresses`, `convertPartitionParquetToNative`, `getParquetColumnType`, `TIMESTAMP_MERGE_ENTRY_BYTES` |
| `ConvertOperatorImpl.java` (griffin) | eager `ALTER COLUMN TYPE` for native partitions + the parquet→native pre-pass (target SYMBOL / chained mismatch only) |
| `row_groups.rs` / `decode.rs` (rust) | physical decode and `post_convert` (fixed→fixed scaling, boolean expansion) |
