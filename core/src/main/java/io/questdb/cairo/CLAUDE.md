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
| `DROP` | a row group fully inside a replace-commit range that brings no rows for it | update mode: `PartitionUpdater.removeRowGroup()` (the next footer omits the group, its bytes become unused); rewrite mode: not copied | no |

**Rewrite vs in-place update.** A partition is rewritten to a new `txn`-named directory
(rather than appended in place) when `isRewrite` is true:

```
isRewrite = isRewriteForced
         || projectedUnusedBytes/parquetSize > ratio  // too many dead bytes, after this commit,
         || projectedUnusedBytes > maxBytes;          // decided when the output opens
isRewriteForced = hasSchemaChange      // missing / extra / type-converted columns
         || forceFullReencode          // legacy Required no-sentinel column present
         || hasStaleColumnTops         // _cv tops a 9.3.4 - 10.0.1 in-place merge left stale
         || rowGroupCount == 1         // any merge replaces the only row group
         || hasCoalescableTie          // a boundary-straddling timestamp run
         || hasForeignParquetIndex      // a rebuilt index named at or above txn, from ATTACH of a copied directory
         || hasLiveIndexUnderColumnTop; // a 10.0.1-or-older index under a column top
hasSchemaChange = hasMissingColumns || hasExtraColumns || hasTypeConvertedColumns;
projectedUnusedBytes = unusedBytes     // already dead (_pm footer)
         + sum(_pm compressed size of every row group a DROP drops or a MERGE re-encodes);
```

`ratio` / `maxBytes` are `cairo.partition.encoder.parquet.o3.rewrite.unused.ratio` (0.5) /
`.max.bytes` (1 GiB). The dead-bytes gate looks one commit ahead: an in-place MERGE appends
the re-encoded row group and leaves the old one as dead bytes, so a commit that re-encodes
most of the file (a whole-partition replace, O3 or a dedup merge into most row groups) is
rewritten to one clean file instead of roughly doubling it in place. This applies to every
parquet O3 commit, not only replace. `isRewriteForced` is decided before the merge actions run;
the dead-bytes gate (`isDeadBytesOverLimit()`) when the output opens, once the commit knows which
MERGEs re-encode their row groups (`getProjectedDeadBytes()`). An output that opens upfront counts
every DROP and every MERGE but the identical ones the UPSERT identity check leaves as stored
(`O3ParquetMergeContext.isUnchangedMergeAction()`). A SKIP-mode commit (see Deduplication) decides
at its first writing action and counts that action alone: the actions before it wrote nothing, and
the dedup of the later MERGEs runs after the output opens. The first MERGE whose NULLs of a BOOLEAN,
BYTE or SHORT column may be a kept column top counts as stored too: the partition updater settles
it only after the open (`isKeptTopMergeUnchanged()`). The estimate sums
`ParquetMetaFileReader.getRowGroupCompressedSize()` (TOTAL_COMPRESSED over the group's column
chunks), O(actions x columns), after replace filter merges with no row in the window have been
downgraded to COPY. It is a lower bound of what update mode actually adds to `unusedBytes`: it
omits the old parquet footer, the replaced groups' column/offset indexes, and the MERGEs above that
turn out to re-encode their row groups. So a SKIP commit that writes into most row groups updates
the file in place, and the gate of a later commit, which counts the bytes it left dead, or the idle
compaction (`PartitionCompactionScanJob`) rewrites it. All these bytes lie inside the current file,
so the ratio stays below 1.0; tests that force in-place updates with ratio 1.0 and maxBytes
`Long.MAX_VALUE` are unaffected.

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
  files in the live partition directory. Every name the table creates is below the current txn,
  but ATTACH of a partition directory copied from another table pins the column name txns of its
  files (`attachPartitionPinColumnVersions`), which can be at or above it. Update mode never
  rebuilds such an index: `isForeignParquetIndex()` sends the partition to a rewrite. DROP INDEX
  and RENAME COLUMN never leave such a version behind in a parquet partition directory, whose files,
  purge or readers a later in-place commit at that txn would collide with: they move the partition
  to a new directory version instead (`TableWriter.relocateParquetPartition`, see
  `isForeignParquetColumnVersion()`), and RENAME of a column without an index queues no purge of
  it. So the files update mode names `txn` are new or an uncommitted attempt's.
  It rebuilds the whole index, placing each row group's rows at the index rows the new footer's
  index row map gives it (`collectRowGroupsInIndexRowOrder`): a kept row group keeps its index
  rows, a written one takes fresh rows past the committed count, and the rows of a replaced or
  dropped group get no entries. A table without a symbol index drops the map
  (`PartitionUpdater.dropIndexRowMap()`), so its footer keeps the identity map.
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
this, except for an index named at or above txn: it names that one `txn`, and the rewrite branch of
`o3ConsumePartitionUpdateSink` moves `_cv` to it (`versionForeignParquetIndexes`, on the shared
predicate `isForeignParquetIndex()`), queuing no purge: the old version leaves with the old
partition directory. Keep the worker and the writer on the shared predicates: a column the worker rebuilds but
`_cv` does not move is read from the stale committed files, and a column `_cv` moves but the
worker did not build has no files. Pinned by `ParquetInPlaceIndexPinnedReaderTest`, and under
concurrent readers and purge jobs by `ParquetInPlaceIndexConcurrentReaderTest`.

**`_cv` invariant.** After any parquet O3 publish (rewrite or in-place), every live column's
`_cv` top is 0. update.rs `end()` keeps in the file only the column tops below the first row the
update changed (`PartitionUpdater.setColumnTopRowLimit()`, see `computeColumnTopRowLimit()`), and
the decoder reads those rows as NULL, so `_cv` keeps no top. TableWriter's publish branches (rewrite, in-place update and fresh FORMAT PARQUET partition) call
`zeroColumnTopsAfterParquetRewrite` so the two agree. A new publish branch must do the same, or
`_cv` consumers (CONVERT TO NATIVE, dedup, symbol index) treat present values as absent. The
in-place branch also moves every rebuilt index column to the new column name txn
(`versionRebuiltParquetIndexes`, see Index MVCC above); a new in-place publish branch must do
that too, or readers keep opening the committed index files for the new row layout. The rewrite
branch moves the index columns named at or above the txn to it (`versionForeignParquetIndexes`); a
new rewrite publish branch must do that too, or readers open index files the rewrite did not build.

**Column tops on CONVERT TO NATIVE.** `CONVERT PARTITION TO PARQUET` keeps a native column top in
the file's QuestDB metadata but zeroes a top below the row count in `_cv`. The Rust decoder skips
every row group that lies whole inside the file's column top, so `produceNativeFromParquet` writes
nothing for those rows. It counts the skipped rows before the first written chunk, plus the leading
NULLs of that chunk for a no-sentinel column (BOOLEAN/BYTE/SHORT/CHAR), as the native `_cv` top
(`parquetDecodedColumnLeadingRows`); a skipped row group after written data becomes an all-NULL
chunk. `restoreIndexFilesAfterParquetToNative` links the parquet partition's index files only for
a column with a zero top and a footer with the identity index row map; otherwise it rebuilds the
index. Before this, a top spanning a whole row group (small row groups, a column added
mid-partition) left `_cv` at 0 over a shorter native file: shifted values, and a read past the
file's end.
Pinned by `AlterTableConvertPartitionTest.testConvertRoundTripColumnTopSpanningRowGroups`.

`hasTypeConvertedColumns` is set when a column maps into the parquet file through its
`getOriginalWriterIndex()` (the `replacingIndex` chain head), its current writer index
differs — i.e. it went through `ALTER COLUMN TYPE` — **and** the type stored in the parquet
file (ignoring `PARQUET_SYMBOL_NOT_NULL_HINT`) differs from the table's column type. Every
parquet writer stores a column under its original writer index, so a file that a previous
rewrite already materialized to the new type (or a VARCHAR -> STRING -> VARCHAR round
trip that left the file untouched) still matches by that index but needs no rewrite. So a
pending conversion always forces a rewrite; the in-place `copyRowGroupWithNullColumns()` path
is taken only when the schema is unchanged. On that path the file's column order may differ
from the writer's column order, and the Rust `ParquetUpdater::align_columns_to_schema()`
matches the supplied columns to the file schema by parquet field id.

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
  merge index        -> replace: createReplaceMergeIndex (see Replace commits)
                        non-dedup: Vect.mergeTwoLongIndexesAsc
                        dedup:     malloc index, build dedup-compare addresses, mergeDedup, realloc
                     -> returns the number of rows the dedup or the replace range dropped
processParquetPartition
  SKIP mode, all O3 rows dropped -> the row groups stay as they are, no stage 2
  UPSERT identity check found the MERGE identical -> the row groups stay as they are
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
  keeps an O3 row, or a `COPY_O3`, and decides there whether to rewrite (see Rewrite vs
  in-place update). On that first write a rewrite copies the row groups of the actions before
  it, in order. When no action writes, the partition stays as it is, and the
  partition sink reports it as unchanged as the native path does (`updatePartitionSink(...,
  newPartitionSize = -1, partitionMutates = 0)`). Other commits open the output upfront.

`TableWriter` applies nothing for a partition reported unchanged (`isO3PartitionUnchanged()`):
`o3ConsumePartitionUpdateSink()` leaves its `_txn` entry as it is, and the posting index seal
(`sealPostingIndexesForO3Partitions()`) does not rebuild its index, which would read and re-index
the whole partition column on every commit that re-sends rows the table has.

UPSERT takes the same shortcut on parquet partitions, as the native merge's
`checkDedupCommitIdenticalToPartition` does for native partitions. `isUpsertIdenticalUpToPartitionMax()`
runs before the output opens: per `MERGE` up to the partition max it decodes the keys and dedups in
UPSERT mode (`isKeyColumnsOnly = true`, `isSkipMode = false`), stops unless every O3 row at or below
the max is a duplicate, then decodes the other columns and compares them with
`FrameAlgebra.isColumnMergeIdentical()`, the native comparer (fixed, VARCHAR, STRING, BINARY, ARRAY;
NULLs included). It reads the Phase 1a prepared buffers, so it sees converted types and the NULL
buffer of a column the file lacks. A `COPY_O3` at or below the max ends the check. When every
compared row is identical, the commit runs in SKIP mode: an identical resend leaves the partition
as it is (no file, `_pm` or index written), and one that also brings rows past the max only
appends, which keeps every column top, as native turns such a commit into a noop or an append.
When a `MERGE` changes the partition, the merge loop leaves the identical `MERGE`s before it as
stored (`O3ParquetMergeContext.setUnchangedMergeActions()`), unless they hold a NULL of a BOOLEAN,
BYTE or SHORT column, which their merge writes as 0/false as native materializes a top
(`hasNoSentinelNull()`). The check keeps what it prepared of the `MERGE` where it stops, so the
merge loop decodes no row group twice, except where a rewrite copies row groups through
`ParquetRowGroupMaterializer` first. `MERGE`s after the first change merge and write their row
groups even when their O3 rows carry the stored values. The caller of `mergeRowGroupPrepare()`
adds the duplicates to the dedup counter, so the check does not count them twice.

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

**The dedup-key column top is in row-group units, never the `_cv` top.** The comparer reads
the existing side with row-group-local indexes (`col_index >= column_top`, `dedup_comparers.h`),
so `mergeRowGroupPrepare` passes `rowGroupSize` for a key column missing from the file (all NULL, the
native `columnTop > mergeDataHi` rule, data pointer never read) and `0` for a decoded one (the
decoder writes each def-level-0 row as the type's NULL value, and `prepareSourceColumn`
NULL-fills an all-NULL chunk, so `srcPtrs` holds every row; for BYTE/SHORT/CHAR/BOOLEAN, which
have no NULL sentinel, that value is 0, which is also the comparer's null for those types). The partition-level `_cv` top is wrong here: a key
added after CONVERT while the partition was last has `top == partition rows`, not the row-group
size (this used to trip an `-ea` assert and suspend the table). Where tops are not zeroed after an O3
publish (master before the in-place `_cv` zeroing, or any future publish path that skips it), a key
written into the file can also keep a stale full-partition top that would read its real values as
NULL and dedup them away.

`ConvertOperatorImpl` has **no dedup-key pre-pass** — the merge path above handles a dedup-key
column whose conversion crosses the fixed↔var/symbol boundary while the partition stays lazy
parquet, so enabling dedup or altering a dedup key never eagerly rewrites partitions.

## Replace commits

A replace commit (`WAL_DEDUP_MODE_REPLACE_RANGE`) passes its per-partition window
`[replaceLo, replaceHi]` to `computeMergeActions`. A row group the window intersects
without bringing O3 rows becomes a filter-only `MERGE` (empty O3 slice, `o3Hi < o3Lo`)
or, when fully covered, a `DROP`. `mergeRowGroupPrepare` builds its index with
`createReplaceMergeIndex` (existing rows before the window, O3 rows, existing rows after
it) instead of the plain or dedup merge; replace and dedup never combine. A replace commit
never runs in SKIP mode, so its output opens upfront, and `computeColumnTopRowLimit()` ends
the prefix of rows that stay in place at `replaceLo - 1` or at the first `DROP`. Before any
full decode, `processParquetPartition` decodes only the timestamp column of each
filter-only `MERGE` whose window lies strictly inside the row group
(`hasRowInReplaceRange`; a window holding the row group's min or max row has a row in
range); when no row lies in `[replaceLo, replaceHi]` (e.g. the window falls strictly
between two rows) the action becomes a `COPY_ROW_GROUP_SLICE`, so the row group is not
re-encoded. A window that misses every row with no O3 rows publishes a no-op (file size
and name txn unchanged); one that drops every row group with no O3 rows publishes a
size-0 removal. Neither writes a file.

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
- **No `_cv` tops in `mergeRowGroupPrepare`.** Column tops there are row-group-local (see
  Deduplication). `tableWriter.getColumnTop(partitionTs, ...)` is a partition-level value.
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
| `O3ParquetMergeStrategy.java` | `computeMergeActions`, the `MergeAction` types (`MERGE` / `COPY_ROW_GROUP_SLICE` / `COPY_O3` / `DROP`) |
| `O3ParquetMergeContext.java` | O3-specific extension of `ParquetConversionContext` (`getNullBufs`, `getMergeDstBufs`, merge ranges) |
| `DedupColumnCommitAddresses.java` | the dedup-compare address sink read by `Vect.mergeDedupTimestampWithLongIndexIntKeys` |
| `O3CopyJob.java` | `mergeCopy` — consumes prepared source + merge index into destination buffers |
| `TableWriter.java` | `isCommitDedupMode`, `getDedupCommitAddresses`, `convertPartitionParquetToNative`, `getParquetColumnType`, `TIMESTAMP_MERGE_ENTRY_BYTES` |
| `ConvertOperatorImpl.java` (griffin) | eager `ALTER COLUMN TYPE` for native partitions + the parquet→native pre-pass (target SYMBOL / chained mismatch only) |
| `row_groups.rs` / `decode.rs` (rust) | physical decode and `post_convert` (fixed→fixed scaling, boolean expansion) |

## Symbol null flag

`SymbolMapWriter` keeps a null flag in the `.o` file header. `LATEST ON` adds one to its
distinct-key target when the flag is set and stops scanning once every target key is found, so
a flag left unset on a column that holds NULL rows leaves the scan one group short: it drops
the group the backward scan reaches last, which is the NULL group or a symbol. The unindexed
requested-key paths (`s = NULL`, `s IN (..., NULL)`) also read the flag and drop a NULL key
while it is unset.

Every write path sets the flag: `SymbolMapWriter.put(null)`, WAL apply through
`SymbolMapDiff.hasNullValue()`, and the `ADD COLUMN` nullers. `TableWriter` repairs it on
`ATTACH PARTITION` from column tops, parquet chunk null counts, the partition's own data, or a
parquet decode when statistics are absent. For an indexed column of a native partition, attach
reads the index instead of the data: a NULL entry sets the flag, and the absence of one counts
only when the index max value reaches the last row. Otherwise, or when the index cannot be
read, attach scans the data. Two attach paths skip the index probe and that scan: an attach
without `_dmeta` takes the answer from its column validation pass, which already maps the data,
and a parallel `COPY` attach relies on `SymbolMapWriter.mergeSymbols()` having set the flag from
the imported symbol tables. `Mig1002` repairs existing databases from column tops, parquet chunk
null counts and bitmap index NULL entries only. It never reads column data, so an upgrade
costs no scan proportional to table size.

### Accepted residual

A database written before this repair can still carry an unset flag on a native partition
without a BITMAP index, or on a parquet partition written without statistics, that gained NULL
rows through `DETACH` -> `TRUNCATE` -> `ATTACH` or through a parquet round trip of a converted
column.
An unfiltered `LATEST ON` over such a column comes up one group short when other symbols
exist; the all-NULL case still resolves because the scan no longer stops on a zero target. A
requested NULL key matches nothing.

The pre-repair engine behaved the same way for the unfiltered scan, `s = NULL` and
`s IN (..., NULL)`. It did not for an OR of key equalities: `s = 'A' OR s = NULL` ran as a row
filter, which never reads the flag, and returned the NULL group whenever the scan reached it
before the distinct-key target. `SqlCodeGenerator.normaliseLatestByKeyOr()` now rewrites that
OR to `s IN ('A', NULL)`, so on such a column the query loses a NULL group it returned before
the upgrade. The OR form keeps its NULL group on an indexed column, where the key lookup
resolves NULL without the flag.

The maintainer accepts this, the OR form included, in exchange for an upgrade that does not
scan column data and an unindexed key list that stops early on a column without NULL rows. Do
not report it again, require `Mig1002` to read `.d` files or decode parquet, or require the
rewrite to skip a NULL arm.
`Mig1002Test.testKeepsNullFlagUnsetForParquetPartitionWithoutStatistics` pins the migration's
boundary and `testLatestOnFindsNullGroupOfLegacyAttachedPartitionWithoutFlag` pins the
all-NULL case.
