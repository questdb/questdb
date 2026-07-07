# Zero-Copy Partition Split (Partition Top)

An O3 commit that lands inside a partition splits it without copying the suffix:
the suffix becomes a new partition that **hardlinks** the donor's column files and
exposes only a row range of them through a per-partition **partition top**. This
covers the on-disk format, the split decision, read/index paths, every writer
operation that can touch a split partition afterwards, and squash.

## 1. Mechanism

One O3 commit into partition `[0, N)` at rows `[R1, R2)` produces three partitions:

```
prefix donor   [0, R1)     same directory, logical size shrunk to R1, files untouched
middle         merge rows  fresh directory, the only data written to disk
suffix child   [R2, N)     fresh directory of HARDLINKS to donor files, partitionTop = R2
```

The child's `_txn` records `partitionTop = R2`: "logical row 0 lives at file row R2".

**partition top vs column top — same word, opposite sign.** *column top* (`_cv`) =
leading logical rows a late-added column lacks → SUBTRACTS from the file address.
*partition top* = file rows below logical row 0 → ADDS. Combined, used identically by
every reader/writer:

```
file_row = logical_row + partitionTop - columnTop
NULL when: logical_row < max(0, columnTop - partitionTop)
```

The clamp governs NULL determination only; address arithmetic always uses the
unclamped signed difference (on a child, `columnTop - partitionTop` is routinely
negative and lands at a positive file row in the donor's data region).

## 2. On-disk format

### 2.1 `_txn` slot 3: packed partition top + DONOR flag

Stride stays 4 longs/partition (`LONGS_PER_TX_ATTACHED_PARTITION`); no version bump,
no migration. Slot 3 (`PARTITION_TOP_OFFSET == PARTITION_PARQUET_FILE_SIZE_OFFSET == 3`)
is format-overloaded: parquet file size for a parquet partition, else a packed long:

| bits | meaning |
|---|---|
| 0-43 | `partitionTop` (`PARTITION_TOP_MASK = 0x00000FFFFFFFFFFFL`) |
| 44-62 | reserved, preserved by RMW |
| 63 | DONOR flag (`PARTITION_DONOR_FLAG = 0x8000000000000000L`): shares hardlinked inodes with a split relative |

Legacy `-1L` (written by `TxReader.initPartitionAt`) is the **unset sentinel** →
`{top:0, donor:false}`. Decode is **mask-based, never sign-based** (a donor-with-top is
a negative long that must decode to a positive top):

```java
getPartitionTopByRawIndex:  v == -1L ? 0 : (v & PARTITION_TOP_MASK)
isPartitionDonorByRawIndex: v != -1L && (v & PARTITION_DONOR_FLAG) != 0
```

A mutation leaving the packed value at 0 stores `-1L` (never persist `0L`).

- `TxReader`: `getPartitionTop(int)`, `getPartitionTopByTimestamp(long)` (0 if missing),
  `isPartitionDonor(int)`, `isPartitionDonorByRawIndex(int)`; native decoders assert
  `!isPartitionParquet`.
- `TxWriter`: `setPartitionTop`/`setPartitionTopByRawIndex` (preserve DONOR bit),
  `setPartitionDonor`/`clearPartitionDonor` (preserve top), `resetPartitionTop(int)` →
  stores `-1L` (clears top AND donor) on consolidation. Every mutator bumps
  `recordStructureVersion` + `partitionTableVersion` (full record commit → concurrent
  readers reload cached tops).
- `TableWriter`: `getPartitionTopByTimestamp(long)`, per-column
  `getPartitionTopByTimestamp(long,int)` (§2.3), `isPartitionDonorByTimestamp(long)`.

`setPartitionParquetFormat` asserts native→parquet only on a materialized partition
(`top==0 && !donor`); callers force-squash first (§7.7, §8).

### 2.2 Directory layout / hardlinked files

`TableWriter.hardlinkSuffixChild` populates a normal `<ts>.<nameTxn>` dir with hardlinks:

- `.d`: always linked. `.i`: linked for var-size types.
- Bitmap `.k`/`.v`: **linked** (donor indexed all `[0, donorFullSize)` at physical IDs;
  child reads `[R2, donorFullSize)` via `+partitionTop`, no rebuild). Safe: donor frozen
  after split, shared inode only grows via the child's own appends above every donor entry.
- Posting: immutable `.pv`/`.pc`/`.pci` **linked**; mutable `.pk` **copied** (byte copy,
  not link). `.pk` is a single-head txn-ordered chain both siblings pick from → private
  copy lets each seal independently. Only the *live* generation's `.pv`/`.pc` is linked;
  older chain entries reference superseded files a pinned reader never maps. See §5.
- Table-root symbol maps: not per-partition, untouched.

`linkFile` falls back to `ff.copy` on EXDEV (correctness kept, sharing lost). Link
failure mid-way → child dir removed, rethrow (nothing committed to `_txn` yet). Inode
reclamation is POSIX link count; existing purge (`O3PartitionPurgeJob`,
`processPartitionRemoveCandidates`) unchanged.

### 2.3 Column tops on a child: shared-coordinate convention

Child `_cv` records are in **shared-file coordinates**; a per-column **birth gate**
decides participation:

```java
columnParticipates = columnVersionReader.getColumnTopPartitionTimestamp(col) < partitionTimestamp  // strict <
```

- **Born before child** (gate true): file is a donor hardlink; `_cv` = donor's top
  unchanged (file frame); full canonical formula applies.
- **Born at/after child** (gate false): file is private + 0-based; `_cv` = child-logical;
  effective per-column top = 0.

Child `_cv` at split time, per column:

| donor state | child `_cv` |
|---|---|
| shared record (donor has file-frame top) | donor's raw top, unchanged |
| donor-local record (donor is itself a child) | `donorPartitionTop + donorColumnTop` (re-based to file frame) |
| no record (column absent from donor) | `donorPartitionTop + donorFullSize` (explicit end-of-file top) |

The record carries the donor's `columnNameTxn` so the child addresses the same files.

The one unclassifiable case — a column **born in a later partition but O3-backfilled into
the donor** (data physically present, gate would say child-local → wrong NULLs) — is
prevented at write time: `isHardlinkSplitBlockedByBackfilledColumn` declines the split
(§3). Row IDs stay logical everywhere (`Rows.toRowID`, UPDATE, `recordAt`).

## 3. When a hardlink split happens

`O3PartitionJob.processPartition` computes prefix/merge/suffix geometry, then
`canHardlinkSplit` requires all of:

1. source native (not parquet);
2. `isHardlinkSplitEnabled()` — `cairo.partition.top.wal.enabled` (WAL) /
   `cairo.partition.top.non.wal.enabled` (non-WAL), **both default false**; gate
   creation only, read/write of existing tops always supported;
3. suffix covers the whole tail: `suffixType == O3_BLOCK_DATA && suffixHi == srcDataMax-1
   && suffixLo == mergeDataHi+1 && suffixChildRowCount > 0`;
4. `ts[R2] > o3TimestampHi` (no suffix same-timestamp reduction; a run straddling R2
   fails this gate);
5. `!isHardlinkSplitBlockedByBackfilledColumn` (§2.3);
6. `isHardlinkSplitWithinSquashCap` — `pieces + 2 <= cap` (`o3.last.partition.max.splits`
   default 20 for last logical partition, else `o3.mid.partition.max.splits` default 1),
   else the post-commit squash would immediately re-fold (and folding a donor range costs
   a whole-partition copy). **Consequence: a mid-partition hardlink split needs
   `o3.mid.partition.max.splits >= 3`.**

Overall: `prefixType == O3_BLOCK_DATA && (mergeType == MERGE || O3) && (canHardlinkSplit
|| <classic copy-split cost gates>)`. The hardlink path **bypasses both classic cost
gates** (nothing to copy → nothing to amortize). Prefix same-timestamp reduction kept
(binary-search shrink when `maxSourceTimestamp == o3TimestampLo`; no reducible prefix →
no split). When taken, `suffixType` is forced `O3_BLOCK_NONE` (workers write only the
middle).

**Dedup no-op undo:** if dedup collapses the merge the split is undone before any link
(all-dup → true no-op, phantom middle removed; append-only remainder → plain in-place
append). Hardlink creation is keyed on the final decision at consume time.

**Replace-range** (`isCommitReplaceMode`) has no dedicated variant; the
commit-identical fast paths (`checkReplaceCommitIdenticalToPartition`,
`checkDedupCommitIdenticalToPartition`) are skipped when the target carries a top.

## 4. Commit protocol

Decision/geometry on the O3 worker; all linking + `_txn`/`_cv` mutation single-threaded
on the writer after every column task and the final dedup decision. Update-sink header is
12 longs (`PARTITION_SINK_SIZE_LONGS`); slots 0-7 legacy, hardlink slots written by
`O3PartitionJob` at decision time:

| slot | content |
|---|---|
| 8 | `splitMode` (`SPLIT_NONE=0`, `SPLIT_THREE_WAY_HARDLINK=1`) |
| 9 | `suffixChildTimestamp` = `ts[R2]` |
| 10 | suffix top, composed: `sourcePartitionTop + (mergeDataHi + 1)` |
| 11 | `suffixChildRowCount` |

`o3ConsumePartitionUpdateSink`, in order: (1) insert middle (lo ts = `maxSourceTimestamp
+ 1`); (2) if donor was active, `closeActivePartition(committedLastPartitionSize)` first
to flush mmapped tails; (3) `hardlinkSuffixChild` — links files + writes child `_cv`
(§2.3), sharing indexes inline; (4) attach child, `setPartitionTop(childTs, composedTop)`,
then `setPartitionDonor(childTs)` **and** `setPartitionDonor(donorTs)` (both sharers
flagged; middle gets neither); (5) update `minSplitPartitionTimestamp`.

Row accounting: mid-split → all three fixed (`fixedRowCount += (R1-oldSize) + middleRows
+ suffixChildRowCount`). Donor-was-last → child is new last and stays transient
(`commitTransientRowCount = suffixChildRowCount`; donor's `R1` + middle go to
`fixedRowCount`).

## 5. Indexes

Per index type at split:

- **Bitmap** (`.k`/`.v`): **hardlinked** with `.d`/`.i` (`linkColumnIndexFiles`). No
  rebuild — child reads `[R2, donorFullSize)` via `+partitionTop`. The shared inode
  stays intact because a donor-flagged bitmap column is never rebuilt/overwritten in
  place, and writer closes are safe by construction: exactly one writer is live per
  index inode at a time (the child-as-last reuses the table writer's indexer — see
  the `OPEN_LAST_PARTITION_FOR_APPEND` dispatch — and transient writers open fresh),
  and every writer loads its sizes from the `.k` header at open, so a
  truncate-to-cached-size close only trims slack beyond every sibling's data.
- **Posting**: `.pv`/`.pc`/`.pci` **hardlinked**, `.pk` **copied** (`linkColumnIndexFiles`
  `copyKeyFile` param). No rebuild — copied `.pk` points at the hardlinked live `.pv`.
  `.pk` can't be linked (single-head txn-ordered chain both siblings pick from); private
  copy lets each seal independently. Only the live generation's `.pv`/`.pc` is linked.
  The donor's own reseal (`sealPostingIndexesForO3Partitions` sweep) rotates it onto a
  fresh `.pv`, leaving the shared live `.pv` referenced only by the child (its hardlink
  keeps it alive through the donor's purge). The child is deliberately **not** resealed.

Single convention: **stored index row IDs are physical (shared-file frame); every API
boundary is logical**, shift applied inside the reader/indexer. A donor reading its
shared `.k`/`.v` while the child appends is ordinary single-writer/concurrent-reader:
the reader clips every cursor to its logical row range, never observing the sibling's
entries.

**Read side** (`IndexReader.of(...)` gained trailing `long partitionTop`; `default long
getPartitionTop()` for cache revalidation):

- Every cursor entry (`BitmapIndex{Fwd,Bwd}Reader.getCursor`,
  `ConcurrentBitmapIndexFwdReader.initCursor`, `PostingIndex{Fwd,Bwd}Reader.getCursor`)
  shifts the logical window: `minValue += partitionTop; if (maxValue != Long.MAX_VALUE)
  maxValue += partitionTop;`. The open-bound sentinel is exempt (overflow would drop all
  rows).
- The `columnTop` passed to `of(...)` is always the raw `_cv` (physical); NULL branch
  (`key==0 && columnTop>0 && minValue<columnTop`) compares physical vs physical.
- Cursors return `next - minValue` → returned rows are **logical**.
- `TableReader.getIndexReader` reopens a cached reader when `getPartitionTop()` no longer
  matches (picks up a squash that materialized the child).

**Write side:**

- `ColumnIndexer.setPartitionTop(long)` (no-op default) /
  `SymbolColumnIndexer.partitionTop`: `index(...)` shifts both bounds → active-partition
  indexing of a child-as-last stores physical IDs. `configure{Writer,FollowerAndWriter}`
  reset the field to 0; every reconfigure site re-applies `setPartitionTop`.
- `SymbolColumnIndexer.rollback` uses `rollbackValues(maxRow + partitionTop)`:
  `rollbackIndexes` passes a LOGICAL bound but the index stores PHYSICAL IDs.
- REINDEX / post-UPDATE rebuild (`RebuildColumnBase`, `IndexBuilder.doReindex`) thread
  the top, compute gate + logical top, index `[logicalTop, partitionSize)`.
  `RecoverVarIndex` sizes a rebuilt `.i` to the whole shared file.

## 6. Read path

- **TableReader.** `openPartition0` caches the top in `openPartitionInfo` slot 6
  (`PARTITIONS_SLOT_OFFSET_PARTITION_TOP`); parquet caches 0. `getPartitionTop(int,int)`
  applies the birth gate. `reloadColumnAt` widens every mmap to `mappedRowCount =
  partitionTop + (partitionRowCount - columnTop)` (fixed `<< pow2SizeOf`; var aux
  `getAuxVectorSize`; var data `getDataVectorSizeAt(aux, mappedRowCount-1)`).
  `partitionRowCount - columnTop` may be negative while `mappedRowCount` is positive.
  No-data columns open `NullMemoryCMR` with `columnTops = partitionRowCount`.
- **Page-frame cursors** (`{Fwd,Bwd}TableReaderPageFrameCursor.computeNativeFrame`):
  boundary uses clamped `max(0, columnTop - partitionTop)`; slice uses unclamped
  `partitionLo/HiAdjusted = partitionLo/adjustedHi - columnTop + partitionTop`. Fixed
  pages start at `pageAddress(0) + (partitionLoAdjusted << shl)`; var **data page is
  always the whole-file base** (aux holds file-absolute offsets — partial map
  double-adds). `NullMemoryCMR` exempt (`colPartitionTop = 0`). Downstream (JIT, vector
  agg, group-by) receive pre-shifted addresses.
- **Interval scans.** `NativeTimestampFinder` loads the top in `prepare()`; designated
  ts has no column top → `file_row = logical + partitionTop`; `findTimestamp` searches
  `[rowLo+top, rowHi+top]` and maps back (incl. negative insertion points).
- **Var-size drivers.** `Varchar/ArrayTypeDriver.getDataVectorSize` always compute
  `offset(rowHi end) - offset(rowLo)`; the old `rowLo==0` shortcut is gone (row 0 of a
  window can sit at any absolute data offset).
- **SHOW PARTITIONS / table_partitions().** No new columns; counts logical. Native
  min/max read at file rows `[top, top+numRows-1]`. Reported disk sizes double-count
  shared inodes (cosmetic).

## 7. Writing into split partitions afterwards

A split child is a first-class writable partition ("full partition-top-aware O3 write").

### 7.1 What the DONOR flag gates

"This partition shares inodes with a relative." Enforced at 4 points (+ the native→parquet
assert, §7.7):

1. **Append into a prefix donor** (`DONOR && top==0`): `O3PartitionJob` forces
   `canAppendOnly=false` → rewrite into a fresh dir (in-place append at file row R1 would
   clobber the child's shared bytes). The rewrite severs sharing; consume calls
   `resetPartitionTop`.
2. **Squash target**: non-force squash never writes into a donor-flagged partition (§8).
3. **Force squash**: a donor target is copied into a fresh dir, not overwritten.
4. **O3 merge scratch**: `mergeFixColumn`/`mergeVarColumn` place their null-materialization
   scratch beyond the physical end of a donor-flagged partition's column files instead of
   its logical data end (§7.3).

A **suffix child** (`top > 0`) is exempt from the append veto — its logical tail *is* the
true file tail, so an in-place append lands beyond every sharer's range.

### 7.2 In-order append into a suffix child

`openPartition` caches `lastOpenPartitionTop`; `setColumnAppendPosition` computes `pos =
size + perColumnTop - rawColumnTop`. WAL lag on a child-as-last follows the same rule
(lag region at `transientRowCount + perColumnTop - columnTop`; for designated ts,
`transientRowCount + top`); every stash/read-back site adds the per-column top
(`cthMergeWal*WithLag`, `o3MoveUncommitted`, `applyFromWalLagToLastPartition`,
`applyLagToLastPartition`, dedup lag mapping, `processWalCommit` `tsLagOffset`). Appends
stay zero-copy.

### 7.3 O3 write into a suffix child

`O3PartitionJob` maps the source designated-ts file wider (`(top + srcDataMax) * 8`) and
uses `srcTimestampVecAddr = srcTimestampAddr + top*8` so all geometry stays logical. Per
column, `O3OpenColumnJob` decomposes raw `_cv` into two mutually exclusive values
(asserted ≤1 non-zero):

```
srcDataTop          = min(srcDataMax, max(0, rawColumnTop - perColumnTop))  // child-logical null rows
srcDataPhysBaseRows = max(0, perColumnTop - rawColumnTop)                   // file rows below child row 0
```

Source reads offset by the base; var aux widened by the base, var data mapped from byte 0;
`dstIndexAdjustRows` makes index writers store physical IDs.

**O3 merge into a child always writes a fresh contiguous dir** — the merge *consumes* the
top; consume's `partitionMutates` branch calls `resetPartitionTop` (relatives keep the old
inodes alive). **O3 split of a child composes** — grandchild links the same inodes,
`partitionTop = parentTop + localR2` (slot 10 pre-composed); `hardlinkSuffixChild` re-bases
donor-local `_cv` by the donor's top → all grandchild records uniformly shared-file-relative.

**Null-materialization scratch stays off shared bytes.** When a merged column's top sits
above the merge prefix (`srcDataTop > prefixHi`, or an O3 prefix), `mergeFixColumn` /
`mergeVarColumn` materialize a nulls+data image of the source column *inside the source
file* ("we will be discarding it anyway"). On a DONOR-flagged partition (either sharer)
the scratch starts beyond the file's PHYSICAL end (`ff.length`), never at the partition's
logical data end — a prefix donor's logical end sits *inside* bytes its suffix child reads
(a donor re-split used to null over the child's data from file offset 0).

### 7.4 UPDATE

`UpdateOperatorImpl`: per column `logicalTop = max(0, raw - perColumnTop)`, `srcRowShift =
max(0, perColumnTop - raw)`; reads old vectors at `row + srcRowShift`; writes the
replacement as a child-local 0-based file; new `_cv` written back in shared coords
(`effectiveTop + perColumnTop`). Post-UPDATE reindex threads the top through
`RebuildColumnBase`/`IndexBuilder`.

### 7.5 ALTER COLUMN TYPE

`ConvertOperatorImpl`: `srcShift` (per-column top), `logicalTop`, `skipRows = max(0,
srcShift - columnTop)`. `skipRows > 0` runs synchronously (async task has no shift slot).
Converted column is child-local; new top recorded in shared coords.

### 7.6 DROP PARTITION and boundary reads

Min/max reads (`readPartitionMinMaxTimestamps`, `readNativeMinMaxTimestamps`,
`readMinTimestampNative`) at `[top, top+size-1]`. When dropping the last partition makes a
child the new last, `ColumnVersionWriter.replaceInitialPartitionRecords(newLastTs,
transientRowCount, lastPartitionTop)` handles the subtlety: moving the
`COL_TOP_DEFAULT_PARTITION` record onto the child flips its birth gate child-local, so an
explicit file-frame top is converted to child-logical, clamped to `min(transientRowCount,
max(0, fileTop - lastPartitionTop))`.

### 7.7 DETACH, ATTACH, parquet, TRUNCATE

`detachPartition`, native→parquet conversion, parquet generation/switch all run
`squashPartitionForce` first (materialize before the op; `setPartitionParquetFormat`
asserts). ATTACH inits slot 3 to `-1L`. TRUNCATE drops entries wholesale; shared inodes
die with the last unlinked dir.

## 8. Squash

Folds split pieces of a logical partition into one contiguous partition — now zero-copy
on the source side, donor-aware on the target side.

1. **Triggers.** Post-commit housekeeping squashes from `minSplitPartitionTimestamp` with
   the last-partition cap; mid groups fold to `o3.mid.partition.max.splits`, last to
   `o3.last.partition.max.splits`. Force squash (detach, parquet, `squashPartitions()`
   SQL) folds to one.
2. **Target.** `canOverwrite = canSquashOverwritePartitionTail(i) &&
   !isPartitionDonor(i)`. Non-force never overwrites a donor in place and never copies;
   if every candidate is donor-flagged/reader-pinned, no-op. Force accepts the first
   candidate and copies a non-overwritable one into a fresh dir (`copyTargetFrame`,
   read-side opens with the target's own top). An overwritable target asserts `top==0`.
3. **Sources.** Any piece. `FrameFactory.openRO(..., partitionTop)`: prefix donor reads
   `[0, R1)`, child reads `[top, top+size)` — zero-copy readback of the logical slice.
4. **Frame offset.** `FrameImpl` carries the top as `Frame.getOffset()`; every non-offset
   open resets to 0 (pooled). Per column the birth gate selects `colOffset = offset` or 0.
   `ContiguousFileFix/VarFrameColumn.append` addresses `file_row = logical - columnTop +
   offset`; var aux from byte 0 with file-absolute data offsets; `FrameAlgebra.append`
   splits null-pad vs data in child-logical space (`max(0, columnTop - offset)`) and
   rewrites target tops as plain logical. Write-side frame always offset 0 (asserted).
5. **After.** `resetPartitionTop(targetPartitionIndex)` (slot 3 → `-1L`); superseded
   sources → purge queue; cached index readers reopen on the top change (§5).

Reader safety = pre-existing machinery: in-place overwrite requires the txn-scoreboard
range check; everything else is nameTxn versioning + purge.

## 9. Configuration

| key | default | effect |
|---|---|---|
| `cairo.partition.top.wal.enabled` | `false` | allow hardlink splits on WAL tables (creation only) |
| `cairo.partition.top.non.wal.enabled` | `false` | allow hardlink splits on non-WAL tables (creation only) |
| `cairo.o3.last.partition.max.splits` | 20 | max pieces of last logical partition before squash folds; also caps creation (`pieces+2 <= cap`) |
| `cairo.o3.mid.partition.max.splits` | 1 | same for non-last; must be >= 3 for any mid-partition hardlink split |
| `cairo.o3.partition.split.min.size` | 50MB | classic copy-split threshold; **not consulted** by hardlink path |

Neither top key is dynamic. Disabling never strands data: existing children stay
readable/writable and every consolidation path materializes them normally.

## 10. Invariants

1. **Canonical formula everywhere**: `file_row = logical + partitionTop - columnTop`;
   clamp `max(0, columnTop - partitionTop)` is NULL-only.
2. **Birth gate**: shift iff `getColumnTopPartitionTimestamp(col) < partitionTimestamp`
   (strict). Gate-true `_cv` = shared coords; gate-false = child-logical. Every writer
   keeps a gate-true record in shared coords.
3. **Var data mapped/addressed from byte 0** in every path; aux holds file-absolute
   offsets (partial `[aux[top]..]` map double-adds).
4. **Index row IDs physical; index API boundaries logical.** The `columnTop` handed to an
   index reader is the raw physical `_cv`. Every min/max shift guards `maxValue ==
   Long.MAX_VALUE`.
5. **DONOR flag on both sharers, never the middle.** A prefix donor is never appended in
   place; no donor is squash-overwritten in place. A suffix child may be appended in place
   and be a zero-copy source anywhere.
6. **Slot 3 native-only**: parquet never carries a top/flag; force-squash before any
   parquet transition; packed accessors assert `!isPartitionParquet`.
7. **`-1L` sentinel discipline**: `-1L` ↔ `{0, false}`; empty packed value normalized to
   `-1L`; decode mask-based.
8. **Materialization resets slot 3**: any op giving a top/donor partition a fresh dir (O3
   merge rewrite, squash) calls `resetPartitionTop`.
9. **Only one of `{child-logical top, physical base}` non-zero** per column on O3 write
   (`assert srcDataPhysBaseRows == 0 || srcDataTop == 0`).
10. **Merge scratch beyond physical EOF on donors**: the null-materialization scratch in
    `mergeFixColumn`/`mergeVarColumn` never touches bytes below `ff.length` of a
    DONOR-flagged partition's column files (§7.3).

## 11. Known limitations

- **Execution paths not yet top-aware** (must not see a split child; why creation gates
  default off): `BitmapIndexFwdReader.getFrameCursor` (raw-physical `IndexFrame`s; sole
  consumer `SampleByFirstLastRecordCursorFactory`, in-code deferral comment);
  `LatestByAllIndexedRecordCursor` and ranged
  `AbstractPostingIndexReader.collectDistinctKeysInRange` pass unshifted bounds;
  `TableWriter.indexNativePartition` (ADD INDEX on historic partitions) and
  `rollbackIndexes` use logical bounds against physical-ID indexes. The
  `PageFrame.getIndexReaderPartitionTop` hook is plumbed through both page-frame cursors
  for future wiring.
- **Disk-size reporting double-counts shared inodes** (cosmetic; reclamation is POSIX
  link count).
- **Write amplification**: every slot-3 mutation bumps `partitionTableVersion` → full
  `_txn` commit (acceptable, splits rare per commit).
- **EXDEV degrades to copy** (correct, not zero-copy).
- **44-bit top width** (~17.6T rows); masked, not range-checked.
