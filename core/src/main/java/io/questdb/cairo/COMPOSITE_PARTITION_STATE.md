# Composite partitions: implementation state

Running record of what is BUILT, what is DECIDED, and where the work stands. Update this file in the same
commit as the change it describes.

Branch `lazy-geometry2`, worktree `questdb-wt-lazygeom2`, based on `f8cf9e468e` - the head of `master`.

## The goal

A partition can be several PIECES over one set of column files. Incoming rows are appended at the tail, the
pieces they do not touch are left exactly where they are, and the partition's geometry lives in a
`_geometry` file inside its own directory. `_txn` carries a pointer to it.

The model is parquet's, throughout: a composite partition is the parquet FILE, a piece is a ROW GROUP.
`_geometry` opens only when a query or a commit lands on THAT partition, exactly as row-group metadata is
read when a parquet partition is scanned and not at load.

## Why this is a second attempt

The first attempt (`questdb-wt-lazygeom`, branch `lazy-geometry`) built the same feature by growing it out
of an existing partition-split implementation. That left an expansion step in `TxReader` - every `_txn`
load read every composite partition's `_geometry` and spliced the results into one flat piece list - and a
piece-indexed API spread across ~160 call sites in `TableWriter` alone. Removing the expansion meant
converting all of them first, so the lazy read could never actually land.

This tree starts from clean `master` and adds only what is needed, in dependency order. **The expansion is
deliberately not ported.** `attachedPartitions` stays exactly as master has it: one 4-long record per
partition. Every index in the new code is the ordinary partition index, and the word "piece" appears only
inside `PartitionGeometry`, the planner, the executor and the two frame cursors that consume it.

## Status

| # | item | state |
|---|---|---|
| 1 | `PartitionGeometryFile` - the `_geometry` codec | **BUILT** |
| 2 | `_txn` slot 3 carries the composite flag + offset | **BUILT** |
| 3 | `PartitionGeometry` - the lazy resolver | **BUILT** |
| 4 | Read path: map `[0, E)`, one frame per piece | **BUILT**, untested against a real composite partition |
| 5 | `processCompositePartition` + the decision list | **BUILT** |
| 6 | Transaction clustering feeding the cut list | **BUILT**, 11 tests |
| 7 | `FrameAlgebra.merge` | **BUILT**, fixed-width and var-size |
| 8 | Executor: KEEP / NEW_PIECE / MERGE | **BUILT** |
| 9 | Geometry pointer back through the partition sink | **BUILT** |
| 10 | Routing behind `cairo.o3.partition.merge.append.enabled` | **BUILT**, default OFF |
| 11 | End-to-end test | **GREEN** - 2 cases, rows and geometry both asserted |
| 12 | Var-size column merge | **BUILT**, all four var types |
| 13 | Index maintenance for merged rows | **BUILT** - build, seal, rebuild and covering sidecar all handle a composite directory |
| 14 | In-order append into a COMPOSITE last partition | **BLOCKED like parquet**, but still POSITIONED at `E` - see below |
| 15 | Merge that reads below a column top | **BUILT** - top-aware kernels, both sides |
| 16 | Interval scan over a composite partition | **BUILT** - `CompositeTimestampFinder` |
| 17 | `ALTER TABLE ... ALTER COLUMN TYPE` over a composite partition | **BUILT** - the conversion spans `E` |

With the flag off - the default - nothing above is reachable and the tree behaves exactly as master.

## Commits

```
            Merge and append var-size columns
635631dca5  Make the composite write produce the right rows
23388e278b  Record the implementation state
8572f15955  Keep _geometry out of TxReader and TxWriter
e5a8f69414  Integrate the plan with its execution, and test it end to end
4151a7c636  Read each piece through its own frame, sized to that piece
6bf690434b  Execute the plan: KEEP writes nothing, NEW_PIECE appends, MERGE interleaves
47141d2e0d  Route every native partition through the composite path when enabled
39e0efdc06  Carry the geometry pointer back through the partition sink
d5bbcf55e5  Add merge to the frame algebra, and implement it for fixed columns
705e51035f  Decide the cut list from transaction clustering as well as the batch
ced4ca39fc  Add processCompositePartition and the decisions it makes
f5bb905d84  Read a composite partition, one frame per piece
074b2db140  Resolve a composite partition's pieces on demand
b07153036a  Read and write the _geometry file, and point _txn at it
```

## WHERE THE WORK STANDS

`O3CompositePartitionTest` is GREEN, end to end: a WAL table, fixed-width columns, both a backdated batch
landing inside a day and a batch landing above everything the day holds. Each case asserts the ROWS against
a UNION ALL oracle and the GEOMETRY separately, because rows that read back correctly out of a partition
quietly rewritten whole would prove nothing.

Regression, all with 0 failures: 364 tests across `io.questdb.test.cairo.o3`, 641 across the reader, writer
and WAL-writer suites, 1161 across the frame, squash and clusterer suites.

The route from red to green was seven defects, worth recording because most were not where the symptom
pointed:

1. **`merge` read each source over the WHOLE merge index.** `getContiguousDataAddr(srcLo + mergeIndexRows)`
   asked each side for its own rows PLUS the other side's. The bound is each source's own `Hi`, so
   `FrameColumn.merge` now takes both, and the row-0 base is derived per column - the shuffle picks by
   ABSOLUTE row id, so a column with a top maps from its top and steps its base back to where row 0 would
   be.
2. **`insertResolved` never stamped the key it is found by.** `commitUpdate` inserted a resolved slot with a
   zeroed `(partitionTimestamp, nameTxn)`, so the `publish` that followed could not find it and asserted.
   The key is now stamped by the insert rather than by each caller.
3. **`NO_GEOMETRY_REF` collided with a real value.** It was `Long.MIN_VALUE`, which is exactly
   `PARTITION_COMPOSITE_FLAG` with generation 0 at offset 0 - the ref of a partition's FIRST record. So
   every first composite write published a pointer the sink then discarded as "nothing changed", and the
   partition read back as one flat piece. It is 0 now: a real ref always carries the flag, so it is never
   zero.
4. **An unbounded piece claimed every incoming row.** A partition with no geometry records no `tsHi` -
   `_txn` holds a row count and nothing about timestamps - so its single piece could be neither cut nor
   kept, and the FIRST write to any partition rewrote the whole of it. The planner now reads the bound off
   the piece's last row: one 8-byte read, and only for a piece the geometry does not already describe.
5. **`getGeometry()` trimmed the live `path`.** `openPartition0` passes `path` to `openPartitionColumns` and
   evaluates `getPartitionPhysicalRowCount` in the same argument list, so the first resolve truncated the
   partition directory out from under the open. Both owners now build the root afresh.
6. **`FrameAlgebra.append` could not read the O3 buffers at all** - `ContiguousFileFixFrameColumn.append`
   handled only a file source. It now handles a memory source, as one contiguous run: the O3 rows reach a
   partition task already in timestamp order, which is the same assumption the per-column O3 path makes.
7. **The designated timestamp is not in the O3 columns.** `publishOpenColumnTasks` takes it from
   `sortedTimestampsAddr`, never from `oooColumns`, because that slot is the 16-byte index on one path and a
   WAL segment's own encoding on another. Reading it as a column produced timestamps of 0, 1, 2 microseconds
   - row ids, not timestamps. The O3 frame now carries the index (`FrameColumn.isTimestampIndex`), `append`
   de-interleaves it, and `merge` takes the timestamps straight out of the merge index, which already holds
   both sides'.

Neither the geometry work nor the frame work was at fault in 1, 4, 6 or 7: each was a place where the
composite path had to make the same choice the existing O3 path already makes, and made a different one.

## Decisions

### `_txn` carries the pointer; the OWNERS do the I/O

`TxReader` and `TxWriter` carry the composite flag and the `_geometry` offset in and out of slot 3, and
reach no file. The resolver belongs to `TableReader` (reads) and `TableWriter` (writes), which already know
the table's path. Each creates it on first use, so a table with no composite partition allocates nothing.

Slot 3 of a NATIVE record is 1 composite bit, 19 generation bits and a 44-bit byte offset. It stays the
parquet FILE SIZE whenever either parquet bit of slot 1 is set, so every read of it checks that first - a
parquet partition is materialized whole and is never composite.

### Non-composite is not a special case

A partition with no geometry is the ONE-PIECE case: the piece starts at the partition's own timestamp, at
file row 0, and spans every row it holds. The plan takes that as input like any other, and the moment it
cuts, the partition IS composite. The cut costs one extra entry over files that never move. So promotion
happens in flight, and there is no conversion step and no code that exists only to perform one.

### A cut lands where the DATA says, not where the timestamps suggest

`rowsBelow` apportions a piece's rows evenly across its timestamp range. That is fine for deciding whether
a cut is worth proposing and useless for deciding where it lands: any gap in the data makes it wrong, and
a gap is exactly the shape a cut aims at. `applyCut` used it for both, so a cut at a hole's edge landed
hundreds of rows inside real data.

So the caller resolves every cut against the partition's designated-timestamp column - one `SCAN_UP`
binary search over the piece's own file rows - and hands `applyCut` the row plus each half's own bound.
The column is mapped ONCE per partition and serves the missing-`tsHi` reads as well.

The search is bounded to `[rowOffset, rowOffset + rowCount)` deliberately. A merge-append relocates a
piece to the tail of the shared files, so file order is not timestamp order across the partition and a
search over the whole column would cross into another piece's rows.

**A piece's bounds describe the rows it holds, not the range it routes.** The halves of a cut taken across
a data gap are bounded by their own last and first rows, so the gap belongs to neither, and a later batch
landing in it founds a piece of its own instead of merging into a neighbour that holds nothing near it.
The same rule binds a MERGE: its image spans both sides, so it records the OUTER pair. The low side needs
the min as much as the high side needs the max, because a batch in a gap is folded into the piece ABOVE it
when that piece is small enough that rewriting it beats carrying an extra piece - and those rows sit below
that piece's old floor. Keeping the floor left the piece claiming a `tsLo` above rows it held.
Recording `cutTs - 1` and `cutTs` instead makes the lower half claim a hole it has no rows in, and the
batch merges into it - rewriting the whole piece to add rows sitting hours above everything it holds.

### Two cut sources, different questions

Transaction clustering (`WalTxnClusterer`) bins the partition's range into strides, marks every bin an
incoming transaction covers as hot, and cuts at the edges of the cold gaps worth keeping. It sees the whole
block, so it spares data no single batch straddles. The batch edges then cut around where one batch lands
inside a piece. Clustering runs first, as the coarser division.

The two are wired differently because they know different things. The transaction ranges are known on the
WRITER thread, so `commitWalInsertTransactions` buffers them (`bufferClusterTxnRanges`) before the O3
fan-out and the workers only read them. The partition's own data range is known on the WORKER, after step 1
has resolved the piece bounds against the timestamp column, so the histogram is built there - over the rows
that exist, not over the day. A range-replace transaction contributes its DECLARED range rather than the
span its rows cover, because the apply rewrites the whole declared range.

Clustering is worth its keep: the block-apply scenario in `O3PartitionPreSplitTest` writes 5114 rows with
it suppressed and 1352 with it on. Batch edges alone cannot see the cold stretch BETWEEN two hot strides -
one batch's own edges say nothing about the gaps its neighbours leave.

A clustering cut is a TIMESTAMP, not a piece index - it was chosen from incoming work, not from the piece
list - so `applyCutAt` locates the piece containing it, re-finding it per call so cuts are
order-independent, and declines a timestamp no piece holds rather than guessing.

### One writer at E, any number of readers below it

Everything is written at the TAIL, above every row the partition already holds, so nothing live is
overwritten and a reader pinned on the old geometry keeps addressing the bytes it always did.

Each MERGE opens its OWN read-only frame reaching no further than `rowOffset + rowCount` of the piece it
reads. Sizing the mapping to the piece rather than to `E` is what keeps the cost proportional to the data
being rewritten, which is the claim the design rests on. KEEP opens nothing at all.

### The plan carries row OFFSETS, not just sizes

The executor reads a piece straight out of the files, so it has to know where the piece IS. A cut splits
the offset with the rest: both halves address the same files at the same places, so the lower half keeps
the offset it had and the upper half starts that many rows further in.

An earlier version read at the piece's CUMULATIVE row, which is only correct while nothing has ever been
rewritten - exactly the state this design stops being true.

### Column tops belong to the column

Each side of a merge offsets by its OWN top, as `append` already does:

```java
source1Lo -= sourceColumn1.getColumnTop();
source2Lo -= sourceColumn2.getColumnTop();
appendOffsetRowCount -= columnTop;
```

A row below a column's top is not in that column's file, so the top is the gap between the row a caller
names and the row the file holds - and the column is what knows it. Nothing a level up reasons about them.

## Var-size columns

`ContiguousFileVarFrameColumn` now implements both primitives, and one implementation covers every var type:
the interleaving itself is `ColumnTypeDriver.o3ColumnMerge`, which is the same call `O3CopyJob.mergeCopy`
makes, so VARCHAR, STRING, BINARY and ARRAY each get the kernel they already had.

What the frame layer has to supply is the ADDRESSES, and the rule is the same one the fixed-width merge
follows: the merge index carries absolute row ids and an aux entry carries an absolute data offset, so both
vectors are handed the address their ROW 0 and BYTE 0 would be at, not the address of the slice being read.
For a source with a column top that means stepping the aux base back by the top's worth of entries. For the
DESTINATION it means handing the kernel `mappedAddr - targetDataOffset`, because the offsets it writes into
the aux entries are the same values it addresses its own writes by.

The merged image is exactly as long as the two slices put together - every row is written once and carries
its own bytes - so the data allocation needs no scan of the merge index to size it.

`append` gained the memory-source branch the fixed-width one already had, since a NEW_PIECE reads the O3
buffers rather than a file. There are no fds to copy between there, so it maps the destination and reads the
source from its addresses; the aux vector goes down through `shiftCopyAuxVector`, exactly as the file case
does it.

`O3CompositePartitionTest` carries one column of each: three VARCHARs covering the inlining regimes
separately (all inlined, so the data vector stays empty at zero bytes; none inlined; and mixed with nulls),
a STRING and a BINARY for the N+1 aux shape, and a `DOUBLE[]`. Their values are functions of `x` alone, so
the UNION ALL oracle reproduces them exactly, and a separate aggregate pins their non-null counts and sums
so a column that came back empty could not pass.

## The ported pre-split suite

`O3PartitionPreSplitTest` carries 36 scenarios ported from the earlier split implementation - everything
there that does not turn on a replace commit or on compaction. **32 pass, 4 fail, and NOTHING is
`@Ignore`d**: the suite states the truth about the feature rather than hiding it, so the red list IS the
to-do list. `O3CompositePartitionTest` adds a column-top merge case and is fully green, 4 of 4.

The rest of the `io.questdb.test.cairo.o3` package - 365 further tests across 11 classes - is green.

Only parquet conversion, replace commits and compaction are out of scope. BITMAP and POSTING are both in:
this tree is based on `f8cf9e468e`, whose own subject is a POSTING fix.

All 4 are ordinary assertion failures, and every one of them is DEDUP - no index, addressing or
whole-partition-read scenario is red any more. Three of the original reds used to take the JVM down with a
SIGSEGV, which also took every other test in the fork with it, and two more read past a mapping. A
whole-class run therefore reports honestly now; before, it reported nothing at all.

```
for m in $(grep -o 'public void test[A-Za-z]*' <test> | sed 's/public void //'); do
  mvn -o -pl core -Dtest="O3PartitionPreSplitTest#$m" surefire:test
done
```

### 14. The ACTIVE partition is written at its LIVE row count, not at `E` - FIXED

Every in-place write into the last partition takes its file position from
`txWriter.getTransientRowCount()`, the partition's LIVE row count. A composite partition's files run to
`E`, and a merge that relocated a piece to the tail put live rows between the two - so the write landed
inside that piece and the close that followed truncated whatever it did not reach.

A composite last partition now refuses those writes exactly as a PARQUET one does, through
`isLastPartitionAppendBlocked()`. Sites:

- **`noLag`**. The WAL LAG lives INSIDE the last partition's column files, appended past the live row count
  and committed in place later. This is the one that was doing the damage; blocking it is what turned
  `testPreSplitsLastLogicalPartition` and `testTailPiecePublishesGeometryOnceALaterPartitionArrives` green.
  The refusal is for the WHOLE TABLE once the flag is on, not only once the partition is already
  composite. It has to be: the danger arrives with the commit that CREATES the composite partition, and
  at that point the partition still looks ordinary, so a per-partition test is one commit too late.
  Merge-append tables therefore take no lag at all - the batching of small WAL transactions is given up
  for as long as the flag is set.
- **the per-partition `append` flag** in the O3 fan-out, beside `!isParquet`. `srcDataMax` is the live row
  count and would be the append's file row.
- plus the drop-partition reopen and the column-file reopen after a metadata change.

**`openLastPartitionAndSetAppendPosition` is blocked too, and the guard that stopped it from being is
relaxed.** A composite partition's own writes go through `processCompositePartition`, which calls
`frameFactory.openRW` and opens its own frames, so the writer's mapped `columns` do nothing for it. What
made the block impossible before is one named check at the top of every WAL commit:

```java
if (isLastPartitionClosed()) {
    if (isEmptyTable()) { ... }
    else if (!isLastPartitionParquet()) { throw "cannot resolve WAL table last partition"; }
}
```

Its own comment says why it is there: "WAL processing needs last partition to store LAG data". Block the
open without touching it and every commit throws, `ApplyWal2TableJob` suspends the table, and the
transaction never lands - which reads as lost rows, not as an error, in
`testAddColumnAfterMergeAppendRelocatedAPiece`. The exemption is now `!isLastPartitionAppendBlocked()`,
parquet OR composite, and it is sound for the same reason the block is: `noLag` is already TABLE-WIDE the
moment the flag is on, so a merge-append table has no LAG to park there.

`txWriter.initLastPartition` stays unblocked.

**The block does NOT replace the positioning below - it covers a different case.** On its own it takes the
suite to 10 failures plus 5 ERRORS, worse than either change alone, because it only governs a FRESH writer
open. A partition that turns composite while the writer already holds it mapped stays mapped at the old
position, and `testMergeAppendsActivePartition` and `testPreSplitsLastLogicalPartition` fail in exactly
that variant.

**Blocking an append is not the same as refusing a POSITION**, and the two sites that conflated them were
the last of this defect. A composite partition holds native files, so the writer maps them like any other -
and every close truncates each column from wherever the append memories were left. Refusing to position
them does not protect the files; it leaves them wherever the last commit put them, or at zero.

- `openLastPartitionAndSetAppendPosition`, and the end-of-O3-commit pair in `finishO3Commit`, now position
  at `getLastPartitionFileRowCount(...)`, which lifts the caller's own row count to `E` for a composite
  partition and returns it untouched otherwise. It takes the count as an ARGUMENT: the constructor path
  includes the WAL lag and `finishO3Commit` does not, and deciding that here regressed two `O3FailureTest`
  lag scenarios.
- The `o3ConsumePartitionUpdateSink` arm that positions the last partition is skipped for a composite one
  entirely - all three of its branches speak the live row count - and `finishO3Commit` repositions it a
  moment later. That arm has to read the geometry reference off the SINK rather than off `txWriter`, which
  does not learn it until further down the same loop, so a partition that BECAME composite in this commit
  would be missed.

The end-of-commit `setAppendPosition` had been blocked outright, which is what left `finishO3Commit`'s
reopen just above it running with no position at all: an active partition whose memories were closed
mid-commit was reopened and then truncated to ZERO bytes on the next writer close.

Both remaining scenarios turned green on this - `testMergeAppendedPieceSurvivesActivePartitionClose` and
`testDedupCommitOfOneTimestampAppliesAsABlock`, the suite's last two "a fault occurred in an unsafe memory
access operation" errors. The suite now reports failures only.

### 15. A merge that reads below a column top - FIXED

The kernels already existed and simply had not been carried into this tree; `ooo.cpp` carried two comments
saying the `WithTop` family was "removed and now executed as Merge Copy without Top". They are back:
`merge_shuffle_top_vanilla` plus the var / varchar / array `merge_copy_*_top_vanilla` trio, 10 JNI entries,
10 `Vect` declarations, and `ColumnTypeDriver.o3ColumnMergeWithTop` with its four implementations.

Their contract is what makes back-filling unnecessary. The data side arrives UNBIASED - the column file's
first stored row IS logical row `srcDataTop` - and the kernel subtracts the top itself, emitting NULL below
it. The O3 side never has a top, so only one side ever needs the treatment. `ContiguousFileFixFrameColumn`
and `ContiguousFileVarFrameColumn` pick the top-aware call when `source1Lo` falls below the top, and the
var side sizes its data buffer with `getDataVectorMinEntrySize()` per null - the bytes a null costs STRING
and BINARY, and zero for VARCHAR and ARRAY.

### 16. An interval scan resolved its rows against the FILE order - FIXED

`NativeTimestampFinder` binary-searches one contiguous address range and returns file rows. That holds for
an ordinary partition, where file order IS timestamp order. It does not hold for a composite one: a
merge-append parks a rewritten piece at the tail, above pieces that sort before it. So a
`WHERE ts BETWEEN ...` over a merged piece returned that piece's rows AND whatever else shared its file
range - a 200-row window came back with 3080 rows, over-counting by exactly the untouched upper piece.

`CompositeTimestampFinder` is the fix, and it is the native analogue of `ParquetTimestampFinder`, which
searches one row group at a time for the same reason. Every row index it takes and returns is a DIRECTORY
row, which is the space the partition frame already speaks; pieces are ordered by timestamp and do not
overlap, so that space is ascending end to end and a binary search over it is sound. The search runs per
piece, over the one range of file rows that is both contiguous and sorted, and shifts the answer back. Most
pieces need no read at all - the stored `tsLo` / `tsHi` bracket them, so a piece wholly at or below the
value contributes its whole clipped range and the first piece wholly above it ends the walk.

`AbstractIntervalPartitionFrameCursor.initTimestampFinder` selects it on `hasGeometryChain`, after the
parquet test. Six ported scenarios turned green on this alone, and it is what makes
`O3CompositePartitionTest` fully green.

**Nothing per-piece may be linear.** A directory holds thousands of pieces once a fine cut floor has been
in use for a while, so `findPiece` and `findPieceByRow` are binary searches and `getPieceCumulativeLo` is a
constant-time read of a derived slot in the in-memory piece stride. Summing that slot on demand is what the
first version did, and since `getPieceShift` calls it and BOTH frame cursors call `getPieceShift` per
frame, the read path was quadratic in the piece count. The slot is derived and never stored - the
`_geometry` record is still the four longs it always was.

### 17. A column conversion walked the LIVE row count, not `E` - FIXED

`ConvertOperatorImpl.convertColumn0` took its per-partition extent from
`TableWriter.getPartitionSize(partitionIndex)`. That is the live row count, and for an ordinary partition
it is also the number of rows the column files hold, so the two readings never had to be told apart.

A composite partition breaks the coincidence. Its live rows are scattered over `[0, E)` and a merge-append
parks a rewritten piece above every other, so the last live row sits at `E - 1` while the live count is far
below it. Converting `[columnTop, liveRows)` therefore rewrote the dead space it happened to cross and left
every live row above `liveRows` unconverted - in one scenario the destination file ended 1226 rows short of
the source, and the rows past the end read back as symbol key 0, which decodes to whatever value the table
interned first. No exception anywhere: the conversion reported success and the table came back with wrong
values, which is the worst shape this class of defect takes.

The extent is now `max(getPartitionSize, getPartitionPhysicalRowCount)`. `getPartitionPhysicalRowCount`
answers `E` for a composite partition and the record's own size otherwise, and the `max` covers the active
partition, whose transient row count `_geometry` is not authoritative for. `changeColumnType` commits
before it converts, so for an ordinary partition the two terms agree and the extent is unchanged.

The same reading governs the branch below it: a column absent from the partition records its new top as
that extent, so its first future row lands at file row 0 of a file that does not exist yet.

Three ported scenarios turned green: both `testChangeColumnType*` cases and
`testInPlaceAppendDoesNotOverReserveForItsSibling`, whose oracle comparison runs after a
`VARCHAR -> SYMBOL` conversion.

### 19. Index maintenance took a piece's rows where it needed `E` - FIXED

Three separate sites, all the same substitution, all ported from the parent tree's M28/M35/M39. One
directory holds one file per column and ONE index that every piece reads, so any range applied to that
index has to be the directory's whole shared frame `[columnTop, E)`.

- **The index BUILD range.** `indexNativePartition`, `indexLastPartition` and both of
  `RebuildColumnBase`'s (ADD INDEX and REINDEX) built over the LIVE row count, so every row a merge
  relocated above it stayed out of the index its own piece scans. `getPartitionFileRowCount` is the
  accessor. REINDEX runs standalone with no `TableWriter` to borrow a resolver from, so `reindex0` opens
  its own `PartitionGeometry`. `indexLastPartition` also gained the closed-partition branch a composite
  last partition now needs - it reads the column file directly rather than following a mapping that
  section 14 deliberately does not create.
- **`sealPostingIndexForPartition`'s `partitionSize`.** POSTING-only, and one number doing three jobs:
  `rollbackConditionally` evicted every entry above the live count - precisely the relocated piece's -
  a rebuild covered only the rows below it, and the `columnTop >= partitionSize` skip fired for any
  column whose top was recorded at an earlier `E`. That last one is the sharp edge: `ADD COLUMN` records
  its top at `E` and a composite partition always has `liveRows < E`, so a column added to one got NO
  `.pk` at all. This is why the BITMAP half of each scenario passed while POSTING failed.
- **`PostingIndexWriter.close()` trimmed the `.pk` to its own stale in-memory `regionLimit`**, cutting the
  tail off a gen-dir slot another writer instance had published. The entry's `GEN_COUNT` survives the cut,
  so the next reader counts a generation that reads back as zeroes and trips
  `assert txnAtSeal >= prevTxnAtSeal` in `PostingGenLookup.snapshotMetadata`. `readPublishedRegionLimit`
  re-reads the on-disk header under its seqlock and the trim takes the max. **This one is not a composite
  bug at all** - it reproduces with the flag off, and it was simply missing from this tree.

Most of the parent's index work needed no porting: its `(dirTs, nameTxn)` dedupe and its per-piece
`SymbolColumnIndexer.partitionTop` shift are artifacts of the split design, and here one record IS one
directory and the indexer has no shift.

### 20. The covering sidecar asserted ascending timestamps - FIXED

`CoveringCompressor.compressLongsLinearPred` asserted `lastValue >= firstValue` under the comment "Caller
guarantees sorted ascending", and `PostingIndexWriter.compressSidecarBlock` routes the designated timestamp
there unconditionally. One `.pk` serves a whole directory, so a key's posting list mixes pieces - and a
merge-append parks a relocated piece at the TAIL of the shared files, which makes the sequence step
BACKWARDS at the piece boundary. The assert fired inside `sealFull -> reencodeAllGenerations`, poisoned the
posting writer and SUSPENDED the table on an `ADD INDEX ... POSTING INCLUDE (ts)` apply.

The precondition was never real: the assert only ever compared the two ENDPOINTS, and linear prediction is
lossless for any input - decode is `first + i*stride + residual` and nothing binary-searches a sidecar. It
is now a fall-back to plain `compressLongs`, which encodes such a run at least as tightly because a
backwards stride carries no signal.

**Any "a partition's rows are physically in timestamp order" assumption is false for a composite
directory.** This assert and section 16's interval scan are the two that had been written down.

### 21. A replace-range commit's own lower bound leaked into the table's min timestamp - FIXED

`processCompositePartition` reported the caller's raw `o3TimestampMin` as the partition's new floor in the
sink it hands back to `TableWriter`. That is right for an ordinary append or merge, where `o3TimestampMin`
IS the incoming batch's own minimum. It is wrong under a REPLACE RANGE commit: `commitWalInsertTransactions`
widens `o3TimestampMin`/`o3TimestampMax` to the declared replace range so the O3 fan-out can walk existing
partitions that hold no incoming rows at all (see `TableWriter.java:9459` on), and hands that widened value
straight to the composite path with no per-partition adjustment - unlike the ordinary O3 merge path, which
computes a real `calculateMinDataTimestampAfterReplacement` for exactly this reason.

So a replace range whose lower bound falls inside a partition, but that contributes no O3 rows to it
(`srcOooLo > srcOooHi`), reported that BOUND - not a timestamp the partition holds - as its floor. When that
partition is the table's first, `o3ConsumePartitionUpdateSink`'s `isFirstPartitionReplaced` branch takes the
sink's value as the table's new `minTimestamp` outright. `TableWriter.processWalCommit`'s post-replace
assert checks that the new min timestamp is either outside the replace range or equal to the transaction's
own min - and a bare replace-range bound is neither, so it fired and suspended the table.

The fix reports the partition's own first piece (`ctx.pieces.getQuick(0)`, the first `tsLo` in ascending
order) instead of the caller's `o3TimestampMin`. Pieces describe the rows a partition actually holds
regardless of why the write happened, so this is correct for every path through this method, not only the
replace-range one - and unlike `calculateMinDataTimestampAfterReplacement`, it costs nothing extra, since
`ctx.pieces` is already built by the time the sink is written.

**This does not implement replace-range deletion over a composite partition** - that stays the known gap
below. A partition already composite, or promoted in flight, whose data falls inside a replace range with no
O3 rows of its own is left untouched (`keep`, no merge, no new piece) rather than trimmed. The fix only
makes what gets REPORTED about that untouched partition honest, so the table stops corrupting its own
`minTimestamp` and suspending itself over data it was silently keeping - not asking a bug found by chance
to also fix an unrelated documented gap.

## Known gaps

- **Replace-range commits over a composite-eligible partition perform no deletion.** A commit that declares
  a replace range but contributes no rows of its own to a partition inside that range takes `KEEP` for every
  existing piece, leaving the "replaced" data in place. Section 21 fixed the resulting min-timestamp
  corruption and table suspension; the missing deletion itself is still open.

- **The dedup no-op fast path** does not recognise a piece that starts above file row 0, so a fully
  duplicate commit rewrites the piece instead of writing nothing.
- **Two dedup scenarios fail unattributed** around the batch boundary. Leads, not diagnoses.
- **`NEW_PIECE`'s floor**: the executor records the new piece's `tsLo` as the batch's FIRST timestamp, not
  the lower bound of the gap it fills. So a later row landing between the previous piece's `tsHi` and this
  piece's `tsLo` routes to the previous piece. This needs deciding deliberately rather than by accident.
- **Dedup**: the plan has no dedup term at all. `liveRows` is the plain sum of piece rows.
- **The split threshold is in ROWS derived from an average record size**, so a narrow table needs a much
  smaller `cairo.o3.partition.split.min.size` than a wide one before any cut is proposed. A var-size column
  counts as 28 bytes there whatever it actually holds, which is why the test had to raise its setting from
  1K to 8K as columns were added.

## Working notes

- `JAVA_HOME` must be Java 25: `/opt/homebrew/opt/java/libexec/openjdk.jdk/Contents/Home`.
  `/usr/libexec/java_home` returns 24.0.2 and maven-enforcer rejects it.
- `mvn -Dtest='A+B+C'` selects NOTHING and passes vacuously with `failIfNoTests=false`. Use
  `-Dtest.include="%regex[.*(A|B).*]"` and confirm against the MTIMES of `core/target/surefire-reports/*.txt`.
- A JVM crash leaves `core/hs_err_pid*.log` in the worktree; its "Problematic frame" plus the Java frames
  under it name the offending call directly, which is faster than reasoning about the native side.
- Do not edit source while a maven build is running - it fails in unrelated files.
