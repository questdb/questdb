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
| 18 | `UPDATE` over a composite partition | **BUILT** - collect/merge two-phase rewrite, physical-row addressed |
| 19 | Partition SPLIT-squash over a composite target/source | **BUILT** - opportunistic squash leaves them alone, forced squash compacts first, see below |

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

8. **A cold-opened writer never configured a BITMAP indexer for an already-composite active partition.**
   `TableWriter`'s constructor reaches `initLastPartition` -> `openLastPartitionAndSetAppendPosition` ->
   `populateDenseIndexerList` unconditionally, even when the active partition is composite and the first
   call is a no-op (`isLastPartitionAppendBlocked` - "nothing appends to it in place").
   `populateDenseIndexerList` adds every non-null `ColumnIndexer` to `denseIndexers` regardless of whether
   it was ever `configureFollowerAndWriter`/`configureWriter`-ed, so a fresh `SymbolColumnIndexer`'s
   `BitmapIndexWriter` - never `of()`-ed, `keyMem` unmapped - landed in the set every future `commit()`
   indexes. `finishO3Commit` already carries the fix for the same gap when a commit MAKES the active
   partition composite mid-session (`configureIndexersForClosedActivePartition`, called from its own
   `isLastPartitionAppendBlocked` branch), but nothing called it from the writer's OWN construction, so a
   table whose active partition was ALREADY composite BEFORE this writer instance ever opened - the writer
   pool evicting one instance and a later access cold-opening the next, exactly what a WAL-apply time-quota
   ejection does mid-fuzz-run - reached every subsequent commit with an unconfigured indexer still sitting
   in `denseIndexers`. The crash surfaced through `PartitionCompactionScanJob#dispatchComposite`'s own
   `commit()`, which runs unconditionally after compacting an entirely different, non-active, idle
   partition - `updateIndexesSlow` computes a genuinely empty `[lo, hi)` for the untouched active partition
   in that case, but `BitmapIndexWriter#getMaxValue` (`keyMem.getLong(MAX_VALUE_OFFSET)`) is called before
   the loop that would have skipped the empty range, so it dereferences the unmapped memory regardless -
   `AssertionError` in `AbstractMemoryCR#addressOf`, indistinguishable at the stack-trace level from a dozen
   other "read past the mapped extent" bugs already recorded here. Fixed by calling
   `configureIndexersForClosedActivePartition` from `openLastPartitionAndSetAppendPosition`'s own blocked
   branch too, so a cold open ends up in the same state a mid-session transition already did.
   `PartitionCompactionScanJobTest#testScanCommitCrashesOnColdOpenedWriterWithComposedActivePartition`
   reproduces it deterministically (evict the writer while the active partition is composite, then dispatch
   a compaction on an unrelated idle partition) - no fuzz replay needed once the mechanism was identified,
   though the original symptom (`MatViewFuzzTest#testStressWalPurgeJob`) only reproduced on roughly 1 run in
   25-45 even with a fixed outer fuzz seed, because the writer-pool eviction is its own race against
   WAL-apply's time-quota timer.

9. **`compactPartitionNoCommit`'s active-partition reopen let `PostingIndexWriter`'s own crash-recovery
   walk drop the compaction's freshly-published chain entries, mid-transaction, on every run.**
   `compactPartition0`'s REWRITE publishes fresh posting-index generations for every POSTING-indexed
   column (`FrameAlgebra.append`'s `upcomingTableTxn` parameter, always `txWriter.getTxn() + 1L` here -
   the standard "commit in progress" stamp: droppable by recovery if the encompassing commit never lands).
   `compactPartitionNoCommit`'s own `closeActivePartition` + `openLastPartition()` reopen immediately
   afterward, still inside the SAME uncommitted transaction, routes through `openPartition`'s per-column
   loop, which calls `indexer.getWriter().setCurrentTableTxn(txWriter.getTxn())` before
   `configureFollowerAndWriter` for every indexed column - the OLD, not-yet-incremented txn, since
   `dispatchComposite`'s own `writer.commit()` (which would bump it) hasn't run yet. For a POSTING column
   this arms `PostingIndexWriter#of`'s recovery walk, which treats any chain entry whose `txnAtSeal`
   exceeds `currentTableTxn` as abandoned by a prior distressed writer - true for a genuine crash, false
   here: this transaction is mid-flight, about to be committed successfully a few lines further up the
   call stack. So the walk drops the compaction's own not-yet-committed entries in the very transaction
   that built them (`posting index recovery [..., dropped=2]` for `sym2`+`sym_top` on every dispatch that
   reached this branch). `finishO3Commit` already carries the fix for the identical hazard on its own
   reopen (`o3FinishInFlight`, which `openPartition`'s `setCurrentTableTxn` call is explicitly guarded
   on), but `compactPartitionNoCommit` didn't set it. Fixed by wrapping this reopen in the same flag.

   Confirmed via A/B log diff across looped fuzz reruns of a fixed seed (`WalWriterFuzzTest`
   `#testWalWriteManySmallTransactions`, 859705727469541L/1787610026502L, one of the runs that also caught
   defect #10 below) rather than a dedicated unit test: `PartitionCompactionScanJob#dispatchComposite`
   succeeding at a REWRITE is the only condition under which the drop is ever logged at all (0 occurrences
   across every run where it never wins the writer-acquire race), and after the fix the same scenario logs
   `dropped=0` instead of `dropped=2` on every run where it does win. **Confirmed harmless for read
   correctness regardless of the fix**: `sealPostingIndexForPartition(partitionTs, false)` runs
   unconditionally right after this reopen and rebuilds the chain from the column `.d` file - not from
   whatever the recovery walk left - so the wrong drop was pure wasted work (an extra rebuild cycle), never
   a wrong answer, and is not the cause of defect #10's row mismatch (fixing it alone did not change that
   failure's rate). No dedicated regression test: the bug has no black-box-observable effect to assert on
   (that unconditional rebuild is exactly why), and the only way to observe it directly is the log line
   itself or a new `@TestOnly` accessor exposing `PostingIndexWriter`'s internal generation count - neither
   used here.

10. **OPEN - a second bug, same trigger as #9, still unidentified: rows genuinely differ (not just
   posting-index bookkeeping) between the WAL table and its synchronous non-WAL oracle after the same
   fuzz-generated sequence of inserts and `ALTER TABLE ... DROP PARTITION WHERE` statements, whenever the
   active composite partition gets REWRITE-compacted mid-run.**
   `WalWriterFuzzTest#testWalWriteManySmallTransactions` fails intermittently (roughly 1 run in 6-20, both
   hardcoded seeds `859705727469541L, 1787610026502L` and `859705722835375L, 1787610026498L` reproduce
   it) with a whole different row (different symbol values, different columns, different timestamp) at the
   same ordinal position - not a narrow value mismatch. `table_partitions()` on a failing run showed the
   two tables' surviving windows for that day as disjoint, non-overlapping ranges (one keeping
   `[15:18:49,16:58:53]`/343 rows, the other `[08:05:46,10:36:12]`/109 rows).

   Trigger confirmed exactly, by direct instrumentation (temporary `LOG.info()` calls in
   `PartitionCompactionScanJob#dispatchComposite`, since removed) rather than the feature-flag A/B testing
   an earlier pass at this investigation used: across 20 baseline runs, `dispatchComposite` winning the
   writer-acquire race and completing a REWRITE on the table's only (therefore active) composite partition
   occurred in exactly the 3 failing runs and zero of the 17 passing ones - a direct per-run correlation,
   not an aggregate rate. Chasing that trigger surfaced and fixed #9 above (a real, confirmed-harmless bug
   in the exact code path the correlation points at), but a rerun of the same instrumented loop after
   shipping the #9 fix still failed at a comparable rate (6/30) with the identical `dispatchComposite`
   correlation intact (6 of 7 winning runs failed; the one exception passed with the SAME code, differing
   only in scheduling) - so #9 was A cause found on this path, not THE cause of the row content itself.

   New lead from the same instrumented rerun, not yet chased down: at the exact moment `dispatchComposite`
   wins the race, `ApplyWal2TableJob` is caught actively spinning against the same writer acquisition
   (`"unsolicited table lock"`, dozens of attempts logged within single-digit milliseconds) rather than
   idly waiting between batches, and its next WAL transaction block is then applied through a writer
   instance it did not choose to have cold-reopened out from under it (`PartitionCompactionScanJob`'s own
   REWRITE forces exactly that reopen). Whether `ApplyWal2TableJob`'s own segment/offset bookkeeping for
   the block it was mid-preparing survives that swap correctly is the next thing to check - not yet
   verified either way. No deterministic reproducer exists yet; this remains a genuine cross-thread timing
   race, confirmed only by repeated fuzz/instrumented reruns, not a single seed replay (a fixed-seed replay
   of `testWalWriteManySmallTransactions` passes most of the time and needs looping ~10-20x to catch the
   failure).

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

### Squash reads a directory as one flat frame, so a composite one is neither its source nor its target

`squashSplitPartitions` folds SPLIT sibling directories of one logical partition together by reading
each one - target and every source - through `frameFactory.openRO`/`openRW` sized to
`getPartitionRowCountByTimestamp` (live rows) and starting at file row 0. That is only correct for an
ordinary partition: a composite sibling can hold dead space below that row count, or a piece a
merge-append relocated to the tail, either of which the flat read gets wrong. This runs from
`housekeep()` after every commit, so any sibling a merge-append just made composite is exactly the
directory most likely to sit next in the squash queue.

The opportunistic pass (`force=false`, the one `housekeep()` runs) never picks a composite partition as
target and stops - does not skip past - the first composite partition it meets among the sources: it
leaves the whole remaining run for `runCompaction` (which `housekeep()` calls right after squash) to
resolve, and a later commit's squash picks up where this one left off once compaction has made it
plain. Skipping over one instead of stopping was tried and rejected - a later, still-plain sibling
folded into target across a composite one in between would land target's rows ahead of that
partition's in `attachedPartitions`, even though it covers the earlier time range.

A forced squash (`force=true` - `detachPartition`, `convertPartitionNativeToParquet`) cannot leave
anything behind: both need exactly one plain directory when squash returns, the first because it
asserts the logical partition squashed to one entry before detaching it, the second because
`produceParquetFromNative` maps the result as one flat range too. So `force=true` keeps the old
behaviour instead: `compactPartitionToPlain(partitionIndex, "squash")` forces a composite target or
source plain before it is read, reusing the helper `convertPartitionNativeToParquet` already carried
for its own target under the name `compactPartitionForConversion`. MOVE-TAIL stays disallowed there for
the reason it always was, since a fresh sibling appearing mid-squash would shift the index range the
loop is iterating over.

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

## Write amplification, measured

`O3SplitWriteAmplificationBenchTest` (`core/src/test/java/io/questdb/test/cairo/o3/`) - not a regression
test, it prints numbers, with a light row-count assertion to keep it honest. Five scenarios (in-order,
slightly-out-of-order via a bounded 1000-row reorder window, multi-writer at fixed lag offsets, catch-up,
random-order), each run through an `avg.rows.piece.lim` sweep, at 3 partitions / 1M rows each / 120
virtual seconds. `getPhysicallyWrittenRows` (what `amp` is built from) is traced statically end to end -
every `FrameAlgebra.append`/`merge` call is paired with a matching increment, every no-copy action (JOIN,
MAKE-PLAIN, KEEP, DROP) correctly has none - so the numbers can be trusted.

Requires `cairo.o3.partition.split.min.size=0`: the property defaults to `1024G`
(`core/src/test/resources/server.conf`), a ~1.7-billion-row floor at this bench's row size, which blocks
every pre-split cut and forces full-piece rewrites on every touch.

A piece-count or waste-ratio breach, caught proactively inside `processCompositePartition`
(`TableWriter.wouldBreachCompactionThresholds`), defers to MOVE-TAIL instead of paying for a full
`O3PartitionJob.assembleFreshPartitionVersion` rewrite whenever `TableWriter.wouldMoveTailSucceed` says
one would work: piece 0 untouched by this commit (a `KEEP`) and its share of anticipated live rows
clearing `cairo.partition.compaction.prefix.min.percent`. When it holds, this commit's own write lands
normally over just the piece(s) it touches, and the reactive `runCompaction` pass right after (MOVE-TAIL
tried before REWRITE there) splits off only the tail - copying what actually needs to move, not the
whole directory. Falls through to the full rewrite only when no clean-enough front survives (geometry
chain exhausted, or every commit lands somewhere different, as under random-order).

**The piece-count cap scales with a folder's own live rows, never below a flat floor.** A flat
`cairo.partition.compaction.piece.threshold=1000` (this property was named `max.pieces` earlier in this
pass - renamed since "threshold" better describes a value the rule compares pieces AGAINST, not a hard
ceiling pieces can never cross) punished a large folder for a fragmentation level the query-cost
measurements below show costs it almost nothing to carry, while a much smaller flat number is right for a
typical small folder. `PartitionCompactionPolicy.effectiveMaxPieces` computes
`max(getPartitionCompactionPieceThreshold(), liveRows / getPartitionCompactionAvgRowsPieceLim())`, and both
`wouldBreachCompactionThresholds` (proactive) and `selectPartition` (reactive) use it. Both halves of that
formula are independent properties: `cairo.partition.compaction.piece.threshold` (the flat floor, default
lowered from 1000 to 20) and `cairo.partition.compaction.avg.rows.piece.lim` (the scaling divisor, renamed
from `min.rows.per.piece`, default 4096, unchanged) - the divisor was chosen from the query-cost data
below: 25 pieces (~40K rows/piece at this bench's scale) measured flat, 400 pieces (~2.5K rows/piece)
already cost 3.6x on a full scan, so 4096 sits inside the "still flat" zone with headroom.

An earlier version of this pass removed the flat floor entirely (leaving only the divisor), which broke 43
of the 59 tests in `O3CompositePartitionTest`/`O3PartitionPreSplitTest` - none of which test compaction
directly. Root cause: `liveRows / 4096` rounds to 0 or 1 for nearly every fixture in the suite (most hold
far fewer than 4096 rows), so with no floor the piece-count rule fired on the very first relocated piece
merely from having merge-append on. The old flat default of 1000 had been silently protecting every
composite-partition test in the suite, not just the compaction-specific ones. Restoring the floor as its
own configurable property (default 20, well above what an ordinary fixture's buildup produces) fixed this
without any suite-wide test-only override - one test (`O3CompositePartitionTest
#testIntervalScanAcrossManyPieces`) deliberately builds ~21 pieces and needed its own explicit
`piece.threshold=1000` override, since 20 is a real, intentionally small production default now, not a
value picked to be always-permissive.

`O3PartitionCompactionTest` pins `cairo.partition.compaction.avg.rows.piece.lim` to `Long.MAX_VALUE` in its
own `enableCompaction()`, keeping the scaled term at ~0 so the flat floor (`piece.threshold`) is the only
threshold in play - this suite tests the mechanism at small, exact piece counts on fixtures with thousands
of rows, and the scaled term would otherwise raise the effective cap well above those counts. The handful
of tests that target an exact trigger point set `piece.threshold` directly (`"2"`, `"4"`), the same values
this suite used before `avg.rows.piece.lim` (nee `min.rows.per.piece`) existed at all.

Also changed this pass: `cairo.partition.compaction.dead.rows.ratio`'s default dropped from 3 to 1, so the
waste-ratio rule fires once dead rows merely equal live rows rather than needing to triple them. The
property is now a `double` (was `int`), so fractional ratios below 1 are configurable too.

Final numbers, baseline (no compaction) vs `cairo.partition.compaction.max.pieces=20` (scaled, measured
under this property's OLD name before the `piece.threshold` rename), measured before the
`dead.rows.ratio` default change - not yet re-measured under either new default:

| scenario | pieces: baseline -> compacted | amp: baseline -> compacted | dead%: baseline -> compacted |
|---|---|---|---|
| in-order | 1 -> 1 | 1.0 -> 1.0 | 0.0% -> 0.0% (never composite) |
| slightly-out-of-order | 185 -> 148 | 1.7 -> 1.9 | 17.2% -> 14.0% |
| multi-writer | 617 -> 97 | 1.2 -> 2.5 | 5.6% -> 3.8% |
| catch-up | 377 -> 29 | 1.2 -> 2.1 | 5.3% -> 0.0% |
| random-order | 136 -> 136 | 2.2 -> 2.2 | 26.7% -> 26.7% (no stable front - every commit lands somewhere new, so MOVE-TAIL never applies, and the scaled cap is rarely even breached) |

Before scaling, the same `max.pieces=20` cost 9.9x (multi-writer) and 7.4x (catch-up) - compaction was
paying full REWRITE-or-MOVE-TAIL cost to hold every folder to the same 20 pieces regardless of size.
Scaled, it holds folders to a size-proportional cap instead (97, 29, 148 pieces above, not 20), at a much
lower amp cost, and leaves random-order (which cannot benefit) alone almost entirely.

Verified against `O3CompositePartitionTest`, `O3PartitionPreSplitTest`, `O3PartitionCompactionTest` (71
tests, the same 2 pre-existing failures as always, 0 regressions).

### Piece count vs query cost, measured

`ScratchPieceCountQueryCostTest` (uncommitted, `core/src/test/java/io/questdb/test/cairo/o3/`) - three
1M-row, one-day tables at a fixed total row count: a plain (never-composite) baseline, one forced into
25 pieces, one into ~2000. Each piece is founded by its own out-of-order WAL commit into a distinct,
non-adjacent time slot, drained individually (draining once for the whole backlog instead merges it as
one combined sort, producing a single clean piece - not what this measures) with compaction disabled so
nothing folds pieces back together.

| pieces | full scan (`sum`) | `GROUP BY` | narrow range (1%) | wide range (50%) |
|---|---|---|---|---|
| 1 | 283us | 10411us | 65us | 168us |
| 25 | 384us (1.4x) | 10413us (flat) | 53us (flat) | 185us (flat) |
| 1998 | 1686us (6.0x) | 11487us (flat) | 61us (flat) | 624us (3.7x) |

Cost is real but sublinear: 80x more pieces (25 -> 1998) costs 4.4x on a full scan and 3.4x on a wide
range, nowhere near proportional. `GROUP BY` and a narrow range are flat across the whole span - the
narrow-range result confirms `CompositeTimestampFinder`'s own claim ("two binary searches, never a
walk") empirically: finding the range boundary does not care how many pieces exist. The per-piece cost
comes from `FwdTableReaderPageFrameCursor` cutting one page frame per piece (a frame cannot span a piece
boundary), so more pieces means more, smaller frames and more per-frame column-address setup - a fixed
cost per frame, not per row. At the piece counts compaction's piece-count rule actually produces in
practice (tens, not thousands), this cost is close to noise against the write amplification compacting
away those same pieces costs.

## Working notes

- `JAVA_HOME` must be Java 25: `/opt/homebrew/opt/java/libexec/openjdk.jdk/Contents/Home`.
  `/usr/libexec/java_home` returns 24.0.2 and maven-enforcer rejects it.
- `mvn -Dtest='A+B+C'` selects NOTHING and passes vacuously with `failIfNoTests=false`. Use
  `-Dtest.include="%regex[.*(A|B).*]"` and confirm against the MTIMES of `core/target/surefire-reports/*.txt`.
- A JVM crash leaves `core/hs_err_pid*.log` in the worktree; its "Problematic frame" plus the Java frames
  under it name the offending call directly, which is faster than reasoning about the native side.
- Do not edit source while a maven build is running - it fails in unrelated files.
