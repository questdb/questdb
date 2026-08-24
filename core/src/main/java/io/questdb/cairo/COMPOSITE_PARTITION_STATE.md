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
test, it prints numbers, with a light row-count assertion to keep it honest. Ported from the enterprise
`feat-partition-top-split` branch's bench of the same name, then rewritten: that branch's `_txn`-per-piece
design doesn't exist here, so piece/dead-row accounting goes through `PartitionGeometry` and
`TableReader.getPartitionPhysicalRowCount` instead of the folder-level dead/live table this branch never
had. Two bench-only static hooks the old branch added to `TableWriter` itself
(`benchSquashFullThreshold`, `benchForceBlockApply`/`benchForceOneByOne`) have no equivalent here and were
dropped rather than faked.

Five scenarios, each run through the same ladder (split-only, +compaction, +compaction hot, then a
`max.pieces` sweep at 50/20/10) at the default scale (3 partitions, 1M rows each, 120 virtual seconds):

| scenario | what it simulates | amp | dead% |
|---|---|---|---|
| in-order | one writer, strictly ascending timestamps - the baseline | 1.0 | 0.0% |
| slightly-out-of-order | one real-time stream, delivery jittered by a bounded reorder window of 1000 rows - several writers on one stream with independent network latency | 1.7 | 17.2% |
| multi-writer | 5 writers at fixed lag offsets (realtime, 5s, 1m, 1h, 1d, 2d), interleaved commit+drain | 1.2 | 5.6% |
| catch-up | the same 5-writer schedule, but committed to WAL in full before a single drain applies the backlog | 1.2 | 5.3% |
| random-order | every row at a uniformly random position across the whole span - unbounded reorder | 2.2 | 26.7% |

Two findings from building it:

**The pre-split cut has a floor, and the test harness's own default blocks it outright.**
`O3PartitionJob`'s pre-split (`computeCuts`, the batch-edge cuts) never cuts a piece below
`getPartitionO3SplitThreshold()` = `cairo.o3.partition.split.min.size / avgRecordSize`.
`core/src/test/resources/server.conf` sets that property to `1024G` - at this bench's ~30-byte rows, a
floor of roughly 1.7 BILLION rows, so no cut can ever clear it without an explicit override. Two of the
five `run*Scenario` methods were first written without `setProperty(CAIRO_O3_PARTITION_SPLIT_MIN_SIZE,
0)` - only `runScenario` had it, copied from the ported-from bench. With pre-split silently disabled,
every commit that touched an already-composite piece rewrote the piece WHOLE regardless of how much of
it the new batch actually overlapped: catch-up's first measured amp was 4.9 (dead 54.2%) purely from
this, collapsing to 1.2 (dead 5.3%) once the override was added to the two affected methods too. The
lesson generalizes past this file: any test or bench that wants pre-split to fire at a small row scale
must force `cairo.o3.partition.split.min.size` down - the default, in both production (50MB) and this
harness (1024G), assumes real-sized rows and partitions.

**The physically-written-rows metric (`TableWriterMetrics.getPhysicallyWrittenRows`, what `amp` is built
from) was traced statically end to end** across composite piece processing
(`O3PartitionJob.executeCompositePlan`), all four compaction strategies
(`TableWriter.compactPhysicalPartition`'s JOIN / MOVE-TAIL / MAKE-PLAIN / REWRITE) and the fresh-piece
rewrite path (`O3PartitionJob.assembleFreshPartitionVersion`, used when a directory's geometry generation
chain is exhausted or a commit would breach compaction's thresholds). Every branch that calls
`FrameAlgebra.append`/`merge` is paired with a matching `addPhysicallyWrittenRows` call using the exact
row count written - the post-dedup `mergeRows`, not the pre-dedup ceiling, where dedup applies - and
every branch that copies nothing (JOIN, MAKE-PLAIN, KEEP in `executeCompositePlan`, DROP) correctly has
no such call. No missing or double-counted increment found - the bench's numbers can be trusted.

**Compaction showed no measurable effect at first - root cause was a test-harness bug, not a compaction
defect.** `Overrides.setProperty` (`core/src/test/java/io/questdb/test/cairo/Overrides.java`) kept a
single shared `changed` flag, overwritten rather than accumulated on every call:
`changed = !Chars.equalsNc(value, existing)`. `applyCompactionSettings()` calls
`setProperty(MAX_PIECES, ...)` then `setProperty(COOLDOWN, ...)` in that order; across the `max.pieces`
sweep, cooldown stays `"0"` unchanged the whole time, so its call is always a same-value no-op that runs
LAST and overwrites `changed` back to `false` - silently discarding the real `MAX_PIECES` change the
call just before it made. `Overrides.getConfiguration` only rebuilds the live `CairoConfiguration` when
`changed` is `true`, so `cairo.partition.compaction.max.pieces` stayed stuck at whatever it was the last
time BOTH calls in one `applyCompactionSettings()` invocation happened to register a real change (1000,
then later `Integer.MAX_VALUE`) - the 50/20/10 sweep values were computed and matched against
thresholds on paper (traced with temporary logging in `wouldBreachCompactionThresholds` and
`shouldAssembleFreshPartitionVersion`) but never actually reached the running engine. Confirmed directly:
one folder's piece count grew to 617 identically whether `max.pieces` was 1000, 50, 20 or 10.

Fixed by accumulating instead of overwriting: `changed = changed || !Chars.equalsNc(value, existing)`
(and the same for the property-removal branch). This is a general correctness fix to shared test
infrastructure - any test that calls `setProperty` more than once per "batch" before the config is next
read, where a later call happens to be a same-value no-op, was silently losing an earlier real change.
Verified against `O3CompositePartitionTest`, `O3PartitionPreSplitTest`, `O3PartitionCompactionTest` (71
tests, the same 2 pre-existing failures as always, 0 regressions).

With the fix, `processCompositePartition`'s proactive rewrite-on-breach
(`TableWriter.wouldBreachCompactionThresholds` / `O3PartitionJob.assembleFreshPartitionVersion`, called
from inside every composite-partition commit, not from `housekeep`'s separate `runCompaction`) works
exactly as designed: piece count stays bounded near the configured cap instead of growing unbounded, and
compaction shows the real trade-off its own code comments always described - copying to reclaim dead
space costs additional physical writes, so amp rises sharply as the cap tightens while dead% collapses
toward 0:

| scenario | baseline (no cap) | pieces≤50 | pieces≤20 | pieces≤10 |
|---|---|---|---|---|
| random-order | 2.2 / 26.7% | 8.0 / 4.3% | 16.8 / 3.6% | 41.7 / 0.6% |
| catch-up | 1.2 / 5.3% | 5.9 / 2.4% | 13.2 / 2.4% | 24.3 / 0.0% |
| slightly-out-of-order | 1.7 / 17.2% | 4.8 / 2.1% | 9.1 / 1.0% | 16.9 / 0.5% |
| in-order | 1.0 / 0.0% | 1.0 / 0.0% | 1.0 / 0.0% | 1.0 / 0.0% |
| multi-writer | 1.2 / 5.6% | 8.4 / 3.0% | 21.0 / 0.0% | 41.7 / ~0.0% |

**The breach-resolution rewrite paid for far more than it had to: it always copied the WHOLE
partition, never just a tail.** `assembleFreshPartitionVersion` has no MOVE-TAIL option - every action
(KEEP included) copies into the one fresh directory it builds, so a piece-count breach on a directory
whose oldest piece is untouched by this commit still rewrote everything, not just the touched region.
Confirmed from `wouldBreachCompactionThresholds`'s own `anticipatedLiveRows` log field on the
slightly-out-of-order bench: one partition's breach events fired every ~72-74K rows (STEPS apart) but
each copied the FULL total-to-date (581K, then 653K, then 707K, ... up to 977K) - a partition that
never got cheaper to fix, only more expensive, the longer it ran.

Fixed by teaching `shouldAssembleFreshPartitionVersion` to check `TableWriter.wouldMoveTailSucceed`
(new) before committing to the full rewrite: same shape `moveTailToFreshPartition` itself requires -
piece 0 untouched by this commit (a `KEEP`, not `MERGE`/`APPEND`) and its share of anticipated live rows
clears `cairo.partition.compaction.prefix.min.percent`. When it holds, the breach is left alone: this
commit's own write lands normally over just the piece(s) it touches, and the reactive `runCompaction`
pass right after (which already has MOVE-TAIL, tried before REWRITE) splits off only the tail - copying
what actually needs to move, not the whole directory. Confirmed the reactive path fires as anticipated:
in one run, 46 commits deferred a breach to MOVE-TAIL, 22 actual MOVE-TAILs ran, 0 fell through to a
plain REWRITE.

Effect at `max.pieces=20`, before -> after:

| scenario | before | after |
|---|---|---|
| random-order | 16.8 / 3.6% | 16.8 / 3.6% (unaffected - no commit ever lands on a stable, un-random front) |
| catch-up | 13.2 / 2.4% | 7.4 / 2.4% |
| slightly-out-of-order | 9.1 / 1.0% | 3.0 / 1.8% |
| in-order | 1.0 / 0.0% | 1.0 / 0.0% (never composite - unaffected) |
| multi-writer | 21.0 / 0.0% | 9.9 / 0.0% |

slightly-out-of-order also flattens across the sweep once fixed (3.0 at both `pieces≤20` and
`pieces≤10`, instead of climbing 9.1 -> 16.9) - exactly what "copy only the tail" predicts: cost stops
scaling with how long the partition has been accumulating and starts scaling with how much actually
needs to move. random-order is the one workload this cannot help, by construction: every commit lands
at a uniformly random position across the whole span, so piece 0 is essentially never left untouched
long enough to qualify as a stable front.

Verified against `O3CompositePartitionTest`, `O3PartitionPreSplitTest`, `O3PartitionCompactionTest` (71
tests, the same 2 pre-existing failures as always, 0 regressions).

## Working notes

- `JAVA_HOME` must be Java 25: `/opt/homebrew/opt/java/libexec/openjdk.jdk/Contents/Home`.
  `/usr/libexec/java_home` returns 24.0.2 and maven-enforcer rejects it.
- `mvn -Dtest='A+B+C'` selects NOTHING and passes vacuously with `failIfNoTests=false`. Use
  `-Dtest.include="%regex[.*(A|B).*]"` and confirm against the MTIMES of `core/target/surefire-reports/*.txt`.
- A JVM crash leaves `core/hs_err_pid*.log` in the worktree; its "Problematic frame" plus the Java frames
  under it name the offending call directly, which is faster than reasoning about the native side.
- Do not edit source while a maven build is running - it fails in unrelated files.
