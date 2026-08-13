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
| 13 | Index maintenance for merged rows | NOT STARTED |

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

## Known gaps

- **Indexed columns**: `ContiguousFileIndexedFrameColumn` inherits the fixed merge, but index entries for
  merged rows are not maintained.
- **`NEW_PIECE`'s floor**: the executor records the new piece's `tsLo` as the batch's FIRST timestamp, not
  the lower bound of the gap it fills. So a later row landing between the previous piece's `tsHi` and this
  piece's `tsLo` routes to the previous piece. This needs deciding deliberately rather than by accident.
- **Dedup**: the plan has no dedup term at all. `liveRows` is the plain sum of piece rows.
- **A merge below a column top throws**, for var-size columns as well as fixed. Each source offsets by its
  own top, but a row BELOW a top is not in the file at all and has to be written as a null, which needs a
  kernel neither merge has. `rowZeroAddr` / `rowZeroAuxAddr` refuse rather than reading the wrong bytes.
  Reachable by ADD COLUMN followed by a backdated insert.
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
