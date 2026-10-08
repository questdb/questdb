# Change MOVE-TAIL eligibility for merge-append

Status: implemented; matched benchmark pending. Reuse the existing MOVE-TAIL
implementation; do not introduce a second physical-split mechanism.

## Decision

Change MOVE-TAIL's criteria to match classic O3's large-prefix/small-tail write
pattern, using composite fragmentation as the trigger:

```text
projected dead rows > 10% of projected live rows
OR
projected piece count > 1000
```

Projected dead space must also exceed `cairo.o3.partition.split.min.size`, default
**50 MiB**. This minimum applies to dead space, not to the retained prefix. Keep
the classic relative size guard: leave a large prefix and move a small tail,
including the incoming rows when assessing the tail's size.

This is a MOVE-TAIL trigger, not a new whole-partition REWRITE threshold. Do not
change the general compaction thresholds to 10%/1,000.

## Why

The two-day, 414.72M-row TSBS run on frame-syscalls wrote fewer rows than nightly
(1.956x versus 3.173x), but accumulated a 41.65 GB composite folder before copying
26.54 GB of live data in one REWRITE. That took 56.2 seconds and coincided with an
ingestion-rate drop from roughly 2.3M to 1.0M rows/s.

Nightly instead performed 759 squashes, averaging 67.6 MB of source data. The aim
is to regain that incremental write pattern without giving up merge-append inside
the small active tail. These runs used different deployment/profiler modes; the
performance benefit still needs a matched benchmark.

## Reuse what already exists

`TableWriter.moveTailToFreshPartition` already:

1. Chooses a prefix/tail boundary.
2. Copies the live tail pieces into a fresh sibling directory.
3. Keeps the prefix in the original directory.
4. Publishes the prefix geometry and the new partition entry.
5. Updates `minSplitPartitionTimestamp`, so ordinary split squash finds the sibling.
6. Maintains column versions, indexes, counters and active-partition bookkeeping.

Keep that mechanism for housekeeping. For the ingestion path, the partition task
itself executes the move - see "Integrate with the current forecast and squash". Keep
the existing squash policy; there is no split gate (`COMPACTION_PRESSURE_AND_SPLITS.md`).
No new background job, physical-size cap, parking delay, or separate split executor is
part of this change.

## Criteria changes

### Fragmentation trigger

Use the normal merge plan to forecast the folder's post-commit shape before
executing it:

- `L_after`: projected live rows.
- `E_after`: projected physical extent.
- `D_after = E_after - L_after`: projected dead rows.
- `P_after`: projected nonempty pieces after normal plan coalescing.

Trigger on `D_after > 0.10 * L_after OR P_after > 1000`, with `L_after > 0`.
Exactly 10% or exactly 1,000 pieces does not trigger. The 10% is
`cairo.partition.compaction.move.tail.dead.rows.percent` (default 10); the 1,000 limit
is `cairo.partition.compaction.move.tail.piece.threshold` (default 1000), and is not
scaled with the folder's row count. Use overflow-safe comparisons.

Reuse the accounting behind `wouldBreachCompactionThresholds`, but do not substitute
these split limits into the general compaction policy. MERGE retires the old piece;
APPEND does not retire its existing rows. Deduplication must account for rows that
do not survive rather than assuming every input becomes live.

### Minimum size and economical tail

Reuse `getPartitionO3SplitThreshold()` to express the dead-space minimum in rows.
Projected dead rows must exceed this threshold (default **50 MiB** at the table's
estimated average record size). Exactly 50 MiB does not qualify. Both fragmentation
triggers require this dead-space floor. There is no absolute prefix-size minimum.

The prefix must dominate the existing tail plus incoming rows, as in classic O3
(more than `cairo.partition.compaction.move.tail.prefix.multiple`, default 2, times the
tail plus the incoming rows). Do not use dead physical bytes to
make an otherwise expensive tail copy appear economical.

### Untouched, not necessarily file-contiguous or old

Current `coldPrefixPieceCount` insists that the prefix tiles file rows `[0, n)` and
passes commit/time hotness checks. For this ingestion-driven MOVE-TAIL, use the
incoming-write boundary instead:

- The prefix contains only rows below the incoming range and is untouched by the
  affected commit/block.
- Where the existing pre-block pass considers loaded future transactions, preserve
  that lookahead guard against moving the frontier past already queued backfill.
- The prefix can consist of several pieces with holes or nonzero file offsets.
- Do not require ten quiet commits or ten seconds for this path: a large untouched
  prefix is enough, as in classic O3. Existing housekeeping-only decisions without
  an incoming range can retain their coldness checks.

Choose a valid piece boundary before the touched tail. Keep equal timestamps on
one side; touching pieces cannot be separated at their shared timestamp. If moving
the boundary left makes the prefix too small or the tail too large, decline the
move. A lack of a suitable boundary is not permission to rewrite the prefix.

This is a tail optimization, not a guarantee that the prefix will never receive
backfill. Later writes still route to older physical folders normally.

## Necessary geometry adjustment

Changing only the eligibility boolean would corrupt fragmented prefixes: the mover
currently publishes one piece at row offset zero for all retained prefix rows.

Instead, retain each prefix piece's actual timestamp bounds, row offset, row count
and provenance. Fold pieces only when they are adjacent both logically and in the
files. Leave the original `E` unchanged, exactly as MOVE-TAIL already does.

Moving the tail makes its former ranges dead in the old folder; include those rows
in waste accounting. A split alone does not reclaim disk space. MAKE-PLAIN remains
valid only when the retained geometry really is a single contiguous piece at row 0;
otherwise leave the prefix composite for normal squash/compaction.

This is the supporting publication change required by the new criteria, not a new
split algorithm.

## Integrate with the current forecast and squash

The partition task decides and executes the move
(`O3PartitionJob.processCompositePartition` -> `moveTailToFreshPartition`), off the
one plan it already built for the commit: the ts-column map, clustering, pre-split cuts,
actions and the dedup forecast are computed once per partition per commit, and each
MERGE's dedup index is built once and reused by execution. The writer does no work
before the block: `TableWriter.moveTailCut(partitionIndex, bounds, plan)` is the
decision, read-only on the writer, with the lookahead floor
(`o3MoveTailFutureFloor`) computed once per block before the tasks are dispatched.

The task writes the tail pieces `[cut, n)` together with the commit's rows ONCE into
a fresh sibling directory at the first tail timestamp, under the current txn - KEEP and
APPEND copy, MERGE with the forecast's index, NEW_PIECE append, in timestamp order, so
the sibling is a plain directory with no dead rows. The prefix pieces `[0, cut)` are
folded and republished on the original directory under its own name txn and `E`. The
sink reports a split exactly as the classic O3 split does (the sibling's timestamp and
size, the prefix's new size and geometry reference), and `o3ConsumePartitionUpdateSink`
inserts the sibling, applies the prefix's size and reference, retires the prefix's old
geometry generation and lowers `minSplitPartitionTimestamp`. The sibling's posting
indexes are sealed from its data; the prefix's files are untouched, so its indexes are
not resealed. Nothing is published until the writer consumes the sink: a failure
part-way removes the sibling's directory and leaves the prefix as it was. Several
partitions of one block may each move their tail; each inserts its own sibling.

A valid MOVE-TAIL wins over a discretionary fresh-version rewrite. The one mandatory
fallback ahead of it is geometry-generation exhaustion: the prefix's republish needs a
generation (`hasGenerationForNextPublish(partitionIndex, cut)`), and without one the
commit assembles a fresh version instead. Touching-dedup partitions take the staging
path before either decision.

After the move, incoming data goes into the small tail. Later threshold breaches
repeat the same operation. Existing squash folds eligible older siblings while
retaining the active tail:

```text
[older prefix] [current composite: untouched prefix | touched tail]
                                      MOVE-TAIL
[older prefix] [retained composite prefix] [small fresh tail]
                           ordinary squash
[growing accumulator]                    [small active tail]
```

Ordinary squash already reads a composite source's live pieces directly, skipping
its dead ranges. Do not REWRITE that source first and then squash it.

Before an ordinary squash, flatten a composite **target** when both conditions hold:

- Its geometry was not updated by the just-applied commit/block.
- Its actual directory size, including dead ranges, variable columns and indexes,
  is strictly below `cairo.partition.compaction.squash.target.size.multiple` (default 4)
  times `cairo.o3.partition.split.min.size` (default **200 MiB**).

Use the existing JOIN/MAKE-PLAIN/REWRITE path to clean the target, then append the
sources directly. A REWRITE copies the target's live rows once, and squash copies
each source's live rows once. Freeze the last-commit transaction for the entire
squash pass: maintenance transactions for an earlier day must not make a target
updated by the same data commit appear cold. Do not clean a target when its only
source is the composite active partition which squash refuses to consume.

Hot or large targets keep the existing append-only composite path. Retain reader,
checkpoint, lag-row and rollback protections; do not force a large accumulator copy
merely to meet a split-count target. Keep the general REWRITE fallback for shapes
where a safe/economical MOVE-TAIL is unavailable.

## Tests

- Projected threshold crossings, strict equality, and each trigger independently.
- Projected 50 MiB dead-space floor, strict equality, and relative tail-size guard,
  including incoming rows; no absolute prefix-size minimum.
- Fragmented/nonzero-offset prefix survives unchanged; only the tail is copied.
- Freshly written but untouched prefix qualifies without a time/commit delay.
- Timestamp ties, no legal cut, empty tail and backfill into an older sibling.
- Single-transaction and block apply agree; multiple partitions publish safely.
- Repeated MOVE-TAIL plus existing squash preserves rows, limits sibling growth and
  avoids REWRITE-then-squash double copies of sources.
- Small cold squash targets become plain; hot targets and targets at or above the
  actual on-disk size limit stay composite. A multi-day pass preserves the original
  data-commit hotness boundary. Indexed/variable columns, column tops and pinned
  readers survive the target cleanup.
- Pinned readers, checkpoints, column tops, variable-length/indexed columns, dedup,
  lag/replace fallbacks, allocation/copy failures and reopen/recovery.
- Physical-write and dead-row counters include tail copies and retained dead ranges
  exactly once.

Use the existing compaction, forecast, split and WAL fuzz suites. Add focused tests
around the changed MOVE-TAIL criteria rather than a parallel suite for a new splitter.

## Benchmark acceptance

Repeat the same TSBS workload with matched deployment, JVM, profiler and cache
conditions. Measure total ingestion/apply completion time, ten-second rates, physical
row writes, MOVE-TAIL/squash/REWRITE counts and copy sizes, peak retained space, and
actual disk bytes/latency. Check query cost and file count as well.

Success is a smoother incremental copy pattern and better total load time, not
merely a lower write-amplification number. The thresholds are soft eligibility
triggers, not a hard upper bound on physical folder size.
