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

Keep that mechanism and its writer-side execution/publication. Keep the existing
split-count and squash policy. No new background job, physical-size cap, parking
delay, or separate split executor is part of this change.

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
is fixed, not scaled with the folder's row count. Use overflow-safe comparisons.

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
(roughly more than twice the resulting tail). Do not use dead physical bytes to
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

Let the existing writer-side MOVE-TAIL path act on the forecast before applying the
affected block, then rebuild routing/planning against the resulting sibling layout.
Do not commit a split from an O3 worker while other partition tasks are in flight.
The single-transaction apply path must get the same eligibility decision as the
multi-transaction block path.

Align `wouldMoveTailSucceed` with the mover's new prefix and size tests; its current
piece-0-at-offset-0 shortcut no longer describes eligibility. Do not promise a move
that the writer cannot execute, and do not let a discretionary fresh-version
forecast pre-empt a valid MOVE-TAIL. Preserve mandatory correctness fallbacks such
as geometry-generation exhaustion.

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
  is strictly below `4 * cairo.o3.partition.split.min.size` (default **200 MiB**).

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
