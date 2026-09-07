# Review: compaction job + post-commit housekeeping at 40k+ partitions

Scenario: hourly partitions, 3-5 years of history => ~43,800 partitions per table
(5 * 365 * 24). Several such tables per instance.

---

## A. Post-commit housekeeping is O(total partitions), four full passes, every commit

`TableWriter.housekeep` -> `runCompaction` (TableWriter.java:9014, :15837) runs after
EVERY commit, on the WAL-apply throughput path. For a table that has at least one
composite partition it makes four full sweeps of the partition table:

1. `txWriter.hasCompositePartitions()` (TableWriter.java:15847, TxReader.java:565-572)
   - meant as the "one cheap bit test" guard, but it is itself a linear scan with no
   index. It early-exits on the FIRST composite partition; merge-append creates
   composites at the *active* (last) partition, so the common case scans all 43,800
   entries to return `true`.
2. `PartitionCompactionPolicy.selectPartition` (:161, loop at :185-225) - full pass;
   per partition `getPartitionSize` + `getE` + `getPieceCount`, the latter two each
   going through `PartitionGeometry.resolveInternal`.
3. `foldFoldableFolders` -> `selectFoldablePartition` (TableWriter.java:8533,
   PartitionCompactionPolicy.java:264-278) - full pass from index 0, plus two
   `getTicks()` calls.
4. `makePlainFoldableFolders` -> `selectMakePlainCandidate` (TableWriter.java:9850,
   PartitionCompactionPolicy.java:314-322) - full pass from index 0, plus two more
   `getTicks()` calls.

Steps 3 and 4 run in the *steady state* - exactly when `selectPartition` found nothing
to do, i.e. almost always.

Roughly 175k partition-entry visits and 0.5-1M `LongList.getQuick` reads per commit,
independent of how much data the commit carried.

Contrast with the pre-existing split-squash in the same `housekeep`
(TableWriter.java:9011): it is bounded by `minSplitPartitionTimestamp`
(TableWriter.java:425, maintained at :10561, :16489, :16537, :16734), so it never walks
cold history. The composite path has no equivalent watermark.

**Suggested fix**: maintain a `minCompositePartitionTimestamp` (or a composite count +
first-composite raw index) in `TxWriter`, updated where the composite flag is set and
cleared, and start all three passes there. That also makes `hasCompositePartitions()`
O(1). Both `selectFoldablePartition` and `selectMakePlainCandidate` already take a
`fromIndex`, so the plumbing is mostly there.

## B. `isSuppressed` is a linear scan per composite partition, per pass

`PartitionCompactionPolicy.isSuppressed` (:333-340) walks the backoff list - up to
`MAX_TRACKED = 256` entries (:60). It is called from `selectPartition` (:199),
`selectFoldablePartition` (:272) and `selectMakePlainCandidate` (:317), i.e. once per
composite partition per pass.

With a few thousand composite partitions that is millions of comparisons per commit.
`clearBackoff` (:324) and `onDeclined` (:139) are linear too.

**Suggested fix**: back it with a `LongLongHashMap` keyed on partition timestamp, or at
minimum skip the scan entirely when `backoff.size() == 0` (the common case).

## C. Table-pressure totals only count *non-suppressed composite* partitions

In `selectPartition`:

```
if (pieces < 2 && e <= live) continue;          // :195 - plain partitions excluded
if (isSuppressed(partitionTs, nowMicros)) continue;  // :199 - BEFORE the accumulation
deadRowsTable += dead; liveRowsTable += live;   // :203-204
```

Two consequences:

- `total = deadRowsTable + liveRowsTable` (:229) is not the table's row count, it is the
  row count of its composite partitions. A table with 43,800 partitions of which three
  are composite and mostly dead trivially satisfies
  `deadRowsTable * 100 >= total * 50` (:243). Only the 50 MB
  `cairo.partition.compaction.table.dead.threshold` floor stops the rule latching on -
  which is not much at this scale. The knob reads as "table dead percent" but is not.
- A partition entering or leaving backoff silently changes the denominator, so the
  latch can flap between commits for reasons unrelated to actual waste.

**Suggested fix**: accumulate the totals before the suppression check, and decide
explicitly whether the denominator should be the whole table (then include plain
partitions' live rows) or just composite partitions (then rename the knob).

## D. The background sweep only ever REWRITEs - it never JOINs and never MAKE-PLAINs

`PartitionCompactionScanJob.scanTable` (:608-651): for a composite partition the only
gates are "is composite" and "`lastWriteMicros` older than the idle timeout" (:636).
No waste ratio, no piece count, no consultation of `PartitionCompactionPolicy`.
`dispatchComposite` then does a full REWRITE - `buildCompactedComposite` (:186) copies
every live row - and returns `null` only when the partition holds zero live rows.

Two shapes get a full data copy where the writer-side path would move no bytes at all:

- two pieces that are already adjacent in the files -> `JOIN` (free) would do.
- one piece at row 0 with dead space above it -> `MAKE-PLAIN` (free) would do. This is
  precisely the shape `MOVE-TAIL` deliberately leaves behind
  (PARTITION_COMPACTION.md, "What MOVE-TAIL leaves behind"), so the background job
  undoes MOVE-TAIL's whole point by paying the copy it was designed to avoid.

At 32 dispatches per 2-minute sweep (`MAX_DISPATCH_PER_SWEEP`, :100) that is ~23,000
partition rewrites per day. For a table that has gone idle with 3-5 years of hourly
history, the sweep will copy the entire table off disk and back over roughly two days
of continuous I/O, on a single dedicated worker
(`ServerMain.java:643-650`), whether or not there is anything meaningful to reclaim.

**Suggested fix**: gate the composite dispatch on
`PartitionCompactionPolicy.exceedsThresholds(...)` (already static and stateless,
:101-111), and add JOIN-only / MAKE-PLAIN-only swap commands for the shapes that need
no copy. `isMakePlainShape` (:286) is likewise already static and takes only
`TxWriter`/`PartitionGeometry`.

## E. The clean-parquet memo thrashes past 100k partitions

`cleanParquetPartitions` is bounded by `MAX_MEMO_SIZE = 100_000` (:103) and is cleared
**wholesale** when full (:557-559). Three hourly-partitioned parquet tables with five
years of history is ~131k partitions - past the cap. Every sweep then re-`stat`s and
re-mmaps the `_pm` footer of every partition, which the comment at :510-517 identifies
as exactly the steady-state cost the memo exists to avoid.

The comment at :104-105 says the cap is "reached only by a database with tens of
thousands of parquet partitions" - at hourly granularity a *single* table gets there in
under five years.

**Suggested fix**: per-table memo sized against that table's partition count, or LRU
eviction rather than clear-all. Better still, keep the "clean" bit in `_txn` so it
survives restarts and needs no memo.

## F. Tables after the first are starved

`sweep` (:653-668) always starts at table index 0 and terminates the *table* loop on
`dispatchBudget > 0`. If the first table in `tableTokenBucket` keeps producing 32
candidates per sweep, the tables behind it are never scanned at all - and with 43,800
partitions each, a backlog lasts days.

**Suggested fix**: round-robin the starting table index across sweeps (persist a
cursor), or give each table its own share of the budget.

## G. The idle check itself is per-partition file I/O

`geometry.of(...)` calls `discard()` (PartitionGeometry.java:377, :156-163), so the
scan job drops its resolved cache for every table on every sweep. Each composite
candidate's `getLastWriteMicros` (:636) therefore goes through `readInto`
(PartitionGeometry.java:674) - one open + mmap + read + close of that partition's own
`_geometry` file. Bounded by the dispatch budget today only because the loop stops when
the budget runs out.

If the sweep ever needs to *look at* more partitions than it dispatches (which it will,
once D's thresholds are added), this becomes 43,800 file opens per sweep per table.

**Suggested fix**: stamp last-write time into the `_txn` attached-partition record so
the recency gate needs no `_geometry` read at all, matching how the parquet branch
gets away with one `stat`.

## H. Writer open costs one `_geometry` read per composite partition

The first commit after a writer opens runs `selectPartition`, which calls `getE` /
`getPieceCount` for every partition; each composite one misses the (empty) resolved
cache and reads its `_geometry`. With thousands of composite partitions that is a
visible stall on the first commit, amortised thereafter.

## I. Minor

- `isSwapPending` (:566-573) is a linear scan of `pendingSwaps`. At defaults the expiry
  window is 60 min and the budget 32 per 2 min, so it can hold ~960 entries.
- `expirePendingSwaps` (:485-492) is O(n^2) via `removeIndexBlock` in a loop.
- Neither is significant next to A-F, but both scale with the same knobs.

---

## Priority

1. **A** - fixed per-commit cost proportional to total history, on the WAL-apply path.
2. **D** - unbounded I/O amplification; copies whole partitions for no gain.
3. **E**, **F** - the sweep degrades to a no-progress busy loop at this scale.
4. **B**, **C** - correctness/cost of the policy itself.
5. **G**, **H**, **I** - smaller, but all on the same axis.
