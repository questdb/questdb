# Review: compaction job + post-commit housekeeping at 40k+ partitions

Scenario: hourly partitions, 3-5 years of history => ~43,800 partitions per table
(5 * 365 * 24). Several such tables per instance.

---

## A. Post-commit housekeeping: guard is O(1) and the three passes are now watermark-bounded

`TableWriter.housekeep` -> `runCompaction` (TableWriter.java:9046, :15947) runs after
EVERY commit, on the WAL-apply throughput path.

**PARTIALLY FIXED** by commit `1d359ac2b7` ("Composite storage version marking and
in-memory tracking"). The "is there any composite work to do at all" guard is now the
in-memory `compositePartitionCount` field (TableWriter.java:395, checked at :15953),
not a scan. It is seeded by one pass at writer open / `_txn` reload / rollback
(`recountCompositePartitions`, TableWriter.java:6259) and kept in step by +/-1 on the
hot paths that flip a partition composite (`setPartitionGeometryRefTracked`, :6287) or
remove one (`removeAttachedPartitionsTracked`, :6274); a full `_txn` rebuild / truncate
re-seeds it (the `compositePartitionCount = 0` resets scattered through the file). An
`assert assertCompositeCountMatchesScan()` (:6242) cross-checks the tracked count
against a fresh scan on every commit, so a missed mutation point fails the test suite
but costs nothing in production. This replaces the old
`txWriter.hasCompositePartitions()` guard - itself an unindexed linear scan that, in
the common case, walked all 43,800 entries because merge-append creates composites at
the *active* (last) partition. The same in-memory count now O(1)-guards
`compactAheadOfBlock` too (TableWriter.java:10014). `TxReader.hasCompositePartitions()`
(TxReader.java:543) still exists but is only reached off the hot path
(`CopyExportFactory`).

**NOW FIXED** (this change). A `minCompositePartitionTimestamp` watermark bounds the
three passes so they no longer start at index 0:

- The field (TableWriter.java, next to `compositePartitionCount`) is a CONSERVATIVE lower
  bound on the earliest composite partition's timestamp: lowered when a partition flips
  composite (`setPartitionGeometryRefTracked`), reset to `Long.MAX_VALUE` when the count
  reaches zero (`removeAttachedPartitionsTracked`, `resetCompositePartitionTracking`, the
  flip-to-plain branch), and recomputed exactly by `recountCompositePartitions`. It is
  never raised on a removal, so it always sits at or below the true earliest composite - a
  pass may start a few plain partitions early (harmless, they contribute nothing) but can
  never skip a real composite.
- `compositeFromIndex()` maps the watermark to a start index via
  `TxReader.findAttachedPartitionIndexByLoTimestamp` (one binary search, ~16 comparisons
  at 43,800 partitions). `runCompaction` passes it to `selectPartition` (now takes a
  `fromIndex`), and `foldFoldableFolders` / `makePlainFoldableFolders` seed their `from`
  with it instead of 0.
- The three passes now visit only the partitions from the earliest composite onward.
  Every partition below it is plain (`pieces < 2 && e <= live`), so it contributed
  nothing to the table-wide dead/live totals anyway - bounding the start leaves the
  table-pressure denominator unchanged. See C for the separate suppression-order fix.
- `assertCompositeCountMatchesScan` (run under `assert` on every commit) gained a second
  invariant: `count == 0 || watermark <= earliestComposite`. A watermark that ever rose
  above a real composite - the only way a pass could skip one - fails the whole suite.

This mirrors the pre-existing split-squash bound in the same `housekeep`
(`minSplitPartitionTimestamp`), which is why cold history is never walked there.

## B. `isSuppressed` is a linear scan per composite partition, per pass

`PartitionCompactionPolicy.isSuppressed` (:303-310) walks the backoff list - up to
`MAX_TRACKED = 256` entries (:42). It is called from `selectPartition` (:175),
`selectFoldablePartition` (:249) and `selectMakePlainCandidate` (:287), i.e. once per
composite partition per pass.

With a few thousand composite partitions that is millions of comparisons per commit.
`clearBackoff` (:294) and `onDeclined` (:115) are linear too.

**PARTIALLY FIXED** (this change). `isSuppressed` and `clearBackoff` now return
immediately when `backoff.size() == 0` - the common case, since backoff only fills after
a partition is declined. Combined with A (the passes now call `isSuppressed` only over
composite partitions from the watermark, not all 43,800), the remaining exposure is a
populated backoff scanned per composite partition, bounded by `MAX_TRACKED = 256`. The
`LongLongHashMap` index is left as a further step if profiling shows that bound matters.

## C. Table-pressure totals only count *non-suppressed composite* partitions

In `selectPartition`:

```
if (pieces < 2 && e <= live) continue;          // :171 - plain partitions excluded
if (isSuppressed(partitionTs, nowMicros)) continue;  // :175 - BEFORE the accumulation
deadRowsTable += dead; liveRowsTable += live;   // :179-180
```

Two consequences:

- `total = deadRowsTable + liveRowsTable` (:205) is not the table's row count, it is the
  row count of its composite partitions. A table with 43,800 partitions of which three
  are composite and mostly dead trivially satisfies
  `deadRowsTable * 100 >= total * ...ThresholdPercent` (:214). Only the
  `cairo.partition.compaction.table.dead.threshold` byte floor stops the rule latching
  on - which is not much at this scale. The knob reads as "table dead percent" but is not.
- A partition entering or leaving backoff silently changes the denominator, so the
  latch can flap between commits for reasons unrelated to actual waste.

A narrow subcase HAD already been closed: the empty-table latch, where `total == 0`
trivially satisfied `0 >= 0 * pct` and turned table pressure on from a table's first
commit, is guarded by an explicit `total > 0 &&` (:213).

**PARTIALLY FIXED** (this change). Consequence 2 is closed: `selectPartition` now folds a
composite partition's dead/live rows into the table-wide totals BEFORE the suppression
check, so a partition moving in or out of backoff no longer shifts the denominator and
the latch stops flapping with backoff state. Consequence 1 (the denominator is
composite-partition rows, not whole-table rows, so the knob is misnamed) is left as-is:
changing it would re-scope and re-calibrate the table-pressure rule, a behavioural change
out of scope for a cost/correctness pass.

**Remaining decision**: accumulate the totals before the suppression check (done), and decide
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

**PARTIALLY DONE (not this change).** `PartitionCompactionScanJob.scanTable` has since
grown a MAKE-PLAIN branch: a composite candidate matching
`PartitionCompactionPolicy.isMakePlainShape` is dispatched via `dispatchMakePlain`
(copy-free, in place) instead of a REWRITE, so the second shape above is handled. Still
open: the REWRITE dispatch has no `exceedsThresholds` gate (an idle composite with tiny
waste is still fully copied), and there is no JOIN-only path for two already-adjacent
pieces.

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

The new `recountCompositePartitions` seed scan at writer open (TableWriter.java:6259)
does NOT add to this: it only reads each slot's composite flag via
`txWriter.isPartitionComposite(i)` (`_txn`, in memory), never `_geometry`. Its one
benefit here is that a table with zero composite partitions now skips `selectPartition`
altogether on the first commit (guard returns at :15953), so this stall is confined to
tables that actually carry composites.

## I. Minor

- `isSwapPending` (:566-573) is a linear scan of `pendingSwaps`. At defaults the expiry
  window is 60 min and the budget 32 per 2 min, so it can hold ~960 entries.
- `expirePendingSwaps` (:485-492) is O(n^2) via `removeIndexBlock` in a loop.
- Neither is significant next to A-F, but both scale with the same knobs.

---

## Priority / status

1. **A** - DONE. Guard was already O(1) via `compositePartitionCount` (commit
   `1d359ac2b7`); this change adds the `minCompositePartitionTimestamp` watermark that
   bounds the three per-commit passes, so they no longer walk cold history. Covered by a
   commit-time invariant assert plus two new tests in `O3PartitionCompactionTest`.
2. **D** - PARTIALLY DONE. The sweep already dispatches copy-free MAKE-PLAIN for that
   shape; the REWRITE `exceedsThresholds` gate and a JOIN-only path remain (not this
   change).
3. **E** - OPEN. Clean-parquet memo still clears wholesale at the cap. **F** - FIXED
   (round-robin sweep cursor, not this change).
4. **B** - PARTIALLY DONE (empty-backoff fast path; `LongLongHashMap` left as a further
   step). **C** - PARTIALLY DONE (totals now accumulate before the suppression check, so
   the latch no longer flaps with backoff; the composite-only denominator is a separate
   design call).
5. **G**, **H**, **I** - smaller. **H** is subsumed by A/the count seed scan; **G** and
   **I** (per-partition `_geometry` idle read, linear `pendingSwaps` scans) remain OPEN.

### Implemented in this change (A, B, C)

- `TableWriter`: `minCompositePartitionTimestamp` field + `compositeFromIndex()`, watermark
  maintenance folded into `setPartitionGeometryRefTracked` / `removeAttachedPartitionsTracked`
  / `recountCompositePartitions` / new `resetCompositePartitionTracking` (replacing the seven
  inline `compositePartitionCount = 0` resets), the extended invariant assert, and the three
  pass call sites (`runCompaction`, `foldFoldableFolders`, `makePlainFoldableFolders`).
- `PartitionCompactionPolicy`: `selectPartition` gains a `fromIndex`; dead/live totals move
  ahead of the suppression check; `isSuppressed` / `clearBackoff` gain the empty-backoff
  fast path.
- Tests: `testCompactionSkipsColdPlainsButStillReclaimsALaterComposite`,
  `testCompactionFindsAFreshCompositeAfterAllPriorOnesDrained`.
- Not touched here: D (REWRITE gate + JOIN), E (parquet memo), G, I.
