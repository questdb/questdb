# Widening in-memory tier routing for live view reads

Status: in progress - phases 0, 1 and 3 shipped; phase 2 is all that is left of the
plan, and it is now UNBLOCKED: the `TIMESTAMP(col)` hole phase 0 surfaced is closed,
rejected at CREATE by `CairoEngine.validateLiveViewTimestamp`. That hole turned out to
be reachable only through `row_number() OVER ()` - every other window shape trips the
cached-window gate first, and answers with a message about incremental refresh rather
than about timestamps, so the shape reads as already-rejected until probed precisely.
Before the fix a descending output ts made `seamTs` report the slot's MAXIMUM, which
would have served every disk row below it from both tiers. Phase 3's
keystone is IN - the frame cursor is wired up and routing, so the fresh-or-fast fork
this document was written against is closed. A filtered live-view read now runs the parallel / JIT
filter over the tier's own frame and sees the un-flushed lead. All three risks
the frame cursor was expected to carry are discharged: row-id encoding turned out
not to be one (page-frame row ids are frame-encoded already), frame sizing shipped
as a documented single whole-slot frame, and slot lifetime needed no new
machinery - the pin outlives every frame consumer by construction. The wiring's
real cost was elsewhere and unanticipated: consumers that want the LV as a TABLE
(WINDOW / HORIZON JOIN slaves, the projection wrappers) cast the frame cursor to
`TablePageFrameCursor`, which forced the routed cursor to become one and forced a
decision about `toPartition`. Two live bugs turned up under that rock; see phase 3. The row-id step since made
that three: retiring the `IN_MEM_ROW_ID_FLAG` tag, which this document filed as
hygiene, turned out to fix a wrong-results bug in ASOF / LT JOIN and an
out-of-bounds read in a projection sort - the tag's value was `Long.MIN_VALUE`,
which is the "no row" sentinel its consumers key off.
What the keystone did NOT do is unlock its own table row: an interval-filtered read
(`WHERE ts > $1`) is still disk-only, because that filter is applied BENEATH the
wrapper by the base scan, and the row-count seam cut has nothing sound to cut
against. "The filter composes for free" holds only for filters above the wrapper -
which never needed it. See the correction under change 3.
Phase 3 is now COMPLETE: the concurrency and pin-leak tests were the last of it. The
concurrency arm did not find a bug in the tier - the lifetime argument held, as the wiring
step predicted - but it did find one in itself, and the finding outlives it: the soak
raced nothing at all until the writers were paced to the refresh driver, because they
finish inside its first tick at ANY row count. The reader-churn soaks alongside it share
that shape and are worth a look on their own track. What remains in this document is
phase 2 (unblocked, not started) and the two follow-ups phase 3 named.
Branch context: `puzpuzpuz_live_view`
Owner: Andrei Pechkurov

## Summary

The live view (LV) in-memory tier can serve the un-flushed lead - rows the refresh
worker has computed but not yet written to the LV's on-disk tier - so a read sees
data fresher than the last flush. Today almost no real query reaches that path.
The routing gate is narrow enough that effectively only a bare `SELECT * FROM lv`
routes through the tier; adding a projection list, a `WHERE` clause, an
`ORDER BY ts DESC`, or any aggregate silently drops the read to disk-only and one
`FLUSH EVERY` cycle of staleness.

This document explains why the gate is that narrow, distinguishes the misses worth
fixing from the ones that are working as designed, and proposes three changes in
dependency order: a column mapping, a lead-only routing mode, and - the keystone -
exposing the tier as a page-frame source.

## Background

### The two tiers

A live view keeps its output in two places. The on-disk tier is a regular
WAL-backed table that the refresh worker flushes to on the `FLUSH EVERY` cadence
(`LiveViewRefreshJob` computes `flushDue = lastFlushUs == LONG_NULL || nowUs -
lastFlushUs >= flushEveryMicros`). The in-memory tier is an N=2 slot ring of
`LiveViewInMemoryBuffer`, each slot holding a column-major slab of complete output
rows over a retention window the `IN MEMORY` clause governs.

A slot's rows split at `leadStart = rowCount - leadRowCount`:

- rows `[0, leadStart)` are the **overlap** - already on disk, held in RAM so a
  read can skip the LV table's hot tail partition(s);
- rows `[leadStart, rowCount)` are the **lead** - not yet on disk, and the only
  copy that exists.

The overlap is a performance asset. The lead is a freshness asset. They are
independent, and the current design couples them.

### How a read routes today

`LiveViewRecordCursorFactory` wraps the `PageFrameRecordCursorFactory` that
`SqlCodeGenerator` builds for the LV's table. `LiveViewRecordCursor.of` pins a
tier slot and sets `routingEligible` only when all of the following hold:

1. `isFullSchemaProjection` - the projection has the same column count as the
   buffer, matching types index-for-index, a designated timestamp
   (`timestampColumnIndex >= 0`), and `isIdentityColumnMapping` (the frame
   cursor's `ColumnMapping` satisfies `getColumnIndex(i) == i` for every `i`);
2. the base scan is forward (`diskScanAscending`);
3. `diskReaderSeqTxn(diskCursor) != LONG_NULL`;
4. the fence holds: `pinnedSlot.rowCount() > 0 && pinnedSlot.lvSeqTxn() !=
   LONG_NULL && pinnedSlot.lvSeqTxn() == diskSeqTxn`.

When `routingEligible` holds, `hasNext()` performs the **seam split**: serve disk
rows with `ts < seamTs`, stop the disk scan at the first row at or above the seam,
then serve the whole slot. The seqTxn fence is what makes the overlap safe to
serve - equal seqTxns mean the slot and the disk snapshot reflect the same LV
table version, so the overlap band agrees with disk row-for-row.

`LiveViewRecordCursorFactory.isInMemRoutable` hoists the timing-independent part
of this decision to construction time and publishes it as the EXPLAIN `inMemory`
attribute. It also picks the execution path outright:

```java
public boolean supportsPageFrameCursor() {
    return !inMemRoutable && base.supportsPageFrameCursor();
}
```

### The gate is narrower than it looks

`diskReaderSeqTxn` is the tightest constraint, and it is easy to miss how tight:

```java
if (diskCursor instanceof PageFrameRecordCursorImpl pfrc
        && pfrc.getRowCursorFactory().isEntity()
        && pfrc.getRowCursorFactory().isForwardScan()
        && pfrc.getPageFrameCursor() instanceof TablePageFrameCursor tpfc
        && !tpfc.hasIntervalFilter()
        && !tpfc.hasActivePushdownFilter()) { ... }
return Numbers.LONG_NULL;
```

Combined with `isFullSchemaProjection`, the routable set is roughly bare
`SELECT * FROM lv` with no filter and no ordering override. Everything below is
permanently disk-only:

| Query shape | Disqualifier | |
| --- | --- | --- |
| `SELECT ts, price FROM lv` | column count mismatch in `isFullSchemaProjection` | fixed by phase 1 |
| `SELECT price, ts FROM lv` | non-identity `ColumnMapping` | fixed by phase 1 |
| `SELECT * FROM lv WHERE ts > $1` | `hasIntervalFilter()` -> `LONG_NULL` | NOT fixed by phase 3 - see the correction under change 3 |
| `SELECT * FROM lv WHERE sym = 'a'` | `hasActivePushdownFilter()` -> `LONG_NULL` | row was wrong: it already routed - see the correction under change 3 |
| `SELECT * FROM lv ORDER BY ts DESC` | backward scan | phase 2 |
| `SELECT max(rn) FROM lv` | pruning drops ts, `timestampColumnIndex < 0` | phase 2 |

(The table describes the state this document was written against. The right-hand
column tracks where each row is addressed; phases 1 and 3's keystone have shipped.
Two rows did not survive contact: the `sym = 'a'` row was never disk-only in the
first place - `hasActivePushdownFilter()` is a parquet-pushdown gate, and a residual
symbol filter over a native LV sits above the wrapper rather than in the scan - and
the interval row is still disk-only today. Both corrections are under change 3.)

## Problem statement

Two distinct failure modes hide behind "the read did not use the tier", and only
one is worth engineering against.

**Transient misses are benign and self-healing.** A fence miss serves the applied
prefix of the disk snapshot, which is internally consistent and complete as of
that snapshot - stale by less than one flush cycle, never wrong:

- *slot newer than disk*: `getCursor`'s `isSlotNewerThanDisk()` retry re-opens
  against a fresh snapshot, bounded by `MAX_STALE_DISK_RETRIES = 8`;
- *slot older than disk*: the flush already landed, so disk holds those rows;
- *tier empty* (`rowCount() == 0`): there is no lead to miss;
- *rebuild skipped* because both slots are reader-pinned: a later cycle
  republishes.

These need no fix. Chasing them adds machinery for staleness the `FLUSH EVERY`
contract already bounds.

*One exception to the "never wrong" framing, found by phase 0 and not otherwise
part of this document's scope. CLOSED - see phase 0.* Everything above assumes the
LV's output designated timestamp is the base's, which nothing validated - a view
declaring `TIMESTAMP(col)` over a different column made the refresh job's O3 detection
compare a base-space commit minimum against an output-space watermark. Late rows
in the output ts space then escaped diversion, which breaks the ts-ascending
premise the tier already relies on (`seamTs` as the slot minimum, the eviction
binary search) and can put the lead below the on-disk max. Reproduced; see phase
0. What this does *not* establish is that the materialized window results are
themselves wrong - that was not tested, and the claim here is only about ordering.
No phase below created the hole and none depended on it being open: it was live on
the shipped seam path. `CairoEngine.validateLiveViewTimestamp` now rejects the shape
at CREATE, which retires this exception for every phase at once.

**Systematic misses are the actual problem.** The shape disqualifiers in the table
above are properties of the query *text*. A query that lands in that set never
sees the lead - not on an unlucky execution, but on every execution, forever. A
user who sets `FLUSH EVERY 5s` expecting five-second staleness and writes
`SELECT ts, price FROM lv WHERE sym = 'BTC'` gets five-second staleness for
reasons no part of the query suggests, and `EXPLAIN` reports `inMemory: false`
without saying why.

There is a second cost. Because `supportsPageFrameCursor()` forks on
`inMemRoutable`, the design forces an either/or: a routable read is fresh but runs
the single-threaded interpreted record-cursor path, and a non-routable read gets
the parallel filter, the JIT filter, and LIMIT pushdown but is stale. No query can
have both. The read that most wants freshness - a filtered scan over recent data -
is exactly the one the fork sends to disk. (Closed by phase 3's wiring step; the
fork's own test arm, which asserted a routable filtered read must not go parallel,
flipped its verdict there.)

**Goal.** Not "always read the tier" - that is unreachable and, for an empty tier
or a fence miss, meaningless. The goal is: *no query shape should be permanently
blind to the lead, and routing should not cost the page-frame fast paths.*

## Proposed changes

### 1. Column mapping (independent, cheap) - SHIPPED, see phase 1

`isFullSchemaProjection` requires identity mapping only because `MergedRecord`
passes record column indices straight through to the buffer's column arrays. The
tier stores the complete output row, so a pruned or reordered projection is just a
subset of what the slot already holds - the data is there, the indirection is not.

Add an output-column -> tier-column `int[]` to `MergedRecord`, built in
`of()` from the frame cursor's `ColumnMapping`. Replace the identity check with a
subset check: every projected column resolves to some tier column of a matching
type. Relax `isFullSchemaProjection` accordingly (`getColumnCount()` equality and
the index-for-index type comparison both go).

This lifts pruning and reordering together, and the ordinary `SELECT ts, price
FROM lv` becomes fresh. It is local to the cursor, introduces no new concurrency,
and phase 3 needs the same mapping to build frames - so it is not throwaway work.

Note the timestamp requirement survives this change: the seam split needs
`timestampColumnIndex >= 0`, so a projection that prunes the timestamp is still
disk-only. Change 2 addresses that.

### 2. Lead-only routing mode (independent, medium)

The seam exists to skip the LV table's hot tail partition - it is a *performance*
optimization, not a freshness mechanism. Freshness comes only from the lead.

Decoupling them yields a second routing mode: serve the disk cursor in full, then
serve only rows `[leadStart, rowCount)`. Disk holds every applied row and the lead
holds exactly what disk lacks, so their union covers every row exactly once. The
mode needs no seam, no `ts` comparison in `hasNext()`, and therefore no timestamp
in the projection and no ascending requirement:

- **ascending**: disk ascending, then lead ascending (the lead is the newest band);
- **descending**: lead reversed (`rowCount-1` down to `leadStart`), then disk
  descending;
- **aggregates**: no ordering constraint at all.

It gives up the hot-tail skip, which is the right trade for the shapes that
currently get nothing from the tier. Keep the seam split as the preferred mode
when its preconditions hold, and fall back to lead-only rather than to disk-only.

The fence still applies. `leadStart` is only meaningful against the slot's own
`lvSeqTxn`; if disk has advanced past it, rows the slot calls lead are already on
disk and the union would double-count.

*Settled by phase 0 (see below): the invariant holds, but only NON-strictly, and
only for a view whose output timestamp is the base's.* `min(lead ts) >= max(disk
ts)`, with ties both reachable and load-bearing.

The tie is benign for this mode, and it is worth being precise about why, so that
nobody builds machinery against a non-problem. The union stays exact because the
split is *by row* (`leadStart`), not by timestamp - a lead row sharing a ts with a
disk row is still a distinct row, so nothing double-counts. Both orderings stay
monotone across the shared boundary: ascending serves disk (`..., X`) then lead
(`X, ...`), descending serves the reversed lead (`..., X`) then disk (`X, ...`),
and equal-ts neighbours need no particular relative order. What the tie forbids is
purely negative: no strict `>` assertion anywhere on the lead/disk boundary, and
no splitting of the two bands on a ts comparison. The `TIMESTAMP(col)` hole below
has closed, so the invariant now holds for every view that can be created.

### 3. Tier as a page-frame source (keystone, expensive)

This is the change that removes the fresh-or-fast fork and the filter
disqualifiers together.

**Why filters disqualify rather than merely degrade.** When the optimizer pushes
an interval or pushdown filter into the base scan, the LV cursor sits *above* a
scan that has already filtered, and holds no filter of its own. Routing would emit
unfiltered tier rows next to filtered disk rows - wrong results, not stale ones.
`diskReaderSeqTxn` returning `LONG_NULL` for those cursors is a correctness
guard. Plumbing the seqTxn through filtered cursors would not help; the filter has
to *apply to the tier rows*, which the record-cursor path cannot express.

As a frame, it composes for free: the filter runs over the tier frame the same way
it runs over a native partition.

*Correction, from phase 3's wiring step: that sentence is true of one kind of filter
and false of the other, and this paragraph conflates them.* A filter that sits ABOVE
the wrapper - the residual `WHERE sym = 'a'`, which an LV can never turn into an
index intrinsic - composes exactly as described, and now runs in parallel over the
tier frame. But it was never the problem: the base scan under it is unfiltered, so
that read ALREADY routed and already saw the lead (`testFilteredReadFiltersUnflushedLead`
predates all of this). What it lacked was speed, not freshness. The filter the table
below actually names - an interval pushed INTO the base scan - is applied BY the base
frame cursor, beneath the wrapper. It shrinks both the frames and `base.size()`, so
the overlap band is no longer the scan's trailing `leadStart` rows and the row-count
seam cut has nothing sound to cut against. `diskReaderSeqTxn` still returns
`LONG_NULL` for it and the read is still disk-only after phase 3. Composing THAT
means applying the interval to the slot frame too (cutting the slot by ts, not by
row), which is real work and is not done. The keystone did not unlock its own table
row.

**Why the layout cooperates.** `LiveViewInMemoryBuffer` is already column-major,
holding a `(dataMem, auxMem)` pair per column that deliberately mirrors
`TableWriter`'s primary/secondary model - `dataMem` is always a real
`MemoryCARWImpl` carrying the payload at `row << shift` for fixed-width and SYMBOL
columns, and `auxMem` carries the per-row offset/header vector for STRING, BINARY,
VARCHAR and ARRAY (parked at the `NullMemory.INSTANCE` stub otherwise). That is
the shape `PageFrame` column addresses already want. ARRAY already binds a
`BorrowedArray` over the `(auxMem, dataMem)` pair "exactly as
`PageFrameMemoryRecord` does over its page addresses" - the resemblance is not
accidental.

*Confirmed by phase 3's first step, with one caveat that costs nothing.* STRING /
BINARY store N aux entries where the on-disk format has N+1. That is invisible to
a frame: a native frame publishes an aux extent of `N * 8` for N rows too, and no
frame consumer reads `aux[r + 1]`. The N+1 entry is producer-side sizing state -
see the phase 3 plan below for the one rule it imposes.

**Shape.** A `LiveViewPageFrameCursor` emits the base's disk frames (cut at the
seam, or in full under lead-only) followed by one synthetic frame over the pinned
slot, exposing `dataMem`/`auxMem` base addresses per column as page addresses.
`supportsPageFrameCursor()` then returns `base.supportsPageFrameCursor()` with no
`inMemRoutable` term, and `getPageFrameCursor`'s `assert !inMemRoutable` drops
away.

*Shipped as described, plus one member the shape did not anticipate: the cursor is a
`TablePageFrameCursor`, not a bare `PageFrameCursor`, because consumers cast it to
that. That assert was also not decoration - it was already firing on a reachable,
untested path. See the wiring step.*

**Hard parts.**

- *Symbol resolution (RETIRED - the extraction below shipped).* `LiveViewSymbolTable`
  overlays the disk symbol table with `LiveViewSymbolCache` for lead-only ids, and this
  used to be per-cursor state reached through `getSymbolTable`, invisible to the frame
  consumers - parallel filter workers above all - that resolve symbols through the frame
  cursor. `LiveViewSymbolTableSource` now holds it as a standalone `SymbolTableSource`
  the frame cursor binds like any other consumer, and each overlay carries its slot's
  `newSymbolMaxIds` horizon, so a worker's key scan stays bounded. What this leaves the
  frame cursor is a constructor argument, not a design problem.
- *Row ids (RETIRED - both paths are frame-encoded now; see phase 3's row-id step).*
  In-mem rows on the RECORD path used to tag the sign bit (`IN_MEM_ROW_ID_FLAG =
  Long.MIN_VALUE`) over the buffer row index. This bullet billed retiring it as
  hygiene owed to the frame cursor; it was a live correctness bug, because the tag
  put buffer row 0's rowId on `Numbers.LONG_NULL` and three consumers use that as
  their "no row" sentinel. The record path now takes a reserved frame index
  (`Rows.MAX_SAFE_PARTITION_INDEX`) like the frame cursor's slot frame does.
  What the frame cursor settled is that it inherits none of this: `PageFrameMemoryRecord.getRowId()` is
  `Rows.toRowID(frameIndex, rowIndex)` - the frame index, NOT the partition index -
  so the synthetic frame is addressable purely by being handed a frame index like
  any other. The frame's `partitionIndex` reaches only `rowIdOffset` ->
  `getUpdateRowId()`, which an LV never reads.
- *Slot lifetime (DISCHARGED by the wiring step, and it cost nothing).* The pin is
  per-cursor: parallel filter workers, and any reduce/collect stage, must not outlive
  the frame cursor that holds it, since a worker touching a released slot is a
  use-after-free rather than a wrong answer. This was billed as the highest-risk item.
  The frame cursor holds the pin for its life and drops it in `close()`, and the
  wiring's parallel-filter test drives real workers over a routed frame - the exercise
  the bullet was waiting for - with no further machinery needed: a frame consumer is
  scoped to the cursor that produced it. What the wiring DID surface was the mirror
  image, and the doc did not see it coming: consumers that want the LV as a table walk
  it by partition and must be kept off the tier entirely. See the wiring step's
  `toPartition` note.
- *Frame sizing (SETTLED: one whole-slot frame, deliberately).* The synthetic
  frame does not honour `changePageFrameSizes`; it publishes the slot as a single
  frame. It is the scan's last frame over the `IN MEMORY` window's recent tail, so
  work distribution is dominated by the disk frames ahead of it - but a wide window
  makes that frame large, and one worker carries it while the rest idle. Splitting
  it is follow-up, and the aux rebasing it needs is worked out under the alignment
  task below.
- *Data-page sizing for STRING / BINARY (RETIRED - the alignment task below shipped).*
  This used to be a rule the frame cursor had to remember: the buffer stored N aux
  start offsets where the on-disk format has N+1, so `aux[rowCount]` was never
  written and the cursor had to size the data page from `dataSize(col)` rather than
  point a driver sizing helper at the slot's aux. The buffer now writes the
  terminator, so `dataSize(col)` and `getDataVectorSizeAt(aux, rowCount - 1)` agree
  and either works. Nothing here constrains the frame cursor any more.

**What it unlocks beyond filters.** `getTimeFrameCursor` is disk-only today, with
the code noting that "a synthetic in-mem frame that bridges the lead is a deferred
enhancement". That is the same enabling work behind a different frame API, so
ASOF-JOIN-as-RHS and interval intrinsics come within reach once this lands.

### Out of scope

- **Unsupported column types.** `isTierSupported` gates the tier's type support,
  and the buffer's javadoc is explicit that "the (data, aux) storage itself
  imposes no further restriction" - so admitting a type such as INTERVAL is
  plausibly a gate change plus write-path support. Narrow enough to defer.
- **Transient fence misses.** Working as designed; see the problem statement.
- **Closing the refresh gap.** Both tiers sit downstream of the refresh worker.
  None of this makes a read see base-table rows the worker has not processed.

## Implementation plan

The phases ship independently and in value order. Each is separately revertable.

**Phase 0 - settle the lead ordering invariant. DONE.**

*Verdict: the invariant holds NON-strictly - `min(lead ts) >= max(disk ts)` - for
any view whose output timestamp is the base's designated timestamp. Equality is
reachable by design, and one escape hatch breaks the invariant outright.* Phase 2
depends on this; phase 1 does not.

- **The bound is non-strict, and the tie is load-bearing.** The O3 diversion
  compare is `txnMinTs < latestSeen` - strict - so an additive commit whose min ts
  *equals* the frontier is not diverted, and its rows append into the lead at
  exactly the on-disk max ts. This is not incidental: `hasNext()`'s
  `leadStart == 0` branch exists precisely to stop a disk row at that shared ts
  being dropped by both tiers, and the eviction clamp keeps a same-ts overlap row
  resident for the same reason. Phase 2 must not assume a strict `>`.
- **The compare is against a watermark, not disk.** `latestSeen` is the frontier
  over disk *and* lead, so the invariant is derived, not enforced - nothing
  asserts it.
- **`rebuildInMemoryTier` is vacuous for this question.** It restages from the
  rewritten disk and sets `leadRowCount = 0`, so a rebuilt slot has no lead band.
  This is also why heavy-O3 workloads rarely hold a lead at all.
- **~~Open hole: a `TIMESTAMP(col)` view breaks it.~~ CLOSED - the shape is now
  rejected at CREATE.** Nothing validated that the LV's output designated timestamp
  *is* the base's - the pass-through was emergent from unrelated checks.
  `... AS SELECT ts2, ... FROM base TIMESTAMP(ts2)` was accepted, and the frontier
  compare then read `txnMinTs` in the base's ts space against `latestSeen` in `ts2`
  space. With `ts2` descending against an ascending base ts, nothing diverts, and the
  lead lands *below* the on-disk max with the slot's rows in descending order - which
  also breaks the ts-ascending premise the eviction binary search and `seamTs` already
  rely on. It was a live bug for the *existing* seam path too, not just for phase 2.
  `CairoEngine.validateLiveViewTimestamp` now rejects it, comparing the output
  designated timestamp's name against the base's; phase 2 is unblocked.
  - *The reproduction, before the fix, was blunter than the write-up above.* With `ts`
    ascending and `ts2` descending the slot's ts ladder ran backwards outright and
    `seamTs` reported the slot's MAXIMUM rather than its minimum - so the seam split
    would serve every disk row below that maximum from BOTH tiers. Not a subtle
    ordering wobble: duplicate rows.
  - *Why it survived, and this is the part worth keeping:* the shape is reachable only
    through `row_number() OVER ()`. That window needs no ORDER BY, so it stays
    single-pass once `ts2` is designated. Every window that orders by the base's `ts`
    needs a sort under the overridden timestamp and the cached-window gate rejects it
    first - with a message about incremental refresh that says nothing about
    timestamps. So the obvious probes of this hole all come back "rejected" for the
    wrong reason, which is a good way to conclude it is already closed. It is not.
  - *The name comparison is exact, which is not obvious and is what makes the fix a
    one-liner.* An alias or an expression in the projection (`SELECT ts AS t, ...`)
    fronts the scan with a `SelectedRecordCursorFactory` / `VirtualRecordCursorFactory`,
    which `validateLiveViewFactory` already rejects - so every projected column is a
    plain base column carrying its base name, and no alias can rename the output ts out
    from under the check. A window function cannot occupy the ts index either: the
    `TIMESTAMP(col)` clause binds to a base column, so the index always lands on a
    pass-through.
  - Scope of the claim, unchanged: the reproduction establishes the *ordering* breakage.
    It does not establish that the materialized window results were wrong - plausible,
    since the rows are processed in arrival rather than output-ts order, but untested.
    Moot for the routing work now that the shape cannot be created.
  - Tests: `LiveViewValidationTest.testRejectOverriddenDesignatedTimestamp` covers the
    three reachable divergent shapes and two positive controls - a view that merely
    PROJECTS a second timestamp column, and one that names the base's own designated
    timestamp explicitly (`TIMESTAMP(ts)`), which must not be caught by the reject.
    Disabling the check fails the arm. A `Chars.equals` mutant survives (an equivalent
    mutant: the projection carries the base metadata's stored name, so the two sides
    are byte-identical today) - `equalsIgnoreCase` stands because it is the right
    comparison for a SQL identifier, not because a test forces it.
- **Regression coverage:** `LiveViewFuzzTest.testFuzzLeadOrderingUnderO3` drives
  late-arriving commits at a resident un-flushed lead and asserts
  `min(lead ts) >= max(disk ts)` on every cycle where the seqTxn fence holds. The
  existing arms could not cover this: their `refreshCycle` advances the clock past
  `FLUSH EVERY`, so every drain flushes and the lead is always empty at the
  observation point. The arm holds the flush off on most cycles, and asserts it
  performed at least one real compare - without that guard a mis-tuned harness
  (e.g. an `IN MEMORY` window narrower than the lead's ts span, which makes the
  worker emergency-flush) silently reports green while testing nothing.

**Phase 1 - column mapping. DONE.**

*Pruned and reordered projections now route; `SELECT ts, price FROM lv` sees the
lead. A timestamp-pruned projection is still disk-only - that is phase 2's.*

- The mapping landed as an `IntList` on the cursor, shared with both
  `MergedRecord`s, built in `LiveViewRecordCursor.of` from the frame cursor's
  `ColumnMapping`. The record's flyweight views stay keyed by OUTPUT column: a
  projection may repeat a tier column (`SELECT ts, x, x`), and two output columns
  must not share one flyweight.
- `isFullSchemaProjection` + `isIdentityColumnMapping` collapsed into
  `isTierAddressableProjection` (subset + per-column type check).
- **`isInMemRoutable` needed no widening** - it never checked the projection shape,
  only scan direction, `timestampColumnIndex >= 0`, and per-column tier-type
  support. So a pruned read was ALREADY reporting `inMemory: true` and already
  taking the record-cursor path (`supportsPageFrameCursor` forks on the same flag)
  while routing disk-only: the worst of both. That is the gap this phase closed;
  the execution-path fork itself is untouched. Only the javadoc moved.
- **The symbol overlay was the sharp edge, as predicted.** `LiveViewSymbolCache`
  and the slot's `newSymbolMaxId` horizon key off the TIER column, while the disk
  symbol table is fetched by OUTPUT column - they diverge the moment a projection
  prunes a column ahead of a SYMBOL. A mis-keyed overlay does not just return a
  wrong string: `isSymbolColumn` says false, so no overlay is built at all, the
  lead's cache band drops, and a lead-only symbol resolves against disk alone and
  matches nothing.
- Tests: `testPrunedProjectionServesLeadFromRam`,
  `testPrunedSymbolProjectionFiltersOnLeadOnlyValue` (drives `keyOf` via WHERE,
  where a mis-keyed overlay silently matches nothing rather than reading wrong),
  and `testReorderedSameTypeProjectionRoutesDiskOnly` rewritten as
  `...ServesLeadFromRam` - its verdict flips from "must route disk-only" to "must
  route and read the right column". A new `buildMixedFlushedPlusLead` helper gives
  an `(ts, x, g SYMBOL, rn)` LV so a projection can prune AROUND a column and move
  the symbol off its tier index; pruning only trailing columns leaves an identity
  mapping and tests nothing.
- Both mappings were mutation-checked: neutering the record mapping to identity,
  and re-keying the symbol overlay by output column, each fail the new tests. The
  898-test LV suite is green.

**Phase 2 - lead-only mode. UNBLOCKED, not started.** Phase 0 is done and the
invariant holds; the `TIMESTAMP(col)` hole it surfaced is now closed at CREATE
(see phase 0), so nothing gates this but the work itself.
- Add the mode to `LiveViewRecordCursor` alongside seam routing; prefer the seam
  when its preconditions hold, fall back to lead-only, then to disk-only.
- Drop the `timestampColumnIndex >= 0` and forward-scan preconditions on the
  lead-only path only.
- Teach `size()` and `skipRows()` the mode - both currently assume
  `diskRouted = diskSize - leadStart`, which is a seam-split identity.
- Honour the non-strict bound: no strict `>` on the lead/disk boundary and no
  ts-based split between the bands (the row-index split already handles the tie -
  see change 2 above).
- Tests: `ORDER BY ts DESC` and `SELECT max(rn)` both serving lead rows; a
  fuzz run comparing routed output against a disk-only read after a forced flush.
  `testFuzzLeadOrderingUnderO3` already guards the invariant this mode rests on.

**Phase 3 - page-frame source.** Depends on phase 1's mapping. First step shipped.

- **Slot-address exposure. DONE.** `dataAddress` / `dataSize` / `auxAddress` /
  `auxSize` on `LiveViewInMemoryBuffer`; read-only, no caller yet, no behaviour
  change. It shipped with the *extent* accessors the original bullet did not
  anticipate: a `PageFrame` column needs `getPageSize` / `getAuxPageSize` as much
  as the addresses, and the extents are where the traps turned out to live.
  - A fixed-width column never advances its data append cursor (it writes in
    place at `row << shift`), so `dataSize` derives `rowCount * stride`. The
    cursor reads 0 however many rows the slot holds - the mutation check for this
    reported `expected:<8192> but was:<0>`.
  - A fixed-width column's `auxMem` is the `NullMemory` stub, whose `addressOf`
    *throws*. `auxAddress` / `auxSize` return the 0 sentinel that
    `PageFrameAddressCache` already stores for a column with no aux page rather
    than delegating to it.
  - **The layout does cooperate - including for STRING / BINARY.** The first pass
    of this note claimed otherwise and was wrong; the correction is worth keeping
    because the wrong version invents work. STRING / BINARY write exactly N 8-byte
    start offsets where the on-disk format has N+1. That entry turns out to be
    *producer-side* state: a native frame cursor reads it via
    `getDataVectorSizeAt` only to size the data page of an mmap'd column file, and
    does not republish it - `FwdTableReaderPageFrameCursor` sets a frame's aux
    extent to `(hi - lo) * 8`, i.e. N entries for N rows, exactly what this buffer
    stores. No frame consumer reads `aux[r + 1]` either: every var read in
    `PageFrameMemoryRecord` is at `rowIndex << 3` and takes the value's length from
    the payload's own prefix. So the regions are frame-shaped as-is and need no
    terminator and no write-path change. The single rule this leaves for the frame
    cursor is recorded under "Hard parts" above: size the data page from
    `dataSize(col)`, never by pointing a driver sizing helper at the slot's aux.
    The alignment task below closes the gap anyway - on simplification grounds, not
    because the frame path needs it.
  - Tests in `LiveViewInMemoryTierTest`: raw reads cross-checked against the
    buffer getters at the column stride; a `VarcharTypeDriver.getSplitValue`
    decode straight off the raw addresses, bounded by the reported extents rather
    than the allocated limits; extents when empty and after `reset()` (which keeps
    the pages, so a `dataSize` reporting the allocation would still claim the old
    fill); and the STRING / BINARY terminator gap pinned against the drivers' own
    `getAuxVectorSize()`. All three mutations - append-cursor `dataSize`, N+1
    `auxSize`, stub-delegating `auxAddress` - fail them. (The alignment task below
    then inverted the terminator-gap test and split the empty-extent arm by type;
    both facts it pinned changed, and deliberately.)
- **Align STRING / BINARY with the native aux layout. DONE.** Not required by the
  frame path - the step above settled that - but it deletes the buffer's last
  hand-rolled var-size encoding and, with it, the one rule the frame cursor would
  otherwise have to remember. It landed before the frame cursor, so the cursor never
  inherits the special case at all. Every column is now driver-readable, and the
  "Hard parts" data-page-sizing rule above is retired: `dataSize` for STRING / BINARY
  and the driver's own `getDataVectorSizeAt(aux, rowCount - 1)` now agree, which the
  tests assert rather than assume. `dataSize` itself kept its append-cursor
  implementation - the two are equivalent once the terminator exists, and the cursor
  needs no aux read.
  - *Why the buffer diverged in the first place:* STRING / BINARY were the only
    var-size types it wrote without going through their `ColumnTypeDriver`.
    VARCHAR and ARRAY call `VarcharTypeDriver.appendValue` / `ArrayTypeDriver.appendValue`
    and inherit the native layout for free; STRING / BINARY hand-rolled
    `off = data.getAppendOffset(); data.putStr(v); aux.putLong(off)`, storing each
    row's START offset. The drivers instead append the value's END offset
    (`auxMem.putLong(dataMem.putStr(value))` - both `putStr` and `putBin` return
    `getAppendOffset()`), over a leading `0` seeded by `configureAuxMemMA`. Since
    `start_0 == 0` and `start_r == end_(r-1)`, the two vectors agree entry for
    entry over `[0, N)`; native just carries the extra terminator at index N.
  - Route `appendStr` through `StringTypeDriver.appendValue(aux, data, value)`.
    BINARY has no static `appendValue` to reuse (only the instance `appendNull`),
    so write `aux.putLong(data.putBin(value))` directly - the same shape
    `BinaryTypeDriver.appendNull` already uses. Both shipped as written.
  - Seed the leading `0` per STRING / BINARY column. `configureAuxMemMA` takes a
    `MemoryMA` and cannot be called against the buffer's `MemoryCARW`, so this is a
    plain `aux.putLong(0)` - at construction *and* in `reset()`, which rewinds the
    aux cursor to 0 and would otherwise drop the seed on every refill. That is the
    likeliest way to get this wrong, and it was: the `reset()` seed is the only one
    of the two that a first-fill test cannot catch. Both call sites went in as
    `seedAuxLeadingOffset(col)`, a no-op for every non-STRING / BINARY type.
  - The append-order assert shifts from `(dstRow << 3) == aux.getAppendOffset()` to
    `((dstRow + 1) << 3)`, since the seed puts the cursor one entry ahead.
  - *Unanticipated, and the one caller-visible change beyond the extent:* an EMPTY
    STRING / BINARY column no longer reports a `(0, 0)` aux region. The seed writes at
    construction, so the region is allocated up front and `auxSize` reports 8 rather
    than 0 for a slot holding no rows. The `auxAddress` javadoc's "an empty slot
    reports 0" now holds for VARCHAR / ARRAY only. Nothing consumes these accessors yet
    (phase 3's frame cursor is their first caller), so this cost nothing to absorb - but
    a frame cursor must not read "aux extent 0" as "no aux vector".
  - *Cost is a wash*, which is the main reason to bother: one aux store per row
    either way, plus a one-off seed per column per fill. This is not a hot-loop
    regression.
  - *`auxSize` stays honest, and the frame publishes it as-is. SETTLED.*
    `getAppendOffset()` then reports `(rowCount + 1) * 8` for STRING / BINARY, one
    entry more than the `rowCount * 8` a native frame publishes for the same rows.
    That gap is harmless: the aux extent is consumed purely as a bounds guard - every
    STRING / BINARY read in `PageFrameMemoryRecord` is
    `if (auxPageLim < auxOffset + 8) throw` - and nothing derives a row count from it
    or reads `aux[r + 1]`. Publishing the extra entry only loosens the guard by one
    entry, and the frame's row range caps `rowIndex` at `rowCount - 1` regardless. So
    keep `auxSize` meaning "used extent", let the frame publish it directly, and skip
    the `getAuxVectorOffset(hi) - getAuxVectorOffset(lo)` derivation the native cursor
    needs. One rule fewer, not one more.
  - *The one caveat, if the slot is ever split across several frames* (see "Frame
    sizing" above): a sub-frame's aux base is `auxAddress(col) + (lo << 3)`, so its
    extent must be relative to that base - `auxSize(col) - (lo << 3)`, not the
    whole-slot `auxSize`. For the single whole-slot frame `lo == 0` and the two
    coincide, which is why the simple case needs no arithmetic at all.
  - The payoff: every column becomes driver-readable, so `dataSize` for STRING /
    BINARY could come from `getDataVectorSizeAt(aux, rowCount - 1)` like anywhere
    else, and the "Hard parts" data-page-sizing rule above disappears.
  - Tests: `testStringBinaryAuxRegionOmitsNativeTerminator` inverted into
    `...MatchesNativeLayout` - it now asserts the vector MATCHES
    `StringTypeDriver.getAuxVectorSize` / `BinaryTypeDriver.getAuxVectorSize`, that
    entry 0 is the seed, that the terminator bounds the payload, and that the driver's
    `getDataVectorSizeAt` agrees with `dataSize`. The extent gap that remains moved to
    its own `testStringBinaryAuxSizeReportsNativeVector`, so the two facts (shape
    matches, extent deliberately does not) cannot be conflated again.
    `testStringBinaryRefillAfterResetPreservesValues` covers fill -> `reset()` ->
    refill; it refills with SHORTER values than the first fill, so a dropped seed
    resolves live bytes and fails on the value rather than tripping a bound and passing
    for the wrong reason. The round-trip and realloc-mid-fill arms passed untouched, as
    predicted.
  - Mutation-checked, all three fail the suite: dropping the `reset()` seed (caught only
    by the two reset arms), dropping the construction seed (caught by 8 tests, mostly
    the order assert), and reverting `appendStr` to start offsets over the seed - which
    shifts the vector one entry and makes every read resolve the PREVIOUS row's payload
    (`expected:<b1> but was:<b0>`). The 910-test LV suite is green.
- **Move the symbol overlay off cursor state. DONE.** `LiveViewSymbolTableSource`
  now owns the resolution as a standalone `SymbolTableSource`, bound by
  `LiveViewRecordCursor` today and by the frame cursor next; the `newSymbolMaxIds`
  bound stays per slot. No behaviour change - the record cursor resolves exactly what
  it did before. The bullet said "to frame-cursor state", which the frame cursor does
  not exist to hold yet; the step that survives is making the overlay *bindable* by
  one, which is the whole of what blocked it.
  - The pass-through case turned out to carry the interesting decision. A read that
    does not route binds a null cache, which was previously spelt
    `routingEligible && symbolCache != null` at each call site. Folding it into the
    binding is not just tidier: it keeps resolution off a slot the statically-disk-only
    branch of `of()` has ALREADY released. A frame cursor that bound the cache
    unconditionally would read a slot it does not pin - the same class of bug as the
    lifetime risk under "Hard parts", reached from the other side.
  - The horizon bound has a per-call site as well as a per-bind one:
    `newSymbolTable()` reads `slot.newSymbolMaxId(tierColumn)` fresh on every call,
    which is exactly what a parallel filter worker does at an arbitrary point in the
    read. Sourcing it from the cache's live `newSymbolMaxIdExclusive()` there would let
    a worker resolve ids its slot never carried. Sourcing it from the slot cannot,
    which is why this stayed a one-line read rather than a snapshot.
  - Tests: `LiveViewSymbolTableSourceTest` drives the source against a real cache and a
    real slot over a stub disk source - no CREATE LIVE VIEW, no refresh worker. It pins
    the tier-column keying and the per-slot horizon (both fail as an EMPTY result, not
    a wrong one - see the phase 1 note), the ownership split (the shared
    `getSymbolTable` view borrows the disk cursor's table, a `newSymbolTable` clone owns
    and closes its own), and the rebind reset. Five mutations, all failing: keying by
    output column, bounding by the cache's live size, flipping either `ownsBase`, and
    dropping the rebind reset. The 920-test LV suite is green.
  - *One test expectation was wrong and the code was right,* which is worth keeping: a
    rebind CLOSES the stale overlay rather than merely unlinking it, so a leaked
    reference to one NPEs rather than quietly resolving the previous slot's band. The
    test asserts the live contract (a rebind returns a new overlay that sees the new
    slot's value) instead of the fail-fast, which is an implementation detail.
- **Build `LiveViewPageFrameCursor`. DONE.** Base frames cut at the seam plus the
  synthetic slot frame, carrying the pin for the cursor's life. No caller yet - the
  `supportsPageFrameCursor` switch below is what wires it in, and it stays a separate
  step so it lands against a component that already has coverage. (The bullet's "or
  full" arm is phase 2's lead-only mode, which does not exist yet.)
  - **The cut is by ROW COUNT, not by `seamTs`.** The bullet says "seam-cut" and the
    obvious reading is a per-frame binary search on the timestamp column; that is not
    what shipped, and the reason generalises. Under the fence the slot's overlap band
    IS the disk scan's trailing `leadStart` rows, so the disk band is exactly
    `base.size() - leadStart` - the identity `size()` and `skipRows()` already use.
    Cutting on it makes all three agree by construction rather than by an invariant
    holding; cutting on ts would make `size()` correct only while the two boundaries
    coincide. It also needs no timestamp read, which a parquet frame (no page address)
    and a metadata-only skip frame (addresses not populated) cannot serve, and it
    disposes of the phase 0 tie for free: the split is by row, so a lead row sharing a
    ts with a disk row stays a distinct row. `leadStart == 0` collapses to "disk serves
    everything", matching `hasNext()`'s branch for it.
  - *The `skipTarget` pass-through is sound and worth recording, since it looks like it
    should not be.* A base frame is address-less exactly when `frameRows <= skipTarget`,
    and a cut can only shrink it, so a cut frame the consumer would actually READ
    (`cutRows > skipTarget`) implies `frameRows > skipTarget`, i.e. the base populated
    it. No clamp needed.
  - **Frame sizing: one whole-slot frame, and the rationale is a trade, not an
    omission.** The slot is the scan's LAST frame and holds only the `IN MEMORY`
    window's recent tail, so a parallel filter's work distribution is dominated by the
    disk frames ahead of it. The cost is real and should not be glossed: a wide window
    makes that trailing frame large and one worker carries it while the rest idle.
    Splitting is follow-up work; it needs the per-sub-frame aux rebasing the alignment
    task's caveat already worked out, which `lo == 0` avoids entirely.
  - *Row ids turned out to be a non-problem here, which retires half of the next
    bullet's risk.* `PageFrameMemoryRecord.getRowId()` is `Rows.toRowID(frameIndex,
    rowIndex)` - FRAME-encoded, not partition-encoded - so the synthetic frame is
    addressable just by getting a frame index like any other. The frame's
    `partitionIndex` feeds only `rowIdOffset` -> `getUpdateRowId()`, which an LV never
    reads. It reports `Rows.MAX_SAFE_PARTITION_INDEX` so a lead row's update row id
    cannot alias a real disk row's.
  - The truncated-frame wrapper delegates EVERY accessor, including the covered
    (posting-index sidecar) ones. A covered frame cannot reach it (the fence admits only
    a plain entity scan), but `PageFrameAddressCache` asserts a frame's per-column
    `DataSource.COVERED` flags agree with its per-frame covered accessors, and it names
    a half-delegating wrapper as the realistic way to break that - so full delegation is
    the cheap way to stay out of it. Its column extents stay the whole frame's: consumers
    read them only as bounds guards, and nothing derives a row count from them (a vector
    aggregate counts from `getFrameSize`, i.e. the narrowed row range). Same argument the
    STRING / BINARY aux extent already settled.
  - Tests: `LiveViewPageFrameCursorTest` drives the cursor against a real tier slot over
    a stub disk frame cursor, so the seam can be placed INSIDE a frame, on a frame edge,
    or with no overlap at all - none of which a query-level test can arrange. The frames
    are read back through the real `PageFrameAddressCache` / `PageFrameMemoryPool` /
    `PageFrameMemoryRecord` stack rather than by poking the frame's accessors, so a wrong
    address or extent reads as a wrong value or a boundary exception instead of a passing
    test.
  - Six mutations, all failing: indexing the slot by output column, dropping the seam cut,
    not narrowing the straddling frame, skipping the pin release, publishing a 0 aux
    extent, and each half of the `calculateSize` / `getRemainingRowsInInterval` split.
    *The last one is the one worth keeping:* it survived at first. The two methods split
    one answer (the consumer adds `getRemainingRowsInInterval()` itself, then calls
    `calculateSize`), and `calculateSize` nets off whatever the first reports - so an
    unclamped `getRemainingRowsInInterval` cancels out and the TOTAL stays right. Only a
    direct assertion on the split catches it. Anything asserting only the total leaves
    that clamp untested.
  - *The stub cursor had the same class of bug the tests exist to catch, which is a
    warning about the scaffolding itself.* It keyed its symbol tables by STORAGE column
    while a real page-frame cursor keys everything by OUTPUT column and resolves through
    its own `columnIndexes`. That quietly modelled an identity projection: the pruned-
    projection test would have passed without exercising the mapping at all. It surfaced
    only because the mis-keyed table NPE'd - the phase 1 note's point about mis-keying
    reading as an empty result, arriving from the stub's side.
  - The 927-test LV suite is green.
- **Wire the frame cursor in. DONE.** `supportsPageFrameCursor()` follows the base and
  `getPageFrameCursor` routes in its own right: it pins, fences, and binds the cursor.
  The `assert !inMemRoutable` is gone - and it was not decoration, see below. The
  fresh-or-fast fork is closed: `SELECT * FROM lv WHERE sym = 'x'` now plans as an
  `Async JIT Filter` over a `LiveView` reporting `inMemory: true`, and a filter worker
  reads the slot's frame. `LiveViewTest`'s plan arm asserting a routable filtered read
  must NOT go Async flipped its verdict; that arm WAS the fork.
  - **Both paths now share their predicates through a new `LiveViewRouting`.** They must
    agree row-for-row on what they serve, so they must agree on when - a fence that
    drifts between them is a wrong-results bug, not a staleness one. The record path
    keeps the two checks a frame cursor cannot answer (a non-entity or backward row
    cursor factory), which the frame path does not need: `getPageFrameCursor` bypasses
    the row cursor factory entirely, so the frames ARE the full scan the row-count cut
    is taken against.
  - *The pin releases on every non-routing frame outcome, including a version-fence
    miss* - unlike the record path, which holds that one for `getCursor`'s retry. The
    frame path makes the retry decision inside the binding call, against the slot it
    still holds, so nothing downstream needs the pin. That is the "re-check the release
    still fires" item in the risks list, discharged rather than deferred.
  - **The slot-lifetime risk did not land as predicted, and the reason is worth
    keeping.** The doc expected the wiring to expose it, since only a routed read
    reaching a filter worker can exercise it. It does - the parallel filter test drives
    exactly that - but the pin is held for the cursor's life and nothing outlives the
    cursor, so it needed no new machinery. What the wiring exposed instead was a
    lifetime problem from the OTHER side: consumers that want the *table*, below.
  - **`LiveViewPageFrameCursor` had to become a `TablePageFrameCursor`, and this was the
    real work of the step.** Five call sites cast what `getPageFrameCursor` returns
    straight to that interface (WINDOW / HORIZON JOIN slaves, the
    `SelectedRecordCursorFactory` and `ExtraNullColumnCursorFactory` projections). They
    are gated on `supportsTimeFrameCursor()`, which for a `PageFrameRecordCursorFactory`
    is true for very nearly the same shapes `isInMemRoutable` accepts - so the overlap
    is near total, not a corner. Returning the routed cursor without this would have
    turned every such read into a ClassCastException.
  - **`toPartition()` carries the one behavioural decision.** It scopes the walk to a
    disk partition and drops the tier out of the read until `toTop()`. Its caller
    (`ConcurrentTimeFrameState`) derives its whole frame model from the table reader's
    per-partition row counts and then walks a partition to patch in addresses; the slot
    is not a partition of that table, so a surprise extra frame corrupts that model
    rather than enriching it. Serving it the applied prefix leaves it exactly where
    `getTimeFrameCursor` already leaves an LV. Not a hack - the same disk-only stance,
    reached through a different API.
  - **Two live bugs on that path, found by covering it, fixed in the same commit.** The
    `assert !inMemRoutable` fires TODAY for `... WINDOW JOIN lv ...`: reproduced before
    changing anything. Behind it, `newTimeFrameCursor()` was never implemented while
    `supportsTimeFrameCursor()` claimed the base's `true` - the interface gates one on
    the other - so the default `null` NPE'd in `AsyncWindowJoinAtom`'s constructor. A
    parallel WINDOW / HORIZON JOIN over a live view has never worked; the assert masked
    it in dev builds and nothing covered it. Also caught in self-review: the staleness
    retry could not use `null` as its retry signal, because
    `base.getPageFrameCursor()` returns `null` for a non-framing base - an infinite
    loop on the query thread.
  - Tests: a filtered read serving lead rows through the parallel filter, including a
    LEAD-ONLY symbol (only the bound overlay can resolve it, so a missing overlay reads
    as an empty result rather than a wrong one - the phase 1 note's point, arriving on
    the frame path); pin lifetime across routed / interval-filtered / backward-order /
    fence-miss frame reads; a partition-scoped walk serving the disk tier only, with
    `toTop` restoring routing; and a WINDOW JOIN over an LV asserting the applied prefix.
  - *One mutation survived at first, and it is the useful one.* Killing the
    `toPartition` scoping did not fail the WINDOW JOIN test: the slot's overlap band
    holds the same values as disk by construction (that is what the fence guarantees),
    so the join's sum could not tell the two tiers apart. The kill needs the unit test,
    where the slot's ts ladder runs past disk's and the walk reports 12 rows against 14.
    Any query-level assertion over an overlap band is blind this way. The other five
    mutations - the routing itself, the `toTop` reset, the backward-order guard, the pin
    release, the `newTimeFrameCursor` delegation - all failed as written. The 932-test
    LV suite is green (935 after rebasing onto two unrelated LV commits).
- **Retire the `IN_MEM_ROW_ID_FLAG` tagging in favour of frame-encoded row ids. DONE.**
  An in-mem rowId is now `Rows.toRowID(SLOT_FRAME_INDEX, bufferRow)` - the same encoding
  the page-frame scan beneath the cursor already uses - with `SLOT_FRAME_INDEX` reserved
  at `Rows.MAX_SAFE_PARTITION_INDEX`, the same index `LiveViewPageFrameCursor` already
  reserves for its synthetic slot frame. So both read paths now agree on the slot's
  address, which is all the bullet asked for.
  - *This was not cleanup. The tag was a live correctness bug, and the audit the bullet
    called for is what found it.* `IN_MEM_ROW_ID_FLAG` IS `Long.MIN_VALUE`, so buffer row
    0's rowId was exactly `Numbers.LONG_NULL` - the "no row" sentinel its consumers use.
    Three of them alias it: the light ASOF / LT JOIN cursors (keyed and non-keyed) gate
    their running slave match on `rowId != Long.MIN_VALUE`, so a master row matching the
    slot's FIRST row joined against NULL - or against the previous master's slave row,
    which is worse than a null. And `SortKeyMaterializingRecordCursor` keys its
    rowId -> ordinal map with `noEntryKey = Long.MIN_VALUE`, so that id is stored but
    never found again; `get()` falls back to `noEntryValue = -1` and `MaterializedRecord`
    turns the -1 ordinal into a NEGATIVE native offset. That one is an out-of-bounds read,
    not a wrong answer.
  - *Why the doc did not see it.* The bullet framed the tag as a tidiness debt the frame
    cursor had already settled for itself, and the tag DID round-trip correctly through
    `recordAt` - which is the only thing the cursor's own test asserted
    (`testInMemRowIdRoundTrip` even classified in-mem rows BY the sign bit, so it encoded
    the bug as the contract). The defect was never in the cursor; it was in what the value
    means to everyone else. A rowId is not private to its cursor, and the tag put it
    outside the space the engine's sentinels are chosen against.
  - The reserved index is safe because the base scan numbers its frames from 0 up and
    `Rows.toRowID` overflows into the sign bit above `MAX_SAFE_PARTITION_INDEX`, so a scan
    that could reach it is already outside the engine's rowId space. `getRowId()` asserts
    a disk rowId never occupies it. Frame encoding also puts every in-mem rowId ABOVE
    every disk one - the order the seam split serves them in, where the tag inverted it.
  - *One latent landmine recorded at the constant rather than fixed:* nothing forwards
    `setRecordAtRows` / `setParquetDecodeHint` to the disk cursor today (the `RecordCursor`
    defaults are no-ops), and that is the only thing keeping the reserved index out of
    `PageFrameMemoryPool`, which would index its address cache at 524287 against a frame
    count of a handful. Anyone adding that forwarding for the parquet decode win has to
    deal with it.
  - Tests: a linear ASOF JOIN matching the tier's buffer row 0, non-keyed and keyed - the
    `asof_linear` hint is load-bearing, since the default fast path takes the disk-only
    TIME-frame cursor and never reads the tier at all (`testAsOfJoinRhsSeesAppliedPrefixNotLead`
    pins that); a projection sort reaching `SortKeyMaterializing`, forced with two config
    overrides and guarded by a plan assertion so it cannot silently stop reaching the path;
    and `testInMemRowIdRoundTrip` re-pointed to assert every id is non-negative. All four
    fail against the tagged scheme. The keyed ASOF arm needs a master row BELOW the slot's
    first row to set up: the map stores any rowId verbatim, so only the dangling
    `lastSlaveRowID` hits the sentinel. The 946-test LV suite is green.
- **Tests: the concurrency and pin-leak arms. DONE.** ~~filtered scans (interval and
  pushdown) serving lead rows~~ - a residual filter over a tier frame shipped with the
  wiring, and the interval / pushdown arms cannot be written, because those shapes do NOT
  serve lead rows and are not made to by this phase (see the correction under change 3).
  The other two shipped as `LiveViewConcurrencyTest.testParallelFilterRacesTierSwap` (real
  filter workers over the tier's frame while the refresh worker swaps slots under them) and
  a pin error-path arm per read path in `LiveViewInMemReadTest`.
  - *The soak's first draft passed while testing nothing, and the reason generalises to the
    reader-churn soaks next to it.* It raced NOTHING: zero routed reads, and the tier still
    null when every thread stopped. The writers finish inside the refresh driver's FIRST
    tick - at any row count, since the tick drains what they wrote, so raising rows 10x
    changed the tick count from 1 to 1 - and `running` clears the moment they join. The
    driver therefore never published, and the tier does not exist until the first publish.
    Three things fix it: the writers pace to the driver (one batch per tick, which is what
    makes the batch size the publish count and why this soak's batch is 4..7 where the
    others' is 5..24), the tier is pre-warmed single-threaded, and the growth budget is 0.
    *The growth budget turned out to be a determinism knob rather than an enabler* - a
    reader's pin defeats the fast-path CAS and drops the writer onto the slow path anyway,
    so killing that branch left the swaps in place. It makes every publish a swap instead of
    only the collisions.
  - **The run asserts its own counters**, and that is the load-bearing part: a soak that has
    quietly stopped routing (or swapping) is indistinguishable from one that has not, which
    is exactly the state the first draft sat in. It counts reads that found the tier's frame
    and swaps caught between refresh passes - per PASS, since a tick runs up to 64 and the
    published index only alternates between two slots, so a per-tick sample reports the
    parity of a swap run rather than its length (single digits against ~10x that).
  - *The mutation that matters fails it:* releasing the frame cursor's pin at open instead
    of holding it for the cursor's life makes a filter worker read a slot the refresh worker
    reclaimed, and it lands as a torn `rn` sequence (`expected=489, actual=501`) rather than
    a crash. So the closed argument now has a test that would notice it reopening. Killing
    the routing fails the routed-reads counter.
  - **The pin's error paths needed a hook, and the record path's disk-cursor half needed a
    different assertion than expected.** A throw before the pin was already reachable
    through `onDiskCursorOpenedHook`; a throw WITH the pin held was not reachable at all, so
    a single-shot `onSlotPinnedHook` fires once a read holds it - covering
    `bindFrameCursor`'s release and the record path's `of()`-threw catch. The frame path's
    other fork (`cursor != null`, i.e. `of()` threw after adopting) stays undriven on
    purpose: `of()` takes ownership in its first statements, so only its own asserts can
    throw past them and driving it would amount to asserting an assert.
  - *`assertMemoryLeak` does not catch the stranded disk cursor, which is worth recording
    because it makes the obvious version of this test vacuous.* The mutation that drops
    `Misc.free(diskCursor)` SURVIVED the first draft: the base factory owns the cursor
    instance it hands out and frees it on its own `close()`, so the test's own
    try-with-resources covered the strand up. It shows only while the factory is still open
    - which is the state a cached factory leaves a live server in - so the arms assert
    `getBusyReaderCount() == 0` directly. All four cleanups then fail their arm: both pin
    releases, and both `Misc.free(diskCursor)` calls.
- Follow-up, not part of phase 3: `getTimeFrameCursor` over a synthetic frame. The
  wiring step made this more interesting than it looked: the parallel WINDOW / HORIZON
  JOIN slave path reaches the tier through `getPageFrameCursor` + `toPartition`, not
  through `getTimeFrameCursor`, so bridging the lead into a time frame means bridging
  BOTH.
- Follow-up, surfaced by the wiring step: cut the slot frame by ts so an
  interval-filtered read can route. This is the table row phase 3 was supposed to fix
  and did not.

## Risks

- **Phase 3 slot lifetime is a memory-safety risk, not a correctness risk.** A
  parallel worker outliving the pin reads freed native memory. This deserved the bulk
  of the review attention and a dedicated concurrency test. *Retired by the wiring
  step:* a frame consumer cannot outlive the cursor that handed it the frame, so
  holding the pin for the cursor's life is the whole fix. *The test now exists and agrees*
  - it found nothing, and a mutation dropping the pin at open fails it as a torn read, so
  the argument is covered rather than merely made. The risk that ACTUALLY materialised here
  was invisible to this document: consumers reaching for the LV as a table
  (`TablePageFrameCursor`, `toPartition`, `newTimeFrameCursor`), where two live bugs
  were already sitting.
- **Phase 2 rests on an invariant that is weaker than assumed.** Phase 0 retired
  the question: the bound is non-strict (ties are legitimate), and a
  `TIMESTAMP(col)` view violated it outright. Phase 2 must still handle the tie. The
  hole is now closed - CREATE rejects the shape, so the invariant holds for every view
  that can exist - and phase 2 is unblocked.
- **Widening the gate widens exposure to the fence.** More shapes routing means
  more reads pinning slots, and sustained concurrent readers straddling a swap can
  pin both slots and force the refresh worker to emergency-flush the lead every
  cycle. `of()` already releases the slot on the statically-disk-only path for
  this reason; each phase should re-check that the release still fires everywhere
  routing cannot engage. *The error paths are now covered too* (see phase 3's test step):
  a throw that strands a pin costs the same emergency flush, permanently, and it has no
  symptom of its own. The concurrency soak also puts a number on the both-slots-pinned
  stall for the first time - under 4 readers churning the frame path it is rare (single
  digits of publishes out of ~64), not the norm.
- **EXPLAIN drift.** `inMemory` is already a capability flag rather than a
  guarantee. As the gate widens, the gap between "routable" and "routed" grows.
  Consider reporting the runtime outcome instead - or in addition - once phase 3
  makes the frame path the single path.
