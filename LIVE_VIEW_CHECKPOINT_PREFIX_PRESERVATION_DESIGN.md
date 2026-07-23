# Live View Checkpoint Prefix Preservation Design (Finding 2.1)

Status: draft for review (no code landed yet)<br>
Author: investigation on `puzpuzpuz_live_view`<br>
Scope: `LIVE_VIEW_CHECKPOINT_TIMELINE_REVIEW_HANDOFF.md` finding 2.1<br>
Base revision: `f8f91f0550fa`

## 1. What the finding asks

The versioned-timeline design requires an out-of-order (O3) publication to replace
roots in `[C, H)` and reuse every root outside that interval. The implementation
honors this only for a *finite-H* localized repair (the range splice,
`publishRepair`). Two other localized paths instead retire the **whole** timeline,
destroying the still-valid prefix roots below the repair floor:

- `o3HeadMissReplay` when `localized && !finiteHighBound` (influence reaches EOF) --
  `retireCheckpointStateOnO3(instance, true)` at `LiveViewRefreshJob.java:4187`.
- `replayFromAnchor` (predecessor-resume) -- `retireCheckpointStateOnO3(instance, true)`
  at `LiveViewRefreshJob.java:3787`.

Both are *localized*: they have a floor `R` (`plan.getOutputLowTs()` / the
predecessor `replayLowTs`) below which nothing changes, so the roots with
`maxTimestamp < R` remain correct. Retiring them loses every long-term anchor
because of a single near-head O3 event, and forces a subsequent older O3 event to
replay from the view boundary instead of resuming from a preserved predecessor.

The finding's recommended change:

1. Add a persistent truncate/splice-above-`C` timeline operation.
2. Preserve the prefix for EOF repair and predecessor resume.
3. Publish a normalized generation over the preserved prefix and rebuilt tail.
4. Do not reset the checkpoint ID space for insert-only O3.

## 2. Mechanics established by investigation

All file:line references are at base revision `f8f91f0550fa`.

### 2.1 The "fresh history reset" is file absence, not an explicit reset

`append0` derives generation and checkpoint id purely from whether the superblock
is still valid:

```java
// LiveViewCheckpointTimelineStoreWriter.java:565-568
final long generation = metaStore.isValid()
        ? checkedIncrement(superblock.generation, "generation") : 1;
final long checkpointId = metaStore.isValid() ? superblock.nextCheckpointId : 0;
```

`retireTimeline` deletes the `_timeline` superblock plus the `meta`/`data`/`repair`
trees (`LiveViewCheckpointLifecycle.retireTimeline`), so the next append finds
`metaStore.isValid() == false` and re-bases to generation 1 / checkpointId 0 / zero
byte totals. **Consequence:** any operation that keeps the superblock *valid* and
carries `nextCheckpointId`, generation (incremented), and byte totals forward
satisfies requirement 4 for free. `publishRepair`
(`LiveViewCheckpointTimelineStoreWriter.java:265-496`) is the closest template: it
increments generation, preserves each entry's `checkpointId`/`maxTimestamp`/
`createdLvSeqTxn`, never touches `nextCheckpointId`, and applies signed byte deltas.

### 2.2 The row-position delta tree self-cancels, so it can carry forward unchanged

Each entry stores a *base* `lvRowPosition = effectiveLvRowPosition - prefixSum(key)`
(`append0:646-654`, `publishRepair:396-413`) and every reader adds
`deltaReader.prefixSum(key)` / `effectivePosition(entry)` back
(`reader restorePinned:373-376`). The subtract-at-write / add-at-read symmetry means
any delta point with a key at or above the truncate floor cancels for every entry
-- prefix, tail-gap, and future appends alike. A truncate can therefore carry the
delta root forward unchanged, exactly as `append0` does
(`superblock.rowPositionDeltaBytes` carried forward, `:695`, `:698`).

### 2.3 The timeline is a copy-on-write B+ tree; there is no truncate primitive

`LiveViewCheckpointTimelineWriter` exposes `append` (path-copy insert, splits on
overflow) and `splice` (re-version existing keys, shape preserved). Neither removes
a suffix of keys. A new `truncateAbove` primitive is required (see section 5.1).

### 2.4 The crash-safety model: retire-before-scan forces a base-table rebuild

The current EOF/predecessor paths retire the timeline **before** the replay scan on
purpose:

> "Retiring ahead of the scan also keeps a crash mid-replay cheap - a restart then
> finds no timeline and rebuilds from the applied base."
> (`LiveViewRefreshJob.java:3682-3684`)

A mid-replay crash then hits `tryRestoreFromTimeline`, finds the timeline absent
(`:5932-5939`) or fails restore (`:6022-6031`), and routes to
`rebuildTimelineRecoveryFromAppliedBase` -> `o3HeadMissReplay(..., fullRebuild=true)`
(`:6035-6053`). That rebuild reads the **applied base table** through a page-frame
cursor (`o3HeadMissReplay:4196-4215`) -- fully durable, watermark-free, always
correct.

### 2.5 Incremental restore replays raw base WAL from the *superblock* watermark

`restoreLatestCompatible` floors to a root at or below the durable frontier
(`reader:220`) and replays the tail forward via
`replayToApplied(instance, wf, restored.normalizedBaseSeqTxn, durableBase, [rootMaxTs+1, frontier])`
(`tryRestoreFromTimeline:5966-5977`). Crucially:

- `restored.normalizedBaseSeqTxn = pin.getNormalizedBaseSeqTxn()` -- the superblock's
  **global** watermark (`reader restorePinned:379`). Timeline entries carry
  `createdLvSeqTxn` but **no per-entry base seqTxn**
  (`LiveViewCheckpointTimelineStoreWriter.java:407-413`, `:653-654`).
- `replayToApplied` -> `drainBaseWal` reads **raw base WAL segment files**
  (`getCursor(baseToken, fromSeqTxn)` at `:2069`, then `wal<id>/<seg>` at `:2112`),
  not the base table. So the replay-from point must have its base WAL still
  retained.

In the normal flow this is consistent because every seal sets the superblock's
`normalizedBaseSeqTxn` to the *head root's* base snapshot, restore never floors below
the head, and base WAL from the head's watermark is retained (the view pins it via
`metaStore.getWalPurgeFloor()`).

## 3. The blocker

A prefix-preserving truncate makes an **old prefix root the timeline head** while
the durable tail (rewritten by the replay) sits above it. On a crash between the
truncate and the post-replay seal, incremental restore cannot reconstruct that tail:

- The superblock's `normalizedBaseSeqTxn` still names the *old* (higher) head. Base
  WAL below it is purged, so restore cannot replay from the prefix root's true
  (lower) seqTxn; and replaying from the old head's watermark misses every in-order
  tail row in `(S_prefix, E]`.
- Entries carry no base seqTxn, so the truncated head has no correct watermark to
  advertise.

The finite-H splice avoids this entirely because its correction stays *below* a
still-retained head root -- the head never moves. The EOF/predecessor case is hard
precisely because the head is rewritten. Relying on the post-replay consistency
check (`:5985-5995`) to catch a bad restore is a heuristic on row count and frontier;
a same-row-count, value-only correction with the prefix head near the frontier could
pass it and restore silently wrong. That is below the bar for this subsystem.

So finding 2.1 is not merely "add a truncate op": it needs machinery so a mid-repair
crash deterministically rebuilds the rewritten tail from the applied base table.

## 4. Two viable designs

### Design A -- truncate keeps a valid superblock + durable repair-in-progress marker (recommended)

Keep the superblock valid across the repair (so the id space and generation carry
forward directly, per requirement 4), and add a durable marker that forces a
mid-repair-crash restart to rebuild instead of trusting the truncated head.

Sequence for an EOF / predecessor-resume repair:

1. **Write a durable "repair-in-progress" marker** recording the floor `R` (and the
   repair epoch). Ordered before the truncate publish.
2. **`publishTruncate`** a new generation whose timeline drops every entry with
   `maxTimestamp >= R`, keeps the prefix by page reference, releases the dropped
   roots' data segments, carries the delta root + `nextCheckpointId` + byte totals
   (minus the dropped suffix) forward, and re-points the in-memory head at the
   surviving prefix head (not `LONG_NULL`). Watermark is carried forward unchanged
   -- correctness on crash comes from the marker, not the watermark.
3. **Replay** the tail (`REPLACE_RANGE [R, +inf)`), exactly as today.
4. **Post-replay seal** appends the new head above the prefix -- an ordinary
   `append` onto the still-valid, prefix-only timeline. No special seal mode.
5. **Clear the marker** as part of (or immediately after) the seal's atomic publish.

Restart rule: if the marker is present, treat the timeline as if retired -- run the
applied-base rebuild and clear the marker. This reproduces today's deterministic
crash behavior (full rebuild from the durable base table) while the *non-crash* path
preserves the prefix and the id space.

Crash matrix:

| Crash point | Timeline on disk | Restart action | Correct? |
|-------------|------------------|----------------|----------|
| after marker, before truncate | full old timeline + marker | base rebuild | yes |
| after truncate, before REPLACE_RANGE | prefix-only + marker | base rebuild | yes |
| after REPLACE_RANGE, before seal | prefix-only + marker | base rebuild | yes |
| during seal A/B publish | prefix-only OR prefix+head + marker | base rebuild | yes |
| after seal, before clear | prefix+head + stale marker | base rebuild (unnecessary, loses continuity that turn) | yes, minor inefficiency |
| after clear | prefix+head, no marker | normal restore | yes, prefix preserved |

Cost: a crash *during* the repair loses id-space continuity (rebuild -> generation
1). That is the rare path; the steady state preserves everything the finding asks
for. The step-5 inefficiency is removable by stamping a repair epoch in the
superblock and treating a marker older than it as stale.

Open item: whether to reuse/extend the existing `repair/` descriptor
(`LiveViewCheckpointRepairState`) or add a dedicated marker file. The finite-H
resumable repair already writes a descriptor there but is crash-*discarded*; the
EOF marker must instead force a rebuild, so the semantics differ and a flag or a
separate record is needed.

### Design B -- logical-retire preserving segment files + prefix-aware seal

Invalidate the superblock before the scan (so a crash routes to the applied-base
rebuild through the *existing* absent-timeline path -- no new marker), but do **not**
delete the prefix's `meta`/`data` segment files. The post-replay seal then rebuilds
a new generation whose timeline re-references the preserved prefix roots plus the new
head.

- Deterministically crash-safe with no new restart integration (invalid superblock
  -> base rebuild, exactly as today).
- But it resets generation to 1 and, because the superblock (which holds
  `nextCheckpointId`) is gone, needs the id-space counter stashed in memory across
  the retire+seal and a **special prefix-aware seal mode** that re-references the
  preserved roots and continues the id space. More seal-path surface than Design A.
- Retire must become "logical" (clear superblock/head, keep segment files); orphaned
  prefix segments must be protected from the reconcile/purge sweep until the seal
  re-references them, or re-adopted by it.

### Recommendation

Design A. It preserves the id space and generation continuity directly (requirement
4) with an ordinary append as the post-replay seal, and isolates all new complexity
into one durable marker + one restart check. Design B spreads complexity into the
seal path and only partially meets requirement 4.

## 5. Shared implementation pieces

### 5.1 `LiveViewCheckpointTimelineWriter.truncateAbove`

A path-copy suffix truncation, mirroring `splice`'s recursion but *removing* keys
`>= floor` instead of re-versioning them:

- Leaf: keep entries `[0, k)` with key `< floor`; return "empty" when `k == 0`.
- Internal: find the child spanning `floor`; keep children `[0, ci)` by reference,
  recurse into `ci`, drop `(ci, ...)`; propagate "empty" upward when a node loses all
  children.
- Root collapse: while the new root is internal with a single child, promote that
  child (by reference).
- Returns whether any prefix survived; the caller falls back to full retire when it
  did not (an empty prefix has nothing to preserve).

Proof obligations (each needs a unit test):

- The reader (`floor`/`successor`/`predecessor`/`last`/`findExact`) and the
  `append`/`splice` writers tolerate **under-full** nodes and a **single-child
  root** produced by truncation (they binary-search per node and never assert
  min-occupancy -- confirm and lock with a test).
- Truncating to a floor below every key returns "empty"; to a floor above every key
  returns the tree unchanged.
- A subsequent `append` onto a truncated tree keeps keys sorted and lookups exact.

### 5.2 `LiveViewCheckpointTimelineStoreWriter.publishTruncate`

Modeled on `publishRepair`:

- Read + validate the current superblock (definitionTxn/historyEpoch identity,
  generation), increment generation.
- Enumerate dropped entries via `timelineReader.range(oldRoot, floor, MAX, visitor)`;
  for each, open its checkpoint root, collect segment ids, and
  `directoryWriter.applyRootReferenceChanges(removed, empty, generation)` so the
  purge job can reclaim them.
- Build the new timeline root via `truncateAbove`.
- Carry the delta root forward (section 2.2); publish the segment directory; write
  the superblock with generation++, `nextCheckpointId` unchanged, byte totals minus
  the dropped suffix's contribution, watermark carried forward (Design A).

### 5.3 Refresh-job wiring

- In `o3HeadMissReplay`, when `localized && !finiteHighBound`, replace
  `retireCheckpointStateOnO3(instance, true)` (`:4187`) with: write marker ->
  `publishTruncate(R = emitLowTs)` -> keep head pointed at prefix head. Preserve the
  existing behavior when the prefix is empty (fall back to retire).
- In `replayFromAnchor`, replace `retireCheckpointStateOnO3(instance, true)`
  (`:3787`) with the same, using `R = replayLowTs` (the anchor floor).
- Clear the marker at the post-replay seal.
- `tryRestoreFromTimeline`: if the marker is present, route to
  `rebuildTimelineRecoveryFromAppliedBase` and clear the marker.

## 6. Test plan

Unit:

- `truncateAbove` shape/coverage tests (5.1 proof obligations), including under-full
  and single-child-root cases and an append-after-truncate.
- `publishTruncate` segment-release accounting: dropped roots' segments become
  reclaimable; prefix segments retained; delta root carried forward; `nextCheckpointId`
  and generation monotonic.

Integration (the handoff's required regressions):

- Build a long timeline; apply a near-head O3 whose influence reaches EOF; assert
  every root below `C` remains addressable and the generation/id space did **not**
  reset. This replaces the current `testOnlyATimelineRetirementResetsTheLogicalEntrySet`
  assertion (`LiveViewCheckpointLogicalRetentionTest.java:188-227`), which locks in
  the reset -- it must be rewritten to assert preservation.
- Then apply an **older** O3 correction and assert it resumes from one of the
  preserved predecessors rather than replaying from the view boundary.
- Predecessor-resume preserves the prefix rather than resetting the timeline.

Crash (Design A marker matrix, per row in section 4):

- Inject failure at each stage (reuse `setTestFailureStage` style hooks) and assert
  restart rebuilds correctly and clears the marker; assert the post-clear state
  restores normally with the prefix intact.

## 7. Open questions for review

1. Design A vs B (recommendation: A).
2. Marker storage: extend the `repair/` descriptor with an EOF "force-rebuild" flag,
   or add a dedicated marker file?
3. Is the rare crash-during-repair id-space reset (Design A) acceptable, or must the
   id space survive a crash too (which would push toward per-entry base-seqTxn +
   extended WAL retention -- a larger format change)?
4. Should predecessor-resume and the EOF head-miss path share one `publishTruncate`
   call site, or stay separate given their different floor derivations?
