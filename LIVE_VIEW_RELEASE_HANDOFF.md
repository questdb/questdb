# Live Views - make the in-memory tier lead disk (Mode A)

Goal: switch the live-view in-memory tier from **Mode B** (the tier is a strict
subset of disk - it serves the recent *overlap* band from RAM for low-latency
reads, but never holds a row disk does not already have) to **Mode A** (the tier
*leads* disk by the **un-flushed lead** - rows refreshed into RAM but not yet
flushed to the live view's own on-disk tier).

The cursor's `seam_ts` routing already shipped in Mode B and is the foundation
Mode A builds on; it does not change. The Mode A change is on the **write side**
- decouple refresh from flush so the tier holds rows ahead of disk - plus a
finer-grained eviction and a union `size()`.

**This plan is aligned with the RFC ("Live View Design v2").** The hand-off ring
(RFC Phase 4 - the in-memory row hand-off from `WalWriter` into the refresh
worker) is **out of scope**: the refresh worker keeps reading base rows via the
disk path (`WalSegmentPageFrameCursor`), exactly as today. **Everything else from
the RFC's tiered-storage / flush / recovery design is in scope.**

**STATUS: Steps 1-3 DONE (2026-06-29); Steps 4-6 not started.** Decision recorded
2026-06-29: adopt the RFC's in-RAM-lead model (defer the whole flush; lead lives
in RAM; recover from the retained base WAL); no hand-off ring. This rewrite
supersedes the earlier "Mode A via deferred apply" draft, which had diverged from
the RFC - it served *all of disk + a lead suffix* with a *store-only-lead* tier
(giving up the seam skip) and committed the LV WAL every cycle. The seam_ts cut
and the in-RAM lead are restored here. Flush materialization went with **option
(a)** (per-flush writer, re-serialise the lead from the tier), not option (b) -
see 1.1 for why (checkpoint-freeze conflict).

- RFC: https://github.com/questdb/rfc/discussions/123 ("Live View Design v2")
- PR: https://github.com/questdb/questdb/pull/6939 (`feat(sql): live views`)
- Branch: `puzpuzpuz_live_view`
- Last updated: 2026-06-29

Relationship to the RFC phases: the `seam_ts` cursor routing the RFC bundles into
the "Phase 3a completion" (RFC section "Phase 3a") has *shipped*; under inline
apply it is a read-latency optimization, "not a freshness gain." This plan is the
step that makes it a freshness gain - by letting the tier lead disk. (Note: the
RFC's "Phase 3b" is the BACKFILL feature, unrelated to this work; an earlier
draft mislabeled this work "Phase 3b".)

Key source files (two packages):
- `core/src/main/java/io/questdb/cairo/lv/` - `LiveViewRefreshJob`,
  `LiveViewInMemoryTier`, `LiveViewInMemoryBuffer`, `LiveViewInstance`,
  `LiveViewDefinition`, `LiveViewStateReader` (write side, tier, definition,
  watermarks).
- `core/src/main/java/io/questdb/griffin/engine/lv/` - `LiveViewRecordCursor`
  (incl. nested `MergedRecord`), `LiveViewRecordCursorFactory` (read side).

---

## 0. Baseline: what Mode B shipped (the foundation, not the thing replaced)

The refresh cycle (`LiveViewRefreshJob.incrementalRefresh`, rate-limited to
FLUSH EVERY) runs, in order:

1. window pipeline -> LV WAL (`walWriter.newRow` + `copier.copy` + `row.append`)
   and mirror each row to a worker-local `stagingBuffer` (rows carry base
   WAL-segment-local symbol ids at this point);
2. `walWriter.commitLiveView(advanceTo)` - the LV's own WAL commit (durable);
3. `applyJob.applyWalDirect(...)` - apply LV WAL -> on-disk LV table; capture
   `lvAppliedSeqTxn` via `getTxnTracker(lvToken).getWriterTxn()`;
4. `advanceLiveViewConsumedSeqTxn(advanceTo)` - persist `lvConsumedSeqTxn`, release
   base WAL segments;
5. `translateStagingSymbolsToLvSpace(...)` - rewrite staging SYMBOL columns with
   LV-table-space ids read back from the just-applied disk rows;
6. `publishToInMemoryTier(...)` - publish staging to the in-mem tier.

Because publish (step 6) follows apply (step 3), **the tier is a strict subset of
disk** in steady state.

What already exists that Mode A reuses unchanged:

- **The `seam_ts` cursor cut** (`LiveViewRecordCursor.hasNext`): serve disk for
  `ts < seam_ts`, serve the pinned in-mem slot for `ts >= seam_ts`
  (`seam_ts` = the slot's minimum ts). The slot serves *every* in-mem row for
  `ts >= seam_ts`. With `IN MEMORY > FLUSH EVERY` the overlap band
  `[seam_ts, applied_watermark]` is served from RAM, skipping the hot disk tail
  (the "seam skip" / IN MEMORY low-latency benefit). **This is already the RFC's
  eventual cursor shape.**
- **The seqTxn fence** (`LiveViewRecordCursor.of`):
  `routingEligible = inMemEligible && diskScanAscending && rowCount > 0 &&
  lvSeqTxn != LONG_NULL && slot.lvSeqTxn == diskReaderSeqTxn(diskCursor)`.
  Guarantees the overlap agrees with disk row-for-row, even across O3; any
  mismatch -> disk-only. `diskReaderSeqTxn()` reaches the LV `TableReader`'s
  `getSeqTxn()`, returning `LONG_NULL` for a non-table cursor or an
  interval-filtered scan.
- **The dual-slot tier** (`LiveViewInMemoryTier`): readers pin a slot via an
  `acquireRead`/`releaseRead` CAS refcount; the writer takes a slot with a
  `0 -> -1` sentinel and either `publishSwap` (slow path) or
  `releaseWriteWithoutPublish` (fast in-place append). Both-slots-pinned -> writer
  stalls (`writerStallStartUs`).
- **Per-slot metadata** (`LiveViewInMemoryBuffer`): `seamTs`, `maxSeqTxn`,
  `lvSeqTxn`, `rowCount`, `footprintBytes()`. Column-major fixed-width storage
  (`isColumnTypeSupported`); SYMBOL stored as an int id; var-length unsupported.
- **The durability clamp** in `publishToInMemoryTier`'s slow path: ages a row out
  only when `pubSlot.maxSeqTxn <= appliedWatermark`. **Vacuous under Mode B**
  (apply precedes publish), per-slot granularity. Mode A makes it load-bearing
  (see 1.4).
- **O3**: `o3Replay` rewrites disk (REPLACE_RANGE), then `rebuildInMemoryTier` ->
  `stageInMemoryWindowFromDisk` repopulates the tier from the rewritten disk
  (LV-space ids straight from disk). The `tierStale` flag covers the
  both-slots-pinned rebuild-skip.
- **Restart**: the first refresh cycle restores window-fn state from the head
  `.cp`, then drains base WAL forward.

The three seqTxn watermarks (`LiveViewStateReader`, all base-seqTxn space,
persisted to `_lv.s`):

- `lastProcessedSeqTxn` - highest base seqTxn consumed; drives next cycle's
  `fromSeqTxn`.
- `appliedWatermark` - **today set to `advanceTo` every cycle, before apply**;
  used by the durability clamp and the checkpoint sweep.
- `lvConsumedSeqTxn` - base-WAL purge floor; advanced after apply.

In Mode B all three coincide each cycle, which is why nobody notices that
`appliedWatermark` tracks the *commit* point, not the *apply* point.

---

## 1. The Mode A design (RFC-aligned, no hand-off ring)

### 1.1 The core change: decouple refresh from flush so the tier leads disk

RFC ("Tiered storage", "Flush"): rows enter the in-mem tier at **refresh** time
and are flushed to the on-disk tier on the **FLUSH EVERY** cadence. Mode B
collapses refresh and flush into one FLUSH-EVERY cycle that *applies before it
publishes*, so the tier can never lead. Mode A separates them:

- **Refresh** (driven by base-table notifications / disk-read of base WAL; runs
  more often than FLUSH EVERY): filter -> `computeNext` -> eager-intern SYMBOLs
  into the LV's own symbol table (1.5) -> append output rows to the in-mem tier.
  **No LV WAL commit, no apply.** The tier's lead (rows above `applied_watermark`)
  grows. Advance an in-RAM "refreshed up to base seqTxn" pointer.
- **Flush** (FLUSH EVERY cadence): write the un-flushed rows (the lead) to the LV
  WAL -> `commitLiveView` (obtain `T_w`) -> `applyWalDirect` inline -> advance
  `applied_watermark = T_w` and `lvConsumedSeqTxn = max(prev, maxBaseSeqTxnInBlock)`
  -> write the head `.cp` ordered after the `_txn` advance. The lead becomes
  overlap.

So commit and apply move *together* to the flush cadence (this is "defer the
whole flush", not "defer only apply"). The lead lives in RAM; the LV WAL is not
written until flush.

Implementation - **option (a)** (chosen 2026-06-29, superseding the earlier
option (b)): refresh appends output rows to the in-mem tier only (no LV WAL
write); flush opens a fresh `WalWriter`, re-serialises the tier's lead rows into
it via the compiled copier, `commitLiveView`, applies inline, then re-stamps the
slot as a subset of disk. The lead is in RAM only; a crash before flush loses it
(recovered from base WAL).

This reverses the earlier preference for option (b) (hold the `WalWriter` open
across the FLUSH-EVERY window, committing the buffered rows at flush). Option (b)
collides with the checkpoint agent: the agent copies each live view's `wal<n>/`
segments after freezing the refresh worker, but the freeze's latch handshake only
guarantees no refresh is *mid-tick* - it cannot release a writer the worker holds
*between* ticks, and `startCheckpoint` runs on the agent thread, which cannot
safely close the worker's non-thread-safe `WalWriter`. Option (a) keeps the
writer's lifetime entirely inside a single flush (exactly as the disk-subset
cycle today), so the checkpoint freeze, drop, invalidate, and error paths are
unchanged. The cost is re-iterating the lead (fixed-width, in RAM, cheap) at
flush. Two consequences:
- the writer is per-flush, not held across the window;
- if the tier publish cannot place the lead in RAM - both slots reader-pinned (a
  stall) or a publish error mid-swap - the refresh **emergency-flushes** the prior
  tier lead plus the just-drained staging rows straight to disk so no row is lost
  and the window state stays consistent with the applied point (a re-drain would
  double-advance the window functions). The published slot is left stale and marked
  for rebuild (`tierStale`); the fence routes reads disk-only (disk is now current)
  until the next refresh drops the stale slot and rebuilds a clean one.
- under O3 (1.6) the in-RAM lead is **discarded** and `o3Replay` recomputes from
  base (the lead's base rows are retained because `lvConsumedSeqTxn = applied`) and
  rebuilds the tier from the rewritten disk - no force-flush needed for V1. (The
  RFC's force-flush-before-replay is a later optimization, not required while the
  recompute is correct.)

### 1.2 Reads: the seam_ts cut, unchanged - now serving the lead

The shipped `seam_ts` cut already serves *all* in-mem rows for `ts >= seam_ts`.
Once the tier leads disk, those rows include the lead (above `applied_watermark`),
so the cursor serves the lead with **no change to `hasNext`**. The apply boundary
(`applied_watermark`) is *interior* to the in-mem range `[seam_ts, latest]`, so
there is no tier switch there and **no duplicate-ts hazard at it** - the only
tier switch is at `seam_ts`, well below it, inside the fence-guaranteed overlap.
(Contrast: a store-only-lead design cuts at the apply boundary and must
special-case dup ts; we avoid that entirely.)

Two cursor changes:

- `size()` = `disk.size() + (rowCount - leadStart)`, where `leadStart` is the
  tier index of the first lead row (the overlap/lead boundary). For Mode B
  `leadStart == rowCount` -> `size() == disk.size()`, unchanged. (Derivation: the
  cursor serves `disk[ts < seam_ts]` plus all `rowCount` in-mem rows;
  `disk[ts < seam_ts] = disk.size() - overlap_count` and
  `overlap_count == leadStart`.)
- `recordAt` already addresses in-mem rows via tagged rowIds; the lead rows live
  in the same slot, so no change.

**Fence (same comparison, RFC-consistent meaning).** The slot is stamped with the
`applied_watermark` its overlap agrees with and its lead extends - i.e. the
*last-flushed* LV seqTxn, which now *lags* the slot's content. `routingEligible`
holds when `slot.lvSeqTxn == diskReaderSeqTxn`. When flush advances
`applied_watermark`, the disk reader advances; slots stamped at the old watermark
fail the fence -> disk-only, which is now itself fresh (disk just absorbed the
lead). The next refresh stamps at the new watermark. **This is the same plumbing
as Mode B; only the stamped value changes** (last-flushed, not this-cycle-applied).

**Disk-only fallback stays the correctness floor.** Any cursor whose slot does not
match the disk reader serves the applied prefix only - always correct, at worst
one FLUSH EVERY stale.

### 1.3 Durability and restart (lead in RAM, recover from base WAL)

- The lead is in RAM only. **`lvConsumedSeqTxn = applied_watermark`** (durability
  based): base WAL is retained up to the applied point and **not** released for
  the un-flushed lead. The RFC is emphatic that any non-durable floor - releasing
  base WAL for rows that exist only in RAM - corrupts crash recovery.
- A crash before flush loses the in-RAM lead. Restart (RFC "Restart recovery"):
  1. **restore** window-fn state from the head `.cp` (at its checkpoint base
     seqTxn, `<= applied_watermark`);
  2. **replay-to-applied** - re-feed base rows from the `.cp` point up to
     `applied_watermark` to advance accumulators to the disk state, *without
     re-emitting* (disk already has those rows). This closes the `.cp`-to-applied
     gap left by the checkpoint cadence;
  3. **drain forward** from `applied_watermark` to rebuild the lead into the tier.
  No data loss; there is no un-applied LV WAL to re-apply.
- This builds on today's restart (restore `.cp` + drain forward). The new piece is
  step 2: under Mode A the `.cp` sits at/below the applied point and the lead must
  be recomputed from base WAL, so accumulators must be advanced to the disk state
  before the lead is rebuilt.

Because commit and apply are atomic at flush, there is **no commit/apply watermark
split**: `appliedWatermark` advances at flush, alongside `lvConsumedSeqTxn`, and
`lastProcessedSeqTxn` for *persisted* purposes never leads the applied point (the
refresh resume point on restart is `applied_watermark`). The in-RAM
"refreshed-to" pointer that leads the applied point is not persisted - it is
rebuilt by drain-forward. (This sidesteps the `appliedWatermark`-set-at-commit
landmine that a defer-only-apply design would have created.)

### 1.4 Eviction (the durability clamp goes load-bearing)

Mode B's *per-slot* clamp (retain everything when `slot.maxSeqTxn >
appliedWatermark`) is too coarse for a leading tier: the lead keeps the slot's
`maxSeqTxn` above the watermark, so the overlap never trims and the slot grows
unbounded. Replace it with the RFC's `readStart` logical eviction: advance the
buffer's lower bound past rows that are **both** aged (`ts < latest - IN MEMORY`)
**and** durable (`seqTxn <= applied_watermark`). The lead
(`seqTxn > applied_watermark`) is never evicted - the clamp is now **load-bearing**
(no eviction of un-flushed rows). `leadStart` marks the overlap/lead boundary
(shared with `size()`). This bounds the tier at `[latest - IN MEMORY, latest]`
while never dropping an un-flushed row, and forces flush before the lead would
span the IN MEMORY window.

### 1.5 Symbols: eager interning at refresh time

**Realised (Step 3, 2026-06-29) with a per-tier in-memory symbol cache, not the
on-disk `SymbolMapWriter` this section sketches** - the on-disk path made a value
new to the un-flushed lead invisible to a query reader until flush (`_txn`-bounded
symbol count). The realisation keeps the same id space and resolution shape but
holds the lead's new symbols in RAM; see Step 3 above. The sketch below is the
original (superseded) plan.

Mode B interns symbols at apply, then `translateStagingSymbolsToLvSpace` reads
LV-space ids back from disk. Under Mode A the lead has no disk to read from, so
interning moves to **refresh time** (RFC "Symbol columns"):

- The refresh worker maintains a `base_id -> lv_id` translation table `T`. Per row,
  per SYMBOL output column: look up `T`; on miss, resolve `base_id -> string`,
  intern the string into the LV's own symbol table via the LV `SymbolMapWriter`
  (returning/allocating `lv_id`), cache `T[base_id] = lv_id`. Store `lv_id` in the
  tier.
- The cursor resolves lead rows via the LV symbol cache exactly as it resolves
  overlap rows today.
- The flush's LV WAL block carries the symbol-table additions alongside the rows,
  so apply / replication / recovery see them atomically.
- On restart `T` is rebuilt lazily on miss; re-interning is idempotent by string,
  so the same base symbol re-derives the same `lv_id`. (Replaces the
  read-back-from-disk path, which is removed.)

### 1.6 O3 under deferred flush

On O3 detection, **force-flush the lead** - commit the buffered uncommitted rows
(do not roll them back; they are all in-order, the O3 row sits below them) so the
on-disk tier is current, then run the existing `o3Replay` (REPLACE_RANGE) and
`rebuildInMemoryTier` / `stageInMemoryWindowFromDisk`. The lead is pure-append
above disk max and is never itself O3-contested; a reader pinned to a pre-O3 slot
whose disk reader is post-O3 fails the fence -> disk-only. Preserve the
`tierStale` both-slots-pinned skip. After replay the lead is empty (just flushed)
and the rebuilt slot is stamped at the post-replay applied seqTxn.

### 1.7 ASOF JOIN as RHS / time-frame: disk-only for V1

The time-frame cursor stays disk-only in V1: ASOF-as-RHS and interval intrinsics
see the applied prefix and trail the lead by at most one FLUSH EVERY cycle - an
**accepted RFC sharp edge**. A synthetic in-mem frame that bridges the lead is the
deferred enhancement. Note this is a freshness *regression vs Mode B* for
ASOF-RHS (where the disk-only frame was fully fresh because disk was current);
under Mode A it trails by the lead. Documented limitation, not a correctness
issue.

### 1.8 What changes vs Mode B

| Aspect | Mode B (shipped) | Mode A (this plan) |
|---|---|---|
| Cycle structure | one FLUSH-EVERY cycle: apply, then publish | refresh (-> tier) decoupled from flush (-> disk, FLUSH EVERY) |
| Tier vs disk | strict subset of disk | leads disk by the un-flushed lead |
| Read cut | `seam_ts` cut (overlap from RAM) | `seam_ts` cut (overlap + lead from RAM) - same `hasNext` |
| `size()` | `disk.size()` | `disk.size() + (rowCount - leadStart)` |
| Fence stamp | this-cycle applied seqTxn | last-flushed applied seqTxn (lags the lead) |
| Durability clamp | vacuous, per-slot | load-bearing, per-row (`readStart`) |
| Symbols | intern at apply, read back from disk | eager intern at refresh into LV symbol table |
| Lead durability | n/a (no lead) | in RAM; recover from retained base WAL |
| `lvConsumedSeqTxn` | == applied each cycle | == applied (advances only at flush) |
| Seam skip | yes | **yes (preserved)** |
| Perf win | skip hot disk tail | seam skip + lead-from-RAM (refresh freshness); flush off the refresh path |

### 1.9 Honest assessment

- **Freshness is a real but bounded win without the ring.** Decoupling refresh
  (frequent -> tier) from flush (FLUSH EVERY -> LV disk) lets tier reads see
  *refresh* freshness (base-apply latency) instead of *flush* freshness. The
  hand-off ring (Phase 4) would later cut refresh latency to base-commit time
  (sub-FLUSH). Do not sell Mode A as sub-ms freshness on its own.
- **The seam skip is preserved** (unlike the earlier store-only-lead draft) - reads
  still skip the hot disk tail. No read regression vs Mode B except ASOF-RHS (1.7).
- **It widens the durability/concurrency surface.** The clamp is load-bearing; the
  tier holds rows not on disk; flush timing is decoupled; symbols intern eagerly.
  Fuzz / concurrency / restart / crash tests must hammer the lead lifecycle.
- **RAM ceiling is the IN MEMORY window** (`[latest - IN MEMORY, latest]`), the
  same as Mode B's overlap window plus at most the un-flushed lead on top (bounded
  by FLUSH EVERY).

---

## 2. Staged steps

The disk-only fallback keeps every intermediate state correct (at worst staler).
Steps are development milestones on one branch, not separate PRs. Restart (Step 2)
is correctness-critical because the lead is in RAM from Step 1 onward.

### Step 1 - decouple refresh from flush; tier leads disk (non-SYMBOL) - DONE (2026-06-29)

- DONE. Split the cycle into **refresh** (drain base WAL -> `computeNext` -> append
  to the tier as the lead; no LV WAL write) and **flush** (re-serialise the tier
  lead into a fresh `WalWriter` -> `commitLiveView` -> `applyWalDirect` -> `.cp`
  via `maybeWriteHeadCheckpoint` -> re-stamp the slot). Refresh runs every tick
  with new base commits; flush runs on the FLUSH EVERY cadence. Uses **option (a)**
  (per-flush writer), not (b) - see 1.1.
  - `LiveViewRefreshJob`: `incrementalRefresh(..., boolean leadMode)`; shared
    `drainBaseWal` (nullable `walWriter`); `finishLeadRefresh`; `flushLead`;
    `restampSlotAfterFlush`; `ensureLeadEligible` (cached on the instance).
    `refreshInstance` decides the cadence: lead-eligible -> refresh-always +
    flush-when-due; else the old coupled, FLUSH-EVERY-gated cycle.
  - `LiveViewBufferRecord` (new): a flyweight `Record` over a tier buffer row, so
    the flush feeds the existing copier instead of a hand-rolled per-type `put`.
- DONE. Slot stamped with the *last-flushed* applied seqTxn (read from the txn
  tracker at publish), not this cycle's; flush re-stamps to the new applied seqTxn.
- DONE. `size() = disk.size() + leadRowCount` when routing-eligible; `leadStart`
  (= `rowCount - leadRowCount`) snapshotted in the cursor's `of()`.
- DONE. Per-row eviction via the overlap/lead split in `publishToInMemoryTier`
  (the trailing `leadRowCount` rows never age out); the old slot-granular
  `pubSlotDurable` clamp is gone. In subset mode `leadRowCount == 0`, so behaviour
  is unchanged.
- DONE. Publish-failure recovery: if the lead cannot enter RAM (both slots
  reader-pinned, or a publish error), `finishLeadRefresh` emergency-flushes the
  prior tier lead + the just-drained staging rows to disk so no row is lost and the
  window state stays consistent (no re-drain). The incomplete slot is left stale +
  `tierStale` so the next refresh rebuilds it; disk serves reads meanwhile. (This
  is the in-RAM-only lead's analogue of the Step-2 crash-recovery story for the
  rare can't-publish path; full crash/restart recovery is still Step 2.)
- DONE. Reads serve the lead via the existing `seam_ts` cut (`hasNext` unchanged).
  **Non-SYMBOL-output LVs only**: a SYMBOL or var-length output LV is not
  lead-eligible and keeps the coupled cycle (apply every cycle, tier = subset of
  disk) until eager interning lands in Step 3.
- DONE. Tests in `LiveViewInMemReadTest`: `testServesUnflushedLeadFromRam`
  (differential oracle: lead read == recompute; disk-only stamp-off == applied
  prefix; `leadRowsServed == 2`), `testLeadSizeAndLimitPushdown` (`LIMIT 4` /
  `LIMIT -2` via size()), `testFlushPromotesLeadToOverlap` (forced flush ->
  `leadRowsServed == 0`, fence re-stamped), `testSymbolLvIsNotLeadEligible`.
  NB: assert lead content with `printSql` (or `assertQuery(...).noLeakCheck()`),
  not plain `assertQuery` - the latter's battery calls `engine.clear()`, which
  drops the registry entry and wipes the lead.
- DONE. `LiveViewSmokeTest` updates for the new model: `testEvictionRetainsAgedUnflushedLead`
  (replaces the old slot-granular-clamp test; an aged un-flushed lead row survives
  the slow-path swap while the aged overlap row evicts), `testLeadPublishFailureEmergencyFlushesToDisk`
  (replaces the old swap-failure test; the injected publish failure emergency-flushes
  to disk with no data loss / no deadlock), and `testWorkerYieldsAtTurnBudget`
  switched to a SYMBOL-output (non-lead) view so the per-cycle `lastProcessedSeqTxn`
  cadence it observes still holds (the turn budget in `drainBaseWal` is shared).

Exit (met): a SELECT serves the un-flushed lead from RAM on top of the overlap and
disk prefix; disk-only fallback proven for the gated shapes. Full LV suite green
(`LiveViewInMemReadTest`, `Test`, `Fuzz`, `Concurrency`, `Checkpoint`,
`InMemoryTier`; `Smoke` pending at hand-off).

### Step 2 - restart / recovery + checkpoint at applied - DONE (2026-06-29)

- DONE. The head `.cp` is already written at flush (applied point), ordered after
  the `_txn` advance: `flushLead` calls `maybeWriteHeadCheckpoint(..., advanceTo,
  ...)` after `advanceLiveViewConsumedSeqTxn` (Step 1). No change needed here.
- DONE. Restart now closes the **checkpoint-cadence gap**. The head `.cp`'s
  manifest `baseSeqTxn` can lag the persisted `appliedWatermark` because
  `maybeWriteHeadCheckpoint` is cadence-gated (rows / duration), not written every
  flush - so on disk the LV table holds rows the `.cp`'s accumulators do not.
  `tryRestoreFromHead` now:
  1. snapshots the persisted `appliedWatermark` (disk truth) **before** the restore
     overwrites the in-memory watermarks;
  2. restores accumulators from the `.cp` (at `manifestBaseSeqTxn`);
  3. when `appliedWatermark > manifestBaseSeqTxn`, calls the new `replayToApplied`
     to re-feed the base WAL over `(manifestBaseSeqTxn, appliedWatermark]` through
     the window pipeline (`drainBaseWal` with `walWriter == null`, `populateTier ==
     false`) so the accumulators catch up to the disk state **without re-emitting**;
  4. resumes the refresh worker at the applied point, so the next incremental
     refresh drain-forward rebuilds only the un-flushed lead. (Without this the
     restore resumed at the head and re-emitted the rows disk already held,
     duplicating them on the next flush.)
- DONE. `replayToApplied` processes the whole gap in one restore call (resets the
  per-turn yield budget per pass) so it never stops mid-gap and leaves accumulators
  short of disk. On out-of-order arrival mid-gap (only reachable when a prior
  post-O3 `.cp` write failed) it hands off to `o3Replay` with `advanceTo = applied`
  so the REPLACE_RANGE rewrite covers everything disk holds; `o3Replay` re-stamps
  the watermarks and writes a fresh head `.cp`. A replay-to-applied error
  invalidates the view (pending-reason hook) rather than serving wrong results.
- DONE. Crash-before-flush recovery: the in-RAM lead is recovered from the retained
  base WAL (`lvConsumedSeqTxn = applied`, base WAL retained to applied, set in
  Step 1) by the drain-forward after restore. New `LiveViewInstance.getAppliedWatermark()`
  getter delegates to the state reader.
- DONE. Tests in `LiveViewInMemReadTest`:
  `testRestartRecoversUnflushedLeadFromBaseWal` (crash between refresh and flush:
  the RAM lead is lost on restart, the first post-restart refresh rebuilds it from
  the retained base WAL, read == recompute, no row loss; note the rebuilt tier holds
  only the lead - the overlap is served from disk until a later flush rebuilds the
  resident window) and `testRestartReplaysCheckpointCadenceGap` (forces a `.cp` <
  applied gap via two flushes where the second misses the cadence; restart replays
  the gap, resumes at applied, and the disk-only prefix holds the applied rows
  exactly once - a flush of the rebuilt lead confirms no duplicate batch on disk).
  Full LV suite green (`InMemRead`, `Smoke` 391, `Test`, `Fuzz`, `Concurrency`,
  `Checkpoint`, `InMemoryTier`).

Exit (met): the in-RAM lead is recoverable; restart matches recompute including the
`.cp`-to-applied gap.

### Step 3 - eager symbol interning - DONE (2026-06-29)

**Realised with a per-tier in-memory symbol cache, NOT decision B's on-disk
`SymbolMapWriter`.** Decision B as written had a correctness hole: it interned
into the LV's *on-disk* symbol table and resolved the lead "via the disk reader
exactly as overlap rows today", but `SymbolMapReaderImpl.valueOf` bounds-checks
`key < symbolCount` and `symbolCount` only advances when `_txn` is committed at
apply - so a value *new to the un-flushed lead* would be invisible to a query
reader (resolve to null) until flush. Since option (a) (per-flush writer) was
already chosen, the flush re-interns via `putSym(string)` exactly like Mode B, so
the on-disk eager-intern bought nothing at flush and only added risk (custom
`_txn` symbol-count commits, two writers on the same files). The user confirmed
the in-mem-cache realisation 2026-06-29.

- DONE. **`ensureLeadEligible` drops the SYMBOL exclusion**: a fixed-width output
  schema is lead-eligible whether or not it carries SYMBOL columns. SYMBOL-output
  LVs flip from the coupled Mode-B cycle to the Mode A refresh/flush split and now
  serve the lead. The only non-lead-eligible tier-populating shape left is
  var-length output (no tier at all).
- DONE. **`LiveViewSymbolCache`** (new, owned by `LiveViewInMemoryTier`, freed with
  the tier): the lead drain eager-interns each SYMBOL output value into the LV
  table's id space - a committed value resolves to its committed id via the LV
  table's `SymbolMapReader.keyOf` (a `TableReader` opened for the drain), a value
  new to the lead is assigned the next id at or above the committed symbol count
  (`nextNewId`, re-anchored via `anchor(col, committedCount) = max(...)` each drain
  so a flush/O3 that advanced the count moves assignment past it). The interned id
  is stored in the tier (overwriting the segment-local id `copyRowFromRecord`
  wrote). Because the drain processes base commits in the same order the flush's
  apply re-interns them (in-order leads only; O3 is diverted to `o3Replay`), the
  assigned id equals the one apply produces, so after flush the lead's overlap
  agrees with disk. The cache holds an append-only `id -> string` list (read by
  cursors) plus a per-window `value -> id` map (writer-side, cleared at flush /
  O3 via `onFlush` / `onO3`).
- DONE. **One LV-table id space, resolved by `LiveViewSymbolTable`** (new overlay):
  `getSymbolTable(col)` / `newSymbolTable(col)` return an overlay that resolves a
  committed id via the disk reader's symbol table and a lead-only id via the cache.
  Keeping a single id space (not a separate tier-id space) is load-bearing: it
  keeps raw-int-key consumers correct (WHERE filters, GROUP BY, static ORDER BY -
  all path (b) `getSymbolTable().valueOf(getInt())`), not just the per-record
  `getSymA` path (printing / HTTP / PGWire). `getSymbolCount()` folds in the lead's
  max id so a static-symbol consumer sizes its key space to cover the lead.
- DONE. **Flush re-serialises the lead's symbols** by resolving each stored id back
  to its string through the same overlay (built over a pre-flush reader +
  the cache) - `LiveViewBufferRecord.getSymA`/`getSymB` delegate to it - then
  `putSym(string)` re-interns into the WAL, so apply / replication / recovery are
  unchanged from Mode B. `translateStagingSymbolsToLvSpace`'s disk read-back is
  removed.
- DONE. **O3 / restart**: O3 rebuilds the tier from disk (committed ids, resolved
  via the disk reader - no intern) and `onO3` drops the stale window map; the next
  drain re-anchors `nextNewId` to the post-replay committed count. Restart loses
  the RAM cache; the first post-restart drain re-interns the lead afresh (fresh
  cache + the restored disk symbol table), re-deriving correct ids.
- DONE. Tests in `LiveViewInMemReadTest`: `testSymbolLvIsLeadEligible`,
  `testSymbolLvServesUnflushedLeadFromRam` (new + committed symbols in the lead,
  differential oracle, disk-only = applied prefix), `testSymbolLvLeadFilterOnLeadOnlyValue`
  (WHERE on a lead-only value + WHERE on a committed value + ORDER BY the symbol -
  pins path (b)), `testSymbolLeadFlushPromotesToOverlap`, `testSymbolLeadRecoversFromBaseWalOnRestart`,
  `testSymbolLeadSurvivesO3`. `testSymbolLvIsNotLeadEligible` removed/replaced.
  `LiveViewSmokeTest.testWorkerYieldsAtTurnBudget` switched from a SYMBOL view to a
  VARCHAR-output (still non-lead-eligible) view so the per-cycle
  `lastProcessedSeqTxn` cadence it observes still holds.

Exit (met): SYMBOL LVs serve the lead from RAM with correct id resolution. Full LV
suite green (`InMemRead` 28, `Smoke` 391, `Test` 30, `Fuzz` 7, `Concurrency` 8,
`Checkpoint` 12, `InMemoryTier` 9).

Known V1 cost: the cache's `id -> string` list is append-only (never cleared, so a
pinned cursor can keep resolving its slot), so it accumulates one string per
distinct value that has ever passed through the lead - bounded like the symbol
table itself, which is fine for SYMBOL's low-cardinality design intent but a real
RAM cost for a very high cardinality column that should not be typed SYMBOL.

### Step 4 - O3 under deferred flush

- Force-flush the lead before `o3Replay`; rebuild the tier from disk; preserve
  `tierStale`.
- Tests: O3 (head-hit and head-miss) with a non-empty lead at detection; post-O3
  cursor regains Mode A and matches recompute; oracle survives restart.

Exit: O3 correct with a non-empty lead; flush-before-replay keeps the rebuild's
"from disk" assumption intact.

### Step 5 - size()/LIMIT, time-frame/ASOF, EXPLAIN

- Lock down `size()` / LIMIT pushdown with dedicated tests.
- Time-frame / ASOF-RHS disk-only (1.7); document and pin with a test
  (ASOF-RHS over an LV with a non-empty lead sees the applied prefix).
- EXPLAIN: keep the `inMemory` capability attribute; update its meaning to
  "eligible for Mode A lead routing"; de-stale Mode-B-specific javadocs.

Exit: read-path observability and ASOF behavior are explicit and tested.

### Step 6 - fuzz, concurrency, benchmark gate

- `LiveViewFuzzTest`: a Mode A read-back knob - SELECT routes through the lead;
  cross-check Mode A == recompute == disk-only-prefix + lead, under O3 / restart /
  crash-before-flush / backfill / forced-flush interleavings.
- `LiveViewConcurrencyTest`: readers churning while refresh publishes leads and
  flush runs underneath; per-snapshot invariant (ts ascending, gapless); Mode A
  engaged post-quiescence.
- JMH: Mode A (seam skip + lead-from-RAM) vs Mode B (seam, subset) vs
  forced-disk-only, whole-view and seam-split shapes, warm/cold cache. Report the
  net honestly.

Exit: lead lifecycle exercised end-to-end with asserted results; the read
tradeoff is quantified.

---

## 3. Decisions (resolved against the RFC)

- **A. Apply/flush trigger:** FLUSH EVERY. The mechanism is decoupling refresh
  cadence from flush cadence (not a footprint cap). O3 force-flushes.
- **B. SYMBOL lead:** eager interning at refresh time, keeping a single LV-table
  id space. **Realised (Step 3) with a per-tier in-memory symbol cache** (the "b1"
  in-mem option), not the on-disk `SymbolMapWriter` this decision originally chose:
  the on-disk path could not make a value new to the un-flushed lead visible to a
  query reader before flush (the reader's symbol count is `_txn`-bounded). A
  committed value still resolves to its LV-table id via the disk reader's
  `keyOf`; a lead-only value is assigned the next id at/above the committed count
  and held in RAM, resolved through the `LiveViewSymbolTable` overlay. See Step 3.
- **C. Time-frame / ASOF-RHS:** disk-only for V1 (accepted RFC sharp edge; trails
  by <= FLUSH EVERY). Synthetic in-mem bridging frame deferred.
- **D. Tier contents:** overlap + lead, served by the `seam_ts` cut. **Not**
  store-only-lead - keeping the overlap is what preserves the seam skip.
- **E. Base WAL release:** after apply / durability-based;
  `lvConsumedSeqTxn = applied`. The only safe rule.
- **Durability model:** in-RAM lead, recovered from the retained base WAL (defer
  the whole flush; no per-cycle LV WAL commit). **No hand-off ring** (user
  directive, 2026-06-29) - the refresh worker reads base rows via the disk path.

---

## 4. What "done" looks like

A SELECT over a live view serves the un-flushed lead (rows refreshed into RAM but
not yet on the LV's on-disk tier) from the in-mem tier via the existing `seam_ts`
cut, on top of the overlap (also from RAM, skipping the hot disk tail) and the
older applied prefix from disk - returning results identical to a from-scratch
recompute in every fuzz / concurrency / differential case including O3, restart,
and crash-before-flush. The flush (LV WAL write + apply + checkpoint) runs on the
FLUSH EVERY cadence off the per-refresh path; the lead is recoverable from the
retained base WAL (`lvConsumedSeqTxn = applied`); the seqTxn fence routes any
cursor whose slot does not match the disk reader to a disk-only (applied-prefix)
read, always correct and at worst one FLUSH EVERY stale. The deliberate disk-only
shapes (pruned/aggregated projection, ts-interval filter, backward scan, non-table
cursor, var-length output) and the V1 time-frame / ASOF-RHS path are the only ones
that never serve the lead.

The hand-off ring (RFC Phase 4 - sub-FLUSH freshness by handing base rows into the
refresh worker faster than the disk path) remains out of scope. This plan delivers
the in-RAM lead, the FLUSH-EVERY flush, and the recovery story the ring would
build on; when the ring lands it only lowers refresh latency, not the tier/disk
structure.
