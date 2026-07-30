# PR #6939 `feat(sql): add live views` — code review handoff

- **Head reviewed:** `20307b9c00` (branch `puzpuzpuz_live_view`), merge-base `1fa621336d`
- **Size:** 426 files, +176 235 / -3 058 (297 production, 128 test)
- **Review:** level 3, round 7 (rounds 5 and 6 are comments on the PR; 15 commits landed since round 6)
- **Date:** 2026-07-29
- **Verdict:** request changes

**Method.** 18 scoped agents plus per-finding source verification. The first pass re-verified round 6's 12 Criticals and 3 blockers against the current head rather than trusting commit titles. Every finding below was traced to source. One gap in the process is recorded under [Coverage](#coverage).

---

## Fix status (updated 2026-07-30)

Worked through `/fix-pr`: validate against source, prove a red test, fix, then an
independent read-only review round. Items not listed below are **untouched**.

B2 and Criticals 1-6 landed on `puzpuzpuz_live_view` as `0309df4430` ("Fix five
live-view recovery and checkpoint defects"), rebased onto `422a86d573`.

Criticals 7, 10 and 11 landed on top as `09445501cc` ("Fix three live-view checkpoint
and read defects"), pushed 2026-07-30. Each carries a regression test re-verified red
with its production fix reverted.

Critical 12 landed as `feb231d024` ("Stop a wedged checkpoint seal pinning the base WAL"),
pushed 2026-07-30, after the design decisions recorded in its section below. Full
`io.questdb.test.cairo.lv` package: **1391 tests, 0 failures**. CI has not run on
`feb231d024` yet.

Criticals 8 and 9 are **not fixed**. Both are confirmed against source and both need a
decision rather than a patch - see their sections below. Critical 8 is deferred to a
separate session by request; Critical 9 is still open.

| Item | Status | Notes |
|---|---|---|
| [B1 Enterprise CI](#b1-enterprise-ci-fails) | **ROOT-CAUSED** | Was wrongly recorded as blocked - a checkout exists. Fixed in enterprise `1c715c344`; awaiting a green run. See below. |
| [B2 Windows checkpoint store](#b2-the-windows-checkpoint-store-is-still-broken) | **FIXED** | 3 review rounds. See below. |
| [Critical 1 stranded writer sentinel](#1-a-stranded-writer-sentinel-becomes-an-unkillable-100-cpu-livelock-of-every-reader-on-the-view) | **FIXED** | 2 review rounds. See below. |
| [Critical 2 restart floor reconcile](#2-the-restart-floor-reconcile-releases-the-base-wal-purge-floor-for-rows-that-were-never-applied) | **FIXED** | 3 rounds. Earlier attempt reverted; different fix landed. |
| [Critical 3 `-1` contract](#3-three-throw-sites-escape-readliveviewmaxbaseseqtxns--1-contract-and-burn-the-one-shot-restore-flag) | **FIXED** | 1 round. Severe half was already closed by Critical 2. |
| [Critical 4 wiped accumulators](#4-a-failed-recovery-rebuild-leaves-wiped-accumulators-and-the-next-turn-commits-over-them) | **FIXED** | 2 rounds. Root cause broader than reported. |
| [Critical 5 generation floor](#5-after-a-root-corruption-fallback-no-checkpoint-can-ever-publish-again) | **FIXED** | 2 rounds. |
| [Critical 6 poisoned reader cache](#6-a-failed-segment-open-poisons-the-reader-cache-and-defeats-the-corrupt-root-predecessor-fallback) | **FIXED** | 1 round. Consequence worse than reported. |
| [Critical 7 errno 0 state pages](#7-bounded-state-page-reads-raise-errno-0-so-the-fallback-never-engages-for-the-corruption-class-it-was-written-for) | **FIXED** | 1 round. Severity corrected down to latent - see below. |
| [Critical 8 `_checkpoints` growth](#8-_checkpoints-grows-without-bound-in-a-default-deployment) | **DEFERRED** | Confirmed, but two supporting claims are wrong and the fix changes the on-disk format. Findings below. |
| [Critical 9 worker-count=0](#9-matviewrefreshworkercount0-pins-the-base-wal-forever) | **pending decision** | Confirmed. Needs an explicit non-default setting, and restates a deferral `c8f442ebaa` documents in code. |
| [Critical 10 `lag` IGNORE NULLS](#10-lagignore-nulls-declares-an-unsound-checkpoint-state-extent) | **FIXED** | 1 round. Wrong results reproduced and fixed. |
| [Critical 11 `dim_length()`](#11-dim_length-over-a-live-view-reads-the-wrong-row-and-npes-on-order-by-ts-desc) | **FIXED** | 1 round. Worse than reported - the forward path NPEs too. |
| [Critical 12 RANGE ring ceiling](#12-a-range-ring-hits-a-hard-format-ceiling-and-the-partition-can-then-never-seal) | **FIXED** | The swallow was fixed, not the ceiling. 1 round. See below. |

### B2 - fixed

`LiveViewCheckpointLayout.publishOverwrite()` retries a tmp->final rename after
unlinking the destination, but **only** on the Windows collision errno
(`ERROR_ALREADY_EXISTS` / `ERROR_FILE_EXISTS`, new `CairoException` constants).
Applied at the two sites that genuinely re-publish a fixed name -
`LiveViewCheckpointRepairMarker.write()` and `LiveViewCheckpointRepairState.persist()`.

Two deviations from this document's prescription, both deliberate:

- The review said to add `removeQuiet` before **all four** renames. Sites 3-4
  (meta/data segment writers) never re-publish - `skipPublishedSegmentIds()` runs
  before all ~17 id allocations and `superblock.nextSegmentId` only advances on
  `publish()`. Adding replace semantics there would have let a re-publication
  silently destroy a segment the *fallback* superblock slot still references,
  turning a latent non-issue into data loss. Left alone.
- Gating on the errno rather than on `ff.exists(finalPath)` matters: on POSIX
  `rename` already replaces, so an exists-based gate could only ever fire on an
  *unrelated* failure and would then delete a good record.

Severity was **understated** here: the repeat publication is the throwing
`persist()` via `addOwnedSegmentId()` (`LiveViewRefreshJob:732`), not the
best-effort `persistQuiet()`. Every localized repair aborts on Windows and
silently degrades to a retire, logged only as "capture unavailable".

Residual risk: the retry is **not atomic** on Windows. `MoveFileExW` with
`MOVEFILE_REPLACE_EXISTING` is the proper fix but needs a `libquestdb.dll`
rebuild. To keep the crash window safe, `RepairMarker.exists()` now also treats a
lone `_repairing.tmp` as a live marker, and `LiveViewCheckpointLifecycle.retireTimeline`
removes that sibling so it costs one rebuild rather than one per restart.

POSIX behaviour change worth noting in the PR body: a crash during the *first*
marker write now forces one full applied-base rebuild where it previously restored
incrementally.

### Critical 1 - fixed

The suggested fix ("move the sentinel CAS to the top") was **rejected**: the
release CAS is the happens-before edge that publishes the stamped horizon, so
releasing first would let a reader resolve symbols against an unstamped horizon.

Implemented instead: `stampSymbolHorizon` does every infallible
`setNewSymbolMaxId` store *before* any fallible `pruneReverseIndex`, so a prune
failure leaves the horizon complete and the slot safe to publish;
`releaseWriteWithoutPublish` wraps the stamp in `try/finally` so the sentinel
always drops; `publishSwap` deliberately keeps the sentinel held on failure
(its caller's catch releases it). `acquireRead` now spins 64 times then
`Os.sleep(1)`, modelled on `LiveViewInstance.awaitRefreshLatch`.

Known limitation carried forward: `acquireRead`'s sleep still polls no circuit
breaker, so a genuinely stuck writer blocks readers without burning CPU but
remains uncancellable. The class javadoc's "the spin is short" is now imprecise.

### Critical 2 - fixed (supersedes the earlier BLOCKED entry)

The recommended fix - bound `WalUtils.readLiveViewMaxBaseSeqTxn`'s backward scan by
the LV table's applied seqTxn - was **rejected**. That method has a second caller,
`CairoEngine.buildViewGraphs`'s torn-`_lv.s` recovery, where *committed* is the correct
answer: the pending block's derived rows are already durable in the view's own WAL, so
clamping lower would re-derive a range the pending block also carries and duplicate it
(the LV forward-append commit has no dedup).

Landed instead: `reconcileAppliedFloorAfterRestart` verifies its own documented
precondition from disk truth (`TableSequencerAPI.lastTxn` vs the LV table's `_txn`
seqTxn, failing closed), defers rather than clamps when it does not hold, and the
caller stamps `setCheckpointRestoreAttempted()` only on success so the whole restore
retries. This is the same rule the pre-existing `reconcilePendingReplacement` gate
already applies to the identical hazard. A cheap `SeqTxnTracker` short-circuit skips
the retry for states a retry cannot move, because a deferred turn is otherwise
re-entered on every base commit.

Note the tracker is consulted only when `isInitialised()` - that is exactly why the
earlier attempt failed, since an uninitialised tracker answers "nothing pending".

Red test: `LiveViewSmokeTest#testCrashFloorReconcileSkipsUnappliedLvWal`, red with
`expected:<1> but was:<2>`. It drives the **busy-writer** failure (a held
`TableWriter`), because that is the one apply failure that does not suspend and so is
the only one the applied check can catch - a file fault suspends and is caught by the
cheaper gate above it.

### Critical 3 - fixed, and partly overtaken by Critical 2

The three throw sites are real. But the stated consequence - the throw burns the
one-shot restore flag, so `tryRestoreFromTimeline` never runs again and the next tick
drains over durable output with cold accumulators - **is no longer reachable**: the
Critical 2 fix moved that flag to after a successful reconcile. The second caller is
unharmed either way, because `buildViewGraphs`' enclosing `catch (Throwable)`
registers the same droppable `state_unreadable` stub the documented `-1` produces.

What remains, and what was fixed: the method now honours its `-1` contract on every
path via `catch (Exception)` (not `Throwable` - an OOM here is not an unreadable log).
The realistic trigger is a concurrent DROP losing `_txnlog`.

### Critical 4 - fixed, with a corrected root cause

Two corrections to the finding:

- The nominated trigger, a per-view memory-limit breach, **cannot** produce the bug -
  `handleRefreshFailure` has a `"query memory limit exceeded"` branch that invalidates.
  Any ordinary IO fault does.
- The localized (`finiteHighBound`) repair path was **already covered** by
  `LiveViewCheckpointRepairSession.close()`'s overlay restore. Marking it too broke
  `LiveViewCheckpointTimelineRepairTest#testABaseSchemaRecompileDiscardsAParkedRepair`
  by escalating a recoverable localized fault into a full recompute that also discards
  the timeline.

The dominant defect is broader than reported: even when `windowStateDirty` **was** set
correctly, `refreshInstance` reset it at every turn entry, so a failed rebuild's
dirtiness could never survive to the turn that would act on it. That is what let the
corrupting turn call `recordRefreshSuccess()` and reset the retry budget.

Landed: a sticky `LiveViewInstance.windowStateDirty` set at the wipe sites that hold no
overlay, seeded per turn, cleared by `fencedLiveViewCommit` (which gained the instance
as a parameter), plus a rebuild-before-drain gate. A rebuild that keeps failing charges
the flush-retry budget until the view invalidates honestly.

Red test: `LiveViewSmokeTest#testMidDrainRebuildFailureDoesNotDrainOverWipedWindowState`,
red with a running sum of `9` where the recompute says `6`. The gate is the load-bearing
element (disabling it reproduces the failure).

### Critical 5 - fixed

Confirmed exactly as described, with one correction: the two watermark ceilings do
**not** wedge permanently - they sit behind the generation arm and self-heal as the
view ingests. Only the generation arm is permanent.

Landed the review's second suggestion: `publish()` gates on the slot the publication
leaves standing, read fresh, rather than on the `select()`-cached global maximum. The
first suggestion (recompute in `selectFallbackSlot()`) was rejected because
`isSelectedSlotNewest()` is literally `generation == generationFloor`, so lowering that
field would defeat the compaction-target safety gate in `LiveViewCheckpointDataStore`.
Two now-dead ceiling fields were deleted and `generationFloor` renamed
`newestValidGeneration`, since serving `isSelectedSlotNewest()` is all it still does.

Red test: `LiveViewCheckpointMetaStoreTest#testFallbackSlotCanPublishOverTheCorruptSlot`,
red with `generation must advance [current=2, next=2]`.

**Residual limit worth stating in the PR body:** when the *only* CRC-valid slot is slot
1 and its peer is torn, publication still cannot advance. Same WAL-retention harm,
different entry state, not addressed here.

### Critical 6 - fixed, consequence understated

All four sites confirmed. One thing the finding does not mention: every one of these
readers resets its id cache inside its own `of(...)`, so the poisoning window is
bounded to a single binding. It is still live, because
`restoreLatestCompatible` retries `restorePinned` in a `while (true)` predecessor walk
**within** one binding, and three of the four caches are not re-bound between attempts.

The consequence is worse than reported. Because every tree descent re-opens the root
segment, the poison lands on the one page every lookup traverses, and no code path
re-opens it: a cold reader answers 10 of 12 keys after one metadata segment is removed,
while a single long-lived binding answers **none**.

Red test: `LiveViewCheckpointSegmentDirectoryTest#testAFailedSegmentOpenDoesNotPoisonTheReaderCache`,
red with `expected:<10> but was:<0>`. It covers one of the four readers; the other three
took the identical one-line change.

---

### Critical 7 - fixed, severity corrected down

`LiveViewStatePageReader.invalid()` now raises `LV_CHECKPOINT_TIMELINE_INVALID` instead
of errno 0, so `restoreLatestCompatible`'s predecessor-root fallback
(`LiveViewCheckpointTimelineStoreReader.java:320-323`) actually engages, and the whole
`corruptCeilingMaxTs` heal-and-retry path at `LiveViewRefreshJob.java:6637-6671` stops
being dead code for this page family.

One deviation: `ensureOpen()` was split OUT of `invalid()` and kept on errno 0. A reader
that was never opened is a caller defect, not stored corruption; folding it into the
corruption errno would let the fallback silently swallow a real bug as a bad root.
Verified no production path reaches `ensureOpen()` from corrupt data - `source` is
assigned only in `of()` and never nulled, and a corrupt page ref fails inside `of()`,
which throws through `invalid()`.

**Severity is overstated in the finding.** Two independent traces agree the review's
headline corruption class cannot reach this reader. All 60 window-function decoders that
loop on a page-embedded count are `*OverPartitionRangeFrameFunction` and report
`supportsCheckpointRingState()`, so `restoreFunction` routes them to the ring reader
(`LiveViewCheckpointTimelineStoreReader.java:560-563`), which already classifies
correctly. Non-ring page decoders read at compile-time-fixed offsets, so a byte flip
yields wrong-but-in-bounds values, and `getStrA` is reachable only via `openKeyPage`,
whose bytes come from the CRC-protected meta segment. The no-CRC premise is correct
(`LiveViewCheckpointDataSegmentWriter.java:43-48`) but does not produce the stated
consequence. This is latent hardening, not a live corruption path - the classification is
still wrong and worth fixing, and it removes a landmine for the day a non-ring decoder
gains a page-embedded count.

A knock-on: `LiveViewFunctionSnapshot` documents and enforces errno 0 for all
disagreements, but decodes through this same reader over its in-RAM scratch overlay, so
its inner page failures now carry `-116`. Behaviourally inert - both call sites catch
`Throwable` - and documented rather than re-wrapped, because a catch there would either
clobber meaningful function-level errnos or need errno-narrowing machinery on a path
where no caller reads the errno.

Tests: `LiveViewStatePageTest`'s errno-blind `assertInvalid` helper is replaced by
`assertCorrupt` (asserts `-116`) and `assertCallerDefect` (asserts 0); two tests added.
Red evidence was 3/6 failures, all `expected:<-116> but was:<0>`.

### Critical 8 - deferred, with corrections (2026-07-30)

**The core defect is real.** Superseded metadata segments in `_checkpoints/meta/` are
never reclaimed. Per seal, roughly 2 files are genuinely dead - the timeline spine and
the segment-directory spine, each a copy-on-write path copy whose predecessor's copied
nodes die the moment the next seal publishes. The roots (checkpoint root, function
roots, anchor root) are *not* leak: the timeline names every boundary forever, so they
are retained state by design.

**Two of the review's supporting claims are wrong.**

1. *"With `max.duration.micros` = 5m an idle view seals 288x/day."* **False - an idle
   view seals zero times.** All four production callers of `maybeWriteHeadCheckpoint`
   require rows behind them (`LiveViewRefreshJob.java:1590`, `:1944`, `:2631`, `:5570`),
   so the cadence check is never even evaluated for an idle view. Independently,
   `append0:899-904` throws `BoundaryNotAboveHeadException` on a non-advancing
   timestamp *before* `dataWriter.of()` or any builder runs, so zero bytes reach disk.
   The correct framing is a *continuously ingesting* view, which seals at least 288x/day.

2. *"Superseded live metadata segments are unlinked only by `LiveViewCheckpointCompaction`."*
   **False, and it makes the finding worse rather than better.** Compaction never unlinks
   a metadata segment: it repacks live *state pages* into a fresh **data** segment
   (`LiveViewCheckpointCompaction.java:192-199`), touches metadata only via an `exists()`
   probe at `:240`, and `publishCompaction` then *writes* new root/timeline/directory
   segments and adds to `metadataBytes` (`LiveViewCheckpointTimelineStoreWriter.java:386`).
   **Setting `cairo.live.view.checkpoint.compaction.interval` non-zero does not bound
   `meta/` at all. There is no metadata-side reclamation mechanism anywhere.**

**Confirmed as written:** the segment directory catalogues data segment ids only
(`LiveViewCheckpointSegmentDirectory.java:28-30`; all three `addSegment` call sites pass
a data segment id); `PurgeSweep.onEntry` builds only `dataSegmentPath` (`:610`) and no
`metaSegmentPath` caller anywhere deletes; `purgeFinalOrphans` covers only
`[protectedCeiling, orphanUpperBound)` (`LiveViewCheckpointLifecycle.java:441`), which
superseded metadata is structurally below; `dataStore.purge()` has one caller gated by
`lifecycleReconciledDirs` (`LiveViewCheckpointTimelineStoreWriter.java:156`); the
compaction interval defaults to 0 = disabled (`PropServerConfiguration.java:1525`,
`LiveViewRefreshJob.java:631-634`); `metadataBytes` is only ever `checkedAdd` while
`logicalStateBytes` is decremented at `publishTruncate:803`. Minor: the orphan window is
half-open, not `(lo, hi]`, and it sweeps `data/` as well as `meta/`.

**Every other reclamation path is whole-timeline `rmdir`, never selective:** DROP LIVE
VIEW (`CairoEngine.java:1900-1907`), a non-localizable O3 repair
(`LiveViewRefreshJob.java:3959`, `:4392`), seed-sweep completion (once per view
lifetime), an epoch/definition change (`LiveViewCheckpointLifecycle.java:163`), and a
foreign-format reset. None runs for a steadily in-order-ingesting view.

**Impact.** ~1,150 meta files/day at light continuous ingest (~half dead weight),
~4,000/day at ~1B output rows/day; ~210K dead files/year. Bytes are the lesser problem
(~1-3 MiB/day logical, ~4.6 MiB/day block-allocated). **The file count is the real
hazard:** `cleanupSegmentDir` (`LiveViewCheckpointLifecycle.java:298-345`) does a full
directory walk on every boot reconcile and on the first `append()` of every refresh-job
writer, so startup cost grows linearly with the leak.

**Why this was not fixed here.** Metadata segments are copy-on-write path copies, so a
newly published spine root still points *into* older segment files. The previous spine
cannot be deleted without knowing which of its nodes remain referenced. That leaves two
real options, both changing the on-disk format and both deserving their own PR and CI
lineage:

- **Per-segment reference counting** - a metadata-segment catalogue mirroring the data
  one, refcount maintenance at every publish site, `PurgeSweep` extended to
  `metaSegmentPath` under the same slot-floor + pin rule, `metadataBytes` made a signed
  running total, plus crash-safety work.
- **Metadata compaction** - a metadata-side analogue of `LiveViewCheckpointCompaction`
  that repacks the live tree into a fresh segment so everything below a floor is
  droppable. Reuses the existing compaction shape.

**Correction to the review's test note.** `LiveViewCheckpointCompactionTest.purgeCycle`
does call `reconcile` directly four times, bypassing the production gate - but that is
not why no test catches this. `purge()` is a data-segment sweep by construction and
would never delete metadata however often it ran. The test asserts on `dataSegmentIds`
only. No test asserts any bound on `meta/` file count.

### Critical 9 - confirmed, pending a decision (2026-07-30)

**Confirmed.** With `mat.view.refresh.worker.count=0` and `cairo.live.view.enabled=true`,
`ServerMain.java:727-747` skips `setupLiveViewJobs` while `isLiveViewEnabled()` stays
true. `initialLvConsumedSeqTxn` is clamped to `>= 0` (`CairoEngine.java:1530-1535`), so
it always trips `WalPurgeJob`'s `lvConsumed > -1` test at `:591-594`, but nothing
advances it. Every sweep clamps `safeToPurgeTxn` to that frozen value and
`getCursor(tableToken, safeToPurgeTxn)` retains every segment above it. Traced every
release path - timeout, "no instances registered", `isInvalid`, `isDropped`,
`isRefreshEnabled`, read-only - and none fires. Base WAL and sequencer txn-log parts grow
until the disk fills.

**Two corrections to the finding.**

1. The mat-view contrast is only half right. For a freshly created view the finding is
   correct - mat views contribute no floor until first refresh
   (`MatViewState.java:132,142`). But a mat view that *has* refreshed persists
   `lastRefreshBaseTxn >= 0`, and on restart it freezes and pins identically. So the
   `WalPurgeJob` comment's justification is wrong for the fresh case and right for the
   restart case.
2. The `ApplyWal2TableJob` compounding is real but bounded, not unbounded. On a primary
   with 0 workers only the refresh job commits to an LV's WAL, so there are no
   notifications to drop. The one real case is a restart with 0 workers after a crash
   left unapplied LV WAL txns; no new commits arrive, so it is a bounded leak. `DROP` is
   exempted at `ApplyWal2TableJob.java:1224`.

**Severity: Moderate, not Critical.** `matViewRefreshWorkerCount` defaults to
`cpuWalApplyWorkers`, which is 2, 3 or 4 and never 0 (`PropServerConfiguration.java:1555`,
`:1089-1102`). The bug needs an explicit operator opt-out. No test covers worker count 0;
`LiveViewMatViewDisabledTest.java:62` sets it to `"1"` and is the natural template.

**Why it is unfixed.** `WalPurgeJob.java:556-568` already documents this as a conscious
deferral - *"A zero worker count is NOT covered ... Deliberately left as-is ... Fix it by
giving live views a pool that is really governed by their own flag"* - added in
`c8f442ebaa` on 2026-07-29. The finding restates a decision the author had already
made and recorded.

**If it is taken, the shape to use.** Collapse the three static inputs into one
config-derived predicate (`isLiveViewEnabled() && matViewRefreshWorkerCount > 0`) and use
it at both `CairoEngine.buildViewGraphs`' registration guard and the `WalPurgeJob` gate,
which the existing comment already asks to keep identical. Both evaluate on the boot
thread before `workerPoolManager.start`, so no purge can race registration. Rejected
alternatives: making `initialLvConsumedSeqTxn` `-1` breaks the healthy configuration
(`-1` is the no-floor sentinel, so purge would delete WAL the first drain still needs);
having `LiveViewRefreshJob` register itself races `buildViewGraphs`, which
`ServerMain.java:612-613` requires to run first. A full fix must also decide what
`CREATE LIVE VIEW` does under 0 workers - registering a view that silently does not pin
would corrupt it the moment the pool returns - and that is user-visible.

### Critical 10 - fixed, wrong results reproduced

`BaseLagOverPartitionFunction.hasFrameLocalCheckpointState()` now returns `!ignoreNulls`,
and `checkpointRowsStateExtentOverride()` returns `Long.MIN_VALUE` under IGNORE NULLS.
The first is what actually closes the bug: `rowsPlan` bails at
`LiveViewCheckpointFunctionCompiler.java:490`, `isDependencyComplete` then fails, and the
repair records `DENIAL_INCOMPLETE_DEPENDENCY` instead of localizing against a floor two
rows deep.

Correcting the finding's rationale for the second half: `configure` reads the extent
**unconditionally** at `:192` and uses it at `:204`, before `hasFrameLocalState` is
computed at `:236` - so `Long.MIN_VALUE` does change the descriptor (to the frame's
`-5`), it is simply not load-bearing once the claim is declined. It is correct and
harmless, not a safety improvement on its own. There is exactly one consumer of the
override in the tree.

**The consequence is real and was reproduced**, unlike Critical 7. Getting it red took
care: an O3 correction placed after the last boundary takes `resume from anchor`, and one
below every boundary rebuilds from the history's first row - both land on the right answer
by accident. Only a mid-history correction *inside* a null run earns the `localized
rebuild` whose floor exposes it. Pre-fix that read `expected:<10> but was:<null>`.

Cost of declining is lower than expected: with an anchor below the correction the repair
resumes from it rather than rebuilding the whole history.

Tests: `testLagIgnoreNullsDeniesTheRepairItsFloor` and
`testLagIgnoreNullsOverANullRunRepairsToTheSameAnswer`, plus
`testLagRespectNullsOverANullRunLocalizesTheSameRepair` as a true one-variable control
that pins the disposition the first pair's redness depends on, plus
`LiveViewCheckpointFunctionCompilerTest.testLagIgnoreNullsDeclinesFrameLocalState` as a
pricing-independent guard. `lead()` is unaffected - rejected at CREATE for live views and
does not inherit either override.

### Critical 11 - fixed, worse than reported

`MergedRecord.getArrayDimLen` added. The finding's fix snippet was correct verbatim;
every name in it checked out.

**The finding understates it.** It describes `ROUTING_SEAM`/`ROUTING_LEAD_ONLY_FWD` as
"silently wrong, no exception" and only `ROUTING_LEAD_ONLY_DESC` as an NPE. In fact the
forward path NPEs too whenever `diskRoutedRows == 0` (`LiveViewRecordCursor.java:570`),
which is the ordinary case when the tier holds the whole scan - the new test hit the NPE
on the *forward* read, before the DESC one. "Silently wrong" applies only when disk
actually serves rows first.

The class javadoc enumerating the ARRAY overrides was stale - it named
`getArrayDouble1d2d` and not `getArrayDimLen`, which is precisely the artifact a reader
consults to learn which accessors are covered, in a bug class that *is* "an accessor
nobody remembered to override". Updated to state the rule rather than the list.

Independently re-verified the finding's audit: `Record` declares 45 accessors,
`DelegatingRecord` overrides all 45, and after this fix exactly three fall through -
`getInterval` (INTERVAL is not tier-storable, so such a view gets no tier at all),
`getRecord` (throws either way), and `getUpdateRowId` (UPDATE is rejected on live views;
the AsOf-join caller reads a `PageFrameMemoryRecord`, never a `MergedRecord`). No fourth
gap. `recordA` and `recordB` are both `MergedRecord`, so one override covers both.

Test covers forward and DESC routing, a NULL array row, and `dim_length(a2, 2)` over a
`DOUBLE[][]` - the last pins the 1-based `dim` to 0-based `getDimLen` conversion, which
`ArrayView`'s own range assert only guards under `-ea`.

### Critical 12 - fixed as `feb231d024` (2026-07-30)

Landed the swallow fix, not the ceiling. `MAX_STATE_PAGE_REFS` and `MAX_LIVE_CHUNKS` are
untouched, and there is no CREATE-time gate: a per-key ring above the sharing wall stays a
supported shape, it just stops being one the checkpoint layer keeps paying for.

`maybeWriteHeadCheckpoint`'s `catch (Throwable)` now counts consecutive failures on the
instance. Below `MAX_CONSECUTIVE_SEAL_FAILURES` = 3 nothing changes - that is the right
behaviour for a held writer or a momentarily full disk. At 3 the view clears the head and
retires the timeline (`LiveViewRefreshJob.java:2900-2902`'s existing pair), releasing both
`WalPurgeJob` floor arms, then arms an exponential cooldown capped at an hour. Any
successful seal clears the streak and the cooldown, so a view whose ring shrank back under
the bound recovers with no restart. Plus `live_views().checkpoint_seal_failures`.

Red test: `LiveViewCheckpointTimelineSealTest#testRepeatedSealFailureRetiresTheTimelineAndReleasesTheWalFloor`,
driving a deterministic seal failure through the existing `setCheckpointTimelineTestFailureStage`
hook. Each half of the fix was verified load-bearing by disabling it alone: without the
retire, `expected:<-9223372036854775808> but was:<2>` - the head arm frozen at base seqTxn 2,
which *is* the WAL pin; without the cooldown gate, `expected:<3> but was:<4>` - the seal
re-attempts and re-burns the encode.

**Corrections this work produced.**

1. **The re-image cost figures in the earlier analysis were the RAM figures.** State pages
   are encoded (`LiveViewCheckpointStateCodec`): timestamps delta-of-delta varint, DOUBLE
   values Gorilla XOR, both adaptive with a >=6.25%-and->=16-byte saving rule. But
   `isLongColumn` (`LiveViewCheckpointRangeRingStateReader.java:499-501`) is
   `valueKind != VALUE_KIND_DOUBLE && != VALUE_KIND_DEQUE_DOUBLE`, so **only DOUBLE rings
   get value compression** - LONG, DECIMAL128 and DECIMAL256 all store `LONG_RAW_64`
   verbatim, deliberately (a NaN bit pattern would canonicalize). Per-row disk: ~1 B
   valueless, ~3-5 B DOUBLE, ~9 B LONG, ~17 B DECIMAL128, ~33 B DECIMAL256, against a flat
   16 B/row in RAM (`RECORD_SIZE = Long.BYTES + Double.BYTES`). So the re-image
   amplification above the sharing wall is ~23 GB/day for DOUBLE, ~52 for LONG and ~190 for
   DECIMAL256 at 10 keys x 2M rows - not a flat 92.
2. **Compression does not move either wall.** `maxChunkRows` is `CHUNK_ROWS / valueWords`
   and `refCount` counts pages, so both are row-count bounds: 1,048,576 and 134,217,728
   rows per key for a one-word ring stand exactly as computed, however well the data
   compresses.
3. **The ceiling kills the whole view's checkpointing, not one key.** The throw propagates
   out of `freezeBoundary` -> `append0` -> `appendCheckpointTimelineRoot`, so one
   pathological partition key fails the entire root append.
4. **`MIN_SHARED_CHUNK_ROWS` is calibrated against raw row bytes** while what sharing
   avoids re-writing is the *encoded* bytes. At ~3 B/row a 64-row chunk saves 192 bytes
   against the 80 a chunk's two refs cost per root - 2.4:1, not the 12.8:1 the javadoc's
   arithmetic implies; for a near-constant double stream sharing is a net loss. The
   constant is left alone and the javadoc corrected, along with its "lets a large one share
   almost everything" claim.
5. **The orphaned `.tmp` is bounded, not cumulative.** A failed seal leaves its data
   segment tmp behind, but the burned id is reused (`superblock.nextSegmentId` only
   advances on publish) and `MemoryCMARWImpl.map` resets the append offset for a `size < 1`
   open, so the next attempt overwrites it in place.

**Deliberately out of scope.** `maybeWriteSeedCheckpoint` keeps its own swallow: it holds
no head arm, and its timeline exposure clears when the sweep completes and the cadence path
takes over. `live_views()` was free to extend - the table does not exist on `master`.

### Critical 12 - the original finding, confirmed (2026-07-30)

**Confirmed arithmetic.** `MAX_STATE_PAGE_REFS = 1 << 16`
(`LiveViewCheckpointMetadata.java:38`), `CHUNK_ROWS = 4096`,
`maxChunkRows = CHUNK_ROWS / valueWords`, `pagesPerChunk = 2`. Ceiling is 32768 chunks:
**134,217,728** rows for a 1-word ring and **33,554,432** for DECIMAL256. The finding's
figures are exact, and the scope is live rows per partition key, not view totals.

**Two corrections.**

1. **It is not a format ceiling.** The on-disk ref count is an int32
   (`LiveViewCheckpointPartitionMapNode.java:296` / `:115`). `1 << 16` is a self-imposed
   sanity bound whose job is capping the allocation a corrupt file can induce. Raising it
   is a validation relaxation, backward-compatible on read - no migration. "Hard format
   ceiling" implies a cost that does not exist.
2. **A much earlier wall exists.** `LiveViewCheckpointRingSeal.seal()` gates sharing on
   `chunkCount < chunkCap(rowCount)` with `MAX_LIVE_CHUNKS = 256` (`:69-113`, `:198-199`).
   Since `chunkCount >= ceil(rowCount / 4096)`, any 1-word ring above **~1,048,576 live
   rows per key** can never share: every seal takes `builder.ofEmpty()` and re-encodes the
   entire ring, every cadence tick. The 134M ceiling is the far end of a regime that is
   already pathological two orders of magnitude earlier.

**The consequence chain is confirmed, and it is the real bug.** Once crossed,
`freezeFunction` fails deterministically every tick (the rebuild-from-empty regime
restreams the same rows), `LiveViewRefreshJob.java:5921-5930` swallows it in a generic
`catch (Throwable)`, and `setHeadCheckpoint` at `:5894` never runs - so
`headCheckpointBaseSeqTxn` and `checkpointTimelineWalPurgeFloor` freeze and
`WalPurgeJob.java:617-620` pins the base WAL indefinitely. `WalPurgeJob.java:541-550`
already names this exact failure mode as the reason invalid views are excluded. The only
symptom is a `LOG.critical`; `live_views()` has no column for it and the view is not
marked invalid. No test covers the ceiling.

**Framing for whoever takes it: the bug is the swallow, not the ceiling.** Any
permanently-failing seal pins the base WAL forever behind a log line; the ceiling is one
member of that class. Fixing the ceiling alone leaves the class open.

**Smallest complete fix (~40 lines).** Give the `catch (Throwable)` a consecutive-failure
budget on the instance, mirroring the existing `recordRefreshFailure`/`recordRefreshSuccess`
streak idiom. On exhaustion call `setHeadCheckpoint(LONG_NULL, ...)` plus
`retireCheckpointTimeline(instance)` - both already exist and are already used together
for the O3-incapable disposition (`LiveViewRefreshJob.java:3345-3354`, `:2978`). That
releases both floor arms: `getHeadCheckpointBaseSeqTxn()` returns `LONG_NULL` and fails
the `> -1` test, and `clearCheckpointTimelineOwnership()` nulls the timeline floor. Reset
on a successful seal.

Retire rather than invalidate: the checkpoint timeline is derived state
(`LiveViewRefreshJob.java:5764-5766`), so losing it costs fast restart recovery, not
correctness, and the view keeps serving correct results. Invalidation is terminal,
requires DROP + CREATE, and the precedent for it is a memory-limit breach - a genuine
resource-safety stop.

**Three calls a human should make.** ~~Whether a per-key RANGE ring above ~1M live rows is
a shape QuestDB intends to support at all (if not, the fix is a documented enforced limit,
not a bigger constant, and `MAX_LIVE_CHUNKS` needs revisiting); retire-timeline vs
invalidate-view as the terminal disposition; and whether to add a
`checkpoint_seal_failures` column to `live_views()`, which is a public catalogue schema
change and is diagnostics rather than a fix - it does not release the WAL.~~

**Decided 2026-07-30**, see the fixed section above. A large per-key ring stays supported
and degrades rather than being rejected; retire, not invalidate; the column was added, and
it cost nothing because `live_views()` is new in this PR.

## Contents

1. [Merge blockers](#merge-blockers)
2. [Round-6 delta](#round-6-delta)
3. [Critical](#critical)
4. [Moderate](#moderate)
5. [Performance](#performance)
6. [Minor](#minor)
7. [Downgraded](#downgraded)
8. [Coverage](#coverage)
9. [PR body corrections](#pr-body-corrections)

---

## Merge blockers

Red on the current head. Not review opinion.

### B1. Enterprise CI fails

**Root-caused 2026-07-30 in enterprise `1c715c344`. Awaiting a green run.**

The status recorded in this document was wrong on both counts: a `questdb-enterprise`
checkout does exist (`/home/puzpuzpuz/projects/questdb-enterprise`, on
`puzpuzpuz_live_view`, HEAD matching PR #1087), and the failure was not carried a third
time. `1c715c344` ("Fix live-view CI failures on e2e, Windows and GCS legs") fixes three
independent failures, all in test code:

- **Replication GCS** — `LiveViewFailoverContinuationTest` hardcoded a `0/0/10` throttle
  triplet, which GCS rejects at Bootstrap
  (`replication.primary.index.upload.throttle.interval must be >= 1000`). The index-upload
  throttle, the throttle window and the request retry interval now route through
  `adjustForGCS` together, so the required relationship between them holds.
- **e2e (linux-amd64)** — the submodule bump renamed the sender connect-string key
  `sf_max_bytes` to `sf_max_segment_bytes`; three SF kill9 tests failed at connect.
- **windows-amd64-lifecycle** — both CHECKPOINT CREATE tests in
  `LiveViewReplicatedDropTest` errored with "Checkpoint is not supported on Windows"; now
  skipped via `Assume.assumeFalse(Os.isWindows())`.

So the failure was replication-adjacent only in the sense that it was a replication *test*
whose fixture violated a GCS-only config floor. Not a product defect, and not the
`prefersAppliedBaseRefresh` / read-only-removal hypothesis this section raised.

**Remaining:** confirm the enterprise pipeline is green on `1c715c344`. Nothing else is
outstanding on B1.

Original finding, for the record: build `#20260728.41` (buildId 256403), job **Replication GCS**, "had test failures"
(2 errors), on tandem PR questdb-enterprise#1087 head `59f2fdd05c`. Every other enterprise job is green, so this is a
test failure, not a compile break.

### B2. The Windows checkpoint store is still broken

The four `tmp -> final` publication sites round 6 identified are **byte-identical** since then
(`git diff 1c5029091e..HEAD` over them is empty), and Windows CI has not been re-run since.

`Files.rename` on Windows is `MoveFileW` (`core/src/main/c/windows/files.c:979`), which refuses an existing
destination with errno 183 (`ERROR_ALREADY_EXISTS`). Two of the four provably re-publish over an existing name:

| Site | Why the destination exists |
|---|---|
| `LiveViewCheckpointRepairMarker.java:195` | renames onto the fixed-name `_repairing` file; a second repair without an intervening successful seal hits it |
| `LiveViewCheckpointRepairState.java:744` | renames onto the same `repairId` descriptor on every `persistQuiet()` in-flight update (`:583`, `:595`) |
| `LiveViewCheckpointMetaSegmentWriter.java:163` | bare rename, no destination guard |
| `LiveViewCheckpointDataSegmentWriter.java:118` | has an `ff.exists` pre-check that throws a clearer error, but still cannot re-publish |

**Fix:** `ff.removeQuiet(finalPath)` immediately before each `ff.rename(...)`, or add a replace-capable rename to
`FilesFacade`. Then run the `macwin` pipeline.

---

## Round-6 delta

Verified against source, not commit titles.

| Round-6 item | Status on `20307b9c00` | Evidence |
|---|---|---|
| B1 Windows rename | **OPEN** | four files unchanged since `1c5029091e` |
| B2 `testReaderChurnSoak` mac timeout | not re-verified | needs a mac CI run |
| B3 Enterprise CI | **ROOT-CAUSED** | enterprise `1c715c344`; see B1 |
| C1 repair freezes a boundary one group late | **FIXED** | `BoundaryFreezingCursor` added; `LiveViewRefreshJob.java:4550` |
| C2 skipped seal clears the repair marker | **FIXED** | `maybeWriteHeadCheckpoint` returns `boolean`; `:3991`, `:4884` |
| C3 generation-floor deadlock after fallback | **OPEN** | see Critical 5 |
| C4 mat-view TRUNCATE fence | **FIXED** | `advanceToBeforeCommit` at `:2067` |
| C5 failed rebuild leaves wiped accumulators | **OPEN** | see Critical 4 |
| C6 `WalReader.of()` identity by table name | **FIXED** | now `tableDirName`/`getDirName()`; identity commits moved below the throwing calls |
| C7 RANGE ring format ceiling | **FIXED** | the swallow, not the ceiling; see Critical 12 |
| C8 `waitForApply` blocks with no breaker | **PARTIAL** | breaker added, but still ignores `freezeInProgress` |
| C9 metadata segments never reclaimed | **OPEN** | see Critical 8 |
| C10 purge runs once per process, compaction off | **OPEN** | see Critical 8 |
| C11 no integrity check on the tree readers | **OPEN** | see Critical 7 |
| C12 replicated-rename re-key untested | **OPEN** | see Coverage |

---

## Critical

### 1. A stranded writer sentinel becomes an unkillable 100%-CPU livelock of every reader on the view

**in-diff** — `LiveViewInMemoryTier.java:315` (`publishSwap`), `:377` (`releaseWriteWithoutPublish`), `:167-181`
(`acquireRead`)

`stampSymbolHorizon()` runs **before** the sentinel-release CAS in both release paths, with no `try/finally`. It
dereferences `slots[slotIdx]` and `slots[1 - slotIdx]` unguarded (`freeNativeMemory:445` nulls both) and calls
`pruneReverseIndex`, which allocates. A throw leaves the `-1` sentinel set forever — and `LiveViewRefreshJob.java:7072`'s
recovery calls `releaseWriteWithoutPublish`, which re-runs the same stamp and throws again.

Meanwhile `acquireRead()` spins `Os.pause()` in an unbounded `while (true)` with no spin budget, no sleep backoff and no
breaker poll, and it runs at cursor-open **before** the query's circuit breaker exists — so `CANCEL QUERY`, the query
timeout and `signalClose()` cannot break a reader out. Every query thread that touches the view pegs a core permanently.

**Fix:** move the sentinel CAS to the top of both methods (check-then-mutate rather than mutate-then-check), and bound
the spin then `Os.sleep(1)`. `acquireRead` returning `-1` is already a fully handled outcome that routes disk-only
(`LiveViewRecordCursor.of:470`, `LiveViewRecordCursorFactory.bindFrameCursor:266`) — strictly better than pegging a
core. The same edit fixes the related defect where `releaseWriteWithoutPublish` mutates an already-published,
already-pinned slot. `LiveViewInstance.awaitRefreshLatch:2229` is the in-repo model for the bounded-spin shape.

### 2. The restart floor reconcile releases the base-WAL purge floor for rows that were never applied

**in-diff** — data loss — `LiveViewRefreshJob.java:8017-8033`

`applyWalDirect(token, ...)`'s result is ignored. `ApplyWal2TableJob`'s own javadoc (`:1113-1123`) says it returns
silently without applying on memory-pressure backoff, on `EntryUnavailableException`, and after `handleWalApplyFailure`
suspended the table. `readLiveViewAppliedMaxBaseSeqTxn` then reads the last **committed** `LIVE_VIEW_DATA` block from the
sequencer log, and the clamp advances `lastProcessedSeqTxn` / `appliedWatermark` / `lvConsumedSeqTxn` over rows that are
not on disk. `WalPurgeJob.java:578` uses `lvConsumedSeqTxn` as the base-WAL floor, so the source is deleted.
`hasPendingLiveViewApply` skips suspended tables (`:7903`), so nothing retries.

Both javadocs (`WalUtils.java:359`, `CairoEngine.java:3115`) assert "last **applied**" — an invariant the code does not
establish.

**Fix:** mirror `flushLead` (`:2704`/`:2713`) and `retryPendingLiveViewApply` (`:7933`/`:7939`) — read
`getTxnTracker(token).getWriterTxn()` before and after, and skip the clamp unless it advanced. Correct both javadocs.

### 3. Three throw sites escape `readLiveViewMaxBaseSeqTxn`'s `-1` contract and burn the one-shot restore flag

**in-diff** — `WalUtils.java:379, 403, 422`; call site `LiveViewRefreshJob.java:8025`

The narrowed `catch (Exception)` sits only inside `liveViewMaxBaseSeqTxnFromRecord`. Unguarded throw sites:

- `:379` `txnLogMemory.smallFile(.../txn_seq/_txnlog)` — throws when the file is gone (concurrent DROP between the
  `isDropped()` check at `:8206` and here)
- `:403` `partMem.smallFile(.../txn_parts/N)` — throws for a purged V2 sequencer part
- `:422` `throw new UnsupportedOperationException(...)`

The call at `:8025` is **outside** the surrounding try, and `setCheckpointRestoreAttempted()` already ran at `:8283`. So
`tryRestoreFromTimeline` never runs for that view for the life of the process, and the next tick drains incrementally
over durable output with cold accumulators — exactly what the comment at `:8276` says must never happen. Silently wrong
output, no error surfaced. `windowStateDirty` is `false` at that point, so the mid-drain rebuild arm at `:8577` is
skipped too.

**Fix:** wrap the call in `try/catch (Throwable)`, or push the guard into `readLiveViewMaxBaseSeqTxn` so it honours its
own documented `-1` contract on every path.

### 4. A failed recovery rebuild leaves wiped accumulators and the next turn commits over them

**in-diff** — round-6 C5, still open — `LiveViewRefreshJob.java:4414`, `:4297`, `:8195`, `:8577`

`o3HeadMissReplay` (method starts `:4150`; both lines are inside it) calls `clearWindowState` under `if (!resuming)` at
`:4414`, while `windowStateDirty = true` is set only under `if (resuming)` at `:4297`. `refreshInstance` clears the flag
unconditionally at turn entry (`:8195`).

So a non-resuming turn wipes the accumulators without marking them dirty. A throw after that point — a per-view
memory-limit breach on the rebuild scan is the realistic case, since the rebuild is an O(view) scan into the same ceiling
the original fault hit — makes `handleRefreshFailure`'s guard at `:8577` read `false` and skip
`rebuildWindowStateAfterMidDrainFailure`. The next turn drains forward from the unchanged `lastProcessedSeqTxn` over
wiped state and calls `recordRefreshSuccess()`, which resets the retry budget — so the view never invalidates and commits
wrong cumulative output for the rest of its life.

There is no re-trigger: the fault was not an O3, the head and timeline were already retired, `isCheckpointRestoreAttempted`
is long since true, and `setLatestSeenTs` is monotone so the frontier is unchanged.

**Fix:** a sticky per-instance `hasDirtyWindowState` set wherever the runtime is wiped or partially restored
(`clearWindowState`, `restoreAnchorRoot`, the map clear in `replayFromAnchor:3865`), cleared only by
`fencedLiveViewCommit`. Seed `windowStateDirty` from it at `:8195` instead of assigning `false`, and refuse the forward
drain while it is set.

### 5. After a root-corruption fallback, no checkpoint can ever publish again

**in-diff** — round-6 C3, still open — `LiveViewCheckpointSuperblock.java:344`, `:455-471`, `:491-524`;
`LiveViewCheckpointMetaStore.java:159`; `LiveViewCheckpointTimelineStoreWriter.java:312`

`select()` computes `generationFloor` as the **max over both CRC-valid slots**. `loadSlot()` — which
`selectFallbackSlot()` calls — does not recompute it, `resetFields()` does not reset it, and the javadoc at `:382`
states the fallback "deliberately leaves it alone".

Path: slot0 = gen9 (valid), slot1 = gen10 (CRC-valid, referenced root pages broken). `select()` picks slot1,
`generationFloor = 10`. `validateSelectedRoots()` throws, `selectFallbackSlot()` loads slot0 (`generation = 9`), floor
stays 10. Next seal computes `generation = checkedIncrement(9) = 10`; `publish()` rejects `10 <= 10` and throws
"generation must advance". Generations advance by exactly 1 and alternate slots, so this never resolves.
`normalizedBaseSeqTxnCeiling` / `coveredLvSeqTxnCeiling` lock the same way.

`maybeWriteHeadCheckpoint` swallows it, so `setHeadCheckpoint` is never reached: the head's `baseSeqTxn` and
`checkpointTimelineWalPurgeFloor` freeze, `WalPurgeJob` min-combines both, and **the base table's WAL is retained forever
while it keeps ingesting**. `LiveViewCheckpointLifecycle.reconcile` only retires on a `definitionTxn`/`historyEpoch`
mismatch, so a restart does not clear it.

**Fix:** derive the floor and both ceilings from the slot that *survives* the publication, not the global max —
recompute in `selectFallbackSlot()`, or read the non-target slot directly in `publish()`.

Note: `isSelectedSlotNewest()` was added this round but serves a different purpose (compaction-target safety at
`LiveViewCheckpointDataStore.java:235`); it does not address this.

### 6. A failed segment open poisons the reader cache and defeats the corrupt-root predecessor fallback

**in-diff** — `LiveViewCheckpointTimelineStoreReader.java:439`, `LiveViewCheckpointPartitionMapReader.java:248`,
`LiveViewCheckpointSegmentDirectoryReader.java:351`, `LiveViewCheckpointRowPositionDeltaReader.java:242`

All four do `readers[slot].of(...)` and then `ids[slot] = segmentId`. `LiveViewCheckpointMetaSegmentReader.of` closes and
resets the reader **up front** (`:179-182`) and can then throw (`:201` file too small, `:217` header validation), leaving
the slot advertising the previous healthy id against a closed reader.

`LiveViewCheckpointTimelineReader.readerFor:366` gets this right and carries a four-line comment predicting exactly this
failure. The four siblings did not get the fix. Three of them have a `&& readers[i] != null` guard, which catches a null
slot but not a closed-but-non-null one — which is the actual failure mode.

Consequence: the predecessor root reuses a poisoned segment, `ensureOpen()` raises errno `0`,
`restoreLatestCompatible:321` only recovers on `LV_CHECKPOINT_TIMELINE_INVALID`, and one damaged page escalates into "no
usable root" — forcing a full rebuild from the applied base, the outcome the bounded fallback exists to prevent.

**Fix:** set `ids[slot] = -1` immediately before `of()` in all four. Consider factoring the four identical clock-replacement
caches into one helper so a fifth copy cannot regress again.

### 7. Bounded state-page reads raise errno 0, so the fallback never engages for the corruption class it was written for

**in-diff** — compounds Critical 6 — `LiveViewStatePageReader.java:113`

```java
private static CairoException invalid(CharSequence message) {
    return CairoException.critical(0).put("live view checkpoint ").put(message);
}
```

Every bounds violation raises errno `0`. Data segments carry **no CRC** (stated explicitly in
`LiveViewCheckpointDataSegmentWriter`'s javadoc and in `LiveViewCheckpointStateCodec`'s "read a successful decode as
'well-formed', never as 'uncorrupted'"), so a flipped bit in a stored count or length that a function decoder loops on is
*the* expected corruption for a whole-image state page. It surfaces as `boundsCheck` -> errno 0 -> whole-restore failure,
never as a per-root fallback. `restoreLatestCompatible`'s javadoc claims "a structurally invalid data page in the
selected root does not fail the whole generation"; for this page family that is untrue. The ring path *is* correctly
classified with `LV_CHECKPOINT_TIMELINE_INVALID`, which makes this an oversight rather than a decision.

**Fix:** `CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)`. Only reachable from checkpoint page
decoding, so there is no over-classification risk.

**Related and still open (round-6 C11):** `LiveViewCheckpointTimelineReader`, `LiveViewCheckpointSegmentDirectoryReader`
and `LiveViewCheckpointRowPositionDeltaReader` contain no magic, no version, no framing and no checksum (0 `crc32` hits
in each). `LiveViewCheckpointSuperblockTest#testEveryNewestSlotByteCorruptionFallsBack` shows the authors know how to test
this — it is applied to the 176-byte slot and nothing else. Per-page CRC32 in the same shape, plus a byte-flip test per
reader.

### 8. `_checkpoints` grows without bound in a default deployment

**in-diff** — round-6 C9 + C10, still open; independently re-derived this round —
`LiveViewCheckpointDataStore.java:595-626`, `LiveViewCheckpointLifecycle.java:154`, `:404-442`,
`PropServerConfiguration.java:1525`

- `PurgeSweep.onEntry` builds only `LiveViewCheckpointLayout.dataSegmentPath` (`:610`). The segment directory catalogues
  data segment ids only.
- `purgeFinalOrphans` deletes metadata only for ids in `(protectedCeiling, orphanUpperBound]` — i.e. **above** the durable
  ceiling, which are crash orphans that were never committed.
- Superseded live metadata segments are therefore unlinked only by `LiveViewCheckpointCompaction`
  (`cairo.live.view.checkpoint.compaction.interval` defaults to **0** = off) or by whole-timeline retirement.
- `dataStore.purge()` has exactly one caller (`LiveViewCheckpointLifecycle.reconcile:154`), gated by
  `lifecycleReconciledDirs`, so each view's `data/` is swept at most once per writer instance.

Each seal writes N+3 to N+4 metadata files (one per function root, plus checkpoint root, timeline spine, directory
spine). With `cairo.live.view.checkpoint.max.duration.micros` = 5m an *idle* view seals 288x/day. The accounting
corroborates it: `superblock.metadataBytes` is only ever `checkedAdd` (`append0:1016`, `publishRepair:659`,
`publishCompaction:386`, `publishTruncate:802`) and never decremented, while `logicalStateBytes` *is* decremented in
`publishTruncate:803`.

`LiveViewCheckpointCompactionTest.purgeCycle` calls `reconcile` directly four times, bypassing the gate the server
enforces — which is why no test sees it.

**Fix:** catalogue metadata segments with the same per-root reference counting data segments already get, and extend
`PurgeSweep.onEntry` to unlink `metaSegmentPath(...)` under the identical slot-floor + pin rule. Split reclamation out of
`reconcile` and run `dataStore.purge()` from the seal path every Nth seal, keeping the one-shot gate for orphan/repair
reconciliation only. Make `metadataBytes` a signed running total. Add a test that seals N times and asserts the `meta/`
count stays bounded.

### 9. `mat.view.refresh.worker.count=0` pins the base WAL forever

**in-diff** — `WalPurgeJob.java:556`, `ServerMain.java:728-746`, `CairoEngine.java:1526`,
`ApplyWal2TableJob.java:1219`

The gate's own comment says *"Skip the LV arm when the feature is off: ServerMain then starts no LiveViewRefreshJob"*.
True for `cairo.live.view.enabled=false` — but `ServerMain` **also** skips `setupLiveViewJobs` when
`getMatViewRefreshPoolConfiguration().getWorkerCount() <= 0`, while `isLiveViewEnabled()` stays `true`. Its own advisory
says so: *"CREATE MATERIALIZED VIEW and CREATE LIVE VIEW still succeed, but nothing will refresh them."*

In that configuration:

1. `CREATE LIVE VIEW` succeeds (`SqlParser.java:1289` gates on `isLiveViewEnabled()` only).
2. `initialLvConsumedSeqTxn = Math.max(0, ...)` is deliberately `>= 0` so it always trips `WalPurgeJob`'s
   `lvConsumed > -1` test.
3. No refresh worker exists, so nothing advances it.
4. Every purge sweep clamps `safeToPurgeTxn` to that frozen value -> **unbounded base WAL growth until the disk fills.**

Strictly worse than the mat-view analogue, which starts at `lastRefreshBaseTxn == -1` and contributes no floor until it
has refreshed once.

Compounding it: `ApplyWal2TableJob.doRun`'s gate uses `engine.getLiveViewStateStore().isRefreshEnabled()`, which is
unconditionally `true` for `LiveViewStateStoreImpl`, so the global apply job **drops** LV apply notifications in exactly
the configuration where nothing else will apply them.

**Fix:** gate on a signal that actually reflects a running refresh worker (have `ServerMain` tell the engine, or have
`LiveViewRefreshJob` register itself), and use the same signal in both places. Failing that, do not register instances
when no worker will exist.

### 10. `lag(...) IGNORE NULLS` declares an unsound checkpoint state extent

**in-diff** — wrong results — `LeadLagWindowFunctionFactoryHelper.java:520`, `:544`

Both `checkpointRowsStateExtentOverride()` (returns `-offset`) and `hasFrameLocalCheckpointState()` (returns `true`) are
unconditional. But under `IGNORE NULLS` the ring advances only on non-null arguments: `computeNext:387` advances
`firstIdx`/`count` only when `computeNext0` returns `respectNulls = !ignoreNulls || value != NULL`
(e.g. `LagLongFunctionFactory.java:151`, and identically in `LagDouble`/`Date`/`Timestamp` and the four narrow DECIMAL
widths). So the state is the last `offset` **non-null** values — a look-behind unbounded in ROWS.

`LiveViewCheckpointFunctionCompiler.configure:204-213` takes the override verbatim and `:236` keeps the frame-local
claim, so `rowsPlan` builds a localized repair bounded at `offset` rows. The sibling `last_value` carve-out at `:847`
*does* exclude `isIgnoreNulls()`; lag has no such exclusion on either side.

Reachable: `CREATE LIVE VIEW v ... AS SELECT ts, sym, lag(x, 2) IGNORE NULLS OVER (PARTITION BY sym ORDER BY ts ROWS
BETWEEN 5 PRECEDING AND CURRENT ROW) FROM base` passes every CREATE gate. An O3 row then repairs against a too-narrow
bound and wrong values are persisted to the view's table.

**Fix:**

```java
public long checkpointRowsStateExtentOverride() { return ignoreNulls ? Long.MIN_VALUE : -offset; }
public boolean hasFrameLocalCheckpointState() { return !ignoreNulls; }
```

Both are needed — deferring to the frame is not enough, since an IGNORE-NULLS ring is not bounded by the frame either.
The whole-state checkpoint restores `count` and the ring verbatim and stays correct. **No LV test covers
`lag(...) IGNORE NULLS`.**

### 11. `dim_length()` over a live view reads the wrong row, and NPEs on `ORDER BY ts DESC`

**in-diff** — found independently by two agents — `LiveViewRecordCursor.java:1021-1426`

`MergedRecord extends DelegatingRecord` and overrides every tier accessor, including `getArray:1051` and
`getArrayDouble1d2d:1056` — but not `getArrayDimLen`. `Record`'s own default (`Record.java:85`) would have been correct,
because it calls the virtual `getArray`; but `DelegatingRecord.java:46` overrides it to delegate hard to `base`, and
`MergedRecord` inherits that.

`ArrayDimLengthFunctionFactory.java:146` and `:214` call it directly when the argument unwraps to a plain array column,
and ARRAY is tier-supported (`LiveViewInMemoryBuffer.java:612`).

- **`ROUTING_SEAM` / `ROUTING_LEAD_ONLY_FWD`:** every tier-served row reports the dimension length of the last disk row
  served. Silently wrong, no exception.
- **`ROUTING_LEAD_ONLY_DESC`** (`:345-357`): the slot band is served first via `nextSlotRowBackward()` and
  `diskCursor.hasNext()` is only reached after the lead is exhausted, so `base` has never been positioned —
  `PageFrameMemoryRecord.getArrayDimLen0:1441` dereferences a null `auxPageAddresses`. **NPE on the first row.**

The page-frame path answers correctly (`SlotPageFrame` publishes real addresses), so the two read paths disagree on the
same query.

**Fix:**

```java
@Override
public int getArrayDimLen(int col, int columnType, int dim) {
    if (!inMemMode) {
        return super.getArrayDimLen(col, columnType, dim);
    }
    final ArrayView array = buffer.getArray(bufferRow, tierCol(col), arrayView(col));
    return array.isNull() ? Numbers.INT_NULL : array.getDimLen(dim - 1);
}
```

I audited the whole class: `getArrayDimLen` is the only real gap. `getInterval`, `getRecord` and `getUpdateRowId` are
also un-overridden but unreachable (INTERVAL is non-persisted, `getRecord` is for nested records, UPDATE is rejected on
live views). A `Record`-shaped test walking every accessor over a routed lead row would have caught this.

### 12. A RANGE ring hits a hard format ceiling and the partition can then never seal

**in-diff** — round-6 C7, still open — `LiveViewCheckpointRangeRingStateBuilder.java:422`

`MAX_STATE_PAGE_REFS = 1 << 16` (`LiveViewCheckpointMetadata.java:38`). With `CHUNK_ROWS = 4096` and
`maxChunkRows = CHUNK_ROWS / valueWords`, the ceiling is roughly 134M live rows for a 1-word ring and 33.5M for a
DECIMAL256 one — inside QuestDB's stated domain.

There is no admission check at CREATE and no degradation path. Once crossed, `freezeFunction` fails on every cadence
tick, `maybeWriteHeadCheckpoint`'s generic catch swallows it, and the head, its `baseSeqTxn` and
`checkpointTimelineWalPurgeFloor` all stop advancing — so the base WAL is retained indefinitely while it keeps
ingesting. A `LOG.critical` is the only symptom; it is not surfaced in `live_views()`.

**Fix:** reject the shape at CREATE when the frame width and the base's rate make the ceiling reachable, or degrade to
whole-state for that partition instead of throwing. At minimum surface it in `live_views()`.

---

## Moderate

### Reachable with no live view present (fix here — same CI lineage)

- **`WINDOW JOIN`'s fifth bound conversion is unguarded and its overflow detector is unsound.** *pre-existing* —
  `AsyncWindowJoinRecordCursorFactory.java:3099`. The comment claims *"`from()` multiplies by a positive constant, so a
  negative result means the multiplication overflowed"*, but `TimestampDriver.from:175-178` narrows to `int` first for
  `m`/`h`/`d`/`w`, and a wrapped multiply is not always negative. On a micros master,
  `RANGE BETWEEN <col> SECOND PRECEDING` with `18_446_744_073_710` gives `+448_384` — a 0.448-second window where
  584 942 years were asked for; `18_446_744_073_709` gives a negative and saturates to full history. `4_294_967_296 day`
  narrows to exactly 0. The literal spelling of the same query is now rejected, so two spellings of one query disagree.
  `WindowJoinRecordCursorFactory:394/544` and `AsyncWindowJoinRecordCursorFactory:531/584/678/731/824` all route through
  this one helper, so a single edit covers every dynamic site. Use the ceiling test and saturate; delete the
  now-unreachable `scaled < 0` branch.
- **The mat-view `REFRESH LIMIT` shares the TTL bug class this PR just fixed.** *pre-existing* —
  `MatViewRefreshJob.java:674`. `refreshLimitHoursOrMonths` comes from the same `toHoursOrMonths` encoding, with no
  `validateTtlGranularity` and no timestamp-type check. `ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 5124095 HOURS` on a
  nanos base wraps `minTs` about 35 minutes into the future, `intersectIntervals` prunes the whole refresh range, and the
  view silently stops refreshing. Same point-of-use guard.
- **QWP/UDP: the new hard reject half-applies a datagram.** — `QwpTudCache.java:461` widened from `return null` to a
  throw. `QwpUdpReceiver.processDatagram:399` used to `continue` past the offending table block; the throw now unwinds to
  `catch (Throwable) { droppedParseErrorCount++; return DATAGRAM_DROPPED; }`. Blocks appended *before* it stay in their
  TUDs and are committed by the next `forceCommitAll()`; blocks after are silently lost; UDP means the producer never
  learns; and it is counted as a parse error. This regresses existing **mat-view** users, not just live views, and is not
  mentioned in the PR body. Catch per table block in the UDP receiver and restore skip-one semantics with its own counter.
- **`hasActivePushdownFilter()` is far coarser than the defect it guards.** — `FwdTableReaderPageFrameCursor.java:153`
  returns true whenever conditions were *extracted*, independent of whether `prepareFilterList` succeeded or a single row
  group was ever dropped. The defect lives only in the skeleton branch at `:188`. Result: `skipRows` / `calculateSize` /
  `size` degrade to O(rows) for every parquet scan carrying any pushdown condition — including the live-view seed resume
  this PR added the fix for. Better fix: make the skeleton branch fall through to `nextSlow()` when the partition is
  parquet with a live filter list (and the same in `BwdTableReaderPageFrameCursor:182`).
- **`ReadParquetRecordCursor.size()` / `calculateSize()` are pruning-blind** (`:338`, `:210`) — the last ungated member of
  the family this PR normalised. Unreachable today only because a residual `FilteredRecordCursor` always wraps it. Add the
  symmetric guard or the "unreachable because ..." comment.
- **`BlockFileReader` has no fallback to the previous region on checksum failure.** *pre-existing* —
  `BlockFileWriter.java:104-115`, `BlockFileReader.java:75-118`. `commit()` bumps the version with a volatile store and
  syncs afterwards, both in the same mmap, so a power loss can expose version N+1 with a torn region N+1. The checksum
  catches it and throws, but there is no roll-back to region N — a single torn commit makes the whole file unreadable.
  `_lv.s` and `MatViewState` both inherit this.
- **`ParallelCsvFileImporter` has no token-type gate.** *pre-existing, identical for VIEW/MAT_VIEW* — `:852`, `:1483`.
  The only barrier is compile-time `checkViewModification`, but COPY runs asynchronously and re-resolves the name.

### Live-view specific

- **`SHOW CREATE LIVE VIEW` emits DDL that cannot be re-executed.** `SqlParser.java:1542`. `parseDml` consumes the
  terminating `;` and calls `unparseLast()`, which pushes the token back but never rewinds `_pos`
  (`GenericLexer.java:379`). So `selectTextEnd` includes the semicolon, `_lv` persists `view_sql` with it (also visible in
  `live_views()`), and re-parsing the emitted `AS (\n<sql>;\n)` fails on `')' expected`. The round-trip test passes only
  because every test `CREATE` omits the trailing semicolon, which no real pgwire/psql/REST client does. The same defect
  blocks the un-parenthesised enterprise `OWNED BY` spelling. `parseCreateMatView:2736` carries the identical pattern
  (*pre-existing* there).
- **`DROP LIVE VIEW` is the only DROP arm with no compile-time kind check** (`SqlCompilerImpl.java:3021`), so
  `DROP LIVE VIEW <plain_table>` passes the compile-only `/validate` endpoint and the pgwire PARSE/DESCRIBE round trip.
  The other three arms check; the PR's own comment at `:2962` says it wanted to close this asymmetry.
- **`skipRows()` on the LV cursor is absolute-from-top but nothing marks the cursor as skipped.**
  `LiveViewRecordCursor.java:652-758`; the guard at `:673` reads `hasStartedIteration`, which only `hasNext()` sets
  (`:301`). Chained skips — which `PageFrameRecordCursorImpl.skipRows` supports — re-anchor from row 0. Set the flag once
  the fast path moves anything.
- **Routed reads silently drop `maxRowsAfterSkip`**, losing the LIMIT decode clamp
  (`LiveViewRecordCursor.java:654-665`). `LimitRecordCursor.toTop()` calls `base.skipRows(counter, baseRowsToTake)`
  unconditionally so a zero-row skip still pushes the cap; the routed branch returns before forwarding it. `SELECT * FROM
  lv LIMIT 10` decodes a whole page frame where the base table decodes 10 rows — and the disk-only fallback is *faster*
  than the routed path, which is the tell.
- **`CairoEngine.rename0:4622` is the one surviving call site that can still spell away `LIVE_VIEW`** — it uses the
  5-boolean `lockTableName` overload, which hardcodes `isLiveView=false`. Unreachable today (every production LV token is
  WAL, so `rename` takes the other arm), but removing the boolean `TableToken` constructor was meant to make this class of
  mistake structurally impossible. Add a `Type`-taking overload and delete the boolean ones.
- **`cairo.live.view.flush.retry.max <= 0` bricks a view on its first fault** — `PropServerConfiguration` parses it with
  no minimum, and `LiveViewRefreshJob:8622` evaluates `retryCount >= maxRetry` so `0 >= 0` exhausts the budget
  immediately, durably invalidating the view. The adjacent duration budget was explicitly hardened against exactly this,
  with a comment reasoning it through. Parse with `minValue = 1`.
- **`replayFromAnchor`'s replay loop consults no circuit breaker** (`LiveViewRefreshJob.java:3905`), unlike the head-miss
  loop directly below it (`:4485`), and cannot yield. For a view with no `WHERE`, nothing in the cursor stack is
  cancellable, so DROP and shutdown wait out a scan of the whole base tail. Same omission in
  `reconstructCorruptCheckpointRoots`' warm-up loop (`:5989`).
- **A persistent mid-drain fault never ticks the retry budget** (`:8580`), so `rederiveFromAppliedBaseAfterWalLoss` — the
  designed last resort for a missing WAL segment — is structurally unreachable whenever the drain got a row in first. The
  view loops a full-view rebuild forever with the budget at zero and no invalidation. The SEEDING sibling (`:7285`) is
  worse: it returns `null` unconditionally, so a deterministic mid-sweep fault re-arms the sweep every tick with no
  progress at all.
- **A yielded repair loses `prefixMarkerLive`** (`:4286` declaration, `:4312` sole assignment under `if (!resuming)`,
  `:4894` resolution), stranding the durable `_repairing` marker. `tryRestoreFromTimeline:6498` then reads it as live and
  forces a full applied-base rebuild on every restart. Move the flag onto `LiveViewCheckpointRepairSession`.
- **`waitForApply` still ignores `freezeInProgress`** (round-6 C8, partial), so the three-way stall stands: seed holds the
  refresh latch and waits for base apply -> `ApplyWal2TableJob` holds the base `TableWriter` and parks in
  `waitForUnfrozen()` -> `DatabaseCheckpointAgent.startCheckpoint()` spins for the latch. Up to 60s. Also
  `LiveViewInstance.startCheckpoint():2031` should acquire the latch *before* publishing `freezeInProgress`.
- **WAL apply blocks uninterruptibly in `waitForUnfrozen()` while holding the base `TableWriter`** —
  `CairoEngine.java:2804`, reached from `ApplyWal2TableJob.java:499` (schema change), `:858` (mat-view TRUNCATE), `:990`
  (UPDATE). `Object.wait()` with no timeout, parked for the whole duration of that view's checkpoint file copy. A stall,
  not a deadlock — but an unbounded one on the ingest apply path. Queue the invalidation for the LV worker instead.
- **`_lv` `depCount` / `partitionColumnCount` are disk-driven and unchecked** — `LiveViewDefinition.java:429`, `:462`.
  Each sizes an `ObjList` *and* bounds a walk of `ReadableBlock`, which performs no bounds check
  (`BlockFileReader.Block:244-262`). A negative count throws `NegativeArraySizeException`; a large one OOMs or walks past
  the region. The region checksum catches random corruption first, but the writer's own consistency guard (`:197-205`) is
  an `assert`, so a checksum-valid malformed block reaches this. The sibling `LiveViewStatePageReader` bounds-checks every
  read — follow that pattern.
- **`max`/`min` ring restore treats `frameSize` as an unvalidated ring index** —
  `MaxDecimalWindowFunctionFactory.java:1608, 3533, 5384, 7326, 9138, 10954` and `MaxMinWindowFunctionFactoryHelper.java:1029`.
  `LiveViewCheckpointRangeRingStateReader:434` deliberately validates only its sign ("frameSize is the function's own
  aggregate cardinality, not a ring index"). Every sibling family only *stores* it; max/min is the sole consumer that
  indexes with it. The row-count check two lines above shows the intent. Add `frameSize <= size`.
- **`max`/`min` ring restore drops the null-value guard its `sum`/`avg` siblings carry** —
  `MaxDecimalWindowFunctionFactory.java:1813` etc. vs `SumDecimalWindowFunctionFactory.java:1626-1634`. For `min` over
  DECIMAL8/16/32/64 the NULL sentinel is `Type.MIN_VALUE`, so a null that reaches the ring wins the comparison and becomes
  the emitted minimum — wrong answer with no diagnostic, where `sum` fails loudly on identical input.
- **`DirectSymbolMap` never reaches its documented 0.5 load factor** — `rehash:585` adds `oldCapacity/2` instead of
  recomputing from occupancy, so load converges to **0.25** forever and the table allocates twice the slots it needs.
  Also: `copyFrom:139` silently corrupts a source whose keys are not dense (it iterates `[0, size)` against a general
  int->int map, storing explicit nulls for gaps and dropping keys `>= size`); and the three lookup entry points disagree
  on null (`intern` rejects with a message, `keyOf(value, lo, hi)` returns `-1`, `keyOf(CharSequence)` NPEs).
- **`releaseRead` leaks the global pin lease if `releasePerSlotRc` throws** — `LiveViewInMemoryTier.java:347`. `state`
  stays above `CLOSED_BIT` forever, so `freeNativeMemory()` never runs and both slots' arenas plus the refcount block leak
  for the process's life. Put the decrement in a `finally`.
- **`LiveViewSymbolCache.idToString` is never pruned**, is indexed by the **absolute** LV-table symbol id
  (`LiveViewSymbolCache.java:262`, `ConcurrentCharSequenceList.java:73`), and lives on the **heap** — so it is outside
  `cairo.live.view.refresh.memory.limit.bytes` and invisible to `live_views().in_mem_bytes`. It reaches the view's
  lifetime symbol high-water mark plus one retained `String` per lead assignment; repeated O3 replays make it strictly
  worse. The *reverse* index is carefully pruned and documented; the larger structure gets no equivalent. Apply the same
  horizon-based compaction, or key by a dense band offset.
- **`close()` / `freeNativeMemory()` ignore the writer sentinel** — `LiveViewInMemoryTier.java:203`, `:442`. `state`
  counts read pins only; `tryAcquireWrite`, `publishSwap`, `releaseWriteWithoutPublish` and `releasePerSlotRc` all
  dereference `refCountsAddr` without participating in it. Safe today only because every close path happens to take
  `refreshLatch` first — nothing in the class states or enforces it, and a miss is a CAS at address 0. At minimum document
  the invariant; better, add `assert refCountsAddr != 0` at the five entry points.
- **`TableSnapshotRestore.java:168` leaves a stale `_checkpoints` after restore when the checkpoint skipped `_lv`.**
  `clearLiveViewCheckpointDir` is gated on the *source* `_lv` existing, but `DatabaseCheckpointAgent:501-509` deliberately
  skips copying `_lv` when it is absent. Bounded (with no `_lv`, `buildViewGraphs` reaps the view), but gate on the
  destination instead.
- **`TableSequencerImpl.java:436`**: `notifyTxnCommitted(txn)` sits **outside** the `try/catch (Throwable) { distressed =
  true; }`, unlike `nextStructureTxn:385` where the same call is inside it. A throw there surfaces a commit failure to the
  client for a txn already durably in the transaction log.
- **`WalWriter.java:407-411`**: `assert txnMaxTimestamp <= hiTs` is weaker than `WalEventWriter.java:339`'s
  `if (replaceRangeHiTs <= maxTimestamp) throw`. For a zero-row commit the sentinels are `Long.MAX_VALUE` / `-1`, so with
  `hiTs == -1` the assert passes and `appendData` throws, marking the writer distressed. `commitMatView:355` carries the
  identical assert (*pre-existing*).
- **`WalUtils.readLiveViewMaxBaseSeqTxn` inverts its sibling's ownership convention** — it never trims the caller's
  `Path` back (every other Path-taking helper in the file documents "restored on return"), and `readMatViewState:293`
  *closes* the caller's memory while this one does not. Two near-identical functions, opposite contracts.
- **`MatViewRefreshJob` has three unguarded consumers of the now-mixed dependents list** (`:476`, `:1448`, `:1793`) where
  `WalPurgeJob:508` got an explicit `if (viewToken.isLiveView()) continue;`. Benign today only because every arm
  short-circuits on `stateStore.getViewState(lvToken) == null` — an unasserted invariant, not a check.
- **`CairoEngine.dropLiveView:1827-1924` unwinds all in-memory LV state before the on-disk drop**, so a throw from
  `dropTableOrViewOrMatView` leaves a half-dead view: unregistered (so it vanishes from `live_views()`, routes disk-only
  forever and no worker advances it, and `WalPurgeJob` stops holding the base WAL floor) while still queryable and
  serving permanently frozen rows. Move the on-disk drop first, or re-register in a catch.
- **`liveViewStateStore` is published without `volatile`** — `CairoEngine.java:311`. Assigned in `load()` (`:2931`), read
  from worker threads; the neighbouring `metadataCache` carries an explicit volatile-and-why comment for the same hazard.
  `matViewStateStore` (`:312`) has the same defect (*pre-existing*), but the new field now gates a WAL-apply skip and a
  WAL purge floor. Safe only because `load()` precedes worker-pool start — not safe under any re-`load()` (role promote /
  restore).
- **`CREATE LIVE VIEW IF NOT EXISTS` can report success having created nothing** —
  `CairoEngine.java:4041-4046, 4077-4084`. When `lockAll` returns a reason and `ifNotExists` is true, the method falls
  through the `while(true)` instead of retrying; `deferredHandoff` stays false and `createLiveView` returns silently. The
  `tableToken == null && ifNotExists` arm 20 lines above does `Os.pause(); continue;` — this one should too.
- **`repack` leaks the temporary target segment when `writer.of` throws** — `LiveViewCheckpointDataStore.java:300-303`.
  The id is registered in `candidate.targetSegmentIds` *after* the open, so `abortCandidate` never unlinks the `.tmp`.
  Bounded (the next `reconcile` sweeps it) but the abort path claims to handle it.
- **`LiveViewCheckpointMetaSegmentWriter.of` does not reject an existing final name** (`:222-256`), unlike its data-segment
  sibling (`:204-209`), and `commit()` does a bare rename. Unreachable today because every allocation site is preceded by
  `skipPublishedSegmentIds` — but the invariant lives entirely in the caller, and overwriting a published metadata segment
  destroys pages the *fallback* superblock slot still references.
- **Pooled `LiveViewCheckpointGenerationPin.close()` is not idempotent once recycled** (`:72-84`) despite the javadoc
  claiming it is. All seven acquisition sites are try-with-resources today, so no double close exists — but the blast
  radius is use-after-free of a purged data segment. Add an arm epoch, or drop the claim and make a second close throw.
- **`FirstNotNull*` inherits `checkpointStateFormatVersion()` from `FirstValue*` despite an incompatible same-length state
  layout** (`FirstValueDecimalWindowFunctionFactory.java:1857`/`2084` and 5 sibling widths). Not reachable today
  (`outputPosition` separates them), but the format version is documented as *the* layout guard and here it does not
  discriminate.
- **`BasePartitionedWindowFunction.retainPartitions:188` resets `tombstoneCount = 0` while
  `rebuildKeepingMembers` copies each survivor's value block verbatim**, so a surviving entry can keep a set tombstone
  *byte* with the counter that would clear it reset to 0. Both checkpoint freeze paths skip on the byte alone. Masked only
  by an accidental ordering in `LiveViewWindow.processRow:565-590`, which the `markPartitionAlive` javadoc
  (`WindowFunction.java:482-501`) describes backwards.
- **`FullPartitionFrameCursorFactory.getCursor(ctx, IntList, long timestampLo)`** silently overloads
  `getCursor(ctx, IntList, int order)` — an `int`-typed timestamp binds to the wrong method with no warning, and the two
  mean opposite things. It has **no callers anywhere**. Delete it or rename it `getCursorFromTimestamp`.
- **`getCursorInTimestampRange` has no designated-timestamp precondition** — for a table with none,
  `getTimestampIndex()` is `-1` and construction reaches `AbstractIntervalPartitionFrameCursor`'s
  `assert timestampIndex > -1`: an `AssertionError` under `-ea`, a cursor reading column `-1` without.

---

## Performance

`WalReader` had **no** production caller on master (`git grep getWalReader 1fa621336d -- core/src/main` returns only its
definition in `CairoEngine`); `WalSegmentPageFrameCursor.java:270` is the first. So items 1-2 are newly on the
ingest-shadowing refresh path.

1. **Every drain turn re-materialises the base's entire clean symbol dictionary** — `WalReader.openSymbolMaps:423`,
   triggered by `WalSegmentPageFrameCursor.releaseSegment:309`. One Java `String` per distinct symbol plus a
   char-by-char copy back off-heap, `>= 20x/sec/view` (a turn ends at 64 commits or 50 ms). A 1M-cardinality symbol
   column is ~20M allocations/sec/view. **Fix:** hold the `SymbolMapReader` open across turns and overlay only the
   per-txn diff (already built separately in `buildTxnSymbolDiffs:344`); or cache the reader per `(walId, segmentId)`.
2. **`WalReader.of()` tears down and remaps everything per base commit** (`:244-323`). `metadata.open()` is
   unconditional — outside the `sameTableWalSegment` guard — and re-parses an immutable segment `_meta`, allocating a
   `String` and a `TableColumnMetadata` per column. `openSegmentColumns:415` maps **all** columns rather than the
   projection. `MemoryCMRImpl.of` munmaps before remapping despite the comment at `:246-251` claiming reuse; `extend()`
   is the in-place primitive. `walEventReader.of(path, -1)` remaps `_event` down to the header and then extends it back
   out.
3. **Per-row `int -> String -> int` symbol round trip in the drain** — `LiveViewRefreshJob.java:2315`, `:1857`. The
   mapping is a function of (column, segment, txn diff), not of the row. Memoise as a dense `int[]` built once per
   transaction. Also in `LiveViewSymbolCache.intern`: `windowMap.keyIndex(value)` is computed twice (`:236`, `:245`) with
   no intervening mutation, and `:268` does `get` then `put` on the same key where one `compute` would do.
4. **Checkpoint seal walks the whole runtime partition map and re-descends the durable B-tree three times per key** —
   `LiveViewCheckpointTimelineStoreWriter.java:185-225`, `:1110-1128`; `LiveViewCheckpointFunctionRootBuilder.java:189`;
   `LiveViewCheckpointPartitionMapWriter.java:185-228`. Allocates a `byte[]` per key per descent. Should be O(dirty keys)
   with one sorted merge-join. Sub-finding: `LiveViewCheckpointStateCodec.selectTimestampCodec` / `selectDoubleCodec`
   run a full O(rows) pass doing the identical arithmetic purely to size the output, then the encode redoes it.
5. **Parquet pruning forces O(rows) where O(row groups) exists** — `PageFrameRecordCursorImpl.java:73`, `:203`, `:227`.
   Pruning drops whole frames, never rows inside one, so the yielded count is obtainable from parquet footers. See also
   the `hasActivePushdownFilter` coarseness under Moderate.
6. **Per-row JNI clock reads** — `LiveViewRefreshJob.java:5400` (seed sweep) and `:820` (repair replay) call
   `Os.currentTimeMicros()` per row to enforce a 50 ms budget. Sample on a 1024-row mask, as `drainBaseWal:2060` already
   does per commit. Also `LiveViewInstance.setLatestSeenTs:1866` does a volatile long store per row for a catalogue
   reader that does not need per-row freshness.
7. **Staging buffer freed and re-malloc'd every refresh cycle** — `LiveViewRefreshJob.java:8466` frees it
   unconditionally in a `finally`, so `:6787` always re-allocates a `MemoryCARWImpl` per column. `reset()` exists
   (`:1312`).
8. **`SqlOptimiser.java:5940` opens and mmaps `_meta` unpooled on every compile** of a query over a live view, and drops
   the `CairoException -> positioned SqlException` mapping both neighbouring branches have. The same compile already opens
   a pooled `TableReader` on that token at `SqlCodeGenerator.java:10648`. Use `engine.getTableMetadata(tableToken)`.
9. **`LiveViewCheckpointRowsBounds.seekDependencyLowTs:665`** re-acquires the reader and re-authorizes per key, up to
   `cairo.live.view.checkpoint.repair.scan.max.keys` (default **100 000**) times. Open/authorize once, rebind per key.
10. **Live-view notify runs inside the sequencer's exclusive per-table write lock** — `TableSequencerImpl.java:558`,
    unconditional (the mat-view equivalent is gated and runs from `ApplyWal2TableJob:653`, outside the lock). Per commit
    on every table with zero live views: an interface dispatch plus a case-insensitive `ConcurrentHashMap` probe
    (O(name length)). Cache a per-table marker on `TableSequencerImpl`/`SeqTxnTracker` and move the enqueue outside the
    lock.
11. **`scanForLaggingViews:7838` takes the base sequencer read lock per view per idle tick** — the same `schemaLock`
    every WAL commit takes exclusively — to feed a guard documented as "a strict no-op on a healthy node", when the
    lock-free `getTxnTracker(baseToken).getWriterTxn()` is read two lines later.
12. **O(columns) name-hash schema-drift check per base commit** — `WalSegmentPageFrameCursor.java:282-288`; the answer
    cannot change within a segment.
13. Minor: `LiveViewRefreshJob.java:835` and `:3078` each take the **global** metadata-cache read lock once per refresh
    cycle for results stable over the compiled factory's life; `LiveViewPageFrameCursor.java:475`/`:481` call
    `LiveViewIntervalBands.cut` twice with identical arguments under lead-only routing; `DirectSymbolMap.append:313`
    copies char-by-char where `Vect.memcpy` applies, and `ValueToKeyMap` stores no hash in its slot so every probe
    dereferences into the payload buffer even to reject.
14. `fencedLiveViewCommit` is called with a **capturing** lambda at seven sites (`LiveViewRefreshJob.java:1447, 1877,
    2681, 3945, 4622, 4660, 5412`), and `buildFlushSymbolResolvers:2785` allocates a fresh `ObjList` plus one
    `LiveViewSymbolTable` per SYMBOL column on **every** flush.

---

## Minor

Grouped; all located, none blocking.

- **Member ordering** is violated systematically. The ~12 new `WindowFunction` interface methods are appended after
  `setMemoryTracker` rather than inserted (`WindowFunction.java:307, 318, 674-845`), and the same appended-block pattern
  repeats across ~150 window classes. Also `LiveViewInstance` (12 sites), `SqlParser.java:1313-1990` (~30 methods as one
  unordered block mixing `private static` and instance members), `DirectSymbolMap.java:63-74`, `CairoEngine.createLiveView`
  and `invalidateLiveViewsForBaseTable0`, `WindowExpression.java:70-101`, `TtlTest`, and 40 of 95 `@Test` methods in
  `LiveViewInMemReadTest`.
- **Orphaned javadoc** now documents the wrong member: `LiveViewRefreshJob.java:3032` (`isDedupBase`'s doc sits above
  `isApplyLagDeferred`), `:6717` (`ensureStagingAndTier`'s above `ensureLeadEligible`), `:5650-5692` (two consecutive
  blocks on `maybeWriteHeadCheckpoint`, so only the second renders), `KSumDoubleWindowFunctionFactory.java:1427-1429`
  (two comments for a removed field), `LeadLagWindowFunctionFactoryHelper.java:315-318`.
  `WindowFunction.java:482-501`'s `markPartitionAlive` javadoc states the inverse of the implemented call order.
- **`LiveViewCheckpointFunctionCompiler.java:1004`** uses a signed range test where the runtime guard
  (`WindowContextImpl.toTimestampUnits:100`) uses a width test, so the two disagree for units whose ceiling is
  `Long.MAX_VALUE` — `RANGE BETWEEN 9223372036854775807 MICROSECOND PRECEDING` compiles as a plain query but is rejected
  by `CREATE LIVE VIEW` with a message stating the width equals the maximum and is out of range. `:1008-1009` also print
  the raw internal unit char (`T`, `u`) instead of `WindowExpression.timeUnitName`.
- **Duration units are case-sensitive** — `LiveViewDefinition.parseDurationUnit:282`, so `FLUSH EVERY 1S` fails. SAMPLE BY
  has to be case-sensitive (`m` vs `M`); the live-view grammar has no such collision.
- **Error positions**: `SqlParser.java:1555` reports at the start of the whole SELECT rather than the offending FROM item;
  `:1744` and `:1813` fall through to position `0` for a window with no anchor, partition or order
  (`WINDOW w AS ()` parses). `SqlCodeGenerator.java:5733-5742` converts `hi` before `lo`, so a doubly-invalid frame
  reports the later position first.
- **`WindowExpression.deepClone:217-234` was half-updated** — it copies the two new `resolvedWindow*` fields but not the
  five ANCHOR fields, producing `isResolvedWindowAnchored() == true` with `ANCHOR_KIND_NONE`, exactly the pair
  `SqlCodeGenerator.java:10053` tests. No callers today; copy the fields or delete the method.
- **`ExpressionParser.java:2265-2298`**: `anchor` is missing from the window-inheritance probe's exclusion lists, so
  `WINDOW w AS (ANCHOR)` reports `window 'anchor' is not defined`.
- **`SqlCompilerImpl.java:2371`** leaves a dead `securityContext` local; **`SqlOptimiser`** and **`ExpressionParser.java:2469`**
  use fully-qualified names where imports exist.
- **`MemoryCARWImpl.detachMemoryTracker():121`** documents "must not be used to grow further" with no enforcement.
- **`TimestampDriver.getMaxUnitValue`'s javadoc** (`:332-341`) claims values in `[-max, max]` "convert exactly"; for the
  dividing units conversion is lossy well inside the range (`from(999, 'n') == 0` on micros).
- **SQL style in tests**: ~100 more `1000000 PRECEDING` sites want `1_000_000` (both files already write `1_000_000`
  elsewhere); 36 multi-line string concatenations of >=3 lines want text blocks; `LiveViewInMemReadTest.java:873` uses a
  bare 16-digit epoch-micros literal; `:3380` `99999`; lowercase SQL keywords in
  `LiveViewCheckpointFunctionCompilerTest` (77 `"select `), `LiveViewCheckpointTimelineRepairTest` (18),
  `LiveViewValidationTest` (12), `LiveViewTest` (9).
- **Test residue**: 13 dead `if (currentMicros < 0)` guards in `LiveViewFuzzTest` (unreachable — `@Before` already sets a
  positive value) and 27 redundant `setCurrentMicros(0L)` calls in `LiveViewInMemReadTest`; four helpers duplicated
  verbatim across `LiveViewFuzzTest` / `LiveViewInMemReadTest` / `LiveViewConcurrencyTest` (`mismatch`,
  `assertModeBMatchesDiskOnly`, the disk-only stamp dance, `unwrapLvFactory`); the fuzz dataset generator (20 lines) and
  quiescent-restart block copy-pasted ~10x; 43 byte-identical assertion messages; `runFuzz` takes five consecutive
  booleans; `CONCURRENT_WRITER_VARIANTS` and `FIXED_WIDTH_VARIANTS` are element-identical.
- **`testFuzzStorageTimingProps` randomizes 7 properties and logs none of them** (`LiveViewFuzzTest.java:772-778` vs the
  `LOG.info()` at `:3127`), so a red CI shows the seed but not the configuration. Two thread-spawning arms
  (`testFuzzConcurrentWriters`, `testFuzzReaderVsRefresh`) are not seed-replayable — worth saying so in the comment.
- **One poll-based coordination site**, `LiveViewFuzzTest.java:4013` — the only timing primitive in 10 297 lines across
  the two largest test files (zero `Thread.sleep`, zero `Os.pause`, zero wall-clock deadlines). The sibling
  `runConcurrentWriterFuzz:2752` already does it correctly with `CyclicBarrier` + `join()`. Fix: publish an
  `AtomicLong targetTxn` and have the refresh driver count down an `SOCountDownLatch` once
  `getLastProcessedSeqTxn() >= targetTxn`.
- **5 assertions hard-code internal A/B slot indices** — `LiveViewInMemReadTest.java:2230, 2237, 2244, 2246, 2258` lock
  the tier's swap *policy*. The precondition the test needs is "both slots are reader-pinned":
  `assertNotEquals(pinA, pinB)` plus the existing `isPublishedSlotReaderPinned` helper. The real oracle at `:2268` is
  already correct.
- **Javadoc/comment claims that are not true of the code**: `beginRepair`'s "the capture pins the generation it was
  opened against" (`LiveViewCheckpointTimelineStoreWriter.java:218`) — the `MetaStore` is local to a try-with-resources
  and releases nothing; `invalidateHeadOnO3`'s "the current cycle still feeds the offending batch" (`:2842`) — the batch
  is silently *dropped*; `TablesFunctionFactory.java:437`'s claim that a `liveView` boolean "would renumber columns 9-44"
  — appending at index 45 renumbers nothing; `DatabaseCheckpointAgent.java:481`'s list of what the fall-through path
  copies includes partitions and `wal<n>/`, which it does not.
- **Both A/B superblock slots occupy the same 512-byte sector** (`SLOT_SIZE = 176`, `FILE_SIZE = 352`). Sound under the
  standard torn-write model, but the conventional layout separates them and padding costs nothing at 352 bytes.
- **`truncateAbove`'s inline collapse produces a height-non-uniform B+-tree**
  (`LiveViewCheckpointTimelineWriter.java:430`). Verified harmless — every navigator recurses until `isLeaf()` — but
  `getLastLookupDepth` becomes path-dependent and the javadoc should say so.
- **`assertFullyConsumed` and the `openPage` codec check are identities on the ring path**
  (`LiveViewCheckpointRangeRingStateReader.java:656-713`) — two of three comparisons can never fail because `openPage`
  copied the values out of the same ref.
- **`RankOverPartitionFunction.onCheckpointRestoreBegin:721`** omits the `map.reopen()` that every sibling performs and
  documents; **`BasePartitionedBivariateWindowFunction:131`** likewise. Safe only because callers happen to open the
  cursor first.
- **`LiveViewCheckpointRangeRingStateBuilder`/`Reader` and `PartitionMapNode.decode` allocate per partition per seal** —
  ~3x`refCount` ref objects, 4 arrays and 2 byte-array copies — and `append0:855-865` constructs nine `Closeable`
  components (each with its own native `Path`) on every cadence seal, though the writer already pools `ringSeal` and
  `keyBuffer`.
- **`PgClassRecordCursor.close()` is empty** and the PR adds one more retained reference (`tableToken`) — the exact
  anti-pattern just fixed in `live_views()`. Same for `ShowCreateLiveViewRecordCursorFactory:107` (retains
  `executionContext`). `PgClassFunctionFactory.java:242` also switches the record before the exhaustion check, so a
  registry with no user tables leaves `tableToken == null` for a `getChar(16)` that is now a deref.
- **`live_views()` resurrects `SeqTxnTracker` entries for dropped base tables** via `computeIfAbsent`
  (`LiveViewsFunctionFactory.java:427`) — unbounded map growth driven by a read-only monitoring query. Identical in
  `MatViewsFunctionFactory:225`, `TablesFunctionFactory:322`, `WalTableListFunctionFactory:263` (*pre-existing*).
- **`LineHttpTudCache.java:181`** leaks a `WalWriter` if the `WalTableUpdateDetails` constructor throws
  (*pre-existing*); `QwpTudCache.java:369` has exactly that guard. `:244` also allocates a String per rejection on the
  ingestion path where every neighbouring throw uses a literal.
- **Em dashes** appear in ~40 added comment/javadoc lines in the `lv` package while the surrounding added code uses ASCII
  ` - `. **Log and exception strings are clean** — I grepped every added line and found zero non-ASCII inside a `LOG.*`
  or `put(...)` argument.
- **Commit hygiene not flagged.** 764 commits, some titles >50 chars and some using Conventional Commits prefixes, many
  merge commits with no body. Per `CLAUDE.md` the branch is squash-merged and its history is throwaway, so this is
  explicitly out of scope.

---

## Downgraded

Checked and found not to be defects. Recorded so they are not re-litigated.

- **Parquet `IS NULL` / `IS NOT NULL` per-type gate.** Full 25-type table rebuilt from the Rust `Nullable` impls
  (`parquet_write/mod.rs:64-190`, `decimal.rs:53-58`, `symbol.rs`, `plain/varlen.rs`, `array.rs`) and the eq-factory
  constant folding. **Every type is correctly bucketed**, including the ones the PR body never mentions: GEOBYTE/SHORT/
  INT/LONG, IPv4, UUID, LONG128/256, all six DECIMAL widths, ARRAY, TIMESTAMP_NS, SYMBOL, STRING, VARCHAR, BINARY.
  INTERVAL is unreachable (`ColumnType.java:1153` non-persisted). The BOOLEAN/BYTE/SHORT arm is load-bearing, not
  theoretical: `NegatingFunctionFactory:74` turns the folded `FALSE` into constant `TRUE`, so pre-PR a row group built
  entirely from column-top rows was dropped, silently losing every row.
- **`size()` returning -1 breaking a caller.** Swept every `size()` consumer — HTTP `count`, `CountRecordCursorFactory`,
  `LimitRecordCursorFactory`, cross/markout/union, every hash-join swap heuristic, every sort/window pre-size. All branch
  on `< 0` or `> -1`; none casts to `int` or uses it as a length.
- **Duplicate or missing rows at the seam under tied timestamps.** Both sides are cut positionally by the same number
  (`diskRoutedRows = diskSize - leadStart` vs the slot band `[0, slotRowCount)`), and `hasNext()`'s stop condition is a
  counter (`:322`), never a timestamp compare. The one place a timestamp does drive a cut,
  `LiveViewIntervalBands.findTs:147`, uses `Vect.BIN_SEARCH_SCAN_DOWN` — the same primitive
  `NativeTimestampFinder.findTimestamp` uses — so the two tiers cut identically.
- **DECIMAL window freeze/restore asymmetry.** 138 checkpoint-capable classes cross-checked mechanically: freeze/restore
  token-stream equality 138/138; offset-advance audit zero violations; per-width cross-diffs (8<->16, 32<->64, 128<->256)
  show only width-driven differences. Hi/lo word ordering symmetric end to end.
- **New keywords breaking existing queries.** `git diff` shows **zero** `KEYWORDS.add`; all new recognisers are
  positionally gated. `SELECT anchor`, `CREATE TABLE live`, `AS start`, `FROM memory`, `SELECT daily`,
  `WINDOW anchor AS (...)` all still parse.
- **ANCHOR silently ignored outside a live view.** `ExpressionParser.anchorAllowed` defaults false and is re-stamped at
  every `SqlParser.parse()` entry (`:6820`). Plain SELECT, CTE, subquery, view body and mat-view body all throw. No
  spelling found that parses and drops it.
- **Non-deterministic function evading the CREATE gate.** `setAllowNonDeterministicFunction(false)` wraps the whole
  `compiler.compile(selectSql)` (`CairoEngine.java:1382`), so the reject fires in
  `FunctionParser.checkAndCreateFunction` for every instantiated node — nesting in CASE, `::` cast, arithmetic, array
  subscript, window args and window ORDER BY / PARTITION BY are all covered.
- **An unrecognised window function silently degrading to a whole-history rebuild.**
  `CairoEngine.validateLiveViewWindowFunction:501` throws at CREATE when `checkpointFunctionIdentity()` or
  `checkpointDependency()` is null, applied to every function in the SELECT — so
  `LiveViewCheckpointFunctionCompiler:378`'s `dependency == null -> continue` is unreachable for a created view.
- **`_lv.s` reading back half-written.** `BlockFileUtils` gives A/B regions, a monotonic version and a per-region CRC
  verified in `getCursor()` before any block is read, plus a spin-lock re-read loop. It throws rather than silently
  resetting a watermark. (The absent *fallback* to the prior region is listed under Moderate as pre-existing.)
- **A missing ingestion gate.** ILP TCP (`LineTcpMeasurementScheduler:492`), ILP HTTP (`LineHttpTudCache:243`), ILP UDP
  (`LineUdpParserImpl:345`), QWP (`QwpTudCache:461`), CSV (`CairoTextWriter:406`) and every SQL mutation path
  (`SqlCompilerImpl.checkViewModification`) all reject via `getType() != Type.TABLE` — by construction, not an
  enumerated list — and all fire before any writer or TUD is acquired. The one gap is `ParallelCsvFileImporter`
  (pre-existing, identical for VIEW/MAT_VIEW), listed under Moderate.
- **Exhaustive-kind regressions from the new `TableToken.Type.LIVE_VIEW`.** Full inventory of every `isView()` /
  `isMatView()` / `isLiveView()` / `getType()` branch in `core/src/main`. Only five files branch on kind and were not
  touched by the PR — `MetadataCache` (x4), `O3PartitionJob`, `ViewCompilerJob` (x4),
  `ShowCreate{MatView,View}RecordCursorFactory`, `TableStorageRecordCursorFactory`, `WalTableListFunctionFactory`,
  `CheckWalTransactionsJob`, `TableSequencerAPI`, `ShowPartitionsRecordCursorFactory`,
  `ApplyWal2TableJob.cleanDroppedTableDirectory` — and every one is correct. **No `else`-chain in the main tree swallows
  LIVE_VIEW into a TABLE branch.** `TableUtils.tableTypeOf` throws on an unknown code rather than degrading.
- **`SecurityContext`'s two new abstract methods.** Every implementation in-repo supplies them;
  `ReadOnlySecurityContext:188` denies, `AllowAllSecurityContext:162` allows, `DenyAllSecurityContext` inherits the
  denial symmetric with the mat-view pair. CREATE authorizes both the permission (`SqlCompilerImpl:4352`,
  `CairoEngine:1295`) and a SELECT on the base's real dependency set (`CairoEngine:1428-1438`, branching to
  `authorizeSelectOnAnyColumn` when empty). Abstract is the right call — 51 of 54 `authorize*` methods are abstract and
  the 3 defaults all delegate to another abstract one, so a no-op default would be the fail-open choice.
- **`buildViewGraphs`'s new mat-view kickstart double-enqueueing.** The no-persisted-state branch returns before the
  persisted-path kickstart; both are mutually exclusive. Cannot fire for a missing/non-WAL base or an INVALID view, and
  `refreshIncremental:1959` re-checks before doing anything.
- **`live_views()` lifecycle.** The retention fix is complete — cursor `close():287-298` releases every reference and
  the factory frees the cursor; all 52 column indices match the metadata order; NULL sentinels are correct per type
  (`getLong` defaults to `LONG_NULL`, so the TIMESTAMP group renders NULL not 1970); the checkpoint tuples are read
  exactly once per row from `volatile long[]`s replaced wholesale.
- **The tier's shared-field publication.** Full audit table built: every writer mutation sits between the `0 -> -1` and
  `-1 -> 0` CAS pair, `publishedIdx` is the last field written before the release and the first read on acquire, and no
  reader caches a column base address (`SlotPageFrame` recomputes per call). Reader flyweights are per-cursor, so
  `recordA`/`recordB` never re-point each other.
- **Reverse-index prune horizon.** Cannot drop a band a live slot can reach — proven for all three call shapes;
  `acquireRead` only ever pins `publishedIdx`, whose horizon was already stamped.
- **Checkpoint codec bounds checking.** Every length, offset and count crossing the disk boundary that drives a loop,
  an allocation or an `Unsafe` access is validated first — `MetaSegmentReader.openPageAt`, all three tree `decode`s
  (which compute `need` in `long` *before* `ensureLeafCapacity`), `PartitionMapNode`, `DataSegmentReader.openPage`,
  `LiveViewStatePageReader`. The superblock CRC covers all 172 preceding bytes — no subset hole. fsync ordering is
  correct in all four publish paths: data pages, then metadata segments, then the superblock that names them.
- **Copy-on-write tree correctness.** Path-copy never mutates a published page; `predecessor` is exact including
  timestamp ties; `floor` special-cases `Long.MAX_VALUE`; `rangeRec`'s skip predicate is conservative; `successorRec` is
  genuinely O(log n).
- **Generation pinning.** The purge rule (`oldestValidSlotGeneration >= retireGeneration && minPinnedGeneration >
  retireGeneration`) is correct and conservative on both axes; all seven pin sites are try-with-resources.
- **Compaction atomicity.** Sources are never deleted by compaction, only retired; a crash mid-compaction leaves a
  final-name orphan above the durable ceiling, reclaimed by `purgeFinalOrphans`; no reader can follow a page while it
  moves, because pages are copied.
- **Dependents-before-bases checkpoint ordering.** Verified in the code (`DependentViewGraph.orderByDependentViews:329`),
  not just the corrected comments. The freeze/copy window is closed: `startCheckpoint` publishes `freezePending`, takes
  the refresh latch, publishes `freezeInProgress` under that hold, then releases.
- **Type-drift detection** is a full-int `!=` (`LiveViewInstance:875`), so it catches DECIMAL precision/scale and ARRAY
  dimensionality as well as SYMBOL<->VARCHAR. Reachable for `ALTER COLUMN TYPE` — the `keepMatViewsValid` escape hatch is
  set only by `SET_DEDUP_ENABLE`.
- **Configuration delegation.** All 16 new getters are **abstract** on `CairoConfiguration`, so a missed wrapper
  delegation is a compile error. Verified anyway: all 16 present in the wrapper, `DefaultCairoConfiguration` and
  `server.conf`.
- **Oracle quality in the fuzz suite.** Every oracle recomputes over the base **via SQL**; there is no test-only Java
  re-implementation of the window/checkpoint/seam algorithm in either large test file. Vacuity guards present where they
  matter (`leadChecks > 0`, `nativeRowsValidated > 0`).
- **VWEMA volume-argument fix.** The bug was real on master (none of the four classes overrode `init`/`toTop`/
  `cursorClosed`); the fix propagates all four hooks on all four classes, `init`/`initPartitionBy` call `super` first, and
  `volumeArg` is freed exactly once.
- **Timestamp-driver ceilings.** All 24 constants recomputed independently and verified exact, with the cliff checked at
  each (`ceiling*div <= LMAX`, `ceiling+1` overflows or narrows negative). Negative/zero widths, `Long.MIN_VALUE` and
  `LONG_NULL` all handled. The TTL guard is semantically exact, and the complete inventory of TTL-field readers confirms
  `isOlderThanTtl` is the only arithmetic consumer — so point-of-use guarding really does cover CREATE, ALTER SET TTL,
  CREATE TABLE LIKE, a restored `_meta` and an older binary.
- **Error positions for the RANGE-overflow reject** are exact (recomputed against the pinned tests at columns 50, 74,
  79) — they point at the first character of the offending width, not the keyword and not the expression start.

---

## Coverage

**Process gap: the test-coverage / regression-efficacy agent did not report, so the per-row coverage map the level-3 pass
requires is not rendered here.** What follows is what I verified myself plus what the other agents established.

Round 6's map was **38 behavioral-change rows: 19 tested, 1 exempt, 18 UNTESTED (14 Critical)**. Nothing verified this
round moves that materially — the still-open Criticals (5, 8, 12 above, plus C11 and C12) are exactly the untested ones.

Specific gaps confirmed by recorded searches:

| Change | Test | Verdict |
|---|---|---|
| `lag(...) IGNORE NULLS` in a live view (Critical 10) | none — `grep` over `core/src/test/java/io/questdb/test/cairo/lv/` finds only respect-nulls `lag` | **UNTESTED** |
| `dim_length()` over a live view (Critical 11) | none — no `dim_length` reference under `test/cairo/lv/` | **UNTESTED** |
| Restart floor reconcile with a suspended LV table (Critical 2) | none | **UNTESTED** |
| Unreadable `_txnlog` / purged sequencer part during reconcile (Critical 3) | none | **UNTESTED** |
| `CairoEngine.applyTableRename` with a live-view token, `LiveViewRegistry.renameView`, `LiveViewInstance.updateToken`, `LiveViewDefinition.updateViewName`, `DependentViewGraph.updateLiveViewToken` (round-6 C12) | none in **either** repository. The only LV rename test, `LiveViewTest#testRejectRename:1062`, asserts the SQL path is *refused*, so it cannot reach these arms | **UNTESTED** |
| `drainAppliedBase`'s use of `effectiveReplaceRangeDeleteLo` and `computeApplyAheadBounds` | none — all three REPLACE_RANGE tests in `LiveViewStartFromReplayTest` create the base without DEDUP, so they exercise `drainBaseWal` only | **UNTESTED** |
| Whole-image state-page corruption falling back to the predecessor (Critical 7) | none — the byte-flip test exists only for the 176-byte superblock slot | **UNTESTED** |
| `meta/` file count staying bounded across N seals (Critical 8) | none — `LiveViewCheckpointCompactionTest.purgeCycle` calls `reconcile` directly, bypassing the gate the server enforces | **UNTESTED** |
| Ingestion gates for ILP/UDP, `/imp` CSV, `COPY`, QWP/UDP against a live view | none — tests exist for ILP/HTTP, ILP/TCP and QWP/TCP only, and QWP/UDP is where the datagram-drop regression is | **UNTESTED** |
| Newly wired per-query `MemoryTracker` sites (tier arenas, staging buffer) | no `*MemoryTrackerTest` for a breach / under-limit / leak-loop | **UNTESTED** |

**The PR's own admitted flake is only half-fixed.** `AbstractCairoTest.currentMicros` is a `protected static`
(`AbstractCairoTest.java:140`) with **no** shared `@Before`/`@After` reset — only `setCurrentMicros` writes it. The two
classes involved in the observed failure were patched individually, so every *other* class that pins the clock into the
future still poisons later classes through surefire ordering. A shared reset in `AbstractCairoTest` fixes it for all
classes and makes the 13 dead guards and 27 redundant calls in the fuzz tests obviously removable.

**Mechanical sweeps, all clean:** 0 new `TODO`/`FIXME`/`HACK`/`XXX`; 0 new `@Ignore`/`@Disabled`; 0 `System.out`/`System.err`
in new test code; 0 new `.returnsOnce(`; 0 new `assertPlanNoLeakCheck(` / `getPlan(` / `assertPlanDoesNotContain(`; the
only 2 new `assertSql(` calls are `serverMain.assertSql(...)`, the sanctioned `TestServerMain` wrapper; 0 non-ASCII in
log messages; `assertMemoryLeak` on 29/29 and 95/95 `@Test` methods in the two largest new test files.

---

## PR body corrections

Two claims in the description are wrong and should be fixed before merge, since the body is unusually load-bearing here.

1. **"the refresh driver appends via the fast-path CAS while readers hold it"** — false. `tryAcquireWrite`
   (`LiveViewInMemoryTier.java:427`) is a `0 -> -1` CAS, so the fast path runs **only** when the published slot's refcount
   is exactly zero; `tryAppendStagingInPlace` (`LiveViewRefreshJob.java:6410`) returns `false` and falls through to the
   slow path otherwise. The code is right and the description is wrong — left as written it invites a future change that
   actually does what it says.
2. **The TTL-overflow fix in `TableUtils.isOlderThanTtl`** landed with no labelled section in the body. Per `CLAUDE.md`
   bundling is expected, but each bundled fix gets its own line. Nothing in the PR title or scope currently signals a TTL
   behaviour change.

---

## Summary

**Request changes.**

- **2 red-CI merge blockers**, both carried from round 6 without root-causing. *(Both since
  closed: B2 fixed on this branch, B1 root-caused in enterprise `1c715c344` and awaiting a
  green run.)*
- **12 Critical** — 6 new this round, 6 round-6 Criticals still open. *(10 since fixed;
  Critical 8 deferred to its own PR, Critical 9 pending a decision.)*
- **~30 Moderate**, **14 ranked performance findings**, Minors grouped above.
- Split: roughly **34 in-diff, 6 out-of-diff / pre-existing in visited code** (the `WINDOW JOIN` dynamic-bound helper,
  the mat-view `REFRESH LIMIT`, `ReadParquetRecordCursor.size()`, `BlockFileWriter`'s missing region fallback,
  `ParallelCsvFileImporter`'s missing gate, `WalDataRecord.getSymA` resolving against the cumulative map). All six are
  cheap to fix here and share this branch's CI lineage.
- **Test gate: not satisfied.** 18 UNTESTED rows carried from round 6, and at minimum Criticals 10 and 11 need regression
  tests written alongside their fixes.

The problems cluster in three places: the in-memory tier's **exception paths**, the checkpoint **reclamation and
integrity** story, and the **restart/recovery seams** in `LiveViewRefreshJob`. The genuinely reassuring results — the
DECIMAL window family, the checkpoint codec layer, the A/B superblock commit ordering, the parquet null-pushdown per-type
gate, the seam row-count identity, the exhaustive-kind inventory and the ingestion-gate matrix — all held up under
adversarial tracing.
