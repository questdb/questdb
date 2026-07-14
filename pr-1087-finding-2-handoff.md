# PR #1087 — Finding #2 handoff: unify live-view startup and replay semantics

Status: **OSS tracks 1-4 and Enterprise track 5 landed. Only the track 6 coverage tail
remains.** The finding was deliberately left out of the Critical-findings fix commit
(`fab9dff3a`), which addressed findings #1, #3, #4 and #5.

## Where this stands (last updated 2026-07-14)

| Track | State |
| --- | --- |
| 1. Grammar + operation model | DONE — `f49300ce0a` ("Require START FROM on CREATE LIVE VIEW") |
| 2. Persisted definition and state | DONE — `41a7876bd9` ("Seed live views from their START FROM boundary") |
| 3. Initial bounded seed | DONE — same commit |
| 4. Forward refresh and applied-base replay audit | DONE — `c1da9477c0` ("Floor live view replays at the START FROM bound") |
| 5. Enterprise reconstruction and replication | DONE — OSS `3f203bb2a8` + ent `ee61c1dda` ("Reconstruct replica live views from the START FROM bound") |
| 6. Regression coverage | mostly done; the restart/failure and fuzz tail is open — see below |

`41a7876bd9` makes event time the only membership rule on the primary: every view whose base
has committed history CREATEs in SEEDING and seeds from its START FROM boundary, BEGINNING has
no floor at all, and the seed / forward / replay paths all apply the same predicate.
`c1da9477c0` finishes the primary side: every applied-base re-derive now bottoms out at the
boundary, and it uses the same inclusive-lower-bound cursor to get there. `ee61c1dda` bumps the
enterprise submodule pin onto that work and closes the replica side, so both nodes now select
the same base rows for every start mode.

**Start here next:** the track 6 tail (below) — the restart/failure half of the primary matrix,
the fuzz generators, and the replica restart/lag/promotion cases. There is no longer a
structural gap: the membership predicate is one expression, applied in one place per path, on
both nodes. What is left is coverage.

This is not a replica-only bug. The old OSS design used commit time for initial membership
and event time for replay membership, so the primary could change its own result after an
O3 replay and a replica could reconstruct a different result immediately. The chosen fix
removes that split: row membership is event-time based everywhere, and CREATE performs an
initial bounded seed using the same timestamp predicate as replay.

## Chosen product semantics

Replace the old implicit non-BACKFILL / optional `BACKFILL` model with a required
`START FROM` clause. Do not retain `BACKFILL` as an alias because the feature has not
shipped and there is no compatibility requirement.

Canonical forms:

```sql
CREATE LIVE VIEW lv
FLUSH EVERY 1s
START FROM NOW
AS SELECT ...;
```

```sql
CREATE LIVE VIEW lv
FLUSH EVERY 1s
START FROM BEGINNING
AS SELECT ...;
```

```sql
CREATE LIVE VIEW lv
FLUSH EVERY 1s
START FROM '2026-04-01T00:00:15.000000Z'
AS SELECT ...;
```

The parser may continue to accept the existing ordering of the other live-view options,
but `SHOW CREATE LIVE VIEW` must emit one stable canonical position for `START FROM`.

### Membership contract

A row belongs to a live view when its designated timestamp satisfies the view's persisted
event-time boundary. Commit time and originating seqTxn do not decide membership.

- `START FROM NOW` resolves the engine microsecond clock exactly once during CREATE,
  converts it to the base table's designated timestamp units, persists that resolved
  boundary, and includes rows with `ts >= boundary`.
- `START FROM BEGINNING` has no timestamp lower bound. It includes every row in the
  CREATE snapshot and every later row, including a later O3 row older than the oldest row
  present at CREATE.
- `START FROM '<timestamp>'` parses a constant timestamp string using the designated
  timestamp type's precision and includes rows with `ts >= parsedBoundary`. It supports
  both TIMESTAMP_MICRO and TIMESTAMP_NANO bases without truncating the supplied precision.
- All finite boundaries are inclusive. A row exactly at the boundary belongs to the view.
- A designated-timestamp NULL is not eligible for a finite boundary. Preserve the base
  table's existing designated-timestamp constraints and handle sentinel NULL explicitly
  rather than allowing it to compare as an ordinary long.
- A pre-CREATE row above a finite boundary is included by the initial seed.
- A post-CREATE row below a finite boundary is excluded on the forward path and on every
  replay path.
- A boundary in the future is valid: the initial seed may be empty, and later rows become
  eligible according to event time rather than wall-clock arrival.
- O3, dedup, REPLACE_RANGE, partition removal, and truncation operate on the current
  applied-base row set; after recomputation, the boundary predicate alone determines
  which surviving rows feed the live-view window functions.

Reject expressions, NULL, non-constant values, and malformed timestamp strings at CREATE
with the error position on the offending token. `NOW` is syntax, not a call to the
non-deterministic SQL `now()` function.

### Meaning of seqTxn after this change

seqTxn remains essential for progress and concurrency, but it is no longer a membership
filter:

- CREATE pins/captures an initial base snapshot and its target seqTxn.
- The seed reads qualifying rows from that snapshot exactly once.
- After the seed, incremental refresh begins at `seedTargetSeqTxn + 1`.
- `lastProcessedSeqTxn`, `lvConsumedSeqTxn`, WAL purge floors, and checkpoint positions
  continue to use seqTxn.
- Replay and replica reconstruction do not need per-row seqTxn provenance because they
  apply the same event-time predicate as the initial seed.

This distinction must appear in comments and documentation: timestamps decide row
membership; seqTxns decide processing progress.

## Implementation plan

### 1. Grammar and operation model (OSS) — DONE

Landed on `puzpuzpuz_live_view` (OSS PR #6939). All seven items below are implemented:

1. ~~Replace the parsed `BACKFILL` boolean with a start specification:
   `NOW`, `BEGINNING`, or `TIMESTAMP_LITERAL`.~~ `CreateLiveViewOperationBuilder` /
   `CreateLiveViewOperation` carry `startFromKind` plus the raw literal and its position.
2. ~~Require exactly one `START FROM` clause. Reject omitted or duplicate clauses.~~
3. ~~Remove `BACKFILL` from the grammar and return a focused error that points users to
   `START FROM BEGINNING`.~~ `SqlKeywords.isBackfillKeyword` survives only to produce
   that reject.
4. ~~Store both the requested start kind and, for finite modes, the resolved boundary in
   `CreateLiveViewOperation` / its implementation.~~ The resolved boundary lands in
   `LiveViewDefinition.viewLowerBoundTimestamp`; `startFromKind` persists next to it in
   the `_lv` CORE block (the `backfillRequested` bool it replaces was the same byte, and
   the format is unshipped, so the version stays 1).
5. ~~Parse a timestamp literal only after resolving the base designated timestamp type so
   MICRO and NANO use their correct drivers and precision.~~
   `LiveViewDefinition.parseStartFromTimestamp` runs in `CairoEngine.createLiveView`.
6. ~~Update `SHOW CREATE LIVE VIEW` to emit `START FROM NOW` / `START FROM BEGINNING` /
   a canonical quoted timestamp.~~ Emitted in one canonical position, after `PARTITION BY`.
7. ~~Update parser, validation, authorization, and SHOW CREATE tests.~~ Eight new
   `LiveViewValidationTest` cases (required, duplicate, BACKFILL, NULL, expression,
   `NOW()`, unquoted, malformed, MICRO + NANO boundary round-trips); the ~560 existing
   `CREATE LIVE VIEW` statements across the OSS suites now carry the clause.

Behavior deliberately NOT changed in this step, because it belongs to tracks 2-4: the
seeding state machine still keys off BEGINNING (`BACKFILL_STATE_*` names intact), NOW and
an explicit boundary remain forward-only (no initial seed over pre-CREATE rows), and
BEGINNING still installs the earliest-base-row floor rather than having no floor.

Enterprise (`questdb-enterprise` PR #1087) still has ~126 `CREATE LIVE VIEW` statements
without the clause, plus a `LiveViewDefinition` constructor call and
`isBackfillRequested()` use in `LiveViewReplicaLeadReconstructionTest`. Its submodule pin
still points at an older OSS commit, so ent CI is green until someone bumps it; the bump
must carry that test migration.

### 2. Persisted definition and state (OSS) — DONE

Landed on `puzpuzpuz_live_view` as `41a7876bd9` ("Seed live views from their START FROM
boundary"), together with track 3.

1. ~~Replace `backfillRequested` with a start-kind enum/byte plus the resolved lower bound.~~
   Done in track 1 (`startFromKind` + `viewLowerBoundTimestamp`).
2. ~~Bump the definition format.~~ Not needed: the `_lv` / `_lv.s` layouts did not change
   (the start-kind byte reuses the `backfillRequested` slot), and both formats are unshipped.
3. ~~Preserve the original start kind for catalogue / SHOW CREATE.~~ Done in track 1.
4. ~~Generalize `BACKFILL_STATE_*` to a startup-seeding state.~~ `LiveViewState.SEED_STATE_ACTIVE`
   / `SEED_STATE_SEEDING`, lifecycle `SEEDING` (`view_status='seeding'`). **Deviation from the
   plan:** a view over an *empty* base starts ACTIVE and never seeds — see below.
5. ~~Generalize the target/checkpoint fields and comments.~~ `seedTargetSeqTxn`, seed cursor,
   `.bcp` -> `.scp`, catalogue `backfill_target_seqtxn` -> `seed_target_seqtxn`. No BACKFILL
   terminology remains outside the parser's rejection of the old keyword.
6. ~~No compatibility alias needed.~~ Confirmed.

### 3. Initial bounded seed (OSS) — DONE

Landed in the same commit.

1. ~~Reuse/generalize the BACKFILL sweep.~~ `runSeedSweep`, one sweep for every start mode.
2. ~~Pin a base reader snapshot, record its seqTxn as `seedTargetSeqTxn`.~~ Unchanged from the
   old sweep: `seedTargetSeqTxn` is the base sequencer head at CREATE; the sweep pins one
   snapshot at `>= target` and drains from `sweepSeqTxn + 1`.
3. ~~Feed the seed through the same filter/anchor/window/copier/WAL/apply/checkpoint machinery.~~
4. ~~Bound the snapshot cursor at the persisted boundary.~~ **Deviation:** it does NOT wrap in
   `TimestampLowerBoundCursor`. It opens the snapshot *at* the boundary via
   `PageFrameRecordCursorFactory.getCursorFromTimestamp` — the same inclusive-lower-bound cursor
   the forward path uses, which culls partitions and binary-searches into the first one. The
   wrapper would have walked the entire sub-boundary history row by row inside a single
   `hasNext()` call, with no turn budget able to interrupt it — and a zero-qualifying-row seed
   is the *common* case (START FROM NOW over a base of past data), so that scan would have been
   the norm, not a corner. It also keeps `dataOffset` (the positional `skipRows` resume) in one
   coordinate space instead of two.
5. ~~BEGINNING scans with no timestamp floor.~~ It persists `Numbers.LONG_NULL` (= `Long.MIN_VALUE`),
   which `cullPartitions` special-cases into a full scan and which makes `ts >= bound` true for
   every row with no mode branch on the hot paths.
6. ~~Persist ACTIVE and advance progress exactly to the pinned seed target.~~
7. ~~Preserve safe publication; do not report ACTIVE until the transition is durable.~~
8. ~~Keep snapshot/cursor/checkpoint recovery deterministic.~~

**Empty base does not seed.** A base with no committed transaction (`baseHeadSeqTxn == 0`) has
no pre-CREATE history, so the view starts ACTIVE. This is load-bearing, not an optimisation:
the sweep cannot pin a reader at an exact past seqTxn, so it scans whatever the base has applied
by the time its first turn runs. Over an empty base that snapshot would already hold the rows of
any commit that landed between CREATE and that turn, and the seed would swallow commits the
incremental drain owns — bypassing the FLUSH EVERY cadence and the in-memory tier for them.
Membership is unaffected either way (both paths apply the same boundary); the drain is simply the
path that is meant to carry post-CREATE rows.

**Known property, not fixed.** A view over a base *with* history can still have post-CREATE
commits absorbed by the seed's snapshot, for the same reason (the snapshot is taken at the first
turn, not at CREATE). Membership stays correct; those rows just skip the tier/flush cadence once.
Removing this needs a reader pinned at CREATE, which does not survive a restart.

**Bug this work introduced and fixed in the same commit** (found by review, reproduced, regression
test added — `testEmptySeedThenO3DoesNotReplaySubBoundaryHistory`): a zero-row seed left
`latestSeenTs == LONG_NULL`, and the sweep's completion path wrote a head checkpoint from it
unconditionally. The O3 *head-hit* replay floors at `headMaxTs + 1`, and `Long.MIN_VALUE + 1`
admits every base row — so the first out-of-order commit replayed the base's entire history into
the view, sub-boundary rows included. Head-hit was, at the time, the one replay path that did not
apply `viewLowerBoundTimestamp`. Fixed on three sides: the sweep writes no head when it emitted no
row, head-hit refuses a head whose `maxTs` is LONG_NULL, and (track 4) head-hit's floor is now
clamped to the boundary regardless of what the head says.

### 4. Forward refresh and applied-base replay (OSS) — DONE

Landed as `c1da9477c0` ("Floor live view replays at the START FROM bound").

1. ~~Make every finite-boundary forward path apply `ts >= resolvedBoundary`.~~ It did already;
   the counter is `below_lower_bound_count` (in-order drops) / `o3_rejected_count` (O3 drops).
2. ~~Make every applied-base reconstruction use the identical predicate.~~ Audited all of them.
   **Every applied-base re-derive funnels through exactly three scan sites:** `o3HeadMissReplay`,
   `o3HeadHitReplay`, and the forward scan shared by `drainAppliedBase` / `runSeedSweep`.
   Checkpoint-less restart, corrupt/old checkpoint fallback, mid-drain failure, base metadata
   drift and the dedup restart gap are all *callers* of `o3HeadMissReplay` with a different
   `advanceTo` / `lateRowTs` — they inherit its boundary floor and needed no change. The IN MEMORY
   tier rebuild (`rebuildInMemoryTier` -> `stageInMemoryWindowFromDisk`) reads the **LV table**,
   never the base, so no boundary applies to it. Head-hit was the one outlier; see item 4.
3. ~~BEGINNING paths must not install a hidden minimum-timestamp floor.~~ The CREATE-time
   min-timestamp floor is gone; BEGINNING persists LONG_NULL.
4. ~~Keep head-hit optimizations only when they are provably equivalent to full recomputation
   under the selected start boundary.~~ `replayLowTs = Math.max(headMaxTs + 1,
   viewLowerBoundTimestamp)` now, as suggested. The invariant it rests on (a head is only ever
   written from seeded/drained output, which already applied the boundary) still holds, so this
   is a no-op today — but it is now stated at the scan rather than four call sites away.
5. ~~Update comments describing `subscribeFromSeqTxn` as an exclusion mechanism.~~ It is now
   documented as a progress coordinate in `LiveViewStateReader`. It is no longer load-bearing for
   membership and is a candidate for removal once track 5 confirms nothing else needs it.
6. ~~Verify REPLACE_RANGE boundaries cannot preserve an output row that the full predicate would
   remove.~~ **The suspected hole was real.** `rangeLo == Long.MIN_VALUE` IS producible —
   `WalWriter.commitWithParams` takes any long, and a commit built that way applies cleanly to the
   base (the new `testBeginningReplaceRangeFromMinTimestampRemovesDeletedRows` drives one end to
   end). No OSS *producer* emits one today (a mat view derives its replace range from real data
   timestamps), but the LV code must not rest on that. Against a BEGINNING view the clamp
   `max(rangeLo, viewLowerBoundTimestamp)` then yields LONG_NULL, which every downstream reader
   takes as "no trigger timestamp": `drainAppliedBase` missed the overlap outright, and
   `drainBaseWal` passed LONG_NULL on as the replay's `lateRowTs`, so the head-miss REPLACE_RANGE
   fell back to `replayMinTs` and froze everything below it — the view kept derived rows for base
   rows the commit had deleted. Both drains now share `effectiveReplaceRangeDeleteLo`, which pins
   the clamped low at the lowest non-null timestamp.
7. ~~Preserve zero-GC behavior on the refresh hot paths.~~ The boundary is a primitive long, and
   BEGINNING needs no branch at all (LONG_NULL == Long.MIN_VALUE makes `ts >= bound` free).

**Also changed (not on the original list).** Both replay scans opened a *full* base cursor and
walked the sub-boundary history row by row through `TimestampLowerBoundCursor`. They now open the
snapshot AT the floor via `getCursorFromTimestamp` — the same inclusive-lower-bound cursor the
seed (track 3) and the forward drain already use, which culls whole partitions and binary-searches
into the first one. This is the same argument track 3 made for the seed, and it applies harder
here: a rebuild fires on any O3 commit, drift, mid-drain failure, corrupt checkpoint or
checkpoint-less restart, and head-miss paid the walk twice per rebuild (probe pass + recompute).
Head-hit paid it too, which negated the whole point of the branch (re-evaluate only the tail above
the head). `TimestampLowerBoundCursor` survives for the raw-WAL drain, whose source is not a
page-frame factory, and for the enterprise `openLeadScanCursor` hook.

**Audited and deliberately left alone.** `o3HeadMissReplay` reached with `lateRowTs == LONG_NULL`
(drift, mid-drain failure, corrupt .cp, checkpoint-less restart) skips the pure-delete
REPLACE_RANGE when the recompute yields zero surviving rows, so pre-existing output stays on disk
while the watermark advances. That is the same "frozen prefix" rule the view applies to TRUNCATE /
DROP PARTITION / TTL (asserted by `LiveViewDedupBaseTest.testBaseTruncateFreezesDerivedPrefix`):
only a DATA commit's trigger timestamp authorises a deletion. It is consistent with the stated
design, but it does mean a *drift* rebuild whose recompiled SELECT now matches nothing keeps its
old rows. Worth a product decision at some point; it is not a START FROM issue.

### 5. Enterprise reconstruction and replication — DONE

Landed as OSS `3f203bb2a8` ("Share the replace-range clamp with the replica") plus ent
`ee61c1dda` ("Reconstruct replica live views from the START FROM bound"), which also bumps the
submodule pin from `d741da41c1` to `3f203bb2a8`.

0. ~~Check the floor `drainLeadOverride` passes to `openLeadScanCursor`.~~ It was already
   `max(latestSeenTs + 1, viewLowerBoundTimestamp)` — the same expression the primary uses. So the
   membership predicate needed no change; what it needed was for `viewLowerBoundTimestamp` to
   *be* the whole rule, which is what tracks 1-4 did on the other side.
1. ~~Read the start kind and resolved boundary from the replicated `_lv`.~~ Free:
   `WalEvents.reconstructLiveViewFiles` already calls `LiveViewDefinition.readFrom`, which now
   reads `startFromKind` + `viewLowerBoundTimestamp` out of the CORE block.
2. ~~Replace the synthesized non-BACKFILL state assumptions.~~ Also free, and correct as-is: the
   synthesized `_lv.s` defaults to `SEED_STATE_ACTIVE`, which is what a replica should have — it
   never sweeps, it receives the primary's seeded rows as ordinary LV WAL. The OSS
   `refreshInstance` SEEDING gate (`:5500`) already serves disk-only for the one node that *can*
   carry SEEDING locally: a primary demoted or restarted mid-sweep.
3. ~~Reconstruct through the same boundary predicate as OSS.~~ Item 0.
4. ~~BEGINNING scans the whole applied base; NOW/explicit scan inclusively from the boundary.~~
   BEGINNING persists LONG_NULL (== `Long.MIN_VALUE`), so `ts >= bound` is true for every row and
   the replica needs no mode branch either. `skipBaseRowsBelowScanLow` disengages on a LONG_NULL
   floor, which is right — nothing sits below it.
5. ~~Do not replicate `_lv.s` to recover the old subscription rule.~~ Confirmed: nothing needs it.
   `subscribeFromSeqTxn` is a progress coordinate now and is still a removal candidate.
6. ~~Verify promote/demote, create/drop/recreate, lagging apply and metadata drift cannot switch
   start modes or recompute from a different boundary.~~ They cannot: the start kind and the
   boundary live in `_lv`, which is written once at CREATE and replicated verbatim, and every
   re-derive on both nodes reads the boundary from the definition rather than recomputing it.

**Bug found and fixed here.** The replica open-coded the REPLACE_RANGE clamp as
`max(rangeLo, viewLowerBoundTimestamp)`, i.e. the version the primary had *before* `c1da9477c0`
added the LONG_NULL pin. Against a BEGINNING view — whose boundary IS LONG_NULL — a commit whose
range low is `Long.MIN_VALUE` clamps to LONG_NULL, which the overlap check reads as "no trigger
timestamp": the O3 hatch never fires, and the in-RAM accumulators go on counting rows the commit
deleted. `effectiveReplaceRangeDeleteLo` is now `protected` in OSS and the replica calls it, so
both nodes raise on exactly the same commits. Pinned RED-first by
`testReplicaConvergesAfterReplaceRangeFromMinTimestamp` (4-row lead, three of them ghosts, vs the
1-row re-derived lead).

This is the same class of bug as track 4 item 6, and it is worth noting *why* it survived: the
clamp existed in two places. The fix was to delete one of them, not to correct it.

### 6. Required regression coverage

Landed in `LiveViewStartFromSeedTest` (new, 10 tests), `LiveViewStartFromReplayTest` (new, 6
tests), plus updates to `LiveViewSmokeTest` and `LiveViewTest`. The whole live-view suite is 808
tests, green.

`LiveViewStartFromReplayTest` is the track 4 half — the boundary on the paths that re-derive from
the applied base:

- ~~Head-hit replay under a finite boundary: the sub-boundary row does not enter and the head's
  own rows are not renumbered.~~
- ~~A REPLACE_RANGE spanning the boundary (deleting and rewriting rows on both sides of it).~~
- ~~A REPLACE_RANGE entirely below the boundary: the view's rows and numbering are untouched.~~
- ~~A REPLACE_RANGE with `rangeLo == Long.MIN_VALUE` against a BEGINNING view.~~ Fails before
  `c1da9477c0` with a ghost row; see track 4 item 6.
- ~~Dedup replacements on either side of a finite boundary (applied-base drain -> replay).~~

The seeding half (track 3):

- ~~NOW with pre-CREATE future rows: the initial primary seed includes them.~~
- ~~The original example: pre-CREATE 10/20, post-CREATE 30/40, then O3 25 -> 10/20/25/30/40 with
  `row_number()` 1..5; no row first appears merely because replay ran.~~
  (`testNowSeedsPreCreateFutureRowsAndO3ReplayDoesNotChangeThem`)
- ~~NOW with post-CREATE rows below the CREATE boundary: forward and O3 paths both exclude them.~~
- ~~Explicit boundary between existing rows: only rows at/above it seed.~~ (also through a WHERE)
- ~~A row exactly equal to an explicit boundary.~~
- ~~Explicit future boundary with an initially empty seed.~~ — plus the head-checkpoint bug that
  case exposed (`testEmptySeedThenO3DoesNotReplaySubBoundaryHistory`).
- ~~BEGINNING followed by an O3 row older than the CREATE-time minimum: the row is included.~~
- ~~MICRO and NANO timestamp bases, including sub-microsecond explicit literals.~~
- ~~Filtered views.~~ Partitioned/unpartitioned window functions: covered incidentally by the
  existing fuzz variants, not directly by a START FROM test.
- ~~Multi-turn seed~~ (`testSeedYieldsAcrossTurnsAndResumesAtTheBoundary`, driven with
  `cairo.live.view.checkpoint.rows=1`) ~~and restart after the SEEDING-to-ACTIVE transition~~.

The enterprise half (track 5) landed in `ee61c1dda`. The ent suite carries 99 migrated
`CREATE LIVE VIEW` statements plus two new tests:

- ~~Replica create/drop/recreate without advancing the frozen clock; update the existing test that
  currently sidesteps the divergence.~~ `testReplicateLiveViewCreateDropRecreateSeedsBelowClock`
  (`LiveViewReplicationTest`) is the finding's own reproduction, now green: both nodes seed the
  eligible rows and serve `rn` 1..4. Its sibling `testReplicateLiveViewCreateDropRecreate` keeps
  the clock-above case and its comment now describes one mechanism instead of two.
- ~~A BEGINNING replica taking a REPLACE_RANGE with `rangeLo == Long.MIN_VALUE`.~~
  `testReplicaConvergesAfterReplaceRangeFromMinTimestamp` (`LiveViewReplicaLeadReconstructionTest`).

Still TODO:

- Seed checkpoint (`.scp`) restore and a restart immediately *before* the SEEDING-to-ACTIVE
  transition; checkpoint-less replay under a finite boundary. (The checkpoint-less re-derive is
  `o3HeadMissReplay`, whose boundary floor the track 4 tests now cover through the O3 and dedup
  triggers — what is missing is the restart harness that reaches it, which needs a base WAL purge
  plus a missing head `.cp`.)
- Base metadata drift and an injected mid-drain failure during seeding.
- IN MEMORY views through the initial seed.
- Fuzz: teach the generators to pick all three start modes (they currently pick BEGINNING or NOW
  only, and NOW always over an empty base, so the seed path is fuzzed only via BEGINNING). This
  applies to the ent fuzz generators too (`LiveViewReplicaLeadFuzzTest`,
  `LiveViewReplicaLeadSymbolFuzzTest`, `SwitchFuzzTest`, `BackupFuzzTest`), which the track 5
  migration moved onto explicit clauses but did not teach to vary them.
- Explicit-timestamp primary/replica parity. NOW and BEGINNING are both covered end to end now;
  an explicit boundary is covered on the primary only. The predicate is shared, so this is a
  coverage gap rather than a suspected bug.
- Replica reconstruction after restart, lag, and promotion under a finite boundary. (O3 is
  covered — that is what the two new tests drive.)
- ~~Parser/SHOW CREATE round trips and rejection of omitted `START FROM`, old `BACKFILL`,
  malformed literals, duplicates, expressions, and NULL.~~ Landed with track 1.

Use fluent `assertQuery(...).returns(...)`, deterministic clocks, and deterministic worker
drains/hooks. Do not use timing sleeps or `returnsOnce(...)` for these stable results.

**Clock hygiene — read this before adding a live-view test.** `AbstractCairoTest.currentMicros`
is a *static* that is never reset between test classes, so whatever the previous class in the JVM
left behind is what the next one starts with. START FROM NOW resolves against that clock, so a
class that pins it ABOVE its own data and does not clean up will silently move the boundary of
every NOW view in the class that runs after it. This is not hypothetical: `LiveViewStartFromSeedTest`
pins the clock into 2027 for its future-boundary cases, lands immediately before `LiveViewFuzzTest`
in surefire's run order, and `LiveViewFuzzTest` pinned its own clock only `if (currentMicros < 0)` —
so the fuzz inherited a 2027 clock, every NOW view's boundary jumped above its 2026 data, and
`testFuzzDedup` failed against its recompute oracle roughly one full-suite run in five. Both sides
are fixed (the fuzz pins unconditionally in `@Before`; the new class restores `-1` in `@After`),
but any new class must pin its own clock unconditionally and not rely on inheriting one.

## Suggested work split and landing order

All work should remain on the existing live-view branch/PR, but later sessions can own
separate implementation tracks:

1. ~~**Syntax + persisted model:** grammar, operation objects, definition format, SHOW CREATE,
   catalogue naming, and focused validation tests.~~ DONE (`f49300ce0a`).
2. ~~**Generalized OSS seeding:** rename/generalize BACKFILL state and checkpoints, apply finite
   cursor bounds, and complete the SEEDING-to-ACTIVE handoff.~~ DONE (`41a7876bd9`).
3. ~~**OSS replay consistency:** audit every applied-base re-derive, add the primary
   O3/dedup/REPLACE_RANGE regression matrix, and remove old subscription-membership
   assumptions.~~ DONE (`c1da9477c0`). The restart/failure half of the matrix is still open —
   see track 6.
4. ~~**Enterprise reconstruction:** consume replicated start metadata and add parity,
   recreate, lag, restart, and promotion coverage.~~ DONE (`ee61c1dda`) for the metadata, the
   predicate, and the parity/recreate coverage. Lag, restart and promotion under a finite
   boundary remain on the track 6 list.
5. **Final integration/fuzz (NEXT):** update the live-view fuzz generators — OSS and ent — to
   choose all three start modes, and compare every replay/restart result with a from-scratch
   query using the same boundary.

The persisted-model track defined the contract both refresh implementations consume, and
enterprise parity did not need a second encoding: it needed the primary to stop having two
membership rules, after which the replica's existing timestamp floor was already correct.

## Acceptance criteria

The work is complete when:

- ~~Initial seed, incremental refresh, every primary replay, and enterprise reconstruction
  select the same base rows for each start mode.~~ DONE. Primary (`c1da9477c0`): seed, forward
  drain, head-miss and head-hit all floor at `viewLowerBoundTimestamp` and reach it through the
  same inclusive-lower-bound cursor. Replica (`ee61c1dda`): the applied-base lead scan floors at
  the same bound, and the REPLACE_RANGE clamp is now literally the same function.
- ~~The original future-dated-row example has identical primary and replica contents before
  and after O3/restart.~~ Primary half asserted by
  `LiveViewStartFromSeedTest.testNowSeedsPreCreateFutureRowsAndO3ReplayDoesNotChangeThem`; the
  primary/replica half by
  `LiveViewReplicationTest.testReplicateLiveViewCreateDropRecreateSeedsBelowClock`. Restart
  parity on the replica is still open (track 6).
- No applied-base path needs per-row commit provenance for live-view membership. *(Holds: no
  path reads seqTxn to decide membership any more.)*
- ~~BEGINNING truly has no timestamp floor.~~ Done.
- ~~SHOW CREATE and persisted definitions preserve the selected mode and boundary.~~ Done on both
  nodes: `_lv` carries the start kind and the resolved boundary, and it replicates verbatim into
  the sequencer directory.
- Existing WAL purge, checkpoint, refresh-failure, schema-change, and IN MEMORY guarantees
  remain covered and green. *(801 live-view tests green, plus `WalPurgeJobTest`, `CheckpointTest`,
  `ShowCreateTableTest`, the mat-view suites. The initial purge floor for a seeding view moved
  one seqTxn lower — `seedTarget - 1` — which retains strictly more base WAL, never less.)*
- ~~No `BACKFILL` syntax or behaviorally significant non-BACKFILL terminology remains.~~ Done —
  the only surviving mention is `SqlKeywords.isBackfillKeyword`, which exists solely to reject
  the old keyword and point at `START FROM BEGINNING`.

## Previous behavior and root cause

Everything below is the original analysis of the *old* commit-based membership model. It is
kept as validation context — the reasoning that picked the event-time design and the shape of
the bug it removed. It is **not** the current contract, and the code it cites has moved:
`subscribeFromSeqTxn` no longer decides membership on either node, and both the primary replay
and the replica reconstruction now floor at `viewLowerBoundTimestamp`. Read it to understand
why the fix is what it is, not to learn how the system behaves today.

## The divergence

A non-BACKFILL live view is supposed to consume only base commits made **after** CREATE.
It enforces that with two independent mechanisms:

1. **A seqTxn subscribe point.** `CairoEngine.createLiveView` captures the base's sequencer
   head at CREATE and records `subscribeFromSeqTxn = baseHead + 1`
   ([`CairoEngine.java:1422`](questdb/core/src/main/java/io/questdb/cairo/CairoEngine.java#L1422)),
   persisted in `_lv.s`. The incremental drain starts there, so rows in commits `<= baseHead`
   are never seen — **whatever their timestamp**.
2. **A timestamp floor.** `viewLowerBoundTimestamp` (the CREATE moment), persisted in `_lv`.

These agree for the normal case — historical base data sits *below* the create moment, so
both exclude it. They diverge the moment the base holds rows whose timestamp is **above** the
create moment at CREATE time (future-dated data). The subscribe point still excludes them;
the timestamp floor does not.

Only `_lv` replicates (it rides into the sequencer directory); `_lv.s` never does. The replica
synthesizes a default `_lv.s`
([`WalEvents.java:402`](questdb-ent/src/main/java/com/questdb/cairo/wal/transfer/WalEvents.java#L402)),
so it has no subscribe point at all, and its lead reconstruction scans the applied base from
the timestamp floor alone
([`EntLiveViewRefreshJob.java:874`](questdb-ent/src/main/java/com/questdb/cairo/lv/EntLiveViewRefreshJob.java#L874)).
It therefore admits the pre-CREATE future-dated rows the primary excluded.

Observed directly, and documented in the test comment at
[`LiveViewReplicationTest.java:1028`](questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/LiveViewReplicationTest.java#L1028):
with the clock left below the base's existing rows, the primary served `{30, 40}` while the
replica served `{10, 20, 30, 40}` with `rn` 1..4. Both the row set **and** the `row_number()`
numbering diverge, because the replica's accumulators are driven over the extra rows too.

`testReplicateLiveViewCreateDropRecreate` sidesteps this by advancing the frozen clock past
the pre-existing rows before the recreate, so the floor and the subscribe point agree. The
divergent case is described there but not asserted.

## Why the primary is not the reference implementation here

This is the part that decides the scope, and it is the reason no enterprise-side fix is
correct on its own.

**The primary's own applied-base replay ignores `subscribeFromSeqTxn` too.**
`o3HeadMissReplay` ([`LiveViewRefreshJob.java:2785`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L2785))
rebuilds the whole view from the applied base table, and it floors that scan at
`viewLowerBoundTimestamp` and nothing else — see the probe pass at
[`:2845`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L2845) and the
rebuild scan at [`:2868`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L2868).
There is no seqTxn bound anywhere in it.

That path is not exotic. It runs on:

- any out-of-order base commit (the O3 head-miss replay),
- `rebuildActiveWindowStateFromAppliedBase`
  ([`:4518`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L4518) →
  [`:4524`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L4524)),
  reached from a mid-drain refresh failure
  ([`:4663`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L4663))
  and from a base metadata change
  ([`:5262`](questdb/core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java#L5262)),
- the checkpoint-less restart re-derive.

So on a primary, a non-BACKFILL live view over a base with future-dated pre-CREATE rows
**excludes** them while it drains incrementally, and **starts including** them the first time
anything triggers a replay. The primary is already inconsistent with itself. The replica is
merely in the "replay" state permanently, because reconstruction *is* a re-derive.

Framing this as "the replica diverges from the primary" understates it: the two exclusion
mechanisms are not equivalent, and the codebase relies on them being equivalent.

## Why the obvious fixes do not work

**Replicate `subscribeFromSeqTxn` and have reconstruction honour it.** This is what the review
proposes, and the review itself notes the catch: applied-table rows do not carry their
originating transaction. Once `ApplyWal2TableJob` has materialised the base, there is no way to
ask "which rows arrived in commits > H" — the rows are merged into timestamp order and carry no
seqTxn. Shipping the number changes nothing without a way to *use* it. The same wall is why the
primary's own replay cannot honour it.

**Derive base row ranges per transaction.** Only sound for strictly-appending commits. Any O3
or REPLACE_RANGE commit interleaves rows into existing partitions, so a `(txn -> rowLo, rowHi)`
map does not survive. And O3 is precisely the case that triggers the replay in the first place.

**Push the timestamp floor up to the base's max ts at CREATE.** Makes the two mechanisms agree
by making the floor the only one — but it changes primary semantics for the worse: a genuinely
new base row arriving later with a timestamp *below* that high-water mark would now be silently
dropped by the view, on both nodes. Trading a rare over-inclusion for routine under-inclusion.

## Options that would preserve the rejected commit-based semantics

Both alternatives below are OSS design changes rather than enterprise-only fixes. They are
no longer the selected direction because event-time membership removes the need for either.

1. **Give the applied base per-row provenance.** Persist the originating seqTxn (or a
   monotonic commit id) per row, so any applied-base re-derive — primary replay and replica
   reconstruction alike — can filter `commitId >= subscribeFromSeqTxn`. This makes the two
   exclusion mechanisms genuinely equivalent and fixes the primary's self-inconsistency at the
   same time. Costs a column's worth of storage on every WAL table that backs a live view.

2. **Re-derive accumulator state from the live view's own output tier, not from the base.**
   The LV's on-disk tier *is* the primary's output, already correctly excluding pre-CREATE rows.
   A re-derive that rehydrates window state from it (rather than replaying the base) never sees
   the excluded rows at all. This is what the head checkpoint already does properly — the
   problem is that checkpoints do not replicate, so the replica has no such artifact. Making a
   replicable equivalent is the larger version of this option.

Option 1 is the smaller change and fixes both nodes. Option 2 is the more principled one and
removes a whole class of "replay disagrees with incremental" bugs, of which this is one.

## Reproducing it

Take `testReplicateLiveViewCreateDropRecreate`
([`LiveViewReplicationTest.java:1043`](questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/LiveViewReplicationTest.java#L1043))
and **remove the clock advance before the recreate**, so the create moment sits below the
existing base rows (10, 20). The primary serves `{30, 40}`; the replica serves
`{10, 20, 30, 40}` with `rn` 1..4.

For the primary-side half — the part that shows this is not replica-specific — create a
non-BACKFILL live view over a base holding future-dated rows, confirm the view excludes them,
then land a single out-of-order base commit to force `o3HeadMissReplay`. The excluded rows
appear.

## Scope note

The reviewer's coverage map lists the old behavior as row **#24 (Non-BACKFILL exclusion of
pre-existing future rows) — UNTESTED Critical**. The product decision is now made: those rows
are eligible when they meet the selected event-time boundary, and CREATE must seed them
before ordinary incremental refresh. Replace the old coverage label with START FROM
initial-seed/replay parity coverage and implement the regression matrix above.
