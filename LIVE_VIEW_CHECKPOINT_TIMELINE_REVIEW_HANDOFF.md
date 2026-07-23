# Live View Checkpoint Timeline Review Handoff

Date: 2026-07-22<br>
Last validated: 2026-07-23<br>
Reviewed branch: `puzpuzpuz_live_view`<br>
Reviewed revision: `ef518834b905`<br>
Validation revision: `f8f91f0550fa`

## 1. Scope

This review compared the completed OSS implementation against:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md`
- `LIVE_VIEW_LAGGING_FRAME_REPAIR_HANDOFF.md`

The audit covered the versioned timeline, O3 repair planning and publication,
restart restore and corruption handling, physical state sharing and lifecycle,
lagging-frame eligibility, RANGE peer semantics, observability, and the
corresponding test suites.

No immediate localized-repair wrong-result regression was found. However, the
timeline design is not fully implemented as written: five material gaps and one
smaller eligibility imperfection remain. A separate RANGE peer-semantics
observation is recorded because it affects SQL behavior, but it does not make
the current checkpoint repair high bound incorrect.

## 2. Findings

### 2.1 Moderate (confirmed, qualified): EOF and predecessor-resume O3 repairs erase valid prefix checkpoints

The design requires an O3 publication to replace roots in `[C, H)` and reuse
every root outside that interval:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:120-140`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:213-214`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:385-393`

The implementation opens a precise timeline splice only when `H` is finite and
the runtime state can be preserved:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:4059-4089`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:4107-4116`

Otherwise, `retireCheckpointStateOnO3(instance, true)` retires the entire
timeline:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:4173-4188`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:2934-2989`

The predecessor-resume path also restores its chosen root and then retires the
whole timeline before replay:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:3778-3797`

For `H = EOF`, there is no converged suffix to preserve, but roots below `C`
remain correct. Deleting that prefix loses every long-term anchor because of a
single near-head O3 event. A subsequent older O3 event can still localize from
its dependency-derived `L` when the dependency plan is complete, but it can no
longer resume from the discarded predecessor. Without a complete dependency
plan, it may replay from the view boundary.

The current retention test locks this in by resetting the timeline generation
and logical checkpoint ID space after an ordinary near-head O3 repair:

- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewCheckpointLogicalRetentionTest.java:188-227`

This is not a persisted `historyEpoch` increment, but it still conflicts with
the design requirement to retain roots outside the replaced interval.

Recommended change:

1. Add a persistent truncate/splice-above-`C` timeline operation.
2. Preserve the prefix for EOF repair and predecessor resume.
3. Publish a normalized generation over the preserved prefix and rebuilt tail.
4. Do not reset the checkpoint ID space for insert-only O3.

Required regression:

- Build a long timeline, apply a near-head O3 correction whose influence reaches
  EOF, and prove all roots below `C` remain addressable.
- Then apply an older O3 correction and prove it uses the preserved predecessor.

Status (2026-07-23): investigated end to end; design drafted in
`LIVE_VIEW_CHECKPOINT_PREFIX_PRESERVATION_DESIGN.md`. No code landed. The common
case is buildable, but a crash *during* an EOF/predecessor repair is a real
correctness blocker: the truncate makes an old prefix root the timeline head, and
restore replays raw base WAL from the superblock's global watermark
(`LiveViewRefreshJob.java:5966-5977`, `drainBaseWal:2069`), which is purged below
the old head -- so a mid-repair crash cannot WAL-replay the rewritten tail.
Timeline entries carry `createdLvSeqTxn` but no per-entry base seqTxn
(`LiveViewCheckpointTimelineStoreWriter.java:407-413`), so a truncated head has no
correct watermark to advertise. A correct fix needs a durable "repair-in-progress"
marker that forces a mid-repair-crash restart to rebuild the tail from the applied
base table (design doc, Design A), not just a truncate primitive. Awaiting a design
decision before implementation.

### 2.2 Moderate (confirmed, qualified): lazy root corruption rebuilds the whole view instead of healing one logical root

The design's lazy-corruption policy says a malformed data page invalidates only
the selected root version. Recovery should choose a safe predecessor or
dependency reconstruction point and rebuild the same logical checkpoint ID:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:538-542`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:939-947`

`restoreLatestCompatible()` performs one floor lookup and directly restores that
root. It does not retry a predecessor after structural or decoder failure:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreReader.java:194-245`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreReader.java:342-389`

Any restore failure is caught by `tryRestoreFromTimeline()` and routed to an
applied-base rebuild:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:5905-6032`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:6035-6053`

That rebuild reaches the unlocalized O3 path and therefore the whole-timeline
retirement described in finding 2.1. The writer supports generic same-ID
re-versioning during localized repair, but the corruption-recovery path does not
invoke it and has no production unusable-root marker or bounded predecessor
retry.

The direct reader test proves only that a truncated data segment is rejected:

- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewCheckpointTimelineSealTest.java:585-630`

It does not exercise restart recovery, predecessor fallback, same-ID repair, or
retention of unrelated roots.

Recommended change:

1. Distinguish selected-root corruption from generation/superblock corruption.
2. Bound predecessor retries under the existing generation pin.
3. Reconstruct and republish the failed logical checkpoint ID.
4. Surface repeated failures as checkpoint-storage corruption without silently
   deleting unrelated logical roots.

### 2.3 Moderate (confirmed, state-payload scope): state sharing is limited to partitioned DOUBLE RANGE sum/avg

The design states that adjacent bounded-window roots should share unchanged
state and should not duplicate complete frame payloads:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:142-149`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:579-597`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:4141-4142`

The timeline writer uses chunk sharing only when a function implements
`supportsCheckpointRingState()`; otherwise it serializes a complete state page
for every partition at every boundary:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java:880-939`

There is only one production override:

- `core/src/main/java/io/questdb/griffin/engine/functions/window/AvgDoubleWindowFunctionFactory.java:881-906`

It covers `AvgOverPartitionRangeFrameFunction` and the partitioned DOUBLE RANGE
`sum` subclass:

- `core/src/main/java/io/questdb/griffin/engine/functions/window/SumDoubleWindowFunctionFactory.java:353`

ROWS rings, min/max deques, decimals, `last_value`, and the other admitted state
families still serialize complete state images. Their payload storage is
proportional to the sum of each boundary's live state image. Scalar states write
one complete state page per boundary rather than one page per partition.

This finding concerns window-state and frame payloads. Timeline/root metadata
still uses copy-on-write sharing, so it would be too broad to say that the
timeline has no other structural sharing.

Recommended change:

- Either narrow the design and acceptance claims to the implemented function
  family, or add persistent ring/deque/chunk contracts to the remaining admitted
  state families, with per-family sharing and restore tests.

### 2.4 Moderate (confirmed, qualified): physical compaction has no production publication path

The design permits sparsely live segments to be repacked and roots redirected in
a normally published generation:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:1029-1034`

`LiveViewCheckpointDataStore.Candidate` implements source/target ownership and
byte repacking:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointDataStore.java:44-54`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointDataStore.java:78-81`
- `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointDataStore.java:350-395`

There are no production callers of `beginCandidate()`, `repack()`, or
`markPublished()`. The soak test explicitly abandons its candidate because no
path redirects published roots to the compacted pages:

- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewCheckpointSoakTest.java:200-210`
- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewCheckpointSoakTest.java:270-293`

Consequences:

- A segment containing even one live page cannot be reclaimed until it becomes
  naturally unreferenced or compaction redirects its live roots.
- The soak's purge may reclaim nothing despite repeated repair.
- The compaction crash/publication protocol is not exercised end to end.

Separately, the segment catalogue is intentionally monotonic within an epoch:
purge unlinks files but does not remove their catalogue entries
(`LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:3851-3853`). That property
is real, but the missing compaction publication path does not cause it.

Recommended change:

1. Add a production compaction policy and driver.
2. Build redirected roots and segment reference-count updates in one candidate
   generation.
3. Publish through the normal A/B protocol before releasing candidate ownership.
4. Test restart and purge on both sides of the publication commit point.

### 2.5 Moderate (confirmed, qualified): accepted views can require age-unbounded, non-resumable O3 repair

The design says a large finite repair may be resumable but must not silently fall
back to an age-unbounded scan:

- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:132-140`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:306-308`
- `LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md:4154-4155`

The handoff also records that the second eligibility gate still fails open:

- `LIVE_VIEW_LAGGING_FRAME_REPAIR_HANDOFF.md:41-59`

The test suite deliberately accepts `lv_none`, whose compiled dependency plan is
`none`:

- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewSmokeTest.java:13772-13816`

At runtime, an uncovered function prevents localization:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:3535-3559`

The unlocalized path owns no resumable repair session and is documented as unable
to yield:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:4081-4089`

For an unlocalized view, when no usable predecessor exists, one old O3 row can
therefore monopolize the refresh worker for work proportional to the full view
age. This state is reachable before the first checkpoint, after timeline
retirement or loss, or when the correction predates the oldest retained root.
When a usable predecessor exists, replay can resume from that anchor and is
tail-bound, although it still does not yield cooperatively.

`checkpoint_repair_plan=none` makes the static dependency gap observable after
the SELECT has compiled, but does not meet the original eligibility or
per-turn-bound contract.

A related observability gap is that `checkpoint_repair_plan` describes only the
static SQL plans. It may report `rows` even though dedup causes every runtime
repair to reject localization:

- `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java:1081-1097`
- `LIVE_VIEW_LAGGING_FRAME_REPAIR_HANDOFF.md:598-619`

Recommended options:

- Reject `none` at CREATE.
- Complete the missing dependency contracts.
- Or make the full-history fallback durably resumable and explicitly opt-in.

Also expose the last effective repair disposition and denial reason, such as
`dedup`, `incomplete dependency`, `scan budget`, or `resume cheaper`.

### 2.6 Separate SQL semantics observation: RANGE peer behavior differs from reference semantics

`EXCLUDE CURRENT ROW` is implemented by rewriting a raw `CURRENT ROW` high bound
from `0` to `-1`:

- `core/src/main/java/io/questdb/griffin/engine/window/WindowContextImpl.java:120-125`
- `core/src/main/java/io/questdb/griffin/engine/window/LiveViewCheckpointFunctionCompiler.java:640-678`

For ROWS, this means one preceding row. For RANGE, it means one designated-
timestamp tick below the current timestamp, excluding the entire current-
timestamp peer group rather than only the current physical row.

Similarly, RANGE `last_value(... CURRENT ROW)` dispatches to a physical-current-
row implementation even when equal-timestamp rows follow:

- `core/src/main/java/io/questdb/griffin/engine/functions/window/LastValueWindowFunctionFactoryHelper.java:438-449`

The handoff correctly records this SQL semantic deviation:

- `LIVE_VIEW_LAGGING_FRAME_REPAIR_HANDOFF.md:727-741`

PostgreSQL's reference behavior defines RANGE `CURRENT ROW` through the last
peer and distinguishes `EXCLUDE CURRENT ROW` from `EXCLUDE GROUP`:

- <https://www.postgresql.org/docs/current/sql-select.html>
- <https://www.postgresql.org/docs/current/sql-expressions.html>

This is a user-visible semantic imperfection, but it does not prove a checkpoint
repair-bound defect. The repair planner uses the exclusive bound
`H = changeMaxTs + W + 1`. For the stateless `W = 0` case, `[m, m + 1)` includes
every row at timestamp `m`, so correcting peer semantics would not by itself
make the timestamp high bound too narrow. A peer-correct implementation would
still need compatible runtime evaluation, dependency, and buffering contracts.

Recommended change:

- Track the RANGE peer behavior as a separate SQL compatibility issue.
- If peer semantics change, update runtime evaluation and live-view dependency
  contracts together and retain tests proving complete timestamp-tie coverage.

### 2.7 Minor (confirmed under current semantics): keyed stateless RANGE last_value is unnecessarily rejected

A keyed default RANGE window is rejected by the window-level unbounded-state gate
before the function-level stateless carve-out can run:

- `core/src/main/java/io/questdb/griffin/SqlParser.java:1814-1833`
- `core/src/main/java/io/questdb/griffin/SqlParser.java:2087-2135`

The negative test explicitly locks in the over-rejection:

- `core/src/test/java/io/questdb/test/cairo/lv/LiveViewValidationTest.java:331-355`

Under current QuestDB semantics,
`last_value(x) OVER (PARTITION BY key ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
is a per-row projection and has no historical state or forward influence.

Recommended change:

- Resolve eligibility per window-function call, or delay the bare-window reject
  until every consumer of the named window is known.

## 3. Highest-value missing tests

1. Near-head/EOF O3 repair preserves every checkpoint below `C`; a subsequent
   older correction successfully uses one of those preserved predecessors.
2. Predecessor-resume repair preserves the prefix rather than resetting the
   timeline generation and logical ID space.
3. Corrupt newest data page during restart: bounded predecessor fallback,
   reconstruction of the same logical ID, and retention of unrelated roots.
4. Production compaction redirects published roots, survives restart, and
   eventually purges the source segments.
5. Sharing tests for ROWS, min/max deque state, decimal state, and `last_value`,
   rather than only partitioned DOUBLE RANGE sum/avg.
6. Separate SQL compatibility tests for RANGE and ROWS `EXCLUDE CURRENT ROW`,
   including duplicate designated timestamps.
7. RANGE stateless `last_value` with duplicate timestamps, explicitly locking
   the chosen peer semantics and proving that repair includes the complete
   designated-timestamp tie.
8. Keyed stateless RANGE `last_value`: positive CREATE, restart restore, localized
   O3 repair, and fresh-recompute comparison.
9. SQL-level high-bound overflow for a MICROS designated timestamp. The handoff
   asks for both bounds and both timestamp drivers, while the current high-bound
   integration case uses NANOS only.
10. Runtime observability for a statically planned ROWS repair denied by dedup.

## 4. Existing coverage assessed as strong

The following areas have useful direct and end-to-end coverage:

- persistent timeline append, lookup, range splice, and logical retention;
- A/B superblock selection, generation pins, and purge protection;
- finite lagging RANGE and ROWS classification;
- localized lagging RANGE/ROWS repair against recompute oracles;
- `last_value` lagging RANGE, lagging ROWS, and ROWS stateless repair;
- timestamp-tie handling at the finite high boundary;
- decimal checkpoint freeze/restore across physical widths;
- timestamp conversion ceilings for nanos, micros, and millis drivers;
- NANOS RANGE overflow integration and widest-valid-bound behavior.

## 5. Verification performed

The focused regression command was:

```bash
mvn -pl core -DskipITs \
  -Dtest=io.questdb.test.cairo.lv.LiveViewCheckpointLogicalRetentionTest,io.questdb.test.cairo.lv.LiveViewCheckpointTimelineSealTest,io.questdb.test.cairo.lv.LiveViewCheckpointFunctionCompilerTest,io.questdb.test.cairo.lv.LiveViewValidationTest,io.questdb.test.griffin.engine.window.WindowRangeFrameOverflowTest \
  test
```

Result:

```text
Tests run: 79, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Validation at `f8f91f0550fa` also ran targeted datastore, boundary-plan,
corrupt-checkpoint restart, and stateless-splice tests:

```text
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Several findings above are current expected behavior and are explicitly locked in
by tests, so the green run does not invalidate them.

## 6. Suggested implementation order

1. Preserve timeline prefixes for EOF and predecessor-resume repair.
2. Implement per-root corruption fallback and same-ID reconstruction.
3. Decide whether plan-less live views are rejected or receive a resumable
   fallback.
4. Add a production compaction publication path.
5. Extend structural sharing across the remaining large-state function families.
6. Remove the keyed stateless RANGE over-rejection and improve runtime repair
   observability.

Track RANGE peer semantics as a separate SQL compatibility decision rather than
as a blocker for the current checkpoint repair bounds.
