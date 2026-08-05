/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the touched-key cadence seal: a seal that freezes the partitions the
 * batch moved rather than every partition the view holds.
 * <p>
 * The whole optimization rests on one invariant - a key whose state moved is in the
 * dirty set, and every path that can remove a key forces a complete scan instead.
 * An end-state comparison alone cannot see a break in it: the runtime keeps serving
 * correct results out of memory, and the omission only surfaces on the next restart.
 * Every case here therefore pairs the from-base recompute oracle with a direct
 * reading of the runtime's dirty set, so a key that stopped being tracked is a
 * failure at the seal rather than a failure a restart happens to expose.
 * <p>
 * A restore is the other way onto that path: reading the generation's head root back
 * leaves the runtime holding exactly what the root holds, which is the same position
 * a publication leaves it in, so the seal that follows may stay incremental. The two
 * restore cases below hold that to the same standard - the head root seeds the
 * baseline and any other root does not.
 * <p>
 * The view is the customer shape the optimization was written for: an anchored
 * WINDOW carrying an unbounded cumulative sum and count per account, which is
 * whole-state per key and therefore takes the incremental branch rather than the
 * ring one.
 */
public class LiveViewCheckpointIncrementalSealTest extends AbstractLiveViewTest {

    // Midnight. ANCHOR DAILY '00:00' is the only daily form the frontier sweep
    // accepts: it desugars into the two-argument timestamp_floor, and
    // LiveViewRefreshJob.isProvablyMonotoneAnchor takes that form and no other.
    private static final String MIDNIGHT_ANCHOR = "00:00";
    // Noon, so a bucket crossing lands in the middle of a day rather than on the
    // calendar boundary a careless oracle would agree with by accident.
    private static final String NOON_ANCHOR = "12:00";
    // Four accounts in one anchor bucket. Every sweep case starts here: the trigger
    // demands at least half the map be reclaimable, so three of these have to fall
    // behind the frontier before anything fires.
    // Enough accounts that a sweep's evicted set dwarfs what one cadence touches, which is
    // the only shape in which the dirty sets' retained capacity is observable at all.
    private static final int SWEEP_CAPACITY_ACCOUNTS = 2_000;
    private static final String SEED_FOUR_ACCOUNTS =
            "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                    + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                    + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                    + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)";

    @Test
    public void testAnchorBucketCrossingCarriesTheNewAnchorValueThroughARestart() throws Exception {
        // One boundary per commit, so each step below is its own seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // Two of the three accounts cross into the 12:00 bucket, one does not.
                // The crossing rewrites their anchor value in place, which is the
                // incremental anchor path: a root that kept the old value would restore
                // a window that thinks it is still in the morning bucket.
                commit("('2026-01-01T12:00:00.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T12:00:01.000000Z', 'acct-2', 2.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                assertDirtySetsClearedByPublish();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(
                        "the view must restore its accumulators from the checkpoint timeline",
                        viewInstance().isCheckpointRestoreSucceeded()
                );
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // The row that reads the restored anchor value back. It sits in the same
                // 12:00 bucket the crossing opened, so a stale anchor value would look
                // like a fresh crossing here and zero acct-1's accumulator - a diff
                // against the recompute rather than a silent saving.
                commit("('2026-01-01T13:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-01T13:00:01.000000Z', 'acct-3', 4.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // And one more crossing, now on top of restored state.
                commit("('2026-01-02T12:00:00.000000Z', 'acct-1', 5.0), "
                        + "('2026-01-02T12:00:01.000000Z', 'acct-4', 6.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                assertDirtySetsClearedByPublish();
            }
        });
    }

    @Test
    public void testFailedPublishKeepsTheTouchedKeysForTheNextSeal() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long baselineGeneration = anchorWindow().getCheckpointBaselineGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                // The seal reaches durable metadata and then dies, so it never calls
                // onCheckpointPersisted. The dirty sets are the runtime's only record
                // of what the failed seal was going to write; clearing them on the way
                // out would leave those keys unwritten by every later seal too.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                );
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                Assert.assertTrue(
                        "the injected failure must be counted as a failed seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
                Assert.assertEquals(
                        "a failed publish must not adopt a new baseline",
                        baselineGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
                Assert.assertTrue(
                        "the failed seal's touched keys must still be dirty",
                        anchorWindow().getCheckpointDirtyAnchorMapSize() > 0
                );
                assertFunctionDirtySize(1);

                // A second key moves while the failure is still injected. Both keys are
                // now owed to the root.
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0)", job);
                assertFunctionDirtySize(2);
                Assert.assertEquals(2, anchorWindow().getCheckpointDirtyAnchorMapSize());

                job.setCheckpointTimelineTestFailureStage(0);
                commit("('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0)", job);
                assertDirtySetsClearedByPublish();
                Assert.assertTrue(
                        "the recovering seal must publish a new generation",
                        anchorWindow().getCheckpointBaselineGeneration() > baselineGeneration
                );
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            // The proof that the three keys the failed seals owed were all written: a
            // restart reads the root and nothing else.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                commit("('2026-01-01T11:00:07.000000Z', 'acct-1', 7.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testASweepInflatedDirtySetGivesItsCapacityBackOnPublish() throws Exception {
        // One boundary per row, so the seed's accounts arrive over many small cadences and
        // the dirty sets never grow to hold more than a couple of keys at a time. That is
        // the steady state the sweep then breaks: it puts every evicted key into those same
        // maps at once, and a plain clear() on publish would leave the peak resident for
        // the view's lifetime.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createViewWithGeneratedSeed(MIDNIGHT_ANCHOR, SWEEP_CAPACITY_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(SWEEP_CAPACITY_ACCOUNTS, anchorWindow().getAnchorMapSize());
                final LongList capacityBefore = readDirtySetKeyCapacities();

                // Two bucket advances with one account following along, so the sweep drops
                // every other account into the dirty sets at once. At one boundary per row
                // the same cadence seals, which is where the capacity has to come back.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(
                        SWEEP_CAPACITY_ACCOUNTS - 1,
                        anchorWindow().getCompactedPartitionCount()
                );
                assertDirtySetsClearedByPublish();

                final LongList capacityAfter = readDirtySetKeyCapacities();
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    Assert.assertTrue(
                            "dirty set " + i + " kept the sweep's capacity: before="
                                    + capacityBefore.getQuick(i) + " after=" + capacityAfter.getQuick(i),
                            capacityAfter.getQuick(i) <= capacityBefore.getQuick(i)
                    );
                }

                // Handing the backing back must not have cost the seal its baseline, nor
                // the view its results.
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testASweepThenAFailedPublishKeepsItsRemovalsForTheRetry() throws Exception {
        // The two halves the pair beside this one covers separately: a seal that dies after
        // durable metadata, and a sweep that fills the dirty sets with removals. Together
        // they are the case where what the failed seal owed the root is not a handful of
        // touched keys but every key the sweep dropped - and the runtime's only record of
        // them is the eviction markers and the inflated capacity holding them. Clearing
        // either on the way out of a failure would leave the root naming partitions no map
        // has, and no later seal would go looking.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createViewWithGeneratedSeed(MIDNIGHT_ANCHOR, SWEEP_CAPACITY_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(SWEEP_CAPACITY_ACCOUNTS, anchorWindow().getAnchorMapSize());

                // One account follows the frontier into the next bucket, leaving the other
                // 1999 a bucket behind. This one seals cleanly; the readings that the failure
                // must not disturb are taken after it, not before.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                assertDirtySetsClearedByPublish();
                final LongList capacityBefore = readDirtySetKeyCapacities();
                final long baselineGeneration = anchorWindow().getCheckpointBaselineGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                // The second advance is what sweeps, and at one boundary per row the same
                // cadence seals - so the seal carrying 1999 removals is the one that dies.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                );
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);

                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(
                        SWEEP_CAPACITY_ACCOUNTS - 1,
                        anchorWindow().getCompactedPartitionCount()
                );
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                Assert.assertTrue(
                        "the injected failure must be counted as a failed seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
                Assert.assertEquals(
                        "a failed publish must not adopt a new baseline",
                        baselineGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
                // Every removal the failed seal was going to write is still marked, and the
                // capacity holding them is still standing - a shrink here would drop them.
                assertEvictionMarkerCount(SWEEP_CAPACITY_ACCOUNTS - 1);
                final LongList capacityDuring = readDirtySetKeyCapacities();
                boolean isInflated = false;
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    isInflated |= capacityDuring.getQuick(i) > capacityBefore.getQuick(i);
                }
                Assert.assertTrue("the sweep's removals must still be held", isInflated);

                // The retry is what publishes them, and only then does the capacity go back.
                job.setCheckpointTimelineTestFailureStage(0);
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0)", job);
                assertDirtySetsClearedByPublish();
                assertEvictionMarkerCount(0);
                Assert.assertTrue(
                        "the recovering seal must publish a new generation",
                        anchorWindow().getCheckpointBaselineGeneration() > baselineGeneration
                );
                final LongList capacityAfter = readDirtySetKeyCapacities();
                for (int i = 0, n = capacityBefore.size(); i < n; i++) {
                    Assert.assertTrue(
                            "dirty set " + i + " kept the sweep's capacity: before="
                                    + capacityBefore.getQuick(i) + " after=" + capacityAfter.getQuick(i),
                            capacityAfter.getQuick(i) <= capacityBefore.getQuick(i)
                    );
                }
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }

            // The proof the removals reached the root rather than merely leaving the maps:
            // a restart reads the root and nothing else, and must find only the survivor.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testDirtyKeyMissingWithoutASweepStillFails() throws Exception {
        // Four rows per boundary, so the three rows below leave the dirty sets standing
        // and the fourth is what seals.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            // What this holds to its contract is the per-function missing-state branch,
            // which a fused group has no state to reach: emptying a grouped function's
            // private map describes nothing the seal reads, because the accumulator is a
            // slice of the window's own entry. The branch still runs for every residual.
            createUnfusedView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-01T12:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-01T12:00:01.000000Z', 'acct-2', 2.0)", job);
                commit("('2026-01-01T12:00:02.000000Z', 'acct-3', 3.0)", job);
                Assert.assertEquals(
                        "no anchor bucket was crossed, so nothing may have been swept",
                        0,
                        anchorWindow().getCompactionCount()
                );

                // State the seal is owed simply disappears, with no sweep anywhere in
                // the picture. That is a bookkeeping bug, not a removal, and relaxing
                // the seal's missing-value branch must not have turned it into one:
                // the root would silently stop naming three live accounts.
                clearFunctionStateMaps();
                commit("('2026-01-01T12:00:03.000000Z', 'acct-4', 4.0)", job);
                Assert.assertTrue(
                        "a dirty key with no live state and no eviction marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    /**
     * The end-state case at the tightest cadence there is - one boundary per row, so the
     * batch that sweeps is the batch that seals and nothing of the sweep's own state
     * outlives the publication. That makes this the one sweep case here that cannot
     * assert the incremental gate: the gate opens and closes inside a single refresh, and
     * the demoting full scan this change replaced arrives at the same root anyway. What
     * it holds is the end state - the root drops the evicted keys and a restart neither
     * resurrects them nor loses the survivor. {@link
     * #testFrontierSweepRecordsEvictionsAndKeepsTheSealIncremental} carries the gate.
     */
    @Test
    public void testFrontierCompactionDropsEvictedKeysFromTheRoot() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Low enough that the sweep fires as soon as the anchor advances past a
        // bucket and the map is holding more than a couple of accounts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final long survivors;
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                Assert.assertEquals(4, anchorWindow().getAnchorMapSize());
                Assert.assertEquals(0, anchorWindow().getCompactionCount());

                // One boundary per row, which is the tightest cadence the sweep can land
                // in: the batch that sweeps is the batch that seals. The bucket advances
                // with only acct-1 following along, and the second advance puts the other
                // three accounts a whole bucket behind the frontier - the eviction cutoff.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                commit("('2026-01-04T01:00:00.000000Z', 'acct-1', 3.0)", job);
                Assert.assertTrue(
                        "the frontier sweep must have run",
                        anchorWindow().getCompactionCount() > 0
                );
                survivors = anchorWindow().getAnchorMapSize();
                Assert.assertTrue(
                        "the sweep must have evicted the behind-frontier accounts, map size=" + survivors,
                        survivors < 4
                );
                // The recorded removals are what take the evicted accounts out of the
                // root; before, only the complete freeze the sweep forced could.
                assertHeadRootPartitionCount((int) survivors);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                assertDirtySetsClearedByPublish();
                sealedLogicalBytes = readLogicalStateBytes();
            }

            // If the seal had kept the evicted accounts, they would come back to life
            // here carrying an accumulator no live row supports.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(
                        "the restore must rehydrate the swept map, not the pre-sweep one",
                        survivors,
                        anchorWindow().getAnchorMapSize()
                );
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                // An evicted account comes back. It starts a fresh bucket, so its
                // accumulator restarts - resurrected state from the root would show up
                // as an inflated sum.
                commit("('2026-01-04T01:00:01.000000Z', 'acct-2', 4.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testFrontierSweepRecordsEvictionsAndKeepsTheSealIncremental() throws Exception {
        // Four rows per boundary, so the sweep lands mid-cadence and the state it leaves
        // behind is readable before the seal consumes it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final long survivors;
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                final long generation = publishedGeneration();

                // Two bucket advances with only acct-1 following along, both under the
                // four-row boundary, so the second one sweeps without sealing.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                survivors = anchorWindow().getAnchorMapSize();
                Assert.assertEquals("only the account that followed the frontier survives", 1, survivors);

                // The claim under test: the sweep no longer pins the next seal to a
                // complete freeze of every live key of every function.
                assertIncrementalGateOpen(generation);

                // What replaced the demotion - the evicted keys are named, alongside the
                // one account the two commits touched.
                Assert.assertEquals(4, anchorWindow().getCheckpointDirtyAnchorMapSize());
                assertFunctionDirtySize(4);
                assertEvictionMarkerCount(3);

                // The fourth row seals, and the recorded removals are what take the three
                // evicted accounts out of the root.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount((int) survivors);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(survivors, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                commit("('2026-01-03T04:00:00.000000Z', 'acct-2', 5.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testKeyEvictedThenRecreatedInOneCadenceIsUpserted() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long generation = publishedGeneration();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                assertEvictionMarkerCount(3);

                // acct-2 was swept two rows ago and is back inside the same cadence. Its
                // dirty entry now has to mean an upsert again: emitting both the removal
                // the sweep recorded and the put this row asks for names one key twice,
                // which the partition-map writer rejects outright.
                commit("('2026-01-03T02:00:00.000000Z', 'acct-2', 3.0)", job);
                assertEvictionMarkerCount(2);
                assertIncrementalGateOpen(generation);

                commit("('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertEquals(
                        "the re-created key must not have produced a duplicate mutation",
                        sealFailuresBefore,
                        viewInstance().getCheckpointSealFailures()
                );
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(2);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(2, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);

                // The re-created account's accumulator restarted with its new bucket, so a
                // resurrected pre-sweep image shows up here as an inflated sum.
                commit("('2026-01-03T05:00:00.000000Z', 'acct-2', 6.0)", job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    @Test
    public void testKeyTouchedThenEvictedInOneCadenceIsRemoved() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            final LongList sealedLogicalBytes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long generation = publishedGeneration();

                // acct-2 is dirty before the cadence crosses a single bucket boundary, and
                // swept two boundaries later - inside the same cadence. Nothing bounds a
                // cadence to one bucket, so the dirty entry has to carry both facts and the
                // seal has to land on the removal.
                commit("('2026-01-01T12:00:00.000000Z', 'acct-2', 1.0)", job);
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 2.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 3.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                assertEvictionMarkerCount(3);
                assertIncrementalGateOpen(generation);

                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(1);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                sealedLogicalBytes = readLogicalStateBytes();
            }

            // The accounting proof: a restore recomputes the logical size by walking the
            // root it reads, so a seal that subtracted the touched-then-evicted key twice
            // - or not at all - disagrees with it here.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(1, anchorWindow().getAnchorMapSize());
                assertLogicalStateBytesEqual(sealedLogicalBytes);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
            }
        });
    }

    /**
     * The fail-safe the three-argument retention default exists for. A function that
     * keeps an incremental baseline, implements retention and never learns to record the
     * sweep's evictions would publish a root still naming keys the runtime dropped, so
     * the sweep's entry point refuses rather than delegates.
     */
    @Test
    public void testRetentionWithoutRemovalTrackingCannotStayIncremental() {
        final RetainingFunctionStub incremental = new RetainingFunctionStub(false);
        try {
            incremental.retainPartitions(null, null, false);
            Assert.fail("retention without removal tracking must not stay incremental");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "without checkpoint removal tracking");
        }
        Assert.assertFalse(
                "the guard must refuse before the retention itself runs",
                incremental.isRetained
        );

        // Recording the removals is what opens the door.
        incremental.retainPartitions(null, null, true);
        Assert.assertTrue(incremental.isRetained);

        // A function already committed to a complete freeze needs no door: the freeze
        // walks its whole map and finds the dropped keys on its own.
        final RetainingFunctionStub fullScan = new RetainingFunctionStub(true);
        fullScan.retainPartitions(null, null, false);
        Assert.assertTrue(fullScan.isRetained);
    }

    @Test
    public void testUnrelatedDirtyAnchorKeyMissingAfterASweepStillFails() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());
                assertEvictionMarkerCount(3);

                // One of the three keys the sweep dropped loses its provenance while the
                // other two keep theirs. A sweep-wide "something was evicted" flag would
                // wave this one through and publish a root missing an entry no sweep took
                // out; the per-key marker is what makes it a hard error.
                Assert.assertEquals(1, anchorWindow().clearCheckpointEvictionMarkers(1));
                Assert.assertEquals(2, anchorWindow().getCheckpointEvictionMarkerCount());
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertTrue(
                        "a dirty anchor key missing without its own marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    @Test
    public void testUnrelatedDirtyFunctionKeyMissingAfterASweepStillFails() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // A residual function, because its own dirty map is what this case breaks -
            // see createUnfusedView.
            createUnfusedView(MIDNIGHT_ANCHOR, SEED_FOUR_ACCOUNTS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                final long sealFailuresBefore = viewInstance().getCheckpointSealFailures();

                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                Assert.assertEquals(1, anchorWindow().getCompactionCount());

                // The same break, one channel over: the anchor keeps every marker it
                // recorded and freezes cleanly, so the raise has to come from the function.
                Assert.assertTrue(clearFunctionEvictionMarkers() > 0);
                Assert.assertEquals(
                        "the anchor's own markers must be untouched",
                        3,
                        anchorWindow().getCheckpointEvictionMarkerCount()
                );
                commit("('2026-01-03T02:00:00.000000Z', 'acct-1', 3.0), "
                        + "('2026-01-03T03:00:00.000000Z', 'acct-1', 4.0)", job);
                Assert.assertTrue(
                        "a dirty partition key missing without its own marker must fail the seal",
                        viewInstance().getCheckpointSealFailures() > sealFailuresBefore
                );
            }
        });
    }

    @Test
    public void testRestoreFromANonHeadRootDoesNotAdoptAnIncrementalBaseline() throws Exception {
        // One boundary per commit, so the timeline below holds several of them and the
        // head has a predecessor to restore instead.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit("('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)", job);
                assertDirtySetsClearedByPublish();
            }

            final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
            final LiveViewCheckpointTimelineEntry predecessor = new LiveViewCheckpointTimelineEntry();
            readHeadBoundaries(head, predecessor);

            // A cadence seal always builds on the timeline head, so state restored from
            // any other root does not describe the entries an incremental seal would
            // leave alone. Restoring the predecessor must therefore leave the runtime on
            // the full scan even though the generation is the one a seal would name.
            final long generation = restoreRoot(predecessor.maxTimestamp, predecessor.checkpointId);
            Assert.assertFalse(
                    "restoring a predecessor must leave the seal's own gate shut",
                    anchorWindow().canFreezeCheckpointIncrementally(generation)
            );
            assertPinnedToFullScan();

            // The same reader, the same runtime, the same generation - only the root
            // differs, and that is what decides it.
            Assert.assertEquals(generation, restoreRoot(head.maxTimestamp, head.checkpointId));
            assertIncrementalBaseline(generation);
        });
    }

    @Test
    public void testRestoreFromTheHeadRootLeavesTheNextSealIncremental() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
            final long restoredGeneration = publishedGeneration();

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                // Reading a root back publishes nothing, so the generation the restore
                // stamped on the runtime is still the one the next seal builds on.
                Assert.assertEquals(
                        "a restore must not publish a generation of its own",
                        restoredGeneration,
                        publishedGeneration()
                );
                assertIncrementalBaseline(restoredGeneration);

                // The first seal after the resume therefore freezes the one key this
                // commit touches. The other three accounts keep the entries the restored
                // root already holds - so the root must still name all four, and their
                // accumulators must survive the next read-back.
                driveRefreshToQuiescence(job);
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);

                // Rows for the accounts the incremental seal left alone. A root that
                // dropped or staled their entries shows up here as a restarted or
                // inflated accumulator rather than as a missing key.
                commit("('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertViewMatchesRecompute(NOON_ANCHOR);
            }
        });
    }

    @Test
    public void testTombstonedTouchedKeyIsRemovedFromTheRoot() throws Exception {
        // Four rows per boundary, so the seed seals and the batch below is one boundary.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            // A residual function, because the removal branch this case holds to its
            // contract is the per-function one - see createUnfusedView.
            createUnfusedView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(4);

                // No supported path leaves a tombstone standing at a seal: resetPartition
                // is the only writer of the bit, and processRow cancels it through
                // markPartitionAlive on the very same row. The removal branch would
                // therefore never run, so poke the bits in directly and hold the branch
                // to its contract - it deletes the key from the root, which is the one
                // thing that must not happen by accident.
                tombstoneEveryPartition();
                commit("('2026-01-01T11:00:04.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-01T11:00:05.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:06.000000Z', 'acct-3', 3.0), "
                        + "('2026-01-01T11:00:07.000000Z', 'acct-4', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertHeadRootPartitionCount(0);
            }
        });
    }

    @Test
    public void testTouchedKeysAreTheOnlyDirtyStateBetweenSeals() throws Exception {
        // Four rows per boundary, so a commit smaller than that refreshes without
        // sealing and leaves the dirty sets readable mid-cadence.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
            createView(
                    NOON_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0), "
                            + "('2026-01-01T11:00:04.000000Z', 'acct-5', 50.0), "
                            + "('2026-01-01T11:00:05.000000Z', 'acct-6', 60.0)"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertDirtySetsClearedByPublish();
                Assert.assertEquals(6, anchorWindow().getAnchorMapSize());

                // One row, one key, no seal. The dirty sets now name what the next seal
                // owes the root, and what they name is the batch rather than the view:
                // this assertion holds at six accounts and at six million.
                commit("('2026-01-01T11:00:06.000000Z', 'acct-1', 1.0)", job);
                Assert.assertEquals(
                        "the anchor dirty set must name the touched key and no other",
                        1,
                        anchorWindow().getCheckpointDirtyAnchorMapSize()
                );
                assertFunctionDirtySize(1);
                assertFunctionStateSize(6);

                // Two more rows over one further key. Still under the boundary, so the
                // dirty sets accumulate rather than reset, and a key touched twice is
                // still one entry.
                commit("('2026-01-01T11:00:07.000000Z', 'acct-2', 2.0), "
                        + "('2026-01-01T11:00:08.000000Z', 'acct-2', 3.0)", job);
                Assert.assertEquals(2, anchorWindow().getCheckpointDirtyAnchorMapSize());
                assertFunctionDirtySize(2);

                // The fourth row crosses the boundary and seals.
                commit("('2026-01-01T11:00:09.000000Z', 'acct-3', 4.0)", job);
                assertDirtySetsClearedByPublish();
                assertViewMatchesRecompute(NOON_ANCHOR);
            }

            final long restoredGeneration = publishedGeneration();
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(NOON_ANCHOR);
                // The restore rehydrated the head root, so the runtime holds exactly what
                // that root holds and the seal after it stays on the touched-key path.
                Assert.assertEquals(
                        restoredGeneration,
                        anchorWindow().getCheckpointBaselineGeneration()
                );
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * The live compiled window functions of the view, so a case can read the dirty
     * set the seal will consume rather than infer it from what the seal wrote.
     */
    private static ObjList<WindowFunction> windowFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory.getWindowFunctions();
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    private LiveViewWindow anchorWindow() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must carry an anchored window", window);
        return window;
    }

    private void assertDirtySetsClearedByPublish() {
        Assert.assertEquals(
                "a published seal must clear the anchor dirty set",
                0,
                anchorWindow().getCheckpointDirtyAnchorMapSize()
        );
        assertFunctionDirtySize(0);
        Assert.assertNotEquals(
                "a published seal must leave a baseline generation behind",
                Numbers.LONG_NULL,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        final long generation = publishedGeneration();
        Assert.assertTrue(
                "the seal must be able to freeze the next boundary incrementally",
                anchorWindow().canFreezeCheckpointIncrementally(generation)
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.getCheckpointDirtyPartitionMap() == null) {
                continue;
            }
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan after a publish",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold the published generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
        }
    }

    /**
     * Asserts the anchor window and every dirty-tracking window function carry exactly
     * {@code expected} eviction markers - the record the sweep leaves behind and the seal
     * turns into removals. A sweep that recorded nothing still leaves correct results in
     * memory, so without this the omission would only surface on a restart.
     * <p>
     * A fused group records nothing of its own: the sweep drops one entry carrying the
     * anchor value and every component together, so the anchor's marker above is the
     * whole record and the tracked-count guard applies only to the residual functions.
     */
    private void assertEvictionMarkerCount(int expected) {
        Assert.assertEquals(
                "anchor eviction marker count",
                expected,
                anchorWindow().getCheckpointEvictionMarkerCount()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tracked = 0;
        boolean fused = false;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                fused = true;
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (dirty == null || tombstoneIndex < 0) {
                continue;
            }
            tracked++;
            final MapRecordCursor cursor = dirty.getCursor();
            final MapRecord record = dirty.getRecord();
            int marked = 0;
            while (cursor.hasNext()) {
                if (record.getValue().getByte(tombstoneIndex) == 1) {
                    marked++;
                }
            }
            Assert.assertEquals("function " + i + " eviction marker count", expected, marked);
        }
        Assert.assertTrue("no window function tracks dirty partitions", fused || tracked > 0);
    }

    /**
     * Asserts every function that keeps a dirty set of its own holds exactly
     * {@code expected} keys.
     * <p>
     * A function the window has fused keeps none: the group's touched keys are the
     * anchor's, marked once for the whole entry. The helper then asserts that one set
     * instead of passing vacuously, which is what the tracked-count guard is for - a
     * view where nothing at all tracked would mean the seal had lost its incremental
     * path rather than moved it.
     */
    private void assertFunctionDirtySize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tracked = 0;
        boolean fused = false;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                fused = true;
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            if (dirty == null) {
                continue;
            }
            tracked++;
            Assert.assertEquals("function " + i + " dirty key count", expected, dirty.size());
        }
        if (fused) {
            Assert.assertEquals(
                    "the fused group's dirty keys are the anchor's",
                    expected,
                    anchorWindow().getCheckpointDirtyAnchorMapSize()
            );
            return;
        }
        Assert.assertTrue("no window function tracks dirty partitions", tracked > 0);
    }

    private void assertFunctionStateSize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                // Its accumulator is a slice of the window's own entry, so the live key
                // count for it is the window's.
                Assert.assertEquals(
                        "function " + i + " live key count",
                        expected,
                        anchorWindow().getAnchorMapSize()
                );
                continue;
            }
            final Map state = function.getPartitionMap();
            if (state == null) {
                continue;
            }
            Assert.assertEquals("function " + i + " live key count", expected, state.size());
        }
    }

    /**
     * Whether the view's anchored window has adopted a fused plan, and so owns the state
     * the grouped functions would otherwise each keep. The per-function assertions below
     * have nothing to read for such a function; the window's own are what carry them.
     */
    private boolean isWindowStateFused() {
        return anchorWindow().getCheckpointWindowStatePlan() != null;
    }

    /**
     * Asserts every per-partition state root at the head boundary names exactly
     * {@code expected} partitions.
     * <p>
     * This view's two calls fuse, so the head's state root is one window root holding
     * both of them and the function directory is empty. The legacy arm is kept because
     * the shape is a property of the compiled plan rather than of the assertion: a view
     * the plan declines still seals one root per function, and the count means the same
     * thing either way.
     */
    private void assertHeadRootPartitionCount(int expected) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue(metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), head));
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                root.of(dir, head.rootRef);
                root.getStateRootRef(stateRootRef);
                int stateRoots = 0;
                if (!stateRootRef.isNull() && windowRoot.ofIfWindowRoot(dir, stateRootRef)) {
                    windowRoot.getPartitionMapRootRef(partitionMapRoot);
                    assertPartitionCount("window state root", expected, partitions, partitionMapRoot);
                    stateRoots++;
                }
                root.getFunctionDirectoryRef(functionDirectoryRef);
                functions.of(dir, functionDirectoryRef);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getRootRef(i, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    assertPartitionCount("function " + i + " root", expected, partitions, partitionMapRoot);
                    stateRoots++;
                }
                Assert.assertTrue("the view declares per-partition checkpoint state", stateRoots > 0);
            }
        }
    }

    private static void assertPartitionCount(
            String what,
            int expected,
            LiveViewCheckpointPartitionMapReader partitions,
            LiveViewCheckpointPageRef partitionMapRoot
    ) {
        final int[] count = {0};
        partitions.iterateAll(partitionMapRoot, partition -> count[0]++);
        Assert.assertEquals(what + " partition count", expected, count[0]);
    }

    /**
     * Asserts the anchor window and every partition-mapped window function hold
     * {@code generation} as their incremental baseline, carry no dirty keys and are off
     * the full scan. Unlike {@link #assertDirtySetsClearedByPublish()} it tolerates a
     * function whose dirty map is still null, which is the state a restart leaves
     * behind: the first row the resumed view processes is what creates it.
     */
    private void assertIncrementalBaseline(long generation) {
        Assert.assertEquals(
                "the anchor window must hold the restored root's generation as its baseline",
                generation,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        Assert.assertFalse(
                "the anchor window must not be pinned to a full scan after a head restore",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertEquals(
                "a freshly restored anchor map must carry no dirty keys",
                0,
                anchorWindow().getCheckpointDirtyAnchorMapSize()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()
                    || function.isWindowStateOwned()
                    || function.getPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            checked++;
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan after a head restore",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold the restored root's generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            Assert.assertTrue(
                    "function " + i + " must carry no dirty keys",
                    dirty == null || dirty.size() == 0
            );
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || checked > 0);
    }

    /**
     * Asserts the anchor window and every partition-mapped window function may still
     * freeze the next boundary on top of {@code generation}. Says nothing about what
     * they have dirty, which is what makes it usable mid-cadence where
     * {@link #assertIncrementalBaseline(long)} is not.
     */
    private void assertIncrementalGateOpen(long generation) {
        Assert.assertFalse(
                "the anchor window must not be pinned to a full scan",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertTrue(
                "the anchor window must be able to freeze the next boundary incrementally",
                anchorWindow().canFreezeCheckpointIncrementally(generation)
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()
                    || function.getCheckpointDirtyPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            checked++;
            Assert.assertFalse(
                    "function " + i + " must not be pinned to a full scan",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must still hold the published generation as its baseline",
                    generation,
                    function.getCheckpointBaselineGeneration()
            );
        }
        Assert.assertTrue("no window function tracks dirty partitions", isWindowStateFused() || checked > 0);
    }

    /**
     * Asserts the anchor window and every partition-mapped function charge what
     * {@code expected} recorded. Read after a restart it is the accounting oracle: the
     * restore recomputes the figure by walking the root it read, so a seal that
     * subtracted an evicted key twice - or never - disagrees here even though the root's
     * contents are right.
     */
    private void assertLogicalStateBytesEqual(LongList expected) {
        Assert.assertEquals(
                "logical state bytes must survive a restart",
                expected.toString(),
                readLogicalStateBytes().toString()
        );
    }

    /**
     * Asserts the anchor window and every partition-mapped window function still demand
     * a complete freeze, which is where a restore that cannot vouch for its root has to
     * leave them.
     */
    private void assertPinnedToFullScan() {
        Assert.assertTrue(
                "the anchor window must still demand a complete freeze",
                anchorWindow().isCheckpointFullScanRequired()
        );
        Assert.assertEquals(
                "the anchor window must hold no baseline generation",
                Numbers.LONG_NULL,
                anchorWindow().getCheckpointBaselineGeneration()
        );
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int checked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()
                    || function.isWindowStateOwned()
                    || function.getPartitionMap() == null) {
                continue;
            }
            checked++;
            Assert.assertTrue(
                    "function " + i + " must still demand a complete freeze",
                    function.isCheckpointFullScanRequired()
            );
            Assert.assertEquals(
                    "function " + i + " must hold no baseline generation",
                    Numbers.LONG_NULL,
                    function.getCheckpointBaselineGeneration()
            );
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || checked > 0);
    }

    private void assertViewMatchesRecompute(String anchorTime) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute(anchorTime) + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * Clears one eviction marker in each dirty-tracking function's dirty set, and returns
     * how many it cleared. The key stays absent from the function's live state and stays
     * in the dirty set, so what the seal sees is a dirty key with no provenance.
     */
    private int clearFunctionEvictionMarkers() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int cleared = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (dirty == null || tombstoneIndex < 0) {
                continue;
            }
            final MapRecordCursor cursor = dirty.getCursor();
            final MapRecord record = dirty.getRecord();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (value.getByte(tombstoneIndex) == 1) {
                    value.putByte(tombstoneIndex, (byte) 0);
                    cleared++;
                    break;
                }
            }
        }
        return cleared;
    }

    /**
     * Empties every window function's live partition map, leaving the dirty set naming
     * keys whose state is gone with no sweep anywhere in the picture. No production path
     * does this: the map is emptied only by paths that force a complete freeze first.
     */
    private void clearFunctionStateMaps() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int cleared = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Map state = functions.getQuick(i).getPartitionMap();
            if (state == null) {
                continue;
            }
            state.clear();
            cleared++;
        }
        Assert.assertTrue("no window function carries partition state", cleared > 0);
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * The same view, over an argument the fused window-state plan declines: an
     * expression is not a direct column reference, and SQL text equality is not a proof
     * that two expressions are one accumulator.
     * <p>
     * A case uses this when what it holds to its contract is the <b>per-function</b>
     * removal or dirty-set branch. Those still run for every residual function - a ring
     * window, {@code count(*)}, an expression argument - but a fused group takes its key
     * domain and its removals from the anchor instead, so poking a function's own
     * tombstone or eviction bit there describes a state nothing in that path reads.
     */
    private void createUnfusedView(String anchorTime, String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn + 0.0) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    private void createView(String anchorTime, String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    /**
     * Seeds {@code accounts} distinct accounts into one anchor bucket through a generated
     * insert, rather than the literal row list {@link #createView} takes. The rows sit a
     * millisecond apart so the whole seed stays inside the 2026-01-01 bucket however many
     * accounts a case asks for.
     */
    private void createViewWithGeneratedSeed(String anchorTime, int accounts) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("INSERT INTO tx SELECT ('2026-01-01T11:00:00.000000Z'::timestamp + x * 1_000)::timestamp, "
                + "('acct-' || x)::symbol, x * 1.0 FROM long_sequence(" + accounts + ")");
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '"
                + anchorTime + "')");
    }

    private long publishedGeneration() {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the view must have published a generation", metaStore.isValid());
            return metaStore.getSuperblock().generation;
        }
    }

    /**
     * Copies the two newest logical boundaries of the published generation into
     * {@code headOut} and {@code predecessorOut}, so a case can name a root the
     * refresh job would never select on its own.
     */
    private void readHeadBoundaries(
            LiveViewCheckpointTimelineEntry headOut,
            LiveViewCheckpointTimelineEntry predecessorOut
    ) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            metaStore.of(dir);
            Assert.assertTrue("the view must have published a generation", metaStore.isValid());
            timeline.of(dir);
            try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
                Assert.assertTrue(
                        "the view must have sealed a boundary",
                        timeline.last(pin.getTimelineRootRef(), headOut)
                );
                Assert.assertTrue(
                        "the view must have sealed at least two boundaries",
                        timeline.predecessor(pin.getTimelineRootRef(), headOut.maxTimestamp, predecessorOut)
                );
            }
        }
    }

    /**
     * The key capacity the anchor's dirty set and every dirty-tracking function's
     * currently retain, in a fixed order so two readings compare directly. Capacity, not
     * size: a publication empties these maps either way, and what the sweep leaves behind
     * is the backing they hold on to.
     */
    private LongList readDirtySetKeyCapacities() {
        final LongList out = new LongList();
        out.add(anchorWindow().getCheckpointDirtyAnchorMapKeyCapacity());
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()) {
                continue;
            }
            final Map dirty = function.getCheckpointDirtyPartitionMap();
            if (dirty == null) {
                continue;
            }
            out.add(dirty.getKeyCapacity());
        }
        Assert.assertTrue("no window function tracks dirty partitions", isWindowStateFused() || out.size() > 1);
        return out;
    }

    /**
     * The logical byte counts the anchor window and every partition-mapped function
     * currently charge, in a fixed order so two readings compare directly.
     */
    private LongList readLogicalStateBytes() {
        final LongList out = new LongList();
        out.add(anchorWindow().getCheckpointLogicalStateBytes());
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (function.isWindowStateOwned()
                    || function.getPartitionMap() == null
                    || function.supportsCheckpointRingState()) {
                continue;
            }
            out.add(function.getCheckpointLogicalStateBytes());
        }
        Assert.assertTrue("no window function carries partition state", isWindowStateFused() || out.size() > 1);
        return out;
    }

    /**
     * The oracle: the anchored view's semantics restated for the plain window engine.
     * {@code ANCHOR DAILY 'HH:MM'} desugars (SqlParser.desugarDailyAnchor) into
     * exactly this {@code timestamp_floor}, so folding that bucket into the PARTITION
     * BY and running an unbounded frame computes what the anchor computes. Unlike a
     * bare unbounded frame it stays correct across a bucket crossing, which is what
     * these cases are about.
     */
    private String recompute(String anchorTime) {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp)";
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * Rehydrates the live runtime from one exact checkpoint root, the way the restart
     * path does, and returns the generation the restore ran under.
     */
    private long restoreRoot(long maxTimestamp, long checkpointId) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(dir);
            try {
                return reader.restore(
                        maxTimestamp,
                        checkpointId,
                        instance.getLiveViewToken().getTableId(),
                        windowFunctions(instance),
                        instance.getAnchorWindow()
                ).generation;
            } finally {
                reader.detach();
            }
        }
    }

    /**
     * Sets the tombstone bit on every live partition of every window function that
     * carries one. The counter stays where it is on purpose: markPartitionAlive
     * early-exits on a zero count, so the bits survive the rows that follow and reach
     * the seal, which is the state the runtime never produces on its own.
     */
    private void tombstoneEveryPartition() {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tombstoned = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map map = function.getPartitionMap();
            final int tombstoneIndex = function.getTombstoneValueIndex();
            if (map == null || tombstoneIndex < 0) {
                continue;
            }
            final MapRecordCursor cursor = map.getCursor();
            final MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                record.getValue().putByte(tombstoneIndex, (byte) 1);
                tombstoned++;
            }
        }
        Assert.assertTrue("no window function carries a tombstone slot", tombstoned > 0);
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * A window function that implements retention and nothing else of the sweep's
     * contract - no recording hook, no three-argument override. The shape a future
     * implementer produces by migrating half the contract.
     */
    private static final class RetainingFunctionStub implements WindowFunction {
        private final boolean isFullScanRequired;
        private boolean isRetained;

        private RetainingFunctionStub(boolean isFullScanRequired) {
            this.isFullScanRequired = isFullScanRequired;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public boolean isCheckpointFullScanRequired() {
            return isFullScanRequired;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
        }

        @Override
        public void retainPartitions(Map survivingKeys, RecordSink survivingKeySink) {
            isRetained = true;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
        }

        @Override
        public void toPlan(PlanSink sink) {
        }
    }
}
