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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
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
    public void testFrontierCompactionDropsEvictedKeysFromTheRoot() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Low enough that the sweep fires as soon as the anchor advances past a
        // bucket and the map is holding more than a couple of accounts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            createView(
                    MIDNIGHT_ANCHOR,
                    "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                            + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                            + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0), "
                            + "('2026-01-01T11:00:03.000000Z', 'acct-4', 40.0)"
            );
            final long evicted;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                Assert.assertEquals(4, anchorWindow().getAnchorMapSize());
                Assert.assertEquals(0, anchorWindow().getCompactionCount());

                // Bucket advances with only acct-1 following along. The second one puts
                // the other three accounts a whole bucket behind the frontier, which is
                // the sweep's eviction cutoff.
                commit("('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'acct-1', 2.0)", job);
                commit("('2026-01-04T01:00:00.000000Z', 'acct-1', 3.0)", job);
                Assert.assertTrue(
                        "the frontier sweep must have run",
                        anchorWindow().getCompactionCount() > 0
                );
                evicted = anchorWindow().getAnchorMapSize();
                Assert.assertTrue(
                        "the sweep must have evicted the behind-frontier accounts, map size=" + evicted,
                        evicted < 4
                );
                assertViewMatchesRecompute(MIDNIGHT_ANCHOR);
                assertDirtySetsClearedByPublish();
            }

            // A sweep removes keys, so the seal that follows it has to full-scan: only
            // a complete scan sees a key the root still holds and the runtime no longer
            // does. If the seal had stayed incremental, the evicted accounts would keep
            // their entries in the root and come back to life here, carrying an
            // accumulator no live row supports.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the restore must rehydrate the swept map, not the pre-sweep one",
                        evicted,
                        anchorWindow().getAnchorMapSize()
                );
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

    private void assertFunctionDirtySize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        int tracked = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Map dirty = functions.getQuick(i).getCheckpointDirtyPartitionMap();
            if (dirty == null) {
                continue;
            }
            tracked++;
            Assert.assertEquals("function " + i + " dirty key count", expected, dirty.size());
        }
        Assert.assertTrue("no window function tracks dirty partitions", tracked > 0);
    }

    private void assertFunctionStateSize(long expected) {
        final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Map state = functions.getQuick(i).getPartitionMap();
            if (state == null) {
                continue;
            }
            Assert.assertEquals("function " + i + " live key count", expected, state.size());
        }
    }

    /**
     * Asserts every window function's root at the head boundary names exactly
     * {@code expected} partitions.
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
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), head));
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                root.of(dir, head.rootRef);
                root.getFunctionDirectoryRef(functionDirectoryRef);
                functions.of(dir, functionDirectoryRef);
                Assert.assertTrue("the view declares checkpoint-capable functions", functions.size() > 0);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getRootRef(i, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    final int[] count = {0};
                    partitions.iterateAll(partitionMapRoot, partition -> count[0]++);
                    Assert.assertEquals("function " + i + " root partition count", expected, count[0]);
                }
            }
        }
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
        Assert.assertTrue("no window function carries partition state", checked > 0);
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
            if (!function.supportsCheckpointState() || function.getPartitionMap() == null) {
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
        Assert.assertTrue("no window function carries partition state", checked > 0);
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

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
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
}
