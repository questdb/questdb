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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A refresh cycle that consumes base commits and puts no row through the window - the
 * view's filter rejected them all - still moves the view's head checkpoint on the
 * duration cadence, so the base table's WAL is released while nothing matches.
 * <p>
 * Every case drives the clock by hand: each commit is refreshed {@link #STEP_MICROS}
 * after the previous one, past the views' 100ms FLUSH EVERY, against a 1s duration
 * cadence. The expected seqTxns follow from that arithmetic: a head written at commit
 * {@code k} is next due at commit {@code k + 4}, 1.2s later.
 */
public class LiveViewNoRowCheckpointTest extends AbstractLiveViewTest {
    private static final long CADENCE_MICROS = 1_000_000;
    private static final long START_MICROS = 1_000_000_000_000L;
    private static final long STEP_MICROS = 300_000;

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadAppliedBase() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.APPLIED_BASE);
    }

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadLead() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.LEAD);
    }

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadRawWal() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.RAW_WAL);
    }

    @Test
    public void testAViewOwedARecoveryDoesNotSealWithoutRows() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                commit(job, minute(3), "a", -1.0);
                commit(job, minute(4), "a", -1.0);
                final LiveViewInstance instance = instance();
                // The seal is due at seqTxn 5, over a runtime a recovery is owed: freezing it
                // would publish state the durable output does not describe.
                LiveViewTestRefreshHooks.install(job).setSimulateNoRowHeadCheckpointStart(
                        () -> instance.setWindowStateDirty(true)
                );
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                // The next turn restores the runtime from the timeline before it drains, and
                // then seals it.
                commit(job, minute(6), "a", -1.0);
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 2, 6);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewThatNeverEmittedHoldsNoBaseWal() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                for (int i = 1; i <= 6; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                final LiveViewInstance instance = instance();
                Assert.assertEquals(6, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(0, countWalSegments(purgeBaseWal()));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDedupBaseAppliedReadAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();
                // Each rejected commit repeats its row, which the dedup keys collapse. The
                // applied base then differs from the raw WAL, so every one of these cycles
                // reads the applied base.
                for (int i = 3; i <= 13; i++) {
                    execute("INSERT INTO base VALUES ('" + minute(i) + "', 'a', -1.0), ('" + minute(i) + "', 'a', -1.0)");
                    refresh(job);
                }
                Assert.assertEquals(cleanCycles, instance.getDedupRawWalCleanCycles());
                assertRejectedStretchAdvancedTheHead(instance);
            }
        });
    }

    @Test
    public void testDedupBaseRawWalCycleAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                // Nothing collapsed, so every rejected commit took the coupled raw-WAL drain.
                Assert.assertEquals(cleanCycles + 11, instance.getDedupRawWalCleanCycles());
                assertRejectedStretchAdvancedTheHead(instance);
            }
        });
    }

    @Test
    public void testFailedNoRowPublicationKeepsTheHeadAndRetries() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();

                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 2; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                // Due at seqTxn 5, and the publication fails: the head and the floor stay.
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(1, 1, 1);
                Assert.assertFalse(instance.isInvalid());

                // The head kept its old write time, so the next cycle is still due and retries.
                job.setCheckpointTimelineTestFailureStage(0);
                commit(job, minute(6), "a", -1.0);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 6);

                // Three failures in a row retire the timeline, as three failed seals do, and
                // release both of the view's floors with it.
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 7; i <= 12; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(4, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                }

                // The cooldown holds the next seal off, rows or not.
                job.setCheckpointTimelineTestFailureStage(0);
                commit(job, minute(13), "a", -1.0);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());

                // Past it, a cycle without rows opens a fresh history at the runtime frontier.
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                commit(job, minute(14), "a", -1.0);
                Assert.assertEquals(14, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(14, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                assertTimeline(1, 1, 14);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(15), "a", 3.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testFilteredViewReleasesBaseWalWhileNoRowMatches() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance);
                assertViewMatchesRecompute();
                Assert.assertEquals(
                        "the purge must keep only the segments above the floor",
                        13 - 9,
                        countWalSegments(purgeBaseWal())
                );
            }
        });
    }

    @Test
    public void testFirstMatchingRowAfterTheStretchMovesThePurgeFloor() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance);

                // The first matching row once the cadence has elapsed seals a root of its own.
                // The floor is the older slot's coordinate, which the stretch has moved on to
                // seqTxn 13, rather than the coordinate of the seal before the stretch.
                setCurrentMicros(currentMicros + CADENCE_MICROS);
                commit(job, minute(14), "a", 3.5);
                Assert.assertEquals(14, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(13, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(ts(minute(14)), instance.getHeadCheckpointMaxTs());
                assertTimeline(5, 3, 14);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testNextSealAfterNoRowPublicationIsIncrementalFused() throws Exception {
        assertNextSealAfterNoRowPublicationIsIncremental(true);
    }

    @Test
    public void testNextSealAfterNoRowPublicationIsIncrementalUnfused() throws Exception {
        assertNextSealAfterNoRowPublicationIsIncremental(false);
    }

    @Test
    public void testNoRowPublicationAtTheDefaultCadence() throws Exception {
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                final long stepMicros = Micros.MINUTE_MICROS;
                commit(job, minute(1), "a", 1.5, stepMicros);
                final LiveViewInstance instance = instance();
                for (int i = 2; i <= 5; i++) {
                    commit(job, minute(i), "a", -1.0, stepMicros);
                }
                // Four minutes in: inside the five-minute default.
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                commit(job, minute(6), "a", -1.0, stepMicros);
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 6);
            }
        });
    }

    @Test
    public void testNoRowPublicationCarriesOnlyTheBaselinesOfTheGenerationItReplaces() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                seedAccounts(job);
                final LiveViewInstance instance = instance();
                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull(window);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                Assert.assertEquals(1, window.getCheckpointBaselineGeneration());
                final LongList baselines = new LongList();
                int sealedFunctions = 0;
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final long baseline = functions.getQuick(i).getCheckpointBaselineGeneration();
                    baselines.add(baseline);
                    if (baseline == 1) {
                        sealedFunctions++;
                    }
                }
                Assert.assertTrue("the seal must have stamped a function baseline", sealedFunctions > 0);

                final long headMaxTs = instance.getHeadCheckpointMaxTs();
                final long headRootId = instance.getHeadCheckpointRootId();
                final long tableId = instance.getLiveViewToken().getTableId();
                final long lifecycleIdentity = instance.getLifecycleIdentity();
                final ObjList<WindowFunction> noFunctions = new ObjList<>();
                try (
                        LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path dir = checkpointsDir(instance)
                ) {
                    // Over the generation the runtime's baselines name: they move.
                    LiveViewCheckpointTimelineStoreWriter.WatermarkResult result = writer.publishWatermark(
                            dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, functions, window, true
                    );
                    Assert.assertEquals(2, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        Assert.assertEquals(
                                baselines.getQuick(i) == 1 ? 2 : baselines.getQuick(i),
                                functions.getQuick(i).getCheckpointBaselineGeneration()
                        );
                    }

                    // With no runtime named, nothing moves.
                    result = writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, noFunctions, null, true);
                    Assert.assertEquals(3, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());

                    // Over a generation the baselines do not name: they stay where they are.
                    result = writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, functions, window, true);
                    Assert.assertEquals(4, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        Assert.assertEquals(
                                baselines.getQuick(i) == 1 ? 2 : baselines.getQuick(i),
                                functions.getQuick(i).getCheckpointBaselineGeneration()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testNoRowPublicationFollowsTheAdaptiveCadence() throws Exception {
        assertNoRowPublicationCadenceAfterACorrection(true);
    }

    @Test
    public void testNoRowPublicationFollowsTheConfiguredCadenceWithoutAdaptation() throws Exception {
        assertNoRowPublicationCadenceAfterACorrection(false);
    }

    @Test
    public void testNoRowPublicationRefusesWhatItCannotVouchFor() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", -1.0);
                final LiveViewInstance instance = instance();
                final long headMaxTs = instance.getHeadCheckpointMaxTs();
                final long headRootId = instance.getHeadCheckpointRootId();
                final long tableId = instance.getLiveViewToken().getTableId();
                final long lifecycleIdentity = instance.getLifecycleIdentity();
                final ObjList<WindowFunction> noFunctions = new ObjList<>();
                try (
                        LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path dir = checkpointsDir(instance);
                        Path emptyDir = new Path().of(configuration.getDbRoot()).concat("lv_no_timeline")
                ) {
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, false),
                            "replica must not publish a live view checkpoint timeline"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId + 1, noFunctions, null, true),
                            "live view checkpoint watermark names a root that is not the newest [maxTimestamp="
                                    + headMaxTs + ", checkpointId=" + (headRootId + 1) + ']'
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs + 1, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark names a root that is not the newest [maxTimestamp="
                                    + (headMaxTs + 1) + ", checkpointId=" + headRootId + ']'
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 0, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint generation watermarks must not move backwards [storedBase=1, nextBase=0]"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId + 1, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark definition identity mismatch"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 1, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark definition identity mismatch"
                    );
                    Assert.assertEquals(0, configuration.getFilesFacade().mkdirs(emptyDir.slash(), configuration.getMkDirMode()));
                    assertRefused(
                            () -> writer.publishWatermark(emptyDir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "cannot advance the base watermark of a live view checkpoint timeline with no valid generation"
                    );
                    // Every refusal left the published generation alone.
                    try (LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)) {
                        superblock.of(dir);
                        Assert.assertEquals(1, superblock.generation);
                        Assert.assertEquals(1, superblock.normalizedBaseSeqTxn);
                    }
                }

                // The view goes on as if nothing had been tried.
                for (int i = 3; i <= 5; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(2, 1, 5);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testOutOfOrderRowAfterNoRowPublicationsMatchesRecompute() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance());

                // Between the two roots, below the head the stretch republished.
                commit(job, "2026-01-01T00:01:30.000000Z", "a", 9.5);
                assertViewMatchesRecompute();
                // And one the filter rejects, below the frontier too.
                commit(job, "2026-01-01T00:01:40.000000Z", "a", -9.5);
                assertViewMatchesRecompute();
                // The repair of that last commit splices the timeline, and its newest root
                // already sits at the frontier, so it re-stamps the head without a root
                // identity. The cadence still moves the head, naming the root by timestamp.
                final LiveViewInstance instance = instance();
                Assert.assertEquals(15, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                for (int i = 16; i <= 18; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(15, instance.getHeadCheckpointBaseSeqTxn());
                commit(job, minute(19), "a", -1.0);
                Assert.assertEquals(19, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(15, instance.getCheckpointTimelineWalPurgeFloor());
                commit(job, minute(20), "a", -1.0);
                commit(job, minute(21), "a", 4.5);
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(22), "a", 5.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRestartAfterNoRowPublicationsRestoresFromTheTimeline() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                Assert.assertEquals(1, instance().getHeadCheckpointBaseSeqTxn());
            }

            // The restore replays seqTxn 2, so the runtime stands above the restored root, and
            // the head this process never wrote seals at the first cycle without rows.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(3), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 2, 3);
                commit(job, minute(4), "a", -1.0);
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
            }

            // The restore replays seqTxn 4, which the filter rejects, so the runtime is the
            // restored root, and the first cycle without rows publishes it at once.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(5), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(3, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(3, 2, 5);
                commit(job, minute(6), "a", 3.5);
                assertViewMatchesRecompute();
            }

            // A restore from that generation replays only seqTxn 6.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(7), "a", 4.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRowNumberViewAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // row_number() keeps no incremental baseline of its own.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, x, row_number() OVER w rn FROM base WHERE x > 0 " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "b", 2.5);
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance());
                assertRowNumberViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(14), "a", 3.5);
                commit(job, minute(15), "b", 4.5);
                assertRestoredFromTimeline("lv");
                assertRowNumberViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRuntimeAtTheHeadRootAdvancesWithoutANewRoot() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();
                final long headRootId = instance.getHeadCheckpointRootId();
                Assert.assertNotEquals(Numbers.LONG_NULL, headRootId);

                for (int i = 2; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                // 900ms since the seal: inside the cadence.
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 5);

                for (int i = 6; i <= 9; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(3, 1, 9);
                // Still the one root, and the runtime still stands on it.
                Assert.assertEquals(headRootId, instance.getHeadCheckpointRootId());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheCheckpointDirectoryStaysBoundedOverNoRowPublications() throws Exception {
        configureCadence(0);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();
                // A zero cadence publishes on every cycle without rows.
                for (int i = 2; i <= 11; i++) {
                    commit(job, minute(i), "a", -1.0);
                    Assert.assertEquals(i, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(i - 1, instance.getCheckpointTimelineWalPurgeFloor());
                }
                assertTimeline(11, 1, 11);
                final File checkpointsRoot = new File(configuration.getDbRoot(), instance.getLiveViewToken().getDirName()
                        + File.separator + LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                final int filesAfterTen = countFiles(checkpointsRoot);
                final long catalogueAfterTen = catalogueSize(instance);
                for (int i = 12; i <= 41; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertTimeline(41, 1, 41);
                Assert.assertEquals(
                        "every publication supersedes a catalogue segment, which the sweep must reclaim",
                        filesAfterTen,
                        countFiles(checkpointsRoot)
                );
                Assert.assertEquals(
                        "the publications must carry the swept segments out of the catalogue",
                        catalogueAfterTen,
                        catalogueSize(instance)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTiedRowsHoldTheHeadUntilARowAboveIt() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                // On the head's own timestamp: the head root does not hold it, and no
                // boundary above the head can.
                commit(job, minute(1), "b", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 8; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
            }

            // The restore replays the tied row from seqTxn 2, which only holds while the
            // generation still starts its replay there.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(9), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                assertViewMatchesRecompute();

                // The first row above it seals both, replacing the head root.
                commit(job, minute(10), "a", 3.5);
                Assert.assertEquals(10, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(10)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 1, 10);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testUnmatchedRowsAfterAnUnsealedRowSealItAtTheCadence() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                // Due at seqTxn 5: the row seqTxn 2 put through the window gets a root of its
                // own, at the runtime frontier.
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 2, 5);
                assertViewMatchesRecompute();
            }
        });
    }

    private static void assertRefused(Runnable publication, String message) {
        try {
            publication.run();
            Assert.fail("the publication must be refused: " + message);
        } catch (CairoException e) {
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private static long catalogueSize(LiveViewInstance instance) {
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader directory = new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                return directory.size();
            }
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static void configureCadence(long durationMicros) {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, durationMicros);
    }

    private static void configureWalPurge() {
        // One WAL segment per commit, so the segment count reads the purge floor.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        // WalPurgeJob is interval-gated off the simulated clock.
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
    }

    private static int countFiles(File dir) {
        final File[] files = dir.listFiles();
        Assert.assertNotNull(files);
        int count = 0;
        for (File file : files) {
            count += file.isDirectory() ? countFiles(file) : 1;
        }
        return count;
    }

    private static long countWalSegments(String tableDirName) {
        final File tableDir = new File(engine.getConfiguration().getDbRoot(), tableDirName);
        final File[] walDirs = tableDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        if (walDirs == null) {
            return 0;
        }
        long segments = 0;
        for (File walDir : walDirs) {
            final File[] segmentDirs = walDir.listFiles(File::isDirectory);
            if (segmentDirs != null) {
                segments += segmentDirs.length;
            }
        }
        return segments;
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    private static String minute(int minute) {
        return "2026-01-01T00:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z";
    }

    /**
     * Runs the purge job over the base table with every hold but the view's lifted, and
     * returns the base table's directory name.
     */
    private static String purgeBaseWal() {
        releaseSavePointHolds("base");
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 60_000_000L);
        try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
            purgeJob.drain(0);
        }
        return engine.verifyTableName("base").getDirName();
    }

    private static void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * The consumed watermark's write fails once, on the cycle the cadence comes due. The
     * head must not move past what the state file holds, and the next cycle, whose write
     * goes through, moves it.
     */
    private void assertAFailedConsumedAdvanceHoldsTheHead(DrainPath drainPath) throws Exception {
        configureCadence(CADENCE_MICROS);
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME) && stateWriteFailures.getAndDecrement() > 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            if (drainPath == DrainPath.LEAD) {
                createFilteredView();
            } else {
                createDedupBaseView();
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                commitRejected(job, drainPath, 3);
                commitRejected(job, drainPath, 4);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();

                stateWriteFailures.set(1);
                commitRejected(job, drainPath, 5);
                Assert.assertEquals("the one write the fault fails", -1, stateWriteFailures.get());
                Assert.assertEquals(5, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(4, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                commitRejected(job, drainPath, 6);
                Assert.assertEquals(6, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 2, 6);
                Assert.assertEquals(
                        drainPath == DrainPath.RAW_WAL ? cleanCycles + 2 : cleanCycles,
                        instance.getDedupRawWalCleanCycles()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    private void assertAnchoredViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " +
                        "sum(amount) OVER (PARTITION BY sym, bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s, " +
                        "sum(fee) OVER (PARTITION BY sym, bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f " +
                        "FROM (SELECT *, timestamp_floor('1d', ts) bucket FROM base WHERE amount > 0)) ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * What the stretch every case shares leaves behind: seqTxn 1 and 2 matched, 3 to 13
     * did not. The cadence came due at 5, 9 and 13. At 5 the row seqTxn 2 put through the
     * window was not sealed yet, so it got a root of its own; at 9 and 13 the runtime was
     * that root, so they moved the generation's base coordinate alone.
     */
    private void assertNextSealAfterNoRowPublicationIsIncremental(boolean isFused) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                seedAccounts(job);
                final LiveViewInstance instance = instance();
                for (int i = 2; i <= 5; i++) {
                    execute("INSERT INTO base VALUES ('2026-01-01T11:00:" + (10 + i) + ".000000Z', 'acct-1', -1.0, 1.00::decimal(38,2))");
                    refresh(job);
                }
                // The runtime was the seed's root, so seqTxn 5 republished it.
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(2, 1, 5);

                final long windowCaptures = instance.getCheckpointCaptureLedgerForTest().getWindowCaptures();
                final long windowIncremental = instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures();
                final long windowImaged = instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged();
                final long functionCaptures = instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures();
                final long functionIncremental = instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures();
                final long functionImaged = instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged();

                // One key moves, and the cadence is due: the seal images that key alone, on
                // top of the root the publication carried forward.
                setCurrentMicros(currentMicros + CADENCE_MICROS);
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:20.000000Z', 'acct-3', 1.0, 1.25::decimal(38,2))");
                refresh(job);
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(3, 2, 6);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowCaptures() - windowCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures() - windowIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged() - windowImaged);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures() - functionCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures() - functionIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged() - functionImaged);
                assertAnchoredViewMatchesRecompute();
            }

            // The incremental root restores with the keys it did not image.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:21.000000Z', 'acct-4', 1.0, 1.50::decimal(38,2))");
                refresh(job);
                assertRestoredFromTimeline("lv");
                assertAnchoredViewMatchesRecompute();
            }
        });
    }

    /**
     * A late row teaches the adaptive cadence a ten-minute correction depth, which halves to
     * a five-minute cadence under the hour configured. Cycles without rows follow whichever
     * cadence is in force.
     */
    private void assertNoRowPublicationCadenceAfterACorrection(boolean isAdaptive) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, String.valueOf(isAdaptive));
        configureCadence(60 * Micros.MINUTE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(0), "a", 1.0);
                commit(job, minute(20), "a", 2.0);
                commit(job, minute(10), "a", 3.0);
                final LiveViewInstance instance = instance();
                if (isAdaptive) {
                    Assert.assertEquals(
                            5 * Micros.MINUTE_MICROS,
                            instance.getEffectiveCheckpointDurationMicros(60 * Micros.MINUTE_MICROS)
                    );
                }
                final long repairedBase = instance.getHeadCheckpointBaseSeqTxn();
                Assert.assertEquals(3, repairedBase);

                // Four minutes of rejected commits: inside either cadence.
                for (int i = 21; i <= 24; i++) {
                    commit(job, minute(i), "a", -1.0, Micros.MINUTE_MICROS);
                }
                Assert.assertEquals(repairedBase, instance.getHeadCheckpointBaseSeqTxn());
                // The fifth minute: due under the learned cadence alone.
                commit(job, minute(25), "a", -1.0, Micros.MINUTE_MICROS);
                Assert.assertEquals(isAdaptive ? 8 : repairedBase, instance.getHeadCheckpointBaseSeqTxn());
                // An hour on: due under either.
                setCurrentMicros(currentMicros + 60 * Micros.MINUTE_MICROS);
                commit(job, minute(26), "a", -1.0);
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                assertViewMatchesRecompute();
            }
        });
    }

    private void assertRejectedStretchAdvancedTheHead(LiveViewInstance instance) throws Exception {
        Assert.assertEquals(13, instance.getStateReader().getLvConsumedSeqTxn());
        Assert.assertEquals(13, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
        // The older of the two superblock slots.
        Assert.assertEquals(9, instance.getCheckpointTimelineWalPurgeFloor());
        assertTimeline(4, 2, 13);
    }

    private void assertRowNumberViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, x, row_number() OVER (PARTITION BY sym, bucket ORDER BY ts) rn " +
                        "FROM (SELECT *, timestamp_floor('1d', ts) bucket FROM base WHERE x > 0)) ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertTimeline(long generation, long entries, long normalizedBaseSeqTxn) throws Exception {
        assertQuery(
                "SELECT checkpoint_timeline_generation, checkpoint_timeline_entries, " +
                        "checkpoint_timeline_normalized_base_seqtxn FROM live_views() WHERE view_name = 'lv'"
        ).noLeakCheck().noRandomAccess().returns(
                "checkpoint_timeline_generation\tcheckpoint_timeline_entries\tcheckpoint_timeline_normalized_base_seqtxn\n" +
                        generation + "\t" + entries + "\t" + normalizedBaseSeqTxn + "\n"
        );
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s " +
                        "FROM base WHERE x > 0 ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(LiveViewRefreshJob job, String ts, String sym, double x) throws Exception {
        commit(job, ts, sym, x, STEP_MICROS);
    }

    private void commit(LiveViewRefreshJob job, String ts, String sym, double x, long stepMicros) throws Exception {
        execute("INSERT INTO base VALUES ('" + ts + "', '" + sym + "', " + x + ")");
        refresh(job, stepMicros);
    }

    private void commitRejected(LiveViewRefreshJob job, DrainPath drainPath, int minute) throws Exception {
        if (drainPath == DrainPath.APPLIED_BASE) {
            // The dedup keys collapse the repeated row, so the applied base differs from the
            // raw WAL, and the cycle reads the applied base.
            execute("INSERT INTO base VALUES ('" + minute(minute) + "', 'a', -1.0), ('" + minute(minute) + "', 'a', -1.0)");
            refresh(job);
        } else {
            commit(job, minute(minute), "a", -1.0);
        }
    }

    private void createAnchoredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, amount DOUBLE, fee DECIMAL(38,2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // The DECIMAL sum keeps a root of its own beside the window's, fused or not.
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                "SELECT ts, sym, sum(amount) OVER w AS s, sum(fee) OVER w AS f FROM base WHERE amount > 0 " +
                "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
    }

    private void createDedupBaseView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        createView();
    }

    private void createFilteredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        createView();
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s " +
                "FROM base WHERE x > 0");
    }

    private void refresh(LiveViewRefreshJob job) {
        refresh(job, STEP_MICROS);
    }

    private void refresh(LiveViewRefreshJob job, long stepMicros) {
        drainWalQueue();
        setCurrentMicros(currentMicros + stepMicros);
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Eight accounts in one commit, seqTxn 1, which seals the first root over all of them.
     */
    private void seedAccounts(LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base SELECT ('2026-01-01T11:00:00.000000Z'::timestamp + x * 1_000_000)::timestamp, " +
                "('acct-' || x)::symbol, x * 1.0, (x * 0.25)::decimal(38,2) FROM long_sequence(8)");
        refresh(job);
        final LiveViewInstance instance = instance();
        Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(8, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged());
        assertTimeline(1, 1, 1);
    }

    private enum DrainPath {
        // A dedup base whose rejected commits collapse rows: the coupled applied-base drain.
        APPLIED_BASE,
        // A plain base: the lead drain and its cadence flush.
        LEAD,
        // A dedup base whose commits collapse nothing: the coupled raw-WAL drain.
        RAW_WAL
    }
}
