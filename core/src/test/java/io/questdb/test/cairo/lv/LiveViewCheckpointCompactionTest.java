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
import io.questdb.cairo.lv.LiveViewCheckpointCompaction;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.std.Chars;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Acceptance coverage for physical checkpoint compaction: the production
 * publication path that repacks the still-live state pages of sparse data
 * segments into one fresh segment and redirects every root onto the relocated
 * pages, so the drained segments retire for the purge job.
 * <p>
 * The view is a partitioned DOUBLE RANGE {@code sum}, the one ring-shaped state
 * family that shares chunk pages across boundaries. That sharing is what makes a
 * segment sparse: a repair re-versions some of the roots that named a shared
 * chunk while others keep naming it, so the segment holding it keeps some live
 * pages and loses others. A workload of in-order seals plus historical
 * corrections builds that sparsity, and the driver reclaims it.
 * <p>
 * The oracle is the same window recomputed from the base table, asserted equal
 * with the refresh fault count held at zero, so a compaction that corrupted a
 * root would surface as a diff (or as a self-healing rebuild the fault count
 * catches) rather than pass silently. Each case then pins one property beyond
 * correctness: that compaction actually published, that the checkpoint id space
 * carried forward unchanged, that a restart restores from the compacted
 * generation, and that the drained segments are eventually unlinked while the
 * target survives.
 */
public class LiveViewCheckpointCompactionTest extends AbstractLiveViewTest {

    // A 30 second look-behind over rows spaced ten seconds apart, so a frame holds a
    // few predecessors and the ring shares their chunks across boundaries.
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";
    private static final String HIGH_CARDINALITY_VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1200' SECOND PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";
    // In-order seals the history is built from, one logical root per commit.
    private static final int SEALS = 40;
    private static final int HIGH_CARDINALITY_SEALS = 200;
    private static final int HIGH_CARDINALITY_SOURCES = 32;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit: the fastest cadence, so the history accumulates
        // the most boundaries a repair can partially supersede.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_COMPACTION_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 0);
        setCurrentMicros(0);
    }

    @Test
    public void testAbandonedCompactionBeforeCommitUnlinksItsTarget() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final LiveViewInstance instance = buildFragmentedHistory(job);
                assertViewMatchesRecompute();
                Assert.assertTrue("the overlapping corrections must leave sparse segments", sparseSegmentCount(instance) > 0);
                final long generationBefore = generation(instance);
                final LongList segmentsBefore = dataSegmentIds(instance);

                // The negative of the case below: fail one step BEFORE the superblock
                // commits. The repack has already renamed the target to its final name,
                // but nothing durable names it and the catalogue never learns the id, so
                // no later purge could ever reclaim it. Abandoning the candidate must
                // take the file with it.
                writer.setTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                try (Path dir = checkpointsDir(instance)) {
                    LiveViewCheckpointCompaction.compact(
                            configuration,
                            dir,
                            writer,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            instance.getLifecycleIdentity(),
                            true,
                            null,
                            100,
                            1,
                            64
                    );
                    Assert.fail("the injected pre-commit failure must propagate");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "test failure after live view checkpoint metadata publication"
                    );
                } finally {
                    writer.setTestFailureStage(0);
                }

                Assert.assertEquals(
                        "a failure before the commit point must not advance the generation",
                        generationBefore,
                        generation(instance)
                );
                final LongList segmentsAfter = dataSegmentIds(instance);
                Assert.assertEquals(
                        "the abandoned target must leave no data segment behind",
                        segmentsBefore.size(),
                        segmentsAfter.size()
                );
                for (int i = 0, n = segmentsBefore.size(); i < n; i++) {
                    Assert.assertEquals("data segment at index " + i, segmentsBefore.getQuick(i), segmentsAfter.getQuick(i));
                }
                assertCataloguedDataSegmentsExist(instance);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testCommittedCompactionSurvivesAFailureAfterSuperblockPublication() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final LiveViewInstance instance = buildFragmentedHistory(job);
                assertViewMatchesRecompute();
                Assert.assertTrue("the overlapping corrections must leave sparse segments", sparseSegmentCount(instance) > 0);
                final long generationBefore = generation(instance);

                // Commit the superblock and then throw. That is the shape a real
                // failure takes past the commit point: the msync that follows the
                // slot write under a non-NOSYNC commit mode reports EIO, or the
                // result tail exhausts the heap. Neither rolls the generation back.
                writer.setTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_SUPERBLOCK_PUBLISH);
                try (Path dir = checkpointsDir(instance)) {
                    LiveViewCheckpointCompaction.compact(
                            configuration,
                            dir,
                            writer,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            instance.getLifecycleIdentity(),
                            true,
                            null,
                            100,
                            1,
                            64
                    );
                    Assert.fail("the injected post-commit failure must propagate");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "test failure after live view checkpoint superblock publication"
                    );
                } finally {
                    writer.setTestFailureStage(0);
                }

                // The commit stands, so the compacted target is named by the roots a
                // restart selects. Abandoning the candidate must not unlink it.
                Assert.assertEquals(
                        "a failure past the commit point must not roll the generation back",
                        generationBefore + 1,
                        generation(instance)
                );
                assertCataloguedDataSegmentsExist(instance);

                // Corroboration rather than a second independent proof: both checks below
                // also hold if the view self-heals by rebuilding, so they bound the damage
                // without pinning the mechanism. The assertion above is the load-bearing one.
                assertViewMatchesRecompute();
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance restored = viewInstance();
                try (LiveViewRefreshJob restartJob = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(restartJob);
                }
                assertViewMatchesRecompute();
                Assert.assertTrue(
                        "the restart must restore from the committed compacted generation",
                        generation(restored) >= generationBefore + 1
                );
            }
        });
    }

    @Test
    public void testCompactionRedirectsRootsSurvivesRestartAndPurgesSources() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final LiveViewInstance instance = buildFragmentedHistory(job);

                assertViewMatchesRecompute();
                Assert.assertTrue("the overlapping corrections must leave sparse segments", sparseSegmentCount(instance) > 0);
                final LongList idsBefore = logicalCheckpointIds(instance);
                final long nextIdBefore = nextCheckpointId(instance);
                final long generationBefore = generation(instance);
                final LongList dataSegmentsBefore = dataSegmentIds(instance);

                // Compact with the loosest policy that still reclaims something: a single
                // segment with any dead byte qualifies, so the pass is driven by whatever
                // sparsity the corrections produced rather than a hand-picked threshold.
                final LiveViewCheckpointCompaction.Result result;
                try (Path dir = checkpointsDir(instance)) {
                    result = LiveViewCheckpointCompaction.compact(
                            configuration,
                            dir,
                            writer,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            instance.getLifecycleIdentity(),
                            true,
                            null,
                            100,
                            1,
                            64
                    );
                }
                Assert.assertTrue("the fragmented history must offer a sparse segment to compact", result.isPublished());
                Assert.assertTrue("a compaction must rewrite at least one root", result.getRootsRewritten() > 0);
                Assert.assertTrue("the compaction target must be a fresh segment", result.getTargetSegmentId() >= dataSegmentsBefore.getLast());
                Assert.assertEquals("a compaction advances the generation by one", generationBefore + 1, result.getGeneration());
                Assert.assertTrue("the compacted target segment is on disk", dataSegmentFileExists(instance, result.getTargetSegmentId()));

                // Compaction relocates bytes without changing a single logical coordinate.
                Assert.assertEquals("the checkpoint id space carries forward unchanged", nextIdBefore, nextCheckpointId(instance));
                final LongList idsAfter = logicalCheckpointIds(instance);
                Assert.assertEquals("every logical entry survives compaction", idsBefore.size(), idsAfter.size());
                for (int i = 0, n = idsAfter.size(); i < n; i++) {
                    Assert.assertEquals("logical entry at index " + i, idsBefore.getQuick(i), idsAfter.getQuick(i));
                }
                Assert.assertEquals("compaction drains every selected sparse segment", 0, sparseSegmentCount(instance));
                assertViewMatchesRecompute();

                // A restart restores from the compacted generation, proving the relocated
                // pages are addressable through their new segment.
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                final LiveViewInstance restored = viewInstance();
                try (LiveViewRefreshJob restartJob = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(restartJob);
                }
                assertViewMatchesRecompute();
                Assert.assertTrue(
                        "the restart restores from the compacted generation rather than rebuilding",
                        generation(restored) >= result.getGeneration()
                );

                // A fresh in-order row folds correctly onto the reconstructed window state.
                appendAndRefresh(job, (SEALS + 5) * 10, 500, 600);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();

                // Advance the generation past the fallback A/B slot and purge: the drained
                // sources lose their last reference at the compaction generation, so once
                // the slot that still named the pre-compaction generation is overwritten and
                // no reader pins it, the purge unlinks them while the target survives.
                for (int i = 0; i < 4; i++) {
                    appendAndRefresh(job, (SEALS + 6 + i) * 10, 700 + i, 800 + i);
                    driveRefreshToQuiescence(job);
                    purgeCycle(instance);
                }
                assertViewMatchesRecompute();

                final LongList dataSegmentsAfter = dataSegmentIds(instance);
                Assert.assertTrue("the compaction target must survive the purge", dataSegmentsAfter.indexOf(result.getTargetSegmentId()) >= 0);
                int purgedSources = 0;
                for (int i = 0, n = dataSegmentsBefore.size(); i < n; i++) {
                    if (dataSegmentsAfter.indexOf(dataSegmentsBefore.getQuick(i)) < 0) {
                        purgedSources++;
                    }
                }
                Assert.assertTrue(
                        "the purge must reclaim at least one drained source segment",
                        purgedSources > 0
                );
            }
        });
    }

    @Test
    public void testCompactionTrackerLimitCleanupAndScratchReuse() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final LiveViewInstance instance = buildFragmentedHistory(job);
                final long generationBefore = generation(instance);
                final int planIdentity = writer.getCompactionPlanIdentityForTest();

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 64);
                final MemoryTracker limitedTracker = acquireRefreshTracker(1);
                try (Path dir = checkpointsDir(instance)) {
                    try {
                        LiveViewCheckpointCompaction.compact(
                                configuration,
                                dir,
                                writer,
                                instance.getLiveViewToken().getTableId(),
                                0,
                                instance.getLifecycleIdentity(),
                                true,
                                limitedTracker,
                                100,
                                1,
                                64
                        );
                        Assert.fail("expected compaction scratch to breach the view tracker");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "LIVE_VIEW_REFRESH");
                    }
                } finally {
                    Assert.assertEquals("failed compaction must return the tracker clean", 0, limitedTracker.getUsed());
                    limitedTracker.close();
                }
                Assert.assertEquals("tracker breach must not publish", generationBefore, generation(instance));
                Assert.assertEquals(planIdentity, writer.getCompactionPlanIdentityForTest());

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 67_108_864);
                final MemoryTracker tracker = acquireRefreshTracker(2);
                try (Path dir = checkpointsDir(instance)) {
                    final LiveViewCheckpointCompaction.Result result = LiveViewCheckpointCompaction.compact(
                            configuration,
                            dir,
                            writer,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            instance.getLifecycleIdentity(),
                            true,
                            tracker,
                            100,
                            1,
                            64
                    );
                    Assert.assertTrue("under-limit compaction must publish", result.isPublished());
                    Assert.assertEquals("successful compaction must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals(planIdentity, writer.getCompactionPlanIdentityForTest());
                    final int readerShellCount = writer.getCompactionReaderShellCountForTest();
                    Assert.assertTrue("fragmented sources must exercise the reader pool", readerShellCount > 1);

                    Assert.assertFalse(
                            "a zero-live-fraction pass must only warm discovery scratch",
                            LiveViewCheckpointCompaction.compact(
                                    configuration,
                                    dir,
                                    writer,
                                    instance.getLiveViewToken().getTableId(),
                                    0,
                                    instance.getLifecycleIdentity(),
                                    true,
                                    tracker,
                                    0,
                                    1,
                                    64
                            ).isPublished()
                    );
                    Assert.assertEquals("repeated discovery must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals(planIdentity, writer.getCompactionPlanIdentityForTest());
                    Assert.assertEquals(readerShellCount, writer.getCompactionReaderShellCountForTest());
                } finally {
                    tracker.close();
                }
                assertCataloguedDataSegmentsExist(instance);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testCompactionTrackerReuseCrossesInitialCapacities() throws Exception {
        assertMemoryLeak(() -> {
            final long[] firstOutcome = new long[5];
            final Object[][] firstReaderShells = new Object[1][];
            try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                final int candidateIdentity = writer.getCompactionCandidateIdentityForTest();
                final int planIdentity = writer.getCompactionPlanIdentityForTest();
                final int[] compactionVisitorIdentities = visitorIdentities(writer, true);
                final int[] rootVisitorIdentities = visitorIdentities(writer, false);
                assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);

                createView(HIGH_CARDINALITY_VIEW_SQL);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = buildHighCardinalityFragmentedHistory(job);
                    final CompactionInputStats inputStats = compactionInputStats(instance);
                    Assert.assertTrue(
                            "fixture must exceed the initial reader-index capacity",
                            inputStats.sparseSegmentCount >= HIGH_CARDINALITY_SOURCES
                    );
                    final LiveViewCheckpointCompaction.Result result = compact(instance, writer, null);
                    Assert.assertTrue(
                            "null-tracker compatibility compaction must publish [nativePages="
                                    + writer.getCompactionLastLivePageCountForTest()
                                    + ", nativeSegments=" + writer.getCompactionLastLiveSegmentCountForTest()
                                    + ", selected=" + writer.getCompactionLastSelectedSegmentCountForTest() + "]",
                            result.isPublished()
                    );
                    Assert.assertEquals(candidateIdentity, writer.getCompactionCandidateIdentityForTest());
                    Assert.assertEquals(planIdentity, writer.getCompactionPlanIdentityForTest());
                    assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);
                    Assert.assertEquals(0, writer.getCompactionOpenReaderCountForTest());
                    Assert.assertTrue(
                            "reader lookup must cross its initial capacity [readers="
                                    + writer.getCompactionReaderShellCountForTest()
                                    + ", selected=" + writer.getCompactionLastSelectedSegmentCountForTest()
                                    + ", sparse=" + inputStats.sparseSegmentCount + "]",
                            writer.getCompactionReaderShellCountForTest() >= HIGH_CARDINALITY_SOURCES
                    );
                    firstReaderShells[0] = new Object[writer.getCompactionReaderShellCountForTest()];
                    for (int i = 0, n = firstReaderShells[0].length; i < n; i++) {
                        firstReaderShells[0][i] = writer.getCompactionReaderShellForTest(i);
                    }
                    Assert.assertTrue(
                            "physical-page index must cross the configured initial capacity",
                            writer.getCompactionLastTargetPageCountForTest() > configuration.getSqlSmallMapKeyCapacity()
                    );
                    Assert.assertEquals(
                            "every sparse source must be selected",
                            inputStats.sparseSegmentCount,
                            writer.getCompactionLastSelectedSegmentCountForTest()
                    );
                    Assert.assertEquals(inputStats.distinctSparsePageCount, writer.getCompactionLastTargetPageCountForTest());
                    Assert.assertTrue("fixture must contain shared source refs", inputStats.sparsePageReferenceCount > inputStats.distinctSparsePageCount);
                    firstOutcome[0] = writer.getCompactionLastSelectedSegmentCountForTest();
                    firstOutcome[1] = writer.getCompactionLastTargetPageCountForTest();
                    firstOutcome[2] = result.getRootsRewritten();
                    firstOutcome[3] = result.getGeneration();
                    firstOutcome[4] = inputStats.sparsePageReferenceCount;
                    assertCataloguedDataSegmentsExist(instance);
                    assertViewMatchesRecompute(HIGH_CARDINALITY_VIEW_SQL);
                }

                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE base");
                setCurrentMicros(0);
                createView(HIGH_CARDINALITY_VIEW_SQL);

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 67_108_864);
                final MemoryTracker tracker = acquireRefreshTracker(3);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final LiveViewInstance instance = buildHighCardinalityFragmentedHistory(job);
                    final CompactionInputStats inputStats = compactionInputStats(instance);
                    Assert.assertEquals(firstOutcome[0], inputStats.sparseSegmentCount);
                    Assert.assertEquals(firstOutcome[1], inputStats.distinctSparsePageCount);
                    Assert.assertEquals(firstOutcome[4], inputStats.sparsePageReferenceCount);
                    final long generationBefore = generation(instance);
                    final LongList segmentsBefore = dataSegmentIds(instance);

                    writer.setCompactionTestFailAfterReaderOpenCount(HIGH_CARDINALITY_SOURCES / 2);
                    try {
                        compact(instance, writer, tracker);
                        Assert.fail("the injected mapped-reader failure must propagate");
                    } catch (CairoException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "test failure after live view checkpoint compaction reader open"
                        );
                    } finally {
                        writer.setCompactionTestFailAfterReaderOpenCount(0);
                    }
                    Assert.assertEquals("reader failure must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals("reader failure must close every mapping", 0, writer.getCompactionOpenReaderCountForTest());
                    assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);
                    Assert.assertEquals("reader failure must not publish", generationBefore, generation(instance));
                    assertLongListEquals("reader failure must unlink its target", segmentsBefore, dataSegmentIds(instance));

                    writer.setCompactionTestFailAfterRepackedPageCount(configuration.getSqlSmallMapKeyCapacity());
                    try {
                        compact(instance, writer, tracker);
                        Assert.fail("the injected repack failure must propagate");
                    } catch (CairoException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "test failure after live view checkpoint compaction page repack"
                        );
                    } finally {
                        writer.setCompactionTestFailAfterRepackedPageCount(0);
                    }
                    Assert.assertEquals("repack failure must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals(
                            "repack failure must close every mapping",
                            0,
                            writer.getCompactionOpenReaderCountForTest()
                    );
                    assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);
                    Assert.assertEquals("repack failure must not publish", generationBefore, generation(instance));
                    assertLongListEquals("repack failure must unlink its target", segmentsBefore, dataSegmentIds(instance));

                    writer.setTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    try {
                        compact(instance, writer, tracker);
                        Assert.fail("the injected publication failure must propagate");
                    } catch (CairoException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "test failure after live view checkpoint metadata publication"
                        );
                    } finally {
                        writer.setTestFailureStage(0);
                    }
                    Assert.assertEquals("publication failure must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals("publication failure must close every mapping", 0, writer.getCompactionOpenReaderCountForTest());
                    assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);
                    Assert.assertEquals("publication failure must not publish", generationBefore, generation(instance));
                    assertLongListEquals("publication failure must unlink its target", segmentsBefore, dataSegmentIds(instance));

                    final LiveViewCheckpointCompaction.Result result = compact(instance, writer, tracker);
                    Assert.assertTrue("tracker-bound compaction must publish", result.isPublished());
                    Assert.assertEquals("successful compaction must return the tracker clean", 0, tracker.getUsed());
                    Assert.assertEquals("successful compaction must close every mapping", 0, writer.getCompactionOpenReaderCountForTest());
                    Assert.assertEquals(candidateIdentity, writer.getCompactionCandidateIdentityForTest());
                    Assert.assertEquals(planIdentity, writer.getCompactionPlanIdentityForTest());
                    assertVisitorShellsClearAndStable(writer, compactionVisitorIdentities, rootVisitorIdentities);
                    Assert.assertEquals(firstOutcome[0], writer.getCompactionLastSelectedSegmentCountForTest());
                    Assert.assertEquals(firstOutcome[1], writer.getCompactionLastTargetPageCountForTest());
                    Assert.assertEquals(firstOutcome[2], result.getRootsRewritten());
                    Assert.assertEquals(firstOutcome[3], result.getGeneration());
                    Assert.assertEquals(
                            "equal high-water passes must reuse every reader shell",
                            firstOutcome[0],
                            writer.getCompactionReaderShellCountForTest()
                    );
                    for (int i = 0, n = firstReaderShells[0].length; i < n; i++) {
                        Assert.assertSame(
                                "equal high-water passes must preserve reader shell identity [index=" + i + "]",
                                firstReaderShells[0][i],
                                writer.getCompactionReaderShellForTest(i)
                        );
                    }
                    assertCataloguedDataSegmentsExist(instance);
                    assertViewMatchesRecompute(HIGH_CARDINALITY_VIEW_SQL);
                } finally {
                    Assert.assertEquals("tracker must be clean before unbind", 0, tracker.getUsed());
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testDisabledPolicyIsANoOp() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final LiveViewInstance instance = buildFragmentedHistory(job);
                final long generationBefore = generation(instance);
                final long tableId = instance.getLiveViewToken().getTableId();

                try (Path dir = checkpointsDir(instance)) {
                    // maxSourceSegments == 0 is the disabled sentinel: nothing is published.
                    Assert.assertFalse(
                            LiveViewCheckpointCompaction.compact(
                                    configuration, dir, writer, tableId, 0, instance.getLifecycleIdentity(), true, null, 100, 1, 0
                            ).isPublished()
                    );
                    // A policy that admits nothing - a segment must be at most 0% live -
                    // publishes nothing either.
                    Assert.assertFalse(
                            LiveViewCheckpointCompaction.compact(
                                    configuration, dir, writer, tableId, 0, instance.getLifecycleIdentity(), true, null, 0, 1, 64
                            ).isPublished()
                    );
                }
                Assert.assertEquals("a no-op pass leaves the generation untouched", generationBefore, generation(instance));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRefreshJobAutoCompactsWhenEnabled() throws Exception {
        // Attempt a compaction pass on every seal. The fixed refresh-job policy - a
        // segment at most half live, at least two of them - matches the sparsity the
        // overlapping corrections build, so the worker reclaims it with no manual driver
        // call, which is what proves the config-gated production trigger is wired.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_COMPACTION_INTERVAL, 1);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildFragmentedHistory(job);
                // One more in-order seal gives the cadence a turn on the now-sparse
                // timeline, so the worker's own pass drains it.
                appendAndRefresh(job, (SEALS + 5) * 10, 500, 600);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                // The sibling case proves this same history leaves three sparse segments
                // with the trigger off. The min-two policy drains them in pairs and only
                // ever leaves a lone unpaired one behind, so at most one survives here -
                // which is only reachable if the worker actually compacted.
                Assert.assertTrue(
                        "the enabled refresh worker must reclaim the fragmented sparse segments",
                        sparseSegmentCount(instance) <= 1
                );
            }
        });
    }

    private MemoryTracker acquireRefreshTracker(long queryId) {
        return engine.getMemoryTrackerProvider().acquire(
                AllowAllSecurityContext.INSTANCE,
                queryId,
                MemoryTrackerWorkload.LIVE_VIEW_REFRESH
        );
    }

    private LiveViewCheckpointCompaction.Result compact(
            LiveViewInstance instance,
            LiveViewCheckpointTimelineStoreWriter writer,
            MemoryTracker tracker
    ) {
        try (Path dir = checkpointsDir(instance)) {
            return LiveViewCheckpointCompaction.compact(
                    configuration, dir, writer,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    instance.getLifecycleIdentity(),
                    true,
                    tracker,
                    100,
                    1,
                    64
            );
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static boolean dataSegmentFileExists(LiveViewInstance instance, long segmentId) {
        try (Path checkpointsDir = checkpointsDir(instance); Path path = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    // The data segment ids with a final-name file on disk, ascending. A purge unlinks
    // the file but leaves the catalogue entry, so on-disk presence is the reclaim test.
    private static LongList dataSegmentIds(LiveViewInstance instance) {
        final LongList ids = new LongList();
        try (Path checkpointsDir = checkpointsDir(instance); Path dataDir = new Path()) {
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            final File dir = new File(dataDir.toString());
            final String[] names = dir.list();
            if (names != null) {
                for (String name : names) {
                    if (name.endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)
                            || !Chars.startsWith(name, LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX)) {
                        continue;
                    }
                    try {
                        ids.add(Long.parseLong(name.substring(LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX.length())));
                    } catch (NumberFormatException ignore) {
                        // A name that is not d.<number> is not a data segment we track.
                    }
                }
            }
        }
        ids.sort();
        return ids;
    }

    private static void assertLongListEquals(String message, LongList expected, LongList actual) {
        Assert.assertEquals(message + " size", expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(message + " at index " + i, expected.getQuick(i), actual.getQuick(i));
        }
    }

    private static String timestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    // Commits one (sym, ts) group of two rows - two partitions keep the ring holding
    // more chunk pages, so a repair supersedes some while others stay shared - and
    // drives one refresh turn over it.
    private void appendAndRefresh(LiveViewRefreshJob job, int second, long xa, long xb) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = timestamp(second);
        execute("INSERT INTO base (ts, sym, x) VALUES " +
                "('" + rowTs + "', 'a', " + xa + "), " +
                "('" + rowTs + "', 'b', " + xb + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Every data segment the committed generation still references must have its
    // file on disk. A catalogued-but-unlinked segment is an unreadable timeline:
    // the roots name pages nothing can open, and the fallback slot names the
    // sources compaction has just retired.
    private void assertCataloguedDataSegmentsExist(LiveViewInstance instance) {
        final LongList missing = new LongList();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointSegmentDirectoryReader segments = new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                segments.of(dir, pin.getSegmentDirectoryRootRef());
                segments.iterateAll(entry -> {
                    if (!entry.isMetadata()
                            && entry.referenceCount > 0
                            && !dataSegmentFileExists(instance, entry.segmentId)) {
                        missing.add(entry.segmentId);
                    }
                });
            }
        }
        Assert.assertEquals(
                "the committed generation references unlinked data segments " + missing,
                0,
                missing.size()
        );
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecompute(VIEW_SQL);
    }

    private void assertViewMatchesRecompute(String viewSql) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    // Builds an in-order history, then folds overlapping correction pairs back through
    // it. A repair re-versions its whole range into one capture segment whose ring pages
    // are shared across the boundaries it re-froze; a second correction one group higher
    // re-versions the upper part of that range, so the capture's pages that only those
    // upper boundaries named lose their last reference while the page the lowest boundary
    // still names stays live - which is what leaves the capture segment partially dead
    // for compaction to reclaim.
    private LiveViewInstance buildFragmentedHistory(LiveViewRefreshJob job) throws Exception {
        for (int commit = 1; commit <= SEALS; commit++) {
            appendAndRefresh(job, commit * 10, commit, 100L + commit);
        }
        driveRefreshToQuiescence(job);
        final LiveViewInstance instance = viewInstance();
        // Each base group gets two corrections ten seconds apart, both inside the 30s
        // frame so their repair ranges overlap. Bases are far enough apart that the pairs
        // do not interfere. The +3 offset keeps every correction off an in-order group.
        for (int base : new int[]{6, 16, 26}) {
            correct(job, instance, base * 10 + 3, 9000L + base);
            correct(job, instance, base * 10 + 13, 9100L + base);
        }
        return instance;
    }

    private LiveViewInstance buildHighCardinalityFragmentedHistory(LiveViewRefreshJob job) throws Exception {
        for (int commit = 1; commit <= HIGH_CARDINALITY_SEALS; commit++) {
            appendAndRefresh(job, commit * 10, commit, 100L + commit);
        }
        driveRefreshToQuiescence(job);
        final LiveViewInstance instance = viewInstance();
        for (int source = 0; source < HIGH_CARDINALITY_SOURCES; source++) {
            final int base = 6 + source * 6;
            correct(job, instance, base * 10 + 3, 20_000L + base);
            correct(job, instance, base * 10 + 23, 30_000L + base);
        }
        return instance;
    }

    // Commits one out-of-order correction and drives the refresh over it, asserting it
    // was repaired rather than appended so the fragmentation the case relies on is real.
    private void correct(LiveViewRefreshJob job, LiveViewInstance instance, int second, long value) throws Exception {
        final long repairedBefore = repairedRows(instance);
        appendAndRefresh(job, second, value, value + 1);
        driveRefreshToQuiescence(job);
        Assert.assertTrue(
                "correction at second " + second + " must be repaired rather than appended",
                repairedRows(instance) > repairedBefore
        );
    }

    private void createView() throws Exception {
        createView(VIEW_SQL);
    }

    private void createView(String viewSql) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private LongList logicalCheckpointIds(LiveViewInstance instance) {
        final LongList ids = new LongList();
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            reader.iterateAll(pin.getTimelineRootRef(), entry -> ids.add(entry.checkpointId));
        }
        return ids;
    }

    private long nextCheckpointId(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().nextCheckpointId;
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        }
        return reader;
    }

    private void purgeCycle(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            final LiveViewCheckpointLifecycle.ReconcileResult result = LiveViewCheckpointLifecycle.reconcile(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    true
            );
            Assert.assertFalse("the definition and epoch are fixed for the case", result.isEpochReplaced());
            Assert.assertFalse("this build wrote the directory it is reconciling", result.isFormatReset());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
        }
    }

    private long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    // Referenced data segments whose live bytes are a strict fraction of their file
    // length - the compaction candidates. Mirrors the driver's enumeration: the live
    // bytes of a segment are the sum of the distinct state pages any root still names in
    // it, and a segment with dead bytes has some page no surviving root reaches.
    private CompactionInputStats compactionInputStats(LiveViewInstance instance) {
        final HashMap<PhysicalPageKey, PageUse> pageUses = new HashMap<>();
        final LongObjHashMap<long[]> liveBytes = new LongObjHashMap<>();
        final LongHashSet sparseSegments = new LongHashSet();
        final CompactionInputStats stats = new CompactionInputStats();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration);
                    LiveViewCheckpointSegmentDirectoryReader segments = new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                segments.of(dir, pin.getSegmentDirectoryRootRef());
                final LiveViewCheckpointPageRef fdRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef frRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalar = new LiveViewCheckpointStatePageRef();
                final LiveViewCheckpointPageRef pmRoot = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), e -> {
                    root.of(dir, e.rootRef);
                    root.getFunctionDirectoryRef(fdRef);
                    functions.of(dir, fdRef);
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getRootRef(i, frRef);
                        functionRoot.of(dir, frRef);
                        functionRoot.getScalarStateRef(scalar);
                        if (!scalar.isNull()) {
                            addPageUse(pageUses, liveBytes, scalar);
                        }
                        functionRoot.getPartitionMapRootRef(pmRoot);
                        partitions.iterateAll(pmRoot, pe -> {
                            for (int p = 0, m = pe.getStatePageCount(); p < m; p++) {
                                final LiveViewCheckpointStatePageRef ref = pe.getStatePageRef(p);
                                addPageUse(pageUses, liveBytes, ref);
                            }
                        });
                    }
                });
                segments.iterateAll(entry -> {
                    if (entry.isMetadata() || entry.referenceCount <= 0) {
                        return;
                    }
                    final long[] live = liveBytes.get(entry.segmentId);
                    if (live != null && live[0] > 0 && live[0] < entry.fileLength) {
                        sparseSegments.add(entry.segmentId);
                        stats.sparseSegmentCount++;
                    }
                });
            }
        }
        for (Map.Entry<PhysicalPageKey, PageUse> entry : pageUses.entrySet()) {
            if (sparseSegments.contains(entry.getKey().segmentId)) {
                stats.distinctSparsePageCount++;
                stats.sparsePageReferenceCount += entry.getValue().references;
            }
        }
        return stats;
    }

    private int sparseSegmentCount(LiveViewInstance instance) {
        final LongObjHashMap<long[]> liveBytes = new LongObjHashMap<>();
        final int[] sparse = {0};
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration);
                    LiveViewCheckpointSegmentDirectoryReader segments = new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                segments.of(dir, pin.getSegmentDirectoryRootRef());
                final LiveViewCheckpointPageRef fdRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef frRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalar = new LiveViewCheckpointStatePageRef();
                final LiveViewCheckpointPageRef pmRoot = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), e -> {
                    root.of(dir, e.rootRef);
                    root.getFunctionDirectoryRef(fdRef);
                    functions.of(dir, fdRef);
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getRootRef(i, frRef);
                        functionRoot.of(dir, frRef);
                        functionRoot.getScalarStateRef(scalar);
                        if (!scalar.isNull()) {
                            addLiveBytes(liveBytes, scalar.getSegmentId(), scalar.getStoredLength());
                        }
                        functionRoot.getPartitionMapRootRef(pmRoot);
                        partitions.iterateAll(pmRoot, pe -> {
                            for (int p = 0, m = pe.getStatePageCount(); p < m; p++) {
                                addLiveBytes(liveBytes, pe.getStatePageRef(p).getSegmentId(), pe.getStatePageRef(p).getStoredLength());
                            }
                        });
                    }
                });
                segments.iterateAll(entry -> {
                    if (entry.referenceCount <= 0) {
                        return;
                    }
                    final long[] live = liveBytes.get(entry.segmentId);
                    if (live != null && live[0] > 0 && live[0] < entry.fileLength) {
                        sparse[0]++;
                    }
                });
            }
        }
        return sparse[0];
    }

    private static void addPageUse(
            Map<PhysicalPageKey, PageUse> pageUses,
            LongObjHashMap<long[]> liveBytes,
            LiveViewCheckpointStatePageRef ref
    ) {
        final PhysicalPageKey key = new PhysicalPageKey(ref.getSegmentId(), ref.getOffset(), ref.getStoredLength());
        PageUse use = pageUses.get(key);
        if (use == null) {
            use = new PageUse();
            pageUses.put(key, use);
            addLiveBytes(liveBytes, ref.getSegmentId(), ref.getStoredLength());
        }
        use.references++;
    }

    private static final class CompactionInputStats {
        private int distinctSparsePageCount;
        private int sparsePageReferenceCount;
        private int sparseSegmentCount;
    }

    private static void assertVisitorShellsClearAndStable(
            LiveViewCheckpointTimelineStoreWriter writer,
            int[] compactionVisitorIdentities,
            int[] rootVisitorIdentities
    ) {
        Assert.assertTrue("compaction visitor bindings must be clear", writer.isCompactionVisitorShellStateClearForTest());
        Assert.assertTrue("root-builder visitor bindings must be clear", writer.isRootBuilderVisitorShellStateClearForTest());
        Assert.assertNotEquals(
                "nested compaction timeline/partition traversals require distinct shells",
                compactionVisitorIdentities[0],
                compactionVisitorIdentities[1]
        );
        Assert.assertNotEquals(
                "nested root timeline/partition traversals require distinct shells",
                rootVisitorIdentities[0],
                rootVisitorIdentities[1]
        );
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(compactionVisitorIdentities[i], writer.getCompactionVisitorShellIdentityForTest(i));
            Assert.assertEquals(rootVisitorIdentities[i], writer.getRootBuilderVisitorShellIdentityForTest(i));
        }
    }

    private static int[] visitorIdentities(LiveViewCheckpointTimelineStoreWriter writer, boolean compaction) {
        final int[] identities = new int[3];
        for (int i = 0; i < identities.length; i++) {
            identities[i] = compaction
                    ? writer.getCompactionVisitorShellIdentityForTest(i)
                    : writer.getRootBuilderVisitorShellIdentityForTest(i);
        }
        return identities;
    }

    private static final class PageUse {
        private int references;
    }

    private record PhysicalPageKey(long segmentId, long offset, int storedLength) {
    }

    private static void addLiveBytes(LongObjHashMap<long[]> map, long segmentId, int bytes) {
        long[] acc = map.get(segmentId);
        if (acc == null) {
            acc = new long[]{0};
            map.put(segmentId, acc);
        }
        acc[0] += bytes;
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
