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
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Phase 5 step 5 of LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md: the
 * localized out-of-order repair publishes as a timeline range splice rather than
 * as a whole-timeline retire.
 * <p>
 * Every case here drives a real cadence history through the refresh job - one
 * logical root per commit - and then repairs a chosen {@code [C, H)} window
 * through {@link LiveViewCheckpointTimelineStoreWriter#beginRepair} /
 * {@link LiveViewCheckpointTimelineStoreWriter#publishRepair}, which is what a
 * replay does once it has re-materialised that window. The properties under test
 * are the ones design section 20 argues and section 24 accepts on: the prefix and
 * the converged suffix keep their payload roots by page identity, only the
 * repaired interval receives new root versions, and the suffix's cumulative
 * recovery position is corrected through the persistent delta index without the
 * splice walking it.
 */
public class LiveViewCheckpointTimelineRepairTest extends AbstractLiveViewTest {

    // Fields of one snapshotted logical entry: key, root page reference, effective position.
    private static final int ENTRY_CHECKPOINT_ID = 1;
    private static final int ENTRY_EFFECTIVE_POSITION = 5;
    private static final int ENTRY_MAX_TIMESTAMP = 0;
    private static final int ENTRY_ROOT_LENGTH = 4;
    private static final int ENTRY_ROOT_OFFSET = 3;
    private static final int ENTRY_ROOT_SEGMENT = 2;
    private static final int ENTRY_SIZE = 6;
    // The history every case builds: one commit (and so one logical root) per 10 seconds.
    private static final int HISTORY_COMMITS = 6;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testCadenceAppendComposesWithARepairedSuffix() throws Exception {
        // The delta index is generation state, not repair state: a later cadence seal
        // stores its position net of the correction, so it reads back as the raw
        // runtime count while the corrected suffix keeps its shift. The repair's
        // positions here are synthetic - no replay produced them - so the resulting
        // sequence is deliberately not the monotone one a real materialization has;
        // what it pins is the arithmetic, which a real repair then feeds real numbers.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                repair(instance, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6}, 2);
                appendAndRefresh(job, 70, 7);

                Assert.assertEquals(HISTORY_COMMITS + 1, entryCount(instance));
                final LongList after = snapshotTimeline(instance);
                final long[] expected = {1, 2, 4, 6, 7, 8, HISTORY_COMMITS + 1};
                for (int i = 0; i < expected.length; i++) {
                    Assert.assertEquals(
                            "effective position at index " + i,
                            expected[i],
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
            }
        });
    }

    @Test
    public void testCrashBeforeSuperblockPublishKeepsThePriorGeneration() throws Exception {
        assertCrashBeforeSuperblockPublish(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH);
        assertCrashBeforeSuperblockPublish(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
    }

    @Test
    public void testEmptyRepairIntervalCorrectsTheSuffixAndAdvancesTheGeneration() throws Exception {
        // A repair whose [C, H) holds no logical boundary still owes the suffix its
        // position correction, and still owes the whole reused timeline the
        // generation watermark that declares it valid against the pinned snapshot.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                try (
                        LiveViewCheckpointTimelineStoreWriter writer =
                                new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    final LiveViewCheckpointTimelineStoreWriter.RepairResult result;
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        Assert.assertEquals(0, capture.size());
                        result = publish(writer, capture, instance, ts(timestamp(31)), 3);
                    }
                    Assert.assertEquals(generationBefore + 1, result.getGeneration());
                    Assert.assertEquals(0, result.getRootsVersioned());
                    Assert.assertEquals(0, result.getDataBytesAdded());
                    // The first key at or above H is the 40s root.
                    Assert.assertEquals(ts(timestamp(40)), result.getSuffixBreakpointTimestamp());
                }

                final LongList after = snapshotTimeline(instance);
                Assert.assertEquals(generationBefore + 1, generation(instance));
                Assert.assertEquals(before.size(), after.size());
                for (int i = 0; i < HISTORY_COMMITS; i++) {
                    assertSameRoot(before, after, i);
                    final long shift = (i + 1) * 10 >= 40 ? 3 : 0;
                    Assert.assertEquals(
                            "effective position at index " + i,
                            before.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION) + shift,
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
            }
        });
    }

    @Test
    public void testLocalizedO3ReplaySplicesTheTimelineInPlace() throws Exception {
        // The replay side of the splice, driven by a real out-of-order commit rather
        // than a synthetic capture: the refresh job plans the repair, segments its
        // replay at the boundaries in [C, H), and publishes the splice itself. Every
        // number asserted below is one the replay derived.
        //
        // The ring is capped at two anchors so the change - at 25s, below both
        // survivors - finds none and takes the boundary rebuild, which is the whole
        // pathology: the ring lost the anchors while the timeline kept every root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, 2);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                // W is 30s, so the repair localizes to R = 25s and converges one
                // microsecond past 25s + 30s = 55s, which the 60s runtime frontier
                // clears. That puts the 30s, 40s and 50s roots inside [C, H), leaves
                // 10s and 20s as the reused prefix, and 60s as the converged suffix.
                // L saturates at the view boundary here (R - W is below it), so this
                // history is short enough that only H bounds the scan; the two-sided
                // bound is measured in LiveViewCheckpointRingBoundaryFixtureTest.
                appendAndRefresh(job, 25, 100);

                Assert.assertEquals("no anchor survives below the change", 0, instance.getO3ResumeReplayRows());
                Assert.assertEquals("the rebuild must stop at H", 6, instance.getO3ReplayScanRows());
                Assert.assertEquals("the rebuild must re-emit [R, H) only", 4, instance.getO3BoundaryReplayRows());

                final LongList after = snapshotTimeline(instance);
                Assert.assertEquals(
                        "the splice must neither drop a logical entry nor add one - the runtime"
                                + " stands where the repair found it, so there is no new boundary",
                        before.size(),
                        after.size()
                );
                Assert.assertEquals(
                        "the splice is this repair's one and only timeline publication",
                        generationBefore + 1,
                        generation(instance)
                );

                // Prefix: nothing at or below 20s changed, so the payload roots and
                // their positions are the same objects the cadence wrote.
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                Assert.assertEquals(1, after.getQuick(ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(2, after.getQuick(ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));

                // Repaired interval: same logical keys, new root versions, and
                // positions the replay derived as "durable rows below R plus rows
                // emitted at or below this boundary" - 2 + 2, 2 + 3, 2 + 4.
                for (int i = 2; i <= 4; i++) {
                    assertNewRoot(before, after, i);
                    Assert.assertEquals(
                            "repaired position at index " + i,
                            i + 2,
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }

                // Converged suffix: the payload root is reused by page identity and
                // only its cumulative position moves, by the one row the replacement
                // added, through the persistent range-add.
                assertSameRoot(before, after, 5);
                Assert.assertEquals(6 + 1, after.getQuick(5 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
            }

            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t103.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t106.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t110.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t114.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n");
        });
    }

    @Test
    public void testRangeSpliceReVersionsOnlyTheRepairedInterval() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                Assert.assertEquals(HISTORY_COMMITS * ENTRY_SIZE, before.size());
                // One row lands per commit, so the cadence positions are 1..6.
                for (int i = 0; i < HISTORY_COMMITS; i++) {
                    Assert.assertEquals(i + 1, before.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                }
                final long generationBefore = generation(instance);

                final LiveViewCheckpointTimelineStoreWriter.RepairResult result =
                        repair(instance, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6}, 2);
                Assert.assertEquals(generationBefore + 1, result.getGeneration());
                Assert.assertEquals(2, result.getRootsVersioned());
                Assert.assertEquals(ts(timestamp(50)), result.getSuffixBreakpointTimestamp());
                Assert.assertTrue(result.getDataBytesAdded() > 0);

                final LongList after = snapshotTimeline(instance);
                Assert.assertEquals("the splice must not add or drop a logical entry", before.size(), after.size());
                Assert.assertEquals(generationBefore + 1, generation(instance));

                // Prefix (10s, 20s): payload roots reused by page identity, positions untouched.
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                Assert.assertEquals(1, after.getQuick(0 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(2, after.getQuick(1 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));

                // Repaired interval (30s, 40s): same keys, new root versions, replay positions.
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                Assert.assertEquals(4, after.getQuick(2 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(6, after.getQuick(3 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));

                // Converged suffix (50s, 60s): payload roots reused, positions corrected by
                // the range-add alone - the splice never rewrote their leaves.
                assertSameRoot(before, after, 4);
                assertSameRoot(before, after, 5);
                Assert.assertEquals(5 + 2, after.getQuick(4 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(6 + 2, after.getQuick(5 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
            }
        });
    }

    @Test
    public void testRangeSpliceRestoresTheCapturedStateAndLeavesThePrefixIntact() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                // The 10s root's state, taken back when the history was one commit long.
                final byte[][] prefixState = restoreRoot(instance, functions, ts(timestamp(10)), 0);
                // What the repair captures: the runtime as it stands at the end of the
                // history, deliberately different from the state either repaired root held.
                restoreRoot(instance, functions, ts(timestamp(60)), 5);
                final byte[][] capturedState = snapshotRuntime(functions);
                Assert.assertFalse(
                        "the two states must differ, or the restore assertions below prove nothing",
                        java.util.Arrays.deepEquals(prefixState, capturedState)
                );

                repair(instance, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6}, 2);

                assertRuntimeState(capturedState, restoreRoot(instance, functions, ts(timestamp(30)), 2));
                assertRuntimeState(capturedState, restoreRoot(instance, functions, ts(timestamp(40)), 3));
                assertRuntimeState(prefixState, restoreRoot(instance, functions, ts(timestamp(10)), 0));
            }
        });
    }

    @Test
    public void testRepairRefusesABackwardWatermarkAndAnOutOfRangeBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final long generationBefore = generation(instance);

                try (
                        LiveViewCheckpointTimelineStoreWriter writer =
                                new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        captureRange(instance, capture, functions, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6});
                        try {
                            // H below the boundaries the capture holds: the splice would
                            // re-version roots the suffix range-add also shifts.
                            publish(writer, capture, instance, ts(timestamp(30)), 2);
                            Assert.fail("expected an out-of-range repair boundary rejection");
                        } catch (CairoException e) {
                            TestUtils.assertContains(
                                    e.getFlyweightMessage(),
                                    "repair boundary is at or above the convergence bound"
                            );
                        }
                    }
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        try {
                            writer.publishRepair(
                                    capture,
                                    instance.getLiveViewToken().getTableId(),
                                    normalizedBaseSeqTxn(instance) - 1,
                                    coveredLvSeqTxn(instance),
                                    0,
                                    true,
                                    ts(timestamp(50)),
                                    0
                            );
                            Assert.fail("expected a backward generation watermark rejection");
                        } catch (CairoException e) {
                            TestUtils.assertContains(
                                    e.getFlyweightMessage(),
                                    "generation watermarks must not move backwards"
                            );
                        }
                    }
                }
                Assert.assertEquals("a refused repair must not publish", generationBefore, generation(instance));
            }
        });
    }

    @Test
    public void testRepairRefusesAGenerationPublishedUnderTheCapture() throws Exception {
        // The capture holds root references resolved against one generation. A cadence
        // seal landing in between supersedes the tree they belong to, so splicing them
        // would graft stale references onto a newer timeline.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);

                try (
                        LiveViewCheckpointTimelineStoreWriter writer =
                                new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        captureRange(instance, capture, functions, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6});
                        appendAndRefresh(job, 70, 7);
                        try {
                            publish(writer, capture, instance, ts(timestamp(50)), 2);
                            Assert.fail("expected a moved-generation rejection");
                        } catch (CairoException e) {
                            TestUtils.assertContains(
                                    e.getFlyweightMessage(),
                                    "timeline moved under the repair capture"
                            );
                        }
                    }
                }
                Assert.assertEquals(HISTORY_COMMITS + 1, entryCount(instance));
            }
        });
    }

    @Test
    public void testRangeSpliceRetiresSupersededDataSegmentsAndSharesTheCaptureSegment() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList supersededSegmentIds = new LongList();
                supersededSegmentIds.add(rootDataSegmentId(instance, ts(timestamp(30)), 2));
                supersededSegmentIds.add(rootDataSegmentId(instance, ts(timestamp(40)), 3));
                final long reusedSegmentId = rootDataSegmentId(instance, ts(timestamp(60)), 5);

                final LiveViewCheckpointTimelineStoreWriter.RepairResult result =
                        repair(instance, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6}, 2);

                final long captureSegmentId = rootDataSegmentId(instance, ts(timestamp(30)), 2);
                Assert.assertEquals(
                        "both repaired roots must share the one segment the capture wrote",
                        captureSegmentId,
                        rootDataSegmentId(instance, ts(timestamp(40)), 3)
                );
                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointSegmentDirectory directory =
                                new LiveViewCheckpointSegmentDirectory(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                    Assert.assertEquals(2, directory.getReferenceCount(captureSegmentId));
                    Assert.assertEquals(
                            LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE,
                            directory.getRetireGeneration(captureSegmentId)
                    );
                    for (int i = 0, n = supersededSegmentIds.size(); i < n; i++) {
                        final long segmentId = supersededSegmentIds.getQuick(i);
                        Assert.assertEquals(
                                "superseded root data must lose its last reference",
                                0,
                                directory.getReferenceCount(segmentId)
                        );
                        Assert.assertEquals(
                                "and retire at the publishing generation",
                                result.getGeneration(),
                                directory.getRetireGeneration(segmentId)
                        );
                    }
                    Assert.assertEquals(
                            "a reused suffix root must keep its data segment",
                            1,
                            directory.getReferenceCount(reusedSegmentId)
                    );
                }
            }
        });
    }

    @Test
    public void testRestartAfterALocalizedO3ReplayRestoresTheCorrectedSuffix() throws Exception {
        // The repaired positions are only as good as a restart's willingness to
        // believe them: recovery selects the newest root at or below the durable
        // frontier and refuses it unless its effective lvRowPosition plus the rows it
        // replays equals the live-view table's own count. The root recovery lands on
        // here is a converged suffix root, whose position moved by the range-add
        // alone - so this is that arithmetic checked against the materialization.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, 2);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHistory(job);
                appendAndRefresh(job, 25, 100);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The first tick restores from the spliced timeline; the row then
                // appends incrementally against the state that restore produced.
                appendAndRefresh(job, 70, 7);
                Assert.assertTrue(reloaded.isCheckpointRestoreSucceeded());
                Assert.assertEquals(
                        "a root whose position the splice corrected must not send restart"
                                + " back to the START FROM boundary",
                        0,
                        reloaded.getO3BoundaryReplayRows()
                );
                Assert.assertEquals(8, reloaded.getLvRowsTotal());
            }

            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t103.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t106.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t110.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t114.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n");
        });
    }

    @Test
    public void testSuffixDeltaAccumulatesAcrossRepairs() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);

                repair(instance, ts(timestamp(20)), ts(timestamp(40)), new long[]{3, 5}, 2);
                repair(instance, ts(timestamp(40)), ts(timestamp(60)), new long[]{9, 11}, -1);

                final LongList after = snapshotTimeline(instance);
                // 10s untouched; 20s/30s from the first repair; 40s/50s from the second;
                // 60s carries both range-adds (+2 then -1).
                final long[] expected = {1, 3, 5, 9, 11, 6 + 2 - 1};
                for (int i = 0; i < HISTORY_COMMITS; i++) {
                    Assert.assertEquals(
                            "effective position at index " + i,
                            expected[i],
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
            }
        });
    }

    private static void assertNewRoot(LongList before, LongList after, int index) {
        final int base = index * ENTRY_SIZE;
        Assert.assertEquals(before.getQuick(base + ENTRY_MAX_TIMESTAMP), after.getQuick(base + ENTRY_MAX_TIMESTAMP));
        Assert.assertEquals(before.getQuick(base + ENTRY_CHECKPOINT_ID), after.getQuick(base + ENTRY_CHECKPOINT_ID));
        Assert.assertTrue(
                "the repaired root at index " + index + " must be a new physical version",
                before.getQuick(base + ENTRY_ROOT_SEGMENT) != after.getQuick(base + ENTRY_ROOT_SEGMENT)
                        || before.getQuick(base + ENTRY_ROOT_OFFSET) != after.getQuick(base + ENTRY_ROOT_OFFSET)
        );
    }

    private static void assertRuntimeState(byte[][] expected, byte[][] actual) {
        Assert.assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertArrayEquals("function state mismatch at index " + i, expected[i], actual[i]);
        }
    }

    private static void assertSameRoot(LongList before, LongList after, int index) {
        final int base = index * ENTRY_SIZE;
        for (int field = ENTRY_MAX_TIMESTAMP; field <= ENTRY_ROOT_LENGTH; field++) {
            Assert.assertEquals(
                    "reused root field " + field + " at index " + index,
                    before.getQuick(base + field),
                    after.getQuick(base + field)
            );
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
    }

    private static byte[] copyBytes(MemoryCARW memory) {
        final int length = (int) memory.getAppendOffset();
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = memory.getByte(i);
        }
        return bytes;
    }

    private static byte[][] snapshotRuntime(ObjList<WindowFunction> functions) {
        final ObjList<byte[]> states = new ObjList<>();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                LiveViewFunctionSnapshot.write(sink, function);
                states.add(copyBytes(sink));
            }
        }
        final byte[][] result = new byte[states.size()][];
        for (int i = 0, n = states.size(); i < n; i++) {
            result[i] = states.getQuick(i);
        }
        return result;
    }

    // A 2026-01-01 microsecond literal at the given second-of-day offset. The whole
    // history sits inside one calendar day, so the base's DAY partitioning never
    // enters the picture.
    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
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

    private void appendAndRefresh(LiveViewRefreshJob job, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + 200_000);
        execute("INSERT INTO base VALUES ('" + timestamp(second) + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void assertCrashBeforeSuperblockPublish(int failureStage) throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                try (
                        LiveViewCheckpointTimelineStoreWriter writer =
                                new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    writer.setTestFailureStage(failureStage);
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        captureRange(instance, capture, functions, ts(timestamp(30)), ts(timestamp(50)), new long[]{4, 6});
                        try {
                            publish(writer, capture, instance, ts(timestamp(50)), 2);
                            Assert.fail("expected the injected publication failure");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "test failure after");
                        }
                    }
                }

                // Immutable-file publication is not the commit point: the prior
                // generation still resolves and every root is the one it was.
                Assert.assertEquals(generationBefore, generation(instance));
                final LongList after = snapshotTimeline(instance);
                Assert.assertEquals(before.size(), after.size());
                for (int i = 0; i < HISTORY_COMMITS; i++) {
                    assertSameRoot(before, after, i);
                    Assert.assertEquals(
                            before.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION),
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
            }
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testUnlocalizedO3ReplayStillRetiresTheTimeline() throws Exception {
        // The scope boundary of the splice, stated as a test. A ROWS frame carries no
        // finite RANGE dependency, so the plan localizes nothing and the rebuild
        // replaces through positive infinity - there is no converged suffix to keep
        // and the runtime it promotes is the replay's own. The timeline is retired
        // whole and the post-replay seal opens a fresh history with one root, exactly
        // as it did before the splice existed. Phase 6 bounds this shape.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, sum(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                            ") s FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                Assert.assertTrue(generation(instance) > 1);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals("the whole history is re-emitted", 7, instance.getO3BoundaryReplayRows());
                Assert.assertEquals(
                        "a retired timeline starts over from the post-replay seal",
                        1,
                        entryCount(instance)
                );
                Assert.assertEquals(1, generation(instance));
            }
        });
    }

    private LiveViewInstance buildHistory(LiveViewRefreshJob job) throws Exception {
        for (int commit = 1; commit <= HISTORY_COMMITS; commit++) {
            appendAndRefresh(job, commit * 10, commit);
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertEquals(HISTORY_COMMITS, entryCount(instance));
        return instance;
    }

    private void captureRange(
            LiveViewInstance instance,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            ObjList<WindowFunction> functions,
            long lowTimestampInclusive,
            long highTimestampExclusive,
            long[] effectivePositions
    ) {
        final ObjList<LiveViewCheckpointTimelineEntry> entries = new ObjList<>();
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            reader.range(
                    pin.getTimelineRootRef(),
                    lowTimestampInclusive,
                    highTimestampExclusive,
                    entry -> entries.add(new LiveViewCheckpointTimelineEntry().copyFrom(entry))
            );
        }
        Assert.assertEquals(effectivePositions.length, entries.size());
        for (int i = 0, n = entries.size(); i < n; i++) {
            capture.capture(entries.getQuick(i), functions, instance.getAnchorWindow(), effectivePositions[i]);
        }
    }

    private long coveredLvSeqTxn(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().coveredLvSeqTxn;
        }
    }

    private void createView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(
                "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                        "SELECT ts, sym, sum(x) OVER (" +
                        "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
                        ") s FROM base"
        );
    }

    private long entryCount(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            return reader.size(pin.getTimelineRootRef());
        }
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private long normalizedBaseSeqTxn(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().normalizedBaseSeqTxn;
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            reader.of(checkpointsDir);
        }
        return reader;
    }

    private LiveViewCheckpointTimelineStoreWriter.RepairResult publish(
            LiveViewCheckpointTimelineStoreWriter writer,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            LiveViewInstance instance,
            long highTsExclusive,
            long suffixRowDelta
    ) {
        return writer.publishRepair(
                capture,
                instance.getLiveViewToken().getTableId(),
                normalizedBaseSeqTxn(instance),
                coveredLvSeqTxn(instance),
                0,
                true,
                highTsExclusive,
                suffixRowDelta
        );
    }

    private LiveViewCheckpointTimelineStoreWriter.RepairResult repair(
            LiveViewInstance instance,
            long lowTimestampInclusive,
            long highTsExclusive,
            long[] effectivePositions,
            long suffixRowDelta
    ) {
        try (
                LiveViewCheckpointTimelineStoreWriter writer =
                        new LiveViewCheckpointTimelineStoreWriter(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture = writer.beginRepair(checkpointsDir)) {
                captureRange(
                        instance,
                        capture,
                        unwrapWindowFunctions(instance),
                        lowTimestampInclusive,
                        highTsExclusive,
                        effectivePositions
                );
                return publish(writer, capture, instance, highTsExclusive, suffixRowDelta);
            }
        }
    }

    private byte[][] restoreRoot(
            LiveViewInstance instance,
            ObjList<WindowFunction> functions,
            long maxTimestamp,
            long checkpointId
    ) {
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir);
            reader.restore(
                    maxTimestamp,
                    checkpointId,
                    instance.getLiveViewToken().getTableId(),
                    functions,
                    instance.getAnchorWindow()
            );
        }
        return snapshotRuntime(functions);
    }

    private long rootDataSegmentId(LiveViewInstance instance, long maxTimestamp, long checkpointId) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue(timeline.findExact(pin.getTimelineRootRef(), maxTimestamp, checkpointId, entry));
            root.of(checkpointsDir, entry.rootRef);
            Assert.assertEquals(1, root.getSegmentIdCount());
            return root.getSegmentId(0);
        }
    }

    /**
     * Flattens every logical entry into {@code (maxTimestamp, checkpointId, root
     * segment/offset/length, effective position)}. Root page identity is what
     * distinguishes a reused payload root from a re-versioned one.
     */
    private LongList snapshotTimeline(LiveViewInstance instance) {
        final LongList rows = new LongList();
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance);
                LiveViewCheckpointRowPositionDeltaReader deltaReader =
                        new LiveViewCheckpointRowPositionDeltaReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            deltaReader.of(checkpointsDir);
            reader.iterateAll(pin.getTimelineRootRef(), entry -> {
                rows.add(entry.maxTimestamp);
                rows.add(entry.checkpointId);
                rows.add(entry.rootRef.getSegmentId());
                rows.add(entry.rootRef.getOffset());
                rows.add(entry.rootRef.getLength());
                rows.add(deltaReader.effectivePosition(pin.getRowPositionDeltaRootRef(), entry));
            });
        }
        return rows;
    }
}
