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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairState;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The localized out-of-order repair publishes as a timeline range splice rather
 * than as a whole-timeline retire.
 * <p>
 * Every case here drives a real cadence history through the refresh job - one
 * logical root per commit - and then repairs a chosen {@code [C, H)} window
 * through {@link LiveViewCheckpointTimelineStoreWriter#beginRepair} /
 * {@link LiveViewCheckpointTimelineStoreWriter#publishRepair}, which is what a
 * replay does once it has re-materialised that window. The properties under
 * test are the ones the correctness argument rests on: the prefix and the
 * converged suffix keep their payload roots by page identity, only the repaired
 * interval receives new root versions, and the suffix's cumulative recovery
 * position is corrected through the persistent delta index without the splice
 * walking it.
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
    private static final int HISTORY_COMMITS = 12;

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
                appendAndRefresh(job, 130, 13);

                Assert.assertEquals(HISTORY_COMMITS + 1, entryCount(instance));
                final LongList after = snapshotTimeline(instance);
                final long[] expected = {1, 2, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, HISTORY_COMMITS + 1};
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
    public void testACaptureStepsOverAFinalNameSegmentLeftByACrash() throws Exception {
        // A publication that died between renaming its data segment and committing
        // the superblock leaves a final-name file the catalogue knows nothing
        // about: nextSegmentId still points straight at it. Segment ids are never
        // reused - not even for a file no generation references - so the next
        // capture steps over it and leaves the orphan exactly as it found it, for
        // reconciliation to dispose of. Writing through it instead would give one
        // id two different published meanings, which is the assumption every
        // reference count and every retired-generation check rests on.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long orphanSegmentId = nextSegmentId(instance);
                try (Path checkpointsDir = checkpointsDir(instance); Path path = new Path()) {
                    LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, orphanSegmentId);
                    Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
                }

                try (
                        LiveViewCheckpointTimelineStoreWriter writer =
                                new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(checkpointsDir)) {
                        Assert.assertTrue(
                                "the capture must allocate above the orphan, not onto it",
                                capture.getDataSegmentId() > orphanSegmentId
                        );
                        captureRange(
                                instance,
                                capture,
                                unwrapWindowFunctions(instance),
                                ts(timestamp(30)),
                                ts(timestamp(50)),
                                new long[]{4, 6}
                        );
                        publish(writer, capture, instance, ts(timestamp(50)), 2);
                    }
                }

                // Still the empty file it was: the splice wrote its own segment
                // beside the orphan rather than through it.
                try (Path checkpointsDir = checkpointsDir(instance); Path path = new Path()) {
                    LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, orphanSegmentId);
                    Assert.assertEquals(0, configuration.getFilesFacade().length(path.$()));
                }
                Assert.assertEquals(HISTORY_COMMITS, entryCount(instance));
            }
        });
    }

    @Test
    public void testCrashedRepairCandidateIsDiscardedOnRestart() throws Exception {
        // A repair that died with its candidate staged leaves a descriptor and the
        // temporary segment it names. Nothing in the timeline references either, and
        // the snapshot the candidate was planned against cannot be reopened, so
        // startup discards both and the next out-of-order row is repaired from a
        // fresh plan against the untouched generation.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                plantCrashedRepair(buildHistory(job), 99, 500);
            }
            Assert.assertTrue(repairDescriptorExists(99));
            Assert.assertTrue(tmpDataSegmentExists(500));

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertFalse(repairDescriptorExists(99));
            Assert.assertFalse(tmpDataSegmentExists(500));

            final long generationBefore = generation(reloaded);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 25, 100);
            }
            Assert.assertEquals(
                    "the discarded candidate must cost the timeline nothing",
                    HISTORY_COMMITS,
                    entryCount(reloaded)
            );
            Assert.assertEquals(generationBefore + 1, generation(reloaded));
            Assert.assertEquals("a published repair owes no descriptor", 0, repairDescriptorCount());

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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
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
                // bound is measured in LiveViewCheckpointBoundaryFixtureTest.
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
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testLaggingRangeFrameSplicesOnTheLookBehindAlone() throws Exception {
        // The same splice as testLocalizedO3ReplaySplicesTheTimelineInPlace, over a frame
        // ending 10s below its own row. Both bounds are functions of W and nothing else, so
        // every count below is the one that test measures on the same W: the scan reads the
        // same 6 rows and the rebuild re-emits the same 4. The lag only removes rows from
        // the affected set - output at 30s is re-emitted with the value it already had,
        // because its frame [0s, 20s] never reached the change at 25s.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, sum(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND '10' SECOND PRECEDING" +
                            ") s FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals("no anchor survives below the change", 0, instance.getO3ResumeReplayRows());
                Assert.assertEquals("the rebuild must stop at H", 6, instance.getO3ReplayScanRows());
                Assert.assertEquals("the rebuild must re-emit [R, H) only", 4, instance.getO3BoundaryReplayRows());
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                final LongList after = snapshotTimeline(instance);
                // Prefix (10s, 20s): below the correction floor, so nothing about them moved.
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                // Repaired interval (30s, 40s, 50s), converged suffix (60s) reused.
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                assertNewRoot(before, after, 4);
                assertSameRoot(before, after, 5);
            }
            assertNoRefreshFaults("lv");

            // The oracle: 30s reads [0s, 20s] and is unmoved, 40s reads [10s, 30s] and 50s
            // reads [20s, 40s], so those two are the only outputs the change reaches - the
            // frame at 60s starts at 30s, above it. The earliest row's frame is empty.
            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\tnull\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t106.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t109.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t12.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t15.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t21.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t24.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t27.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t30.0\n");
        });
    }

    @Test
    public void testLaggingRangeLastValueSplicesOnTheFrameWidth() throws Exception {
        // last_value over a lagging RANGE frame, which took the whole-history rebuild until
        // the RANGE-frame implementations declared frame-local state. It accumulates nothing -
        // it emits the newest base row at or below t - 5s - but its ring is still evicted at
        // the frame's own start, so the state is a function of the rows in [t - 30s, t] and
        // the width bounds the repair on both sides exactly as it does for the sum over the
        // same frame above: the scan reads the same 6 rows and the rebuild re-emits the same
        // 4. The lag is 5s rather than 10s so the change at 25s actually reaches an output -
        // a row inserted off the 10s grid is the newest row below t - 10s for no t on it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, last_value(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND '5' SECOND PRECEDING" +
                            ") l FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals("no anchor survives below the change", 0, instance.getO3ResumeReplayRows());
                Assert.assertEquals("the rebuild must stop at H", 6, instance.getO3ReplayScanRows());
                Assert.assertEquals("the rebuild must re-emit [R, H) only", 4, instance.getO3BoundaryReplayRows());
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                // The prefix and the converged suffix keep their payload roots by page
                // identity, which is what separates the localized repair from the
                // whole-history rebuild this shape took before: that one versions every root.
                final LongList after = snapshotTimeline(instance);
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                assertNewRoot(before, after, 4);
                assertSameRoot(before, after, 5);
            }
            assertNoRefreshFaults("lv");

            // The oracle: every row emits the newest row in [t - 30s, t - 5s]. The insert at
            // 25s is that row for 30s alone - 40s reads 30s both before and after - so it is
            // the only output the change moves, and the repair's interval covers it.
            assertQuery("select ts, sym, l from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\tl\n" +
                            "2026-01-01T00:00:10.000000Z\ta\tnull\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t1\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t2\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t100\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t3\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t4\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t5\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t6\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t7\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t8\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t9\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t10\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t11\n");
        });
    }

    @Test
    public void testCostPrefersTheSpliceOverASurvivingAnchor() throws Exception {
        // Every logical root retained, which is the shape the versioned timeline makes
        // ordinary: a sealed predecessor almost always sits below a correction, here at
        // 20s under a change at 25s. Resuming from it because it exists replays every
        // row above it - 11 of the 13 in the base - while the dependency interval is two
        // frame widths wide however old the correction and however long the view has
        // been running. The plan prices both and takes the splice.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job, 12);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals(
                        "an anchor at 20s qualifies for the resume and must lose on price",
                        0,
                        instance.getO3ResumeReplayRows()
                );
                Assert.assertEquals("the rebuild must stop at H", 6, instance.getO3ReplayScanRows());
                Assert.assertEquals("the rebuild must re-emit [R, H) only", 4, instance.getO3BoundaryReplayRows());
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        12,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                final LongList after = snapshotTimeline(instance);
                // Prefix (10s, 20s) reused, including the 20s root the resume would have
                // restored from and then superseded.
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                // Repaired interval (30s, 40s, 50s).
                for (int i = 2; i <= 4; i++) {
                    assertNewRoot(before, after, i);
                }
                // Converged suffix (60s through 120s): seven roots a resume would have
                // recomputed and re-emitted, reused here by page identity with only
                // their cumulative positions moved by the one inserted row.
                for (int i = 5; i < 12; i++) {
                    assertSameRoot(before, after, i);
                    Assert.assertEquals(
                            "suffix position at index " + i,
                            i + 2,
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testCostKeepsTheAnchorForAChangeNearTheHead() throws Exception {
        // The other side of the same comparison, and the reason it is a comparison. A
        // correction at 115s leaves the anchor at 110s a two-row tail, while localizing
        // would warm the frame up from 85s and - the frame reaching past the runtime
        // frontier - read to the end of the base anyway. The resume is cheaper and the
        // plan takes it, with the same anchors retained and the same view as above.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job, 12);

                appendAndRefresh(job, 115, 100);

                Assert.assertEquals(
                        "the resume re-evaluates 115s and 120s and nothing else",
                        2,
                        instance.getO3ResumeReplayRows()
                );
                Assert.assertEquals(0, instance.getO3BoundaryReplayRows());
                Assert.assertEquals(2, instance.getO3ReplayScanRows());
            }

            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t6.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t10.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t14.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:01:55.000000Z\ta\t130.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t142.0\n");
        });
    }

    @Test
    public void testLocalizedRepairOwnsItsCandidateThroughADescriptor() throws Exception {
        // The same localized repair as above, watched through the filesystem. The
        // temporary data segment the capture writes is named by no metadata until the
        // splice commits the superblock, so the descriptor the repair publishes into
        // repair/ is the only thing that could tell a later startup the segment exists
        // and whose it is. Once the splice publishes, the segment is reachable from a
        // generation and the descriptor retires with the ownership it recorded.
        final ObjList<String> descriptorWrites = new ObjList<>();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.containsAscii(to, LiveViewCheckpointLayout.REPAIR_DIR_NAME)) {
                    descriptorWrites.add(Utf8s.stringFromUtf8Bytes(to));
                }
                return super.rename(from, to);
            }
        };
        assertMemoryLeak(ff, () -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                Assert.assertEquals("a cadence seal stages no repair candidate", 0, descriptorWrites.size());

                appendAndRefresh(job, 25, 100);

                // One write opens the descriptor, and the replay and the publication
                // stamp their progress into the same record.
                Assert.assertTrue(
                        "the repair must record its ownership before it stages anything",
                        descriptorWrites.size() > 1
                );
                final String descriptor = descriptorWrites.getQuick(0);
                for (int i = 1, n = descriptorWrites.size(); i < n; i++) {
                    Assert.assertEquals(
                            "every stamp belongs to the one repair in flight",
                            descriptor,
                            descriptorWrites.getQuick(i)
                    );
                }
                Assert.assertEquals(
                        "the descriptor is named after the snapshot the repair pinned",
                        descriptorPath(instance.getAppliedWatermark()),
                        descriptor
                );
                Assert.assertEquals("a published repair owes no descriptor", 0, repairDescriptorCount());
            }
        });
    }

    @Test
    public void testLocalizedRepairYieldsAndResumesAcrossRefreshTurns() throws Exception {
        // The same localized repair as testLocalizedO3ReplaySplicesTheTimelineInPlace,
        // driven one base row per turn. The replay stops on its per-turn row budget,
        // parks the pinned snapshot, the uncommitted replacement and the roots it has
        // staged, and continues on the next turn - and what it finally publishes is
        // what the single-turn run publishes: the same rows read, the same rows
        // emitted, the same splice and the same output.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);
                final long processedBefore = instance.getLastProcessedSeqTxn();

                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();

                int turns = 0;
                int parkedTurns = 0;
                while (turns < 64 && job.processNotificationsForTest()) {
                    turns++;
                    if (instance.getSuspendedRepair() == null) {
                        continue;
                    }
                    parkedTurns++;
                    // Nothing a reader or a restart can see moves while the repair is
                    // parked: the replacement is uncommitted, so the durable output is
                    // the pre-repair one; no generation names the staged roots; and the
                    // base range stays unconsumed.
                    Assert.assertEquals(HISTORY_COMMITS, durableRowCount(instance));
                    Assert.assertEquals(generationBefore, generation(instance));
                    Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
                    Assert.assertEquals(
                            "a parked repair owns its staged files through its descriptor",
                            1,
                            repairDescriptorCount()
                    );
                }
                drainWalQueue();

                Assert.assertTrue("the replay must have yielded at least once", parkedTurns > 0);
                Assert.assertNull("the repair must finish", instance.getSuspendedRepair());
                Assert.assertEquals("a published repair owes no descriptor", 0, repairDescriptorCount());

                // Identical to the single-turn run: the resume skips the rows its own
                // turn already folded, so no row is read, folded or emitted twice.
                Assert.assertEquals("the rebuild must stop at H", 6, instance.getO3ReplayScanRows());
                Assert.assertEquals("the rebuild must re-emit [R, H) only", 4, instance.getO3BoundaryReplayRows());

                final LongList after = snapshotTimeline(instance);
                Assert.assertEquals(before.size(), after.size());
                Assert.assertEquals(
                        "however many turns it took, the splice is one publication",
                        generationBefore + 1,
                        generation(instance)
                );
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                for (int i = 2; i <= 4; i++) {
                    assertNewRoot(before, after, i);
                    Assert.assertEquals(
                            "repaired position at index " + i,
                            i + 2,
                            after.getQuick(i * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION)
                    );
                }
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
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testAForeignWorkerLeavesAParkedRepairAlone() throws Exception {
        // A parked repair holds a pinned base snapshot, a live-view writer with
        // uncommitted rows and a capture that freezes through its owner's timeline
        // store writer - all of them taken out on the owning worker's thread. Another
        // worker must therefore skip the view outright rather than plan a second
        // repair over it, and the owner must not depend on the view coming back around
        // to it: it drives its own parked repairs at the top of every run.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            try (
                    LiveViewRefreshJob owner = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewRefreshJob foreign = new LiveViewRefreshJob(1, engine, 1)
            ) {
                final LiveViewInstance instance = buildHistory(owner);
                final long generationParked = generation(instance);

                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                Assert.assertTrue(owner.processNotificationsForTest());
                Assert.assertNotNull("the first turn must park the repair", instance.getSuspendedRepair());

                Assert.assertFalse("a foreign worker reports no work on a parked view", drainJob(foreign));
                Assert.assertNotNull(
                        "a foreign worker must not continue - or replan - another worker's repair",
                        instance.getSuspendedRepair()
                );
                Assert.assertEquals(generationParked, generation(instance));
                Assert.assertEquals(HISTORY_COMMITS, durableRowCount(instance));

                for (int turn = 0; turn < 64 && instance.getSuspendedRepair() != null; turn++) {
                    owner.processNotificationsForTest();
                }
                Assert.assertNull("the owner must finish what it parked", instance.getSuspendedRepair());
                drainWalQueue();
                Assert.assertEquals(generationParked + 1, generation(instance));
                Assert.assertEquals(HISTORY_COMMITS + 1, durableRowCount(instance));
            }
        });
    }

    @Test
    public void testClosingTheOwningWorkerAbandonsAParkedRepair() throws Exception {
        // Only the worker that parked a repair can continue it, so a worker on its way
        // out abandons what it holds instead of leaving a pinned reader, an
        // uncommitted replacement and a staged segment behind with nobody to claim
        // them. The view keeps its pre-repair output and its timeline, the window
        // state goes back to the one that output belongs to, and the next worker
        // simply replans the same out-of-order row at a freshly pinned snapshot.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            final LiveViewInstance instance;
            final long generationBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                instance = buildHistory(job);
                generationBefore = generation(instance);
                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertNotNull(instance.getSuspendedRepair());
                Assert.assertEquals(1, repairDescriptorCount());
            }

            Assert.assertNull("a closing worker must let go of its repair", instance.getSuspendedRepair());
            Assert.assertEquals(
                    "the abandoned candidate leaves nothing for a startup sweep",
                    0,
                    repairDescriptorCount()
            );
            Assert.assertEquals(generationBefore, generation(instance));
            Assert.assertEquals(HISTORY_COMMITS, durableRowCount(instance));

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                Assert.assertNull(instance.getSuspendedRepair());
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testABaseSchemaRecompileDiscardsAParkedRepair() throws Exception {
        // A base-metadata recompile frees the compiled factory, and with it both the
        // window functions the parked replay is standing part-way through and the ones
        // its overlay holds the pre-repair state of. The candidate cannot outlive them:
        // resuming would continue a half-finished replay through a factory rebuilt at
        // identity, and putting the overlay back would write into freed objects. Drift
        // discards the candidate - the pinned snapshot was fine, the runtime was not -
        // and because nothing durable moved, the change is still unconsumed and the
        // timeline still describes exactly the output on disk, so the replan that
        // follows is the same localized repair rather than an age-unbounded rebuild.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long generationBefore = generation(instance);

                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertNotNull("the first turn must park the repair", instance.getSuspendedRepair());
                Assert.assertEquals(1, repairDescriptorCount());

                // What recoverFromBaseMetadataDrift does before it rebuilds.
                instance.prepareForBaseSchemaRecompile();

                Assert.assertNull("a recompile must let go of the candidate", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the discarded candidate leaves nothing for a startup sweep",
                        0,
                        repairDescriptorCount()
                );
                Assert.assertFalse("a discarded candidate is not a view failure", instance.isInvalid());
                Assert.assertEquals(generationBefore, generation(instance));
                Assert.assertEquals(HISTORY_COMMITS, durableRowCount(instance));
                Assert.assertEquals(
                        "the timeline the candidate would have spliced into survives it",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );

                // The out-of-order row is still unconsumed, so the next turns replan it
                // against the surviving timeline and reach the single-turn answer.
                for (int turn = 0; turn < 64 && instance.getLastProcessedSeqTxn() < HISTORY_COMMITS + 1; turn++) {
                    job.processNotificationsForTest();
                }
                drainWalQueue();
                Assert.assertNull(instance.getSuspendedRepair());
                Assert.assertEquals(generationBefore + 1, generation(instance));
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testACancelledTurnDiscardsTheCandidateAndKeepsTheTimeline() throws Exception {
        // DROP, invalidation and engine shutdown all trip the refresh circuit breaker, so
        // a repair in flight throws out of its replay rather than finishing. That is a
        // cancellation, not a failure: the view must not invalidate, nothing durable may
        // move, and the candidate is discarded and replanned rather than routed into a
        // full-history rebuild. The unwind used to retire the whole timeline
        // unconditionally, which would have left the replan with no anchor below the
        // correction at all.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long generationBefore = generation(instance);
                final long processedBefore = instance.getLastProcessedSeqTxn();
                final long faultsBefore = instance.getRefreshFaultCount();

                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertNotNull("the first turn must park the repair", instance.getSuspendedRepair());
                Assert.assertEquals(1, repairDescriptorCount());

                instance.cancelRefresh();
                job.processNotificationsForTest();

                Assert.assertNull("a cancelled turn must let go of the candidate", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the cancelled candidate leaves nothing for a startup sweep",
                        0,
                        repairDescriptorCount()
                );
                Assert.assertFalse("a cancellation is not a refresh failure", instance.isInvalid());
                Assert.assertTrue(instance.getRefreshFaultCount() > faultsBefore);
                Assert.assertEquals(generationBefore, generation(instance));
                Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(HISTORY_COMMITS, durableRowCount(instance));
                Assert.assertEquals(
                        "a candidate that committed nothing must not take the timeline with it",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
            }

            // The pre-repair output, unchanged: the replacement never committed.
            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t6.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t10.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t14.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
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
    public void testRepairPublishesThePinnedSnapshotAsTheGenerationWatermark() throws Exception {
        // The splice reuses the prefix and the converged suffix by page reference,
        // so nothing those roots carry records that they are valid against the
        // repair's pinned snapshot E. The generation watermark is the only place
        // that fact lands, and the WAL floor is what reads it back: recovery
        // replays every base transaction above the watermark, so a repair that
        // left it behind would replay its own correction against roots that
        // already incorporate it, and would pin the base WAL there for good.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long baseBefore = normalizedBaseSeqTxn(instance);
                final long lvBefore = coveredLvSeqTxn(instance);

                final LiveViewCheckpointTimelineStoreWriter.RepairResult first = repair(
                        instance,
                        ts(timestamp(30)),
                        ts(timestamp(50)),
                        new long[]{4, 6},
                        2,
                        baseBefore + 5,
                        lvBefore + 2
                );
                Assert.assertEquals(baseBefore + 5, normalizedBaseSeqTxn(instance));
                Assert.assertEquals(lvBefore + 2, coveredLvSeqTxn(instance));
                // Publication wrote the inactive slot, so the generation this
                // repair superseded is still a recovery source and still holds the
                // floor at the base it needed.
                Assert.assertEquals(baseBefore, first.getWalPurgeFloor());

                final LiveViewCheckpointTimelineStoreWriter.RepairResult second = repair(
                        instance,
                        ts(timestamp(30)),
                        ts(timestamp(50)),
                        new long[]{4, 6},
                        0,
                        baseBefore + 9,
                        lvBefore + 3
                );
                Assert.assertEquals(baseBefore + 9, normalizedBaseSeqTxn(instance));
                // The pre-repair generation has been overwritten, so the floor
                // follows the first repair's snapshot rather than staying where
                // the cadence left it.
                Assert.assertEquals(baseBefore + 5, second.getWalPurgeFloor());
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
                        appendAndRefresh(job, 130, 13);
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
                        LiveViewCheckpointSegmentDirectoryReader directory =
                                new LiveViewCheckpointSegmentDirectoryReader(configuration);
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
                appendAndRefresh(job, 130, 13);
                Assert.assertTrue(reloaded.isCheckpointRestoreSucceeded());
                Assert.assertEquals(
                        "a root whose position the splice corrected must not send restart"
                                + " back to the START FROM boundary",
                        0,
                        reloaded.getO3BoundaryReplayRows()
                );
                Assert.assertEquals(14, reloaded.getLvRowsTotal());
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n" +
                            "2026-01-01T00:02:10.000000Z\ta\t46.0\n");
        });
    }

    @Test
    public void testStalledApplyDefersTheRepairAndRepeatsItOnceReconciled() throws Exception {
        // The reconciliation between the replacement's commit and everything that
        // describes it. With the live view's inline apply stalled, the block sits in
        // the view's own WAL but not in its table, so the repair may not publish a
        // generation whose root positions it would read off that table, may not seal a
        // head, and may not consume the base range. It defers instead, and the deferred
        // repair simply runs again once the block lands.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final long processedBefore = instance.getLastProcessedSeqTxn();
                final long rowsBefore = instance.getLvRowsTotal();
                Assert.assertEquals(HISTORY_COMMITS, rowsBefore);

                job.setSimulateRepairApplyFailureForTest(true);
                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                // Exactly one pass: it drains the notification and runs the repair, so
                // the fallback scan - which re-drives a view's outstanding apply - does
                // not get to run and the stalled state is observable.
                Assert.assertTrue(job.run());
                drainWalQueue();

                Assert.assertTrue(
                        "an unapplied replacement must be left for reconciliation",
                        instance.getPendingReplacementLvSeqTxn() != Numbers.LONG_NULL
                );
                Assert.assertEquals(
                        "no watermark may walk past output the live view table does not hold",
                        processedBefore,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals(
                        "the lifetime row count tracks the table, which has not moved",
                        rowsBefore,
                        instance.getLvRowsTotal()
                );
                Assert.assertFalse(
                        "the replacement supersedes what every root describes, so the"
                                + " timeline must not outlive it",
                        hasTimeline(instance)
                );

                // The deferred repair is repeated, not resumed. The base range was never
                // consumed, so the next tick re-detects the same out-of-order row and
                // rebuilds it - this time against a table that holds the replacement the
                // fallback scan's apply retry landed.
                job.setSimulateRepairApplyFailureForTest(false);
                driveRefreshToQuiescence(job);

                Assert.assertEquals(Numbers.LONG_NULL, instance.getPendingReplacementLvSeqTxn());
                Assert.assertTrue(instance.getLastProcessedSeqTxn() > processedBefore);
                Assert.assertEquals(
                        "the repeated replacement must replace, not duplicate",
                        HISTORY_COMMITS + 1,
                        instance.getLvRowsTotal()
                );
                Assert.assertEquals(
                        "a retired timeline starts over from the post-replay seal",
                        1,
                        entryCount(instance)
                );
                Assert.assertEquals(1, generation(instance));
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testStalledApplyKeepsThePrimaryRuntimeUsable() throws Exception {
        // The runtime half of the same ordering. A repair that converges below the
        // frontier replays through the compiled factory's own window functions, so a
        // turn that stops short of publishing still owes the runtime its state back -
        // the alternative is a factory holding the replay's state for [L, H) and
        // nothing for the frontier above it, which the following in-order rows would
        // then accumulate onto. Here the view keeps ingesting in order after the
        // stalled repair, which is what reads that state rather than the output.
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHistory(job);

                job.setSimulateRepairApplyFailureForTest(true);
                setCurrentMicros(currentMicros + 200_000);
                execute("INSERT INTO base VALUES ('" + timestamp(25) + "', 'a', 100)");
                drainWalQueue();
                Assert.assertTrue(job.run());
                drainWalQueue();

                job.setSimulateRepairApplyFailureForTest(false);
                driveRefreshToQuiescence(job);
                // 130s is 10 seconds past the 120s frontier, so the RANGE 30 SECOND
                // frame holds 100s, 110s, 120s and itself: 10 + 11 + 12 + 13 = 46.
                appendAndRefresh(job, 130, 13);
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
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n" +
                            "2026-01-01T00:02:10.000000Z\ta\t46.0\n");
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
                // 60s onward carries both range-adds (+2 then -1).
                final long[] expected = {1, 3, 5, 9, 11, 7, 8, 9, 10, 11, 12, 13};
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
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static Path checkpointsDir(Path dst) {
        return dst.of(configuration.getDbRoot())
                .concat(engine.verifyTableName("lv"))
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static String descriptorPath(long repairId) {
        try (Path dir = new Path(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir(dir), repairId);
            return path.toString();
        }
    }

    /**
     * Plants the leftovers of a repair that died with its candidate staged: the
     * descriptor plus the temporary data segment it claims ownership of.
     */
    private static void plantCrashedRepair(LiveViewInstance instance, long repairId, long segmentId) {
        try (
                LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                Path checkpointsDir = checkpointsDir(instance);
                Path path = new Path()
        ) {
            state.begin(
                    checkpointsDir,
                    repairId,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    1,
                    repairId,
                    repairId - 1,
                    ts(timestamp(30)),
                    ts(timestamp(10)),
                    ts(timestamp(25)),
                    ts(timestamp(60)),
                    LiveViewCheckpointContracts.HighBoundTag.FINITE
            );
            state.addOwnedSegmentId(segmentId);
            LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir, segmentId);
            Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
        }
    }

    private static int repairDescriptorCount() {
        int count = 0;
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = new Path(); Path repairDir = new Path()) {
            LiveViewCheckpointLayout.repairDirPath(repairDir, checkpointsDir(dir));
            if (!ff.exists(repairDir.$())) {
                return 0;
            }
            final long findPtr = ff.findFirst(repairDir.$());
            if (findPtr == 0) {
                return 0;
            }
            final StringSink name = new StringSink();
            try {
                do {
                    final long namePtr = ff.findName(findPtr);
                    name.clear();
                    if (namePtr != 0
                            && Utf8s.utf8ToUtf16Z(namePtr, name)
                            && Chars.startsWith(name, LiveViewCheckpointLayout.REPAIR_DESCRIPTOR_PREFIX)) {
                        count++;
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        }
        return count;
    }

    private static boolean repairDescriptorExists(long repairId) {
        try (Path dir = new Path(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir(dir), repairId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private static boolean tmpDataSegmentExists(long segmentId) {
        try (Path dir = new Path(); Path path = new Path()) {
            LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir(dir), segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
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
    public void testRowsSpliceLocalizesFromTheDiscoveredBounds() throws Exception {
        // The ROWS shape reaches the same splice as RANGE, by discovery rather than by
        // arithmetic. A ROWS 3 PRECEDING frame at the i-th row above the change holds
        // the changed row exactly while i <= 3, so the row at 60s - the fourth above
        // 25s - has converged and H is its timestamp. Everything in [25s, 60s) is
        // re-emitted, the row at 60s is not, and neither are the two below the floor.
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
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);
                Assert.assertTrue(generationBefore > 1);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals(
                        "only the rows in [R, H) are re-emitted: 25s, 30s, 40s, 50s",
                        4,
                        instance.getO3BoundaryReplayRows()
                );
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                final LongList after = snapshotTimeline(instance);
                // Prefix (10s, 20s): below the correction floor, so nothing about them
                // changed - not their payload roots and not their positions.
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                Assert.assertEquals(1, after.getQuick(ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(2, after.getQuick(ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                // Repaired interval (30s, 40s, 50s): new root versions off the replay,
                // and positions that count the row the replay inserted at 25s.
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                assertNewRoot(before, after, 4);
                Assert.assertEquals(4, after.getQuick(2 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(5, after.getQuick(3 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                Assert.assertEquals(6, after.getQuick(4 * ENTRY_SIZE + ENTRY_EFFECTIVE_POSITION));
                // Converged suffix (60s): its frame no longer reaches the change, so its
                // payload root is reused and only its cumulative position moves.
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
                            "2026-01-01T00:00:40.000000Z\ta\t109.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t112.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t22.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t26.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t30.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t34.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t38.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t42.0\n");
        });
    }

    @Test
    public void testLaggingRowsFrameSplicesOnTheDiscoveredCount() throws Exception {
        // The ROWS counterpart of testLaggingRangeFrameSplicesOnTheLookBehindAlone. The
        // discovery counts predecessors, not frame extent: the frame at the i-th row above
        // the change spans [i - 3, i - 1] rows back from itself, so it holds the changed row
        // while 1 <= i <= 3 and the row at 60s - the fourth above 25s - has converged. That
        // is the same H the unlagged frame discovers, and the same 4 rows are re-emitted.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, sum(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING" +
                            ") s FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals(
                        "only the rows in [R, H) are re-emitted: 25s, 30s, 40s, 50s",
                        4,
                        instance.getO3BoundaryReplayRows()
                );
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                final LongList after = snapshotTimeline(instance);
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                assertNewRoot(before, after, 4);
                assertSameRoot(before, after, 5);
            }
            assertNoRefreshFaults("lv");

            // The oracle: the row at 25s enters the frame of the next three rows and leaves
            // the frame of the fourth, so 30s, 40s and 50s move and 60s does not. The
            // earliest row has no predecessor, so its frame is empty.
            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\tnull\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t103.0\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t105.0\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t107.0\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t12.0\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t15.0\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t18.0\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t21.0\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t24.0\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t27.0\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t30.0\n");
        });
    }

    @Test
    public void testUnboundedStartLastValueSplicesOnTheHighBoundLag() throws Exception {
        // The frame starts at UNBOUNDED PRECEDING, which is what the CREATE-time reject used
        // to turn away and what a whole-history rebuild is the fallback for. last_value
        // accumulates nothing: it emits the row 2 back, so its state is the 2 values behind
        // the current row and the discovery runs on that count rather than on the frame's.
        // Inserting at 25s leaves the 2nd predecessor of every row from 50s up where it was -
        // 30s reads 20s and 40s reads 25s, but 50s reads 30s both before and after - so only
        // the 2 rows above the change move and the repair re-emits [25s, 50s).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, last_value(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING" +
                            ") l FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                final LongList before = snapshotTimeline(instance);
                final long generationBefore = generation(instance);

                appendAndRefresh(job, 25, 100);

                Assert.assertEquals(
                        "only the rows in [R, H) are re-emitted: 25s, 30s, 40s",
                        3,
                        instance.getO3BoundaryReplayRows()
                );
                Assert.assertEquals(
                        "a spliced timeline keeps every logical entry it had",
                        HISTORY_COMMITS,
                        entryCount(instance)
                );
                Assert.assertEquals(generationBefore + 1, generation(instance));

                // The prefix and the converged suffix keep their payload roots by page
                // identity, which is what separates a localized repair from the whole-history
                // rebuild an unbounded frame start took before: that one versions every root.
                final LongList after = snapshotTimeline(instance);
                assertSameRoot(before, after, 0);
                assertSameRoot(before, after, 1);
                assertNewRoot(before, after, 2);
                assertNewRoot(before, after, 3);
                assertSameRoot(before, after, 4);
                assertSameRoot(before, after, 5);
            }
            assertNoRefreshFaults("lv");

            // The oracle: every row emits the value 2 rows behind it. The insert shifts what
            // 30s and 40s read and leaves 50s onward reading the rows they already did.
            assertQuery("select ts, sym, l from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\tl\n" +
                            "2026-01-01T00:00:10.000000Z\ta\tnull\n" +
                            "2026-01-01T00:00:20.000000Z\ta\tnull\n" +
                            "2026-01-01T00:00:25.000000Z\ta\t1\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t2\n" +
                            "2026-01-01T00:00:40.000000Z\ta\t100\n" +
                            "2026-01-01T00:00:50.000000Z\ta\t3\n" +
                            "2026-01-01T00:01:00.000000Z\ta\t4\n" +
                            "2026-01-01T00:01:10.000000Z\ta\t5\n" +
                            "2026-01-01T00:01:20.000000Z\ta\t6\n" +
                            "2026-01-01T00:01:30.000000Z\ta\t7\n" +
                            "2026-01-01T00:01:40.000000Z\ta\t8\n" +
                            "2026-01-01T00:01:50.000000Z\ta\t9\n" +
                            "2026-01-01T00:02:00.000000Z\ta\t10\n");
        });
    }

    private LiveViewInstance buildHistory(LiveViewRefreshJob job) throws Exception {
        return buildHistory(job, HISTORY_COMMITS);
    }

    private LiveViewInstance buildHistory(LiveViewRefreshJob job, int commits) throws Exception {
        for (int commit = 1; commit <= commits; commit++) {
            appendAndRefresh(job, commit * 10, commit);
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertEquals(commits, entryCount(instance));
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

    /**
     * Rows the live view's own table durably holds.
     */
    private long durableRowCount(LiveViewInstance instance) {
        try (TableReader reader = engine.getReader(instance.getLiveViewToken())) {
            return reader.size();
        }
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

    /**
     * True while the view still has a published checkpoint timeline superblock.
     */
    private boolean hasTimeline(LiveViewInstance instance) {
        try (
                Path checkpointsDir = checkpointsDir(instance);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            return configuration.getFilesFacade().exists(timelinePath.$());
        }
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private long nextSegmentId(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().nextSegmentId;
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
        return publish(
                writer,
                capture,
                instance,
                highTsExclusive,
                suffixRowDelta,
                normalizedBaseSeqTxn(instance),
                coveredLvSeqTxn(instance)
        );
    }

    private LiveViewCheckpointTimelineStoreWriter.RepairResult publish(
            LiveViewCheckpointTimelineStoreWriter writer,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture,
            LiveViewInstance instance,
            long highTsExclusive,
            long suffixRowDelta,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn
    ) {
        return writer.publishRepair(
                capture,
                instance.getLiveViewToken().getTableId(),
                normalizedBaseSeqTxn,
                coveredLvSeqTxn,
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
        return repair(
                instance,
                lowTimestampInclusive,
                highTsExclusive,
                effectivePositions,
                suffixRowDelta,
                normalizedBaseSeqTxn(instance),
                coveredLvSeqTxn(instance)
        );
    }

    private LiveViewCheckpointTimelineStoreWriter.RepairResult repair(
            LiveViewInstance instance,
            long lowTimestampInclusive,
            long highTsExclusive,
            long[] effectivePositions,
            long suffixRowDelta,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn
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
                return publish(
                        writer,
                        capture,
                        instance,
                        highTsExclusive,
                        suffixRowDelta,
                        normalizedBaseSeqTxn,
                        coveredLvSeqTxn
                );
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
