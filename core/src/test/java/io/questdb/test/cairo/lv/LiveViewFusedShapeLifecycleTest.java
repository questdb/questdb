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
import io.questdb.cairo.lv.LiveViewCheckpointCompaction;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointScratchOverlay;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * The checkpoint lifecycle, walked shape by shape for the two durable shapes step 8
 * added to the fused window state.
 * <p>
 * Both are shapes in which the group is <b>not</b> the whole factory, which is what
 * makes them worth walking separately from the target shape:
 * <ul>
 *     <li>a <b>truncated group</b> - one component more than the leaf budget carries,
 *     so the plan keeps the prefix that fits and hands the rest back. Its state lives
 *     in two places at once: the window's fused value for the kept prefix and a private
 *     map for the residual, and every path below has to move both;</li>
 *     <li>a <b>DECIMAL view</b> - {@code count(dec)} joins the group while
 *     {@code sum(dec)} and {@code avg(dec)} keep roots of their own, now carrying their
 *     whole image in the leaf rather than in a data page.</li>
 * </ul>
 * Seed, incremental seal, restart and restore are covered for both by
 * {@code LiveViewWindowStateRuntimeTest}. What this class adds is the rest of the
 * matrix - out-of-order repair, the repair scratch overlay, frontier compaction,
 * timeline retention, physical checkpoint compaction over a page-backed residual, and
 * publication fault injection - each driven end to end and each checked against a
 * from-base recompute rather than against the runtime's own arithmetic.
 * <p>
 * Three cases give each shape a ring-backed residual as well. A fused group writes no
 * data page at all, so a ring beside it is the only way these shapes reach the paths
 * that are about data segments: a torn data segment is the cheapest structural
 * corruption a head can carry, the data-publish stage of a seal is never reached
 * without one, and a compaction pass over a page-free timeline would have no catalogue
 * to walk.
 */
public class LiveViewFusedShapeLifecycleTest extends AbstractLiveViewTest {

    private static final String ANCHOR_DAY = "2026-01-01T";
    private static final String NEXT_DAY = "2026-01-02T";
    // One component past MAX_INLINE_LEAF_STATE_BYTES, computed the same way the plan
    // does, so the case follows the constant rather than pinning a number beside it.
    private static final int TRUNCATED_COLUMNS =
            (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES)
                    / (Double.BYTES + Long.BYTES) + 1;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so every case below builds a timeline a
        // repair, a purge and a compaction have something to work over.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testADecimalViewFallsBackToASafePredecessorWhenItsHeadIsTorn() throws Exception {
        assertTornHeadReconstructs(decimalShape(true));
    }

    @Test
    public void testADecimalViewRetainsEveryLogicalEntryAcrossAPurgeAndARestart() throws Exception {
        assertRetainsEveryLogicalEntry(decimalShape(false));
    }

    @Test
    public void testADecimalViewsRepairScratchRoundTripsBothOwners() throws Exception {
        assertRepairScratchRoundTrips(decimalShape(false));
    }

    @Test
    public void testADecimalViewSurvivesAPublicationFault() throws Exception {
        assertPublicationFaultRetries(decimalShape(true));
    }

    @Test
    public void testADecimalViewSurvivesAnOutOfOrderCorrection() throws Exception {
        assertOutOfOrderCorrectionSurvives(decimalShape(false));
    }

    @Test
    public void testADecimalViewSurvivesPhysicalCompaction() throws Exception {
        assertPhysicalCompactionPreservesTheShape(decimalShape(true));
    }

    @Test
    public void testADecimalViewSweepsItsFrontierThroughBothOwners() throws Exception {
        assertFrontierSweepReclaimsTheShape(decimalShape(false));
    }

    @Test
    public void testATruncatedGroupFallsBackToASafePredecessorWhenItsHeadIsTorn() throws Exception {
        assertTornHeadReconstructs(truncatedShape(true));
    }

    @Test
    public void testATruncatedGroupRetainsEveryLogicalEntryAcrossAPurgeAndARestart() throws Exception {
        assertRetainsEveryLogicalEntry(truncatedShape(false));
    }

    @Test
    public void testATruncatedGroupsRepairScratchRoundTripsBothOwners() throws Exception {
        assertRepairScratchRoundTrips(truncatedShape(false));
    }

    @Test
    public void testATruncatedGroupSurvivesAPublicationFault() throws Exception {
        assertPublicationFaultRetries(truncatedShape(true));
    }

    @Test
    public void testATruncatedGroupSurvivesAnOutOfOrderCorrection() throws Exception {
        assertOutOfOrderCorrectionSurvives(truncatedShape(false));
    }

    @Test
    public void testATruncatedGroupSurvivesPhysicalCompaction() throws Exception {
        assertPhysicalCompactionPreservesTheShape(truncatedShape(true));
    }

    @Test
    public void testATruncatedGroupSweepsItsFrontierThroughBothOwners() throws Exception {
        assertFrontierSweepReclaimsTheShape(truncatedShape(false));
    }

    /**
     * Adds one state page to the per-segment live-byte tally, counting a page shared by
     * several roots once.
     */
    private static void addLivePage(
            LongObjHashMap<long[]> liveBytes,
            HashSet<String> seen,
            LiveViewCheckpointStatePageRef ref
    ) {
        if (ref.isNull()
                || !seen.add(ref.getSegmentId() + ":" + ref.getOffset() + ":" + ref.getStoredLength())) {
            return;
        }
        long[] acc = liveBytes.get(ref.getSegmentId());
        if (acc == null) {
            acc = new long[]{0};
            liveBytes.put(ref.getSegmentId(), acc);
        }
        acc[0] += ref.getStoredLength();
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static byte[] copyBytes(MemoryCARW sink) {
        final int length = (int) sink.getAppendOffset();
        final byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = sink.getByte(i);
        }
        return out;
    }

    /**
     * Every runtime state the shape owns, as bytes: the window's fused value per
     * partition, then one frame per function whose state the window does not own. A
     * restore, an overlay round trip and a rebind all have to reproduce it exactly.
     */
    private static byte[][] snapshotRuntime(ObjList<WindowFunction> functions, LiveViewWindow window) {
        final byte[][] states = new byte[functions.size() + 1][];
        int count = 0;
        try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            window.snapshot(sink);
            states[count++] = copyBytes(sink);
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState() || function.isWindowStateOwned()) {
                continue;
            }
            try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                LiveViewFunctionSnapshot.write(sink, function);
                states[count++] = copyBytes(sink);
            }
        }
        return Arrays.copyOf(states, count);
    }

    private static String timestamp(String day, int secondOfDay) {
        return String.format(
                "%s%02d:%02d:%02d.000000Z",
                day,
                9 + secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
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

    private long assertEpochRetainsEveryEntry(LiveViewInstance instance, long previousEntries, String when) {
        final LongList ids = logicalCheckpointIds(instance);
        final long nextCheckpointId = nextCheckpointId(instance);
        Assert.assertEquals(
                "the epoch must hold every checkpoint id it allocated " + when,
                nextCheckpointId,
                ids.size()
        );
        for (int i = 0, n = ids.size(); i < n; i++) {
            Assert.assertEquals("logical entry at index " + i + " " + when, i, ids.getQuick(i));
        }
        Assert.assertTrue(
                "the logical entry count must never drop " + when
                        + " [before=" + previousEntries + ", after=" + ids.size() + ']',
                ids.size() >= previousEntries
        );
        return ids.size();
    }

    /**
     * Drives the frontier sweep and requires it to have reclaimed through both owners:
     * the window's one fused map for the kept components, and each residual's private
     * map for the rest. A sweep that pruned one and not the other would leave the two
     * describing different key sets, which the next seal would publish as a fused entry
     * beside a residual root that disagrees about which partitions exist.
     */
    private void assertFrontierSweepReclaimsTheShape(Shape shape) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_STALE_PERCENT, 50);
        assertMemoryLeak(() -> {
            shape.createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                // Three accounts in the first bucket, then two bucket advances, so the
                // first bucket falls behind the retained pair and its accounts become
                // reclaimable.
                shape.insert(job, timestamp(ANCHOR_DAY, 0), "acct-1", 1);
                shape.insert(job, timestamp(ANCHOR_DAY, 10), "acct-2", 2);
                shape.insert(job, timestamp(ANCHOR_DAY, 20), "acct-3", 3);
                shape.insert(job, timestamp(NEXT_DAY, 0), "acct-4", 4);
                shape.insert(job, timestamp("2026-01-03T", 0), "acct-5", 5);

                final LiveViewWindow window = window();
                Assert.assertTrue("the sweep must have fired", window.getCompactionCount() > 0);
                Assert.assertEquals("only the two retained buckets survive", 2, window.getAnchorMapSize());
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                int sweptResiduals = 0;
                for (int i = 0, n = plan.getResidualFunctions().size(); i < n; i++) {
                    final WindowFunction residual = plan.getResidualFunctions().getQuick(i);
                    if (residual.getPartitionMap() == null || !residual.getPartitionMap().isOpen()) {
                        continue;
                    }
                    Assert.assertEquals(
                            "residual " + residual.getName() + " must sweep with the window",
                            window.getAnchorMapSize(),
                            residual.getPartitionMap().size()
                    );
                    sweptResiduals++;
                }
                // Both shapes keep at least one function out of the group, so a run that
                // compared nothing would mean the shape stopped being what it is named for.
                Assert.assertTrue("the shape must carry a residual with a map of its own", sweptResiduals > 0);
                shape.assertMatchesRecompute();

                // And the head sealed over a swept runtime restores it back.
                final byte[][] before = snapshotRuntime(unwrapWindowFunctions(instance()), window);
                restoreHead();
                assertSameRuntime(before, snapshotRuntime(unwrapWindowFunctions(instance()), window));
                shape.assertMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Commits a row below the frontier and requires the view to have repaired rather
     * than appended, then re-sealed a head that restores on its own.
     * <p>
     * An anchored view prices a repair against a resume from the sealed boundary below
     * the change, and that resume always wins here - a daily anchor bounds a rebuild by
     * the segment starting at midnight, which reads more base rows than replaying the
     * tail. So what these shapes actually meet is a restore from one of their own
     * boundaries, the tail retired above it, and the replay re-sealing on top.
     */
    private void assertOutOfOrderCorrectionSurvives(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 10; second <= 60; second += 10) {
                    shape.insert(job, timestamp(ANCHOR_DAY, second), "acct-1", second / 10);
                }
                shape.insert(job, timestamp(ANCHOR_DAY, 70), "acct-2", 9);
                final long repairedBefore = repairedRows(instance());

                shape.insert(job, timestamp(ANCHOR_DAY, 35), "acct-1", 100);
                Assert.assertTrue(
                        "the row below the frontier must be repaired rather than appended",
                        repairedRows(instance()) > repairedBefore
                );

                shape.assertMatchesRecompute();
                final byte[][] before = snapshotRuntime(unwrapWindowFunctions(instance()), window());
                restoreHead();
                assertSameRuntime(before, snapshotRuntime(unwrapWindowFunctions(instance()), window()));
                shape.assertMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Runs a physical checkpoint compaction over a shape whose ring-backed residual is
     * what writes the data segments there are to relocate, and requires the fused root
     * to come through it addressable.
     * <p>
     * <b>The fused root contributes nothing for compaction to do</b>, and that is the
     * property this case pins first: it names no data segment at all, so the plan finds
     * no reference in it to redirect and reuses it untouched. The ring-backed residual
     * beside it is what names data segments, so the timeline is not trivially page-free
     * and the pass has a real catalogue to walk.
     * <p>
     * Whether the pass selects anything is then a property of the <i>history</i> rather
     * than of the shape, and for an anchored view it usually selects nothing: a
     * compaction source has to be partially dead, and an anchored repair retires a suffix
     * of the timeline whole rather than re-versioning an interval inside it, so a data
     * segment ends up either wholly live or wholly dead. The case asserts both halves -
     * that no source is left unclaimed when one exists, and that the shape is intact
     * either way - and the restart is what proves the preserved root is still readable.
     */
    private void assertPhysicalCompactionPreservesTheShape(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                driveSeedToCompletion(job, "lv");
                for (int commit = 1; commit <= 20; commit++) {
                    shape.insert(job, timestamp(ANCHOR_DAY, commit * 10), "acct-" + (commit % 3), commit);
                }
                // Corrections at two depths, so the timeline holds retired versions beside
                // surviving ones and the catalogue the pass walks is a real one.
                for (int base : new int[]{6, 12}) {
                    final long repairedBefore = repairedRows(instance());
                    shape.insert(job, timestamp(ANCHOR_DAY, base * 10 + 3), "acct-" + (base % 3), 90 + base);
                    Assert.assertTrue(
                            "the correction at group " + base + " must be repaired rather than appended",
                            repairedRows(instance()) > repairedBefore
                    );
                }

                final LiveViewInstance instance = instance();
                Assert.assertTrue(isFusedHead());
                Assert.assertEquals(
                        "a fused root names no data segment, so compaction finds nothing in it to redirect",
                        0,
                        headWindowRootDataSegmentCount()
                );
                Assert.assertTrue(
                        "the ring-backed residual must be writing the data segments the pass walks",
                        instance.getCheckpointDataSegmentCount() > 0
                );

                final LiveViewCheckpointCompaction.Result result;
                try (Path dir = checkpointsDir(instance)) {
                    result = LiveViewCheckpointCompaction.compact(
                            configuration,
                            dir,
                            writer,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            true,
                            100,
                            1,
                            64
                    );
                }
                if (result.isPublished()) {
                    Assert.assertTrue("a published compaction rewrites at least one root", result.getRootsRewritten() > 0);
                } else {
                    // Not a silent pass: an anchored repair retires a suffix whole, so
                    // every data segment is either wholly live or wholly dead and none of
                    // them is a compaction source. Read off the catalogue rather than
                    // assumed, so a future disposition that does re-version an interval
                    // fails here instead of quietly weakening the case.
                    Assert.assertEquals(
                            "the pass declined, so no data segment may be partially dead",
                            0,
                            partiallyDeadDataSegmentCount(instance)
                    );
                }
                Assert.assertTrue("the fused root must survive the pass", isFusedHead());
                shape.assertMatchesRecompute();

                // The restart is what proves the preserved root is still addressable.
                restartCycle();
                Assert.assertTrue("the restart must restore a fused head", isFusedHead());
                shape.assertMatchesRecompute();

                // And a fresh row folds onto the reconstructed state rather than onto a
                // group that silently restarted at zero.
                shape.insert(job, timestamp(ANCHOR_DAY, 210), "acct-0", 11);
                shape.assertMatchesRecompute();
            }
        });
    }

    /**
     * Injects a publication failure into a seal, at each of the two stages a crash can
     * leave immutable files behind without a durable slot naming them, then lets the
     * next seal retry.
     * <p>
     * The failed publication leaves an orphan the retry has to allocate past rather than
     * write through, and the retry may reach that state through a retired epoch - so
     * neither the generation nor the id space is a fixed point here, and what is asserted
     * instead is that the shape comes back: a fused root beside its residuals, matching a
     * from-base recompute, surviving a purge and restoring after a restart.
     * <p>
     * {@code getCheckpointSealFailures()} is what keeps the injection from being a silent
     * no-op. It matters most for the data stage, which a fully inline seal never reaches
     * at all - and is exactly why these cases carry a ring-backed residual.
     */
    private void assertPublicationFaultRetries(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                shape.insert(job, timestamp(ANCHOR_DAY, 0), "acct-1", 1);
                shape.insert(job, timestamp(ANCHOR_DAY, 10), "acct-2", 2);

                for (int stage : new int[]{
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH,
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH
                }) {
                    final long failuresBefore = instance().getCheckpointSealFailures();
                    job.setCheckpointTimelineTestFailureStage(stage);
                    shape.insert(job, timestamp(ANCHOR_DAY, 20 + stage), "acct-1", 3 + stage);
                    Assert.assertTrue(
                            "the injected failure must actually fire [stage=" + stage + ']',
                            instance().getCheckpointSealFailures() > failuresBefore
                    );

                    job.setCheckpointTimelineTestFailureStage(0);
                    shape.insert(job, timestamp(ANCHOR_DAY, 30 + stage), "acct-2", 4 + stage);
                    Assert.assertTrue(
                            "the retry must publish a fused head over the orphan [stage=" + stage + ']',
                            isFusedHead()
                    );
                    shape.assertMatchesRecompute();
                }

                purgeCycle(instance());
                shape.assertMatchesRecompute();

                restartCycle();
                Assert.assertTrue("the restart must restore a fused head", isFusedHead());
                shape.assertMatchesRecompute();
            }
        });
    }

    /**
     * Captures the runtime into the repair scratch overlay, wipes it the way a localized
     * replay does, and requires the restore to put every owner back byte for byte.
     * <p>
     * The overlay filters both passes on {@code isWindowStateOwned()}, so a shape whose
     * group is only part of the factory is exactly where the two passes could disagree:
     * the fused prefix rides in the window's own frame and each residual keeps a frame
     * of its own, and a skip on one side alone would restore the wrong bytes into the
     * wrong function.
     */
    private void assertRepairScratchRoundTrips(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                shape.insert(job, timestamp(ANCHOR_DAY, 0), "acct-1", 1);
                shape.insert(job, timestamp(ANCHOR_DAY, 10), "acct-2", 2);
                shape.insert(job, timestamp(ANCHOR_DAY, 20), "acct-1", 3);

                final LiveViewWindow window = window();
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance());
                final byte[][] before = snapshotRuntime(functions, window);
                try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                    overlay.capture(functions, window, null);
                    Assert.assertTrue(overlay.isCaptured());

                    // What a localized replay does to the live instances before it starts.
                    window.toTop();
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getQuick(i).toTop();
                    }
                    Assert.assertEquals(0, window.getAnchorMapSize());

                    overlay.restore(functions, window);
                    Assert.assertFalse("the overlay releases the buffer with its state", overlay.isCaptured());
                }
                assertSameRuntime(before, snapshotRuntime(functions, window));

                // The restored state is not merely equal but usable: the next row folds
                // onto it, which a restore that put the accumulators back in the wrong
                // slots would not survive.
                shape.insert(job, timestamp(ANCHOR_DAY, 30), "acct-1", 4);
                shape.assertMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Requires the epoch to hold every logical checkpoint id it ever allocated - before
     * and after a purge, a restart and a resumed cadence.
     * <p>
     * The oracle is the id set rather than its size: the epoch allocates from zero and
     * monotonically, so a contiguous run ending one below the next id to allocate proves
     * no boundary went missing, where a count alone would pass a publication that
     * dropped an old entry and appended a new one.
     */
    private void assertRetainsEveryLogicalEntry(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second < 60; second += 10) {
                    shape.insert(job, timestamp(ANCHOR_DAY, second), "acct-" + (second % 20), second / 10);
                }
                final LiveViewInstance instance = instance();
                long entries = assertEpochRetainsEveryEntry(instance, 0, "after the cadence");
                Assert.assertTrue("the cadence must have sealed several boundaries", entries > 1);

                purgeCycle(instance);
                entries = assertEpochRetainsEveryEntry(instance, entries, "after the purge");

                restartCycle();
                entries = assertEpochRetainsEveryEntry(instance(), entries, "after the restart");
                shape.assertMatchesRecompute();

                // The restarted view keeps sealing into the same epoch rather than
                // starting a new one, which a retired timeline would have forced.
                shape.insert(job, timestamp(ANCHOR_DAY, 70), "acct-1", 8);
                final long resumed = assertEpochRetainsEveryEntry(instance(), entries, "after resuming");
                Assert.assertTrue("the resumed cadence appends rather than restarts", resumed > entries);
                shape.assertMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    private void assertSameRuntime(byte[][] expected, byte[][] actual) {
        Assert.assertEquals("the runtime must hold the same number of state owners", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertArrayEquals("runtime state differs at owner " + i, expected[i], actual[i]);
        }
    }

    /**
     * Truncates the newest root's data segment by one byte - the cheapest structural
     * corruption a torn write can leave - and requires the restart to reconstruct that
     * boundary in place rather than to retire the timeline.
     * <p>
     * The byte belongs to the ring-backed residual, since the fused group writes no data
     * page at all. What it puts under test is the fused root beside it: a reader that
     * fell back to a predecessor has to find that predecessor's window root and restore
     * the group from it, and the reconstructed head must then be addressable at its
     * original id.
     */
    private void assertTornHeadReconstructs(Shape shape) throws Exception {
        assertMemoryLeak(() -> {
            shape.createView();
            final long generationBefore;
            final long nextIdBefore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second < 50; second += 10) {
                    shape.insert(job, timestamp(ANCHOR_DAY, second), "acct-" + (second % 20), second / 10);
                }
                final LiveViewInstance instance = instance();
                generationBefore = generation(instance);
                nextIdBefore = nextCheckpointId(instance);
                corruptNewestRootDataSegment(instance);
            }

            restartCycle();
            final LiveViewInstance restored = instance();
            Assert.assertTrue(
                    "reconstruction must advance the generation, not reset it [generation="
                            + generation(restored) + ']',
                    generation(restored) > generationBefore
            );
            Assert.assertEquals(
                    "reconstruction re-versions ids in place and mints none",
                    nextIdBefore,
                    nextCheckpointId(restored)
            );
            assertEpochRetainsEveryEntry(restored, 0, "after reconstruction");
            Assert.assertTrue("the healed head must still be a fused root", isFusedHead());
            shape.assertMatchesRecompute();

            // A fresh row folds onto the reconstructed state, so this exercises what the
            // heal put back rather than only the durable table it left alone.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                shape.insert(resumed, timestamp(ANCHOR_DAY, 60), "acct-0", 9);
            }
            shape.assertMatchesRecompute();
        });
    }

    // Truncates the newest logical root's first data segment by one byte, so the reader
    // rejects its state page on a length check.
    private void corruptNewestRootDataSegment(LiveViewInstance instance) {
        final long segmentId;
        final long fileLength;
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointSegmentDirectoryReader directory =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the timeline must hold a newest root", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            Assert.assertTrue("the newest root must name a data segment", root.getSegmentIdCount() > 0);
            segmentId = root.getSegmentId(0);
            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            fileLength = directory.getFileLength(segmentId);
        }
        try (Path checkpointsDir = checkpointsDir(instance); Path dataPath = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(dataPath, checkpointsDir, segmentId);
            final FilesFacade ff = configuration.getFilesFacade();
            final long fd = ff.openRW(dataPath.$(), 0);
            try {
                Assert.assertTrue("truncating the data segment must succeed", ff.truncate(fd, fileLength - 1));
            } finally {
                ff.close(fd);
            }
        }
    }

    /**
     * {@code count(dec)} joins the group under the DECIMAL width's own null test while
     * {@code sum(dec)} and {@code avg(dec)} keep roots of their own - their accumulators
     * are a Decimal128 or Decimal256 beside a flag or a counter, which the component
     * families do not describe.
     */
    private Shape decimalShape(boolean withRingResidual) {
        return new Shape() {
            @Override
            void assertMatchesRecompute() throws Exception {
                assertMatchesRecompute(
                        "sum(amt_txn) " + FRAME + " as s, avg(amt_txn) " + FRAME + " as a, "
                                + "count(amt_txn) " + FRAME + " as c",
                        "s, a, c",
                        withRingResidual
                );
            }

            @Override
            void createView() throws Exception {
                execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn decimal(38,2)) "
                        + "timestamp(created_at) partition by hour wal");
                execute("create live view lv flush every 100ms start from beginning as "
                        + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                        + "avg(amt_txn) over w as a, count(amt_txn) over w as c"
                        + (withRingResidual ? ", " + RING_PROJECTION : "")
                        + " from tx window w as "
                        + "(partition by cod_acct_no order by created_at anchor daily '00:00')");
            }

            @Override
            void insert(LiveViewRefreshJob job, String timestamp, String account, int ordinal) throws Exception {
                execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                        + ordinal + ".25::decimal(38,2))");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        };
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    /**
     * How many <b>data</b> segments the head's fused window root names. Zero for every
     * shape this class drives: the group's whole state is inline in the leaf, so the
     * segments the root does reference are the metadata ones its own partition map lives
     * in - which compaction is not a candidate for.
     */
    private int headWindowRootDataSegmentCount() {
        final LiveViewInstance instance = instance();
        int dataSegments = 0;
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointSegmentDirectoryReader segments =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            Assert.assertTrue("the head must be a fused root", windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef));
            segments.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            final LongList dataSegmentIds = new LongList();
            segments.iterateAll(entry -> {
                if (!entry.isMetadata()) {
                    dataSegmentIds.add(entry.segmentId);
                }
            });
            for (int i = 0, n = windowRoot.getSegmentUseCountSize(); i < n; i++) {
                if (dataSegmentIds.indexOf(windowRoot.getSegmentId(i)) >= 0) {
                    dataSegments++;
                }
            }
        }
        return dataSegments;
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private boolean isFusedHead() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            return !stateRootRef.isNull() && windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef);
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

    /**
     * The number of data segments the published generation still names some bytes of but
     * not all - the shape a compaction source has to have. Computed the way
     * {@code LiveViewCheckpointCompaction} computes it: distinct live pages across every
     * root, summed per segment and compared against the segment's own length.
     */
    private int partiallyDeadDataSegmentCount(LiveViewInstance instance) {
        final LongObjHashMap<long[]> liveBytes = new LongObjHashMap<>();
        final HashSet<String> seen = new HashSet<>();
        final int[] partial = {0};
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
                final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    root.of(dir, entry.rootRef);
                    root.getFunctionDirectoryRef(directoryRef);
                    functions.of(dir, directoryRef);
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getRootRef(i, functionRootRef);
                        functionRoot.of(dir, functionRootRef);
                        functionRoot.getScalarStateRef(scalarRef);
                        addLivePage(liveBytes, seen, scalarRef);
                        functionRoot.getPartitionMapRootRef(partitionMapRoot);
                        partitions.iterateAll(partitionMapRoot, partitionEntry -> {
                            for (int p = 0, m = partitionEntry.getStatePageCount(); p < m; p++) {
                                addLivePage(liveBytes, seen, partitionEntry.getStatePageRef(p));
                            }
                        });
                    }
                });
                segments.iterateAll(entry -> {
                    if (entry.isMetadata()) {
                        return;
                    }
                    final long[] live = liveBytes.get(entry.segmentId);
                    if (live != null && live[0] > 0 && live[0] < entry.fileLength) {
                        partial[0]++;
                    }
                });
            }
        }
        return partial[0];
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
            Assert.assertFalse("the definition and epoch are fixed for the whole case", result.isEpochReplaced());
            Assert.assertFalse("this build wrote the directory it is reconciling", result.isFormatReset());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
        }
    }

    // Base rows a repair replayed over this instance's lifetime, through either
    // disposition: the resume from a boundary below the change, or the localized
    // rebuild over the change's own dependency interval.
    private long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    /**
     * Restores the published head over the live runtime, through the same reader the
     * refresh worker uses after a restart.
     */
    private void restoreHead() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir);
            reader.restoreLatest(
                    instance.getLiveViewToken().getTableId(),
                    unwrapWindowFunctions(instance),
                    instance.getAnchorWindow()
            );
        }
    }

    /**
     * One component more than the leaf budget carries, so the plan keeps the prefix of
     * the canonical order that fits and turns the last one into a residual.
     */
    private Shape truncatedShape(boolean withRingResidual) {
        return new Shape() {
            @Override
            void assertMatchesRecompute() throws Exception {
                // The first fused output and the residual one: the two ends of the
                // truncation, one read off the window's value and one off the function's
                // private map.
                assertMatchesRecompute(
                        "sum(q1) " + FRAME + " as s1, sum(q" + TRUNCATED_COLUMNS + ") " + FRAME + " as sn",
                        "s1, s" + TRUNCATED_COLUMNS + " as sn",
                        withRingResidual
                );
            }

            @Override
            void createView() throws Exception {
                final StringBuilder ddl = new StringBuilder();
                final StringBuilder projections = new StringBuilder();
                for (int i = 1; i <= TRUNCATED_COLUMNS; i++) {
                    ddl.append(", q").append(i).append(" double");
                    projections.append(", sum(q").append(i).append(") over w as s").append(i);
                }
                execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn double" + ddl + ") "
                        + "timestamp(created_at) partition by hour wal");
                execute("create live view lv flush every 100ms start from beginning as "
                        + "select created_at, cod_acct_no" + projections
                        + (withRingResidual ? ", " + RING_PROJECTION : "")
                        + " from tx window w as "
                        + "(partition by cod_acct_no order by created_at anchor daily '00:00')");
            }

            @Override
            void insert(LiveViewRefreshJob job, String timestamp, String account, int ordinal) throws Exception {
                final StringBuilder values = new StringBuilder();
                for (int i = 1; i <= TRUNCATED_COLUMNS; i++) {
                    values.append(", ").append(ordinal * i).append(".0");
                }
                execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                        + ordinal + ".0" + values + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        };
    }

    private LiveViewWindow window() {
        final LiveViewWindow window = instance().getAnchorWindow();
        Assert.assertNotNull("the anchored view must have built its window", window);
        return window;
    }

    /**
     * One durable shape, with everything a lifecycle case needs to drive it: how to
     * create it, how to append a row, and what a from-base recompute of it looks like.
     */
    private abstract class Shape {
        static final String FRAME = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        // A bounded RANGE frame keeps the live rows behind its scalar tail in its image,
        // so it is neither fixed width nor inlineable and stays page-backed - which is
        // what gives a case a data segment to compact or to tear.
        static final String RING_PROJECTION = "sum(amt_txn) over (partition by cod_acct_no order by created_at "
                + "range between '30' second preceding and current row) as ring";

        abstract void assertMatchesRecompute() throws Exception;

        abstract void createView() throws Exception;

        abstract void insert(LiveViewRefreshJob job, String timestamp, String account, int ordinal) throws Exception;

        /**
         * Compares the shape's own outputs against a from-base recompute of the same
         * window. ANCHOR is live-view syntax, so the daily bucket is written out as an
         * ordinary partition term.
         *
         * @param recomputed the window calls the base side computes, each aliased
         * @param selected   the same aliases read off the view, so a shape whose view
         *                   names more columns than a case compares can rename them
         */
        final void assertMatchesRecompute(String recomputed, String selected, boolean withRingResidual)
                throws Exception {
            final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(select created_at, cod_acct_no, " + recomputed
                            + (withRingResidual ? ", " + RING_PROJECTION : "")
                            + " from (select *, " + bucket + " as bucket from tx)) order by 2, 1",
                    "(select created_at, cod_acct_no, " + selected
                            + (withRingResidual ? ", ring" : "")
                            + " from lv) order by 2, 1",
                    LOG,
                    true
            );
        }
    }
}
