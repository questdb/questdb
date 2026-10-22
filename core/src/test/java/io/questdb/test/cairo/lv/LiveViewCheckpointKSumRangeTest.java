/*******************************************************************************
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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointRepairSession;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSink;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code ksum} over a bounded, partitioned RANGE frame persists each partition's ring
 * through the chunked ring codec: the buffered {@code (timestamp, value)} pairs, oldest
 * first, behind a two-word scalar carrying the exact Kahan pair {@code (sum, compensation)}
 * and a frame size carrying the in-frame count. Restore takes the pair verbatim instead of
 * re-running the compensated sum over the ring, so a restored partition continues bit for
 * bit where the runtime left it.
 * <p>
 * {@link #testBoundedRangeFramesDeclareRingCheckpoint()} pins the disposition per frame
 * shape. {@link #testBoundedRangeSurvivesRestartAndRepair()} proves the durable ring
 * round-trips: the views restore from the timeline after a restart, share chunk pages across
 * roots, and survive out-of-order corrections; the last one resumes every view from a sealed
 * root, so the repair itself restores the ring. {@link
 * #testBoundedRangeRepairCopiesStateAsideWithoutIsolatedRuntime()} drives the in-RAM form
 * through the repair's copy-aside overlay. {@link
 * #testBoundedRangeRestoresExactValuesAcrossRestart()} pins restored values against
 * hand-computed ones, including a ring that restores a pair the frame has not reached yet.
 * {@link #testBoundedRangeContinuesBitExactlyAcrossRestart()} proves the carried
 * compensation term: over values whose Kahan compensation is nonzero at the seal, the view
 * continues after a restart to the plain query's exact bits. {@link
 * #testBoundedRangeRestoreCarriesKahanPairAndRejectsCorruptRing()} pins both restore paths
 * and every corrupt-ring rejection directly.
 * <p>
 * The codec covers a bounded frame start only. The rejection cases pin that every
 * unbounded-start spelling stays out of a live view: the CREATE-time gates refuse the
 * unanchored ones, and the class itself declines the anchored one those gates let through.
 */
public class LiveViewCheckpointKSumRangeTest extends AbstractLiveViewTest {

    private static final String CLASS_NAME = "KSumOverPartitionRangeFrameFunction";
    private static final int COMMITS = 6;
    // Commits the copy-aside case lays down before its correction: enough history above
    // DEEP_SECOND_A that every narrow frame's influence stops short of the head, which is what
    // makes its repair converge.
    private static final int COPY_ASIDE_COMMITS = 9;
    // Where the deep corrections land: above the widest frame's width, so no frame's reach
    // runs down into the views' START FROM floor, and below the restart's head.
    private static final int DEEP_SECOND_A = 217;
    private static final int DEEP_SECOND_B = 233;
    // Wide enough that a partition's ring holds more than twice
    // LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS pairs, which is what lets a seal keep
    // the previous root's chunk and reference it instead of re-imaging the ring.
    private static final String FRAME_CURRENT = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '200' SECOND PRECEDING AND CURRENT ROW";
    // EXCLUDE CURRENT ROW folds into a high bound one tick below the current row, so the
    // current pair waits in the ring outside the frame.
    private static final String FRAME_EXCLUDED = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '6' SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW";
    // The issue's short form of N PRECEDING AND CURRENT ROW.
    private static final String FRAME_ISSUE = "PARTITION BY sym ORDER BY ts RANGE '1' MINUTE PRECEDING";
    // A high bound below the current row: the ring holds pairs the frame has not reached yet.
    private static final String FRAME_LAGGED = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING";
    private static final String[] NARROW_VIEWS = {"lv_lag", "lv_excl", "lv_issue"};
    // Distinct ascending seconds per key per commit.
    private static final int ROWS_PER_COMMIT = 40;
    // Seconds in the short commit the near-head correction lands in.
    private static final int TAIL_ROWS = 3;
    private static final String[] VIEWS = {"lv_cur", "lv_lag", "lv_excl", "lv_issue"};

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so the
        // checkpoint timeline a restart or a repair restores from is deep.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testAnchoredUnboundedRangeStaysRejected() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // EXCLUDE CURRENT ROW keeps the ANCHOR (the anchor validator reads the bounds, not
            // the exclusion) and the folded high bound sends the call to the bounded RANGE class
            // with an unbounded start. The finite-influence gate lets an anchored window through,
            // so the class itself has to decline: its ring codec covers a bounded start only.
            final String frame = "PARTITION BY sym ORDER BY ts "
                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW";
            final CompiledFunction compiled = compileSoleWindowFunction(frame, true);
            Assert.assertEquals(CLASS_NAME, compiled.className);
            Assert.assertFalse("ksum over an unbounded start must decline", compiled.supportsCheckpointState);
            Assert.assertFalse("ksum over an unbounded start must decline", compiled.supportsCheckpointRingState);
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, sym, ksum(px) OVER w AS k FROM fx "
                        + "WINDOW w AS (" + frame + " ANCHOR DAILY '00:00')");
                Assert.fail("an anchored unbounded-start RANGE ksum must be refused at CREATE");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "live view select cannot use window function ksum(); incremental snapshot is not supported for this function yet"
                );
            }
        });
    }

    @Test
    public void testBoundedRangeContinuesBitExactlyAcrossRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // Three frames over values mixing magnitudes 1e16 and 3e15 with fractions, so the
            // Kahan compensation is nonzero when the root seals. A restore that dropped the
            // compensation term diverges on the narrow lagged frame; one that re-ran the
            // compensated sum over the ring diverges on the two 10-second frames, whose running
            // sums carry the rounding of pairs that already expired.
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND CURRENT ROW) AS k1,
                           ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING) AS k2,
                           ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '6' SECOND PRECEDING AND '1' SECOND PRECEDING) AS k3
                    FROM fx""");
            final String plain = """
                    (SELECT ts, sym,
                            ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND CURRENT ROW) AS k1,
                            ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING) AS k2,
                            ksum(px) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '6' SECOND PRECEDING AND '1' SECOND PRECEDING) AS k3
                    FROM fx) ORDER BY sym, ts""";

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertKahanSeconds(0, 20);
                drainJob(job);
                drainWalQueue();
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain("SELECT ts, sym, k1, k2, k3 FROM lv ORDER BY sym, ts", plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertKahanSeconds(20, 20);
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                // The printed form is the shortest text that round-trips a double, so equal
                // text is equal bits: every row after the restart runs on the carried Kahan pair.
                assertViewPrintsLikePlain("SELECT ts, sym, k1, k2, k3 FROM lv ORDER BY sym, ts", plain);
            }
        });
    }

    @Test
    public void testBoundedRangeFramesDeclareRingCheckpoint() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String[] frames = {FRAME_CURRENT, FRAME_LAGGED, FRAME_EXCLUDED, FRAME_ISSUE};
            for (String frame : frames) {
                assertRingDisposition(frame);
            }
            // Outside a live view compile the same class declines: the codec needs the
            // tombstone slot and the key layout copy only a live view compile builds.
            final CompiledFunction plain = compileSoleWindowFunction(FRAME_CURRENT, false);
            Assert.assertEquals(CLASS_NAME, plain.className);
            Assert.assertFalse("ksum outside a live view compile must not claim checkpoint state", plain.supportsCheckpointState);
            Assert.assertFalse("ksum outside a live view compile must not claim a ring", plain.supportsCheckpointRingState);
        });
    }

    @Test
    public void testBoundedRangeRejectsUnboundedFrameStart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // Compiles to the bounded-RANGE class with an unbounded low bound. A trailing high
            // bound reads as a bounded frame, so the finite-influence gate is what turns it away.
            assertUnboundedFrameStartRejected(
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND '3' SECOND PRECEDING",
                    "frame starting at UNBOUNDED PRECEDING"
            );
            // CURRENT ROW folded one tick down by EXCLUDE CURRENT ROW reads as the default
            // frame, so without an ANCHOR the bare-unbounded-window rule refuses it first.
            assertUnboundedFrameStartRejected(
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW",
                    "live view unbounded window must have an ANCHOR clause"
            );
        });
    }

    @Test
    public void testBoundedRangeRejectsUnsupportedPartitionKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ux (ts TIMESTAMP, u UUID, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, u, ksum(px) OVER (PARTITION BY u ORDER BY ts "
                        + "RANGE BETWEEN '3' SECOND PRECEDING AND CURRENT ROW) AS k FROM ux");
                Assert.fail("a UUID partition key must keep ksum out of a live view");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "live view select cannot use window function ksum(); incremental snapshot is not supported for this function yet"
                );
            }
        });
    }

    @Test
    public void testBoundedRangeRepairCopiesStateAsideWithoutIsolatedRuntime() throws Exception {
        // With the isolated repair runtime declined, a repair whose influence converges below
        // the frontier replays on the primary runtime and copies every partition's state aside
        // first, through the in-RAM freezeCheckpointState / restoreCheckpointState pair. A
        // one-row replay budget parks the repair after the copy, which is what lets the test
        // see it was taken. The narrow frames are the ones whose localized rebuild beats a
        // resume from the root below the change; the wide one resumes instead, through the
        // durable ring, which testBoundedRangeSurvivesRestartAndRepair covers.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        assertMemoryLeak(() -> {
            createBase();
            createViews(NARROW_VIEWS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COPY_ASIDE_COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
                commitOutOfOrderHalfSecond(null, DEEP_SECOND_A, "a", "4242");
                for (String view : NARROW_VIEWS) {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(view, instance);
                    driveUntilRepairParks(job, instance);
                    final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
                    Assert.assertTrue(
                            view + ": a converging repair on the primary must have copied its state aside",
                            parked.getOverlay().isCaptured()
                    );
                }
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1_000_000);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);
                assertRepairLocalized(NARROW_VIEWS);

                // The forward drain after the repair runs on whatever the copy-aside put back,
                // so a state image that lost a pair or the running sum shows up here.
                for (int commit = COPY_ASIDE_COMMITS + 1; commit <= COPY_ASIDE_COMMITS + 2; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);
            }
        });
    }

    @Test
    public void testBoundedRangeRestoreCarriesKahanPairAndRejectsCorruptRing() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // A lagged frame, so the ring can legitimately hold a pair the frame has not reached.
            final String sql = "SELECT ts, sym, ksum(px) OVER (" + FRAME_LAGGED + ") AS k FROM fx";
            sqlExecutionContext.setLiveViewCompile(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext);
                    MemoryCARWImpl page = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                sqlExecutionContext.setLiveViewCompile(false);
                final WindowFunction function = soleWindowFunction(factory, sql);
                Assert.assertEquals(CLASS_NAME, function.getClass().getSimpleName());
                Assert.assertEquals(1, function.getCheckpointKeyColumnTypes().getColumnCount());
                // A live view compile keys a SYMBOL partition by its resolved string.
                Assert.assertEquals(ColumnType.STRING, function.getCheckpointKeyColumnTypes().getColumnType(0));
                // The partition map opens lazily; the cursor would reopen it.
                Assert.assertTrue(function instanceof Reopenable);
                ((Reopenable) function).reopen();
                function.onCheckpointRestoreBegin();
                final Map map = function.getPartitionMap();
                final StubRingSource source = new StubRingSource();
                final int initialCapacity = configuration.getSqlWindowInitialRangeBufferSize();

                // A Kahan pair no compensated sum over these rows would produce, so a restore
                // that re-ran the sum instead of taking the carried pair shows up as different
                // bits. Two in-frame pairs and one the frame has not reached yet.
                final double carriedSum = 42.5;
                final double carriedCompensation = -1.5e-15;
                source.of(3, 2, carriedSum, carriedCompensation)
                        .row(1_000_000, 0.1)
                        .row(2_000_000, 0.2)
                        .row(9_000_000, 1_000.0);
                MapValue value = newValue(map, "a");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, carriedSum, carriedCompensation, 2, 3, initialCapacity);

                // The durable form re-freezes to the words and rows it restored from.
                final CapturingRingSink ringSink = new CapturingRingSink();
                function.freezeCheckpointRingState(ringSink, value);
                ringSink.assertCaptured(source);

                // The in-RAM form carries the same words: (sum, compensation, frameSize, size),
                // then the pairs. Freeze it, restore it into another partition, and the copy
                // re-freezes to the same durable stream.
                final LiveViewStatePageWriter writer = new LiveViewStatePageWriter();
                final long length = writer.of(page).freeze(function, value);
                Assert.assertEquals(4 * Long.BYTES + 3 * (Long.BYTES + Double.BYTES), length);
                final LiveViewStatePageReader reader = new LiveViewStatePageReader().of(page, writer.getPageStart(), length);
                // The map's value is a flyweight; `value` is not read past this point.
                final MapValue copy = newValue(map, "a-copy");
                Assert.assertEquals(length, function.restoreCheckpointState(reader, 0, copy));
                assertRestoredState(copy, carriedSum, carriedCompensation, 2, 3, initialCapacity);
                ringSink.clear();
                function.freezeCheckpointRingState(ringSink, copy);
                ringSink.assertCaptured(source);

                // An empty frame over a non-empty ring keeps its pair as carried too.
                source.of(1, 0, 0.0, 0.0).row(1_000_000, 5.0);
                value = newValue(map, "b");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, 0.0, 0.0, 0, 1, initialCapacity);

                // The in-RAM path validates the frame size the same way.
                final long badStart = page.getAppendOffset();
                page.putDouble(1.0);
                page.putDouble(0.0);
                page.putLong(2); // frameSize past the single buffered pair
                page.putLong(1);
                page.putLong(1_000_000);
                page.putDouble(1.0);
                final long badLength = page.getAppendOffset() - badStart;
                try {
                    function.restoreCheckpointState(new LiveViewStatePageReader().of(page, badStart, badLength), 0, newValue(map, "c0"));
                    Assert.fail("expected the in-RAM image's frame size to be rejected");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCritical());
                    TestUtils.assertContains(e.getFlyweightMessage(), "live view checkpoint ksum RANGE frame size out of range");
                }

                // The shared reader checks only the frame size's sign; the class refuses a
                // frame that reaches past the ring, and a negative one.
                assertCorruptRingRejected(function, map, "c1", source.of(1, 2, 1.0, 0.0).row(1_000_000, 1.0), "frame size out of range");
                assertCorruptRingRejected(function, map, "c2", source.of(1, -1, 1.0, 0.0).row(1_000_000, 1.0), "frame size out of range");
                // computeNext buffers finite values only, so a non-finite one is a corrupt page.
                assertCorruptRingRejected(function, map, "c3", source.of(1, 1, 1.0, 0.0).row(1_000_000, Double.NaN), "ring value is not finite");
                assertCorruptRingRejected(
                        function, map, "c4", source.of(1, 1, 1.0, 0.0).row(1_000_000, Double.NEGATIVE_INFINITY), "ring value is not finite"
                );
                // The replay must deliver exactly the rows the root declares.
                assertCorruptRingRejected(function, map, "c5", source.of(2, 1, 1.0, 0.0).row(1_000_000, 1.0), "ring row count mismatch");
                // A replay longer than the slab sized from the declared row count would write
                // past it.
                source.of(0, 0, 0.0, 0.0);
                for (int i = 0; i <= initialCapacity; i++) {
                    source.row(1_000_000L * (i + 1), 1.0);
                }
                assertCorruptRingRejected(function, map, "c6", source, "ring overflows its slab");
            } finally {
                sqlExecutionContext.setLiveViewCompile(false);
            }
        });
    }

    @Test
    public void testBoundedRangeRestoresExactValuesAcrossRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // The issue's short form of the frame.
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s START FROM BEGINNING AS "
                    + "SELECT ts, sym, " + issueProjection() + " FROM fx");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           ksum(px) OVER w1 AS k1,
                           ksum(px) OVER w2 AS k2
                    FROM fx
                    WINDOW w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' SECOND PRECEDING AND CURRENT ROW),
                           w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '3' SECOND PRECEDING AND '1' SECOND PRECEDING)""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0, 1.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 10.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'a', 3.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'b', 14.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', null, 1.0),
                        ('2026-01-01T00:00:04.000000Z', 'a', 7.0, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveSeedToCompletion(job, "lv");
                driveSeedToCompletion(job, "lv2");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, k1, k2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tk1\tk2
                                2026-01-01T00:00:00.000000Z\ta\t1.0\tnull
                                2026-01-01T00:00:01.000000Z\ta\t4.0\t1.0
                                2026-01-01T00:00:02.000000Z\ta\t4.0\t4.0
                                2026-01-01T00:00:04.000000Z\ta\t7.0\t3.0
                                2026-01-01T00:00:00.000000Z\tb\t10.0\tnull
                                2026-01-01T00:00:01.000000Z\tb\t24.0\t10.0
                                """);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // a's w1 ring restores holding 7 (04s), which 05s pairs with 5. a's w2 ring
                // restores holding 3 (01s) in its frame and 7 (04s) waiting outside it: 05s
                // expires the first and admits the second, so a ring that lost the waiting pair
                // reads an empty frame there. b's rings restore holding 10 and 14, which 05s
                // expires in both frames.
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:05.000000Z', 'a', 5.0, 1.0),
                        ('2026-01-01T00:00:05.000000Z', 'b', 20.0, 1.0),
                        ('2026-01-01T00:00:07.000000Z', 'a', 9.0, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertRestoredFromTimeline("lv2");
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, k1, k2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tk1\tk2
                                2026-01-01T00:00:00.000000Z\ta\t1.0\tnull
                                2026-01-01T00:00:01.000000Z\ta\t4.0\t1.0
                                2026-01-01T00:00:02.000000Z\ta\t4.0\t4.0
                                2026-01-01T00:00:04.000000Z\ta\t7.0\t3.0
                                2026-01-01T00:00:05.000000Z\ta\t12.0\t7.0
                                2026-01-01T00:00:07.000000Z\ta\t14.0\t12.0
                                2026-01-01T00:00:00.000000Z\tb\t10.0\tnull
                                2026-01-01T00:00:01.000000Z\tb\t24.0\t10.0
                                2026-01-01T00:00:05.000000Z\tb\t20.0\tnull
                                """);
                assertViewMatchesRecompute("lv2", issueProjection(), "k");
            }
        });
    }

    @Test
    public void testBoundedRangeSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createViews(VIEWS);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                // The ring codec's point: a seal reuses the pages the previous root wrote for
                // the pairs both rings hold, so some data segment has more than one referrer.
                assertSomeSegmentShared("lv_cur");
            }

            // A restart rebuilds each view's runtime RANGE state from the timeline through
            // restoreCheckpointRingState, so a following refresh that matches a fresh recompute
            // proves the ring round-tripped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                for (String view : VIEWS) {
                    assertRestoredFromTimeline(view);
                }
                assertViewsMatchRecompute(VIEWS);

                // Out-of-order rows deep in history at fresh sub-second timestamps (no
                // designated-timestamp tie with an existing row) force the repair path. The
                // second one is a NULL, which the ring never stores.
                commitOutOfOrderHalfSecond(job, DEEP_SECOND_A, "a", "4242");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairLocalized(VIEWS);

                commitOutOfOrderHalfSecond(job, DEEP_SECOND_B, "b", "null");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairLocalized(VIEWS);

                // A short tail, then a correction just above the root sealed below it: replaying
                // the tail from that root reads fewer base rows than rebuilding the frame's
                // reach, so the repair resumes from the root, restoring every partition's ring
                // from the timeline before it replays.
                final int tailSecond = (COMMITS + 3) * ROWS_PER_COMMIT;
                commitSeconds(job, tailSecond, TAIL_ROWS);
                driveRefreshToQuiescence(job);
                commitOutOfOrderHalfSecond(job, tailSecond, "a", "77");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                for (String view : VIEWS) {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(view, instance);
                    Assert.assertEquals(
                            view + " must resume from a sealed root",
                            "resume from anchor",
                            LiveViewCheckpointRepairPlan.dispositionName(
                                    instance.getCheckpointRepairLastDisposition(),
                                    instance.getCheckpointRepairLastDenialReason()
                            )
                    );
                }

                // And the in-order path keeps going on the state the repairs left behind.
                commitSeconds(job, tailSecond + TAIL_ROWS, ROWS_PER_COMMIT);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
            }
        });
    }

    private static void assertCorruptRingRejected(
            WindowFunction function,
            Map map,
            String key,
            StubRingSource source,
            String expectedMessage
    ) {
        final MapValue value = newValue(map, key);
        try {
            function.restoreCheckpointRingState(source, value);
            Assert.fail("expected a corrupt-ring reject: " + expectedMessage);
        } catch (CairoException e) {
            Assert.assertTrue(e.isCritical());
            TestUtils.assertContains(e.getFlyweightMessage(), "live view checkpoint ksum RANGE " + expectedMessage);
        }
    }

    private static void assertRestoredState(
            MapValue value,
            double sum,
            double compensation,
            long frameSize,
            long size,
            int initialCapacity
    ) {
        Assert.assertEquals(Double.doubleToRawLongBits(sum), Double.doubleToRawLongBits(value.getDouble(0)));
        Assert.assertEquals(Double.doubleToRawLongBits(compensation), Double.doubleToRawLongBits(value.getDouble(1)));
        Assert.assertEquals(frameSize, value.getLong(2));
        Assert.assertEquals(size, value.getLong(4));
        Assert.assertEquals(WindowFunction.restoredRingCapacity(size, initialCapacity), value.getLong(5));
        // Rebased to index 0.
        Assert.assertEquals(0, value.getLong(6));
        // Tombstone clear.
        Assert.assertEquals(0, value.getByte(7));
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static CompiledFunction compileSoleWindowFunction(String frame, boolean liveViewCompile) throws Exception {
        final String sql = "SELECT ts, sym, ksum(px) OVER (" + frame + ") AS k FROM fx";
        sqlExecutionContext.setLiveViewCompile(liveViewCompile);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
        ) {
            return new CompiledFunction(soleWindowFunction(factory, sql));
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static String frameOf(String view) {
        return switch (view) {
            case "lv_cur" -> FRAME_CURRENT;
            case "lv_lag" -> FRAME_LAGGED;
            case "lv_excl" -> FRAME_EXCLUDED;
            case "lv_issue" -> FRAME_ISSUE;
            default -> throw new AssertionError("no frame for view " + view);
        };
    }

    private static String issueProjection() {
        return projection(FRAME_ISSUE);
    }

    // Values for the bit-exact case: per second, 'a' mixes 1e16 with millis and 'b' mixes
    // -3e15 with cents, each with its own NULL cadence. The magnitudes leave the low-order
    // bits of each sum in the Kahan compensation term.
    private static String kahanValue(String sym, int second) {
        if ("a".equals(sym)) {
            if (second % 7 == 3) {
                return "null";
            }
            return Double.toString((second % 5 == 0 ? 1e16 : 0.0) + ((second * 7_919L) % 1_000) * 0.001);
        }
        if (second % 6 == 1) {
            return "null";
        }
        return Double.toString((second % 4 == 2 ? -3e15 : 0.0) + ((second * 104_729L) % 997) * 0.01 + 0.1);
    }

    private static MapValue newValue(Map map, String key) {
        final MapKey mapKey = map.withKey();
        mapKey.putStr(key);
        return mapKey.createValue();
    }

    private static String projection(String frame) {
        return "ksum(px) OVER (" + frame + ") AS k";
    }

    private static WindowFunction soleWindowFunction(RecordCursorFactory factory, String sql) {
        RecordCursorFactory root = factory;
        while (root instanceof QueryProgress) {
            root = root.getBaseFactory();
        }
        Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
        final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
        Assert.assertEquals(sql + ": expected exactly one window function", 1, functions.size());
        return functions.getQuick(0);
    }

    private static String timestamp(int second) {
        return String.format(
                "2026-01-%02dT%02d:%02d:%02d.000000Z",
                1 + second / 86_400,
                (second % 86_400) / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }

    private void assertRepairLocalized(String[] views) {
        for (String view : views) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
            Assert.assertNotNull(view, instance);
            // The frame-local claim is what gives the view a RANGE repair plan, and the plan
            // is what bounds the repair below by the frame's width rather than rebuilding the
            // whole view from its START FROM boundary.
            Assert.assertEquals(view, LiveViewInstance.REPAIR_PLAN_RANGE, instance.getCheckpointRepairDependencyPlans());
            final int denial = instance.getCheckpointRepairLastDenialReason();
            final String disposition = LiveViewCheckpointRepairPlan.dispositionName(
                    instance.getCheckpointRepairLastDisposition(),
                    denial
            );
            Assert.assertNotNull(view + " must have run a repair", disposition);
            Assert.assertNotEquals(
                    view + " must localize the repair [denial="
                            + LiveViewCheckpointRepairPlan.denialReasonName(denial) + ']',
                    "boundary rebuild",
                    disposition
            );
        }
    }

    private void assertRingDisposition(String frame) throws Exception {
        final String label = "ksum(px) OVER (" + frame + ")";
        final CompiledFunction compiled = compileSoleWindowFunction(frame, true);
        Assert.assertEquals(label, CLASS_NAME, compiled.className);
        Assert.assertTrue(label + " must support checkpoint state", compiled.supportsCheckpointState);
        Assert.assertTrue(label + " must persist through the chunked ring codec", compiled.supportsCheckpointRingState);
        Assert.assertEquals(label + " carries (sum, compensation) as a two-word scalar", 2, compiled.ringScalarWords);
        Assert.assertEquals(label + " stores one double per row", LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, compiled.ringValueKind);
        Assert.assertTrue(label + " must expose its ring arena", compiled.hasRingArena);
        // A ring-shaped function never declares a fixed image width.
        Assert.assertEquals(label, -1, compiled.fixedStateLength);
        Assert.assertTrue(label + " must declare frame-local state", compiled.hasFrameLocalCheckpointState);
        final LiveViewCheckpointDependency dependency = compiled.dependency;
        Assert.assertNotNull(label, dependency);
        Assert.assertTrue(label, dependency.isFiniteRange());
        Assert.assertTrue(label, dependency.hasFrameLocalState());
        Assert.assertEquals(label, NumericConvergence.FLOATING_TOLERANCE, dependency.getNumericConvergence());
    }

    private void assertSomeSegmentShared(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull(viewName, instance);
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointSegmentDirectoryReader directory =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            final int[] sharedSegments = {0};
            directory.iterateAll(entry -> {
                if (entry.referenceCount > 1) {
                    sharedSegments[0]++;
                }
            });
            Assert.assertTrue(
                    viewName + ": no data segment is referenced by more than one root",
                    sharedSegments[0] > 0
            );
        }
    }

    private void assertUnboundedFrameStartRejected(String frame, String expectedMessage) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, ksum(px) OVER (PARTITION BY sym ORDER BY ts " + frame + ") AS k FROM fx");
            Assert.fail("expected an unbounded-frame-start reject for ksum over " + frame);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            if (expectedMessage.startsWith("frame starting")) {
                Assert.assertTrue(
                        "the reject must name ksum [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "cannot use ksum()")
                );
            }
        }
    }

    private void assertViewMatchesRecompute(String viewName, String projection, String columns) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + projection + " FROM fx) ORDER BY 2, 1",
                "(SELECT ts, sym, " + columns + " FROM " + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    // Prints the plain query and asserts the view prints the same text. Unlike the
    // assertSqlCursors comparison, which allows a small tolerance on doubles, equal shortest
    // round-trip text means equal bits.
    private void assertViewPrintsLikePlain(String viewQuery, String plainQuery) throws Exception {
        printSql(plainQuery);
        final String expected = sink.toString();
        assertQuery(viewQuery)
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void assertViewsMatchRecompute(String[] views) throws Exception {
        for (String view : views) {
            assertViewMatchesRecompute(view, projection(frameOf(view)), "k");
        }
    }

    // Commits ROWS_PER_COMMIT ascending seconds and gives the refresh job a turn. Each second
    // carries a row for 'a' and 'b'; every fifteenth carries one for a NULL key too, so that
    // partition's narrow frames empty out between rows and its rings seal with nothing in
    // them. Scattered NULL values, plus a run of them for 'b' wider than the one-minute frame,
    // keep NULLs out of the ring and drive the finite count to zero across a seal. Values are
    // integers whose sums stay well under 2^53, so every compensated sum is exact whatever
    // order a frame adds and drops pairs; that keeps the recompute comparison valid after a
    // localized repair, which otherwise reaches the same value only up to floating-point
    // rounding.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        commitSeconds(job, (commit - 1) * ROWS_PER_COMMIT, ROWS_PER_COMMIT);
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering stays
    // deterministic - and, given a job, gives it a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second, String sym, String value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + ts + "', '" + sym + "', " + value + ", 1.0)");
        drainWalQueue();
        if (job != null) {
            drainJob(job);
            drainWalQueue();
        }
    }

    // Commits `count` ascending seconds from `firstSecond`; see commitDense for the data.
    private void commitSeconds(LiveViewRefreshJob job, int firstSecond, int count) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            final String valueA = second % 7 == 0 ? "null" : Integer.toString(second);
            final String valueB = second % 5 == 0 || (second >= 100 && second < 170)
                    ? "null"
                    : Integer.toString(second + 1_000);
            sql.append("('").append(ts).append("', 'a', ").append(valueA).append(", 1.0), ")
                    .append("('").append(ts).append("', 'b', ").append(valueB).append(", 1.0)");
            if (second % 15 == 0) {
                final String valueN = second % 45 == 0 ? "null" : Integer.toString(2 * second);
                sql.append(", ('").append(ts).append("', null, ").append(valueN).append(", 1.0)");
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE fx (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void createViews(String[] views) throws Exception {
        for (String view : views) {
            execute("CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM NOW AS SELECT ts, sym, "
                    + projection(frameOf(view)) + " FROM fx");
        }
    }

    private void driveUntilRepairParks(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int pass = 0; pass < 64; pass++) {
            if (instance.getSuspendedRepair() != null) {
                return;
            }
            if (!job.processNotificationsForTest()) {
                break;
            }
        }
        Assert.assertNotNull("the repair never parked on its turn budget", instance.getSuspendedRepair());
    }

    // Inserts `count` ascending seconds from `firstSecond` for 'a' and 'b' with kahanValue's
    // data, in one commit.
    private void insertKahanSeconds(int firstSecond, int count) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            sql.append("('").append(ts).append("', 'a', ").append(kahanValue("a", second)).append(", 1.0), ")
                    .append("('").append(ts).append("', 'b', ").append(kahanValue("b", second)).append(", 1.0)");
        }
        execute(sql.toString());
        drainWalQueue();
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    // Records what freezeCheckpointRingState streams, for comparison with the source a
    // restore read.
    private static class CapturingRingSink implements LiveViewCheckpointRingStateSink {
        private final LongList rows = new LongList();
        private long frameSize = -1;
        private long scalarWord0;
        private long scalarWord1;
        private int scalarWords;

        @Override
        public void putRow(long timestamp) {
            throw new AssertionError("a ksum ring streams one-word rows");
        }

        @Override
        public void putRow(long timestamp, long valueBits) {
            rows.add(timestamp);
            rows.add(valueBits);
        }

        @Override
        public void putRow(long timestamp, long hi, long lo) {
            throw new AssertionError("a ksum ring streams one-word rows");
        }

        @Override
        public void putRow(long timestamp, long hh, long hl, long lh, long ll) {
            throw new AssertionError("a ksum ring streams one-word rows");
        }

        @Override
        public void putScalarState(long scalarBits, long frameSize) {
            throw new AssertionError("a ksum ring carries a two-word scalar");
        }

        @Override
        public void putScalarState(long hi, long lo, long frameSize) {
            Assert.assertEquals("one scalar per partition", 0, scalarWords);
            Assert.assertEquals("the scalar precedes the rows", 0, rows.size());
            this.scalarWords = 2;
            this.scalarWord0 = hi;
            this.scalarWord1 = lo;
            this.frameSize = frameSize;
        }

        @Override
        public void putScalarState(long hh, long hl, long lh, long ll, long frameSize) {
            throw new AssertionError("a ksum ring carries a two-word scalar");
        }

        @Override
        public void putScalarState(long word0, long word1, long word2, long word3, long word4, long frameSize) {
            throw new AssertionError("a ksum ring carries a two-word scalar");
        }

        private void assertCaptured(StubRingSource source) {
            Assert.assertEquals(2, scalarWords);
            Assert.assertEquals(source.scalarWord0, scalarWord0);
            Assert.assertEquals(source.scalarWord1, scalarWord1);
            Assert.assertEquals(source.frameSize, frameSize);
            TestUtils.assertEquals(source.rows, rows);
        }

        private void clear() {
            rows.clear();
            frameSize = -1;
            scalarWord0 = 0;
            scalarWord1 = 0;
            scalarWords = 0;
        }
    }

    // What the assertions read off the sole window function, captured before the factory
    // closes.
    private static class CompiledFunction {
        private final String className;
        private final LiveViewCheckpointDependency dependency;
        private final int fixedStateLength;
        private final boolean hasFrameLocalCheckpointState;
        private final boolean hasRingArena;
        private final int ringScalarWords;
        private final int ringValueKind;
        private final boolean supportsCheckpointRingState;
        private final boolean supportsCheckpointState;

        private CompiledFunction(WindowFunction function) {
            this.className = function.getClass().getSimpleName();
            this.dependency = function.checkpointDependency();
            this.fixedStateLength = function.checkpointStateFixedLength();
            this.hasFrameLocalCheckpointState = function.hasFrameLocalCheckpointState();
            this.hasRingArena = function.getRingArena() != null;
            this.ringScalarWords = function.checkpointRingScalarWords();
            this.ringValueKind = function.checkpointRingValueKind();
            this.supportsCheckpointRingState = function.supportsCheckpointRingState();
            this.supportsCheckpointState = function.supportsCheckpointState();
        }
    }

    // Injects a corrupt or hand-built persisted ring straight into the restore path. It stands
    // in for the chunk reader, not for a peer: the rows it replays are ones a correct seal
    // would never write, which is the point.
    private static class StubRingSource implements LiveViewCheckpointRingStateSource {
        private final LongList rows = new LongList();
        private long frameSize;
        private long rowCount;
        private long scalarWord0;
        private long scalarWord1;

        @Override
        public void forEachRow(@NotNull RowConsumer consumer) {
            for (int i = 0, n = rows.size(); i < n; i += 2) {
                consumer.accept(rows.getQuick(i), rows.getQuick(i + 1));
            }
        }

        @Override
        public void forEachRow(@NotNull Decimal128RowConsumer consumer) {
            throw new AssertionError("a ksum ring replays one-word rows");
        }

        @Override
        public void forEachRow(@NotNull Decimal256RowConsumer consumer) {
            throw new AssertionError("a ksum ring replays one-word rows");
        }

        @Override
        public void forEachTimestamp(@NotNull TimestampConsumer consumer) {
            throw new AssertionError("a ksum ring replays one-word rows");
        }

        @Override
        public long getFrameSize() {
            return frameSize;
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }

        @Override
        public long getScalarBits() {
            return scalarWord0;
        }

        @Override
        public long getScalarWord(int index) {
            return switch (index) {
                case 0 -> scalarWord0;
                case 1 -> scalarWord1;
                default -> throw new AssertionError("a ksum ring carries two scalar words, asked for " + index);
            };
        }

        @Override
        public int getScalarWordCount() {
            return 2;
        }

        private StubRingSource of(long rowCount, long frameSize, double sum, double compensation) {
            this.rowCount = rowCount;
            this.frameSize = frameSize;
            this.scalarWord0 = Double.doubleToRawLongBits(sum);
            this.scalarWord1 = Double.doubleToRawLongBits(compensation);
            rows.clear();
            return this;
        }

        private StubRingSource row(long timestamp, double value) {
            rows.add(timestamp);
            rows.add(Double.doubleToRawLongBits(value));
            return this;
        }
    }
}
