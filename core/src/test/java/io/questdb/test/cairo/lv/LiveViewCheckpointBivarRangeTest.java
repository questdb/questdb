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
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The covariance/correlation family over a bounded, partitioned RANGE frame persists each
 * partition's ring through the chunked ring codec: the buffered {@code (timestamp, x, y)}
 * triples, oldest first, as two-word rows, behind the exact running
 * {@code (sumX, sumXX, sumY, sumYY, sumXY)} as a five-word scalar and a frame size carrying
 * the in-frame count. Restore takes the sums and the count verbatim and re-accumulates
 * nothing. One factory backs {@code covar_samp}, {@code covar_pop} and {@code corr}, so the
 * same codec covers all three.
 * <p>
 * {@link #testBoundedRangeFramesDeclareRingCheckpoint()} pins the disposition per function
 * and frame shape. {@link #testBoundedRangeSurvivesRestartAndRepair()} proves the durable
 * ring round-trips: the views restore from the timeline after a restart, share chunk pages
 * across roots, and survive out-of-order corrections, each of which resumes every view from
 * a sealed root, so the repair itself restores the ring. The class declines frame-local
 * state, so no correction takes the localized rebuild, whose warm-up would re-sum the frame
 * and drift from the plain query's running sums past the contract's floating ceiling. {@link
 * #testBoundedRangeRepairResumesOnPrimaryWithoutIsolatedRuntime()} pins that a correction
 * whose influence converges still resumes on the primary runtime rather than copying its
 * state aside, which leaves the in-RAM form to the direct restore case. {@link
 * #testBoundedRangeRestoresExactValuesAcrossRestart()} pins restored values against
 * hand-computed ones, including a ring that restores a pair the frame has not reached yet.
 * {@link #testBoundedRangeContinuesBitExactlyAcrossRestartAndRepair()} proves on tick-like
 * data that every row after a restart and after a repair that restores the ring equals the
 * plain query's bit for bit. {@link #testBoundedRangeRestoreCarriesSumsAndRejectsCorruptRing()}
 * pins the carried words and every corrupt-ring rejection on the restore path directly.
 * <p>
 * The codec covers a bounded frame start only. The rejection cases pin that every
 * unbounded-start spelling stays out of a live view: the CREATE-time gates refuse the
 * unanchored ones, and the class itself declines the anchored one those gates let through.
 */
public class LiveViewCheckpointBivarRangeTest extends AbstractLiveViewTest {

    private static final String CLASS_NAME = "BivarStatOverPartitionRangeFrameFunction";
    private static final int COMMITS = 6;
    // Commits the primary-runtime case lays down before its correction: enough history above
    // DEEP_SECOND_A that every narrow frame's influence stops short of the head, so the repair
    // would converge, and copy its state aside, if the class claimed frame-local state.
    private static final int PRIMARY_RUNTIME_COMMITS = 9;
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
    // The issue's own lv2 spelling: the short form of N PRECEDING AND CURRENT ROW.
    private static final String FRAME_ISSUE = "PARTITION BY sym ORDER BY ts RANGE '1' MINUTE PRECEDING";
    // A high bound below the current row: the ring holds pairs the frame has not reached yet.
    private static final String FRAME_LAGGED = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING";
    private static final String[] FUNCTIONS = {"covar_samp", "covar_pop", "corr"};
    // The issue's lv2 view, verbatim.
    private static final String ISSUE_PROJECTION = "corr(px, qty) OVER (" + FRAME_ISSUE + ") AS c";
    private static final String[] NARROW_VIEWS = {"lv_lag", "lv_excl", "lv_issue"};
    // Distinct ascending seconds per key per commit.
    private static final int ROWS_PER_COMMIT = 40;
    // Seconds in the short commit the near-head correction lands in.
    private static final int TAIL_ROWS = 3;
    // Seconds per commit in the bit-exact case.
    private static final int TICK_COMMIT_SECONDS = 60;
    // The current-row frame of the bit-exact case: narrow enough that pairs expire from the
    // first minute on, so the running sums carry the rounding of many adds and drops.
    private static final String TICK_FRAME_CURRENT = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND CURRENT ROW";
    // All three functions over all four frame shapes: frameIncludesCurrentValue (CURRENT ROW and
    // the issue's short form) and not (N PRECEDING AND M PRECEDING, EXCLUDE CURRENT ROW).
    private static final String TICK_PROJECTION = "covar_samp(px, qty) OVER (" + TICK_FRAME_CURRENT + ") AS c0, "
            + "corr(px, qty) OVER (" + TICK_FRAME_CURRENT + ") AS c1, "
            + "covar_pop(px, qty) OVER (" + FRAME_LAGGED + ") AS c2, "
            + "corr(px, qty) OVER (" + FRAME_LAGGED + ") AS c3, "
            + "covar_samp(px, qty) OVER (" + FRAME_EXCLUDED + ") AS c4, "
            + "covar_pop(px, qty) OVER (" + FRAME_ISSUE + ") AS c5, "
            + "corr(px, qty) OVER (" + FRAME_ISSUE + ") AS c6";
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
            for (String fn : FUNCTIONS) {
                final LiveViewCheckpointCompiledFunction compiled =
                        LiveViewCheckpointCompiledFunction.compile(engine, sqlExecutionContext, fn + "(px, qty)", frame, true);
                Assert.assertEquals(fn, CLASS_NAME, compiled.className);
                Assert.assertFalse(fn + " over an unbounded start must decline", compiled.supportsCheckpointState);
                Assert.assertFalse(fn + " over an unbounded start must decline", compiled.supportsCheckpointRingState);
                try {
                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                            + "SELECT ts, sym, " + fn + "(px, qty) OVER w AS v FROM fx "
                            + "WINDOW w AS (" + frame + " ANCHOR DAILY '00:00')");
                    Assert.fail("an anchored unbounded-start RANGE " + fn + " must be refused at CREATE");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "live view select cannot use window function " + fn
                                    + "(); incremental snapshot is not supported for this function yet"
                    );
                }
            }
        });
    }

    @Test
    public void testBoundedRangeContinuesBitExactlyAcrossRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // Tick-like data: prices on a one-cent random walk near 65_000 and 3_100 against
            // sizes in thousandths. The products and squares leave rounding residue in the five
            // sums as pairs enter and expire, and the covariance and correlation formulas
            // subtract nearly equal sums, which amplifies it. A restore that re-summed the ring
            // instead of taking the carried sums emits different bits from the plain query here;
            // integer data would hide that, as it sums to the same doubles in every order.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + TICK_PROJECTION + " FROM fx");
            final String plain = "(SELECT ts, sym, " + TICK_PROJECTION + " FROM fx) ORDER BY sym, ts";
            final String view = "SELECT ts, sym, c0, c1, c2, c3, c4, c5, c6 FROM lv ORDER BY sym, ts";
            final Rnd rnd = new Rnd(4242, 42);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 0; commit < 5; commit++) {
                    commitTicks(job, rnd, prices, second, TICK_COMMIT_SECONDS);
                    second += TICK_COMMIT_SECONDS;
                    if (commit == 0) {
                        driveSeedToCompletion(job, "lv");
                    }
                    driveRefreshToQuiescence(job);
                }
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 0; commit < 3; commit++) {
                    commitTicks(job, rnd, prices, second, TICK_COMMIT_SECONDS);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                // The printed form is the shortest text that round-trips a double, so equal text
                // is equal bits: every row after the restart runs on the carried sums.
                assertViewPrintsLikePlain(view, plain);

                // A short tail, then a late row just above the root sealed below it. The class
                // declines the localized rebuild, so the repair resumes from that root and
                // restores every partition's ring from the timeline before it replays. That root
                // holds the state forward processing reached, so the repaired rows match the
                // plain query bit for bit as well.
                final int tailSecond = second;
                commitTicks(job, rnd, prices, second, TAIL_ROWS);
                second += TAIL_ROWS;
                driveRefreshToQuiescence(job);
                commitOutOfOrderHalfSecond(job, tailSecond, "btc", "65001.37", "0.731");
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "the late row must resume the view from a sealed root",
                        "resume from anchor",
                        LiveViewCheckpointRepairPlan.dispositionName(
                                instance.getCheckpointRepairLastDisposition(),
                                instance.getCheckpointRepairLastDenialReason()
                        )
                );
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);

                // And the in-order path keeps going on the state the repair left behind.
                commitTicks(job, rnd, prices, second, TICK_COMMIT_SECONDS);
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);
            }
        });
    }

    @Test
    public void testBoundedRangeFramesDeclareRingCheckpoint() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String[] frames = {FRAME_CURRENT, FRAME_LAGGED, FRAME_EXCLUDED, FRAME_ISSUE};
            for (String fn : FUNCTIONS) {
                for (String frame : frames) {
                    assertRingDisposition(fn + "(px, qty)", frame);
                }
                // Outside a live view compile the same class declines: the codec needs the
                // tombstone slot and the key layout copy only a live view compile builds.
                final LiveViewCheckpointCompiledFunction plain =
                        LiveViewCheckpointCompiledFunction.compile(engine, sqlExecutionContext, fn + "(px, qty)", FRAME_CURRENT, false);
                Assert.assertEquals(fn, CLASS_NAME, plain.className);
                Assert.assertFalse(fn + " outside a live view compile must not claim checkpoint state", plain.supportsCheckpointState);
                Assert.assertFalse(fn + " outside a live view compile must not claim a ring", plain.supportsCheckpointRingState);
            }
        });
    }

    @Test
    public void testBoundedRangeRejectsUnboundedFrameStart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            for (String fn : FUNCTIONS) {
                // Compiles to the bounded-RANGE class with an unbounded low bound. A trailing
                // high bound reads as a bounded frame, so the finite-influence gate is what
                // turns it away.
                assertUnboundedFrameStartRejected(
                        fn,
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND '3' SECOND PRECEDING",
                        "frame starting at UNBOUNDED PRECEDING"
                );
                // CURRENT ROW folded one tick down by EXCLUDE CURRENT ROW reads as the default
                // frame, so without an ANCHOR the bare-unbounded-window rule refuses it first.
                assertUnboundedFrameStartRejected(
                        fn,
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW",
                        "live view unbounded window must have an ANCHOR clause"
                );
            }
        });
    }

    @Test
    public void testBoundedRangeRejectsUnsupportedPartitionKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ux (ts TIMESTAMP, u UUID, px DOUBLE, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String fn : FUNCTIONS) {
                try {
                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                            + "SELECT ts, u, " + fn + "(px, qty) OVER (PARTITION BY u ORDER BY ts "
                            + "RANGE BETWEEN '3' SECOND PRECEDING AND CURRENT ROW) AS v FROM ux");
                    Assert.fail("a UUID partition key must keep " + fn + " out of a live view");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "live view select cannot use window function " + fn
                                    + "(); incremental snapshot is not supported for this function yet"
                    );
                }
            }
        });
    }

    @Test
    public void testBoundedRangeRepairResumesOnPrimaryWithoutIsolatedRuntime() throws Exception {
        // With the isolated repair runtime declined, a localized repair whose influence
        // converges below the frontier replays on the primary runtime after copying every
        // partition's state aside. The narrow frames' influence converges here, but the class
        // declines frame-local state, so the view has no localized plan: the correction resumes
        // from the root below it on the primary runtime, restoring the durable ring, and
        // nothing is copied aside or replayed in an isolated runtime.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        assertMemoryLeak(() -> {
            createBase();
            createViews(NARROW_VIEWS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= PRIMARY_RUNTIME_COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);

                commitOutOfOrderHalfSecond(job, DEEP_SECOND_A, "a", "4242", "17");
                driveRefreshToQuiescence(job);
                for (String view : NARROW_VIEWS) {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(view, instance);
                    Assert.assertNull(view + ": the repair must not build an isolated runtime", instance.getRepairRuntime());
                }
                assertViewsMatchRecompute(NARROW_VIEWS);
                assertRepairResumedFromRoot(NARROW_VIEWS);

                // The forward drain after the repair runs on the state the resume restored, so a
                // ring that lost a pair or a sum shows up here.
                for (int commit = PRIMARY_RUNTIME_COMMITS + 1; commit <= PRIMARY_RUNTIME_COMMITS + 2; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);
            }
        });
    }

    @Test
    public void testBoundedRangeRestoreCarriesSumsAndRejectsCorruptRing() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // A lagged frame, so the ring can legitimately hold a pair the frame has not reached.
            final String sql = "SELECT ts, sym, covar_pop(px, qty) OVER (" + FRAME_LAGGED + ") AS v FROM fx";
            sqlExecutionContext.setLiveViewCompile(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext);
                    MemoryCARWImpl page = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                sqlExecutionContext.setLiveViewCompile(false);
                final WindowFunction function = LiveViewCheckpointCompiledFunction.soleWindowFunction(factory, sql);
                Assert.assertEquals(CLASS_NAME, function.getClass().getSimpleName());
                Assert.assertEquals(5, function.checkpointRingScalarWords());
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

                // Sums no summation over these pairs would produce, so a restore that re-summed
                // the ring instead of taking the carried words shows up as different bits. Two
                // in-frame pairs and one the frame has not reached yet: the count comes from the
                // frame size, not from the ring's length.
                final double[] carried = {42.5, 1_234.25, -7.125, 99.0625, -3.5};
                source.of(3, 2, carried)
                        .row(1_000_000, 0.1, 0.2)
                        .row(2_000_000, 0.3, -0.7)
                        .row(9_000_000, 1_000.0, -1_000.0);
                MapValue value = newValue(map, "a");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, carried, 2, 3, initialCapacity);

                // The durable form re-freezes to the words and rows it restored from.
                final CapturingRingSink ringSink = new CapturingRingSink();
                function.freezeCheckpointRingState(ringSink, value);
                ringSink.assertCaptured(source);

                // The in-RAM form carries the same words: (sumX, sumXX, sumY, sumYY, sumXY,
                // count, size), then the triples. Freeze it, restore it into another partition,
                // and the copy re-freezes to the same durable stream.
                final LiveViewStatePageWriter writer = new LiveViewStatePageWriter();
                final long length = writer.of(page).freeze(function, value);
                Assert.assertEquals(7 * Long.BYTES + 3 * (Long.BYTES + 2 * Double.BYTES), length);
                final LiveViewStatePageReader reader = new LiveViewStatePageReader().of(page, writer.getPageStart(), length);
                // The map's value is a flyweight; `value` is not read past this point.
                final MapValue copy = newValue(map, "a-copy");
                Assert.assertEquals(length, function.restoreCheckpointState(reader, 0, copy));
                assertRestoredState(copy, carried, 2, 3, initialCapacity);
                ringSink.clear();
                function.freezeCheckpointRingState(ringSink, copy);
                ringSink.assertCaptured(source);

                // An empty frame over a non-empty ring keeps its pair and its sums as carried.
                final double[] zeros = new double[5];
                source.of(1, 0, zeros).row(1_000_000, 5.0, 7.0);
                value = newValue(map, "b");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, zeros, 0, 1, initialCapacity);

                // A runtime sum can legitimately overflow: a pair of 1e200 squares to +Inf, and
                // expiring it leaves Inf - Inf = NaN behind. The plain query carries the same
                // bits, so restore takes non-finite sums verbatim rather than refusing them.
                final double[] overflowed = {1e200, Double.POSITIVE_INFINITY, 1.0, 1.0, 1e200};
                source.of(1, 1, overflowed).row(1_000_000, 1e200, 1.0);
                value = newValue(map, "inf");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, overflowed, 1, 1, initialCapacity);
                final double[] expired = {0.0, Double.NaN, 0.0, 0.0, Double.NaN};
                source.of(0, 0, expired);
                value = newValue(map, "nan");
                function.restoreCheckpointRingState(source, value);
                assertRestoredState(value, expired, 0, 0, initialCapacity);

                // The in-RAM path validates the frame size the same way.
                final long badStart = page.getAppendOffset();
                for (int i = 0; i < 5; i++) {
                    page.putDouble(1.0);
                }
                page.putLong(2); // frameSize past the single buffered pair
                page.putLong(1);
                page.putLong(1_000_000);
                page.putDouble(1.0);
                page.putDouble(1.0);
                final long badLength = page.getAppendOffset() - badStart;
                try {
                    function.restoreCheckpointState(new LiveViewStatePageReader().of(page, badStart, badLength), 0, newValue(map, "c0"));
                    Assert.fail("expected the in-RAM image's frame size to be rejected");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCritical());
                    TestUtils.assertContains(e.getFlyweightMessage(), "live view checkpoint bivariate RANGE frame size out of range");
                }

                // The shared reader checks only the frame size's sign; the class refuses a
                // frame that reaches past the ring, and a negative one.
                final double[] ones = {1.0, 1.0, 1.0, 1.0, 1.0};
                assertCorruptRingRejected(function, map, "c1", source.of(1, 2, ones).row(1_000_000, 1.0, 1.0), "frame size out of range");
                assertCorruptRingRejected(function, map, "c2", source.of(1, -1, ones).row(1_000_000, 1.0, 1.0), "frame size out of range");
                // computeNext buffers finite pairs only, so a non-finite half is a corrupt page.
                assertCorruptRingRejected(function, map, "c3", source.of(1, 1, ones).row(1_000_000, Double.NaN, 1.0), "ring pair is not finite");
                assertCorruptRingRejected(
                        function, map, "c4", source.of(1, 1, ones).row(1_000_000, 1.0, Double.POSITIVE_INFINITY), "ring pair is not finite"
                );
                // The replay must deliver exactly the rows the root declares.
                assertCorruptRingRejected(
                        function, map, "c5", source.of(2, 1, ones).row(1_000_000, 1.0, 1.0), "ring row count mismatch"
                );
                // A replay longer than the slab sized from the declared row count would write
                // past it.
                source.of(0, 0, zeros);
                for (int i = 0; i <= initialCapacity; i++) {
                    source.row(1_000_000L * (i + 1), 1.0, 1.0);
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
            // The issue's lv2 view.
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s START FROM BEGINNING AS "
                    + "SELECT ts, sym, " + ISSUE_PROJECTION + " FROM fx");
            final String select = """
                    SELECT ts, sym,
                           covar_pop(px, qty) OVER w1 AS cp,
                           covar_samp(px, qty) OVER w1 AS cs,
                           corr(px, qty) OVER w1 AS cr,
                           covar_pop(px, qty) OVER w2 AS cp2,
                           corr(px, qty) OVER w2 AS cr2
                    FROM fx
                    WINDOW w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' SECOND PRECEDING AND CURRENT ROW),
                           w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '3' SECOND PRECEDING AND '1' SECOND PRECEDING)""";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + select);

            final String expectedBeforeRestart = """
                    ts\tsym\tcp\tcs\tcr\tcp2\tcr2
                    2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\tnull\tnull\tnull
                    2026-01-01T00:00:01.000000Z\ta\t2.0\t4.0\t1.0\t0.0\tnull
                    2026-01-01T00:00:02.000000Z\ta\t2.0\t4.0\t1.0\t2.0\t1.0
                    2026-01-01T00:00:04.000000Z\ta\t0.0\tnull\tnull\t0.0\tnull
                    2026-01-01T00:00:02.000000Z\tb\t0.0\tnull\tnull\tnull\tnull
                    2026-01-01T00:00:03.000000Z\tb\t-2.0\t-4.0\t-1.0\t0.0\tnull
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // a's 02s pair has a NULL px, so no ring ever holds it. a seals with only
                // (4, 7) in its w1 frame, and its w2 ring holds (3, 6) in the frame and (4, 7)
                // outside it. b's w2 ring holds (10, 1) in the frame and (14, -1) outside it.
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0, 2.0),
                        ('2026-01-01T00:00:01.000000Z', 'a', 3.0, 6.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', null, 5.0),
                        ('2026-01-01T00:00:02.000000Z', 'b', 10.0, 1.0),
                        ('2026-01-01T00:00:03.000000Z', 'b', 14.0, -1.0),
                        ('2026-01-01T00:00:04.000000Z', 'a', 4.0, 7.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveSeedToCompletion(job, "lv");
                driveSeedToCompletion(job, "lv2");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, cp, cs, cr, cp2, cr2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns(expectedBeforeRestart);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // a's w1 ring restores holding (4, 7), which 05s and 06s join: the frames there
                // combine a restored pair with new ones, so sums a restore failed to carry
                // read wrong. 07s carries a NULL qty and only expires (4, 7). a's w2 ring
                // restores holding (3, 6) in the frame and (4, 7) outside it: 05s expires the
                // first and admits the second, so a ring that lost the waiting pair reads an
                // empty frame there. b's w1 ring restores holding (10, 1) and (14, -1); 05s
                // expires the first and pairs the second with (12, 0). b's w2 ring admits its
                // waiting (14, -1) next to the restored (10, 1).
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:05.000000Z', 'a', 7.0, 4.0),
                        ('2026-01-01T00:00:05.000000Z', 'b', 12.0, 0.0),
                        ('2026-01-01T00:00:06.000000Z', 'a', 10.0, 1.0),
                        ('2026-01-01T00:00:07.000000Z', 'a', 8.0, null)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertRestoredFromTimeline("lv2");
                assertNoRefreshFaults("lv");
                final String expected = """
                        ts\tsym\tcp\tcs\tcr\tcp2\tcr2
                        2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\tnull\tnull\tnull
                        2026-01-01T00:00:01.000000Z\ta\t2.0\t4.0\t1.0\t0.0\tnull
                        2026-01-01T00:00:02.000000Z\ta\t2.0\t4.0\t1.0\t2.0\t1.0
                        2026-01-01T00:00:04.000000Z\ta\t0.0\tnull\tnull\t0.0\tnull
                        2026-01-01T00:00:05.000000Z\ta\t-2.25\t-4.5\t-1.0\t0.0\tnull
                        2026-01-01T00:00:06.000000Z\ta\t-6.0\t-9.0\t-1.0\t-2.25\t-1.0
                        2026-01-01T00:00:07.000000Z\ta\t-2.25\t-4.5\t-1.0\t-6.0\t-1.0
                        2026-01-01T00:00:02.000000Z\tb\t0.0\tnull\tnull\tnull\tnull
                        2026-01-01T00:00:03.000000Z\tb\t-2.0\t-4.0\t-1.0\t0.0\tnull
                        2026-01-01T00:00:05.000000Z\tb\t-0.5\t-1.0\t-1.0\t-2.0\t-1.0
                        """;
                assertQuery("SELECT ts, sym, cp, cs, cr, cp2, cr2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
                // The hand-computed values are the plain query's too.
                assertQuery("SELECT * FROM (" + select + ") ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
                assertViewMatchesRecompute("lv2", ISSUE_PROJECTION, "c");
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
            // proves the ring and the carried sums round-tripped.
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
                // second one is half NULL, which the ring never stores.
                commitOutOfOrderHalfSecond(job, DEEP_SECOND_A, "a", "4242", "17");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairResumedFromRoot(VIEWS);

                commitOutOfOrderHalfSecond(job, DEEP_SECOND_B, "b", "1500", "null");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairResumedFromRoot(VIEWS);

                // A short tail, then a correction just above the root sealed below it, which the
                // repair resumes from, restoring every partition's ring from the timeline before
                // it replays.
                final int tailSecond = (COMMITS + 3) * ROWS_PER_COMMIT;
                commitSeconds(job, tailSecond, TAIL_ROWS);
                driveRefreshToQuiescence(job);
                commitOutOfOrderHalfSecond(job, tailSecond, "a", "77", "-3");
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
            TestUtils.assertContains(e.getFlyweightMessage(), "live view checkpoint bivariate RANGE " + expectedMessage);
        }
    }

    private static void assertRestoredState(
            MapValue value,
            double[] sums,
            long frameSize,
            long size,
            int initialCapacity
    ) {
        // Raw bits: the five sums come back verbatim, a NaN included.
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("sum " + i, Double.doubleToRawLongBits(sums[i]), Double.doubleToRawLongBits(value.getDouble(i)));
        }
        Assert.assertEquals(frameSize, value.getLong(5));
        Assert.assertEquals(size, value.getLong(7));
        Assert.assertEquals(WindowFunction.restoredRingCapacity(size, initialCapacity), value.getLong(8));
        // Rebased to index 0.
        Assert.assertEquals(0, value.getLong(9));
        // Tombstone clear.
        Assert.assertEquals(0, value.getByte(10));
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static String frameOf(String view) {
        return switch (view) {
            case "lv_cur" -> FRAME_CURRENT;
            case "lv_lag" -> FRAME_LAGGED;
            case "lv_excl" -> FRAME_EXCLUDED;
            default -> throw new AssertionError("no frame for view " + view);
        };
    }

    private static MapValue newValue(Map map, String key) {
        final MapKey mapKey = map.withKey();
        mapKey.putStr(key);
        return mapKey.createValue();
    }

    private static String projection(String frame) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < FUNCTIONS.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(FUNCTIONS[i]).append("(px, qty) OVER (").append(frame).append(") AS f").append(i);
        }
        return sb.toString();
    }

    private static String projectionColumns() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < FUNCTIONS.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('f').append(i);
        }
        return sb.toString();
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

    private void assertRepairResumedFromRoot(String[] views) {
        for (String view : views) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
            Assert.assertNotNull(view, instance);
            // The function declines frame-local state, so the view has no RANGE repair plan and
            // the correction cannot take the localized rebuild, whose warm-up would re-sum the
            // frame. It resumes from the sealed root below the change instead, restoring the
            // durable ring and the sums it carries.
            Assert.assertEquals(view, LiveViewInstance.REPAIR_PLAN_NONE, instance.getCheckpointRepairDependencyPlans());
            final int denial = instance.getCheckpointRepairLastDenialReason();
            Assert.assertEquals(
                    view + " must lose the localized plan to the declined frame-local claim",
                    "incomplete dependency",
                    LiveViewCheckpointRepairPlan.denialReasonName(denial)
            );
            Assert.assertEquals(
                    view + " must resume from a sealed root",
                    "resume from anchor",
                    LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
            );
        }
    }

    private void assertRingDisposition(String fn, String frame) throws Exception {
        final String label = fn + " OVER (" + frame + ")";
        final LiveViewCheckpointCompiledFunction compiled =
                LiveViewCheckpointCompiledFunction.compile(engine, sqlExecutionContext, fn, frame, true);
        Assert.assertEquals(label, CLASS_NAME, compiled.className);
        Assert.assertTrue(label + " must support checkpoint state", compiled.supportsCheckpointState);
        Assert.assertTrue(label + " must persist through the chunked ring codec", compiled.supportsCheckpointRingState);
        Assert.assertEquals(
                label + " carries (sumX, sumXX, sumY, sumYY, sumXY) as a five-word scalar",
                5,
                compiled.ringScalarWords
        );
        Assert.assertEquals(
                label + " stores (x, y) as a two-word row",
                LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128,
                compiled.ringValueKind
        );
        // A ring-shaped function never declares a fixed image width.
        Assert.assertEquals(label, -1, compiled.fixedStateLength);
        // A warm-up from the frame's lower edge would re-sum the running sums, and the
        // formula's cancellation would carry the difference past the floating ceiling, so the
        // class declines the frame-local claim and every repair it takes is exact.
        Assert.assertFalse(label + " must decline frame-local state", compiled.hasFrameLocalCheckpointState);
        final LiveViewCheckpointDependency dependency = compiled.dependency;
        Assert.assertNotNull(label, dependency);
        Assert.assertTrue(label, dependency.isFiniteRange());
        Assert.assertFalse(label, dependency.hasFrameLocalState());
        Assert.assertEquals(label, NumericConvergence.EXACT, dependency.getNumericConvergence());
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

    private void assertUnboundedFrameStartRejected(String fn, String frame, String expectedMessage) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + fn + "(px, qty) OVER (PARTITION BY sym ORDER BY ts " + frame + ") AS v FROM fx");
            Assert.fail("expected an unbounded-frame-start reject for " + fn + " over " + frame);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            if (expectedMessage.startsWith("frame starting")) {
                Assert.assertTrue(
                        "the reject must name " + fn + " [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "cannot use " + fn + "()")
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

    private void assertViewsMatchRecompute(String[] views) throws Exception {
        for (String view : views) {
            if ("lv_issue".equals(view)) {
                assertViewMatchesRecompute(view, ISSUE_PROJECTION, "c");
            } else {
                assertViewMatchesRecompute(view, projection(frameOf(view)), projectionColumns());
            }
        }
    }

    // Commits ROWS_PER_COMMIT ascending seconds and gives the refresh job a turn. Each second
    // carries a row for 'a' and 'b'; every fifteenth carries one for a NULL key too, so that
    // partition's narrow frames empty out between rows and its rings seal with nothing in
    // them. Scattered NULLs on either side of the pair, plus a run of NULL px for 'b' wider
    // than the one-minute frame, keep half- and fully-NULL pairs out of the ring and drive the
    // finite count to zero across a seal. Both sides are integers whose squares and products
    // sum well under 2^53, so the five sums stay exact in double whatever order a frame adds
    // and drops pairs. Restores carry the sums, which
    // testBoundedRangeContinuesBitExactlyAcrossRestartAndRepair pins on non-integer data.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        commitSeconds(job, (commit - 1) * ROWS_PER_COMMIT, ROWS_PER_COMMIT);
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
            final String pxA = second % 7 == 0 ? "null" : Integer.toString(second);
            final String qtyA = second % 11 == 0 ? "null" : Integer.toString((second * 3) % 17 - 8);
            final String pxB = second % 5 == 0 || (second >= 100 && second < 170)
                    ? "null"
                    : Integer.toString(second + 1_000);
            final String qtyB = second % 3 == 0 ? "null" : Integer.toString(50 - second % 23);
            sql.append("('").append(ts).append("', 'a', ").append(pxA).append(", ").append(qtyA).append("), ")
                    .append("('").append(ts).append("', 'b', ").append(pxB).append(", ").append(qtyB).append(')');
            if (second % 15 == 0) {
                final String pxN = second % 45 == 0 ? "null" : Integer.toString(2 * second);
                sql.append(", ('").append(ts).append("', null, ").append(pxN).append(", ")
                        .append(second % 13).append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering stays
    // deterministic - and, given a job, gives it a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second, String sym, String px, String qty) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + ts + "', '" + sym + "', " + px + ", " + qty + ")");
        drainWalQueue();
        if (job != null) {
            drainJob(job);
            drainWalQueue();
        }
    }

    // Commits `count` ascending seconds from `firstSecond` of tick-like trades for 'btc' and
    // 'eth' and gives the refresh job a turn. Each price walks in one-cent steps from where
    // `prices` left it and each size is a whole number of thousandths, so no value is an
    // integer. Every eleventh 'btc' second has a NULL price and every thirteenth 'eth' second a
    // NULL size; the ring never stores either half-NULL pair.
    private void commitTicks(LiveViewRefreshJob job, Rnd rnd, double[] prices, int firstSecond, int count) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < count; i++) {
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            for (int s = 0; s < 2; s++) {
                prices[s] += (rnd.nextInt(201) - 100) * 0.01;
                final double qty = (1 + rnd.nextInt(2_500)) * 0.001;
                final boolean isPxNull = s == 0 && second % 11 == 5;
                final boolean isQtyNull = s == 1 && second % 13 == 7;
                if (i > 0 || s > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(ts).append("', '").append(s == 0 ? "btc" : "eth").append("', ")
                        .append(isPxNull ? "null" : Double.toString(prices[s])).append(", ")
                        .append(isQtyNull ? "null" : Double.toString(qty)).append(')');
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
                    + ("lv_issue".equals(view) ? ISSUE_PROJECTION : projection(frameOf(view))) + " FROM fx");
        }
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
        private final long[] scalarWords = new long[5];
        private long frameSize = -1;
        private int scalarWordCount;

        @Override
        public void putRow(long timestamp) {
            throw new AssertionError("a bivariate ring streams two-word rows");
        }

        @Override
        public void putRow(long timestamp, long valueBits) {
            throw new AssertionError("a bivariate ring streams two-word rows");
        }

        @Override
        public void putRow(long timestamp, long xBits, long yBits) {
            rows.add(timestamp);
            rows.add(xBits);
            rows.add(yBits);
        }

        @Override
        public void putRow(long timestamp, long hh, long hl, long lh, long ll) {
            throw new AssertionError("a bivariate ring streams two-word rows");
        }

        @Override
        public void putScalarState(long scalarBits, long frameSize) {
            throw new AssertionError("a bivariate ring carries a five-word scalar");
        }

        @Override
        public void putScalarState(long hi, long lo, long frameSize) {
            throw new AssertionError("a bivariate ring carries a five-word scalar");
        }

        @Override
        public void putScalarState(long hh, long hl, long lh, long ll, long frameSize) {
            throw new AssertionError("a bivariate ring carries a five-word scalar");
        }

        @Override
        public void putScalarState(long word0, long word1, long word2, long word3, long word4, long frameSize) {
            Assert.assertEquals("one scalar per partition", 0, scalarWordCount);
            Assert.assertEquals("the scalar precedes the rows", 0, rows.size());
            this.scalarWordCount = 5;
            scalarWords[0] = word0;
            scalarWords[1] = word1;
            scalarWords[2] = word2;
            scalarWords[3] = word3;
            scalarWords[4] = word4;
            this.frameSize = frameSize;
        }

        private void assertCaptured(StubRingSource source) {
            Assert.assertEquals(5, scalarWordCount);
            Assert.assertEquals("sumX", source.getScalarWord(0), scalarWords[0]);
            Assert.assertEquals("sumXX", source.getScalarWord(1), scalarWords[1]);
            Assert.assertEquals("sumY", source.getScalarWord(2), scalarWords[2]);
            Assert.assertEquals("sumYY", source.getScalarWord(3), scalarWords[3]);
            Assert.assertEquals("sumXY", source.getScalarWord(4), scalarWords[4]);
            Assert.assertEquals("count", source.frameSize, frameSize);
            TestUtils.assertEquals(source.rows, rows);
        }

        private void clear() {
            rows.clear();
            frameSize = -1;
            for (int i = 0; i < 5; i++) {
                scalarWords[i] = 0;
            }
            scalarWordCount = 0;
        }
    }

    // Injects a corrupt or hand-built persisted ring straight into the restore path. It stands
    // in for the chunk reader, not for a peer: the rows it replays are ones a correct seal
    // would never write, which is the point.
    private static class StubRingSource implements LiveViewCheckpointRingStateSource {
        private final LongList rows = new LongList();
        private final long[] scalarWords = new long[5];
        private long frameSize;
        private long rowCount;

        @Override
        public void forEachRow(@NotNull RowConsumer consumer) {
            throw new AssertionError("a bivariate ring replays two-word rows");
        }

        @Override
        public void forEachRow(@NotNull Decimal128RowConsumer consumer) {
            for (int i = 0, n = rows.size(); i < n; i += 3) {
                consumer.accept(rows.getQuick(i), rows.getQuick(i + 1), rows.getQuick(i + 2));
            }
        }

        @Override
        public void forEachRow(@NotNull Decimal256RowConsumer consumer) {
            throw new AssertionError("a bivariate ring replays two-word rows");
        }

        @Override
        public void forEachTimestamp(@NotNull TimestampConsumer consumer) {
            throw new AssertionError("a bivariate ring replays two-word rows");
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
            return scalarWords[0];
        }

        @Override
        public long getScalarWord(int index) {
            if (index < 0 || index >= scalarWords.length) {
                throw new AssertionError("a bivariate ring carries five scalar words, asked for " + index);
            }
            return scalarWords[index];
        }

        @Override
        public int getScalarWordCount() {
            return 5;
        }

        // sums are (sumX, sumXX, sumY, sumYY, sumXY).
        private StubRingSource of(long rowCount, long frameSize, double[] sums) {
            this.rowCount = rowCount;
            this.frameSize = frameSize;
            for (int i = 0; i < scalarWords.length; i++) {
                scalarWords[i] = Double.doubleToRawLongBits(sums[i]);
            }
            rows.clear();
            return this;
        }

        // y is px (the first argument), x is qty.
        private StubRingSource row(long timestamp, double x, double y) {
            rows.add(timestamp);
            rows.add(Double.doubleToRawLongBits(x));
            rows.add(Double.doubleToRawLongBits(y));
            return this;
        }
    }
}
