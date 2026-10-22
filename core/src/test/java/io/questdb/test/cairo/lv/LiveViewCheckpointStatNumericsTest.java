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
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Numeric contracts of the statistical window functions over bounded frames in a live view,
 * on the data shapes where floating-point rounding decides the outcome. The five checkpointed
 * classes are stddev/variance over bounded ROWS and RANGE, covariance/correlation over bounded
 * ROWS and RANGE, and {@code ksum} over bounded RANGE.
 * <p>
 * A restore must be bit-exact: {@link LiveViewCheckpointContracts} states that restored
 * checkpoint bits are exact. {@link #testRestartCarriesAbsorbedAndOverflowedSumsBitExactly()}
 * pins it end to end on data that drives the running sums through absorption (a value lost
 * against 1e17) and overflow (a square of 1e200 that reaches +Inf, then NaN once the value
 * leaves the frame). Only a restore that takes the carried sums verbatim, NaN and Inf
 * included, prints what the plain query prints after a restart.
 * <p>
 * A localized repair re-accumulates the frame from its lower edge during warm-up, so the
 * sums lose the add/drop rounding history the plain query's sliding sums carry. The contract
 * caps what that may cost: approved floating outputs after a localized replay stay within
 * {@link LiveViewCheckpointContracts#FLOATING_RELATIVE_TOLERANCE} of a fresh recompute. The
 * {@code ...LocalizedRepairStaysWithinFloatingCeiling...} cases pin that bound row by row on
 * tick-like data, with {@code avg} (and {@code ksum} over RANGE) on the same frames as
 * controls, on both repair runtimes: the isolated one, and the primary one that copies its
 * state aside first.
 * Pathological data stays out of those cases: after an extreme value leaves a sliding frame
 * the plain query's own sums stay corrupted, so any re-accumulation departs from it, the
 * released {@code avg} included.
 */
public class LiveViewCheckpointStatNumericsTest extends AbstractLiveViewTest {

    // A frame ending at the current row and a lagged one.
    private static final String[] RANGE_FRAMES = {
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND CURRENT ROW",
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING"
    };
    // Where the RANGE case's late row lands: 50 seconds below the head of five 60-second
    // commits, deep enough that rebuilding the frame's reach reads fewer rows than resuming
    // from a sealed root and replaying everything above it.
    private static final int RANGE_LATE_SECOND = 250;
    // Two controls whose formulas do not subtract nearly equal sums, then the four functions
    // whose formulas do.
    private static final String[] RANGE_REPAIR_FUNCTIONS = {
            "avg(px)", "ksum(px)", "stddev_samp(px)", "var_pop(px)", "covar_samp(px, qty)", "corr(px, qty)"
    };
    // The issue's frame, which ends at the current row, and a lagged one.
    private static final String[] ROWS_FRAMES = {
            "PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING",
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 12 PRECEDING AND 3 PRECEDING"
    };
    // ksum over a bounded ROWS frame declares no frame-local state, so it would deny the view
    // a ROWS repair plan and turn the repair into a resume from a sealed root; avg is the
    // sole control here.
    private static final String[] ROWS_REPAIR_FUNCTIONS = {
            "avg(px)", "stddev_samp(px)", "var_pop(px)", "covar_samp(px, qty)", "corr(px, qty)"
    };
    // Where the ROWS case's late row lands: 200 seconds below the head.
    private static final int ROWS_LATE_SECOND = 100;
    private static final int TICK_COMMIT_SECONDS = 60;
    private static final int TICK_COMMITS = 5;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testRangeLocalizedRepairStaysWithinFloatingCeiling() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(RANGE_FRAMES, RANGE_REPAIR_FUNCTIONS, RANGE_LATE_SECOND, false);
    }

    @Test
    public void testRangeLocalizedRepairStaysWithinFloatingCeilingWithoutIsolatedRuntime() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(RANGE_FRAMES, RANGE_REPAIR_FUNCTIONS, RANGE_LATE_SECOND, true);
    }

    @Test
    public void testRestartCarriesAbsorbedAndOverflowedSumsBitExactly() throws Exception {
        // All five checkpointed classes in one view, with avg over both frames as the released
        // reference. Partition 'a' opens with (1e17, 1e17): every small value after it is
        // absorbed into the running sums, so once 1e17 leaves the frame the sums hold rounding
        // residue rather than the frame's true totals. Partition 'b' opens with
        // (1e200, 1e200): its square and product overflow to +Inf, and the drop of 1e200 at the
        // eleventh second turns those sums into NaN, which every later frame inherits.
        final String projection = """
                covar_pop(px, qty) OVER wr AS covar_range,
                corr(px, qty) OVER wr AS corr_range,
                stddev_pop(px) OVER wr AS stddev_range,
                ksum(px) OVER wr AS ksum_range,
                avg(px) OVER wr AS avg_range,
                covar_pop(px, qty) OVER ww AS covar_rows,
                corr(px, qty) OVER ww AS corr_rows,
                stddev_pop(px) OVER ww AS stddev_rows,
                avg(px) OVER ww AS avg_rows
                FROM fx
                WINDOW wr AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND CURRENT ROW),
                       ww AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)""";
        final String plain = "(SELECT ts, sym, " + projection + ") ORDER BY sym, ts";
        final String view = "SELECT * FROM lv ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, " + projection);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1e17, 1e17),
                        ('2026-01-01T00:00:01.000000Z', 'a', 1.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', 2.0, 3.0),
                        ('2026-01-01T00:00:03.000000Z', 'a', 4.0, 2.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 1e200, 1e200),
                        ('2026-01-01T00:00:01.000000Z', 'b', 1.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'b', 2.0, 3.0),
                        ('2026-01-01T00:00:03.000000Z', 'b', 4.0, 2.0)""");
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                // The eleventh second drops the opening pair from both frames; the root this
                // commit seals carries the residue and the NaN sums.
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:11.000000Z', 'a', 3.0, 5.0),
                        ('2026-01-01T00:00:11.000000Z', 'b', 3.0, 5.0)""");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:12.000000Z', 'a', 5.0, 1.0),
                        ('2026-01-01T00:00:12.000000Z', 'b', 5.0, 1.0),
                        ('2026-01-01T00:00:13.000000Z', 'a', 6.0, 4.0),
                        ('2026-01-01T00:00:13.000000Z', 'b', 6.0, 4.0),
                        ('2026-01-01T00:00:14.000000Z', 'a', 2.5, 7.5),
                        ('2026-01-01T00:00:14.000000Z', 'b', 2.5, 7.5)""");
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                // The restored frames no longer hold 1e17 or 1e200, so a restore that re-summed
                // them would emit the frames' true statistics: finite for 'b', and free of the
                // absorption residue for 'a'. The plain query keeps the residue and the NaN, so
                // equal text, which is equal bits, means the restore carried both verbatim.
                assertViewPrintsLikePlain(view, plain);
                // Guard the scenario itself: if the plain query stopped carrying NaN sums for
                // 'b' past the drop, the bit-exact comparison above would no longer tell a
                // carried restore from a re-summed one.
                assertQuery("""
                        SELECT ts, covar_range, corr_range, stddev_range, covar_rows, corr_rows, stddev_rows
                        FROM lv
                        WHERE sym = 'b' AND ts > '2026-01-01T00:00:11.000000Z'
                        ORDER BY ts""")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("""
                                ts\tcovar_range\tcorr_range\tstddev_range\tcovar_rows\tcorr_rows\tstddev_rows
                                2026-01-01T00:00:12.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                2026-01-01T00:00:13.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                2026-01-01T00:00:14.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                """);
            }
        });
    }

    @Test
    public void testRowsLocalizedRepairStaysWithinFloatingCeiling() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(ROWS_FRAMES, ROWS_REPAIR_FUNCTIONS, ROWS_LATE_SECOND, false);
    }

    @Test
    public void testRowsLocalizedRepairStaysWithinFloatingCeilingWithoutIsolatedRuntime() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(ROWS_FRAMES, ROWS_REPAIR_FUNCTIONS, ROWS_LATE_SECOND, true);
    }

    // Each function over each frame, as columns named <function>_w<frame index>.
    private static String columnName(String function, int frame) {
        return function.substring(0, function.indexOf('(')) + "_w" + frame;
    }

    private static String repairColumns(String[] frames, String[] functions) {
        final StringBuilder sb = new StringBuilder();
        for (int f = 0; f < frames.length; f++) {
            for (String function : functions) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(columnName(function, f));
            }
        }
        return sb.toString();
    }

    private static String repairProjection(String[] frames, String[] functions) {
        final StringBuilder sb = new StringBuilder();
        for (int f = 0; f < frames.length; f++) {
            for (String function : functions) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(function).append(" OVER (").append(frames[f]).append(") AS ").append(columnName(function, f));
            }
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

    // Walks the view and the plain query in step and checks every DOUBLE column of every row
    // against the contract's floating ceiling. Both-NULL cells agree; a NULL or a non-finite
    // value against anything else breaches, and is counted apart from finite drift. Reports
    // every breaching column at once, with the worst finite relative error and the first
    // breaching row, so a failure shows which families drift and by how much.
    private void assertAllRowsWithinFloatingCeiling(String viewQuery, String plainQuery) throws Exception {
        try (
                RecordCursorFactory viewFactory = select(viewQuery);
                RecordCursorFactory plainFactory = select(plainQuery);
                RecordCursor viewCursor = viewFactory.getCursor(sqlExecutionContext);
                RecordCursor plainCursor = plainFactory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = plainFactory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            Assert.assertEquals("column count", columnCount, viewFactory.getMetadata().getColumnCount());
            final long[] breaches = new long[columnCount];
            final long[] nonFiniteBreaches = new long[columnCount];
            final double[] worstRelative = new double[columnCount];
            final String[] firstBreach = new String[columnCount];
            final Record viewRecord = viewCursor.getRecord();
            final Record plainRecord = plainCursor.getRecord();
            long rows = 0;
            while (plainCursor.hasNext()) {
                Assert.assertTrue("the view ends after " + rows + " rows, before the plain query", viewCursor.hasNext());
                final long ts = plainRecord.getTimestamp(0);
                Assert.assertEquals("ts at row " + rows, ts, viewRecord.getTimestamp(0));
                final CharSequence sym = plainRecord.getSymA(1);
                Assert.assertTrue(
                        "sym at row " + rows + " [plain=" + sym + ", view=" + viewRecord.getSymA(1) + ']',
                        Chars.equals(sym, viewRecord.getSymA(1))
                );
                for (int col = 2; col < columnCount; col++) {
                    final double expected = plainRecord.getDouble(col);
                    final double actual = viewRecord.getDouble(col);
                    if (Double.isNaN(expected) && Double.isNaN(actual)) {
                        continue;
                    }
                    if (!LiveViewCheckpointContracts.isWithinFloatingTolerance(expected, actual)) {
                        breaches[col]++;
                        if (Double.isFinite(expected) && Double.isFinite(actual)) {
                            final double relative = Math.abs(expected - actual) / Math.max(Math.abs(expected), Math.abs(actual));
                            worstRelative[col] = Math.max(worstRelative[col], relative);
                        } else {
                            nonFiniteBreaches[col]++;
                        }
                        if (firstBreach[col] == null) {
                            firstBreach[col] = "row " + rows + " [sym=" + sym + ", ts=" + ts
                                    + ", plain=" + expected + ", view=" + actual + ']';
                        }
                    }
                }
                rows++;
            }
            Assert.assertFalse("the view has more rows than the plain query's " + rows, viewCursor.hasNext());
            Assert.assertTrue("nothing compared", rows > 0);

            final StringSink report = new StringSink();
            for (int col = 2; col < columnCount; col++) {
                if (breaches[col] > 0) {
                    report.put("\n  ").put(metadata.getColumnName(col)).put(": ").put(breaches[col])
                            .put(" of ").put(rows).put(" rows over the ceiling (").put(nonFiniteBreaches[col])
                            .put(" NULL or non-finite on one side only), worst finite relative error ")
                            .put(worstRelative[col]).put(", first at ").put(firstBreach[col]);
                }
            }
            if (report.length() > 0) {
                Assert.fail("after a localized repair, outputs exceed the documented floating ceiling ("
                        + LiveViewCheckpointContracts.FLOATING_RELATIVE_TOLERANCE + " relative, "
                        + LiveViewCheckpointContracts.FLOATING_ABSOLUTE_TOLERANCE + " absolute):" + report);
            }
        }
    }

    // Lays down five 60-second commits of tick-like data under a view of every repair function
    // over both frames, checks forward processing is bit-exact, then commits one late row deep
    // below the head. The repair must take the localized rebuild, whose warm-up re-accumulates
    // the frame, and after two more in-order commits every output must sit within the
    // contract's floating ceiling of the plain query. With the isolated runtime declined, the
    // repair replays on the primary runtime after copying every partition's state aside; a
    // one-row replay budget parks it after the copy, which is what proves that route ran.
    private void assertLocalizedRepairStaysWithinFloatingCeiling(
            String[] frames,
            String[] functions,
            int lateSecond,
            boolean isCopyAside
    ) throws Exception {
        if (isCopyAside) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        }
        final String projection = repairProjection(frames, functions);
        final String plain = "(SELECT ts, sym, " + projection + " FROM fx) ORDER BY sym, ts";
        final String view = "SELECT ts, sym, " + repairColumns(frames, functions) + " FROM lv ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + projection + " FROM fx");
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int c = 0; c < TICK_COMMITS; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    if (c == 0) {
                        driveSeedToCompletion(job, "lv");
                    }
                    driveRefreshToQuiescence(job);
                }
                assertNoRefreshFaults("lv");
                // Forward processing runs the same sliding sums as the plain query, so up to the
                // repair the view matches it bit for bit. Whatever drifts below comes from the
                // repair.
                assertViewPrintsLikePlain(view, plain);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                if (isCopyAside) {
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
                    commitLateRow(null, lateSecond);
                    driveUntilParked(job, "lv");
                    Assert.assertTrue(
                            "a converging repair on the primary runtime must have copied its state aside",
                            instance.getSuspendedRepair().getOverlay().isCaptured()
                    );
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1_000_000);
                } else {
                    commitLateRow(job, lateSecond);
                }
                driveRefreshToQuiescence(job);
                final int denial = instance.getCheckpointRepairLastDenialReason();
                Assert.assertEquals(
                        "the late row must take the localized rebuild, whose warm-up re-accumulates the frame [denial="
                                + LiveViewCheckpointRepairPlan.denialReasonName(denial) + ']',
                        "localized rebuild",
                        LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
                );

                // In-order commits keep running on the state the repair left behind.
                for (int c = 0; c < 2; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertNoRefreshFaults("lv");
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_status\nactive\n");
                assertAllRowsWithinFloatingCeiling(view, plain);
            }
        });
    }

    // Prints the plain query and asserts the view prints the same text. Unlike a cursor
    // comparison with a tolerance on doubles, equal shortest round-trip text means equal bits.
    private void assertViewPrintsLikePlain(String viewQuery, String plainQuery) throws Exception {
        printSql(plainQuery);
        final String expected = sink.toString();
        assertQuery(viewQuery)
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void commit(LiveViewRefreshJob job, String insert) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute(insert);
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one 'btc' trade half a second past an in-order second - a fresh designated
    // timestamp, so it ties with no existing row and both orderings stay deterministic - and,
    // given a job, gives it a turn.
    private void commitLateRow(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + ts + "', 'btc', 65001.37, 1.234)");
        drainWalQueue();
        if (job != null) {
            drainJob(job);
            drainWalQueue();
        }
    }

    // Commits TICK_COMMIT_SECONDS ascending seconds from `firstSecond` of tick-like trades for
    // 'btc' and 'eth' and gives the refresh job a turn. Each price walks in one-cent steps from
    // where `prices` left it and each size is a whole number of thousandths, so no value is an
    // integer. Every eleventh 'btc' second has a NULL price and every thirteenth 'eth' second a
    // NULL size.
    private void commitTicks(LiveViewRefreshJob job, Rnd rnd, double[] prices, int firstSecond) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < TICK_COMMIT_SECONDS; i++) {
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            for (int s = 0; s < 2; s++) {
                prices[s] += (rnd.nextInt(201) - 100) * 0.01;
                final double qty = (1 + rnd.nextInt(5_000)) * 0.001;
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

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
