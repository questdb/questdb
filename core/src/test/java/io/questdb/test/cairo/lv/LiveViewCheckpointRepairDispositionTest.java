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
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * What one out-of-order repair actually did, as {@code live_views()} reports it:
 * {@code checkpoint_repair_last_disposition} and {@code checkpoint_repair_last_denial}.
 * <p>
 * {@code checkpoint_repair_plan} beside them names only what the view's SQL admits, and
 * the gap between the two is the point. A view whose SELECT carries a bounded ROWS frame
 * reports a {@code rows} plan whatever its base does, while a base that deduplicates
 * denies that plan its answer at every single refresh: the discovery reads the affected
 * key domain off the post-change snapshot, and a dedup replacement can drop a key out of
 * the change interval where neither raw-WAL walk sees it. Reading the plan alone, such a
 * view looks bounded and rebuilds its whole history.
 * <p>
 * Each case therefore asserts the pair against the plan, and pins the denial against a
 * control that differs in exactly the one thing being blamed.
 */
public class LiveViewCheckpointRepairDispositionTest extends AbstractLiveViewTest {

    // The anchored window's from-base equivalent, and the base it reads. ANCHOR is
    // accepted only inside a live view, so an anchored case cannot use its own SELECT as
    // its oracle and spells the reset out as a partition on the bucket the anchor floors
    // to instead.
    private static final String ANCHOR_ORACLE_SOURCE =
            " FROM (SELECT ts, sym, x, timestamp_floor('1d', ts) AS bucket FROM base)";
    private static final String ANCHOR_ORACLE_WINDOW =
            "PARTITION BY sym, bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    // A daily anchor over the same partition and order the ROWS frame below uses. Every
    // row in the history sits in one calendar day, so the segment this proves converges
    // TOMORROW - far above any frontier the fixture's runtime reaches, which is the shape
    // the anchored cases are about. ANCHOR is accepted only on a named WINDOW clause, so
    // the frame lives here and the projections reference it as `w`.
    private static final String ANCHOR_WINDOW_CLAUSE =
            " WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))";
    // In-order commits driven before the out-of-order one, one per 10 seconds.
    private static final int HISTORY_COMMITS = 12;
    // Second-of-day of the null-run cases' correction. Mid-history and inside the null
    // run, which is what earns a localized rebuild whose floor sits above the two
    // non-null rows - see assertNullRunRepairOutcome.
    private static final int NULL_RUN_O3_SECOND = 55;
    // Second-of-day of the out-of-order row. Below the first boundary at 10s, so no
    // resume can qualify and the disposition is the rebuild either way - which is what
    // lets the cases read the denial without the price of a resume entering it.
    private static final int O3_SECOND = 5;
    // The bounded ROWS frame the dedup case and its control share, so the only thing
    // that differs between them is the base table's DEDUP clause.
    private static final String ROWS_WINDOW_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";

    @After
    public void unpinClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void pinClockBelowTestData() {
        // Below the 2026 rows, so a START FROM NOW view admits every one of them.
        setCurrentMicros(0L);
    }

    @Test
    public void testARowsFunctionBesideAnAnchoredOneKeepsTheFiniteBoundRequirement() throws Exception {
        // The union shape, and the one where getting the requirement wrong corrupts state
        // instead of failing loudly. A bounded ROWS frame declared beside the anchored one
        // keeps sliding across every segment boundary, so it holds a key's last Nmax rows
        // however old they are and cannot survive the promotion an EOF bound forces. The
        // requirement therefore rides on the ROWS arm, not on the shape as a whole: this
        // view is denied exactly where the anchored-only case above is not, and the
        // recompute assertion is what says the denial bought correctness rather than
        // hiding a wrong answer.
        assertRepairOutcome(
                "sum(x) OVER w AS a, sum(x) OVER (" + ROWS_WINDOW_FRAME + ")",
                "",
                ANCHOR_WINDOW_CLAUSE,
                "SELECT ts, sym, sum(x) OVER (" + ANCHOR_ORACLE_WINDOW + ") AS a, "
                        + "sum(x) OVER (" + ROWS_WINDOW_FRAME + ") AS s" + ANCHOR_ORACLE_SOURCE,
                "rows+anchor",
                "boundary rebuild",
                "frontier below convergence"
        );
    }

    @Test
    public void testAnAnchoredRepairLocalizesWithoutReachingItsSegmentEnd() throws Exception {
        // The anchor segment is a calendar day and the whole history sits inside one, so
        // the convergence boundary is TOMORROW and the runtime frontier cannot reach it
        // until the day rolls over. The repair localizes anyway: the anchor expires by
        // time, so a key with no row in the correction's segment holds nothing there
        // either, and promoting the replay's state loses only what the next reset was
        // going to discard. Nothing is denied, and the floors come off the segment rather
        // than off the view's START FROM boundary.
        assertRepairOutcome(
                "sum(x) OVER w",
                "",
                ANCHOR_WINDOW_CLAUSE,
                "SELECT ts, sym, sum(x) OVER (" + ANCHOR_ORACLE_WINDOW + ") AS s" + ANCHOR_ORACLE_SOURCE,
                "anchor",
                "localized rebuild",
                null
        );
    }

    @Test
    public void testDedupDeniesAStaticallyPlannedRowsRepair() throws Exception {
        // The observability gap this case and its control close. Both views carry the
        // same bounded ROWS frame and both report a "rows" plan; only the dedup-keyed
        // base denies it at refresh, and before the denial was reported the two were
        // indistinguishable from the catalogue.
        assertRepairOutcome(
                ROWS_WINDOW_FRAME,
                " DEDUP UPSERT KEYS(ts, sym)",
                "rows",
                "boundary rebuild",
                "dedup"
        );
    }

    @Test
    public void testLagIgnoreNullsDeniesTheRepairItsFloor() throws Exception {
        // IGNORE NULLS advances the ring only on non-null rows, so the state is the last
        // `offset` NON-NULL values and reaches back an unbounded number of ROWS - a run of
        // nulls pushes it arbitrarily far. No row-count extent can bound that, so lag must
        // decline the frame-local claim here even though the frame is a bounded ROWS one.
        // The one-variable RESPECT NULLS control is
        // testLagRespectNullsOverANullRunLocalizesTheSameRepair.
        assertRepairOutcome(
                "lag(x, 2) IGNORE NULLS OVER (PARTITION BY sym ORDER BY ts "
                        + "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)",
                "",
                "none",
                "boundary rebuild",
                "incomplete dependency"
        );
    }

    @Test
    public void testLagIgnoreNullsOverANullRunRepairsToTheSameAnswer() throws Exception {
        // What a wrongly claimed frame-local extent costs, in persisted values. A run of nulls
        // separates the two non-null rows the ring must still hold from the correction, so
        // lag(x, 2) IGNORE NULLS reaches back past them while a rows plan warms up only
        // `offset` = 2 rows - both of them null - and emits the default instead.
        // Pre-fix this failed with the 55s row reading NULL where 10 belongs.
        //
        // Declining the claim costs less here than the sibling case above, which corrects
        // below every boundary and so has no anchor to resume from: with a boundary at every
        // row this resumes from the one below the correction and replays forward, rather than
        // rebuilding the whole history.
        assertNullRunRepairOutcome("IGNORE NULLS ", "none", "resume from anchor", "incomplete dependency");
    }

    @Test
    public void testLagRespectNullsOverANullRunLocalizesTheSameRepair() throws Exception {
        // The one-variable control for the case above: same history, same correction, same
        // frame, differing only in IGNORE NULLS. RESPECT NULLS keeps the frame-local claim,
        // and its answer at the correction is the row two back whatever those rows hold, so
        // a two-row warm-up is genuinely sufficient.
        //
        // Its real job is to pin the premise the sibling's redness rests on: that this
        // correction selects the LOCALIZED rebuild. The disposition turns on a scan-cost
        // comparison neither test controls - a correction below every boundary rebuilds from
        // the history's first row, and one above the last boundary resumes from an anchor,
        // both landing on the right answer regardless. Should that pricing ever shift, this
        // control goes red rather than letting the sibling quietly stop testing the bug.
        assertNullRunRepairOutcome("", "rows", "localized rebuild", null);
    }

    @Test
    public void testRowsRepairLocalizesOverABaseThatCannotDedup() throws Exception {
        // The control for testDedupDeniesAStaticallyPlannedRowsRepair: the same SQL over
        // a base that cannot replace a row. The plan is identical, and now the repair
        // reads only the interval the discovery proved and reports nothing denied.
        assertRepairOutcome(
                ROWS_WINDOW_FRAME,
                "",
                "rows",
                "localized rebuild",
                null
        );
    }

    @Test
    public void testUncoveredFunctionDeniesTheRepairItsFloor() throws Exception {
        // A RANGE-framed lag: its state extent is a row count, which cannot bound a
        // timestamp-width repair, so no dependency plan claims it. The static plan
        // already says "none", and the denial says the same thing about the repair that
        // ran - the two agree here, unlike the dedup case above.
        assertRepairOutcome(
                "lag(x, 2) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW)",
                "",
                "none",
                "boundary rebuild",
                "incomplete dependency"
        );
    }

    /**
     * Drives a lag view over a history whose two non-null rows are separated from the
     * correction by a run of nulls, corrects mid-history INSIDE that run, then asserts both
     * the published plan/disposition pair and the view's values against a from-base
     * recompute.
     * <p>
     * The correction's placement is the whole point and is why this does not reuse
     * {@link #assertRepairOutcome}: only a mid-history correction earns a localized rebuild
     * whose floor sits above the non-null rows. A correction below every boundary rebuilds
     * from the history's first row and a correction above the last one resumes from an
     * anchor - both reconstruct the ring correctly and would hide a too-narrow floor.
     *
     * @param ignoreNullsClause {@code "IGNORE NULLS "} or empty, the only thing that differs
     *                          between the two cases
     */
    private void assertNullRunRepairOutcome(
            String ignoreNullsClause,
            String expectedPlan,
            String expectedDisposition,
            String expectedDenial
    ) throws Exception {
        // One boundary per row, so the mid-history correction has an anchor below it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final String viewSql = "SELECT ts, sym, lag(x, 2) " + ignoreNullsClause
                + "OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // 10 and 20 are the two non-null values an IGNORE NULLS ring must still
                // hold at the correction; commits 3..10 are the null run that separates
                // them from it.
                for (int commit = 1; commit <= HISTORY_COMMITS; commit++) {
                    setCurrentMicros(commit * 200_000L);
                    final String value = switch (commit) {
                        case 1 -> "10";
                        case 2 -> "20";
                        case 11 -> "30";
                        case 12 -> "40";
                        default -> "NULL";
                    };
                    execute("INSERT INTO base (ts, sym, x) VALUES ('"
                            + secondsTs(commit * 10) + "', 'a', " + value + ")");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }

                setCurrentMicros((HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + secondsTs(NULL_RUN_O3_SECOND) + "', 'a', NULL)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                        "checkpoint_repair_last_denial FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                                "checkpoint_repair_last_denial\n" +
                                expectedPlan + "\t" + expectedDisposition + "\t"
                                + (expectedDenial == null ? "" : expectedDenial) + "\n");

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

            execute("DROP LIVE VIEW lv");
        });
    }

    /**
     * Drives one view through a history and one out-of-order correction below every
     * logical boundary, then asserts the static plan and the runtime pair the repair
     * published. The projection is passed whole so a case can state either a window
     * function over a frame or a function no plan covers.
     * <p>
     * Both runtime columns are asserted NULL before the correction: a view that has
     * refreshed only forward has run no repair, and reporting a disposition for one that
     * never happened would be worse than reporting nothing.
     *
     * @param dedupClause the base table's DEDUP clause, or an empty string for a base
     *                    that cannot replace a row
     */
    private void assertRepairOutcome(
            String projection,
            String dedupClause,
            String expectedPlan,
            String expectedDisposition,
            String expectedDenial
    ) throws Exception {
        assertRepairOutcome(projection, dedupClause, "", null, expectedPlan, expectedDisposition, expectedDenial);
    }

    /**
     * @param windowClause the view's named {@code WINDOW} clause, or an empty string for a
     *                     view whose frames are all inline. Only an anchored frame needs
     *                     it: the parser accepts {@code ANCHOR} nowhere else
     * @param oracleSql    the from-base recompute the view has to equal, or null when the
     *                     view's own SELECT doubles as its oracle. An anchored view needs
     *                     one of its own for the same reason it needs the window clause
     */
    private void assertRepairOutcome(
            String projection,
            String dedupClause,
            String windowClause,
            String oracleSql,
            String expectedPlan,
            String expectedDisposition,
            String expectedDenial
    ) throws Exception {
        // One boundary per flush, so the correction at 5s sits below every one of them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);

        final String viewSql = (projection.contains(" OVER ")
                ? "SELECT ts, sym, " + projection + " AS s FROM base"
                : "SELECT ts, sym, sum(x) OVER (" + projection + ") AS s FROM base") + windowClause;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                    + dedupClause);
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= HISTORY_COMMITS; commit++) {
                    setCurrentMicros(commit * 200_000L);
                    final String rowTs = secondsTs(commit * 10);
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('" + rowTs + "', 'a', " + commit + "), " +
                            "('" + rowTs + "', 'b', " + (commit + 100) + ")");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }

                // The plan is settled the moment the view compiles its SELECT, while the
                // pair stays NULL: a forward-only history runs no repair to report on.
                assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                        "checkpoint_repair_last_denial FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                                "checkpoint_repair_last_denial\n" +
                                expectedPlan + "\t\t\n");

                setCurrentMicros((HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(O3_SECOND) + "', 'a', 9000), " +
                        "('" + secondsTs(O3_SECOND) + "', 'b', 9100)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                        "checkpoint_repair_last_denial FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                                "checkpoint_repair_last_denial\n" +
                                expectedPlan + "\t" + expectedDisposition + "\t"
                                + (expectedDenial == null ? "" : expectedDenial) + "\n");

                // Whichever disposition ran, the repair is only observable because it was
                // also correct: a denied bound costs latency, never an answer.
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + (oracleSql != null ? oracleSql : viewSql) + ") ORDER BY 2, 1",
                        "(lv) ORDER BY 2, 1",
                        LOG,
                        true
                );
                assertNoRefreshFaults("lv");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Builds a 2026-11-01 microsecond timestamp literal at the given second-of-day offset.
    // Every row shares one calendar day, so the base's DAY partition never enters it.
    private static String secondsTs(int secondOfDay) {
        return String.format("2026-11-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }
}
