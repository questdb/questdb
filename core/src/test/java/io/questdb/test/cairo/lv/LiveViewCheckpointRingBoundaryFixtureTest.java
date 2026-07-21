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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Phase 0 baseline fixtures for the versioned checkpoint timeline
 * (LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md, Phase 0 step 3): deterministic
 * RANGE and ROWS window views with enough history to collapse the retained checkpoint
 * ring and then reproduce an older out-of-order boundary replay.
 * <p>
 * The scenario reproduces, on eligible window shapes, exactly the pathology section 2 of
 * the design calls out and the timeline replaces:
 * <ul>
 *     <li><b>Collapse the ring.</b> With one head sealed per flush
 *     ({@code checkpoint.rows = 1}) the retained ring fills to the {@code retention.count}
 *     budget; every further in-order commit evicts the oldest anchor, so an old anchor that
 *     could have bounded a future O3 is gone even though it was valid.</li>
 *     <li><b>Reproduce the older-O3 boundary replay.</b> An out-of-order row within the
 *     surviving horizon resumes from the nearest retained anchor
 *     ({@code o3_resume_replay_rows}, the ring "win"), but a row older than the whole
 *     collapsed ring finds no anchor and falls back to the O(view age) boundary rebuild
 *     from the {@code START FROM} boundary ({@code o3_boundary_replay_rows}, the residual
 *     the timeline design removes).</li>
 * </ul>
 * These use {@code sum(x)} over a partitioned RANGE / ROWS frame - eligible dependency
 * kinds under the design's window matrix (section 6), unlike the unanchored
 * {@code row_number() OVER ()} the older ring/boundary smoke fixtures lean on, which Phase 0
 * step 4 removes. {@code sum} over small LONG values is bit-exact (section 6.1), so the
 * from-base recompute oracle can compare with exact equality rather than a float tolerance.
 * <p>
 * The class also carries the fixture's counterpart: once the RANGE dependency floor lands
 * (Phase 5 step 4a), the same sub-ring out-of-order row no longer costs the view's age. The
 * localization pair drives a longer history and measures what the rebuild actually read and
 * re-emitted, with the ROWS view as the still-unbounded control until Phase 6 gives it
 * per-key predecessor discovery.
 */
public class LiveViewCheckpointRingBoundaryFixtureTest extends AbstractLiveViewTest {

    // retention.count for the fixture. Pinned in-test rather than relying on the 8 default
    // so the collapse arithmetic (evictions = commits - budget) is hermetic.
    private static final int RETENTION_COUNT = 8;
    // In-order commits driven before any O3. More than RETENTION_COUNT so the ring collapses:
    // the final HISTORY_COMMITS - RETENTION_COUNT commits each evict the oldest anchor.
    private static final int HISTORY_COMMITS = 12;
    // The localization fixture drives a much longer history, so the rows below the
    // dependency floor outnumber the rows above it several times over and the bound is
    // visible rather than incidental. One commit per 10 seconds, two rows each.
    private static final int LOCALIZATION_HISTORY_COMMITS = 40;
    // Second-of-day of the out-of-order row: below every surviving anchor (the ring
    // retains only the top RETENTION_COUNT commits, 330s upward), so no resume qualifies
    // and the repair falls to the boundary rebuild - but high enough that most of the
    // history sits below the dependency floor.
    private static final int LOCALIZATION_O3_SECOND = 315;
    // Look-behind of the localization fixture's RANGE frame, in seconds.
    private static final int LOCALIZATION_RANGE_WIDTH_SECONDS = 30;

    @After
    public void unpinClock() {
        // currentMicros is a static that outlives the class; hand the next one a clean slate.
        setCurrentMicros(-1);
    }

    @Before
    public void pinClockBelowTestData() {
        // Below the 2026 test data, so a START FROM NOW view resolves its boundary below every
        // row it will ever see and admits them all, including the sub-ring O3.
        setCurrentMicros(0L);
    }

    @Test
    public void testRangeDependencyBoundsTheOldO3BoundaryRebuild() throws Exception {
        // The design's phase 5 step 4a win, on the fixture the pathology was built for.
        // The change is older than every surviving anchor, so the repair still runs the
        // boundary rebuild - but a finite RANGE look-behind gives it a floor the ring
        // could not: the state a row at R sees is exactly the rows in [R - W, R], so the
        // rebuild reads from R - W and re-emits from R.
        //
        // W is 30s over rows spaced 10s apart, so L lands at 285s: the scan admits the
        // three timestamp groups below the O3 row and everything above it. 290s..400s is
        // 12 groups of 2 rows, and the O3 commit adds 2 more.
        final ReplayCost cost = runOldO3BoundaryRebuild(
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '" + LOCALIZATION_RANGE_WIDTH_SECONDS
                        + "' SECOND PRECEDING AND CURRENT ROW"
        );
        Assert.assertEquals("the rebuild must read exactly [R - W, +inf)", 26, cost.scannedRows);
        // R is the O3 row's own timestamp (the live-view table's durable frontier sits
        // above it, so no non-durable output lowers the floor): 315s..400s is 9 groups of
        // 2 rows plus the O3 commit's 2.
        Assert.assertEquals("the rebuild must re-emit exactly [R, +inf)", 20, cost.emittedRows);
    }

    @Test
    public void testRangeSumRingCollapseThenOldO3BoundaryReplay() throws Exception {
        // Bounded RANGE frame: 30s look-behind over rows spaced 10s apart -> a 4-row frame.
        assertRingCollapsesThenOldO3ForcesBoundaryReplay(
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW"
        );
    }

    @Test
    public void testRowsDependencyLeavesTheOldO3BoundaryRebuildUnbounded() throws Exception {
        // The control. A ROWS frame carries no finite RANGE descriptor, so the plan
        // derives no floor and the same change costs the whole view history: every base
        // row is read and every output row re-emitted. Phase 6 bounds this shape through
        // per-key predecessor discovery; until then the difference against the RANGE case
        // above is exactly what the dependency floor buys.
        final ReplayCost cost = runOldO3BoundaryRebuild(
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
        );
        final long historyRows = 2L * LOCALIZATION_HISTORY_COMMITS + 2;
        Assert.assertEquals(historyRows, cost.scannedRows);
        Assert.assertEquals(historyRows, cost.emittedRows);
    }

    @Test
    public void testRowsSumRingCollapseThenOldO3BoundaryReplay() throws Exception {
        // Bounded ROWS frame: the current row and its 3 predecessors per partition.
        assertRingCollapsesThenOldO3ForcesBoundaryReplay(
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
        );
    }

    private void assertRingCollapsesThenOldO3ForcesBoundaryReplay(String windowFrame) throws Exception {
        // One head per flush -> a dense ring. The count budget is the binding retention bound:
        // the byte budget is left generous and the event-time horizon disabled so the collapse
        // is governed purely by RETENTION_COUNT.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, RETENTION_COUNT);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MAX_BYTES, 64L * 1024 * 1024);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MICROS, 0L);

        final String viewSql = "SELECT ts, sym, sum(x) OVER (" + windowFrame + ") AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // HISTORY_COMMITS in-order commits, one per 10 seconds, each sealing exactly one
                // head (checkpoint.rows = 1). Once RETENTION_COUNT heads exist the count budget
                // evicts the oldest, so the commits past the budget each drop one anchor.
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

                final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);

                // The ring collapsed. That heads sealed at all proves the window is
                // snapshot-capable; the count caps at the budget and the commits past it each
                // evicted one anchor. The oldest surviving anchor is the 5th commit's head, so
                // every anchor now sits at or above 50s - the four heads below it are gone.
                Assert.assertEquals(
                        "one head seals per flush, so the ring fills to the retention.count budget",
                        RETENTION_COUNT,
                        lv.getRetainedCheckpointCount()
                );
                Assert.assertEquals(
                        "the commits past the budget each evict exactly one anchor",
                        HISTORY_COMMITS - RETENTION_COUNT,
                        lv.getCheckpointRingEvictions()
                );
                Assert.assertEquals(
                        "the oldest surviving anchor is the first commit still inside the budget",
                        ts(secondsTs(50)),
                        lv.getRetainedCheckpointMaxTs(0)
                );

                // In-order forward appends never replay: both O3 counters stay at 0. The view is
                // exactly the from-base recompute at this point.
                assertQuery("SELECT o3_resume_replay_rows, o3_boundary_replay_rows FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("o3_resume_replay_rows\to3_boundary_replay_rows\n0\t0\n");
                assertViewMatchesRecompute(viewSql);

                // An O3 row within the retained horizon: 55s is above the oldest surviving anchor
                // (50s) and below the head (120s), so the replay resumes from that anchor and
                // re-emits only the tail above it. The ring bounds the work - this is its win -
                // and the boundary counter stays untouched.
                setCurrentMicros((HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(55) + "', 'a', 55), " +
                        "('" + secondsTs(55) + "', 'b', 155)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue(
                        "an O3 within the ring horizon must resume from an anchor",
                        lv.getO3ResumeReplayRows() > 0
                );
                Assert.assertEquals(
                        "a bounded resume must not take the boundary rebuild",
                        0,
                        lv.getO3BoundaryReplayRows()
                );
                assertViewMatchesRecompute(viewSql);

                // An O3 row older than the whole collapsed ring: 5s is below every surviving
                // anchor, so no anchor qualifies and the replay falls back to the O(view age)
                // boundary rebuild from the START FROM boundary. This is precisely the residual
                // the versioned timeline removes - the evicted sub-50s anchors would have bounded
                // it. A boundary rebuild is a normal O3 path, not a refresh fault, so the view
                // still converges to the from-base recompute (asserted by assertViewMatchesRecompute).
                final long boundaryBefore = lv.getO3BoundaryReplayRows();
                final long resumeBefore = lv.getO3ResumeReplayRows();
                setCurrentMicros((HISTORY_COMMITS + 2) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(5) + "', 'a', 5), " +
                        "('" + secondsTs(5) + "', 'b', 105)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue(
                        "an O3 older than the whole ring must fall back to the boundary rebuild",
                        lv.getO3BoundaryReplayRows() > boundaryBefore
                );
                // The two paths are disjoint: the boundary rebuild leaves the resume counter
                // untouched, so a growing boundary count isolates the ring's residual cost.
                Assert.assertEquals(
                        "the boundary rebuild must not also bump the resume counter",
                        resumeBefore,
                        lv.getO3ResumeReplayRows()
                );
                assertViewMatchesRecompute(viewSql);
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // The live view must equal the same window recomputed directly over the base table. The
    // view's stored columns are exactly the projection it was created from, so (lv) and (viewSql)
    // share a schema. ORDER BY 2, 1 (sym, ts) gives both sides a total order; genericStringMatch
    // tolerates the SYMBOL-vs-STRING passthrough difference. A refresh fault self-heals into a full
    // recompute this oracle would match either way, so assertNoRefreshFaults guards that the view
    // converged through the real incremental / replay paths rather than a recovery rebuild.
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

    // Drives a long in-order history, collapses the ring onto its top RETENTION_COUNT anchors,
    // then commits one out-of-order row below every one of them so the repair has to take the
    // boundary rebuild. Returns what that rebuild cost: base rows read and output rows
    // re-emitted. Both counters are asserted zero before the out-of-order commit, so the
    // values that come back are that one repair's and nothing else's. The view is checked
    // against the from-base recompute either way - a bounded rebuild that gets the answer
    // wrong is worse than an unbounded one.
    private ReplayCost runOldO3BoundaryRebuild(String windowFrame) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, RETENTION_COUNT);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MAX_BYTES, 64L * 1024 * 1024);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MICROS, 0L);

        final String viewSql = "SELECT ts, sym, sum(x) OVER (" + windowFrame + ") AS s FROM base";
        final ReplayCost cost = new ReplayCost();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= LOCALIZATION_HISTORY_COMMITS; commit++) {
                    setCurrentMicros(commit * 200_000L);
                    final String rowTs = secondsTs(commit * 10);
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('" + rowTs + "', 'a', " + commit + "), " +
                            "('" + rowTs + "', 'b', " + (commit + 100) + ")");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }

                final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertEquals(RETENTION_COUNT, lv.getRetainedCheckpointCount());
                // The oldest surviving anchor sits above the O3 row, so no resume can
                // qualify and the boundary rebuild is the path under measurement.
                Assert.assertTrue(
                        "the O3 row must sit below every surviving anchor",
                        lv.getRetainedCheckpointMaxTs(0) > ts(secondsTs(LOCALIZATION_O3_SECOND))
                );
                // In-order appends never replay, so the counters start clean.
                Assert.assertEquals(0, lv.getO3ReplayScanRows());
                Assert.assertEquals(0, lv.getO3BoundaryReplayRows());
                assertViewMatchesRecompute(viewSql);

                setCurrentMicros((LOCALIZATION_HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(LOCALIZATION_O3_SECOND) + "', 'a', 9000), " +
                        "('" + secondsTs(LOCALIZATION_O3_SECOND) + "', 'b', 9100)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "a change below the whole ring must take the boundary rebuild",
                        0,
                        lv.getO3ResumeReplayRows()
                );
                cost.scannedRows = lv.getO3ReplayScanRows();
                cost.emittedRows = lv.getO3BoundaryReplayRows();
                assertViewMatchesRecompute(viewSql);
            }

            execute("DROP LIVE VIEW lv");
        });
        return cost;
    }

    // Builds a 2026-11-01 microsecond timestamp literal at the given second-of-day offset. All
    // fixture rows share one calendar day, so the base's DAY partition never enters the picture.
    private static String secondsTs(int secondOfDay) {
        final int mm = secondOfDay / 60;
        final int ss = secondOfDay % 60;
        return String.format("2026-11-01T00:%02d:%02d.000000Z", mm, ss);
    }

    // What one boundary rebuild cost: base rows the source cursor pulled and output rows it
    // re-emitted. Mutable because assertMemoryLeak takes a void lambda and the numbers have to
    // leave it.
    private static class ReplayCost {
        private long emittedRows;
        private long scannedRows;
    }
}
