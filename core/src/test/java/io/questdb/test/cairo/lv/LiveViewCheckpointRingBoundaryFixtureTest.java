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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Baseline fixtures for the versioned checkpoint timeline: deterministic RANGE
 * and ROWS window views with enough history to collapse the retained checkpoint
 * ring and then reproduce an older out-of-order boundary replay.
 * <p>
 * The scenario reproduces, on eligible window shapes, exactly the pathology the
 * timeline replaces:
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
 *     the timeline removes).</li>
 * </ul>
 * These use {@code sum(x)} over a partitioned RANGE / ROWS frame - eligible
 * dependency kinds under the supported-window matrix, unlike the unanchored
 * {@code row_number() OVER ()} the older ring/boundary smoke fixtures leant on,
 * which the finite-influence scope cut removes. {@code sum} over small LONG
 * values is bit-exact, so the from-base recompute oracle can compare with exact
 * equality rather than a float tolerance.
 * <p>
 * The class also carries the fixture's counterpart: with the dependency bounds
 * in place, the same sub-ring out-of-order row no longer costs the view's age in
 * either direction. RANGE derives its two ends by arithmetic ({@code R - W} and
 * {@code changeMaxTs + W}); ROWS discovers them by counting each key's rows
 * either side of the change, and pays a scan of its own to do so. The
 * localization group drives a longer history, measures what each rebuild
 * actually read and re-emitted, and then keeps ingesting in order so the runtime
 * state a converging repair restored is exercised rather than only the output it
 * wrote. Its last case is the deleting change set the ROWS bound refuses, which
 * is what the unbounded rebuild still exists for.
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
    // Anchor of the localization fixture's anchored views. Buckets by the minute over
    // rows spaced 10s apart, so one segment holds six of the history's commits and the
    // out-of-order row at 315s lands in [300s, 360s).
    private static final String LOCALIZATION_ANCHOR_EXPRESSION = "timestamp_floor('1m', ts)";

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
    public void testAnchorSegmentBoundsAnAnchoredRankingRebuild() throws Exception {
        // The shape the finite-influence scope cut turned away unanchored and gave back
        // anchored. row_number() has no frame to bound anything with - which is exactly
        // why the unanchored form has no finite H - but the anchor restarts its counter
        // at every bucket, and that restart is the whole contract. The bounds are the
        // cumulative sum's, to the row: they come from the segment, not the function.
        final ReplayCost cost = runAnchoredOldO3BoundaryRebuild("row_number()");
        Assert.assertEquals("the rebuild must read exactly the change's segment", 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 10, cost.emittedRows);
    }

    @Test
    public void testAnchorSegmentBoundsTheOldO3BoundaryRebuild() throws Exception {
        // The third way to reach the same two-sided bound, and the one that needs no
        // frame at all. An anchored cumulative sum resets on every bucket crossing, so
        // one segment is a wall in both directions: the state a row at R holds is the
        // rows from its segment's start, and the change reaches no output past that
        // segment's end. Both walls follow from the designated timestamp alone, so
        // unlike the ROWS discovery the planning reads no base row to find them.
        //
        // The anchor buckets by the minute over rows spaced 10s apart, and the change
        // lands at 315s. L is 300s and H 360s, so the scan admits 300s..350s - six groups
        // of 2 rows - plus the O3 commit's own 2 at 315s, and the replacement re-emits
        // 315s..350s.
        final ReplayCost cost = runAnchoredOldO3BoundaryRebuild("sum(x)");
        Assert.assertEquals("the rebuild must read exactly the change's segment", 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 10, cost.emittedRows);
        // The whole-history rebuild this replaces, for scale.
        Assert.assertEquals(82, 2L * LOCALIZATION_HISTORY_COMMITS + 2);
    }

    @Test
    public void testAnchorSegmentDeclinesAViewTheAnchorDoesNotWhollyReset() throws Exception {
        // The safety boundary of the anchor bound, and the one the anchor clause alone
        // cannot see. The runtime dispatches the reset only to the functions whose frame
        // is UNBOUNDED PRECEDING ... CURRENT ROW, so the bounded ROWS window declared
        // beside the anchored one keeps sliding across every bucket crossing - its state
        // reaches below the segment start, and a repair localized on the segment would
        // warm it up over rows the frame does not contain. The whole factory therefore
        // declines the plan and pays the unbounded rebuild.
        final String slidingWindow = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r";
        final String viewSql = "SELECT ts, sym, sum(x) OVER w AS s, " + slidingWindow + " FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + LOCALIZATION_ANCHOR_EXPRESSION + ")";
        final String oracleSql = "SELECT ts, sym, sum(x) OVER (PARTITION BY sym, " + LOCALIZATION_ANCHOR_EXPRESSION
                + " ORDER BY ts) AS s, " + slidingWindow + " FROM base";
        final ReplayCost cost = runOldO3BoundaryRebuild(viewSql, oracleSql, false);
        final long historyRows = 2L * LOCALIZATION_HISTORY_COMMITS + 2;
        Assert.assertEquals("no bound is derived, so the whole history is read", historyRows, cost.scannedRows);
        Assert.assertEquals("and re-emitted", historyRows, cost.emittedRows);
    }

    @Test
    public void testRangeDependencyBoundsTheOldO3BoundaryRebuild() throws Exception {
        // The two-sided RANGE bound, on the fixture the pathology was built
        // for. The change is older than every surviving anchor, so the repair
        // still runs the boundary rebuild - but a finite RANGE look-behind
        // closes it at both ends: the state a row at R sees is exactly the rows
        // in [R - W, R], and a row at m sits in the frame of every row in
        // [m, m + W] and no other.
        //
        // W is 30s over rows spaced 10s apart. L lands at 285s and H one microsecond
        // past 345s, so the scan admits 290s..345s - six groups of 2 rows - plus the O3
        // commit's own 2 at 315s.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '" + LOCALIZATION_RANGE_WIDTH_SECONDS
                        + "' SECOND PRECEDING AND CURRENT ROW"
        );
        Assert.assertEquals("the rebuild must read exactly [R - W, changeMaxTs + W]", 14, cost.scannedRows);
        // R is the O3 row's own timestamp (the live-view table's durable frontier sits
        // above it, so no non-durable output lowers the floor): 315s..345s is the O3
        // commit's 2 rows plus three groups of 2.
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
    }

    @Test
    public void testRangeSumRingCollapseThenOldO3BoundaryReplay() throws Exception {
        // Bounded RANGE frame: 30s look-behind over rows spaced 10s apart -> a 4-row frame.
        assertRingCollapsesThenOldO3ForcesBoundaryReplay(
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW"
        );
    }

    @Test
    public void testRowsDependencyBoundsTheOldO3BoundaryRebuild() throws Exception {
        // The same two-sided bound over the same fixture, reached by counting rows
        // rather than by timestamp arithmetic. Nmax is 3 and each key has one row per
        // 10-second group, so the change at 315s reaches its key's 340s row and no
        // further: H is 350s, the first distinct timestamp above it. Below the floor
        // the count runs the other way - 310s, 300s and 290s give each key its three
        // predecessors - so L is 290s. The replay reads 290s..340s (six groups of two)
        // and re-emits 315s..340s (four groups).
        //
        // Unlike the RANGE case, the bounds cost reads of their own: the forward pass
        // pulls nine rows to learn H and the backward walk six to learn L, and those
        // fifteen join the same base-rows-scanned counter the replay reports through.
        // 29 against the 82 an unbounded rebuild reads is what the discovery buys here,
        // and a shape whose keys are sparser buys proportionally less.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
        );
        Assert.assertEquals("bound discovery plus the [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
        // The whole-history rebuild this replaces, for scale.
        Assert.assertEquals(82, 2L * LOCALIZATION_HISTORY_COMMITS + 2);
    }

    @Test
    public void testRowsDependencyDeclinesADeletingChangeSet() throws Exception {
        // The safety boundary of the ROWS bound. The affected key domain is read back
        // out of the post-change snapshot, which only describes the keys a change
        // ADDED: a deletion that emptied a key's rows out of the change interval leaves
        // it invisible there while its later rows still pull older history into their
        // frames, so H would land below where that key actually converges. The repair
        // therefore refuses the bound outright unless it can prove the whole
        // incorporated change set insert-only.
        //
        // Here the same two rows arrive inside a replace-range band, so the base ends up
        // holding exactly what the insert above leaves it and the output is identical -
        // only the commit's authority to delete differs, and it costs the localization.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "sum(x)",
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                true
        );
        final long historyRows = 2L * LOCALIZATION_HISTORY_COMMITS + 2;
        Assert.assertEquals("no bound is discovered, so the whole history is read", historyRows, cost.scannedRows);
        Assert.assertEquals("and re-emitted", historyRows, cost.emittedRows);
    }

    @Test
    public void testRowsDependencyDeclinesAFunctionReachingOutsideItsFrame() throws Exception {
        // The other safety boundary of the ROWS bound, and the one the frame shape alone
        // cannot see. The bound is the frame's own extent, so it only describes a function
        // whose state that extent determines. lag() counts predecessors by its own offset -
        // five here, through a frame that promises three - so a repair localized on the
        // frame would warm it up over three rows and emit NULL where the sixth row back
        // belongs. The whole factory therefore declines the plan and pays the unbounded
        // rebuild, which reconstructs every function from the START FROM boundary and needs
        // no dependency floor at all.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "lag(x, 5)",
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                false
        );
        final long historyRows = 2L * LOCALIZATION_HISTORY_COMMITS + 2;
        Assert.assertEquals("no bound is discovered, so the whole history is read", historyRows, cost.scannedRows);
        Assert.assertEquals("and re-emitted", historyRows, cost.emittedRows);
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

    /**
     * The anchored shape of the fixture: one window function on a named WINDOW that
     * buckets by the minute, whose oracle spells the same segmentation as an extra
     * PARTITION BY term. Every accepted anchored function reaches the same bounds
     * through it, because the segment and not the function is what produces them.
     */
    private ReplayCost runAnchoredOldO3BoundaryRebuild(String windowExpression) throws Exception {
        final String viewSql = "SELECT ts, sym, " + windowExpression + " OVER w AS s FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + LOCALIZATION_ANCHOR_EXPRESSION + ")";
        final String oracleSql = "SELECT ts, sym, " + windowExpression + " OVER (PARTITION BY sym, "
                + LOCALIZATION_ANCHOR_EXPRESSION + " ORDER BY ts) AS s FROM base";
        return runOldO3BoundaryRebuild(viewSql, oracleSql, false);
    }

    /**
     * Drives a long in-order history, collapses the ring onto its top RETENTION_COUNT
     * anchors, then commits one out-of-order row below every one of them so the repair
     * has to take the boundary rebuild. Returns what that rebuild cost: base rows read
     * and output rows re-emitted. Both counters are asserted zero before the
     * out-of-order commit, so the values that come back are that one repair's and
     * nothing else's. The view is checked against the from-base recompute either way -
     * a bounded rebuild that gets the answer wrong is worse than an unbounded one - and
     * then again after three further in-order commits, which is what proves the runtime
     * state and not merely the durable output.
     * <p>
     * The view and its oracle are the same statement for a frame-bounded view, but an
     * anchored one has to declare its ANCHOR on a named WINDOW - syntax only a live view
     * accepts - so its oracle spells the same segmentation as an extra PARTITION BY
     * term.
     * <p>
     * With {@code o3AsReplaceRange} the same two rows arrive inside a replace-range band
     * instead of a plain insert: the data the base ends up holding is identical, and so
     * is the output, but the commit now carries a deletion the repair cannot see the
     * effect of.
     */
    private ReplayCost runOldO3BoundaryRebuild(
            String viewSql,
            String oracleSql,
            boolean o3AsReplaceRange
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, RETENTION_COUNT);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MAX_BYTES, 64L * 1024 * 1024);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MICROS, 0L);

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
                assertViewMatchesRecompute(oracleSql);

                setCurrentMicros((LOCALIZATION_HISTORY_COMMITS + 1) * 200_000L);
                if (o3AsReplaceRange) {
                    final TableToken baseToken = engine.verifyTableName("base");
                    try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                        appendFixtureRow(walWriter, ts(secondsTs(LOCALIZATION_O3_SECOND)), "a", 9000);
                        appendFixtureRow(walWriter, ts(secondsTs(LOCALIZATION_O3_SECOND)), "b", 9100);
                        // A band holding nothing but the two rows it inserts, so the
                        // base ends up exactly where the plain insert leaves it.
                        walWriter.commitWithParams(
                                ts(secondsTs(LOCALIZATION_O3_SECOND)),
                                ts(secondsTs(LOCALIZATION_O3_SECOND)) + 1,
                                WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                        );
                    }
                } else {
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('" + secondsTs(LOCALIZATION_O3_SECOND) + "', 'a', 9000), " +
                            "('" + secondsTs(LOCALIZATION_O3_SECOND) + "', 'b', 9100)");
                }
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
                assertViewMatchesRecompute(oracleSql);

                // Keep ingesting in order. A repair that converged below the frontier
                // left the runtime state it entered with in place rather than the state
                // its replay ended on, and only these follow-up rows can tell the two
                // apart: they are evaluated incrementally against whatever the repair
                // left behind, so a runtime rewound to the convergence boundary would
                // compute their frames over rows the state no longer holds. The output
                // the repair already wrote would look right either way.
                for (int commit = LOCALIZATION_HISTORY_COMMITS + 2; commit <= LOCALIZATION_HISTORY_COMMITS + 4; commit++) {
                    setCurrentMicros(commit * 200_000L);
                    final String rowTs = secondsTs(commit * 10);
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('" + rowTs + "', 'a', " + commit + "), " +
                            "('" + rowTs + "', 'b', " + (commit + 100) + ")");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
                Assert.assertEquals(
                        "in-order rows after the repair must append, not replay",
                        cost.emittedRows,
                        lv.getO3BoundaryReplayRows()
                );
                assertViewMatchesRecompute(oracleSql);
            }

            execute("DROP LIVE VIEW lv");
        });
        return cost;
    }

    private ReplayCost runOldO3BoundaryRebuildOverFrame(String windowFrame) throws Exception {
        return runOldO3BoundaryRebuildOverFrame("sum(x)", windowFrame, false);
    }

    /**
     * The frame-bounded shape of the fixture: one window function over one frame, whose
     * live-view statement doubles as its own from-base oracle.
     *
     * @param windowExpression the view's single window function, applied over
     *                         {@code windowFrame}
     */
    private ReplayCost runOldO3BoundaryRebuildOverFrame(
            String windowExpression,
            String windowFrame,
            boolean o3AsReplaceRange
    ) throws Exception {
        final String sql = "SELECT ts, sym, " + windowExpression + " OVER (" + windowFrame + ") AS s FROM base";
        return runOldO3BoundaryRebuild(sql, sql, o3AsReplaceRange);
    }

    // Appends one fixture row - (ts, sym, x) - without committing, so the caller chooses the
    // commit mode. AbstractLiveViewTest's appendRow assumes a two-column (ts, x) base.
    private static void appendFixtureRow(WalWriter walWriter, long ts, CharSequence sym, long x) {
        TableWriter.Row row = walWriter.newRow(ts);
        row.putSym(1, sym);
        row.putLong(2, x);
        row.append();
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
