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
 * and ROWS window views with enough history to reach an out-of-order row older
 * than every logical checkpoint boundary, and to price the two dispositions
 * against each other for one that is not.
 * <p>
 * The scenario covers, on eligible window shapes, both ends of the repair:
 * <ul>
 *     <li><b>A change with a boundary below it.</b> The plan prices the resume
 *     from that boundary - which reads to the end of the base table - against the
 *     localized rebuild's {@code [L, H)}, and takes the cheaper. The counters
 *     ({@code o3_resume_replay_rows} versus {@code o3_boundary_replay_rows}) say
 *     which ran.</li>
 *     <li><b>A change below every boundary.</b> No resume can qualify, so the
 *     rebuild is the only disposition left - and the finite dependency is what
 *     keeps it off the {@code START FROM} boundary.</li>
 * </ul>
 * These use {@code sum(x)} over a partitioned RANGE / ROWS frame - eligible
 * dependency kinds under the supported-window matrix, unlike the unanchored
 * {@code row_number() OVER ()} the older boundary smoke fixtures leant on,
 * which the finite-influence scope cut removes. {@code sum} over small LONG
 * values is bit-exact, so the from-base recompute oracle can compare with exact
 * equality rather than a float tolerance.
 * <p>
 * The class also carries the fixture's counterpart: with the dependency bounds
 * in place, an out-of-order row below every boundary no longer costs the view's
 * age in either direction. RANGE derives its two ends by arithmetic
 * ({@code R - W} and {@code changeMaxTs + W}); ROWS discovers them by counting
 * each key's rows either side of the change, and pays a scan of its own to do
 * so. The localization group drives a longer history, measures what each rebuild
 * actually read and re-emitted, and then keeps ingesting in order so the runtime
 * state a converging repair restored is exercised rather than only the output it
 * wrote. Its last case is the deleting change set the ROWS bound refuses, which
 * is what the unbounded rebuild still exists for.
 */
public class LiveViewCheckpointBoundaryFixtureTest extends AbstractLiveViewTest {

    // In-order commits driven before any O3, one per 10 seconds.
    private static final int HISTORY_COMMITS = 12;
    // The localization fixture drives a much longer history, so the rows below the
    // dependency floor outnumber the rows above it several times over and the bound is
    // visible rather than incidental. One commit per 10 seconds, two rows each.
    private static final int LOCALIZATION_HISTORY_COMMITS = 40;
    // Second-of-day of the out-of-order row: deep enough in history that most of it
    // sits below the dependency floor, so the localized rebuild is priced well under
    // the resume that would replay every row above the boundary beneath it.
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
    public void testAnchorSegmentAndRowsFrameBoundOneRebuildTogether() throws Exception {
        // A view carrying two dependency shapes at once: an anchored cumulative sum the
        // minute bucket resets, and a bounded ROWS window declared beside it that keeps
        // sliding across every crossing. Neither shape describes the other's state, so the
        // repair bounds them together - the earliest L and the latest H the two prove -
        // and here the arms trade ends. The discovery answers L = 290s and H = 350s for
        // the ROWS window; the segment answers 300s and 360s. The union reads from 290s
        // (the ROWS floor, deeper) and stops at 360s (the segment's end, later), which
        // satisfies the sliding frame's warm-up and the anchored function's convergence at
        // the same time.
        //
        // The read is 290s..350s - seven groups of 2 - plus the O3 commit's own 2 at 315s,
        // and the replacement re-emits 315s..350s. The 15-row discovery is the ROWS arm's
        // and the segment adds none: its bounds are arithmetic over the designated
        // timestamp.
        final String slidingWindow = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r";
        final String viewSql = "SELECT ts, sym, sum(x) OVER w AS s, " + slidingWindow + " FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + LOCALIZATION_ANCHOR_EXPRESSION + ")";
        final String oracleSql = "SELECT ts, sym, sum(x) OVER (PARTITION BY sym, " + LOCALIZATION_ANCHOR_EXPRESSION
                + " ORDER BY ts) AS s, " + slidingWindow + " FROM base";
        final ReplayCost cost = runOldO3BoundaryRebuild(viewSql, oracleSql, false);
        Assert.assertEquals("bound discovery plus the union's [L, H) rebuild", 15 + 16, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 10, cost.emittedRows);
        // The whole-history rebuild this replaces, for scale.
        Assert.assertEquals(82, 2L * LOCALIZATION_HISTORY_COMMITS + 2);
    }

    @Test
    public void testAnchorSegmentBoundsAnAnchoredRankingRebuild() throws Exception {
        // The shape the finite-influence scope cut turned away unanchored and gave back
        // anchored. row_number() has no frame to bound anything with - which is exactly
        // why the unanchored form has no finite H - but the anchor restarts its counter
        // at every bucket, and that restart is the whole contract. The bounds are the
        // cumulative sum's, to the row: they come from the segment, not the function.
        final ReplayCost cost = runAnchoredOldO3BoundaryRebuild("row_number()");
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
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
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
        Assert.assertEquals("the rebuild must read exactly the change's segment", 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 10, cost.emittedRows);
        // The whole-history rebuild this replaces, for scale.
        Assert.assertEquals(82, 2L * LOCALIZATION_HISTORY_COMMITS + 2);
    }

    @Test
    public void testAnchorSegmentDeclinesAViewWithAnUncoveredFunction() throws Exception {
        // The safety boundary of the union. Two shapes bounding two functions is only a
        // bound over the view while every function sits inside one of them, and lag()
        // sits inside neither: it counts predecessors by its own offset - five here,
        // through a frame that promises three - so the ROWS plan declines it and the
        // anchor does not reset it either. The replacement over [R, H) is
        // timestamp-global and re-emits its column from the same replay, so one
        // uncovered function costs the whole view its localization rather than only its
        // own arm.
        final String slidingWindow = "lag(x, 5) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r";
        final String viewSql = "SELECT ts, sym, sum(x) OVER w AS s, " + slidingWindow + " FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + LOCALIZATION_ANCHOR_EXPRESSION + ")";
        final String oracleSql = "SELECT ts, sym, sum(x) OVER (PARTITION BY sym, " + LOCALIZATION_ANCHOR_EXPRESSION
                + " ORDER BY ts) AS s, " + slidingWindow + " FROM base";
        final ReplayCost cost = runOldO3BoundaryRebuild(viewSql, oracleSql, false);
        assertResumeBoundsAnUnlocalizableRepair(cost);
    }

    @Test
    public void testRangeAndRowsFramesBoundOneRebuildTogether() throws Exception {
        // The other mixed factory: a bounded RANGE window and a bounded ROWS one, whose
        // bounds are the fixture's own 285s/345s and 290s/350s. The arms trade ends here
        // too - the RANGE width reaches deeper below the floor, the row count reaches
        // further above the change - so the union reads from 285s and stops at 350s.
        //
        // The read is the ROWS case's to the row: no fixture row sits in [285s, 290s), so
        // the deeper floor costs nothing, while the later ceiling is the one the ROWS arm
        // would have set anyway. What the case pins is that neither plan was dropped in
        // favour of the other, which a repair taking only the first shape it found would
        // do - and that the two functions converge to the from-base recompute together.
        final String viewSql = "SELECT ts, sym, "
                + "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '"
                + LOCALIZATION_RANGE_WIDTH_SECONDS + "' SECOND PRECEDING AND CURRENT ROW) AS s, "
                + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r "
                + "FROM base";
        final ReplayCost cost = runOldO3BoundaryRebuild(viewSql, viewSql, false);
        Assert.assertEquals("bound discovery plus the union's [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
    }

    @Test
    public void testRangeDependencyAdmitsTheCompleteTieAtItsHighBound() throws Exception {
        // H is exclusive one microsecond past changeMaxTs + W, not at it, so the
        // complete timestamp tie the change still reaches sits inside the
        // replacement. A RANGE W PRECEDING frame at changeMaxTs + W spans
        // [changeMaxTs, changeMaxTs + W] and therefore holds the changed row;
        // a bound that stopped one tie early would leave that row's durable
        // output carrying the pre-change sum, with nothing above it to correct
        // later.
        //
        // The fixture is what makes the tie observable: W is 30s over rows spaced
        // 10s apart and the out-of-order row lands on 30s, so changeMaxTs + W is
        // 60s - a timestamp the history already holds. Off that alignment the
        // bound could be short by one tie and no row would sit there to notice.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final String viewSql = "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
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

                final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                assertViewMatchesRecompute(viewSql);

                // A second row on 30s, a timestamp the history already sealed a
                // boundary at, so changeMaxTs is 30s and the tie lands on 60s.
                setCurrentMicros((HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(30) + "', 'a', 7), " +
                        "('" + secondsTs(30) + "', 'b', 107)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "the replacement must re-emit [30s, 60s] - four rows on 30s and two on each of 40s, 50s, 60s",
                        10,
                        lv.getO3BoundaryReplayRows()
                );
                // The row on the tie is the one a short bound would strand, and
                // the recompute is what says whether it was corrected: the sum
                // there is 25 with the change and 18 without it.
                assertViewMatchesRecompute(viewSql);
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRangeDependencyBoundsADecimalSumRebuild() throws Exception {
        // The sum/avg family's fixed-point arm. It buffers and accumulates what the DOUBLE arm
        // does, so the bounds are again the fixture's 14 and 8 - what changes is the claim the
        // accumulator can make. A re-accumulated floating sum keeps a rounding difference the
        // contract tolerates; adding and subtracting fixed-point values is exact, so the
        // repaired output has to equal the from-base recompute to the last digit, which is what
        // the oracle's exact cursor comparison holds it to here.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "sum(x::decimal(18, 3))",
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '" + LOCALIZATION_RANGE_WIDTH_SECONDS
                        + "' SECOND PRECEDING AND CURRENT ROW",
                false
        );
        Assert.assertEquals("the rebuild must read exactly [R - W, changeMaxTs + W]", 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
    }

    @Test
    public void testRangeDependencyBoundsAMaxRebuild() throws Exception {
        // The max/min family on the RANGE arm. Its state is a ring of the frame's own rows
        // plus a monotonic deque over exactly those, so the frame bounds it as it bounds the
        // sum - and the bounds are the sum's to the row, because they are read off the frame
        // and not off the function. What the deque adds is a state the replay cannot
        // reproduce byte for byte: its capacity and rotation follow the number of rows the
        // partition ever saw, and a warm-up starts from an empty one. Only the values it
        // frames have to converge, which is what the recompute oracle checks here - and the
        // out-of-order row carries the largest value in the fixture, so every frame it
        // enters changes its answer.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "max(x)",
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '" + LOCALIZATION_RANGE_WIDTH_SECONDS
                        + "' SECOND PRECEDING AND CURRENT ROW",
                false
        );
        Assert.assertEquals("the rebuild must read exactly [R - W, changeMaxTs + W]", 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
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
    public void testRangeSumOldO3BelowEveryBoundaryReplays() throws Exception {
        // Bounded RANGE frame: 30s look-behind over rows spaced 10s apart -> a 4-row frame.
        assertOldO3BelowEveryBoundaryForcesBoundaryReplay(
                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW"
        );
    }

    @Test
    public void testRowsDependencyBoundsADecimalAvgRebuild() throws Exception {
        // The same arm on the ROWS side, through avg() rather than sum(), which divides the
        // accumulator by the frame's own count. Both converge exactly, so the quotient does
        // too - a rounded HALF_EVEN quotient is a function of the pair and nothing else. The
        // discovery and the rebuild cost what the LONG sum's do.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "avg(x::decimal(18, 3))",
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                false
        );
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
        Assert.assertEquals("bound discovery plus the [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
    }

    @Test
    public void testRowsDependencyBoundsAFilteredRebuild() throws Exception {
        // The view's WHERE is what "qualifying" means, and both searches have to mean the
        // same thing by it as the replay does. Here it admits one of the fixture's two
        // keys, so every bound is made of half as many rows as the unfiltered case - and
        // costs exactly as many reads, because a filter reads the rows it rejects.
        //
        // The bounds land where the unfiltered ones do: key 'a' still reaches its third
        // following row at 340s and its third predecessor at 290s. What changes is the
        // replacement, which now re-emits four rows rather than eight over the same
        // interval.
        final String sql = "SELECT ts, sym, sum(x) OVER ("
                + "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s "
                + "FROM base WHERE sym = 'a'";
        final ReplayCost cost = runOldO3BoundaryRebuild(sql, sql, false);
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
        Assert.assertEquals("bound discovery plus the [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild re-emits only what the WHERE admits", 4, cost.emittedRows);
    }

    @Test
    public void testRowsDependencyBoundsAMinRebuild() throws Exception {
        // The same family on the ROWS arm, and through min() rather than max(), which is the
        // same implementation under a reversed comparator. Nmax bounds the ring and the
        // deque alike, so the discovery and the rebuild cost what the sum's do.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "min(x)",
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                false
        );
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
        Assert.assertEquals("bound discovery plus the [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
    }

    @Test
    public void testRowsDependencyBoundsAnExpressionKeyedRebuild() throws Exception {
        // A PARTITION BY the key projector has to compile rather than read off a column.
        // upper(sym) partitions the fixture exactly as sym does, so the discovery answers
        // the same H and L and the rebuild reads and re-emits the same rows - which is the
        // claim worth pinning: an expression key costs a view its index seek, not its
        // repair bound. This fixture's sym carries no index, so here it costs nothing at
        // all.
        final ReplayCost cost = runOldO3BoundaryRebuildOverFrame(
                "sum(x)",
                "PARTITION BY upper(sym) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                false
        );
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
        Assert.assertEquals("bound discovery plus the [L, H) rebuild", 15 + 14, cost.scannedRows);
        Assert.assertEquals("the rebuild must re-emit exactly [R, H)", 8, cost.emittedRows);
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
        Assert.assertEquals("the localized rebuild must win on price", 0, cost.resumedRows);
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
        assertResumeBoundsAnUnlocalizableRepair(cost);
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
        assertResumeBoundsAnUnlocalizableRepair(cost);
    }

    @Test
    public void testRowsSumOldO3BelowEveryBoundaryReplays() throws Exception {
        // Bounded ROWS frame: the current row and its 3 predecessors per partition.
        assertOldO3BelowEveryBoundaryForcesBoundaryReplay(
                "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
        );
    }

    private void assertOldO3BelowEveryBoundaryForcesBoundaryReplay(String windowFrame) throws Exception {
        // One boundary per flush -> a dense timeline, so the O3 at 55s has one just
        // below it and the O3 at 5s has none.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);

        final String viewSql = "SELECT ts, sym, sum(x) OVER (" + windowFrame + ") AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // HISTORY_COMMITS in-order commits, one per 10 seconds, each sealing exactly one
                // logical boundary (checkpoint.rows = 1). None is ever removed.
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

                // That boundaries sealed at all proves the window is snapshot-capable.
                Assert.assertEquals(
                        "the newest boundary sits at the last in-order commit",
                        ts(secondsTs(HISTORY_COMMITS * 10)),
                        lv.getHeadCheckpointMaxTs()
                );

                // In-order forward appends never replay: both O3 counters stay at 0. The view is
                // exactly the from-base recompute at this point.
                assertQuery("SELECT o3_resume_replay_rows, o3_boundary_replay_rows FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("o3_resume_replay_rows\to3_boundary_replay_rows\n0\t0\n");
                assertViewMatchesRecompute(viewSql);

                // An O3 row with a boundary below it: 55s sits above the boundary at 50s and
                // below the newest at 120s, so a resume qualifies. It does not run. A resume
                // reads to the end of the base table, which is the 16 rows above 50s, while
                // the bounded frame's [L, H) holds 14 - so the plan prices the localized
                // rebuild lower and takes it. Both dispositions bound the work; what decides
                // between them is which reads fewer rows.
                setCurrentMicros((HISTORY_COMMITS + 1) * 200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('" + secondsTs(55) + "', 'a', 55), " +
                        "('" + secondsTs(55) + "', 'b', 155)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "a resume priced above the localized rebuild must not run",
                        0,
                        lv.getO3ResumeReplayRows()
                );
                Assert.assertEquals(
                        "the localized rebuild re-emits [55s, 85s): 55s, 60s, 70s, 80s over two keys",
                        8,
                        lv.getO3BoundaryReplayRows()
                );
                assertViewMatchesRecompute(viewSql);

                // An O3 row below every boundary: 5s sits under the first one at 10s, so no
                // resume qualifies at all and the rebuild is the only disposition left. The
                // finite dependency is what keeps it bounded - it reads [L, H) rather than
                // everything from the START FROM boundary. A boundary rebuild is a normal O3
                // path, not a refresh fault, so the view still converges to the from-base
                // recompute (asserted by assertViewMatchesRecompute).
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
                        "an O3 below every boundary must fall back to the boundary rebuild",
                        lv.getO3BoundaryReplayRows() > boundaryBefore
                );
                // The two paths are disjoint: the boundary rebuild leaves the resume counter
                // untouched, so a growing boundary count isolates the residual cost.
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

    /**
     * What a repair that cannot localize costs now that every logical boundary is
     * retained. The plan prices a rebuild that reads the whole view history against a
     * resume from the boundary immediately below the change, and the resume wins by
     * construction: it reads the tail above that boundary, which is a strict subset.
     * Here that is the nine commits above 310s plus the two out-of-order rows.
     * <p>
     * The old ring lost that boundary to its retention budget, which is what made the
     * unbounded rebuild the only disposition left for these shapes. It is not the
     * bounded repair the dependency contract buys - the resume still reads to the end
     * of the base table, so it is O(distance from the change to the head) rather than
     * O(view age) - but it is what the timeline's permanent retention leaves as the
     * residual.
     */
    private static void assertResumeBoundsAnUnlocalizableRepair(ReplayCost cost) {
        final long tailRows = 2L * (LOCALIZATION_HISTORY_COMMITS - LOCALIZATION_O3_SECOND / 10) + 2;
        Assert.assertEquals("no bound is derived, so the resume is the cheaper disposition",
                tailRows, cost.resumedRows);
        Assert.assertEquals("and it reads exactly the tail it re-emits", tailRows, cost.scannedRows);
        Assert.assertEquals("the boundary rebuild must not run at all", 0, cost.emittedRows);
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
     * Drives a long in-order history, then commits one out-of-order row deep enough in
     * it that the localized rebuild prices under a resume from the boundary just below
     * it - the resume would replay every row above that boundary to the end of the base
     * table. Returns what that rebuild cost: base rows read and output rows re-emitted. Both counters are asserted zero before the
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

                cost.resumedRows = lv.getO3ResumeReplayRows();
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
        private long resumedRows;
        private long scannedRows;
    }
}
