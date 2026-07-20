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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Task 1 of the frame-vectorization plan (fail-safe opt-in capability) capstone: proves that
 * {@code CompositePageFrameRecordCursorFactory#supportsPageFrameCursorForUnorderedAggregation()}
 * lets the four order-indifferent group-by selection sites in
 * {@code SqlCodeGenerator#generateSelectGroupBy} pick a vectorized (Rosti) or parallel/async
 * factory over a composite table's real, cell-blind page frames, while
 * {@code supportsPageFrameCursor()} stays {@code false} and every order-sensitive shape (ORDER BY)
 * keeps using the merged, genuinely-ordered {@code getCursor()}.
 * <p>
 * IMPORTANT SHAPE NOTE: a bare {@code select sum(px) from c} (no WHERE, no ts reference) does NOT
 * exercise this feature at all -- {@code SqlCodeGenerator#generateTableQuery0}'s pre-existing 6a
 * timestamp-pruning ({@code queryMeta.getTimestampIndex() != -1}) already routes a scan that needs
 * no ts column straight to the plain, order-indifferent {@code PageFrameRecordCursorFactory},
 * bypassing {@code CompositePageFrameRecordCursorFactory} (and therefore Task 1) entirely -- verified
 * empirically while writing this test (EXPLAIN showed a plain {@code PageFrame} child, not
 * {@code Composite cross-cell merge scan}, even with the fix reverted). Every aggregation query
 * below therefore adds a ts-bounding WHERE clause (a realistic "aggregate over a time window" shape,
 * and the shape the originating benchmark reflects) so the designated timestamp stays part of the
 * required scan columns and the query genuinely reaches the composite merge factory. The bound
 * {@code [2020-02-01, 2020-02-04)} covers the entire 3-day dataset, so results are identical to an
 * unbounded aggregate -- only the EXPLAIN shape (interval scan vs. full scan) differs.
 * <p>
 * Composite {@code c} (2 {@code exch} cells/day) vs. plain twin {@code p}, identical data: an
 * unkeyed {@code sum}, the 5-aggregate unkeyed shape, and a keyed {@code group by sym} must all (a)
 * equal the plain twin (correctness, via {@code assertSqlCursors}) and (b) EXPLAIN to a
 * vectorized/parallel factory -- NOT the serial {@code GroupBy} that runs over the merged row-wise
 * cursor. Before Task 1, (a) already holds (the serial path is correct, just slow) but (b) is RED:
 * EXPLAIN shows {@code GroupBy vectorized: false} over a {@code Composite cross-cell merge scan}
 * child (confirmed empirically by temporarily reverting the composite factory's capability
 * exposure). A trailing ORDER BY non-regression and a plain-table EXPLAIN check prove the
 * merged-cursor / plain-table paths are untouched.
 * <p>
 * Task 2 (the differential capstone) extends this class with the order-SENSITIVE counterpart shapes --
 * {@code ORDER BY} (asc/desc), {@code SAMPLE BY}, {@code LATEST ON}, an ASOF join (composite
 * master/slave/both), and a tail {@code LIMIT -N} -- proving each still equals the plain twin, i.e. that
 * Task 1's opt-in stayed scoped to exactly the four aggregation sites and nothing order-sensitive
 * silently regressed onto the newly-real cell-blind frames. The companion inverted-invariant proof (the
 * capability pair itself, plus the tail-limit / parquet-export PLAN-shape checks) lives in the sibling
 * {@code CompositeFrameExposureSafetyTest}.
 */
public class CompositeVectorizedAggregationTest extends AbstractCairoTest {

    /**
     * Covers the entire 3-day dataset built by {@link #createSingleTableTwins()} -- results are
     * identical to an unbounded aggregate -- but keeps the designated timestamp in the required scan
     * columns, which is what actually routes the query through
     * {@code CompositePageFrameRecordCursorFactory} (see the class doc's shape note).
     */
    private static final String TS_BOUND =
            " where ts >= '2020-02-01T00:00:00.000000Z' and ts <= '2020-02-04T00:00:00.000000Z' ";

    /**
     * ASOF JOIN non-regression, composite on the MASTER, the SLAVE, and BOTH sides: the exhaustive matrix
     * of join kinds/positions over composite (ASOF/LT/SPLICE/WINDOW/HORIZON) already lives in {@code
     * CompositeReadShapesTest} and {@code CompositeWindowHorizonSlaveTest} / {@code
     * CompositeWindowHorizonEndToEndTest} (all re-verified unaffected by this task's broad regression
     * run -- see task-2-report.md) -- this method ties ONE representative join kind directly to THIS
     * class's own fixture/claim, as its capstone. A join factory never consults
     * {@code supportsPageFrameCursorForUnorderedAggregation()} (only the four group-by selection sites
     * do), so a composite slave must still fall back to the LIGHT join, never the fast
     * TimeFrameCursor-based factory ({@code supportsConcurrentTimeFrameCursor()} is false for composite).
     */
    @Test
    public void testAsofJoinNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            createJoinMasterTwins();
            final String q = "select m.ts, m.sym, m.qty, s.px from %s m asof join %s s on (m.sym = s.sym)";
            final String oracle = String.format(q, "jp", "p");
            assertSqlCursors(oracle, String.format(q, "jp", "c")); // composite slave
            assertSqlCursors(oracle, String.format(q, "jm", "p")); // composite master
            assertSqlCursors(oracle, String.format(q, "jm", "c")); // both composite

            // Composite slave must fall back to the LIGHT join, never the fast time-frame-cursor factory.
            assertQuery(String.format(q, "jp", "c")).noLeakCheck().assertsPlanNotContaining("Fast");
        });
    }

    @Test
    public void testFiveAggEqualsPlainTwinAndIsVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select sum(px), count(), avg(px), min(px), max(px) from p" + TS_BOUND,
                    "select sum(px), count(), avg(px), min(px), max(px) from c" + TS_BOUND
            );
            assertAggregationVectorizedOrParallel("select sum(px), count(), avg(px), min(px), max(px) from c" + TS_BOUND);
        });
    }

    @Test
    public void testKeyedGroupByEqualsPlainTwinAndIsVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select sym, sum(px) from p" + TS_BOUND + "group by sym order by sym",
                    "select sym, sum(px) from c" + TS_BOUND + "group by sym order by sym"
            );
            assertAggregationVectorizedOrParallel("select sym, sum(px) from c" + TS_BOUND + "group by sym");
        });
    }

    /**
     * LATEST ON non-regression: {@code LATEST ON ts PARTITION BY sym} (sym is an ORDINARY,
     * non-dimension column, cycling A/B/C) over the same interleaved fixture the aggregation tests above
     * use -- {@code LatestByRecordCursorFactory} walks {@code getCursor()} row-by-row and must still
     * resolve each key's true latest row via the genuinely-ordered merge, not a cell-blind concatenation
     * (which could resolve a key's "latest" from the wrong cell's tail row instead of the globally last
     * one).
     */
    @Test
    public void testLatestOnNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select * from p" + TS_BOUND + "latest on ts partition by sym order by sym",
                    "select * from c" + TS_BOUND + "latest on ts partition by sym order by sym"
            );
        });
    }

    /**
     * DESC counterpart of {@link #testOrderByNonRegressionStillEqualsPlainTwin()}: the merge cursor's
     * max-heap (backward) mode, not just its forward/min-heap mode, must also still equal the plain twin.
     */
    @Test
    public void testOrderByDescNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select * from p order by ts desc",
                    "select * from c order by ts desc"
            );
        });
    }

    /**
     * Non-regression companion to the aggregation tests above: the composite table's
     * ORDER-SENSITIVE shape (a full scan ordered by the designated timestamp) must still equal the
     * plain twin -- proof that {@code getScanDirection()} / {@code supportsPageFrameCursor()} are
     * unchanged and this shape stayed on the merged, genuinely-ordered {@code getCursor()} rather
     * than the newly-exposed cell-blind frames (which would silently misorder it).
     */
    @Test
    public void testOrderByNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select * from p order by ts",
                    "select * from c order by ts"
            );
        });
    }

    /**
     * A plain (non-composite) table's aggregation EXPLAIN must be byte-identical to before this
     * feature: {@code supportsPageFrameCursorForUnorderedAggregation()} defaults to
     * {@code supportsPageFrameCursor()}, so the OR-in at the four group-by selection sites in
     * {@code SqlCodeGenerator} is {@code X||X == X} for any factory that doesn't override the new
     * capability -- which is every plain-table factory. Both shapes must still show
     * vectorized/parallel execution, exactly as they did before Task 1 (a plain table never routes
     * through the composite merge factory in the first place, so this is a pure non-regression
     * check, not a differential one).
     */
    @Test
    public void testPlainTableAggregationExplainUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertAggregationVectorizedOrParallel("select sum(px), count(), avg(px), min(px), max(px) from p" + TS_BOUND);
            assertAggregationVectorizedOrParallel("select sym, sum(px) from p" + TS_BOUND + "group by sym");
        });
    }

    /**
     * SAMPLE BY non-regression (non-keyed, keyed, and the first()/last() shape) over the same interleaved
     * fixture the aggregation tests above use. {@code SAMPLE BY}'s OWN optimized page-frame-based
     * first()/last() fast path ({@code SampleByFirstLastRecordCursorFactory}) is gated by a SEPARATE,
     * independent capability -- {@code convertToSampleByIndexPageFrameCursorFactory()} -- that {@code
     * CompositePageFrameRecordCursorFactory} deliberately does NOT override (see its class doc: the
     * inherited default unconditionally returns null), so this proves that gate -- not just {@code
     * supportsPageFrameCursor()} -- also keeps composite off a frame-based SAMPLE BY path: EXPLAIN must
     * never show {@code SampleByFirstLast}.
     */
    @Test
    public void testSampleByNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select ts, sum(px), count() from p" + TS_BOUND + "sample by 1h",
                    "select ts, sum(px), count() from c" + TS_BOUND + "sample by 1h"
            );
            assertSqlCursors(
                    "select ts, sym, sum(px), count() from p" + TS_BOUND + "sample by 1h order by ts, sym",
                    "select ts, sym, sum(px), count() from c" + TS_BOUND + "sample by 1h order by ts, sym"
            );
            assertSqlCursors(
                    "select ts, first(px), last(px) from p" + TS_BOUND + "sample by 1h",
                    "select ts, first(px), last(px) from c" + TS_BOUND + "sample by 1h"
            );
            assertQuery("select ts, first(px), last(px) from c" + TS_BOUND + "sample by 1h")
                    .noLeakCheck()
                    .assertsPlanNotContaining("SampleByFirstLast");
        });
    }

    /**
     * Tail {@code LIMIT -N} non-regression (correctness only -- the plan-shape / inverted-invariant proof
     * that this exact shape never reaches the composite cell-blind frames lives in {@code
     * CompositeFrameExposureSafetyTest}, the more precise home for a plan/capability assertion): a
     * negative LIMIT trusts the base scan's advertised order to take the LAST N rows (see {@code
     * AsyncFilteredNegativeLimitRecordCursor}) -- must still equal the plain twin's tail. The residual
     * ({@code px > 0}) variant additionally mirrors {@code
     * CompositeReadShapesTest#testTailLimitEqualsPlainTwin}'s "combined with a residual filter, still
     * async-order-sensitive" case: a bare ts-bounded tail limit is fully resolved by interval pruning with
     * no leftover row-wise filter function, so it never even reaches {@code SqlCodeGenerator}'s {@code
     * generateFilter} async-selection site (confirmed empirically -- see task-2-report.md); only the
     * residual-filter shape genuinely exercises it.
     */
    @Test
    public void testTailLimitNonRegressionStillEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select * from p" + TS_BOUND + "limit -5",
                    "select * from c" + TS_BOUND + "limit -5"
            );
            assertSqlCursors(
                    "select * from p" + TS_BOUND + "and px > 0 limit -5",
                    "select * from c" + TS_BOUND + "and px > 0 limit -5"
            );
        });
    }

    @Test
    public void testUnkeyedSumEqualsPlainTwinAndIsVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins();
            assertSqlCursors(
                    "select sum(px) from p" + TS_BOUND,
                    "select sum(px) from c" + TS_BOUND
            );
            assertAggregationVectorizedOrParallel("select sum(px) from c" + TS_BOUND);
        });
    }

    /**
     * Runs {@code explain <sql>} and asserts the plan shows a vectorized (Rosti) or async/parallel
     * group-by factory -- {@code vectorized: true}, {@code Async Group By}, or {@code Async JIT
     * Group By} -- and NOT the serial {@code GroupBy vectorized: false} shape that runs over the
     * merged row-wise cursor.
     */
    private static void assertAggregationVectorizedOrParallel(String sql) throws SqlException {
        printSql("explain " + sql);
        TestUtils.assertContainsEither(sink, "vectorized: true", "Async Group By", "Async JIT Group By");
        TestUtils.assertNotContains(sink, "vectorized: false");
    }

    /**
     * Builds a small master table pair for {@link #testAsofJoinNonRegressionStillEqualsPlainTwin()}:
     * composite {@code jm} ({@code partition by day, exch}) and plain twin {@code jp}, 144 rows over 3
     * days at a 30-minute cadence offset +7 minutes from {@link #createSingleTableTwins()}'s 15-minute
     * slave grid (so no master row can ever collide on ts with a slave row -- the same offset technique
     * {@code CompositeReadShapesTest#createJoinTwins} uses). {@code exch} alternates by row parity (2
     * cells/day) and {@code sym} cycles A/B/C, matching the slave's own key domain so the join has real
     * matching work to do.
     */
    private void createJoinMasterTwins() throws SqlException {
        execute("create table jm (ts timestamp, exch symbol, sym symbol, qty double) timestamp(ts) partition by day, exch wal");
        execute("create table jp (ts timestamp, exch symbol, sym symbol, qty double) timestamp(ts) partition by day wal");

        final String select =
                "select ('2020-02-01T00:07:00.000000Z'::timestamp + (x - 1) * 1800000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "x::double qty " +
                        "from long_sequence(144) order by x desc";
        execute("insert into jm " + select);
        execute("insert into jp " + select);
        drainWalQueue();
    }

    /**
     * Builds composite table {@code c} ({@code partition by day, exch}) and its plain twin {@code p}
     * ({@code partition by day}), 288 rows over 3 days at a 15-minute cadence. {@code exch}
     * alternates X/Y by row parity (2 cells/day, satisfying the composite partitioning minimum), and
     * {@code sym} cycles A/B/C so keyed group-by has real multi-key work to do. Inserted scrambled
     * ({@code order by x desc}) so each cell is O3-sorted by the WAL write path, matching
     * {@code CompositeReadShapesTest#createSingleTableTwins}.
     */
    private void createSingleTableTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day wal");

        final String select =
                "select ('2020-02-01T00:00:00.000000Z'::timestamp + (x - 1) * 900000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "x::double px " +
                        "from long_sequence(288) order by x desc";
        execute("insert into c " + select);
        execute("insert into p " + select);
        drainWalQueue();
    }
}
