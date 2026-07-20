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
