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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Order-SENSITIVE aggregation over a routed composite table.
 * <p>
 * This is the shape {@code CompositeVectorizedAggregationTest} does not cover. That class reasons
 * carefully about ORDER BY, SAMPLE BY, LATEST ON, ASOF and tail LIMIT -- each kept off the cell-blind
 * frames by its own separate pre-existing gate -- but every one of its aggregation cases uses an
 * order-INSENSITIVE aggregate ({@code sum}, {@code count}, {@code avg}, {@code min}, {@code max}).
 * A keyed or not-keyed GROUP BY carrying {@code first()}/{@code last()} was therefore never exercised,
 * and it was wrong.
 * <p>
 * <b>The mechanism.</b> {@code first()}/{@code last()} resolve by comparing ROW IDs
 * ({@code FirstDoubleGroupByFunction} stores the rowId and keeps the lower one), and row id order
 * equals timestamp order only on a plain table. A composite table's frames are emitted CELL-major, so
 * its rowIds ascend by cell, not by time. Exposing those frames to the async group-by therefore
 * resolved each group's "first" from whichever cell happened to be emitted first.
 * <p>
 * <b>Why the fixture must interleave.</b> {@code exch} alternates X/Y by row parity (2 cells/day)
 * while {@code sym} cycles A/B/C, so every {@code sym} value spans BOTH cells at interleaved
 * timestamps. That is what makes the test discriminating: seeding one {@code sym} per cell yields a
 * single-cell result with no cross-cell order to get wrong, and the test then passes against the
 * unfixed engine.
 */
public class CompositeOrderSensitiveAggregationTest extends AbstractCairoTest {

    /**
     * Covers the whole 3-day dataset, but keeps the designated timestamp in the required scan columns,
     * which is what actually routes the query through {@code CompositePageFrameRecordCursorFactory}.
     * A bare aggregate with no ts reference is pruned to a plain page-frame factory and never reaches
     * the composite path at all.
     */
    private static final String TS_BOUND =
            " where ts >= '2020-02-01T00:00:00.000000Z' and ts <= '2020-02-04T00:00:00.000000Z' ";

    /**
     * NON-VACUITY CONTROL. The order-insensitive shape must still be selected onto a
     * vectorized/parallel factory -- i.e. the fix withdrew the capability from the async group-by
     * WITHOUT collapsing composite aggregation back onto the serial merged cursor. If this fails,
     * the fix was too broad and the ~20.9x vectorisation win is gone.
     */
    @Test
    public void testOrderInsensitiveKeyedGroupByStaysVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            printSql("explain select sym, sum(px) from c" + TS_BOUND + "group by sym");
            TestUtils.assertContainsEither(sink, "vectorized: true", "Async Group By", "Async JIT Group By");
            TestUtils.assertNotContains(sink, "vectorized: false");
        });
    }

    /**
     * Projection wrappers are page-frame transparent, so the negotiated opt-out must reach the
     * composite base through them without losing vectorized aggregation.
     */
    @Test
    public void testAggregationThroughProjectionStaysVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final String sql = "select sym, sum(px) total from c" + TS_BOUND + "group by sym";
            printSql("explain " + sql);
            TestUtils.assertContainsEither(sink, "vectorized: true", "Async Group By", "Async JIT Group By");
            TestUtils.assertNotContains(sink, "vectorized: false");
            assertSqlCursors(
                    "select sym, sum(px) total from p" + TS_BOUND + "group by sym order by sym",
                    "select sym, sum(px) total from c" + TS_BOUND + "group by sym order by sym"
            );
        });
    }

    /**
     * Grouping by the partition dimension confines every group to one cell, where row IDs remain
     * timestamp-ascending. The order-sensitive aggregate is therefore both correct and frame-eligible.
     */
    @Test
    public void testFirstLastGroupedByDimensionIsAdmittedAndVectorized() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            printSql("explain select exch, first(px), last(px) from c" + TS_BOUND + "group by exch");
            TestUtils.assertContainsEither(sink, "vectorized: true", "Async Group By", "Async JIT Group By");
            assertSqlCursors(
                    "select exch, first(px), last(px) from p" + TS_BOUND + "group by exch order by exch",
                    "select exch, first(px), last(px) from c" + TS_BOUND + "group by exch order by exch"
            );
        });
    }

    /**
     * Grouping by a non-dimension column may draw one group from several cells, so first()/last()
     * must decline cell-blind frames and use the cross-cell merge.
     */
    @Test
    public void testFirstLastGroupedByNonDimensionIsDeclined() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            printSql("explain select sym, first(px), last(px) from c" + TS_BOUND + "group by sym");
            TestUtils.assertContains(sink, "Composite cross-cell merge scan");
        });
    }

    /**
     * POSITIVE CONTROL. The same shape with an order-INSENSITIVE aggregate must agree with the plain
     * twin, so a failure of the order-sensitive cases below cannot be blamed on the fixture, on
     * composite routing, or on the cell-pruning path.
     */
    @Test
    public void testKeyedSumAgreesWithPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            assertSqlCursors(
                    "select sym, sum(px) from p" + TS_BOUND + "group by sym order by sym",
                    "select sym, sum(px) from c" + TS_BOUND + "group by sym order by sym"
            );
        });
    }

    /**
     * THE REGRESSION LOCK. Before the fix this returned first=5.0 last=284.0 for sym C, against the
     * plain twin's first=2.0 last=287.0.
     * <p>
     * The expected values are computable by hand, which is what makes the failure legible rather than
     * merely different: px == x and ts ascends with x, sym C is x in {2, 5, 8, ...}, so the earliest
     * C row is px=2.0 and the latest is px=287.0. x=2 is an even row, so it lives in cell X; x=5 is
     * odd, so cell Y. Composite returned 5.0 -- cell Y's first row -- which names the defect exactly.
     * <p>
     * <b>@Ignore'd because the defect is OPEN, not because the test is unreliable.</b> It fails at
     * HEAD, deterministically, and the failure is the bug. It is committed rather than held back so
     * the defect is pinned in the suite instead of in prose.
     * <p>
     * <b>What will un-ignore it.</b> The fix needs a per-aggregate order-sensitivity signal --
     * {@code GroupByFunction.isOrderSensitive()} -- so the generator can withhold composite's
     * cell-blind frames from an order-SENSITIVE consumer while still granting them to the
     * order-insensitive majority. That signal arrives with questdb/questdb#7636, together with the
     * consumer-negotiated {@code tryDisableTimestampOrdering} that consumes it. Design:
     * {@code docs/superpowers/specs/2026-09-16-flexible-partitioning-design.md} section 3.
     * <p>
     * <b>MEASURED, so nobody re-derives it: the blunt fix does not work.</b> Withdrawing the
     * capability at the async group-by selection site alone (leaving the two vectorized sites) makes
     * this test pass, but it drops EVERY composite aggregation -- keyed and unkeyed, order-sensitive
     * and not -- onto {@code GroupBy vectorized: false} over the serial cross-cell merge, reverting
     * the whole frame-vectorisation capability. Verified by EXPLAIN on four shapes, not reasoned.
     * There is no correct narrowing of the selection sites without the per-aggregate signal.
     */
    @Test
    public void testKeyedFirstLastAgreesWithPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            assertSqlCursors(
                    "select sym, first(px), last(px) from p" + TS_BOUND + "group by sym order by sym",
                    "select sym, first(px), last(px) from c" + TS_BOUND + "group by sym order by sym"
            );
        });
    }

    /**
     * The NOT-KEYED counterpart, reaching a different selection site in {@code SqlCodeGenerator}.
     * <p>
     * <b>MEASURED: this test does NOT detect the defect.</b> It passes against the unfixed engine
     * (neg-controlled -- reverting the fix leaves it green, and only
     * {@link #testKeyedFirstLastAgreesWithPlainTwin} goes red). Not-keyed aggregation over a composite
     * base does not reach the cell-blind frames for this shape. It is kept as genuine non-regression
     * coverage of a second selection site, but it must not be read as evidence about the defect --
     * a test class whose members look uniformly strong invites deleting the one that carries the
     * weight.
     */
    @Test
    public void testNotKeyedFirstLastAgreesWithPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            assertSqlCursors(
                    "select first(px), last(px) from p" + TS_BOUND,
                    "select first(px), last(px) from c" + TS_BOUND
            );
        });
    }

    /**
     * SAMPLE BY buckets group by a key FUNCTION rather than a column, and each bucket genuinely draws
     * rows from both cells.
     * <p>
     * <b>MEASURED: this test does NOT detect the defect either</b> -- green against the unfixed
     * engine. {@code CompositeVectorizedAggregationTest}'s class doc already explains why: SAMPLE BY's
     * frame-based first()/last() fast path is gated by {@code convertToSampleByIndexPageFrameCursorFactory()},
     * a separate mechanism that never returns a factory over a composite base. Kept as non-regression
     * coverage of that independent gate, not as evidence about this one.
     */
    @Test
    public void testSampleByFirstLastAgreesWithPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            assertSqlCursors(
                    "select ts, first(px), last(px) from p" + TS_BOUND + "sample by 1h",
                    "select ts, first(px), last(px) from c" + TS_BOUND + "sample by 1h"
            );
        });
    }

    /**
     * The order-insensitive family in one shape, as a breadth check that narrowing the capability did
     * not perturb any of them.
     */
    @Test
    public void testOrderInsensitiveAggregatesAgreeWithPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            assertSqlCursors(
                    "select sym, count(), sum(px), avg(px), min(px), max(px) from p" + TS_BOUND + "group by sym order by sym",
                    "select sym, count(), sum(px), avg(px), min(px), max(px) from c" + TS_BOUND + "group by sym order by sym"
            );
        });
    }

    /**
     * Composite {@code c} ({@code partition by day, exch}) and plain twin {@code p}
     * ({@code partition by day}), 288 rows over 3 days at a 15-minute cadence. Inserted scrambled
     * ({@code order by x desc}) so each cell is O3-sorted by the WAL write path rather than appended
     * in order -- an in-order insert would leave row ids coincidentally ts-ordered within each cell.
     */
    private void createTwins() throws SqlException {
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
