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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.CompositePageFrameRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Safety net for composite's cell-blind page frames. The factory still reports
 * {@link RecordCursorFactory#supportsPageFrameCursor()} {@code false}; aggregation can reach its real
 * physical frames only after negotiating an ordering opt-out. Any direct call before negotiation must
 * fail closed, while unrelated page-frame consumers must continue selecting cursor-based plans.
 * <p>
 * This suite pins BOTH halves of that invariant directly (as opposed to only its correctness
 * *consequences*, which {@code CompositeVectorizedAggregationTest} and the pre-existing
 * {@code CompositeReadShapesTest} / {@code CompositeWindowHorizonSlaveTest} /
 * {@code CompositeWindowHorizonEndToEndTest} suites already prove via differential row-equality, and
 * which this task's broad regression run re-verifies unaffected):
 * <ol>
 *     <li>the capability pair itself on the real composite base factory
 *     ({@link #testCompositeFactoryReportsInvertedCapabilityPair()});</li>
 *     <li>the tail {@code LIMIT -N} selection site -- gated on {@code supportsPageFrameCursor()}, NOT the
 *     aggregation-only capability -- never plans an async/page-frame consumer over the composite base
 *     ({@link #testTailLimitDoesNotPlanAsyncPageFrameConsumerOverCompositeBase()});</li>
 *     <li>{@code ParquetExportMode.determineExportMode} -- the exact decision function both the
 *     {@code /exp} HTTP endpoint ({@code ExportQueryProcessor}) and
 *     {@code COPY ... TO ... WITH FORMAT parquet} share -- never selects a page-frame-backed export mode
 *     for a composite base ({@link #testParquetExportModeStaysCursorBasedForCompositeBase()}). CSV export
 *     itself can never regress via this landmine at all: {@code ExportQueryProcessor}'s non-parquet
 *     branch unconditionally calls {@code getCursor()} regardless of factory type, so only the parquet
 *     branch's mode decision is at risk, which is what this test targets directly.</li>
 * </ol>
 * All three assertions were empirically confirmed to have real teeth (not vacuously true) by temporarily
 * flipping {@code CompositePageFrameRecordCursorFactory.supportsPageFrameCursor()} to {@code true} (an
 * in-place Edit + inverse revert -- never a {@code git checkout}/{@code stash} of this uncommitted
 * worktree) and re-running this class: under the mutation,
 * {@link #testTailLimitDoesNotPlanAsyncPageFrameConsumerOverCompositeBase()} went RED -- the plan showed
 * an {@code Async JIT Filter} node directly over the {@code Composite cross-cell merge scan} (the
 * landmine materializing: an order-sensitive consumer wrongly consuming cell-blind unordered frames).
 * Reverting the mutation restored GREEN with zero net production diff.
 */
public class CompositeFrameExposureSafetyTest extends AbstractCairoTest {

    /**
     * Covers the entire 2-day dataset built by {@link #createCompositeTable()} -- a ts-bounding WHERE is
     * what actually routes the query through {@link CompositePageFrameRecordCursorFactory} rather than
     * being 6a-pruned to the plain per-cell factory (see {@code CompositeVectorizedAggregationTest}'s
     * class doc for the full explanation of that pruning shape).
     */
    private static final String TS_BOUND =
            " where ts >= '2020-02-01T00:00:00.000000Z' and ts <= '2020-02-03T00:00:00.000000Z' ";

    /**
     * The caller-visible factory is a QueryProgress wrapper, so unwrap it to prove the real composite
     * base refuses cell-blind frames until a consumer has negotiated the ordering opt-out.
     */
    @Test
    public void testCompositeFactoryFailsClosedBeforeNegotiation() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeTable();
            try (RecordCursorFactory factory = select("select * from c" + TS_BOUND)) {
                RecordCursorFactory unwrapped = ParquetExportMode.unwrapFactory(factory);
                Assert.assertTrue(
                        "expected the composite cross-cell-merge base factory (unwrapped from " +
                                factory.getClass() + "), got " + unwrapped.getClass(),
                        unwrapped instanceof CompositePageFrameRecordCursorFactory
                );
                Assert.assertFalse(
                        "supportsPageFrameCursor() must stay false -- every order-sensitive consumer " +
                                "gates on this and must keep degrading to the merged getCursor()",
                        factory.supportsPageFrameCursor()
                );
                try {
                    unwrapped.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    Assert.fail("unnegotiated composite page-frame access must fail closed");
                } catch (CairoException e) {
                    Assert.assertTrue(
                            e.getFlyweightMessage().toString(),
                            e.getFlyweightMessage().toString().contains("require a negotiated ordering opt-out")
                    );
                }
            }
        });
    }

    /**
     * ParquetExportMode.determineExportMode gates strictly on {@code supportsPageFrameCursor()} (see its
     * source), never the aggregation-only capability, so it must resolve to {@code CURSOR_BASED} for a
     * composite base in both scan directions -- never {@code DIRECT_PAGE_FRAME} / {@code
     * PAGE_FRAME_BACKED}, which would ship raw, cell-local page addresses straight to the parquet/CSV
     * encoder in cell-blind (not globally ts-ordered) order.
     */
    @Test
    public void testParquetExportModeStaysCursorBasedForCompositeBase() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeTable();
            try (RecordCursorFactory factory = select("select * from c" + TS_BOUND)) {
                Assert.assertEquals(
                        ParquetExportMode.CURSOR_BASED,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );
                Assert.assertEquals(
                        ParquetExportMode.CURSOR_BASED,
                        ParquetExportMode.determineExportMode(factory, true, sqlExecutionContext)
                );
            }
        });
    }

    /**
     * The tail {@code LIMIT -N} async/negative-limit selection site ({@code
     * AsyncFilteredRecordCursorFactory}, "Async Filter" in EXPLAIN, built by {@code
     * SqlCodeGenerator}'s {@code generateFilter}) is gated on {@code supportsPageFrameCursor()}, NOT the
     * aggregation-only capability -- so it must still be completely unavailable for a composite base,
     * forcing the plan to keep the row-based (never-async) tail-limit path over the SAME composite
     * cross-cell merge scan every other order-sensitive shape uses.
     * <p>
     * A RESIDUAL (non-timestamp) filter is required to actually reach {@code generateFilter}'s async
     * selection site: a bare {@code limit -5} with only a ts-range WHERE is fully resolved by
     * interval/partition-frame pruning -- no leftover row-wise {@code Function} filter is ever built, so
     * {@code generateFilter} (and therefore the {@code AsyncFilteredRecordCursorFactory} candidacy this
     * test targets) is never even entered, which would make a bare-limit assertion here vacuously true.
     * (Confirmed empirically: the bare-limit shape stayed green even under the negative-control mutation
     * described in this class's doc, precisely because it never enters {@code generateFilter}'s
     * async-selection branch in the first place -- the mutation has nothing to expose there.) {@code
     * px > 0} is therefore the load-bearing part of this test, not decorative: it is what forces a
     * genuine {@code Function} filter to be built and the async page-frame gate to actually be
     * consulted, mirroring the same distinction {@code CompositeReadShapesTest#testTailLimitEqualsPlainTwin}
     * already draws ("combined with a residual filter, still async-order-sensitive").
     */
    @Test
    public void testTailLimitDoesNotPlanAsyncPageFrameConsumerOverCompositeBase() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeTable();
            final String sql = "select * from c" + TS_BOUND + "and px > 0 limit -5";
            // Confirms the query genuinely still reaches the composite merge scan (not vacuously true)...
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Composite cross-cell merge scan");
            // ...and that no async/page-frame consumer was planned over it.
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Async");
        });
    }

    /**
     * Composite {@code c} ({@code partition by day, exch}), 2 {@code exch} cells/day, 192 rows over 2
     * days at a 15-minute cadence, {@code exch} alternating X/Y by row parity (the same interleaved
     * multi-cell shape {@code CompositeVectorizedAggregationTest} uses) -- no plain twin needed here,
     * since this suite asserts capability flags and plan shape directly on the composite factory, not
     * row-for-row differential correctness (that proof lives in the sibling class).
     */
    private void createCompositeTable() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("insert into c " +
                "select ('2020-02-01T00:00:00.000000Z'::timestamp + (x - 1) * 900000000L)::timestamp ts, " +
                "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                "x::double px " +
                "from long_sequence(192) order by x desc");
        drainWalQueue();
    }
}
