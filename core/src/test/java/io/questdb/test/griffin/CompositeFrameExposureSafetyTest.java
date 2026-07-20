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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.CompositePageFrameRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 2 of the frame-vectorization plan: the INVERTED-INVARIANT safety net.
 * <p>
 * Task 1 narrowly widened {@link CompositePageFrameRecordCursorFactory#getPageFrameCursor} to return
 * REAL, cell-blind (unordered-across-cells) page frames -- previously it unconditionally returned null.
 * The factory still reports {@link RecordCursorFactory#supportsPageFrameCursor()} {@code == false}
 * (unchanged), which every OTHER page-frame consumer in the codebase gates on before ever calling
 * {@code getPageFrameCursor()}. That combination -- a reachable, non-null frame cursor behind a
 * capability flag that says "false" -- is an INVERTED INVARIANT relative to every other
 * {@link RecordCursorFactory} implementation: normally {@code supportsPageFrameCursor() == false} is a
 * reliable promise that {@code getPageFrameCursor()} is never called at all. The one narrow, deliberate
 * escape hatch is {@link RecordCursorFactory#supportsPageFrameCursorForUnorderedAggregation()}, consulted
 * ONLY by the four order-indifferent vectorized/parallel group-by selection sites in
 * {@code SqlCodeGenerator#generateSelectGroupBy} (see {@link CompositePageFrameRecordCursorFactory}'s own
 * class doc for the full list). Every OTHER page-frame consumer -- the async filter / tail negative-limit
 * cursor, the fast ASOF/LT/window/horizon joins, CSV/parquet export -- MUST keep gating on the unchanged
 * {@code supportsPageFrameCursor()} and therefore must NEVER observe these frames: a page frame is
 * cell-local, and the cross-cell merge is genuinely required for anything order-sensitive, so leaking
 * these frames to an order-sensitive consumer would silently misorder (or misexport) rows -- never throw,
 * never look obviously wrong.
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
 * worktree) and re-running this class: see task-2-report.md for the exact RED failure text captured
 * before the revert, and the GREEN re-run captured after.
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
     * Pins the capability pair on the factory a real query actually gets back from {@code select()} --
     * which is {@link io.questdb.griffin.engine.QueryProgress}, a telemetry wrapper around the composite
     * base, NOT the composite factory directly (confirmed empirically: an earlier version of this test
     * asserted a direct {@code instanceof} on the un-unwrapped factory and failed with "got class
     * io.questdb.griffin.engine.QueryProgress" -- see task-2-report.md). So this asserts BOTH flags on
     * the OUTER (caller-visible) factory -- proving {@code QueryProgress}'s own delegation (Task 1 also
     * modified {@code QueryProgress#supportsPageFrameCursorForUnorderedAggregation()} to delegate to its
     * base) -- AND, via {@link ParquetExportMode#unwrapFactory}, that the UNWRAPPED factory really is the
     * composite cross-cell-merge factory under test, not some other incidental wrapper.
     */
    @Test
    public void testCompositeFactoryReportsInvertedCapabilityPair() throws Exception {
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
                Assert.assertTrue(
                        "supportsPageFrameCursorForUnorderedAggregation() must be true -- this is the " +
                                "narrow escape hatch only the four aggregation sites consult",
                        factory.supportsPageFrameCursorForUnorderedAggregation()
                );
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
     * described in this class's doc -- see task-2-report.md.) {@code px > 0} is the residual filter that
     * forces a genuine {@code Function} filter to be built, mirroring the same distinction {@code
     * CompositeReadShapesTest#testTailLimitEqualsPlainTwin} already draws ("combined with a residual
     * filter, still async-order-sensitive").
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
