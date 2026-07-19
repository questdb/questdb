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

import io.questdb.cairo.CompositeDimensionTransform;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 5b of composite partitioning (the feature's PERFORMANCE payoff): a {@code WHERE} predicate on
 * the partitioning DIMENSION itself (e.g. {@code WHERE exch = 'BTC'}) must resolve to CELL PRUNING --
 * skipping every partition slot whose registered cell does not match -- instead of scanning every cell
 * and relying on the row-level filter alone. Correctness (== the plain twin) is the oracle that gates
 * the perf claim: the row filter alone was already correct (Tasks 6a/6b/6c), so a WRONG prune (dropping
 * a matching row, or including a wrong cell) would be a pure REGRESSION, never an acceptable trade for
 * speed.
 * <p>
 * <b>Un-gating:</b> before this task, {@code WHERE exch = 'BTC'} on a composite table's INDEXED
 * dimension column unconditionally threw Task 6b's loud "does not yet support an indexed WHERE
 * predicate" gate (see {@code CompositeReadEndToEndTest#testIndexedDimensionWherePrunesAndMatchesPlainTwin}
 * and {@code CompositeEndToEndTest#testMutationsMatchPlainEquivalent}, both updated alongside this
 * task -- they previously asserted the throw for exactly this shape). After 5b, that exact query
 * instead prunes to the matching cell(s) and succeeds, matching the plain twin --
 * {@link #testEqualityOnIndexedDimensionNoLongerThrowsAndMatchesPlainTwin()} proves it directly.
 * <p>
 * <b>Scope narrowing found while grounding Task 5b (not in the original design):</b> pruning was, at 5b,
 * only considered safe to apply when the resolved allowed-cellKey set had size &lt;= 1. Beyond a single
 * equality (or an IN-list that happens to resolve to one real cell), the UNCHANGED row-cursor factory
 * family the code still builds after pruning ({@code FilterOnValuesRecordCursorFactory} and siblings) was
 * never audited for composite cross-cell ORDER -- confirmed by reading {@code
 * FilterOnValuesRecordCursorFactory#getScanDirection()}, which claims {@code SCAN_DIRECTION_FORWARD} from
 * the requested scan order alone, with zero cell-interleaving awareness, exactly the defect Task 6a fixed
 * for the general scan via a SEPARATE class ({@code CompositePageFrameRecordCursorFactory}) this family
 * does not use. {@code SqlCodeGenerator}'s single-column {@code ORDER BY <ts>} sort-skip trusts that flag,
 * so pruning a genuine multi-cell IN-list through this path could silently misorder rows.
 * <p>
 * <b>Task #26 lifts the &lt;= 1 cap for IDENTITY dimensions:</b> rather than auditing the FilterOnValues
 * family, a successfully-pruned multi-cell IDENTITY set now BYPASSES it entirely and routes through Task
 * 6a's own cross-cell-merge factory ({@code CompositePageFrameRecordCursorFactory}) instead -- the same
 * genuinely-ordered path the general (non-indexed) composite scan already uses. An IDENTITY dimension's
 * value&harr;cellKey map is bijective (a cell can only ever hold rows for the ONE raw value that produced
 * its ordinal), so this is ROW-EXACT with no residual filter needed. {@link
 * #testMultiCellIdentityInListNoLongerDeclinedPrunesAndMatchesPlainTwin()} proves the exact query this
 * class used to lock as declined now prunes and matches the plain twin instead; {@link
 * #testMultiCellIdentityInListPrunesInterleavedRowsInGlobalTsOrderAsc()} and its DESC/3-cell/ts-range/
 * residual-filter siblings are the strong order-correctness oracle -- their dataset is deliberately
 * TIME-INTERLEAVED across cells (unlike this class's original BTC-mornings/ETH-afternoons dataset, where
 * naive per-cell concatenation happens to already agree with ts order), so a regression back to
 * concatenation instead of a genuine merge would visibly misorder rows. HASH and TRUNCATE dimensions are
 * NOT bijective (distinct raw values can collide into one bucket/prefix), so they are explicitly OUT of
 * this task's scope and remain declined, unchanged -- {@link
 * #testHashAndTruncateDimensionMultiCellInListStillLoudGated()} is the regression lock for that boundary.
 * <p>
 * Dataset ({@link #createAndPopulateTwins()}): composite {@code c} ({@code partition by day, exch},
 * {@code exch} SYMBOL INDEX) and plain twin {@code p} ({@code partition by day}), 3 days
 * (2020-01-01..03), 2 cells/day (BTC mornings at 00:00/06:00, ETH afternoons at 12:00/18:00 --
 * deliberately ordered so BTC is always first-seen, hence cellKey 0, and ETH cellKey 1 -- {@code _cell}
 * registry size 2), 4 rows/day, 12 rows total. One bulk insert per table (one WAL commit).
 */
public class CompositeCellPruningTest extends AbstractCairoTest {

    /**
     * The un-gating confirmation: pre-5b this exact query (predicate on the INDEXED dimension column
     * itself) threw Task 6b's loud gate unconditionally; it must now succeed and return exactly the
     * plain twin's rows.
     */
    @Test
    public void testEqualityOnIndexedDimensionNoLongerThrowsAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, px from c where exch = 'BTC' order by ts");
            assertSqlCursors(
                    "select count() from p where exch = 'BTC'",
                    "select count() from c where exch = 'BTC'");
        });
    }

    /**
     * Equality prunes to exactly ONE of the registry's 2 cells (BTC=cellKey0, ETH=cellKey1) -- observable
     * directly in EXPLAIN as {@code cellsPruned: 1} -- and the resulting rows match the plain twin
     * exactly, including with an explicit {@code ORDER BY ts} (this table's own natural cell/day
     * iteration order already happens to agree, since only ONE cell per day is ever visited once pruned,
     * but the explicit ORDER BY here is what the brief asks to combine and confirm).
     */
    @Test
    public void testEqualityPrunesToSingleCellWithObservablePlanAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select ts, exch, px from c where exch = 'BTC' order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("cellsPruned: 1")
                    .returns("""
                            ts\texch\tpx
                            2020-01-01T00:00:00.000000Z\tBTC\t1.0
                            2020-01-01T06:00:00.000000Z\tBTC\t1.1
                            2020-01-02T00:00:00.000000Z\tBTC\t2.0
                            2020-01-02T06:00:00.000000Z\tBTC\t2.1
                            2020-01-03T00:00:00.000000Z\tBTC\t3.0
                            2020-01-03T06:00:00.000000Z\tBTC\t3.1
                            """);
            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, px from c where exch = 'BTC' order by ts");
        });
    }

    /**
     * A never-registered value prunes to ZERO cells ({@code cellsPruned: 0}) and correctly returns ZERO
     * rows -- {@code VALUE_NOT_FOUND} contributes no ordinal, so the allowed-cellKey set is genuinely
     * empty rather than falling back to "no pruning" (which would scan everything and still correctly
     * return 0 rows via the row filter, but would not exercise the empty-set path this asserts).
     */
    @Test
    public void testNeverMatchingValuePrunesToZeroCellsAndZeroRows() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select count() from p where exch = 'NONE'",
                    "select count() from c where exch = 'NONE'");
            assertQuery("select count() from c where exch = 'NONE'")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 0")
                    .returns("count\n0\n");
        });
    }

    /**
     * An IN-list still prunes correctly as long as it resolves to AT MOST one real cell: a single-element
     * list behaves exactly like equality, and a two-element list where one value is never registered
     * (contributes no ordinal) also collapses to one real cell. Both are the size-&lt;=-1 case this
     * task's pruning covers (see the class javadoc's "scope narrowing" note) -- contrast with {@link
     * #testMultiCellInListIsSafelyDeclinedNotPrunedWrong()}, where BOTH values are real and distinct.
     */
    @Test
    public void testInListResolvingToAtMostOneCellPrunesAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from p where exch in ('BTC') order by ts",
                    "select ts, exch, px from c where exch in ('BTC') order by ts");
            assertQuery("select count() from c where exch in ('BTC')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 1")
                    .returns("count\n6\n");

            assertSqlCursors(
                    "select ts, exch, px from p where exch in ('BTC','NONE') order by ts",
                    "select ts, exch, px from c where exch in ('BTC','NONE') order by ts");
            assertQuery("select count() from c where exch in ('BTC','NONE')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 1")
                    .returns("count\n6\n");
        });
    }

    /**
     * Equality combined with a ts range, in BOTH scan directions -- exercises the INTERVAL cursor's own
     * pruning (not the full-scan cursor {@link #testEqualityPrunesToSingleCellWithObservablePlanAndMatchesPlainTwin()}
     * exercises), composing with the existing ts culling rather than replacing it.
     */
    @Test
    public void testEqualityCombinedWithTsRangeMatchesPlainTwinBothOrders() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            final String predicate = " where exch = 'BTC' and ts >= '2020-01-02' and ts <= '2020-01-03T23:59:59.999999Z'";
            assertSqlCursors(
                    "select ts, exch, px from p" + predicate + " order by ts",
                    "select ts, exch, px from c" + predicate + " order by ts");
            assertSqlCursors(
                    "select ts, exch, px from p" + predicate + " order by ts desc",
                    "select ts, exch, px from c" + predicate + " order by ts desc");
            assertSqlCursors(
                    "select count() from p" + predicate,
                    "select count() from c" + predicate);

            assertQuery("select ts, exch, px from c" + predicate + " order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval", "cellsPruned: 1")
                    .returns("""
                            ts\texch\tpx
                            2020-01-02T00:00:00.000000Z\tBTC\t2.0
                            2020-01-02T06:00:00.000000Z\tBTC\t2.1
                            2020-01-03T00:00:00.000000Z\tBTC\t3.0
                            2020-01-03T06:00:00.000000Z\tBTC\t3.1
                            """);
        });
    }

    /**
     * Task #26: the exact query this class used to lock as declined (see class javadoc) -- a predicate
     * matching BOTH registered dimension values (BTC=cellKey0 AND ETH=cellKey1, a genuine 2-cell match) --
     * now PRUNES to both cells instead of hitting Task 6b's gate, and matches the plain twin exactly,
     * including with an explicit {@code ORDER BY ts}. Uses this class's original BTC-mornings/
     * ETH-afternoons dataset for direct continuity with the pre-#26 test this replaces; the STRONG
     * order-correctness proof (misorder would be visible) is {@link
     * #testMultiCellIdentityInListPrunesInterleavedRowsInGlobalTsOrderAsc()} below, since -- as that
     * test's own doc explains -- this dataset's per-cell concatenation happens to already agree with ts
     * order, so it alone would not catch a regression to concatenation-instead-of-merge.
     */
    @Test
    public void testMultiCellIdentityInListNoLongerDeclinedPrunesAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from p where exch in ('BTC','ETH') order by ts",
                    "select ts, exch, px from c where exch in ('BTC','ETH') order by ts");
            assertSqlCursors(
                    "select ts, exch, px from p where exch in ('BTC','ETH') order by ts desc",
                    "select ts, exch, px from c where exch in ('BTC','ETH') order by ts desc");

            // count() takes a SEPARATE, order-indifferent frame-counting fast path (never the record-level
            // "Composite cross-cell merge scan" factory -- see the row-returning ASC/DESC siblings below
            // for that), but cellsPruned is emitted by the underlying dfcFactory either way.
            assertQuery("select count() from c where exch in ('BTC','ETH')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 2")
                    .returns("count\n12\n");
        });
    }

    /**
     * THE PRIMARY order-correctness oracle for Task #26's multi-cell IDENTITY prune: {@code exch}'s two
     * registered values are TIME-INTERLEAVED within every day (see {@link
     * #createAndPopulateInterleavedTwins()} -- BTC/ETH/SOL cycle every 10 minutes), so a regression to
     * naive per-cell concatenation (all of cellKey0's rows, THEN all of cellKey1's) would emit rows
     * visibly out of {@code ts} order (e.g. 00:30 BTC before 00:10 ETH) -- unlike {@link
     * #testMultiCellIdentityInListNoLongerDeclinedPrunesAndMatchesPlainTwin()}'s dataset, where
     * concatenation happens to already agree with global order. Matching the plain twin exactly here
     * (an order-sensitive cursor-by-cursor comparison) is only possible through a genuine cross-cell
     * merge. The concrete {@code .returns(...)} pins the exact expected order for a human reader; {@code
     * EXPLAIN} confirms the scan is routed through Task 6a's {@code CompositePageFrameRecordCursorFactory}
     * ("Composite cross-cell merge scan") with {@code cellsPruned: 2} (BTC + ETH; SOL correctly excluded).
     */
    @Test
    public void testMultiCellIdentityInListPrunesInterleavedRowsInGlobalTsOrderAsc() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from pi where exch in ('BTC','ETH') order by ts",
                    "select ts, exch, px from ci where exch in ('BTC','ETH') order by ts");

            assertQuery("select ts, exch, px from ci where exch in ('BTC','ETH') order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Composite cross-cell merge scan", "cellsPruned: 2")
                    .returns("""
                            ts\texch\tpx
                            2020-01-01T00:00:00.000000Z\tBTC\t1.0
                            2020-01-01T00:10:00.000000Z\tETH\t1.1
                            2020-01-01T00:30:00.000000Z\tBTC\t1.3
                            2020-01-01T00:40:00.000000Z\tETH\t1.4
                            2020-01-02T00:00:00.000000Z\tBTC\t2.0
                            2020-01-02T00:10:00.000000Z\tETH\t2.1
                            2020-01-02T00:30:00.000000Z\tBTC\t2.3
                            2020-01-02T00:40:00.000000Z\tETH\t2.4
                            2020-01-03T00:00:00.000000Z\tBTC\t3.0
                            2020-01-03T00:10:00.000000Z\tETH\t3.1
                            2020-01-03T00:30:00.000000Z\tBTC\t3.3
                            2020-01-03T00:40:00.000000Z\tETH\t3.4
                            """);
        });
    }

    /**
     * DESC counterpart of {@link #testMultiCellIdentityInListPrunesInterleavedRowsInGlobalTsOrderAsc()} --
     * same interleaved dataset and prune, backward scan direction. A backward per-cell concatenation bug
     * would be just as visibly wrong (cellKey1 reverse-ts ++ cellKey0 reverse-ts) as the forward case.
     */
    @Test
    public void testMultiCellIdentityInListPrunesInterleavedRowsInGlobalTsOrderDesc() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from pi where exch in ('BTC','ETH') order by ts desc",
                    "select ts, exch, px from ci where exch in ('BTC','ETH') order by ts desc");
        });
    }

    /**
     * A 3-value IN-list spanning all 3 registered cells (BTC/ETH/SOL) -- one step beyond the 2-cell case,
     * proving the fix generalizes past exactly 2. {@code EXPLAIN} shows {@code cellsPruned: 3}.
     */
    @Test
    public void testThreeValueInListPrunesToThreeCellsAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from pi where exch in ('BTC','ETH','SOL') order by ts",
                    "select ts, exch, px from ci where exch in ('BTC','ETH','SOL') order by ts");
            assertSqlCursors(
                    "select ts, exch, px from pi order by ts",
                    "select ts, exch, px from ci where exch in ('BTC','ETH','SOL') order by ts");

            assertQuery("select count() from ci where exch in ('BTC','ETH','SOL')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 3")
                    .returns("count\n18\n");
        });
    }

    /**
     * A value that was never interned ({@code 'NONE'}) mixed into an otherwise-genuine multi-cell
     * IN-list must contribute no ordinal -- only the 2 REAL cells (BTC, ETH) are pruned ({@code
     * cellsPruned: 2}, not 3), and the row set is identical to the 2-value case / the plain twin.
     */
    @Test
    public void testMultiCellInListWithNeverInternedValuePrunesOnlyRealCellsAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from pi where exch in ('BTC','ETH','NONE') order by ts",
                    "select ts, exch, px from ci where exch in ('BTC','ETH','NONE') order by ts");
            assertQuery("select count() from ci where exch in ('BTC','ETH','NONE')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 2")
                    .returns("count\n12\n");
        });
    }

    /**
     * Multi-cell IN combined with a ts range, in BOTH scan directions -- exercises the INTERVAL cursor's
     * own pruning (mirrors {@link #testEqualityCombinedWithTsRangeMatchesPlainTwinBothOrders()} for the
     * single-cell case), composing with the existing ts culling rather than replacing it. Range covers
     * days 2-3 only (8 of the dataset's 12 matching rows).
     */
    @Test
    public void testMultiCellIdentityInListCombinedWithTsRangeMatchesPlainTwinBothOrders() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            final String predicate = " where exch in ('BTC','ETH') and ts >= '2020-01-02' and ts <= '2020-01-03T23:59:59.999999Z'";
            assertSqlCursors(
                    "select ts, exch, px from pi" + predicate + " order by ts",
                    "select ts, exch, px from ci" + predicate + " order by ts");
            assertSqlCursors(
                    "select ts, exch, px from pi" + predicate + " order by ts desc",
                    "select ts, exch, px from ci" + predicate + " order by ts desc");

            assertQuery("select count() from ci" + predicate)
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("Interval", "cellsPruned: 2")
                    .returns("count\n8\n");
        });
    }

    /**
     * The residual-filter variant: {@code exch IN (...)} (the pruned, dropped key predicate) combined
     * with an extra {@code AND px > ...} (an ordinary, non-key residual). Task #26's design explicitly
     * leaves {@code intrinsicModel.filter} untouched when skipping the row-cursor family (see
     * {@code SqlCodeGenerator}'s {@code dimensionPrunedMultiCell} doc) -- this proves that residual still
     * applies correctly over the merged multi-cell scan, matching the plain twin.
     */
    @Test
    public void testMultiCellIdentityInListWithResidualFilterMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateInterleavedTwins();
            engine.releaseInactive();

            final String predicate = " where exch in ('BTC','ETH') and px > 1.35 order by ts";
            assertSqlCursors(
                    "select ts, exch, px from pi" + predicate,
                    "select ts, exch, px from ci" + predicate);
            assertQuery("select count() from ci where exch in ('BTC','ETH') and px > 1.35")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n9\n");
        });
    }

    /**
     * REGRESSION (Task #26 boundary): HASH and TRUNCATE dimensions are explicitly OUT of this task's
     * scope (see {@code resolveDimensionCellPruneSet}'s "Task #26 update" doc) -- their raw-value-to-
     * cellKey map is NOT bijective (distinct raw values can collide into the same bucket/prefix), so a
     * residual-free multi-cell prune would silently include rows for OTHER, non-matching raw values. An
     * IN-list spanning 2 genuinely different buckets (HASH) / prefixes (TRUNCATE) must still hit Task 6b's
     * gate, byte-for-byte unchanged. The two {@code exch} values are found by an explicit runtime search
     * for two DIFFERENT {@code hash(exch,4)} buckets (not assumed/hardcoded), so this test cannot pass by
     * accidentally taking the harmless same-bucket path instead of the genuine 2-cell one.
     */
    @Test
    public void testHashAndTruncateDimensionMultiCellInListStillLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            final int buckets = 4;
            String exchA = null;
            String exchB = null;
            for (int i = 0; i < buckets * 8 && exchB == null; i++) {
                String candidate = "HX" + i;
                if (exchA == null) {
                    exchA = candidate;
                } else if (CompositeDimensionTransform.hashBucket(candidate, buckets)
                        != CompositeDimensionTransform.hashBucket(exchA, buckets)) {
                    exchB = candidate;
                }
            }
            Assert.assertNotNull("expected to find two exch values hashing to different buckets", exchB);

            execute("create table c (ts timestamp, exch symbol index, sku symbol index, px double) timestamp(ts) " +
                    "partition by day, hash(exch, " + buckets + "), truncate(sku, 3) wal");
            execute("create table p (ts timestamp, exch symbol index, sku symbol index, px double) timestamp(ts) partition by day wal");
            final String rows = " values " +
                    "('2020-01-01T00:00:00.000000Z','" + exchA + "','BTCUSDT',1.0), " +
                    "('2020-01-01T01:00:00.000000Z','" + exchB + "','ETHUSDT',1.1)";
            execute("insert into c" + rows);
            execute("insert into p" + rows);
            drainWalQueue();
            engine.releaseInactive();

            final String msg = "composite partitioning does not yet support an indexed WHERE predicate";
            // HASH dimension (exch): a genuine 2-bucket IN-list.
            assertQuery("select ts, exch, sku, px from c where exch in ('" + exchA + "','" + exchB + "') order by ts")
                    .noLeakCheck().failsWith(msg);
            // TRUNCATE dimension (sku): a genuine 2-prefix IN-list.
            assertQuery("select ts, exch, sku, px from c where sku in ('BTCUSDT','ETHUSDT') order by ts")
                    .noLeakCheck().failsWith(msg);

            // plain twin is of course unaffected
            assertQuery("select count() from p where exch in ('" + exchA + "','" + exchB + "')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * Negative control: an ordinary indexed symbol column that is NOT the partitioning dimension (here,
     * {@code sym} -- only {@code exch} is declared as the dimension, via {@code partition by day, exch})
     * must be completely unaffected by this task: Task 6b's gate still fires for it, exactly as before
     * (Scope decision 3) -- mirrors {@code CompositeReadShapesTest#testWhereIndexedSymInListCompositeIsLoudGated}'s
     * own established case, re-proven here as this class's self-contained boundary check.
     */
    @Test
    public void testNonDimensionIndexedColumnStillGatedUnaffectedBy5b() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select ts, exch, sym, px from c where sym = 'BTC' order by ts")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
        });
    }

    // ==========================================================================================
    // CRITICAL (whole-branch review): a composite LATEST ON whose PARTITION BY column is the indexed
    // PARTITIONING DIMENSION, combined with an equality/IN WHERE on that same dimension, must NOT try
    // to cell-prune. Task 5b sets dimensionPruned for exactly that equality/IN and thereby BYPASSES
    // Task 6b's loud gate; the keyColumn-block single-value / IN-list / covering scan family it then
    // falls into NEVER applies LATEST BY (that lives in generateLatestByTableQuery, which composite
    // deliberately declines). The un-applied LATEST BY then reaches the generic post-scan
    // generateLatestBy(), whose `assert nested != null` trips (AssertionError under -ea; a raw NPE at
    // nested.getOrderHash() with assertions disabled in production -- i.e. a hard failure / dropped
    // latest-by, never a correct answer). Fix: decline the dimension prune whenever a LATEST BY is
    // present (require latestByColumnCount == 0 at the 5b prune-decision site) so the prune never
    // bypasses the gate and the query stays LOUD. NOTE (empirically verified): NO_INDEX(exch) does NOT
    // escape this shape -- when the LATEST ON PARTITION BY column IS exch, exch is the latest-by key
    // regardless of the WHERE hint, so keyColumn stays set and the gate still fires. The correct route
    // that the gate does NOT block is LATEST ON over a NON-dimension column (see the twin test below).
    // The dimension prune WITHOUT a latest-on is unaffected (5b's win).
    // ==========================================================================================

    /**
     * (a) The core defect: LATEST ON over the indexed DIMENSION column, filtered by an equality on that
     * same dimension (which 5b would prune), must hit Task 6b's loud gate -- never the pre-fix hard
     * failure (AssertionError/-ea, NPE/prod) that dropped the latest-by. The IN-list-collapsing-to-one-
     * cell variant takes the same 5b prune path and must be gated identically. The plain twin (oracle)
     * answers correctly: the single latest 'BTC' row (3.1).
     */
    @Test
    public void testLatestOnDimensionColumnWithDimensionEqualityIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // oracle: the plain twin collapses the 6 'BTC' rows to the single latest row (3.1)
            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts desc limit 1",
                    "select ts, exch, px from p where exch = 'BTC' latest on ts partition by exch");

            // composite must stay LOUD, never the pre-fix hard failure / silently-dropped latest-by
            assertQuery("select ts, exch, px from c where exch = 'BTC' latest on ts partition by exch")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
            // IN-list collapsing to a single real cell takes the identical 5b prune path -> same gate
            assertQuery("select ts, exch, px from c where exch in ('BTC') latest on ts partition by exch")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
        });
    }

    /**
     * (a2) Task #26 REGRESSION (this is the shape the whole-branch review worried about most): a GENUINE
     * multi-cell IN-list (BTC + ETH, 2 real cells -- exactly the shape {@link
     * #testMultiCellIdentityInListNoLongerDeclinedPrunesAndMatchesPlainTwin()} now prunes when there is NO
     * LATEST ON) combined with LATEST ON that same dimension column must STILL hit Task 6b's loud gate,
     * unchanged. {@code resolveDimensionCellPruneSet} is only ever invoked under {@code latestByColumnCount
     * == 0} (see the guard in {@code SqlCodeGenerator}'s {@code keyColumn != null} block), so {@code
     * dimensionPrunedMultiCell} can never become true here regardless of how many cells the IN-list would
     * otherwise resolve to -- this test is the empirical proof, not just a reading of the guard. Test (a)
     * above only ever exercised the single-cell-collapsing IN-list against this guard; this is the first
     * genuine 2-cell case now that Task #26 makes multi-cell IDENTITY pruning possible at all.
     */
    @Test
    public void testLatestOnDimensionColumnWithGenuineMultiCellInListIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select ts, exch, px from c where exch in ('BTC','ETH') latest on ts partition by exch")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");

            // sanity: the plain twin is unaffected -- exactly one latest row per exch value
            assertQuery("select count() from p where exch in ('BTC','ETH') latest on ts partition by exch")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * (b) The correct route the loud gate does NOT block: LATEST ON over a NON-dimension column ({@code
     * sym}) with the very same dimension WHERE ({@code exch='BTC'}). Here exch stays a residual filter
     * over the 6a-merged scan and LATEST BY applies as latestBy(filter(scan)) -- the composite result
     * must equal the plain twin exactly (the single latest 'BTC' row). Proves the fix does not
     * over-gate a shape that is already correct, and is a regression guard for the residual path.
     */
    @Test
    public void testLatestOnNonDimensionKeyWithDimensionWhereMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' latest on ts partition by sym",
                    "select ts, exch, px from c where exch = 'BTC' latest on ts partition by sym");
        });
    }

    /**
     * (c) The residual-filter variant: an extra {@code AND px > 2.05} makes the carried
     * {@code compositeLatestByFilter} non-null on the gated path. The gate must still fire (loud), and
     * the throw path must FREE that carried filter -- the enclosing {@link #assertMemoryLeak} verifies
     * no native resource leaks on the gate-throw. Pre-fix, 5b's prune bypassed the gate into a
     * keyColumn-block return that both dropped LATEST BY AND leaked the carried filter.
     */
    @Test
    public void testLatestOnDimensionColumnWithResidualIsLoudGatedNoLeak() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select ts, exch, px from c where exch = 'BTC' and px > 2.05 latest on ts partition by exch")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
        });
    }

    /**
     * (d) Regression lock: the dimension prune WITHOUT a latest-on (5b's win) is unaffected by the new
     * {@code latestByColumnCount == 0} guard -- {@code WHERE exch='BTC'} still prunes to one cell and
     * equals the plain twin.
     */
    @Test
    public void testDimensionEqualityWithoutLatestOnStillPrunesUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, px from c where exch = 'BTC' order by ts");
            assertQuery("select count() from c where exch = 'BTC'")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("cellsPruned: 1")
                    .returns("count\n6\n");
        });
    }

    /**
     * Builds composite {@code c} ({@code partition by day, exch}, {@code exch} SYMBOL INDEX -- the
     * dimension source column itself indexed, so an un-pruned WHERE predicate on it would hit Task 6b's
     * gate) and plain twin {@code p} ({@code partition by day}), byte-for-byte identical rows: 3 days
     * (2020-01-01..03), 2 cells/day (BTC mornings at 00:00/06:00, ETH afternoons at 12:00/18:00 --
     * deliberately ordered so BTC is always first-seen, hence cellKey 0, and ETH cellKey 1 -- registry
     * size 2), 4 rows/day, 12 rows total. {@code sym} is an ORDINARY indexed symbol column (NOT a
     * partitioning dimension), used only by {@link #testNonDimensionIndexedColumnStillGatedUnaffectedBy5b()};
     * it is set to {@code exch}'s own value for simplicity, since only its "not a dimension" status
     * matters for that test. One bulk insert per table (one WAL commit).
     */
    private void createAndPopulateTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol index, sym symbol index, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol index, sym symbol index, px double) timestamp(ts) partition by day wal");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','BTC','BTC',1.0), ('2020-01-01T06:00:00.000000Z','BTC','BTC',1.1), " +
                "('2020-01-01T12:00:00.000000Z','ETH','ETH',1.2), ('2020-01-01T18:00:00.000000Z','ETH','ETH',1.3), " +
                "('2020-01-02T00:00:00.000000Z','BTC','BTC',2.0), ('2020-01-02T06:00:00.000000Z','BTC','BTC',2.1), " +
                "('2020-01-02T12:00:00.000000Z','ETH','ETH',2.2), ('2020-01-02T18:00:00.000000Z','ETH','ETH',2.3), " +
                "('2020-01-03T00:00:00.000000Z','BTC','BTC',3.0), ('2020-01-03T06:00:00.000000Z','BTC','BTC',3.1), " +
                "('2020-01-03T12:00:00.000000Z','ETH','ETH',3.2), ('2020-01-03T18:00:00.000000Z','ETH','ETH',3.3)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();
    }

    /**
     * Builds composite {@code ci} ({@code partition by day, exch}, {@code exch} SYMBOL INDEX) and plain
     * twin {@code pi} ({@code partition by day}) for Task #26's multi-cell IDENTITY order-correctness
     * tests. Unlike {@link #createAndPopulateTwins()}'s BTC-mornings/ETH-afternoons split (where per-cell
     * concatenation happens to already agree with ts order, since one cell's rows always entirely precede
     * the other's within a day), {@code exch} here cycles BTC/ETH/SOL every 10 minutes -- TIME-INTERLEAVED
     * within every day -- so a regression to naive per-cell concatenation (all of one cell's rows, THEN
     * the next's) would emit rows visibly out of {@code ts} order. 3 days (2020-01-01..03), 3 cells/day
     * (BTC first-seen =&gt; cellKey 0, ETH =&gt; cellKey 1, SOL =&gt; cellKey 2 -- registry size 3), 6
     * rows/day, 18 rows total. One bulk insert per table (one WAL commit).
     */
    private void createAndPopulateInterleavedTwins() throws SqlException {
        execute("create table ci (ts timestamp, exch symbol index, px double) timestamp(ts) partition by day, exch wal");
        execute("create table pi (ts timestamp, exch symbol index, px double) timestamp(ts) partition by day wal");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','BTC',1.0), ('2020-01-01T00:10:00.000000Z','ETH',1.1), ('2020-01-01T00:20:00.000000Z','SOL',1.2), " +
                "('2020-01-01T00:30:00.000000Z','BTC',1.3), ('2020-01-01T00:40:00.000000Z','ETH',1.4), ('2020-01-01T00:50:00.000000Z','SOL',1.5), " +
                "('2020-01-02T00:00:00.000000Z','BTC',2.0), ('2020-01-02T00:10:00.000000Z','ETH',2.1), ('2020-01-02T00:20:00.000000Z','SOL',2.2), " +
                "('2020-01-02T00:30:00.000000Z','BTC',2.3), ('2020-01-02T00:40:00.000000Z','ETH',2.4), ('2020-01-02T00:50:00.000000Z','SOL',2.5), " +
                "('2020-01-03T00:00:00.000000Z','BTC',3.0), ('2020-01-03T00:10:00.000000Z','ETH',3.1), ('2020-01-03T00:20:00.000000Z','SOL',3.2), " +
                "('2020-01-03T00:30:00.000000Z','BTC',3.3), ('2020-01-03T00:40:00.000000Z','ETH',3.4), ('2020-01-03T00:50:00.000000Z','SOL',3.5)";
        execute("insert into ci" + rows);
        execute("insert into pi" + rows);
        drainWalQueue();
    }
}
