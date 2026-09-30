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

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Task 6a (the CRUX of composite-partitioning read side): a composite table stores rows in per-cell
 * partition subdirectories WITHIN each time partition, so a plain page-frame scan emits, per day,
 * {@code cell0 ++ cell1 ++ ...} -- globally MISORDERED whenever cells interleave in time. This suite is
 * the differential proof that a composite table's {@code getCursor()} now yields a genuinely
 * global-designated-timestamp-ordered stream (forward AND backward), byte-for-byte identical to a plain
 * twin ({@code partition by day}, {@code exch} an ordinary column) holding the same rows.
 * <p>
 * The comparison uses {@link io.questdb.test.tools.TestUtils#assertSqlCursors} which walks both cursors in
 * lock-step (order-SENSITIVE, no re-sort), so a mis-ordered composite scan fails row-for-row against the
 * plain twin. All timestamps are globally unique, so the ts-ordered stream is a total order and the twin
 * comparison is unambiguous.
 * <p>
 * Page frames are forced tiny ({@code max=2}) so every multi-row cell spans several contiguous page frames
 * -- exercising the merge's cross-FRAME advance within one cell as well as its cross-CELL interleave.
 */
public class CompositeOrderedScanTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        // Tiny page frames: every cell of 3+ rows splits into several contiguous frames, so the merge is
        // exercised across frame boundaries within a cell (min=1 keeps a trailing partial frame rather than
        // merging it back into the previous one).
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        super.setUp();
    }

    /**
     * Core differential: bare {@code select *} (natural order) and {@code order by ts} (single-column
     * sort-skip that trusts {@code getScanDirection()}) both ASC and DESC, over a composite table whose
     * cells genuinely interleave in time within each of 4 days (one deliberately single-cell), must equal
     * the plain twin row-for-row. RED before Task 6a (per-cell concatenation diverges from the twin at the
     * first cross-cell interleave); GREEN after.
     */
    @Test
    public void testCompositeScanEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            // bare natural-order scan (full scan, site :10781)
            assertSqlCursors("select * from p", "select * from c");
            // single-column ORDER BY ts -> sort-skip returns the base scan raw, trusting getScanDirection()
            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
            // descending -> backward frame scan + max-heap merge
            assertSqlCursors("select * from p order by ts desc", "select * from c order by ts desc");
        });
    }

    /**
     * Explicit-content proof (independent of the plain twin) that additionally exercises the merge cursor's
     * random-access seam: {@link io.questdb.test.QueryAssertion} re-reads every row via {@code recordAt(rowId)}
     * and compares it to the forward read, and asserts the determinate {@code size()} -- so a broken
     * random-access round-trip or size would fail here even though the forward differential passes.
     */
    @Test
    public void testCompositeScanExplicitContentAndRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            assertQuery("select * from c")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\texch\tpx\n" +
                            "2019-12-31T06:00:00.000000Z\tA\t10.0\n" +
                            "2019-12-31T18:00:00.000000Z\tA\t11.0\n" +
                            "2020-01-01T01:00:00.000000Z\tA\t1.0\n" +
                            "2020-01-01T02:00:00.000000Z\tB\t2.0\n" +
                            "2020-01-01T03:00:00.000000Z\tB\t3.0\n" +
                            "2020-01-01T04:00:00.000000Z\tA\t4.0\n" +
                            "2020-01-01T05:00:00.000000Z\tA\t5.0\n" +
                            "2020-01-01T06:00:00.000000Z\tB\t6.0\n" +
                            "2020-01-02T01:00:00.000000Z\tB\t20.0\n" +
                            "2020-01-02T02:00:00.000000Z\tA\t21.0\n" +
                            "2020-01-02T03:00:00.000000Z\tB\t22.0\n" +
                            "2020-01-02T04:00:00.000000Z\tB\t23.0\n" +
                            "2020-01-02T05:00:00.000000Z\tA\t24.0\n" +
                            "2020-01-02T06:00:00.000000Z\tB\t25.0\n" +
                            "2020-01-03T01:00:00.000000Z\tA\t30.0\n" +
                            "2020-01-03T02:00:00.000000Z\tB\t31.0\n" +
                            "2020-01-03T03:00:00.000000Z\tA\t32.0\n" +
                            "2020-01-03T04:00:00.000000Z\tB\t33.0\n" +
                            "2020-01-03T05:00:00.000000Z\tB\t34.0\n" +
                            "2020-01-03T06:00:00.000000Z\tA\t35.0\n");
        });
    }

    /**
     * The merged stream must also be correct once wrapped by order-INDIFFERENT / aggregating consumers that
     * degrade to the row-based {@code getCursor()} path (composite advertises
     * {@code supportsPageFrameCursor()=false}). This both proves graceful degradation and re-checks the
     * merge under a residual WHERE filter and a keyed GROUP BY.
     */
    @Test
    public void testCompositeAggregatesAndFilterEqualPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            // vectorized-shaped aggregates degrade to row-based getCursor(); must return correct, not throw
            assertSqlCursors("select sum(px), count() from p", "select sum(px), count() from c");
            // residual (non-pruning) filter wraps the merged getCursor()
            assertSqlCursors(
                    "select * from p where px > 3 order by ts",
                    "select * from c where px > 3 order by ts"
            );
            // keyed group-by over the row-based path
            assertSqlCursors(
                    "select exch, count(), sum(px) from p order by exch",
                    "select exch, count(), sum(px) from c order by exch"
            );
        });
    }

    /**
     * The interval-scan routing site (a {@code WHERE ts >= .. and ts <= ..} range compiles to an interval
     * partition-frame cursor, the OTHER framing site Task 6a routes) must also merge cross-cell. Relies on
     * merge order (single-column {@code order by ts} sort-skip and bare natural order), not an explicit
     * {@code order by ts, exch} re-sort, so a mis-ordered interval scan would diverge from the plain twin.
     */
    @Test
    public void testCompositeIntervalScanEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            final String predicate = " where ts >= '2020-01-01' and ts <= '2020-01-02T23:59:59.999999Z'";
            assertSqlCursors(
                    "select * from p" + predicate + " order by ts",
                    "select * from c" + predicate + " order by ts"
            );
            assertSqlCursors(
                    "select * from p" + predicate + " order by ts desc",
                    "select * from c" + predicate + " order by ts desc"
            );
        });
    }

    /**
     * SAMPLE BY is an order-REQUIRING consumer: it makes a single forward pass with a forward-only epoch, so
     * a mis-ordered base scan folds a cell's early rows into the wrong bucket. It consumes the base scan's
     * {@code getCursor()} and gates on {@code getScanDirection()==FORWARD}; both are now truthful for a
     * composite table, so a composite SAMPLE BY must equal the plain twin's -- proving the crux fixes SAMPLE
     * BY through the one merge seam, with no SAMPLE-BY-specific change.
     */
    @Test
    public void testCompositeSampleByEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            assertSqlCursors(
                    "select ts, sum(px), count() from p sample by 1h",
                    "select ts, sum(px), count() from c sample by 1h"
            );
            assertSqlCursors(
                    "select ts, exch, sum(px) from p sample by 6h",
                    "select ts, exch, sum(px) from c sample by 6h"
            );
        });
    }

    /**
     * Deeper multi-frame / deeper-heap coverage: three days, two cells each, ~30 rows per cell generated so
     * the row timestamps strictly alternate cells (A on even seconds, B on odd), forcing a long cross-cell
     * interleave across many small frames. Backward too.
     */
    @Test
    public void testCompositeManyRowsPerCellEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // 180 rows across 3 days (one row/minute), exch alternates every row -> A and B interleave in ts
            // within every day; inserted in DESCENDING ts so the write path must O3-sort each cell.
            final String select =
                    "select ('2020-02-01T00:00:00.000000Z'::timestamp + (x - 1) * 60000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'A' else 'B' end exch, " +
                            "x::double px " +
                            "from long_sequence(180) order by x desc";
            execute("insert into c " + select);
            execute("insert into p " + select);
            drainWalQueue();

            assertSqlCursors("select * from p", "select * from c");
            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
            assertSqlCursors("select * from p order by ts desc", "select * from c order by ts desc");
        });
    }

    /**
     * PLAIN regression (guards that the composite branch is genuinely separate and plain behaviour is
     * unchanged): a plain table's natural scan is already designated-timestamp ordered, so its bare
     * {@code select *} equals its {@code order by ts} -- this must hold identically to before Task 6a, and
     * proves a plain table never takes the composite merge path.
     */
    @Test
    public void testPlainTableScanUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("insert into p values " +
                    "('2020-01-01T05:00:00.000000Z','B',5), ('2020-01-01T01:00:00.000000Z','A',1), " +
                    "('2020-01-01T03:00:00.000000Z','A',3), ('2020-01-01T02:00:00.000000Z','B',2), " +
                    "('2020-01-01T04:00:00.000000Z','B',4), ('2020-01-02T02:00:00.000000Z','A',7), " +
                    "('2020-01-02T01:00:00.000000Z','B',6)");
            drainWalQueue();

            assertSqlCursors("select * from p order by ts", "select * from p");
            assertQuery("select * from p")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\texch\tpx\n" +
                            "2020-01-01T01:00:00.000000Z\tA\t1.0\n" +
                            "2020-01-01T02:00:00.000000Z\tB\t2.0\n" +
                            "2020-01-01T03:00:00.000000Z\tA\t3.0\n" +
                            "2020-01-01T04:00:00.000000Z\tB\t4.0\n" +
                            "2020-01-01T05:00:00.000000Z\tB\t5.0\n" +
                            "2020-01-02T01:00:00.000000Z\tB\t6.0\n" +
                            "2020-01-02T02:00:00.000000Z\tA\t7.0\n");
        });
    }

    /**
     * Builds composite table {@code c} ({@code partition by day, exch}) and its plain twin {@code p}
     * ({@code partition by day}, {@code exch} an ordinary column) and inserts byte-for-byte identical rows
     * into both, in a deliberately SCRAMBLED (non-ts-sorted) order so the write path O3-sorts each cell and
     * the cells of every day genuinely interleave in time on disk. Four days:
     * <ul>
     *   <li>d0 (2019-12-31): a single cell ('A' only) -- the heap-of-one pass-through sanity case;</li>
     *   <li>d1..d3 (2020-01-01..03): two interleaved cells ('A','B') with deliberately uneven per-cell
     *       row counts, so one cell exhausts before its sibling (the heap must shrink mid-day).</li>
     * </ul>
     * All 20 timestamps are globally unique.
     */
    private void createAndPopulateTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

        // Scrambled insertion order (NOT ts-sorted); each (day, exch) cell is O3-sorted on commit.
        final String rows = " values " +
                "('2020-01-02T04:00:00.000000Z','B',23), " +
                "('2019-12-31T18:00:00.000000Z','A',11), " +
                "('2020-01-01T02:00:00.000000Z','B',2), " +
                "('2020-01-03T06:00:00.000000Z','A',35), " +
                "('2020-01-01T05:00:00.000000Z','A',5), " +
                "('2020-01-02T01:00:00.000000Z','B',20), " +
                "('2020-01-01T01:00:00.000000Z','A',1), " +
                "('2020-01-03T02:00:00.000000Z','B',31), " +
                "('2020-01-02T06:00:00.000000Z','B',25), " +
                "('2020-01-01T06:00:00.000000Z','B',6), " +
                "('2019-12-31T06:00:00.000000Z','A',10), " +
                "('2020-01-03T04:00:00.000000Z','B',33), " +
                "('2020-01-02T02:00:00.000000Z','A',21), " +
                "('2020-01-01T03:00:00.000000Z','B',3), " +
                "('2020-01-03T01:00:00.000000Z','A',30), " +
                "('2020-01-02T05:00:00.000000Z','A',24), " +
                "('2020-01-01T04:00:00.000000Z','A',4), " +
                "('2020-01-03T05:00:00.000000Z','B',34), " +
                "('2020-01-02T03:00:00.000000Z','B',22), " +
                "('2020-01-03T03:00:00.000000Z','A',32)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();
    }
}
