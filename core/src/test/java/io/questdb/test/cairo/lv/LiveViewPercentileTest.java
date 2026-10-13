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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Live view coverage for the percentile functions: {@code percentile_disc}, {@code percentile_cont}, their
 * {@code quantile_disc} / {@code quantile_cont} aliases, and the {@code DOUBLE[]} multi-percentile variants
 * of those and of {@code approx_percentile}.
 * <p>
 * A live view cannot use these functions in its own SELECT. Their window form computes over a whole
 * partition in two passes, which incremental refresh cannot drive, and their aggregate form needs GROUP BY
 * or SAMPLE BY, which leaves the view without the window function it refreshes. The
 * {@code testCreateRejects*} tests pin where CREATE turns each shape away, and that the reject leaves no
 * view behind.
 * <p>
 * Queries over a live view do run them, over both of the view's tiers: the rows the refresh has flushed to
 * the view's table, and the un-flushed lead it keeps in memory. Every query the read tests pin reads rows
 * from the lead, and its result differs from the answer over the flushed rows alone. The queries cover the
 * routes a read takes into the view: page frames (parallel GROUP BY, SAMPLE BY, WINDOW JOIN and HORIZON
 * JOIN slaves), the record cursor (single-threaded GROUP BY, window functions that position back into the
 * view by row id), and the interval-filtered and descending scans.
 */
public class LiveViewPercentileTest extends AbstractLiveViewTest {

    private static final String CREATE_LIVE_VIEW = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS ";
    // The parallel fixture's three batches: the first lands at TIERED_DATA_START, the second five seconds on,
    // past the 1s IN MEMORY window, and the third right behind the second.
    private static final long TIERED_BATCH_ROWS = 1_000;
    private static final long TIERED_DATA_START = 1_778_544_000_000_000L; // 2026-05-12T00:00:00Z

    // A non-SEED view drops rows below its CREATE wall-clock floor; pin the clock below the 2026 test data so
    // every row stays in the view.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testApproxPercentileOverLiveView() throws Exception {
        // The multi-percentile approx_percentile does not run in parallel, so GROUP BY reads the view through
        // its record cursor rather than through its page frames. Precision 3 picks the packed histograms,
        // the default precision the plain ones.
        assertMemoryLeak(() -> {
            createLiveViewWithLead();

            // Over the flushed rows alone l is [10, 20, 30], and apl would read [20.0,30.0].
            assertQuery("""
                    SELECT approx_percentile(l, ARRAY[0.5, 1.0], 3) apl, approx_percentile(v, ARRAY[0.5, 1.0], 3) apv,
                        approx_percentile(l, ARRAY[0.5, 1.0]) apl1, approx_percentile(v, ARRAY[0.5, 1.0]) apv1
                    FROM lv
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("GroupBy vectorized: false", "LiveView", "inMemory: true")
                    .returns("""
                            apl\tapv\tapl1\tapv1
                            [30.0,600.0]\t[3.0009765625,60.0302734375]\t[30.0,607.0]\t[3.0625,61.9375]
                            """);
            assertQuery("SELECT sym, approx_percentile(l, ARRAY[0.5, 1.0], 3) apl FROM lv ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("GroupBy vectorized: false", "LiveView")
                    .returns("""
                            sym\tapl
                            a\t[30.0,500.0]
                            b\t[20.0,600.0]
                            c\t[400.0,400.0]
                            """);
            // 'c' has no row until the lead, so its first bucket has no earlier value to fill from and must
            // read as null, not as the histogram of another key. Its 00:00:05 bucket holds only the NULL row,
            // so it stays null, and FILL(PREV) carries that null into 00:00:07.
            assertQuery("""
                    SELECT ts, sym, approx_percentile(l, ARRAY[0.5, 1.0], 3) apl, approx_percentile(v, ARRAY[0.5, 1.0]) apv1,
                        approx_percentile(l, 0.5, 3) ap, approx_median(v) am
                    FROM lv
                    SAMPLE BY 2s FILL(PREV) ALIGN TO FIRST OBSERVATION
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tapl\tapv1\tap\tam
                            2026-05-12T00:00:01.000000Z\ta\t[10.0,10.0]\t[1.0,1.0]\t10.0\t1.0
                            2026-05-12T00:00:01.000000Z\tb\t[20.0,20.0]\t[2.0,2.0]\t20.0\t2.0
                            2026-05-12T00:00:01.000000Z\tc\tnull\tnull\tnull\tnull
                            2026-05-12T00:00:03.000000Z\ta\t[30.0,30.0]\t[3.0,3.0]\t30.0\t3.0
                            2026-05-12T00:00:03.000000Z\tb\t[20.0,20.0]\t[2.0,2.0]\t20.0\t2.0
                            2026-05-12T00:00:03.000000Z\tc\t[400.0,400.0]\t[40.0,40.0]\t400.0\t40.0
                            2026-05-12T00:00:05.000000Z\ta\t[500.0,500.0]\t[50.0,50.0]\t500.0\t50.0
                            2026-05-12T00:00:05.000000Z\tb\t[20.0,20.0]\t[2.0,2.0]\t20.0\t2.0
                            2026-05-12T00:00:05.000000Z\tc\tnull\tnull\tnull\tnull
                            2026-05-12T00:00:07.000000Z\ta\t[500.0,500.0]\t[50.0,50.0]\t500.0\t50.0
                            2026-05-12T00:00:07.000000Z\tb\t[600.0,600.0]\t[60.0,60.0]\t600.0\t60.0
                            2026-05-12T00:00:07.000000Z\tc\tnull\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testCompiledQueryFollowsRefreshes() throws Exception {
        // A live view serves the same query over and over, so a client compiles it once and re-runs it as
        // refreshes land. Each run must rebuild the functions' state from that run's rows: the second run
        // below reads three more rows through the same factories, so a function that carried state over from
        // the first run would report a stale value.
        assertMemoryLeak(() -> {
            createLiveViewWithLead();
            try (
                    RecordCursorFactory parallelGroupBy = select("SELECT percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl, percentile_cont(v, ARRAY[0.25, 0.75]) pca FROM lv");
                    RecordCursorFactory groupBy = select("SELECT approx_percentile(l, ARRAY[0.5, 1.0], 3) apl FROM lv");
                    RecordCursorFactory window = select("SELECT ts, sym, v, percentile_disc(v, 0.5) OVER (PARTITION BY sym) pd, percentile_cont(v, 0.5) OVER () pc FROM lv");
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)
            ) {
                assertFactory(parallelGroupBy)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                pd\tpc\tpl\tpca
                                3.0\t21.5\t30\t[2.25,47.5]
                                """);
                assertFactory(groupBy)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                apl
                                [30.0,600.0]
                                """);
                assertFactory(window)
                        .withContext(sqlExecutionContext)
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tv\tpd\tpc
                                2026-05-12T00:00:01.000000Z\ta\t1.0\t3.0\t21.5
                                2026-05-12T00:00:02.000000Z\tb\t2.0\t2.0\t21.5
                                2026-05-12T00:00:03.000000Z\ta\t3.0\t3.0\t21.5
                                2026-05-12T00:00:04.000000Z\tc\t40.0\t40.0\t21.5
                                2026-05-12T00:00:05.000000Z\ta\t50.0\t3.0\t21.5
                                2026-05-12T00:00:06.000000Z\tc\tnull\t40.0\t21.5
                                2026-05-12T00:00:07.000000Z\tb\t60.0\t2.0\t21.5
                                """);

                execute("""
                        INSERT INTO base (ts, sym, v, l) VALUES
                        ('2026-05-12T00:00:08.000000Z', 'c', 20.0, 200),
                        ('2026-05-12T00:00:09.000000Z', 'a', 30.0, 300),
                        ('2026-05-12T00:00:10.000000Z', 'a', 35.0, 350)
                        """);
                drainWalQueue();
                drainJob(job); // the clock has not moved, so the new rows join the un-flushed lead
                assertTierGeometry(3, 10, 7);

                // v is now [1, 2, 3, 20, 30, 35, 40, 50, 60] and l the same times ten.
                assertFactory(parallelGroupBy)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                pd\tpc\tpl\tpca
                                30.0\t30.0\t300\t[3.0,40.0]
                                """);
                assertFactory(groupBy)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                apl
                                [300.0,600.0]
                                """);
                assertFactory(window)
                        .withContext(sqlExecutionContext)
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tv\tpd\tpc
                                2026-05-12T00:00:01.000000Z\ta\t1.0\t30.0\t30.0
                                2026-05-12T00:00:02.000000Z\tb\t2.0\t2.0\t30.0
                                2026-05-12T00:00:03.000000Z\ta\t3.0\t30.0\t30.0
                                2026-05-12T00:00:04.000000Z\tc\t40.0\t20.0\t30.0
                                2026-05-12T00:00:05.000000Z\ta\t50.0\t30.0\t30.0
                                2026-05-12T00:00:06.000000Z\tc\tnull\t20.0\t30.0
                                2026-05-12T00:00:07.000000Z\tb\t60.0\t2.0\t30.0
                                2026-05-12T00:00:08.000000Z\tc\t20.0\t20.0\t30.0
                                2026-05-12T00:00:09.000000Z\ta\t30.0\t30.0\t30.0
                                2026-05-12T00:00:10.000000Z\ta\t35.0\t30.0\t30.0
                                """);
            }
        });
    }

    @Test
    public void testCreateRejectsPercentileAggregate() throws Exception {
        // A live view refreshes window functions over a scan of its base table. A SELECT built around a
        // percentile aggregate has no window function for the view to refresh, and moving the aggregate into
        // a subquery takes the base table out of FROM.
        assertMemoryLeak(() -> {
            createBaseTable();
            assertRejectedAtStatementToken(
                    "SELECT ts, sym, percentile_cont(v, 0.99) p99 FROM base SAMPLE BY 1m",
                    "lv",
                    "live view select must contain at least one window function"
            );
            assertRejectedAtStatementToken(
                    "SELECT sym, percentile_disc(v, ARRAY[0.5, 0.99]) pda, percentile_disc(l, 0.5) pl, "
                            + "approx_percentile(v, ARRAY[0.5, 0.99], 3) apa FROM base",
                    "lv",
                    "live view select must contain at least one window function"
            );
            assertRejectedAtStatementToken(
                    "SELECT ts, sym, p99, max(p99) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) m "
                            + "FROM (SELECT ts, sym, percentile_cont(v, 0.99) p99 FROM base SAMPLE BY 1m)",
                    "SELECT",
                    "live view requires a single base table in FROM clause"
            );
        });
    }

    @Test
    public void testCreateRejectsPercentileWindowOverOrderedFrame() throws Exception {
        // The window shapes the parser lets through - an anchored named window, which has to ORDER BY the
        // designated timestamp, and a bounded ROWS or RANGE frame - are not whole partitions, and the
        // percentile window functions compute over whole partitions only. The function refuses the frame
        // itself when CREATE compiles the SELECT.
        assertMemoryLeak(() -> {
            createBaseTable();
            assertRejectedAtSelectToken(
                    "SELECT ts, v, percentile_disc(v, 0.5) OVER w p FROM base "
                            + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')",
                    "percentile_disc",
                    "percentile_disc window function only supports whole partition frames"
            );
            assertRejectedAtSelectToken(
                    "SELECT ts, v, percentile_cont(v, ARRAY[0.25, 0.75]) OVER w p FROM base "
                            + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))",
                    "percentile_cont",
                    "percentile_cont window function only supports whole partition frames"
            );
            assertRejectedAtSelectToken(
                    "SELECT ts, v, percentile_cont(v, 0.5) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) p FROM base",
                    "percentile_cont",
                    "percentile_cont window function only supports whole partition frames"
            );
            assertRejectedAtSelectToken(
                    "SELECT ts, v, percentile_disc(v, ARRAY[0.25, 0.75]) OVER (ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) p FROM base",
                    "percentile_disc",
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testCreateRejectsPercentileWindowOverPartition() throws Exception {
        // A whole-partition frame over PARTITION BY keeps state for every key the view ever sees, so the view
        // needs an ANCHOR to bound it. The reject points at the PARTITION BY key. Adding the ANCHOR does not
        // help: an anchored window orders its rows, which testCreateRejectsPercentileWindowOverOrderedFrame
        // covers.
        assertMemoryLeak(() -> {
            createBaseTable();
            assertPartitionedCallRejected("percentile_disc(v, 0.5)");
            assertPartitionedCallRejected("percentile_cont(v, 0.5)");
            assertPartitionedCallRejected("quantile_disc(v, 0.5)");
            assertPartitionedCallRejected("quantile_cont(v, 0.5)");
            assertPartitionedCallRejected("percentile_disc(l, 0.5)");
            assertPartitionedCallRejected("quantile_disc(l, 0.5)");
            assertPartitionedCallRejected("percentile_disc(v, ARRAY[0.25, 0.75])");
            assertPartitionedCallRejected("percentile_cont(v, ARRAY[0.25, 0.75])");
            assertPartitionedCallRejected("quantile_disc(v, ARRAY[0.25, 0.75])");
            assertPartitionedCallRejected("quantile_cont(v, ARRAY[0.25, 0.75])");
        });
    }

    @Test
    public void testCreateRejectsPercentileWindowOverWholeResultSet() throws Exception {
        // OVER () holds a single partition, so the bare-unbounded-window rule lets it through, but its default
        // frame starts at UNBOUNDED PRECEDING: one late row would move the percentile of every row in the
        // view. The finite-influence gate turns that frame away while parsing CREATE, and names the call.
        assertMemoryLeak(() -> {
            createBaseTable();
            assertWholeResultSetCallRejected("percentile_disc(v, 0.5)");
            assertWholeResultSetCallRejected("percentile_cont(v, 0.5)");
            assertWholeResultSetCallRejected("quantile_disc(v, 0.5)");
            assertWholeResultSetCallRejected("quantile_cont(v, 0.5)");
            assertWholeResultSetCallRejected("percentile_disc(l, 0.5)");
            assertWholeResultSetCallRejected("quantile_disc(l, 0.5)");
            assertWholeResultSetCallRejected("percentile_disc(v, ARRAY[0.25, 0.75])");
            assertWholeResultSetCallRejected("percentile_cont(v, ARRAY[0.25, 0.75])");
            assertWholeResultSetCallRejected("quantile_disc(v, ARRAY[0.25, 0.75])");
            assertWholeResultSetCallRejected("quantile_cont(v, ARRAY[0.25, 0.75])");
            // The gate also reaches a call nested in an expression, and one next to a window function the view
            // could refresh incrementally.
            assertRejectedAtStatementToken(
                    "SELECT ts, v, v - percentile_cont(v, 0.5) OVER () d FROM base",
                    "percentile_cont",
                    "live view select cannot use percentile_cont() over a frame starting at UNBOUNDED PRECEDING"
            );
            assertRejectedAtStatementToken(
                    "SELECT ts, v, sum(v) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s, "
                            + "percentile_disc(v, ARRAY[0.5, 0.99]) OVER () p FROM base",
                    "percentile_disc",
                    "live view select cannot use percentile_disc() over a frame starting at UNBOUNDED PRECEDING"
            );
        });
    }

    @Test
    public void testGroupByOverLiveView() throws Exception {
        // These aggregates run in parallel, so GROUP BY and SAMPLE BY read the view through its page frames,
        // the lead's included. Over the flushed rows alone v is [1, 2, 3], l is [10, 20, 30] and rn is
        // [1, 1, 2], and every result below would differ.
        assertMemoryLeak(() -> {
            createLiveViewWithLead();

            // rn is the view's own window column, computed by the refresh rather than read from the base.
            assertQuery("""
                    SELECT percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl,
                        quantile_disc(v, 0.5) qd, quantile_cont(v, 0.5) qc, quantile_disc(l, 0.5) ql,
                        percentile_disc(rn, 0.5) prn
                    FROM lv
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "LiveView", "inMemory: true")
                    .returns("""
                            pd\tpc\tpl\tqd\tqc\tql\tprn
                            3.0\t21.5\t30\t3.0\t21.5\t30\t2
                            """);
            assertQuery("""
                    SELECT percentile_disc(v, ARRAY[0.25, 0.5, 0.75]) pda, percentile_cont(v, ARRAY[0.25, 0.5, 0.75]) pca,
                        quantile_disc(v, ARRAY[0.25, 0.75]) qda, quantile_cont(v, ARRAY[0.25, 0.75]) qca
                    FROM lv
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "LiveView")
                    .returns("""
                            pda\tpca\tqda\tqca
                            [2.0,3.0,50.0]\t[2.25,21.5,47.5]\t[2.0,50.0]\t[2.25,47.5]
                            """);
            // 'c' first appears in the lead, so its key resolves only through the in-memory tier's symbol
            // cache, and its NULL row must not reach the percentile.
            assertQuery("""
                    SELECT sym, percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl,
                        percentile_disc(v, ARRAY[0.25, 0.75]) pda, percentile_cont(v, ARRAY[0.25, 0.75]) pca
                    FROM lv
                    ORDER BY sym
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Group By", "LiveView")
                    .returns("""
                            sym\tpd\tpc\tpl\tpda\tpca
                            a\t3.0\t3.0\t30\t[1.0,50.0]\t[2.0,26.5]
                            b\t2.0\t31.0\t20\t[2.0,60.0]\t[16.5,45.5]
                            c\t40.0\t40.0\t400\t[40.0,40.0]\t[40.0,40.0]
                            """);
            // An interval filter routes the read lead-only: the filtered table scan, then the lead cut by the
            // same interval.
            assertQuery("SELECT percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl FROM lv WHERE ts >= '2026-05-12T00:00:03'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "LiveView", "Interval forward scan")
                    .returns("""
                            pd\tpc\tpl
                            40.0\t45.0\t400
                            """);
            // The first two buckets hold only flushed rows, the last two only lead rows.
            assertQuery("SELECT ts, percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_cont(v, ARRAY[0.5, 1.0]) pca FROM lv SAMPLE BY 2s")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("Async Group By", "LiveView")
                    .returns("""
                            ts\tpd\tpc\tpca
                            2026-05-12T00:00:00.000000Z\t1.0\t1.0\t[1.0,1.0]
                            2026-05-12T00:00:02.000000Z\t2.0\t2.5\t[2.5,3.0]
                            2026-05-12T00:00:04.000000Z\t40.0\t45.0\t[45.0,50.0]
                            2026-05-12T00:00:06.000000Z\t60.0\t60.0\t[60.0,60.0]
                            """);
        });
    }

    @Test
    public void testJoinAggregatesOverLiveViewSlave() throws Exception {
        // A WINDOW JOIN or HORIZON JOIN slave walks the view's page frames one table partition at a time and
        // takes the lead as a partition of its own. Probes at 00:00:05 and 00:00:06 aggregate lead rows only,
        // and the one at 00:00:30 has no view row within a second of it, so every aggregate over its window,
        // approx_percentile's included, must read as null.
        assertMemoryLeak(() -> {
            createLiveViewWithLead();
            execute("CREATE TABLE probe (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO probe VALUES
                    ('2026-05-12T00:00:00.000000Z'),
                    ('2026-05-12T00:00:02.000000Z'),
                    ('2026-05-12T00:00:05.000000Z'),
                    ('2026-05-12T00:00:06.000000Z'),
                    ('2026-05-12T00:00:30.000000Z')
                    """);
            drainWalQueue();

            assertQuery("""
                    SELECT p.ts, percentile_disc(lv.v, 0.5) pd, percentile_cont(lv.v, 0.5) pc, percentile_disc(lv.l, 0.5) pl,
                        percentile_cont(lv.v, ARRAY[0.25, 0.75]) pca
                    FROM probe p
                    WINDOW JOIN lv RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING EXCLUDE PREVAILING
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("Async Window Join", "LiveView")
                    .returns("""
                            ts\tpd\tpc\tpl\tpca
                            2026-05-12T00:00:00.000000Z\t1.0\t1.0\t10\t[1.0,1.0]
                            2026-05-12T00:00:02.000000Z\t2.0\t2.0\t20\t[1.5,2.5]
                            2026-05-12T00:00:05.000000Z\t40.0\t45.0\t400\t[42.5,47.5]
                            2026-05-12T00:00:06.000000Z\t50.0\t55.0\t500\t[52.5,57.5]
                            2026-05-12T00:00:30.000000Z\tnull\tnull\tnull\tnull
                            """);
            // approx_percentile does not run in parallel, so this join runs single-threaded.
            assertQuery("""
                    SELECT p.ts, approx_percentile(lv.l, ARRAY[0.5, 1.0], 3) apl, approx_percentile(lv.l, 0.5, 3) ap, approx_median(lv.v) am
                    FROM probe p
                    WINDOW JOIN lv RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING EXCLUDE PREVAILING
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("Window Join", "LiveView")
                    .withPlanNotContaining("Async")
                    .returns("""
                            ts\tapl\tap\tam
                            2026-05-12T00:00:00.000000Z\t[10.0,10.0]\t10.0\t1.0
                            2026-05-12T00:00:02.000000Z\t[20.0,30.0]\t20.0\t2.0625
                            2026-05-12T00:00:05.000000Z\t[400.0,500.0]\t400.0\t40.0
                            2026-05-12T00:00:06.000000Z\t[500.0,600.0]\t500.0\t50.0
                            2026-05-12T00:00:30.000000Z\tnull\tnull\tnull
                            """);
            // Each offset aggregates the view row ASOF every probe shifted by it. Offset 1 reads rows from
            // both tiers: v [1, 3, NULL, 60, 60] and l [10, 30, NULL, 600, 600].
            assertQuery("""
                    SELECT h.offset / 1_000_000 sec_offs, percentile_disc(lv.v, 0.5) pd, percentile_cont(lv.l, 0.5) pc
                    FROM probe t
                    HORIZON JOIN lv RANGE FROM 0s TO 2s STEP 1s AS h
                    ORDER BY sec_offs
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Horizon Join", "LiveView")
                    .returns("""
                            sec_offs\tpd\tpc
                            0\t50.0\t500.0
                            1\t3.0\t315.0
                            2\t60.0\t600.0
                            """);
        });
    }

    @Test
    public void testParallelGroupByMergesDiskAndInMemoryFrames() throws Exception {
        // 100-row page frames split both tiers into many frames. The view's table holds batches 1 and 2, its
        // in-memory slot batch 2 as overlap plus batch 3 as the un-flushed lead, so the read serves batch 1
        // from ten table frames and batches 2 and 3 from twenty slot frames. Four workers aggregate those
        // frames in parallel, and merge() combines their partial lists whichever tier each frame came from.
        //
        // Row g (1..3,000) carries v = 7g mod 3,000 + 1, which takes every value in 1..3,000 once in an
        // order unrelated to time, l = g and sym = 'k' || g % 4. As 4 divides 3,000, the v values of key
        // k<j> form the arithmetic progression c + 4i, i = 0..749, with c = 1, 4, 3, 2 for j = 0..3, and its
        // l values do too, starting at 4, 1, 2, 3.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            createLiveViewWithEvictedOverlapAndLead();
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            WorkerPoolUtils.setupQueryJobs(pool, engine, true);
            pool.start(LOG);
            try (SqlExecutionContext parallelContext = TestUtils.createSqlExecutionCtx(engine, bindVariableService, 4)) {
                assertQuery("""
                        SELECT percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl,
                            percentile_disc(v, ARRAY[0.25, 0.75]) pda, percentile_cont(v, ARRAY[0.25, 0.75]) pca
                        FROM lv
                        """)
                        .withContext(parallelContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4", "LiveView", "inMemory: true")
                        .returns("""
                                pd\tpc\tpl\tpda\tpca
                                1500.0\t1500.5\t1500\t[750.0,2250.0]\t[750.75,2250.25]
                                """);
                assertQuery("""
                        SELECT sym, percentile_disc(v, 0.5) pd, percentile_cont(v, 0.5) pc, percentile_disc(l, 0.5) pl,
                            percentile_disc(v, ARRAY[0.25, 0.75]) pda, percentile_cont(v, ARRAY[0.25, 0.75]) pca
                        FROM lv
                        ORDER BY sym
                        """)
                        .withContext(parallelContext)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Async Group By workers: 4", "LiveView")
                        .returns("""
                                sym\tpd\tpc\tpl\tpda\tpca
                                k0\t1497.0\t1499.0\t1500\t[749.0,2249.0]\t[750.0,2248.0]
                                k1\t1500.0\t1502.0\t1497\t[752.0,2252.0]\t[753.0,2251.0]
                                k2\t1499.0\t1501.0\t1498\t[751.0,2251.0]\t[752.0,2250.0]
                                k3\t1498.0\t1500.0\t1499\t[750.0,2250.0]\t[751.0,2249.0]
                                """);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testWindowFunctionsOverLiveView() throws Exception {
        // The window factory reads the view through its record cursor. The light factory keeps only row ids,
        // and positions back into the view by row id both for the PARTITION BY key in the second pass and for
        // every column it emits, so a lead row has to come back as the same row. An array result takes the
        // factory that copies the rows instead.
        assertMemoryLeak(() -> {
            createLiveViewWithLead();

            // Each function runs once over PARTITION BY and once over the whole result set, the latter through
            // its quantile alias.
            assertQuery("""
                    SELECT ts, sym, v, percentile_disc(v, 0.5) OVER (PARTITION BY sym) pd, percentile_cont(v, 0.5) OVER (PARTITION BY sym) pc,
                        quantile_disc(v, 0.5) OVER () qd, quantile_cont(v, 0.5) OVER () qc
                    FROM lv
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CachedWindowLight", "LiveView", "inMemory: true")
                    .returns("""
                            ts\tsym\tv\tpd\tpc\tqd\tqc
                            2026-05-12T00:00:01.000000Z\ta\t1.0\t3.0\t3.0\t3.0\t21.5
                            2026-05-12T00:00:02.000000Z\tb\t2.0\t2.0\t31.0\t3.0\t21.5
                            2026-05-12T00:00:03.000000Z\ta\t3.0\t3.0\t3.0\t3.0\t21.5
                            2026-05-12T00:00:04.000000Z\tc\t40.0\t40.0\t40.0\t3.0\t21.5
                            2026-05-12T00:00:05.000000Z\ta\t50.0\t3.0\t3.0\t3.0\t21.5
                            2026-05-12T00:00:06.000000Z\tc\tnull\t40.0\t40.0\t3.0\t21.5
                            2026-05-12T00:00:07.000000Z\tb\t60.0\t2.0\t31.0\t3.0\t21.5
                            """);
            // The ARRAY variants cover the other two pairings: the quantile alias over PARTITION BY and the
            // percentile name over the whole result set.
            assertQuery("""
                    SELECT ts, sym, percentile_disc(v, ARRAY[0.25, 0.75]) OVER (PARTITION BY sym) pda,
                        quantile_cont(v, ARRAY[0.25, 0.75]) OVER (PARTITION BY sym) qca,
                        quantile_disc(v, ARRAY[0.25, 0.75]) OVER () qda, percentile_cont(v, ARRAY[0.25, 0.75]) OVER () pca
                    FROM lv
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CachedWindow", "LiveView")
                    .withPlanNotContaining("CachedWindowLight")
                    .returns("""
                            ts\tsym\tpda\tqca\tqda\tpca
                            2026-05-12T00:00:01.000000Z\ta\t[1.0,50.0]\t[2.0,26.5]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:02.000000Z\tb\t[2.0,60.0]\t[16.5,45.5]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:03.000000Z\ta\t[1.0,50.0]\t[2.0,26.5]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:04.000000Z\tc\t[40.0,40.0]\t[40.0,40.0]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:05.000000Z\ta\t[1.0,50.0]\t[2.0,26.5]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:06.000000Z\tc\t[40.0,40.0]\t[40.0,40.0]\t[2.0,50.0]\t[2.25,47.5]
                            2026-05-12T00:00:07.000000Z\tb\t[2.0,60.0]\t[16.5,45.5]\t[2.0,50.0]\t[2.25,47.5]
                            """);
            // A descending scan serves the lead first, then the view's table in full.
            assertQuery("SELECT ts, sym, v, percentile_cont(v, 0.5) OVER (PARTITION BY sym) pc FROM lv ORDER BY ts DESC")
                    .noLeakCheck()
                    .expectSize()
                    .timestampDesc("ts")
                    .withPlanContaining("CachedWindowLight", "LiveView", "Row backward scan")
                    .returns("""
                            ts\tsym\tv\tpc
                            2026-05-12T00:00:07.000000Z\tb\t60.0\t31.0
                            2026-05-12T00:00:06.000000Z\tc\tnull\t40.0
                            2026-05-12T00:00:05.000000Z\ta\t50.0\t3.0
                            2026-05-12T00:00:04.000000Z\tc\t40.0\t40.0
                            2026-05-12T00:00:03.000000Z\ta\t3.0\t3.0
                            2026-05-12T00:00:02.000000Z\tb\t2.0\t31.0
                            2026-05-12T00:00:01.000000Z\ta\t1.0\t3.0
                            """);
            // The interval drops rows from both tiers before the window sees them.
            assertQuery("SELECT ts, sym, v, percentile_cont(v, 0.5) OVER (PARTITION BY sym) pc FROM lv WHERE ts >= '2026-05-12T00:00:03'")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CachedWindowLight", "LiveView", "Interval forward scan")
                    .returns("""
                            ts\tsym\tv\tpc
                            2026-05-12T00:00:03.000000Z\ta\t3.0\t26.5
                            2026-05-12T00:00:04.000000Z\tc\t40.0\t40.0
                            2026-05-12T00:00:05.000000Z\ta\t50.0\t26.5
                            2026-05-12T00:00:06.000000Z\tc\tnull\t40.0
                            2026-05-12T00:00:07.000000Z\tb\t60.0\t60.0
                            """);
        });
    }

    private static void assertLiveViewNotCreated() {
        Assert.assertNull("a rejected CREATE must not register the view", engine.getLiveViewRegistry().getViewInstance("lv"));
        Assert.assertNull("a rejected CREATE must not create the view's table", engine.getTableTokenIfExists("lv"));
    }

    private static void assertPartitionedCallRejected(String call) throws Exception {
        assertRejectedAtStatementToken(
                "SELECT ts, v, " + call + " OVER (PARTITION BY sym) p FROM base",
                "sym",
                "live view unbounded window must have an ANCHOR clause"
        );
    }

    // CairoEngine.createLiveView() compiles the SELECT text on its own, so a reject raised while compiling it
    // carries a position into the SELECT rather than into the whole statement.
    private static void assertRejectedAtSelectToken(String selectSql, String token, String message) throws Exception {
        assertExceptionNoLeakCheck(CREATE_LIVE_VIEW + selectSql, selectSql.indexOf(token), message);
        assertLiveViewNotCreated();
    }

    // The parser and the factory validation report positions into the whole CREATE statement.
    private static void assertRejectedAtStatementToken(String selectSql, String token, String message) throws Exception {
        final String createSql = CREATE_LIVE_VIEW + selectSql;
        assertExceptionNoLeakCheck(createSql, createSql.indexOf(token), message);
        assertLiveViewNotCreated();
    }

    private static void assertTierGeometry(long tableRows, long slotRows, long leadRows) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull(tier);
        final LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
        Assert.assertEquals("rows in the published in-memory slot", slotRows, slot.rowCount());
        Assert.assertEquals("un-flushed lead rows in the slot", leadRows, slot.leadRowCount());
        try (TableReader reader = getReader("lv")) {
            Assert.assertEquals("rows flushed to the view's table", tableRows, reader.size());
        }
    }

    private static void assertWholeResultSetCallRejected(String call) throws Exception {
        final String name = call.substring(0, call.indexOf('('));
        assertRejectedAtStatementToken(
                "SELECT ts, v, " + call + " OVER () p FROM base",
                name,
                "live view select cannot use " + name + "() over a frame starting at UNBOUNDED PRECEDING"
        );
    }

    private static void createBaseTable() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v DOUBLE, l LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Builds the view the parallel test reads, after LiveViewInMemReadTest's evicted-overlap fixture: a zero
    // growth budget makes every refresh evict, and the 1s IN MEMORY window drops batch 1 from the slot once
    // batch 2 lands five seconds later. Batch 3 refreshes inside FLUSH EVERY and stays the lead.
    private static void createLiveViewWithEvictedOverlapAndLead() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        createBaseTable();
        execute(
                "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1s START FROM NOW AS "
                        + "SELECT ts, sym, v, l, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base"
        );
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            insertBatch(1, TIERED_DATA_START);
            drainWalQueue();
            setCurrentMicros(250_000L); // past FLUSH EVERY: flushes batch 1
            drainJob(job);

            insertBatch(TIERED_BATCH_ROWS + 1, TIERED_DATA_START + 5_000_000L);
            drainWalQueue();
            setCurrentMicros(500_000L); // flushes batch 2 and evicts batch 1 from the slot
            drainJob(job);

            insertBatch(2 * TIERED_BATCH_ROWS + 1, TIERED_DATA_START + 5_100_000L);
            drainWalQueue();
            setCurrentMicros(550_000L); // 50ms on, inside FLUSH EVERY: batch 3 stays the un-flushed lead
            drainJob(job);
        }
        drainWalQueue();
        assertTierGeometry(2 * TIERED_BATCH_ROWS, 2 * TIERED_BATCH_ROWS, TIERED_BATCH_ROWS);
    }

    // Builds the view the other read tests query: the first refresh flushes three rows to the view's table,
    // and a second one, with the clock still inside FLUSH EVERY, keeps four more as the un-flushed lead.
    //
    //   flushed: 00:00:01 a v=1 l=10, 00:00:02 b v=2 l=20, 00:00:03 a v=3 l=30
    //   lead:    00:00:04 c v=40 l=400, 00:00:05 a v=50 l=500, 00:00:06 c NULL NULL, 00:00:07 b v=60 l=600
    private static void createLiveViewWithLead() throws Exception {
        createBaseTable();
        execute(
                "CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS "
                        + "SELECT ts, sym, v, l, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base"
        );
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("""
                    INSERT INTO base (ts, sym, v, l) VALUES
                    ('2026-05-12T00:00:01.000000Z', 'a', 1.0, 10),
                    ('2026-05-12T00:00:02.000000Z', 'b', 2.0, 20),
                    ('2026-05-12T00:00:03.000000Z', 'a', 3.0, 30)
                    """);
            drainWalQueue();
            drainJob(job); // the first refresh always flushes

            execute("""
                    INSERT INTO base (ts, sym, v, l) VALUES
                    ('2026-05-12T00:00:04.000000Z', 'c', 40.0, 400),
                    ('2026-05-12T00:00:05.000000Z', 'a', 50.0, 500),
                    ('2026-05-12T00:00:06.000000Z', 'c', NULL, NULL),
                    ('2026-05-12T00:00:07.000000Z', 'b', 60.0, 600)
                    """);
            drainWalQueue();
            drainJob(job); // the clock has not moved, so these rows stay the un-flushed lead
        }
        assertTierGeometry(3, 7, 4);
    }

    // Appends TIERED_BATCH_ROWS rows numbered from firstRow, 100us apart from firstTimestamp, carrying the
    // values testParallelGroupByMergesDiskAndInMemoryFrames describes.
    private static void insertBatch(long firstRow, long firstTimestamp) throws SqlException {
        final long rowOffset = firstRow - 1;
        execute("INSERT INTO base (ts, sym, v, l) "
                + "SELECT (" + firstTimestamp + " + (x - 1) * 100)::TIMESTAMP, "
                + "'k' || ((x + " + rowOffset + ") % 4), "
                + "((x + " + rowOffset + ") * 7 % 3_000 + 1)::DOUBLE, "
                + "x + " + rowOffset + " "
                + "FROM long_sequence(" + TIERED_BATCH_ROWS + ")");
    }
}
