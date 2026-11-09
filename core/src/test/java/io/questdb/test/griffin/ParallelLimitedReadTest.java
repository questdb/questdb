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
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * A LIMIT over a parallel filter or a HORIZON JOIN projection dispatches page frame tasks ahead of
 * its reader and sizes them to the rows the reader reads. These tests read such LIMITs past the
 * first dispatched frames, stop early, and read again, on a pool of 4 workers, and compare the
 * rows with a model of the table: x runs from 1 to ROW_COUNT and v = x % 1000, so v = 0 holds once
 * per 1_000 rows, as many rows as a page frame holds.
 */
public class ParallelLimitedReadTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_MAX_ROWS = 1_000;
    private static final int ROW_COUNT = 100_000;
    private final Rnd rnd;

    public ParallelLimitedReadTest() {
        this.rnd = TestUtils.generateRandom(LOG);
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 16);
        // 1 caps every limited read at the start, 16 fills the queue at once
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1 + rnd.nextInt(16));
        // the scans ahead of RIGHT and FULL joins start with frames of 1 to 100 rows
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, 1 + rnd.nextInt(100));
        super.setUp();
    }

    @Test
    public void testFilterLimitReadsPastTheFirstFrames() throws Exception {
        withWorkers((engine, ctx) -> {
            // the 51st to 60th match, one per frame: the read collects 60 frames
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = 0 LIMIT 50, 60", sparse(50, 60));
            // the single-bound form pushes the LIMIT into the filter
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = 0 LIMIT 60", sparse(0, 60));
            // no row matches: the read collects every frame
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = -1 LIMIT 0, 10", "x\n");
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = -1 LIMIT 10", "x\n");
            // the reader reads every row after the skip
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = 0 LIMIT 95, 9_223_372_036_854_775_807", sparse(95, 100));
            // the rows of the first frame satisfy the LIMIT
            assertRows(engine, ctx, "SELECT x FROM t WHERE v < 500 LIMIT 1000, 1010", dense(1000, 1010));
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = 0 LIMIT 0, 3", sparse(0, 3));
            // a Java filter
            assertRows(engine, ctx, "SELECT x FROM t WHERE abs(v) = 0 LIMIT 50, 60", sparse(50, 60));
            // backward scan
            assertRows(engine, ctx, "SELECT x FROM t WHERE v = 0 ORDER BY ts DESC LIMIT 20, 25", sparseDesc(20, 25));
        });
    }

    @Test
    public void testHorizonJoinLimitReadsPastTheFirstFrames() throws Exception {
        withWorkers((engine, ctx) -> {
            // q holds a row at every second of t, so the 0s offset matches y = t.x
            engine.execute(
                    """
                            CREATE TABLE q AS (
                                SELECT x y, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(100_000)
                            ) TIMESTAMP(ts) PARTITION BY HOUR
                            """,
                    ctx
            );
            final String horizon = "SELECT a.x, h.offset, b.y FROM t a HORIZON JOIN q b LIST (0s) AS h WHERE a.v = 0";
            assertRowsNoRandomAccess(engine, ctx, horizon + " LIMIT 50, 60", horizonRows(50, 60));
            assertRowsNoRandomAccess(engine, ctx, horizon + " LIMIT 60", horizonRows(0, 60));
            assertRowsNoRandomAccess(engine, ctx, horizon + " LIMIT 0, 3", horizonRows(0, 3));
            assertRowsNoRandomAccess(
                    engine,
                    ctx,
                    "SELECT a.x, h.offset, b.y FROM t a HORIZON JOIN q b LIST (0s) AS h WHERE a.v = -1 LIMIT 10",
                    "x\toffset\ty\n"
            );
        });
    }

    @Test
    public void testJoinMasterLimitReadsPastTheFirstFrames() throws Exception {
        withWorkers((engine, ctx) -> {
            // s holds every second match of t
            engine.execute("CREATE TABLE s AS (SELECT x * 2_000 x, x y FROM long_sequence(50))", ctx);
            final StringBuilder hash = new StringBuilder("x\ty\n");
            for (long k = 21; k <= 30; k++) {
                hash.append(k * 2_000).append('\t').append(k).append('\n');
            }
            assertRowsNoRandomAccess(engine, ctx, "SELECT a.x, b.y FROM t a JOIN s b ON a.x = b.x WHERE a.v = 0 LIMIT 20, 30", hash.toString());
            assertRowsNoRandomAccess(engine, ctx, "SELECT a.x, b.y FROM t a JOIN s b ON a.x = b.x WHERE a.v = -1 LIMIT 0, 10", "x\ty\n");

            // every row of t joins the single row of z
            engine.execute("CREATE TABLE z AS (SELECT 7L y, 0::TIMESTAMP ts FROM long_sequence(1)) TIMESTAMP(ts)", ctx);
            final StringBuilder asOf = new StringBuilder("x\ty\n");
            for (long k = 51; k <= 60; k++) {
                asOf.append(k * 1_000).append("\t7\n");
            }
            assertRowsNoRandomAccess(engine, ctx, "SELECT a.x, b.y FROM t a ASOF JOIN z b WHERE a.v = 0 LIMIT 50, 60", asOf.toString());
        });
    }

    @Test
    public void testLimitBindVariablesOnEveryExecution() throws Exception {
        withWorkers((engine, ctx) -> {
            final String sql = "SELECT x FROM t WHERE v = 0 LIMIT $1, $2";
            final long[][] bounds = {{50, 60}, {0, 3}, {-5, 0}, {90, 95}, {0, 0}, {3, 1}};
            for (long[] bound : bounds) {
                ctx.getBindVariableService().clear();
                ctx.getBindVariableService().setLong(0, bound[0]);
                ctx.getBindVariableService().setLong(1, bound[1]);
                final long lo;
                final long hi;
                if (bound[0] < 0) {
                    // LIMIT -5, 0: the last 5 rows
                    lo = 100 + bound[0];
                    hi = 100 + bound[1];
                } else {
                    // LIMIT 3, 1 reads rows 1 to 2, as LIMIT 1, 3 does
                    lo = Math.min(bound[0], bound[1]);
                    hi = Math.max(bound[0], bound[1]);
                }
                assertRows(engine, ctx, sql, sparse(lo, hi));
            }
            ctx.getBindVariableService().clear();
        });
    }

    @Test
    public void testLimitedReadAgainAfterEarlyStop() throws Exception {
        withWorkers((engine, ctx) -> {
            // the CROSS JOIN reads its slave again for every master row, after the slave's LIMIT
            // stopped it ahead of the frames it dispatched
            for (int[] bound : new int[][]{{0, 3}, {40, 43}}) {
                final StringBuilder expected = new StringBuilder("k\tx\n");
                for (int k = 1; k <= 3; k++) {
                    for (int m = bound[0]; m < bound[1]; m++) {
                        expected.append(k).append('\t').append((m + 1) * 1_000L).append('\n');
                    }
                }
                assertRowsNoRandomAccess(
                        engine,
                        ctx,
                        "SELECT m.k, b.x FROM (SELECT x k FROM long_sequence(3)) m"
                                + " CROSS JOIN (SELECT x FROM t WHERE v = 0 LIMIT " + bound[0] + ", " + bound[1] + ") b",
                        expected.toString()
                );
            }
        });
    }

    @Test
    public void testRightAndFullJoinLimitsReadPastTheRampFrames() throws Exception {
        withWorkers((engine, ctx) -> {
            // s holds every second row of t with v = 0
            engine.execute("CREATE TABLE s AS (SELECT x * 2_000 x, x y FROM long_sequence(50))", ctx);
            final String right = "SELECT a.x, b.y FROM t a RIGHT JOIN s b ON a.x = b.x WHERE a.v = 0";
            assertRowsNoRandomAccess(engine, ctx, right + " LIMIT 3", rightJoinRows(0, 3));
            assertRowsNoRandomAccess(engine, ctx, right + " LIMIT 20, 30", rightJoinRows(20, 30));
            final String full = "SELECT a.x, b.y FROM t a FULL JOIN s b ON a.x = b.x WHERE a.v = 0";
            assertRowsNoRandomAccess(engine, ctx, full + " LIMIT 3", fullJoinRows(0, 3));
            assertRowsNoRandomAccess(engine, ctx, full + " LIMIT 95, 100", fullJoinRows(95, 100));
            // no row of t passes the filter: the read collects every frame
            assertRowsNoRandomAccess(engine, ctx, "SELECT a.x, b.y FROM t a RIGHT JOIN s b ON a.x = b.x WHERE a.v = -1 LIMIT 10", "x\ty\n");
            // the only match sits in the last hour: the read passes every frame of the ramp-up
            engine.execute("CREATE TABLE z AS (SELECT 99_000L x, 1L y FROM long_sequence(1))", ctx);
            assertRowsNoRandomAccess(engine, ctx, "SELECT a.x, b.y FROM t a RIGHT JOIN z b ON a.x = b.x WHERE a.v = 0 LIMIT 1", "x\ty\n99000\t1\n");
        });
    }

    // Rows lo (inclusive) to hi (exclusive) of v < 500: x % 1000 < 500, x from 1.
    private static String dense(int lo, int hi) {
        final StringBuilder sb = new StringBuilder("x\n");
        int index = 0;
        for (long x = 1; x <= ROW_COUNT && index < hi; x++) {
            if (x % 1_000 < 500) {
                if (index >= lo) {
                    sb.append(x).append('\n');
                }
                index++;
            }
        }
        return sb.toString();
    }

    // Rows lo (inclusive) to hi (exclusive) of the FULL join of t and s: every row of t with v = 0, x = 1000 * k,
    // with y = k / 2 for the even k that s holds and NULL for the odd ones.
    private static String fullJoinRows(int lo, int hi) {
        final StringBuilder sb = new StringBuilder("x\ty\n");
        for (long k = lo + 1; k <= Math.min(hi, 100); k++) {
            sb.append(k * 1_000).append('\t');
            if (k % 2 == 0) {
                sb.append(k / 2);
            } else {
                sb.append("null");
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String horizonRows(int lo, int hi) {
        final StringBuilder sb = new StringBuilder("x\toffset\ty\n");
        for (long k = lo + 1; k <= Math.min(hi, 100); k++) {
            sb.append(k * 1_000).append("\t0\t").append(k * 1_000).append('\n');
        }
        return sb.toString();
    }

    // Rows lo (inclusive) to hi (exclusive) of the RIGHT join of t and s: the rows of s, x = 2000 * k and y = k,
    // in the order of t, as every row of s matches a row of t with v = 0.
    private static String rightJoinRows(int lo, int hi) {
        final StringBuilder sb = new StringBuilder("x\ty\n");
        for (long k = lo + 1; k <= Math.min(hi, 50); k++) {
            sb.append(k * 2_000).append('\t').append(k).append('\n');
        }
        return sb.toString();
    }

    // Rows lo (inclusive) to hi (exclusive) of v = 0: x = 1000, 2000, ..., 100000.
    private static String sparse(long lo, long hi) {
        final StringBuilder sb = new StringBuilder("x\n");
        for (long k = lo + 1; k <= Math.min(hi, 100); k++) {
            sb.append(k * 1_000).append('\n');
        }
        return sb.toString();
    }

    private static String sparseDesc(int lo, int hi) {
        final StringBuilder sb = new StringBuilder("x\n");
        for (long k = 100 - lo; k > 100 - hi; k--) {
            sb.append(k * 1_000).append('\n');
        }
        return sb.toString();
    }

    private void assertRows(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected) throws Exception {
        assertQuery(sql)
                .withEngine(engine)
                .withContext(ctx)
                .noLeakCheck()
                .sizeMayVary()
                .returns(expected);
    }

    private void assertRowsNoRandomAccess(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected) throws Exception {
        assertQuery(sql)
                .withEngine(engine)
                .withContext(ctx)
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .returns(expected);
    }

    private void withWorkers(TableTestBody body) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, ctx) -> {
                        engine.execute(
                                """
                                        CREATE TABLE t AS (
                                            SELECT x, x % 1_000 v, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(100_000)
                                        ) TIMESTAMP(ts) PARTITION BY HOUR
                                        """,
                                ctx
                        );
                        body.run(engine, ctx);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @FunctionalInterface
    private interface TableTestBody {
        void run(CairoEngine engine, SqlExecutionContext ctx) throws Exception;
    }
}
