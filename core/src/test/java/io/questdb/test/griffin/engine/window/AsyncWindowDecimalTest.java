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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Async Window over DECIMAL values wider than 64 bits. A record sink copies each output row into
 * its task's row buffer, and stages every DECIMAL128 and DECIMAL256 value in a field of its own on
 * the way: workers that fill their tasks' buffers at the same time must not share one. A shared
 * field returns another row's value, or a mix of two; a window sum, which checks its result for
 * overflow in that field, may also fail the query. The tests cover each buffer the workers fill:
 * the tasks of a key-major scan, the hash shards and the row slices of a plain scan, and the head
 * rows that a GROUP BY step replays.
 * <p>
 * Every test runs on four workers, at the server's default task and round sizes, over 300,000
 * rows, so that several tasks compute at once. Each compares the parallel result with the serial
 * window, with a plain scan of the table, or with sums computed outside QuestDB.
 */
public class AsyncWindowDecimalTest extends AbstractCairoTest {
    private static final String W3 = " OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)";
    // A DECIMAL128 column passed through, and windows with DECIMAL128 and DECIMAL256 results. The
    // planner walks the keys one by one, or shards them, only under an ORDER BY of every column.
    private static final String WINDOWS = "SELECT sym, ts, d128, s128, s256, l256 FROM (" +
            "SELECT sym, ts, d128, sum(d64)" + W3 + " s128, sum(d128)" + W3 + " s256, lag(d256) OVER (PARTITION BY sym ORDER BY ts) l256 FROM t" +
            ") ORDER BY sym, ts, d128, s128, s256, l256";

    @Test
    public void testChainedGroupByOverDecimalHeadRows() throws Exception {
        // A task that continues a key keeps the rows of its first group, DECIMAL columns included,
        // for the cursor to replay after the task before it. Keys A and C hold -x of their rows x,
        // B and D hold +x: c counts a key's rows, s128 and s256 sum its values, and ml is its
        // second to last x, the largest lag(x).
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, true);
            ctx.setParallelWindowEnabled(true);
            assertQuery(
                    "SELECT sym, count() c, sum(v128::DOUBLE) s128, sum(v256::DOUBLE) s256, max(l) ml FROM (" +
                            "SELECT sym, ts, v128, v256, lag(x) OVER (PARTITION BY sym ORDER BY ts) l FROM t" +
                            ") GROUP BY sym ORDER BY sym"
            )
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Async Window workers: 4", "keyShards: sym", "then: group by")
                    .returns("""
                            sym\tc\ts128\ts256\tml
                            A\t75000\t-1.125015E10\t-1.125015E10\t299996
                            B\t75000\t1.1249925E10\t1.1249925E10\t299993
                            C\t75000\t-1.125E10\t-1.125E10\t299994
                            D\t75000\t1.1250075E10\t1.1250075E10\t299995
                            """);
        });
    }

    @Test
    public void testDecimal128ColumnOverRowSlices() throws Exception {
        // the stored x is the row number: the plain scan is the reference
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, false);
            ctx.setParallelWindowEnabled(true);
            final String query = "SELECT ts, v128, d128, row_number() OVER () rn FROM t";
            assertQuery(query)
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window workers: 4", "rowSlices: true");
            assertSameRows(engine, ctx, "SELECT ts, v128, d128, x rn FROM t", query);
        });
    }

    @Test
    public void testDecimal256ColumnOverRowSlices() throws Exception {
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, false);
            ctx.setParallelWindowEnabled(true);
            final String query = "SELECT ts, v256, d256, row_number() OVER () rn FROM t";
            assertQuery(query)
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window workers: 4", "rowSlices: true");
            assertSameRows(engine, ctx, "SELECT ts, v256, d256, x rn FROM t", query);
        });
    }

    @Test
    public void testDecimalWindowOverHashShards() throws Exception {
        // without an index, the workers shard the keys of a plain scan by hash
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, false);
            ctx.setParallelWindowEnabled(true);
            assertQuery(WINDOWS)
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window workers: 4", "hashShards: sym");
            assertMatchesSerialWindow(engine, ctx, WINDOWS);
        });
    }

    @Test
    public void testDecimalWindowOverKeyMajorScan() throws Exception {
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, true);
            ctx.setParallelWindowEnabled(true);
            assertQuery(WINDOWS)
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window workers: 4", "keyShards: sym", "keyMajor: true");
            assertMatchesSerialWindow(engine, ctx, WINDOWS);
        });
    }

    @Test
    public void testGroupByOverDecimalSumWindow() throws Exception {
        // Key k holds v_1..v_n, -x for A and C, +x for B and D. Each v_j is in min(4, n - j + 1)
        // frames, so the sum of the frame sums is 4 * sum(v) - (3 * v_n + 2 * v_n-1 + v_n-2).
        inPool((engine, compiler, ctx) -> {
            createTable(engine, ctx, true);
            ctx.setParallelWindowEnabled(true);
            assertQuery("SELECT sym, sum(s) FROM (SELECT sym, ts, sum(v64)" + W3 + " s FROM t) GROUP BY sym ORDER BY sym")
                    .withEngine(engine)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Window workers: 4", "keyShards: sym")
                    .returns("""
                            sym\tsum
                            A\t-44998800016.0000
                            B\t44997900034.0000
                            C\t-44998200028.0000
                            D\t44998500022.0000
                            """);
        });
    }

    private static void assertAsyncWindow(RecordCursorFactory factory, boolean expected) {
        boolean found = false;
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory) {
                found = true;
                break;
            }
        }
        Assert.assertEquals("Async Window in the factory tree", expected, found);
    }

    // The parallel window returns the serial window's rows, in the same order and bit for bit.
    private static void assertMatchesSerialWindow(CairoEngine engine, SqlExecutionContext ctx, String query) throws SqlException {
        ctx.setParallelWindowEnabled(false);
        try (RecordCursorFactory serial = engine.select(query, ctx)) {
            assertAsyncWindow(serial, false);
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory parallel = engine.select(query, ctx)) {
                assertAsyncWindow(parallel, true);
                assertSameRows(ctx, serial, parallel);
            }
        }
    }

    // Twice, each time through new cursors of the two factories.
    private static void assertSameRows(SqlExecutionContext ctx, RecordCursorFactory expectedFactory, RecordCursorFactory actualFactory) throws SqlException {
        for (int run = 0; run < 2; run++) {
            try (
                    RecordCursor expected = expectedFactory.getCursor(ctx);
                    RecordCursor actual = actualFactory.getCursor(ctx)
            ) {
                TestUtils.assertEqualsExactOrder(expected, expectedFactory.getMetadata(), actual, actualFactory.getMetadata(), false);
            }
        }
    }

    private static void assertSameRows(CairoEngine engine, SqlExecutionContext ctx, String reference, String query) throws SqlException {
        try (
                RecordCursorFactory expectedFactory = engine.select(reference, ctx);
                RecordCursorFactory actualFactory = engine.select(query, ctx)
        ) {
            assertSameRows(ctx, expectedFactory, actualFactory);
        }
    }

    /**
     * Table {@code t}: 300,000 rows one second apart, in four DAY partitions; keys A, B, C and D
     * take turns row by row. {@code v64}, {@code v128} and {@code v256} hold -x for even rows x and +x for odd
     * ones, so keys A and C are negative and B and D positive. {@code d64}, {@code d128} and
     * {@code d256} hold scattered values of both signs, every seventh of them NULL.
     */
    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, boolean isIndexed) throws SqlException {
        engine.execute(
                "CREATE TABLE t (sym SYMBOL" + (isIndexed ? " INDEX" : "") + ", ts TIMESTAMP, x LONG," +
                        " v64 DECIMAL(18,4), v128 DECIMAL(30,4), v256 DECIMAL(50,4)," +
                        " d64 DECIMAL(18,4), d128 DECIMAL(30,4), d256 DECIMAL(50,4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                """
                        INSERT INTO t SELECT
                            CASE WHEN x % 4 = 0 THEN 'A' WHEN x % 4 = 1 THEN 'B' WHEN x % 4 = 2 THEN 'C' ELSE 'D' END,
                            (x * 1_000_000)::TIMESTAMP,
                            x,
                            (CASE WHEN x % 2 = 0 THEN -x ELSE x END)::DECIMAL(18,4),
                            (CASE WHEN x % 2 = 0 THEN -x ELSE x END)::DECIMAL(30,4),
                            (CASE WHEN x % 2 = 0 THEN -x ELSE x END)::DECIMAL(50,4),
                            CASE WHEN x % 7 = 0 THEN NULL ELSE ((x * 7_919) % 2_000_003 - 1_000_000)::DECIMAL(18,4) END,
                            CASE WHEN x % 7 = 1 THEN NULL ELSE ((x * 104_729) % 4_000_037 - 2_000_000)::DECIMAL(30,4) END,
                            CASE WHEN x % 7 = 2 THEN NULL ELSE (CASE WHEN x % 3 = 0 THEN -x ELSE x END * 1_000_000_007)::DECIMAL(50,4) END
                        FROM long_sequence(300_000)
                        """,
                ctx
        );
    }

    private static void inPool(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            executeWithPool(pool, runnable);
        });
    }
}
