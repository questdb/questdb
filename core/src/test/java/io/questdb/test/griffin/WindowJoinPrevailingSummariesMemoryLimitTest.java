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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The per-block summaries a WINDOW JOIN INCLUDE PREVAILING shares between its page frames are an
 * optimisation of the backward scan for the prevailing row. They must never make a query fail that
 * the plain scan serves: under a per-query memory limit the summaries are sized by the keys that can
 * join, and when the limit still refuses them the query falls back to the plain scan.
 */
public class WindowJoinPrevailingSummariesMemoryLimitTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        super.setUp();
    }

    @Test
    public void testManyJoinableKeysFallBackToPlainScanUnderLimit() throws Exception {
        // 30k keys quoted once each on day 1 can all join, so the summaries would hold
        // blocks x 30k row ids - 16 MB, more than the limits below allow. The query must still
        // succeed, on the plain backward scan, and return what it returns without a limit.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 10), x FROM long_sequence(250000)", ctx);
                engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L + 1, 'k' || x, x FROM long_sequence(30000)", ctx);
                engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO trades SELECT '2023-12-31'::timestamp + x, 'k' || x, 0.0 FROM long_sequence(30000)", ctx);
                engine.execute("INSERT INTO trades SELECT '2024-01-03'::timestamp + x * 10_000_000L, 'k' || (x % 200 + 1), x FROM long_sequence(2000)", ctx);
                // not 2 MB: the plain scan's own per-worker caches of the 30k keys need more than that
                assertSameUnderLimits(engine, ctx, 4L << 20, 8L << 20, 64L << 20);
            }, configuration, LOG);
        });
    }

    @Test
    public void testManyMasterSymbolsFewJoinableKeysUnderLimit() throws Exception {
        // 300k master symbols that only grow the master's symbol table and 200 rare keys quoted
        // only on day 1, joined from day 3: the summaries need the 200 keys that can join, not
        // the 300k the master's symbol table holds.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 10), x FROM long_sequence(250000)", ctx);
                engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L + 1, 'r' || x, x FROM long_sequence(200)", ctx);
                engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO trades SELECT '2023-12-31'::timestamp + x, 'm' || x, 0.0 FROM long_sequence(300000)", ctx);
                engine.execute("INSERT INTO trades SELECT '2024-01-03'::timestamp + x * 10_000_000L, 'r' || (x % 200 + 1), x FROM long_sequence(2000)", ctx);
                assertSameUnderLimits(engine, ctx, 2L << 20, 4L << 20, 8L << 20, 64L << 20);
            }, configuration, LOG);
        });
    }

    private void assertSameUnderLimits(CairoEngine engine, SqlExecutionContext ctx, long... limits) throws Exception {
        final String sql = "SELECT t.ts, t.sym, last(q.bid) lb, last(q.ts) lt FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym) "
                + "RANGE BETWEEN CURRENT ROW AND CURRENT ROW INCLUDE PREVAILING WHERE t.ts >= '2024-01-03'";
        final StringSink plan = new StringSink();
        TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
        TestUtils.assertContains(plan, "Async Window Fast Join");

        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
        final StringSink expected = new StringSink();
        TestUtils.printSql(engine, ctx, sql, expected);
        final StringSink counts = new StringSink();
        TestUtils.printSql(engine, ctx, "SELECT count(), count(lb) FROM (" + sql + ")", counts);
        // every trade has a prevailing quote two days back: the scan walks out of its own block
        Assert.assertEquals("count\tcount1\n2000\t2000\n", counts.toString());

        final StringSink actual = new StringSink();
        for (long limit : limits) {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
            try {
                TestUtils.printSql(engine, ctx, sql, actual);
            } catch (Throwable th) {
                throw new AssertionError("query failed under limit=" + limit, th);
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
            TestUtils.assertEquals("limit=" + limit, expected, actual);
        }
    }
}
