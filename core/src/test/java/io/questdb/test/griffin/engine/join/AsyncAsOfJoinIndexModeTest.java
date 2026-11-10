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

package io.questdb.test.griffin.engine.join;

import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Async AsOf Join in its index mode (asof_index over an indexed slave symbol, EXPLAIN "lookup: index")
 * on worker threads, each reading the index from a table reader of its own: its result must match
 * the linear ASOF JOIN, with rare, dense, missing and NULL keys, ties between master and slave
 * timestamps, several slave frames, a filtered and projected slave, and a tolerance.
 */
public class AsyncAsOfJoinIndexModeTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncAsOfJoinIndexModeTest.class);

    @Test
    public void testIndexModeMatchesLinear() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String index : new String[]{"sym SYMBOL INDEX", "sym SYMBOL INDEX TYPE POSTING", "sym SYMBOL INDEX TYPE POSTING INCLUDE (bid)"}) {
            assertMemoryLeak(() -> {
                final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
                try (SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(engine, pool.getWorkerCount())) {
                    TestUtils.setupWorkerPool(pool, engine);
                    pool.start(LOG);
                    try {
                        final long quoteRows = 20_000 + rnd.nextInt(20_000);
                        // quotes three to a timestamp over four days, trades on the same grid: ties
                        final long quantum = 12L * 24 * 3_600_000_000L / quoteRows;
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, " + index + ", bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute(
                                "INSERT INTO quotes SELECT '2024-01-01'::TIMESTAMP + (x / 3) * " + quantum + "L ts, " +
                                        "CASE WHEN x % 997 = 0 THEN 'R' WHEN x % 53 = 0 THEN NULL ELSE 'S' || (x % 7) END sym, " +
                                        "rnd_double() bid FROM long_sequence(" + quoteRows + ")",
                                ctx
                        );
                        final long tradeRows = 2_000 + rnd.nextInt(8_000);
                        engine.execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute(
                                "INSERT INTO trades SELECT x id, '2024-01-01'::TIMESTAMP + (x * " + (quoteRows / 3) + " / " + tradeRows + ") * " + quantum + "L ts, " +
                                        "CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_symbol('R', 'S0', 'S3', 'S6', 'MISSING') END sym " +
                                        "FROM long_sequence(" + tradeRows + ")",
                                ctx
                        );
                        final int[] indexModeJoins = {0};
                        final String[] slaves = {
                                "quotes",
                                "(SELECT ts, sym, bid FROM quotes WHERE sym IN ('R', 'S3'))"
                        };
                        for (String slave : slaves) {
                            for (String where : new String[]{"true", "sym = 'R'", "sym IN ('R', 'S3') OR sym IS NULL"}) {
                                for (String tolerance : new String[]{"", " TOLERANCE 1h"}) {
                                    final String join = " t.id, t.sym, q.bid, q.ts qts FROM (SELECT * FROM trades WHERE " + where + ") t ASOF JOIN " + slave + " q ON (sym)" + tolerance + " ORDER BY t.id";
                                    final String indexed = "SELECT /*+ asof_index(t q) */" + join;
                                    final StringSink plan = new StringSink();
                                    TestUtils.printSql(engine, ctx, "EXPLAIN " + indexed, plan);
                                    // a master or slave whose filter the parallel way cannot steal (e.g. an index
                                    // scan of the slave's IN list) joins serially: its result must match too
                                    if (plan.toString().contains("lookup: index")) {
                                        indexModeJoins[0]++;
                                    }
                                    final StringSink expected = new StringSink();
                                    TestUtils.printSql(engine, ctx, "SELECT /*+ asof_linear(t q) */" + join, expected);
                                    final StringSink actual = new StringSink();
                                    TestUtils.printSql(engine, ctx, indexed, actual);
                                    TestUtils.assertEquals(index + ": " + indexed, expected, actual);
                                }
                            }
                        }
                        Assert.assertTrue("no join ran in the index mode", indexModeJoins[0] > 0);
                        engine.execute("DROP TABLE trades", ctx);
                        engine.execute("DROP TABLE quotes", ctx);
                    } finally {
                        pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                    }
                }
            });
        }
    }
}
