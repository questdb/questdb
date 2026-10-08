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


package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A default multi-key ASOF JOIN over a master that may be small serves its first master rows by
 * Fast's per-row back-scan and hands over to the Dense forward scan when a master-row or a back-scan
 * budget runs out. The hand-over must not change any row. These tests sweep both budgets, so the
 * hand-over lands on different master rows - also inside a back-scan - over two-symbol, symbol and
 * INT, symbol and STRING, and column-top keys, with and without TOLERANCE, and compare every result
 * with the Dense and the linear algorithms.
 */
public class AsOfJoinPreludeHandoverTest extends AbstractCairoTest {
    private static final String[] KEYS = {"sym, ex", "ex, sym", "sym, ex, k", "sym, s", "sym, ex2", "sym, ex, ex2"};
    private static final String[] MASTERS = {
            "(SELECT * FROM trades WHERE px > 1)",
            "(SELECT * FROM trades WHERE px > 9.5)",
            "(SELECT * FROM trades WHERE ts IN '2024-01-02')",
            "trades"
    };

    @Test
    public void testHandoverAtAnyRowKeepsRows() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int compared = 0;
            int withPrelude = 0;
            for (int round = 0; round < 2; round++) {
                sqlExecutionContext.setRandom(new Rnd(rnd.nextLong(), rnd.nextLong()));
                createTables(rnd, rnd.nextBoolean());
                for (int bp : new int[]{1, 2, 5, 40, 10000}) {
                    for (int pct : new int[]{1, 3, 25, 100}) {
                        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_MASTER_BP, bp);
                        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_BACKSCAN_PCT, pct);
                        for (String keys : KEYS) {
                            for (String master : MASTERS) {
                                for (String tolerance : new String[]{"", " TOLERANCE 20m"}) {
                                    final String sql = "SELECT t.ts, t.sym, t.ex, t.k, q.bid, q.ts qts FROM " + master + " t ASOF JOIN quotes q ON (" + keys + ")" + tolerance;
                                    final StringSink plan = new StringSink();
                                    printSql("EXPLAIN " + sql, plan);
                                    if (plan.toString().contains("prelude")) {
                                        withPrelude++;
                                    }
                                    final StringSink linear = new StringSink();
                                    printSql(sql.replaceFirst("SELECT ", "SELECT /*+ asof_linear(t q) */ "), linear);
                                    final String expected = linear.toString();
                                    // an unfiltered master knows its size, and so does the join
                                    final boolean isSized = "trades".equals(master);
                                    final StringSink dense = new StringSink();
                                    printSql(sql.replaceFirst("SELECT ", "SELECT /*+ asof_dense(t q) */ "), dense);
                                    TestUtils.assertEquals("dense vs linear\n" + sql, expected, dense);
                                    try {
                                        if (keys.equals(KEYS[0])) {
                                            // the whole cursor contract, on a share of the queries
                                            assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().expectSize(isSized).returns(expected);
                                        } else {
                                            final StringSink auto = new StringSink();
                                            printSql(sql, auto);
                                            TestUtils.assertEquals(expected, auto);
                                        }
                                    } catch (AssertionError e) {
                                        throw new AssertionError("bp=" + bp + " pct=" + pct + "\n" + sql + "\n" + plan, e);
                                    }
                                    compared++;
                                }
                            }
                        }
                    }
                }
            }
            // the prelude ran on a good share of the comparisons, not on none
            Assert.assertTrue("prelude on " + withPrelude + " of " + compared, withPrelude > compared / 4);
        });
    }

    private void createTables(Rnd rnd, boolean parquetSlave) throws Exception {
        execute("DROP TABLE IF EXISTS quotes");
        execute("DROP TABLE IF EXISTS trades");
        final int k = 4 + rnd.nextInt(40);
        final int dup = 1 + rnd.nextInt(3);
        execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, k INT, s STRING, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        // skewed keys, NULLs in every key column, several rows per timestamp
        execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + (x / " + dup + ") * 5_000_000L, "
                + "CASE WHEN rnd_int(0, 40, 0) = 0 THEN NULL ELSE 'k' || ((rnd_int(0, " + (k - 1) + ", 0) * rnd_int(0, " + (k - 1) + ", 0)) / " + k + ") END, "
                + "CASE WHEN rnd_int(0, 25, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 2, 0) END, "
                + "CASE WHEN rnd_int(0, 30, 0) = 0 THEN NULL ELSE rnd_int(0, 2, 0) END, "
                + "CASE WHEN rnd_int(0, 30, 0) = 0 THEN NULL ELSE 'S' || rnd_int(0, 2, 0) END, "
                + "rnd_double() FROM long_sequence(" + (20000 + rnd.nextInt(20000)) + ")");
        // rare keys, quoted only early on day 1: long back-scans, across partitions
        execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 30_000_000L + 1, 'r' || (x % 40), 'X' || (x % 3), (x % 2)::int, 'S0', rnd_double() FROM long_sequence(120)");
        // column top
        execute("ALTER TABLE quotes ADD COLUMN ex2 SYMBOL");
        execute("INSERT INTO quotes SELECT '2024-01-04'::timestamp + x * 3_000_000L, 'k' || rnd_int(0, " + (k - 1) + ", 0), 'X' || rnd_int(0, 2, 0), "
                + "rnd_int(0, 2, 0), 'S' || rnd_int(0, 2, 0), rnd_double(), CASE WHEN rnd_int(0, 9, 0) = 0 THEN NULL ELSE 'Y' || rnd_int(0, 3, 0) END FROM long_sequence(10000)");
        // same-key ties
        execute("INSERT INTO quotes SELECT ts, sym, ex, k, s, -bid, ex2 FROM quotes WHERE bid < 0.003");
        if (parquetSlave) {
            execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        }
        // trades: k* (some never quoted), r*, z* (never quoted), NULLs; pairs that never occur together
        execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, k INT, s STRING, px DOUBLE, ex2 SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 50_000_000L + rnd_int(0, 3, 0), "
                + "CASE WHEN rnd_int(0, 30, 0) = 0 THEN NULL WHEN rnd_int(0, 5, 0) = 0 THEN 'r' || rnd_int(0, 45, 0) "
                + "WHEN rnd_int(0, 11, 0) = 0 THEN 'z' || rnd_int(0, 3, 0) ELSE 'k' || rnd_int(0, " + (k + 3) + ", 0) END, "
                + "CASE WHEN rnd_int(0, 20, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 3, 0) END, "
                + "CASE WHEN rnd_int(0, 20, 0) = 0 THEN NULL ELSE rnd_int(0, 2, 0) END, "
                + "CASE WHEN rnd_int(0, 20, 0) = 0 THEN NULL ELSE 'S' || rnd_int(0, 3, 0) END, "
                + "rnd_double() * 10, CASE WHEN rnd_int(0, 6, 0) = 0 THEN NULL ELSE 'Y' || rnd_int(0, 4, 0) END FROM long_sequence(" + (3000 + rnd.nextInt(3000)) + ")");
        // trades exactly at quote timestamps
        execute("INSERT INTO trades SELECT ts, sym, ex, k, s, 9.9, ex2 FROM quotes WHERE bid > 0.9995");
    }
}
