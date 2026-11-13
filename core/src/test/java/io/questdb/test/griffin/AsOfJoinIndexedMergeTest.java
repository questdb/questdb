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

package io.questdb.test.griffin;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * ASOF JOIN ON (sym) with the asof_index hint over an indexed right-hand symbol moves a forward cursor
 * per key over its index rows (EXPLAIN: "AsOf Join Indexed Merge"). Its result must match the linear
 * ASOF JOIN and the default plan, with rare, dense, missing and NULL keys, duplicate timestamps and
 * small frames, so that keys move across frames and ties straddle them.
 */
public class AsOfJoinIndexedMergeTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsOfJoinIndexedMergeTest.class);
    private static final String SELECT = "SELECT t.id, t.sym, q.bid, q.ts qts FROM (SELECT * FROM trades WHERE %s) t ASOF JOIN quotes q ON (sym)%s";

    @Test
    public void testMatchesLinearAndDense() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String index : new String[]{"sym SYMBOL INDEX, ex SYMBOL", "sym SYMBOL INDEX TYPE POSTING, ex SYMBOL", "sym SYMBOL INDEX TYPE POSTING INCLUDE (bid), ex SYMBOL"}) {
            assertMemoryLeak(() -> {
                // small frames: keys move across frames, ties straddle them
                sqlExecutionContext.changePageFrameSizes(64 + rnd.nextInt(512), 64 + rnd.nextInt(512));
                execute("CREATE TABLE quotes (ts TIMESTAMP, " + index + ", bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
                final long quoteRows = 10_000 + rnd.nextInt(20_000);
                execute(
                        "INSERT INTO quotes SELECT '2024-01-01'::TIMESTAMP + (x / 3) * " + quantum(quoteRows) + "L ts, " +
                                "CASE WHEN x % 97 = 0 THEN 'R' WHEN x % 53 = 0 THEN NULL ELSE 'S' || (x % 7) END sym, " +
                                "CASE WHEN x % 89 = 0 THEN 'Z' WHEN x % 61 = 0 THEN NULL ELSE 'E' || (x % 4) END ex, " +
                                "rnd_double() bid FROM long_sequence(" + quoteRows + ")"
                );
                execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, ex SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
                final long tradeRows = 500 + rnd.nextInt(2_000);
                execute(
                        "INSERT INTO trades SELECT x id, '2024-01-01'::TIMESTAMP + (x * " + (quoteRows / 3) + " / " + tradeRows + ") * " + quantum(quoteRows) + "L ts, " +
                                "CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_symbol('R', 'S0', 'S3', 'S6', 'MISSING') END sym, " +
                                "rnd_symbol('E0', 'E1') ex " +
                                "FROM long_sequence(" + tradeRows + ")"
                );
                for (String where : new String[]{"true", "sym = 'R'", "sym IN ('R', 'S3') OR sym IS NULL", "id % 7 = 0"}) {
                    for (String tolerance : new String[]{"", " TOLERANCE 1h"}) {
                        final String plain = String.format(SELECT, where, tolerance) + " ORDER BY t.id";
                        final String indexed = plain.replace("SELECT t.id", "SELECT /*+ asof_index(t q) */ t.id");
                        final String linear = plain.replace("SELECT t.id", "SELECT /*+ asof_linear(t q) */ t.id");
                        final StringSink plan = new StringSink();
                        printSql("EXPLAIN " + indexed, plan);
                        Assert.assertTrue(plan.toString(), plan.toString().contains("AsOf Join Indexed Merge"));
                        final StringSink expected = new StringSink();
                        printSql(linear, expected);
                        final StringSink actual = new StringSink();
                        printSql(indexed, actual);
                        TestUtils.assertEquals(index + ": " + indexed, expected, actual);
                        printSql(plain, actual);
                        TestUtils.assertEquals(index + ": " + plain, expected, actual);
                    }
                }
                execute("DROP TABLE trades");
                execute("DROP TABLE quotes");
            });
        }
    }

    // Quotes three to a timestamp on this grid over three days; trades on the same grid, so that
    // master timestamps equal quote timestamps
    private static long quantum(long quoteRows) {
        return 9L * 24 * 3_600_000_000L / quoteRows;
    }

    @Test
    public void testNotIndexedKeepsOtherPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            final StringSink plan = new StringSink();
            printSql("EXPLAIN SELECT /*+ asof_index(t q) */ t.id, q.bid FROM trades t ASOF JOIN quotes q ON (sym)", plan);
            Assert.assertFalse(plan.toString(), plan.toString().contains("Indexed Merge"));
        });
    }
}
