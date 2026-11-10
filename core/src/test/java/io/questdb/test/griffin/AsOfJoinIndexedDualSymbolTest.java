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
 * ASOF JOIN ON (sym, ex) with the asof_index hint over an indexed right-hand symbol walks the index
 * rows of that symbol (EXPLAIN: "AsOf Join Indexed Dual Symbol"). Its result must match the linear
 * ASOF JOIN and the default Dense one, with rare, missing and NULL key pairs across partitions.
 */
public class AsOfJoinIndexedDualSymbolTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsOfJoinIndexedDualSymbolTest.class);
    private static final String SELECT = "SELECT t.id, t.sym, t.ex, q.bid, q.ts qts FROM (SELECT * FROM trades WHERE %s) t ASOF JOIN quotes q ON (sym, ex)%s";

    @Test
    public void testMatchesLinearAndDense() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String index : new String[]{"sym SYMBOL INDEX, ex SYMBOL", "sym SYMBOL INDEX TYPE POSTING, ex SYMBOL", "sym SYMBOL, ex SYMBOL INDEX TYPE POSTING"}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE quotes (ts TIMESTAMP, " + index + ", bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
                final long quoteRows = 10_000 + rnd.nextInt(20_000);
                execute(
                        "INSERT INTO quotes SELECT timestamp_sequence('2024-01-01', " + 3L * 24 * 3_600_000_000L / quoteRows + ") ts, " +
                                "CASE WHEN x % 97 = 0 THEN 'R' WHEN x % 53 = 0 THEN NULL ELSE 'S' || (x % 7) END sym, " +
                                "CASE WHEN x % 89 = 0 THEN 'Z' WHEN x % 61 = 0 THEN NULL ELSE 'E' || (x % 4) END ex, " +
                                "rnd_double() bid FROM long_sequence(" + quoteRows + ")"
                );
                execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, ex SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
                final long tradeRows = 500 + rnd.nextInt(2_000);
                execute(
                        "INSERT INTO trades SELECT x id, timestamp_sequence('2024-01-01T00:00:00.5', " + 3L * 24 * 3_600_000_000L / tradeRows + ") ts, " +
                                "CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_symbol('R', 'S0', 'S3', 'S6', 'MISSING') END sym, " +
                                "CASE WHEN x % 13 = 0 THEN NULL ELSE rnd_symbol('E0', 'E1', 'E3', 'Z', 'Y') END ex " +
                                "FROM long_sequence(" + tradeRows + ")"
                );
                for (String where : new String[]{"true", "sym = 'R'", "ex = 'Z' OR sym IS NULL", "id % 7 = 0"}) {
                    for (String tolerance : new String[]{"", " TOLERANCE 1h"}) {
                        final String plain = String.format(SELECT, where, tolerance) + " ORDER BY t.id";
                        final String indexed = plain.replace("SELECT t.id", "SELECT /*+ asof_index(t q) */ t.id");
                        final String linear = plain.replace("SELECT t.id", "SELECT /*+ asof_linear(t q) */ t.id");
                        final StringSink plan = new StringSink();
                        printSql("EXPLAIN " + indexed, plan);
                        Assert.assertTrue(plan.toString(), plan.toString().contains("AsOf Join Indexed Dual Symbol"));
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

    @Test
    public void testNotIndexedKeepsDense() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, ex SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            final StringSink plan = new StringSink();
            printSql("EXPLAIN SELECT /*+ asof_index(t q) */ t.id, q.bid FROM trades t ASOF JOIN quotes q ON (sym, ex)", plan);
            Assert.assertFalse(plan.toString(), plan.toString().contains("Indexed Dual Symbol"));
        });
    }
}
