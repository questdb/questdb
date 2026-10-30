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

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A parallel GROUP BY on one 4-byte key (a SYMBOL, an INT) finds its map entries by key value in the batch
 * probe's direct table (OrderedMap.probeDirectInt): the groups match those of the same key read as a
 * string (a variable-size key, the map's general probe), whether the key values fall below the direct
 * table's size, above it, are negative or NULL, and with or without a filter.
 */
public class DirectIntKeyGroupByTest extends AbstractCairoTest {

    @Test
    public void testIntKeysInAndBeyondTheDirectTable() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        assertMemoryLeak(() -> {
            // keys from -5 to 40,000: the direct table holds [0, 16384)
            execute("CREATE TABLE t AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 10_000) ts,"
                    + " CASE WHEN x % 97 = 0 THEN NULL ELSE rnd_int(-5, 40_000, 0) END k,"
                    + " rnd_int(0, 1000, 0)::DOUBLE d, rnd_int(1, 100, 0) w"
                    + " FROM long_sequence(200_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            for (String where : new String[]{"", " WHERE d > 300", " WHERE k < 20000"}) {
                assertSame(
                        "SELECT k, count() c, sum(d) s, weighted_avg(d, w) wa FROM t" + where + " ORDER BY k",
                        "SELECT k::INT k, c, s, wa FROM (SELECT k::STRING k, count() c, sum(d) s, weighted_avg(d, w) wa FROM t" + where + ") ORDER BY k"
                );
            }
        });
    }

    @Test
    public void testSymbolKeys() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        assertMemoryLeak(() -> {
            // 20,000 symbols: keys on both sides of the direct table's size, and NULL
            execute("CREATE TABLE s AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 10_000) ts,"
                    + " rnd_symbol(20_000, 2, 6, 5) sym,"
                    + " rnd_int(0, 1000, 0)::DOUBLE d, rnd_int(1, 100, 0) w"
                    + " FROM long_sequence(300_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            for (String where : new String[]{"", " WHERE d > 500", " WHERE sym IN (SELECT sym FROM s LIMIT 50)"}) {
                assertSame(
                        "SELECT sym, count() c, sum(d) sd, weighted_avg(d, w) wa, min(ts) mt FROM s" + where + " ORDER BY sym",
                        "SELECT sym, c, sd, wa, mt FROM (SELECT sym::STRING sym, count() c, sum(d) sd, weighted_avg(d, w) wa, min(ts) mt FROM s" + where + ") ORDER BY sym"
                );
            }
        });
    }

    private static void assertSame(String sql, String reference) throws Exception {
        final StringSink a = new StringSink();
        final StringSink b = new StringSink();
        printSql(sql, a);
        printSql(reference, b);
        TestUtils.assertEquals(b, a);
    }
}
