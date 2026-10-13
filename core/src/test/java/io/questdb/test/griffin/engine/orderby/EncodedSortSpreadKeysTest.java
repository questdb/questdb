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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Encoded sorts past the parallel threshold rewrite a one-word key into a word that spreads over
 * the radix sort's first byte (SortKeyEncoder.spreadKeyWords). The order must be the one the
 * sort gives without it: each query is run with the threshold above its row count (no rewrite,
 * the native sort's sequential path) and at 2 (rewrite, parallel path), light and materialized,
 * with row ids ascending (ordinals) and descending (shift only), narrow and full key ranges,
 * duplicates, NULLs, and a single distinct key.
 */
public class EncodedSortSpreadKeysTest extends AbstractCairoTest {
    private static final String[] KEYS = {
            "f", "f desc", "i", "i desc", "l", "l desc", "d", "d desc", "s", "s desc", "b", "b desc",
            "ts", "ts desc", "wide", "wide desc", "c", "c desc", "fn", "fn desc", "sym", "sym desc", "fo", "fo desc", "io", "io desc"
    };

    @Test
    public void testSpreadKeysKeepTheOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t as (select " +
                            "(200 + rnd_int(0, 5000, 0) / 100.0)::float f, " +
                            "rnd_int(1000, 1100, 0) i, " +
                            "rnd_long(-5, 5, 0) l, " +
                            "rnd_double(0) d, " +
                            "rnd_short(10, 20) s, " +
                            "rnd_boolean() b, " +
                            "rnd_long(-9223372036854775807L, 9223372036854775807L, 0) wide, " +
                            "7 c, " +
                            "rnd_float(3) fn, " +
                            "rnd_symbol('a', 'b', 'c', null) sym, " +
                            // a narrow bulk with outliers on both sides of it
                            "case when x % 97 = 0 then 0.0 when x % 89 = 0 then 70 + x % 5 when x % 83 = 0 then 1e6 " +
                            "else 130 + rnd_int(0, 1500, 0) / 100.0 end::float fo, " +
                            "case when x % 101 = 0 then -2_000_000_000 when x % 103 = 0 then 2_000_000_000 " +
                            "else rnd_int(0, 300, 0) end io, " +
                            "timestamp_sequence('2026-04-01', 1_000_000L) ts " +
                            "from long_sequence(30000)) timestamp(ts) partition by hour"
            );
            int sorted = 0;
            for (String key : KEYS) {
                final String col = key.split(" ")[0];
                final String[] queries = {
                        // light, row ids ascending
                        "select ts, " + col + " from t where i > 1003 order by " + key,
                        // light over a descending scan: row ids descending
                        "select ts, " + col + " from (select * from t order by ts desc) order by " + key,
                        // materialized (a union is not random access)
                        "select ts, " + col + " from (select ts, " + col + " from t where i > 1003 union all " +
                                "select ts, " + col + " from t where i < 1010) order by " + key,
                };
                for (String query : queries) {
                    setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, 1_000_000);
                    final String expected = rows(query);
                    setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, 2);
                    final StringSink plan = new StringSink();
                    printSql("explain " + query, plan);
                    if (Chars.contains(plan, "Encode sort")) {
                        sorted++;
                    }
                    TestUtils.assertEquals(query, expected, rows(query));
                }
            }
            Assert.assertTrue("no encoded sort", sorted > KEYS.length);
        });
    }

    private static String rows(String query) throws Exception {
        final StringSink sink = new StringSink();
        printSql(query, sink);
        return sink.toString();
    }
}
