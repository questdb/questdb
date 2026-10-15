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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A parallel GROUP BY whose percentiles hold many values merges as shards, however few its groups, and
 * computes the percentiles as it merges each shard (GroupByFunction.finalizeValue): the results match a
 * serial GROUP BY's, for few groups and for groups enough to shard as they aggregate, with other
 * aggregates beside, and an invalid percentile is still reported.
 */
public class PercentileShardFinalizeTest extends AbstractCairoTest {

    @Test
    public void testInvalidPercentileIsReported() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1000);
        assertMemoryLeak(() -> {
            createTable();
            assertException("SELECT g, percentile_cont(v, 1.5) FROM t", 29, "invalid percentile");
        });
    }

    @Test
    public void testResultsMatchTheSerialGroupBy() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // 10 x 8 bytes x 1000: past 80,000 bytes of values the GROUP BY merges as shards
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1000);
        assertMemoryLeak(() -> {
            createTable();
            final String serial = "(SELECT * FROM t UNION ALL SELECT * FROM t WHERE 1 = 0)";
            // g: five groups, sharded after aggregation; k: 3,000 groups, sharded as they aggregate
            for (String key : new String[]{"g", "k"}) {
                for (String aggs : new String[]{
                        "percentile_cont(v, 0.5) pc",
                        "percentile_disc(v, 0.9) pd, count() c, sum(w) s, percentile_cont(w, 0.25) pw",
                        "percentile_cont(w, 0.5) m, percentile_disc(v, 0.0) p0, percentile_cont(v, 1.0) p1"
                }) {
                    final String sql = "SELECT " + key + ", " + aggs + " FROM %s ORDER BY " + key;
                    // twice: the second run reuses the shards
                    for (int run = 0; run < 2; run++) {
                        assertSame(String.format(sql, "t"), String.format(sql, serial));
                    }
                }
            }
        });
    }

    private static void assertSame(String sql, String reference) throws Exception {
        final StringSink a = new StringSink();
        final StringSink b = new StringSink();
        printSql(reference, b);
        printSql(sql, a);
        TestUtils.assertEquals(b, a);
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t AS (SELECT"
                + " timestamp_sequence('2024-01-01', 20_000) ts,"
                + " rnd_symbol('a', 'b', 'c', 'd', 'e') g,"
                + " rnd_int(0, 3000, 0) k,"
                + " rnd_int(0, 400, 0)::DOUBLE / 4 v,"
                + " CASE WHEN x % 50 = 0 THEN NULL ELSE rnd_double() * 1000 - 500 END w"
                + " FROM long_sequence(300_000)) TIMESTAMP(ts) PARTITION BY HOUR");
    }
}
