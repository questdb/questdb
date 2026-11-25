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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A GROUP BY over an ASOF JOIN without keys to a small table reads the master's page frames in
 * parallel, the slave's columns being lookups by the master row's timestamp (AsOfLookup). Each
 * query is compared with the same join under a tolerance no row reaches, which keeps the join.
 */
public class AsOfLookupGroupByTest extends AbstractCairoTest {

    @Test
    public void testGroupByOverAsOfLookup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, ex symbol, size long, px double) timestamp(ts) partition by day");
            execute("insert into t select (x * 10_000_000L)::timestamp, rnd_symbol('N','P','T',null), x % 100, x / 3.0 "
                    + "from long_sequence(50_000)");
            // nanosecond buckets over microsecond trades; the first starts after the first trades, two share a
            // timestamp (the later row wins), one bucket has a NULL name
            execute("create table b (bucket symbol, bound timestamp_ns, rowid long, w double, n int, d date, tt timestamp) timestamp(bound)");
            execute("insert into b values "
                    + "('pre', '1970-01-01T00:10:00.000000000Z', 0, 0.5, 1, '1970-01-01', '1970-01-02'), "
                    + "('open', '1970-01-01T09:00:00.000000000Z', 1, 1.5, 2, '1970-01-02', '1970-01-03'), "
                    + "('open2', '1970-01-01T09:00:00.000000000Z', 2, 2.5, null, null, null), "
                    + "(null, '1970-01-02T00:00:00.000000001Z', 3, null, 4, '1970-01-04', '1970-01-05'), "
                    + "('late', '1970-01-04T00:00:00.000000000Z', 4, 4.5, 5, '1970-01-05', '1970-01-06')");
            final String[] queries = {
                    // trades against a small quote table
                    "select ex, timeBucket, cnt, size from (select t.ex as ex, b.rowid as brow, first(b.bucket) as timeBucket, "
                            + "count() as cnt, sum(t.size::double) as size from t asof join b group by t.ex, b.rowid) order by ex, brow",
                    "select b.bucket, count(), sum(t.px), min(t.ts), max(b.w) from t asof join b group by b.bucket order by 1",
                    "select b.n, b.d, b.tt, count() from t asof join b group by b.n, b.d, b.tt order by 1, 2, 3",
                    "select count(), sum(b.w), avg(t.px) from t asof join b",
                    "select b.rowid, count() from t asof join b where t.size > 50 group by b.rowid order by 1",
            };
            for (String query : queries) {
                final StringSink plan = new StringSink();
                printSql("explain " + query, plan);
                TestUtils.assertContains(query, plan, "asof(b.");
                TestUtils.assertNotContains(plan, "AsOf Join");
                TestUtils.assertContains(query, plan, "Group By workers");
                final String reference = query.replace("asof join b", "asof join b tolerance 100000d");
                final StringSink referencePlan = new StringSink();
                printSql("explain " + reference, referencePlan);
                TestUtils.assertContains(reference, referencePlan, "AsOf Join");
                TestUtils.assertEquals(query, printSqlToString(reference), printSqlToString(query));
            }
        });
    }

    private String printSqlToString(String sql) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        return sink.toString();
    }
}
