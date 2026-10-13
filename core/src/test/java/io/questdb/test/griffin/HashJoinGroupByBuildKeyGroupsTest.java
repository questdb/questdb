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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The fused hash join GROUP BY's per-build-key group cache: when every grouping key reads only build columns
 * (a dimension's name, possibly coalesced), a probe row's group follows from the build row it joined, and the
 * aggregation reuses that group's map value. Each shape is compared with the unfused plan: outer no-match rows,
 * build keys that share a group, a probe filter, NULL probe keys, and keys that also read a probe column (which
 * must keep the per-row path).
 */
public class HashJoinGroupByBuildKeyGroupsTest extends AbstractCairoTest {

    @Test
    public void testCoveringIndexProbe() throws Exception {
        // the probe reads a covering-index scan's frames: the posting index's rows of a symbol, decoded
        assertMemoryLeak(() -> {
            execute("create table q (sym symbol, ex symbol, v double, w long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into q select case when x % 20 = 0 then 'B' when x % 20 = 1 then 'C' else 'A' end, rnd_symbol('N','P','T','Z','X', null), "
                    + "(x % 97)::double, x % 13, timestamp_sequence('2024-01-01', 3_000_000L) from long_sequence(30_000)");
            execute("alter table q alter column sym add index type posting include (ex, v, w)");
            execute("insert into q select case when x % 20 = 0 then 'B' when x % 20 = 1 then 'C' else 'A' end, rnd_symbol('N','P','T'), "
                    + "(x % 89)::double, x % 7, timestamp_sequence('2024-01-05', 3_000_000L) from long_sequence(5_000)");
            execute("create table e (ex symbol, name symbol)");
            execute("insert into e values ('N','nyse'),('P','arca'),('T','arca'),('Z',null)");
            final long batchedBefore = AsyncHashJoinGroupByRecordCursorFactory.getBatchedRowsForTesting();
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = HashJoinGroupByQualificationTest.context(engine, workers)) {
                    context.changePageFrameSizes(1_000, 1_000);
                    for (String join : new String[]{" join ", " left join "}) {
                        for (String where : new String[]{" where q.sym = 'B'", " where q.sym in ('B', 'C')", " where q.sym = 'B' and q.v > 10"}) {
                            // per-key frames (an IN list) merge the workers' partial results in another order:
                            // only aggregates exact in any order there
                            final String inexact = where.contains(" in (") ? "" : ", stddev_pop(q.v), weighted_avg(q.v, q.w)";
                            final String sql = "select coalesce(e.name, '') k, sum(q.v), count(), max(q.v - q.w), min(q.w)"
                                    + inexact + " from q" + join + "e on q.ex = e.ex" + where + " order by 1";
                            final StringSink plan = new StringSink();
                            TestUtils.printSql(engine, context, "explain " + sql, plan);
                            TestUtils.assertContains(sql, plan, "CoveringIndex");
                            if (where.contains(" in (")) {
                                TestUtils.assertContains(sql, plan, "per-key (unordered)");
                            }
                            HashJoinGroupByQualificationTest.assertDifferential(sql, context, true);
                        }
                    }
                }
            }
            // the keys read the build alone and the aggregates the probe: the rows updated in batches
            Assert.assertTrue(AsyncHashJoinGroupByRecordCursorFactory.getBatchedRowsForTesting() > batchedBefore);
        });
    }

    @Test
    public void testDimensionChangesBetweenExecutions() throws Exception {
        // the same factory runs again after the dimension changes: what a probe key resolves to is the
        // execution's build, never the last one's
        assertMemoryLeak(() -> {
            execute("create table q (sym symbol, ex symbol, v double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into q select rnd_symbol('A','B'), rnd_symbol('N','P','T','Z'), (x % 97)::double, "
                    + "timestamp_sequence('2024-01-01', 3_000_000L) from long_sequence(20_000)");
            execute("create table e (ex symbol, name symbol)");
            execute("insert into e values ('N','nyse'),('P','arca'),('T','arca')");
            final String[] sqls = {
                    "select coalesce(e.name, '') k, sum(q.v), count(), max(q.v) from q left join e on q.ex = e.ex order by 1",
                    "select e.name k, sum(q.v), count() from q join e on q.ex = e.ex order by 1",
            };
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = HashJoinGroupByQualificationTest.context(engine, workers)) {
                    context.changePageFrameSizes(1_000, 1_000);
                    for (String sql : sqls) {
                        context.setParallelHashJoinGroupByEnabled(true);
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            HashJoinGroupByQualificationTest.assertAgainstBaseline(sql, factory, context);
                            execute("truncate table e");
                            execute("insert into e values ('Z','iex'),('P','nyse'),('X','cboe')");
                            context.setParallelHashJoinGroupByEnabled(true);
                            HashJoinGroupByQualificationTest.assertAgainstBaseline(sql, factory, context);
                            execute("truncate table e");
                            execute("insert into e values ('N','nyse'),('P','arca'),('T','arca')");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testGroupsFromBuildColumnsOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table q (sym symbol, ex symbol, v double, w long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into q select rnd_symbol('A','B','C'), rnd_symbol('N','P','T','Z','X', null), "
                    + "(x % 97)::double, x % 13, timestamp_sequence('2024-01-01', 3_000_000L) from long_sequence(30_000)");
            // X is missing from the dimension (outer no-match), P and T share a name, Z has a NULL name.
            execute("create table e (ex symbol, name symbol)");
            execute("insert into e values ('N','nyse'),('P','arca'),('T','arca'),('Z',null)");
            final String[] keys = {
                    "coalesce(e.name, '') k",
                    "e.name k",
                    "e.name k, e.ex k2",
                    "coalesce(e.name, '') k, q.sym k2"
            };
            final String[] joins = {" join ", " left join "};
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = HashJoinGroupByQualificationTest.context(engine, workers)) {
                    context.changePageFrameSizes(1_000, 1_000);
                    for (String join : joins) {
                        for (String key : keys) {
                            for (String where : new String[]{"", " where q.sym = 'B'"}) {
                                final String sql = "select " + key + ", sum(q.v), count(), max(q.v), min(q.w), sum(q.v * q.w) "
                                        + "from q" + join + "e on q.ex = e.ex" + where + " order by 1, 2";
                                HashJoinGroupByQualificationTest.assertDifferential(sql, context, true);
                            }
                        }
                    }
                }
            }
        });
    }
}
