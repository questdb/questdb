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

package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PercentRankFunctionTest extends AbstractCairoTest {

    @Test
    public void testPercentRankNoOverClause() throws Exception {
        assertException(
                "select ts, percent_rank() from tab",
                "create table tab (ts timestamp, i long) timestamp(ts)",
                11,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testPercentRankIgnoreNullsNotSupported() throws Exception {
        assertException(
                "select ts, percent_rank() ignore nulls over (order by ts) from tab",
                "create table tab (ts timestamp, i long) timestamp(ts)",
                26,
                "RESPECT/IGNORE NULLS is not supported for current window function"
        );
    }

    @Test
    public void testPercentRankNoOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, s symbol) timestamp(ts)");
            execute("insert into tab select (x/4)::timestamp, x/2, 'k' || (x%2) ::symbol from long_sequence(12)");

            // percent_rank() over (partition by) - no order by, all rows are peers with rank 1
            // percent_rank = (1-1)/(n-1) = 0 for all rows
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000003Z\t0.0
                            """,
                    "select ts, " +
                            "percent_rank() over (partition by s) " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );

            // percent_rank() over () - no order by, all rows have percent_rank = 0
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000003Z\t0.0
                            """,
                    "select ts, " +
                            "percent_rank() over () " +
                            "from tab ",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testPercentRankWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, s symbol) timestamp(ts)");
            execute("insert into tab select (x/4)::timestamp, x/2, 'k' || (x%2) ::symbol from long_sequence(12)");

            // percent_rank() over (order by xxx) - 12 total rows
            // rank 1 at ts=0 (3 rows): (1-1)/(12-1) = 0/11 = 0.0
            // rank 4 at ts=1 (4 rows): (4-1)/(12-1) = 3/11 = 0.2727272727272727
            // rank 8 at ts=2 (4 rows): (8-1)/(12-1) = 7/11 = 0.6363636363636364
            // rank 12 at ts=3 (1 row): (12-1)/(12-1) = 11/11 = 1.0
            // TWO_PASS function uses CachedWindowRecordCursorFactory which supports random access
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.2727272727272727
                            1970-01-01T00:00:00.000002Z\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.6363636363636364
                            1970-01-01T00:00:00.000003Z\t1.0
                            """,
                    "select ts," +
                            "percent_rank() over (order by ts) " +
                            "from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankWithPartitionByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, s symbol) timestamp(ts)");
            execute("insert into tab select (x/4)::timestamp, x/2, 'k' || (x%2) ::symbol from long_sequence(12)");

            // percent_rank() over (partition by xxx order by xxx)
            // k0: 6 rows - ts=0(1 row, rank=1), ts=1(2 rows, rank=2), ts=2(2 rows, rank=4), ts=3(1 row, rank=6)
            // k1: 6 rows - ts=0(2 rows, rank=1), ts=1(2 rows, rank=3), ts=2(2 rows, rank=5)
            // percent_rank = (rank - 1) / (total_rows - 1)
            // TWO_PASS function uses CachedWindowRecordCursorFactory which supports random access
            assertQueryNoLeakCheck(
                    """
                            ts\ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\tk0\t0.0
                            1970-01-01T00:00:00.000001Z\tk0\t0.2
                            1970-01-01T00:00:00.000001Z\tk0\t0.2
                            1970-01-01T00:00:00.000002Z\tk0\t0.6
                            1970-01-01T00:00:00.000002Z\tk0\t0.6
                            1970-01-01T00:00:00.000003Z\tk0\t1.0
                            1970-01-01T00:00:00.000000Z\tk1\t0.0
                            1970-01-01T00:00:00.000000Z\tk1\t0.0
                            1970-01-01T00:00:00.000001Z\tk1\t0.4
                            1970-01-01T00:00:00.000001Z\tk1\t0.4
                            1970-01-01T00:00:00.000002Z\tk1\t0.8
                            1970-01-01T00:00:00.000002Z\tk1\t0.8
                            """,
                    "select ts, s," +
                            "percent_rank() over (partition by s order by ts) " +
                            "from tab order by s, ts",
                    "",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            // Test single row partition - should return 0
            // TWO_PASS function uses CachedWindowRecordCursorFactory which supports random access
            execute("create table single_row (ts timestamp, v int) timestamp(ts)");
            execute("insert into single_row values (0, 1)");
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            """,
                    "select ts, percent_rank() over (order by ts) from single_row",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankSingleRowPartition() throws Exception {
        assertMemoryLeak(() -> {
            // Test single row partition with partition by - should return 0
            // This tests the totalRows <= 1 branch in PercentRankOverPartitionFunction.pass2
            execute("create table mixed_partitions (ts timestamp, s symbol) timestamp(ts)");
            execute("insert into mixed_partitions values (0, 'a'), (1, 'b'), (2, 'b')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\ta\t0.0
                            1970-01-01T00:00:00.000001Z\tb\t0.0
                            1970-01-01T00:00:00.000002Z\tb\t1.0
                            """,
                    "select ts, s, percent_rank() over (partition by s order by ts) from mixed_partitions order by s, ts",
                    "",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankExplainPlan() throws Exception {
        // Test toPlan() method for percent_rank functions
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, s symbol) timestamp(ts)");

            // Test plan for percent_rank() over () - no partition, no order
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [percent_rank() over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, percent_rank() over () from tab"
            );

            // Test plan for percent_rank() over (partition by s) - with partition, no order
            assertSql(
                    """
                            QUERY PLAN
                            Window
                              functions: [percent_rank() over (partition by [s])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, percent_rank() over (partition by s) from tab"
            );

            // Test plan for percent_rank() over (order by ts) - no partition, with order
            assertSql(
                    """
                            QUERY PLAN
                            CachedWindow
                              unorderedFunctions: [percent_rank() over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, percent_rank() over (order by ts) from tab"
            );

            // Test plan for percent_rank() over (partition by s order by ts) - with partition and order
            assertSql(
                    """
                            QUERY PLAN
                            CachedWindow
                              unorderedFunctions: [percent_rank() over (partition by [s])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """,
                    "explain select ts, percent_rank() over (partition by s order by ts) from tab"
            );
        });
    }

    @Test
    public void testPercentRankNoOrderByCachedFactory() throws Exception {
        // This test exercises pass1() in PercentRankNoOrderFunction by combining it
        // with a TWO_PASS window function (percent_rank with order by).
        // When any function requires TWO_PASS, all functions go through CachedWindowRecordCursorFactory
        // which calls pass1() on all functions including the ZERO_PASS ones.
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, s symbol) timestamp(ts)");
            execute("insert into tab select (x/4)::timestamp, x/2, 'k' || (x%2) ::symbol from long_sequence(12)");

            // percent_rank() over () returns 0 for all rows (ZERO_PASS, no ORDER BY means all peers)
            // percent_rank() over (order by ts) is TWO_PASS and computes actual percent ranks
            assertQueryNoLeakCheck(
                    """
                            ts\tno_order\twith_order
                            1970-01-01T00:00:00.000000Z\t0.0\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0\t0.0
                            1970-01-01T00:00:00.000000Z\t0.0\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.0\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.0\t0.2727272727272727
                            1970-01-01T00:00:00.000001Z\t0.0\t0.2727272727272727
                            1970-01-01T00:00:00.000002Z\t0.0\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.0\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.0\t0.6363636363636364
                            1970-01-01T00:00:00.000002Z\t0.0\t0.6363636363636364
                            1970-01-01T00:00:00.000003Z\t0.0\t1.0
                            """,
                    "select ts, " +
                            "percent_rank() over () as no_order, " +
                            "percent_rank() over (order by ts) as with_order " +
                            "from tab",
                    "ts",
                    true,
                    true
            );
        });
    }
}
