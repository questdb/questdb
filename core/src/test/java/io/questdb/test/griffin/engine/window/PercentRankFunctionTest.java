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
                              unorderedFunctions: [percent_rank() over (order by [ts])]
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
                              unorderedFunctions: [percent_rank() over (partition by [s] order by [ts])]
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

    @Test
    public void testPercentRankEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table empty_tab (ts timestamp, v int) timestamp(ts)");
            assertQueryNoLeakCheck(
                    "ts\tpercent_rank\n",
                    "select ts, percent_rank() over (order by ts) from empty_tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, v int) timestamp(ts)");
            execute("insert into tab values (0, 1), (1, 2), (2, 3)");
            // With DESC order: ts=2 has rank 1, ts=1 has rank 2, ts=0 has rank 3
            // percent_rank: (1-1)/2=0.0, (2-1)/2=0.5, (3-1)/2=1.0
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t1.0
                            1970-01-01T00:00:00.000001Z\t0.5
                            1970-01-01T00:00:00.000002Z\t0.0
                            """,
                    "select ts, percent_rank() over (order by ts desc) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankMultipleOrderByColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, a int, b int) timestamp(ts)");
            execute("insert into tab values (0, 1, 1), (1, 1, 2), (2, 2, 1), (3, 2, 2)");
            // Ordered by a, b: (1,1), (1,2), (2,1), (2,2)
            // All distinct values, so ranks are 1, 2, 3, 4
            // percent_rank: 0/3=0.0, 1/3=0.333..., 2/3=0.666..., 3/3=1.0
            assertQueryNoLeakCheck(
                    """
                            ts\ta\tb\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t1\t1\t0.0
                            1970-01-01T00:00:00.000001Z\t1\t2\t0.3333333333333333
                            1970-01-01T00:00:00.000002Z\t2\t1\t0.6666666666666666
                            1970-01-01T00:00:00.000003Z\t2\t2\t1.0
                            """,
                    "select ts, a, b, percent_rank() over (order by a, b) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankRespectNullsNotSupported() throws Exception {
        assertException(
                "select ts, percent_rank() respect nulls over (order by ts) from tab",
                "create table tab (ts timestamp, i long) timestamp(ts)",
                26,
                "RESPECT/IGNORE NULLS is not supported for current window function"
        );
    }

    @Test
    public void testPercentRankFramingNotSupported() throws Exception {
        assertException(
                "select ts, percent_rank() over (order by ts rows between unbounded preceding and current row) from tab",
                "create table tab (ts timestamp, i long) timestamp(ts)",
                11,
                "percent_rank() does not support framing; remove ROWS/RANGE clause"
        );
    }

    @Test
    public void testPercentRankWithNullsInOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, v int) timestamp(ts)");
            execute("insert into tab values (0, null), (1, 1), (2, null), (3, 2)");
            // NULLs come FIRST in ASC order (QuestDB default is NULLS FIRST)
            // Order by v: null, null, 1, 2
            // Ranks: 1, 1, 3, 4 (nulls are peers with rank 1)
            // percent_rank: 0/3=0.0, 0/3=0.0, 2/3=0.666..., 3/3=1.0
            assertQueryNoLeakCheck(
                    """
                            ts\tv\tpercent_rank
                            1970-01-01T00:00:00.000000Z\tnull\t0.0
                            1970-01-01T00:00:00.000001Z\t1\t0.6666666666666666
                            1970-01-01T00:00:00.000002Z\tnull\t0.0
                            1970-01-01T00:00:00.000003Z\t2\t1.0
                            """,
                    "select ts, v, percent_rank() over (order by v) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankWithNullsInPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, s symbol) timestamp(ts)");
            execute("insert into tab values (0, null), (1, 'a'), (2, null), (3, 'a'), (4, null)");
            // Partition null: 3 rows at ts=0,2,4
            // Partition 'a': 2 rows at ts=1,3
            assertQueryNoLeakCheck(
                    """
                            ts\ts\tpercent_rank
                            1970-01-01T00:00:00.000001Z\ta\t0.0
                            1970-01-01T00:00:00.000003Z\ta\t1.0
                            1970-01-01T00:00:00.000000Z\t\t0.0
                            1970-01-01T00:00:00.000002Z\t\t0.5
                            1970-01-01T00:00:00.000004Z\t\t1.0
                            """,
                    "select ts, s, percent_rank() over (partition by s order by ts) from tab order by s desc, ts",
                    "",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankAllTies() throws Exception {
        assertMemoryLeak(() -> {
            // All rows have the same ORDER BY value - all are rank 1
            execute("create table tab (ts timestamp, v int) timestamp(ts)");
            execute("insert into tab values (0, 1), (1, 1), (2, 1), (3, 1)");
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000002Z\t0.0
                            1970-01-01T00:00:00.000003Z\t0.0
                            """,
                    "select ts, percent_rank() over (order by v) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankTwoRows() throws Exception {
        assertMemoryLeak(() -> {
            // Two distinct rows: ranks 1 and 2
            // percent_rank: (1-1)/(2-1)=0.0, (2-1)/(2-1)=1.0
            execute("create table tab (ts timestamp, v int) timestamp(ts)");
            execute("insert into tab values (0, 1), (1, 2)");
            assertQueryNoLeakCheck(
                    """
                            ts\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t0.0
                            1970-01-01T00:00:00.000001Z\t1.0
                            """,
                    "select ts, percent_rank() over (order by v) from tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 1000 rows to ensure no performance/memory issues
            execute("create table tab (ts timestamp, v long) timestamp(ts)");
            execute("insert into tab select x::timestamp, x from long_sequence(1000)");
            // All distinct values, so rank = row position
            // First row: (1-1)/(1000-1) = 0.0
            // Last row: (1000-1)/(1000-1) = 1.0
            // Row 501: (501-1)/(1000-1) = 500/999 = 0.5005005005...
            assertSql(
                    """
                            min\tmax
                            0.0\t1.0
                            """,
                    "select min(pr), max(pr) from (select percent_rank() over (order by v) as pr from tab)"
            );
        });
    }

    @Test
    public void testPercentRankWithMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            // Test multiple partitions with different sizes
            execute("create table tab (ts timestamp, s symbol, v int) timestamp(ts)");
            execute("insert into tab values " +
                    "(0, 'a', 1), " +  // partition 'a' - 1 row
                    "(1, 'b', 1), (2, 'b', 2), " +  // partition 'b' - 2 rows
                    "(3, 'c', 1), (4, 'c', 2), (5, 'c', 3)");  // partition 'c' - 3 rows
            assertQueryNoLeakCheck(
                    """
                            ts\ts\tv\tpercent_rank
                            1970-01-01T00:00:00.000000Z\ta\t1\t0.0
                            1970-01-01T00:00:00.000001Z\tb\t1\t0.0
                            1970-01-01T00:00:00.000002Z\tb\t2\t1.0
                            1970-01-01T00:00:00.000003Z\tc\t1\t0.0
                            1970-01-01T00:00:00.000004Z\tc\t2\t0.5
                            1970-01-01T00:00:00.000005Z\tc\t3\t1.0
                            """,
                    "select ts, s, v, percent_rank() over (partition by s order by v) from tab order by s, v",
                    "",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPercentRankCombinedWithOtherRankingFunctions() throws Exception {
        assertMemoryLeak(() -> {
            // Test percent_rank() combined with rank() and dense_rank() in the same query
            execute("create table tab (ts timestamp, v int) timestamp(ts)");
            execute("insert into tab values (0, 1), (1, 1), (2, 2), (3, 3), (4, 3)");
            // Values: 1, 1, 2, 3, 3 (5 rows)
            // rank():        1, 1, 3, 4, 4
            // dense_rank():  1, 1, 2, 3, 3
            // percent_rank(): (1-1)/4=0.0, (1-1)/4=0.0, (3-1)/4=0.5, (4-1)/4=0.75, (4-1)/4=0.75
            assertQueryNoLeakCheck(
                    """
                            ts\tv\trank\tdense_rank\tpercent_rank
                            1970-01-01T00:00:00.000000Z\t1\t1\t1\t0.0
                            1970-01-01T00:00:00.000001Z\t1\t1\t1\t0.0
                            1970-01-01T00:00:00.000002Z\t2\t3\t2\t0.5
                            1970-01-01T00:00:00.000003Z\t3\t4\t3\t0.75
                            1970-01-01T00:00:00.000004Z\t3\t4\t3\t0.75
                            """,
                    "select ts, v, " +
                            "rank() over (order by v) as rank, " +
                            "dense_rank() over (order by v) as dense_rank, " +
                            "percent_rank() over (order by v) as percent_rank " +
                            "from tab",
                    "ts",
                    true,
                    true
            );
        });
    }
}
