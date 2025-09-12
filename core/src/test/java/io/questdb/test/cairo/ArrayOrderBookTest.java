/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class ArrayOrderBookTest extends AbstractCairoTest {

    @Before
    public void setUpThisTest() throws Exception {
        execute("CREATE TABLE order_book (ts TIMESTAMP, asks DOUBLE[][], bids DOUBLE[][])" +
                "TIMESTAMP(ts) PARTITION BY HOUR");
    }

    @Test
    public void testBidAskSpread() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "('2025-07-01T12:00:00Z', ARRAY[ [10.1, 10.2], [0, 0] ], ARRAY[ [9.3, 9.2], [0, 0] ]), " +
                    "('2025-07-01T12:00:01Z', ARRAY[ [10.3, 10.5], [0, 0] ], ARRAY[ [9.7, 9.4], [0, 0] ])"
            );
            assertSql("second\tspread\n" +
                            "0\t0.8\n" +
                            "1\t0.6\n",
                    "SELECT second(ts), round(asks[1][1] - bids[1][1], 2) spread FROM order_book");
        });
    }

    @Test
    public void testImbalanceAtTopLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [0.0,0], [10.0, 15] ], ARRAY[ [0.0,0], [20.0, 25] ]), " +
                    "(1, ARRAY[ [0.0,0], [15.0,  2] ], ARRAY[ [0.0,0], [14.0, 45] ])"
            );
            assertSql("imbalance\n2.0\n0.93\n",
                    "SELECT round(bids[2, 1] / asks[2, 1], 2) imbalance FROM order_book");
        });
    }

    @Test
    public void testImbalanceCumulative() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [0.0,0,0,0], [10.0, 15, 13, 12] ], ARRAY[ [0.0,0,0,0], [20.0, 25, 23, 22] ]), " +
                    "(1, ARRAY[ [0.0,0,0,0], [15.0,  2, 20, 23] ], ARRAY[ [0.0,0,0,0], [14.0, 45, 22,  5] ])"
            );
            assertSql("ask_vol\tbid_vol\tratio\n" +
                            "38.0\t68.0\t1.7894736842105263\n" +
                            "37.0\t81.0\t2.189189189189189\n",
                    "SELECT " +
                            "array_sum(asks[2, 1:4]) ask_vol, " +
                            "array_sum(bids[2, 1:4]) bid_vol, " +
                            "bid_vol / ask_vol ratio " +
                            "FROM order_book"
            );
        });
    }

    @Test
    public void testPriceLevelForTargetVolume() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(1, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0,  5,  3, 12, 18, 20] ], NULL)"
            );
            assertSql("cum_volumes\ttarget_level\tprice\n" +
                            "[10.0,25.0,38.0,50.0,68.0,88.0]\t3\t10.04\n" +
                            "[10.0,15.0,18.0,30.0,48.0,68.0]\t4\t10.1\n",
                    "SELECT " +
                            "array_cum_sum(asks[2]) cum_volumes, " +
                            "insertion_point(cum_volumes, 30.0, true) target_level, " +
                            "asks[1, target_level] price " +
                            "FROM order_book");
        });
    }

    @Test
    public void testPriceWeightedVolumeImbalance() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [6.0, 6.1], [15.0, 25] ], ARRAY[ [5.0, 5.1], [10.0, 20] ]), " +
                    "(1, ARRAY[ [6.2, 6.4], [20.0,  9] ], ARRAY[ [5.1, 5.2], [20.0, 25] ])"
            );
            assertSql("mid_price\tweighted_ask_pressure\tweighted_bid_pressure\n" +
                            "5.5\t[7.5,14.999999999999991]\t[5.0,8.000000000000007]\n" +
                            "5.65\t[10.999999999999996,6.75]\t[11.000000000000014,11.250000000000004]\n",
                    "SELECT " +
                            "round((asks[1][1] + bids[1][1]) / 2, 2) mid_price, " +
                            "(asks[1] - mid_price) * asks[2] weighted_ask_pressure, " +
                            "(mid_price - bids[1]) * bids[2] weighted_bid_pressure " +
                            "FROM order_book");
        });
    }

    @Test
    public void testSuddenVolumeDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0_000_000, ARRAY[ [0.0], [10.0] ], ARRAY[ [0.0], [10.0] ]), " +
                    "(1_000_000, ARRAY[ [0.0], [ 9.0] ], ARRAY[ [0.0], [ 9.0] ]), " +
                    "(2_000_000, ARRAY[ [0.0], [ 4.0] ], ARRAY[ [0.0], [ 8.0] ]), " +
                    "(3_000_000, ARRAY[ [0.0], [ 4.0] ], ARRAY[ [0.0], [ 4.0] ])"
            );
            assertSql("ts\tprev_ask_vol\tcurr_ask_vol\tprev_bid_vol\tcurr_bid_vol\n" +
                            "1970-01-01T00:00:02.000000Z\t9.0\t4.0\t9.0\t8.0\n" +
                            "1970-01-01T00:00:03.000000Z\t4.0\t4.0\t8.0\t4.0\n",
                    "SELECT * FROM (SELECT " +
                            "ts ts, " +
                            "lag(asks[2, 1]) OVER () prev_ask_vol, " +
                            "asks[2, 1] curr_ask_vol, " +
                            "lag(bids[2, 1]) OVER () prev_bid_vol, " +
                            "bids[2, 1] curr_bid_vol " +
                            "FROM order_book)" +
                            "WHERE prev_bid_vol > curr_bid_vol * 1.5 OR prev_ask_vol > curr_ask_vol * 1.5");
        });
    }

    @Test
    public void testVolumeAvailableUpToGivenPrice() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(1, ARRAY[ [10.0, 10.10, 10.12, 10.14, 10.16, 10.18], [1.0, 5, 3, 2, 8, 10] ], NULL)"
            );
            assertSql("inflated_top\n10.1\n10.1\n",
                    "SELECT asks[1,1] + 0.1 inflated_top FROM order_book");
            assertSql("insertion_point\n5\n3\n",
                    "SELECT insertion_point(asks[1], asks[1,1] + 0.1) FROM order_book");
            assertSql("volume\n50.0\n6.0\n",
                    "SELECT array_sum(asks[2, 1:insertion_point(asks[1], asks[1,1] + 0.1)]) volume" +
                            " FROM order_book");
        });
    }

    @Test
    public void testVolumeAvailableWithin1Percent() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.1, 10.12, 10.14], [10.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(1, ARRAY[ [20.0, 20.02, 20.04, 20.1, 20.12, 20.14], [1.0, 5, 3, 2, 8, 10] ], NULL)"
            );
            assertSql("inflated_top\n10.1\n20.2\n",
                    "SELECT 1.01 * asks[1,1] inflated_top FROM order_book");
            assertSql("insertion_point\n5\n7\n",
                    "SELECT insertion_point(asks[1], 1.01 * asks[1,1]) FROM order_book");
            assertSql("volume\n50.0\n29.0\n",
                    "SELECT array_sum(" +
                            "asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]" +
                            ") volume FROM order_book");
        });
    }

    @Test
    public void testVolumeDropoff() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [0.0,0,0,0,0,0], [20.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(1, ARRAY[ [0.0,0,0,0,0,0], [20.0, 25,  3,  7,  5,  2] ], NULL)"
            );
            assertSql("top\tdeep\n22.5\t5.0\n",
                    "SELECT * FROM (SELECT " +
                            "array_avg(asks[2, 1:3]) top, " +
                            "array_avg(asks[2, 3:6]) deep " +
                            "FROM order_book) " +
                            "WHERE top > 3 * deep");
        });
    }

    @Test
    public void testOrderByArrayIndexAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as long)] id, timestamp_sequence(0,1000000000L) as ts " +
                    "  from long_sequence(10)" +
                    ") timestamp(ts) partition by hour");

            assertSql(
                    "id\tts\n" +
                            "[1]\t1970-01-01T00:00:00.000000Z\n" +
                            "[2]\t1970-01-01T00:16:40.000000Z\n" +
                            "[3]\t1970-01-01T00:33:20.000000Z\n" +
                            "[4]\t1970-01-01T00:50:00.000000Z\n" +
                            "[5]\t1970-01-01T01:06:40.000000Z\n" +
                            "[6]\t1970-01-01T01:23:20.000000Z\n" +
                            "[7]\t1970-01-01T01:40:00.000000Z\n" +
                            "[8]\t1970-01-01T01:56:40.000000Z\n" +
                            "[9]\t1970-01-01T02:13:20.000000Z\n" +
                            "[10]\t1970-01-01T02:30:00.000000Z\n",
                    "select * from x order by id[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as long)] id, timestamp_sequence(0,1000000000L) as ts " +
                    "  from long_sequence(10)" +
                    ") timestamp(ts) partition by hour");

            assertSql(
                    "id\tts\n" +
                            "[10]\t1970-01-01T02:30:00.000000Z\n" +
                            "[9]\t1970-01-01T02:13:20.000000Z\n" +
                            "[8]\t1970-01-01T01:56:40.000000Z\n" +
                            "[7]\t1970-01-01T01:40:00.000000Z\n" +
                            "[6]\t1970-01-01T01:23:20.000000Z\n" +
                            "[5]\t1970-01-01T01:06:40.000000Z\n" +
                            "[4]\t1970-01-01T00:50:00.000000Z\n" +
                            "[3]\t1970-01-01T00:33:20.000000Z\n" +
                            "[2]\t1970-01-01T00:16:40.000000Z\n" +
                            "[1]\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x order by id[1] desc"
            );
        });
    }

    @Test
    public void testOrderByMultiElementArrayIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as long), cast(x*10 as long), cast(x*100 as long)] arr, x " +
                    "  from long_sequence(5)" +
                    ")");

            // Order by first element
            assertSql(
                    "arr\tx\n" +
                            "[1,10,100]\t1\n" +
                            "[2,20,200]\t2\n" +
                            "[3,30,300]\t3\n" +
                            "[4,40,400]\t4\n" +
                            "[5,50,500]\t5\n",
                    "select * from x order by arr[1] asc"
            );

            // Order by second element
            assertSql(
                    "arr\tx\n" +
                            "[1,10,100]\t1\n" +
                            "[2,20,200]\t2\n" +
                            "[3,30,300]\t3\n" +
                            "[4,40,400]\t4\n" +
                            "[5,50,500]\t5\n",
                    "select * from x order by arr[2] asc"
            );

            // Order by third element descending
            assertSql(
                    "arr\tx\n" +
                            "[5,50,500]\t5\n" +
                            "[4,40,400]\t4\n" +
                            "[3,30,300]\t3\n" +
                            "[2,20,200]\t2\n" +
                            "[1,10,100]\t1\n",
                    "select * from x order by arr[3] desc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as double)] darr, " +
                    "         array[x::string] sarr, " +
                    "         x " +
                    "  from long_sequence(5)" +
                    ")");

            // Order by double array index (note: string arrays cast as double in QuestDB)
            assertSql(
                    "darr\tsarr\tx\n" +
                            "[1.0]\t[1.0]\t1\n" +
                            "[2.0]\t[2.0]\t2\n" +
                            "[3.0]\t[3.0]\t3\n" +
                            "[4.0]\t[4.0]\t4\n" +
                            "[5.0]\t[5.0]\t5\n",
                    "select * from x order by darr[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select case when x % 2 = 0 then array[cast(x as long)] else array[cast(null as long)] end as arr, x " +
                    "  from long_sequence(6)" +
                    ")");

            // Order by array index with nulls - nulls appear first in ASC order
            assertSql(
                    "arr\tx\n" +
                            "[null]\t1\n" +
                            "[null]\t3\n" +
                            "[null]\t5\n" +
                            "[2]\t2\n" +
                            "[4]\t4\n" +
                            "[6]\t6\n",
                    "select * from x order by arr[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as double)] id, x " +
                    "  from long_sequence(3)" +
                    ")");

            // Accessing out-of-bounds index should return null and be handled gracefully
            assertSql(
                    "id\tx\n" +
                            "[1.0]\t1\n" +
                            "[2.0]\t2\n" +
                            "[3.0]\t3\n",
                    "select * from x order by id[2] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithComplexExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x*2 as double), cast(x*3 as double)] arr, x " +
                    "  from long_sequence(5)" +
                    ")");

            // Order by array index combined with arithmetic expression
            assertSql(
                    "arr\tx\n" +
                            "[2.0,3.0]\t1\n" +
                            "[4.0,6.0]\t2\n" +
                            "[6.0,9.0]\t3\n" +
                            "[8.0,12.0]\t4\n" +
                            "[10.0,15.0]\t5\n",
                    "select * from x order by arr[1] + arr[2] asc"
            );
        });
    }

    @Test
    public void testOrderByNestedArrayIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[array[cast(x as double), cast(x+1 as double)]] nested_arr, x " +
                    "  from long_sequence(3)" +
                    ")");

            // Order by nested array index
            assertSql(
                    "nested_arr\tx\n" +
                            "[[1.0,2.0]]\t1\n" +
                            "[[2.0,3.0]]\t2\n" +
                            "[[3.0,4.0]]\t3\n",
                    "select * from x order by nested_arr[1] asc"
            );
        });
    }

    @Test
    public void testOrderByMultipleArrayIndices() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x % 3 as double), cast(x as double)] arr1, array[cast(x % 2 as double), cast(x as double)] arr2, x " +
                    "  from long_sequence(6)" +
                    ")");

            // Order by multiple array indices
            assertSql(
                    "arr1\tarr2\tx\n" +
                            "[0.0,3.0]\t[1.0,3.0]\t3\n" +
                            "[0.0,6.0]\t[0.0,6.0]\t6\n" +
                            "[1.0,1.0]\t[1.0,1.0]\t1\n" +
                            "[1.0,4.0]\t[0.0,4.0]\t4\n" +
                            "[2.0,2.0]\t[0.0,2.0]\t2\n" +
                            "[2.0,5.0]\t[1.0,5.0]\t5\n",
                    "select * from x order by arr1[1] asc, arr2[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x % 3 as double)] group_arr, x " +
                    "  from long_sequence(9)" +
                    ")");

            assertSql(
                    "group_arr\tcount\n" +
                            "[0.0]\t3\n" +
                            "[1.0]\t3\n" +
                            "[2.0]\t3\n",
                    "select group_arr, count(*) from x " +
                            "group by group_arr " +
                            "order by group_arr[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as double)] id, x " +
                    "  from long_sequence(3)" +
                    ")");

            execute("create table y as (" +
                    "  select array[cast(x+10 as double)] id, x " +
                    "  from long_sequence(3)" +
                    ")");

            assertSql(
                    "id\tx\tid1\tx1\n" +
                            "[1.0]\t1\t[11.0]\t1\n" +
                            "[2.0]\t2\t[12.0]\t2\n" +
                            "[3.0]\t3\t[13.0]\t3\n",
                    "select x.id, x.x, y.id, y.x from x " +
                            "join y on x.x = y.x " +
                            "order by x.id[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexWithWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[cast(x as double)] id, x " +
                    "  from long_sequence(5)" +
                    ")");

            assertSql(
                    "id\tx\trow_num\n" +
                            "[1.0]\t1\t1\n" +
                            "[2.0]\t2\t2\n" +
                            "[3.0]\t3\t3\n" +
                            "[4.0]\t4\t4\n" +
                            "[5.0]\t5\t5\n",
                    "select id, x, row_number() over (order by id[1]) as row_num " +
                            "from x " +
                            "order by id[1] asc"
            );
        });
    }

    @Test
    public void testOrderByArrayIndexPerformance() throws Exception {
        assertMemoryLeak(() -> {
            // Test with larger dataset to ensure performance is acceptable
            execute("create table x as (" +
                    "  select array[rnd_long(1, 1000, 0)] id, " +
                    "         timestamp_sequence(0, 1000000) as ts " +
                    "  from long_sequence(1000)" +
                    ") timestamp(ts) partition by hour");

            // Verify that the query executes without performance issues
            assertSql("count\n1000\n", "select count(*) from x");
        });
    }

    @Test
    public void testOriginalIssueScenario() throws Exception {
        assertMemoryLeak(() -> {
            // This is the exact scenario from issue #6032
            execute("create table x as (" +
                    "  select array[cast(x as double)] id, timestamp_sequence(0,1000000000L) as ts " +
                    "  from long_sequence(10)" +
                    ") timestamp(ts) partition by hour");

            // This should not give "Invalid column: []" error anymore
            assertSql(
                    "id\tts\n" +
                            "[10.0]\t1970-01-01T02:30:00.000000Z\n" +
                            "[9.0]\t1970-01-01T02:13:20.000000Z\n" +
                            "[8.0]\t1970-01-01T01:56:40.000000Z\n" +
                            "[7.0]\t1970-01-01T01:40:00.000000Z\n" +
                            "[6.0]\t1970-01-01T01:23:20.000000Z\n" +
                            "[5.0]\t1970-01-01T01:06:40.000000Z\n" +
                            "[4.0]\t1970-01-01T00:50:00.000000Z\n" +
                            "[3.0]\t1970-01-01T00:33:20.000000Z\n" +
                            "[2.0]\t1970-01-01T00:16:40.000000Z\n" +
                            "[1.0]\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x order by id[1] desc"
            );
        });
    }
}
