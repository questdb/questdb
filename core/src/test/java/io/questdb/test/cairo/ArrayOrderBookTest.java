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
    public void testBasicArrayOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x] id, timestamp_sequence(0,1000000000) as ts" +
                    "  from long_sequence(10)" +
                    ") timestamp(ts) partition by hour");

            assertQuery(
                    "id\n" +
                            "[10.0]\n" +
                            "[9.0]\n" +
                            "[8.0]\n" +
                            "[7.0]\n" +
                            "[6.0]\n" +
                            "[5.0]\n" +
                            "[4.0]\n" +
                            "[3.0]\n" +
                            "[2.0]\n" +
                            "[1.0]\n",
                    "select id from x order by id[1] desc",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testArrayOrderByAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x] id, timestamp_sequence(0,1000000000) as ts" +
                    "  from long_sequence(5)" +
                    ") timestamp(ts) partition by hour");

            assertQuery(
                    "id\n" +
                            "[1.0]\n" +
                            "[2.0]\n" +
                            "[3.0]\n" +
                            "[4.0]\n" +
                            "[5.0]\n",
                    "select id from x order by id[1] asc",
                    null,
                    true,
                    true);
        });
    }


    @Test
    public void testArrayOrderByDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x] id, timestamp_sequence(0,1000000000) as ts" +
                    "  from long_sequence(5)" +
                    ") timestamp(ts) partition by hour");

            assertQuery(
                    "id\n" +
                            "[5.0]\n" +
                            "[4.0]\n" +
                            "[3.0]\n" +
                            "[2.0]\n" +
                            "[1.0]\n",
                    "select id from x order by id[1] desc",
                    null,
                    true,
                    true);
        });
    }


    @Test
    public void testMultiElementArrayOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x, x*10, x*100] data, x::string name" +
                    "  from long_sequence(5)" +
                    ")");


            assertQuery(
                    "data\tname\n" +
                            "[1.0,10.0,100.0]\t1\n" +
                            "[2.0,20.0,200.0]\t2\n" +
                            "[3.0,30.0,300.0]\t3\n" +
                            "[4.0,40.0,400.0]\t4\n" +
                            "[5.0,50.0,500.0]\t5\n",
                    "select data, name from x order by data[1] asc",
                    null,
                    true,
                    true);

            assertQuery(
                    "data\tname\n" +
                            "[5.0,50.0,500.0]\t5\n" +
                            "[4.0,40.0,400.0]\t4\n" +
                            "[3.0,30.0,300.0]\t3\n" +
                            "[2.0,20.0,200.0]\t2\n" +
                            "[1.0,10.0,100.0]\t1\n",
                    "select data, name from x order by data[3] desc",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testArrayOrderByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x] data, x::string name" +
                    "  from long_sequence(10)" +
                    ")");

            assertQuery(
                    "data\tname\n" +
                            "[10.0]\t10\n" +
                            "[9.0]\t9\n" +
                            "[8.0]\t8\n",
                    "select data, name from x order by data[1] desc limit 3",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testArrayOrderByInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x] data, x::string name" +
                    "  from long_sequence(5)" +
                    ")");

            assertQuery(
                    "data\tname\n" +
                            "[5.0]\t5\n" +
                            "[4.0]\t4\n" +
                            "[3.0]\t3\n",
                    "select * from (select data, name from x order by data[1] desc) limit 3",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testMultipleArrayColumnsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x % 3] group_data, array[x] id_data, x::string name" +
                    "  from long_sequence(9)" +
                    ")");

            assertQuery(
                    "group_data\tid_data\tname\n" +
                            "[0.0]\t[3.0]\t3\n" +
                            "[0.0]\t[6.0]\t6\n" +
                            "[0.0]\t[9.0]\t9\n" +
                            "[1.0]\t[1.0]\t1\n" +
                            "[1.0]\t[4.0]\t4\n" +
                            "[1.0]\t[7.0]\t7\n" +
                            "[2.0]\t[2.0]\t2\n" +
                            "[2.0]\t[5.0]\t5\n" +
                            "[2.0]\t[8.0]\t8\n",
                    "select group_data, id_data, name from x order by group_data[1] asc, id_data[1] asc",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testComplexArrayExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x, x*2] data1, array[x*3, x*4] data2, x id" +
                    "  from long_sequence(5)" +
                    ")");

            assertQuery(
                    "data1\tdata2\tid\n" +
                            "[1.0,2.0]\t[3.0,4.0]\t1\n" +
                            "[2.0,4.0]\t[6.0,8.0]\t2\n" +
                            "[3.0,6.0]\t[9.0,12.0]\t3\n" +
                            "[4.0,8.0]\t[12.0,16.0]\t4\n" +
                            "[5.0,10.0]\t[15.0,20.0]\t5\n",
                    "select data1, data2, id from x order by data1[1] asc, data2[2] asc",
                    null,
                    true,
                    true);
        });
    }


    @Test
    public void testArrayOrderByWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[x % 3] data, x id" +
                    "  from long_sequence(9)" +
                    ")");

            assertQuery(
                    "data\n" +
                            "[0.0]\n" +
                            "[1.0]\n" +
                            "[2.0]\n",
                    "select distinct data from x order by data[1] asc",
                    null,
                    true,
                    true);
        });
    }


    @Test
    public void testArrayOrderByIndexBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select array[1.0, 2.0] data, x::string name" +
                    "  from long_sequence(3)" +
                    ")");

            assertQuery(
                    "data\tname\n" +
                            "[1.0,2.0]\t1\n" +
                            "[1.0,2.0]\t2\n" +
                            "[1.0,2.0]\t3\n",
                    "select data, name from x order by data[1] asc",
                    null,
                    true,
                    true);

            assertQuery(
                    "data\tname\n" +
                            "[1.0,2.0]\t1\n" +
                            "[1.0,2.0]\t2\n" +
                            "[1.0,2.0]\t3\n",
                    "select data, name from x order by data[2] asc",
                    null,
                    true,
                    true);
        });
    }
}
