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
    public void testCumulativeImbalance() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [0.0,0,0,0], [10.0, 15, 13, 12] ], ARRAY[ [0.0,0,0,0], [20.0, 25, 23, 22] ]), " +
                    "(0, ARRAY[ [0.0,0,0,0], [15.0,  2, 20, 23] ], ARRAY[ [0.0,0,0,0], [14.0, 45, 22,  5] ])"
            );
            assertSql("ask_vol\tbid_vol\tratio\n" +
                            "38.0\t68.0\t1.7894736842105263\n" +
                            "37.0\t81.0\t2.189189189189189\n",
                    "SELECT " +
                            "array_sum(asks[2, 1:4]) ask_vol, " +
                            "array_sum(bids[2, 1:4]) bid_vol, " +
                            "array_sum(bids[2, 1:4]) / array_sum(asks[2, 1:4]) ratio " +
                            "FROM order_book");
        });
    }

    @Test
    public void testPriceLevelForTargetVolume() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0,  5,  3, 12, 18, 20] ], NULL)"
            );
            assertSql("cum_volumes\ttarget_level\tprice\n" +
                            "[10.0,25.0,38.0,50.0,68.0,88.0]\t3\t10.04\n" +
                            "[10.0,15.0,18.0,30.0,48.0,68.0]\t4\t10.1\n",
                    "WITH q1 AS (SELECT asks, array_cum_sum(asks[2]) cum_volumes FROM order_book), " +
                            "q2 AS (SELECT asks, cum_volumes, insertion_point(cum_volumes, 30.0, true) target_level FROM q1) " +
                            "SELECT cum_volumes, target_level, asks[1, target_level] price " +
                            "FROM q2");
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
                            "t2.ts ts, " +
                            "t1.asks[2, 1] prev_ask_vol, " +
                            "t2.asks[2, 1] curr_ask_vol, " +
                            "t1.bids[2, 1] prev_bid_vol, " +
                            "t2.bids[2, 1] curr_bid_vol " +
                            "FROM order_book t1 JOIN order_book t2 ON t1.ts = t2.ts - 1_000_000) " +
                            "WHERE prev_bid_vol > curr_bid_vol * 1.5 OR prev_ask_vol > curr_ask_vol * 1.5");
        });
    }

    @Test
    public void testVolumeAvailableUpToGivenPrice() throws Exception {
        assertMemoryLeak(() -> {
            execute("INSERT INTO order_book VALUES " +
                    "(0, ARRAY[ [10.0, 10.02, 10.04, 10.10, 10.12, 10.14], [10.0, 15, 13, 12, 18, 20] ], NULL), " +
                    "(0, ARRAY[ [10.0, 10.10, 10.12, 10.14, 10.16, 10.18], [1.0, 5, 3, 2, 8, 10] ], NULL)"
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
                    "(0, ARRAY[ [20.0, 20.02, 20.04, 20.1, 20.12, 20.14], [1.0, 5, 3, 2, 8, 10] ], NULL)"
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
}
