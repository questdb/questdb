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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TwapGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> assertSql("twap\n" +
                        "0.5207429687020669\n",
                "select twap(rnd_double(), x) from long_sequence(10)"
        ));
    }

    @Test
    public void testNonAscendingOrderThrowsError() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertException("select twap(rnd_double(), rnd_long()) from long_sequence(1000000)", -1, "twap requires timestamps to be in ascending order");
            } catch (RuntimeException ex) {
                return;
            }
            Assert.fail();
        });
    }

    @Test
    public void testNull() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (price double, ts timestamp)");
            insert("insert into tab values (null,null),(null,1),(1,null)");
            assertSql("twap\n" +
                            "null\n",
                    "select twap(price, ts) from tab"
            );
        });
    }

    @Test
    public void testTwap() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (price double, ts timestamp)");
            insert("insert into tab values (100,10),(105,40)");
            assertSql("twap\n" +
                            "108.33333333333333\n",
                    "select twap(price, ts) from tab"
            );
        });
    }

    @Test
    public void testTwapGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (price double, ts timestamp, ticker symbol)");
            insert("insert into tab values (100,10,'a'),(105,40,'a'),(102,20,'b'),(103,60,'b')");
            assertSql("ticker\ttwap\n" +
                            "a\t108.33333333333333\n" +
                            "b\t105.55\n",
                    "select ticker, twap(price, ts) from tab order by ticker"
            );
        });
    }

    @Test
    public void testTwapRealData() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (price double, timestamp timestamp) timestamp(timestamp)");
            insert("insert into trades (price, timestamp) values (2615.54,'2022-03-08T18:03:57.609765Z'),\n" +
                    "            (2615.4,'2022-03-08T18:03:57.764098Z'),\n" +
                    "            (2615.4,'2022-03-08T18:03:57.764098Z'),\n" +
                    "            (2615.4,'2022-03-08T18:03:57.764098Z'),\n" +
                    "            (2615.36,'2022-03-08T18:03:58.194582Z'),\n" +
                    "            (2615.37,'2022-03-08T18:03:58.194582Z'),\n" +
                    "            (2615.46,'2022-03-08T18:03:58.194582Z'),\n" +
                    "            (2615.47,'2022-03-08T18:03:58.194582Z'),\n" +
                    "            (2615.35,'2022-03-08T18:03:58.612275Z'),\n" +
                    "            (2615.36,'2022-03-08T18:03:58.612275Z'),\n" +
                    "            (2615.62,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.62,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.62,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.62,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.62,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.63,'2022-03-08T18:03:58.682070Z'),\n" +
                    "            (2615.43,'2022-03-08T18:03:59.093929Z'),\n" +
                    "            (2615.43,'2022-03-08T18:03:59.093929Z'),\n" +
                    "            (2615.38,'2022-03-08T18:03:59.093929Z'),\n" +
                    "            (2615.36,'2022-03-08T18:03:59.093929Z');");
            assertSql("twap\n" +
                    "2615.3947594740202\n", "select twap(price, timestamp) from trades");
        });
    }
}

