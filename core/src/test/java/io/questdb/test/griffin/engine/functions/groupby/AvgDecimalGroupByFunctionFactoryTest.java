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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AvgDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAvg() throws Exception {
        assertQuery(
                """
                        a8\ta16\ta32\ta64\ta128\ta256
                        0\t1.0\t1.5\t4.50\t49.500\t50.500000
                        """,
                "select avg(d8) a8, avg(d16) a16, avg(d32) a32, avg(d64) a64, avg(d128) a128, avg(d256) a256 from x",
                "create table x as (" +
                        "select" +
                        " cast(x%2 as decimal(2,0)) d8, " +
                        " cast(x%3 as decimal(4,1)) d16, " +
                        " cast(x%4 as decimal(7,1)) d32, " +
                        " cast(x%10 as decimal(15,2)) d64, " +
                        " cast(x%100 as decimal(32,3)) d128, " +
                        " cast(x%1000 as decimal(76,6)) d256, " +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(100)" +
                        ") timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgAllNull() throws Exception {
        assertQuery(
                "avg\n\n",
                "select avg(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgKeyed() throws Exception {
        assertQuery(
                """
                        key\ta8\ta16\ta32\ta64\ta128\ta256
                        4\t48\t502.6\t494244.3\t4981441046664.13\t9060521021983552060173214255.211\t31304833156629889693497042595209754441918805445080974369080014084.85358
                        3\t49\t485.3\t492251.2\t4997580785107.12\t9052893851427734152001930119.428\t30416293450185336623817302560742325189774224037945935861577153935.39918
                        2\t48\t499.5\t502127.0\t4963120473043.90\t9171745243146077598389913473.635\t31453177765311683962656058347834093327429020565452336159500533098.52220
                        1\t50\t494.4\t504102.8\t4970590450825.09\t9218679827218681817113584004.012\t30821997753191027869449119701652787675399873875822051937682996696.05940
                        0\t49\t502.0\t501313.0\t4839510335333.02\t9200353642033472407119323164.002\t31609444379365793148224424272849410604357248203045640530705472887.81686
                        """,
                "select id%5 key, avg(d8) a8, avg(d16) a16, avg(d32) a32, " +
                        "avg(d64) a64, avg(d128) a128, avg(d256) a256 " +
                        "from x " +
                        "order by key desc",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_decimal(2,0,2) d8," +
                        " rnd_decimal(4,1,2) d16," +
                        " rnd_decimal(7,1,2) d32," +
                        " rnd_decimal(15,2,2) d64," +
                        " rnd_decimal(32,3,2) d128," +
                        " rnd_decimal(70,5,2) d256," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                true
        );
    }

    @Test
    public void testAvgOverflow() throws Exception {
        assertException(
                "select avg(d) from x",
                "create table x as (" +
                        "select cast('9999999999999999999999999999999999999999999999999999999999999999999999999999' as decimal(76,0)) d " +
                        "from long_sequence(10)" +
                        ")",
                7,
                "avg aggregation failed: Overflow in addition: result exceeds 256-bit capacity"
        );
    }
}
