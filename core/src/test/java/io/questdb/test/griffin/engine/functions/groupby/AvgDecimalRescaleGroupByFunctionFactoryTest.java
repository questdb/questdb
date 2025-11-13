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

public class AvgDecimalRescaleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAvgDecimal256Rescale256ProducesCorrectAverage() throws Exception {
        assertQuery(
                "avg\n173456789012345678901234567890123456.1234\n",
                "select avg(d, 4) avg from d256_values",
                "create table d256_values as (" +
                        "select case x " +
                        "when 1 then cast('123456789012345678901234567890123456.1234' as decimal(70,4)) " +
                        "else cast('223456789012345678901234567890123456.1234' as decimal(70,4)) end d " +
                        "from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgDecimal32Rescale256UsesCount() throws Exception {
        assertQuery(
                "avg\n3.000000000000000000000000000000\n",
                "select avg(d, 30) avg from d32_values",
                "create table d32_values as (" +
                        "select cast(2 * x as decimal(7,1)) d " +
                        "from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgDecimal64Rescale256UsesCount() throws Exception {
        assertQuery(
                "avg\n15.0000000000000000000000000\n",
                "select avg(d, 25) avg from d64_values",
                "create table d64_values as (" +
                        "select cast(10 * x as decimal(15,2)) d " +
                        "from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgDecimalAllNull() throws Exception {
        assertQuery(
                "avg\n\n",
                "select avg(x, 1) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgDecimalNegativeScale() throws Exception {
        assertException(
                "select avg(d, -1) from x",
                "create table x (d decimal(4,0))",
                14,
                "non-negative scale required: -1"
        );
    }

    @Test
    public void testAvgDecimalNoRescale() throws Exception {
        assertQuery(
                """
                        key\ta8\ta16\ta32\ta64\ta128\ta256
                        4\t48\t503\t494244\t4981441046664\t9060521021983552060173214255\t31304833156629889693497042595209754441918805445080974369080014085
                        3\t49\t485\t492251\t4997580785107\t9052893851427734152001930119\t30416293450185336623817302560742325189774224037945935861577153935
                        2\t48\t499\t502127\t4963120473044\t9171745243146077598389913474\t31453177765311683962656058347834093327429020565452336159500533099
                        1\t50\t494\t504103\t4970590450825\t9218679827218681817113584004\t30821997753191027869449119701652787675399873875822051937682996696
                        0\t49\t502\t501313\t4839510335333\t9200353642033472407119323164\t31609444379365793148224424272849410604357248203045640530705472888
                        """,
                "select id%5 key, avg(d8,0) a8, avg(d16,0) a16, avg(d32,0) a32, " +
                        "avg(d64,0) a64, avg(d128,0) a128, avg(d256,0) a256 " +
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
    public void testAvgDecimalOverflow() throws Exception {
        assertException(
                "select avg(d, 0) from x",
                "create table x as (" +
                        "select cast('9999999999999999999999999999999999999999999999999999999999999999999999999999' as decimal(76,0)) d " +
                        "from long_sequence(10)" +
                        ")",
                7,
                "avg aggregation failed: Overflow in addition: result exceeds 256-bit capacity"
        );
    }

    @Test
    public void testAvgDecimalTooLargePrecision() throws Exception {
        assertException(
                "select avg(d, 2) from x",
                "create table x (d decimal(76,0))",
                14,
                "rescaled decimal has precision that exceeds maximum of 76: 78"
        );
    }

    @Test
    public void testAvgDecimalTooLargeScale() throws Exception {
        assertException(
                "select avg(d, 80) from x",
                "create table x (d decimal(4,0))",
                14,
                "scale exceeds maximum of 76: 80"
        );
    }

    @Test
    public void testAvgDecimalWithRescale() throws Exception {
        assertQuery(
                """
                        a8\ta16\ta32\ta64\ta128\ta256
                        0.5\t1.00\t1.50\t4.500\t49.5000\t50.50000000
                        """,
                "select avg(d8,1) a8, avg(d16,2) a16, avg(d32,2) a32, avg(d64,3) a64, avg(d128,4) a128, avg(d256,8) a256 from x",
                "create table x as (" +
                        "select" +
                        " cast(x%2 as decimal(2,0)) d8, " +
                        " cast(x%3 as decimal(4,1)) d16, " +
                        " cast(x%4 as decimal(7,1)) d32, " +
                        " cast(x%10 as decimal(15,2)) d64, " +
                        " cast(x%100 as decimal(32,3)) d128, " +
                        " cast(x%1000 as decimal(68,6)) d256, " +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(100)" +
                        ") timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testAvgDecimalWithRescaleKeyed() throws Exception {
        assertQuery(
                """
                        key\ta8\ta16\ta32\ta64\ta128\ta256
                        4\t48.2\t502.56\t494244.35\t4981441046664.129\t9060521021983552060173214255.2114\t31304833156629889693497042595209754441918805445080974369080014084.85358136
                        3\t49.0\t485.28\t492251.19\t4997580785107.124\t9052893851427734152001930119.4278\t30416293450185336623817302560742325189774224037945935861577153935.39918013
                        2\t47.8\t499.47\t502126.96\t4963120473043.901\t9171745243146077598389913473.6346\t31453177765311683962656058347834093327429020565452336159500533098.52220347
                        1\t49.9\t494.36\t504102.79\t4970590450825.094\t9218679827218681817113584004.0123\t30821997753191027869449119701652787675399873875822051937682996696.05940104
                        0\t49.0\t502.00\t501313.01\t4839510335333.017\t9200353642033472407119323164.0022\t31609444379365793148224424272849410604357248203045640530705472887.81685772
                        """,
                "select id%5 key, avg(d8,1) a8, avg(d16,2) a16, avg(d32,2) a32, " +
                        "avg(d64,3) a64, avg(d128,4) a128, avg(d256,8) a256 " +
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
}
