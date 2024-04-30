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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LevelTwoPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testLevelTwoPrice() throws Exception {
        assertQuery("l2price\n" +
                "9.825714285714286\n", "select l2price(35, 8, 5.2, 23, 9.3, 42, 22.1)");
    }

    @Test
    public void testLevelTwoPriceFailsWithEvenArgs() throws Exception {
        assertException("select l2price(35, 8);", 19, "l2price requires an odd number of arguments.");
    }

    @Test
    public void testLevelTwoPriceFailsWithNullTarget() throws Exception {
        assertException("select l2price(null, 8, 17.2);", 15, "l2price requires a non-null first argument");
    }

    /**
     * l2price should return null for the price if it encounters an order with null values.
     *
     * @throws Exception
     */
    @Test
    public void testLevelTwoPriceIncludingNullValue() throws Exception {
        assertQuery("l2price\n"
                + "null\n", "select l2price(45, null, 3)");
        assertQuery("l2price\n"
                + "null\n", "select l2price(45, 3, null)");
        assertQuery("l2price\n"
                + "null\n", "select l2price(45, 5, 3.2, 12, null)");
        assertQuery("l2price\n"
                + "null\n", "select l2price(999999, 5, 3.2, 12, 5.1, 10, 12.3, 67, 42.4, 38, 29.9, 39, 40.3, null, 5.1)");
        assertQuery("l2price\n"
                + "21.408888888888885\n", "select l2price(45, 5, 3.2, 12, 5.1, 10, 12.3, 67, 42.4, 38, 29.9, 39, 40.3, null, 5.1)");
    }

    @Test
    public void testLevelTwoPriceWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as ( select timestamp_sequence(172800000000, 3600000000) ts, rnd_long(12, 20, 0) as size, rnd_double() as value from long_sequence(10))");
            drainWalQueue();

            assertQuery("avg\n"
                    + "0.5617574066977766\n", "select avg(l2price(14, size, value)) from x");
        });
    }

    @Test
    public void testLevelTwoPriceWithSpread() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as ( select timestamp_sequence(172800000000, 3600000000) ts, " +
                    "rnd_long(12, 20, 0) as ask_size, " +
                    "rnd_double() as ask_value, " +
                    "rnd_long(12, 20, 0) as bid_size, " +
                    "rnd_double() as bid_value " +
                    "from long_sequence(10))");
            drainWalQueue();

            assertPlan("select avg(l2price(14, ask_size, ask_value)) " +
                            "- avg(l2price(14, bid_size, bid_value))" +
                            " as spread from x",
                    "VirtualRecord\n" +
                            "  functions: [avg1-avg]\n" +
                            "    Async Group By workers: 1\n" +
                            "      values: [avg(l2price([14,bid_size,bid_value])),avg(l2price([14,ask_size,ask_value]))]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n");

            assertPlan("select l2price(14, ask_size, ask_value) " +
                            "- l2price(14, bid_size, bid_value)" +
                            " as spread from x",
                    "VirtualRecord\n" +
                            "  functions: [l2price([14,ask_size,ask_value])-l2price([14,bid_size,bid_value])]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n");

            assertQuery("spread\n" +
                            "-0.0745689117121191\n" +
                            "-0.33476968200189616\n" +
                            "-0.2517202513378337\n" +
                            "null\n" +
                            "0.2924569772314709\n" +
                            "-0.43148475135445163\n" +
                            "-0.01642808233254145\n" +
                            "0.4590854085578898\n" +
                            "null\n" +
                            "0.214176258308453\n",
                    "select l2price(14, ask_size, ask_value) " +
                            "- l2price(14, bid_size, bid_value)" +
                            " as spread from x");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LevelTwoPriceFunctionFactory();
    }


}