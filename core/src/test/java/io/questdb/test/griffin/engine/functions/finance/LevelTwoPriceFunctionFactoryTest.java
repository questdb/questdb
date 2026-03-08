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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.BindVariableTestTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LevelTwoPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testBindVarTypeChange() throws Exception {
        assertQuery("l2price\n" +
                "9.825714285714286\n", "select l2price(35, 8, 5.2, 23, 9.3, 42, 22.1)");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "all doubles",
                "l2price\n" +
                        "9.825714285714286\n",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 8);
                    bindVariableService.setDouble(1, 5.2);
                    bindVariableService.setDouble(2, 23);
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "type change",
                "l2price\n" +
                        "9.825714285714286\n",
                bindVariableService -> {
                    bindVariableService.setInt(0, 8);
                    bindVariableService.setDouble(1, 5.2);
                    bindVariableService.setByte(2, (byte) 23);
                }
        ));

        assertSql("select l2price(35, $1, $2, $3, 9.3, 42, 22.1)", tuples);
    }

    @Test
    public void testBindVarTypeChangeDoubleAmount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'btc_trades' (\n" +
                    "  symbol SYMBOL capacity 256 CACHE,\n" +
                    "  side SYMBOL capacity 256 CACHE,\n" +
                    "  price DOUBLE,\n" +
                    "  amount DOUBLE,\n" +
                    "  timestamp TIMESTAMP\n" +
                    ") timestamp (timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(symbol, timestamp);");

            execute("INSERT INTO btc_trades (symbol, side, price, amount, timestamp) VALUES\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00192304, '2023-09-05T16:59:45.804537Z'),\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00254854, '2023-09-05T16:59:46.639102Z'),\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00038735, '2023-09-05T16:59:47.229826Z'),\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00739719, '2023-09-05T16:59:48.289720Z'),\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00052228, '2023-09-05T16:59:48.829058Z'),\n" +
                    "('BTC-USD', 'buy', 25738.01, 0.00071191, '2023-09-05T16:59:49.002844Z'),\n" +
                    "('BTC-USD', 'buy', 25738.76, 0.00367218, '2023-09-05T16:59:51.153025Z'),\n" +
                    "('BTC-USD', 'buy', 25741.68, 0.05034042, '2023-09-05T16:59:52.260796Z'),\n" +
                    "('BTC-USD', 'buy', 25740.38, 0.00219286, '2023-09-05T16:59:53.713701Z'),\n" +
                    "('BTC-USD', 'buy', 25739.75, 0.00037233, '2023-09-05T16:59:56.452825Z'),\n" +
                    "('BTC-USD', 'buy', 25735.97, 0.00006303, '2023-09-05T16:59:58.095714Z'),\n" +
                    "('BTC-USD', 'buy', 25735.97, 0.00816829, '2023-09-05T16:59:58.761737Z'),\n" +
                    "('BTC-USD', 'buy', 25735.97, 0.00816829, '2023-09-05T16:59:58.761737Z'),\n" +
                    "('BTC-USD', 'buy', 25735.97, 0.00816829, '2023-09-05T16:59:58.761737Z')\n;");

            drainWalQueue();

            assertSql("l2price\n" +
                    "25740.038313200002\n", "with recent_trades as\n" +
                    "(\n" +
                    "    select \n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 1 preceding and 1 preceding) as amount1,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 2 preceding and 2 preceding) as amount2,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 3 preceding and 3 preceding) as amount3,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 4 preceding and 4 preceding) as amount4,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 5 preceding and 5 preceding) as amount5,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 6 preceding and 6 preceding) as amount6,\n" +
                    "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 6 preceding and 6 preceding) as amount7,\n" +
                    "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 1 preceding and 1 preceding) as price1,\n" +
                    "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 2 preceding and 2 preceding) as price2,\n" +
                    "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 3 preceding and 3 preceding) as price3,\n" +
                    "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 4 preceding and 4 preceding) as price4,\n" +
                    "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 5 preceding and 5 preceding) as price5,\n" +
                    "     FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 6 preceding and 6 preceding) as price6,\n" +
                    "     FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 6 preceding and 6 preceding) as price7\n" +
                    "    from btc_trades\n" +
                    "    where side = 'buy'\n" +
                    "    limit -12\n" +
                    ")\n" +
                    "select l2price(0.0015, amount1, price1, amount2, price2, amount3, price3, \n" +
                    "amount4, price4, amount5, price5, amount6, price6) from recent_trades\n" +
                    "limit -1;");

            assertFailure(
                    "[869] l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `STRING`.",
                    "with recent_trades as\n" +
                            "(\n" +
                            "    select \n" +
                            "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 1 preceding and 1 preceding) as amount1,\n" +
                            "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 2 preceding and 2 preceding) as amount2,\n" +
                            "    FIRST_VALUE(amount) over(partition by symbol order by timestamp rows between 3 preceding and 3 preceding) as amount3,\n" +
                            "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 1 preceding and 1 preceding) as price1,\n" +
                            "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 2 preceding and 2 preceding) as price2,\n" +
                            "    FIRST_VALUE(price) over(partition by symbol order by timestamp rows between 3 preceding and 3 preceding) as price3\n" +
                            "    from btc_trades\n" +
                            "    where side = 'buy'\n" +
                            "    limit -12\n" +
                            ")\n" +
                            "select l2price(0.0015, amount1, price1, amount2, 'string fail', amount3, price3),\n" +
                            "l2price(0.0015, amount2, price2) from recent_trades"
            );
        });
    }

    @Test
    public void testBindVarTypeFailureErrorPosition() {
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "type failure",
                "l2price\n" +
                        "9.825714285714286\n",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 8);
                    bindVariableService.setDouble(1, 5.2);
                    bindVariableService.setStr(2, "str fail");
                    bindVariableService.setDouble(3, 23);
                }
        ));

        try {
            assertSql("select l2price(35, $1, $2, $3, 9.3, 42, 22.1), l2price(35, $4, 9.3, 42, 22.1)", tuples);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "[27] l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `STRING`");
        }
    }

    @Test
    public void testInconvertibleTypes() throws Exception {
        assertMemoryLeak(() -> {
            assertFailure("[20] l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `STRING`.", "select l2price(1.3, '31', 5)");
            assertFailure("[23] l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `STRING`.", "select l2price(100, 1, '31abascsd')");
            execute("create table x as ( select rnd_str(1, 10, 0) as s from long_sequence(100) )");
            drainWalQueue();
            assertFailure("[23] l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `STRING`.", "select l2price(100, 1, s) from x");
        });
    }

    @Test
    public void testLevelTwoPrice() throws Exception {
        assertQuery("l2price\n" +
                "9.825714285714286\n", "select l2price(35, 8, 5.2, 23, 9.3, 42, 22.1)");
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertFailure("[7] there is no matching function `l2price` with the argument types: (INT)", "select l2price(100)");
    }

    @Test
    public void testLevelTwoPriceFailsWithEvenArgs() throws Exception {
        assertException("select l2price(35, 8, 7, 6);", 25, "l2price requires an odd number of arguments.");
    }

    /**
     * l2price should return null for the price if it encounters an order with null values.
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
    public void testLevelTwoPriceReturnsNullWithNullTarget() throws Exception {
        assertQuery("l2price\nnull\n", "select l2price(null, 8, 17.2);");
    }

    @Test
    public void testLevelTwoPriceUnrolled() throws Exception {
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1)");  // win
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(10, 5, 1, 5, 1)"); // win
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1, 5, 1, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(10, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(15, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1, 5, 1, 5, 1, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(10, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(15, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(20, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\nnull\n", "select l2price(100, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)"); // fail
        assertQuery("l2price\n1.0\n", "select l2price(5, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(10, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(15, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(20, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
        assertQuery("l2price\n1.0\n", "select l2price(25, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1)");  // win
    }

    @Test
    public void testLevelTwoPriceWithAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as ( select timestamp_sequence(172800000000, 3600000000) ts, rnd_long(12, 20, 0) as size, rnd_double() as value from long_sequence(10))");
            drainWalQueue();

            assertQuery("avg\n"
                    + "0.5617574066977766\n", "select avg(l2price(14, size, value)) from x");
        });
    }

    @Test
    public void testLevelTwoPriceWithSpread() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as ( select timestamp_sequence(172800000000, 3600000000) ts, " +
                    "rnd_long(12, 20, 0) as ask_size, " +
                    "rnd_double() as ask_value, " +
                    "rnd_long(12, 20, 0) as bid_size, " +
                    "rnd_double() as bid_value " +
                    "from long_sequence(10))");
            drainWalQueue();

            assertPlanNoLeakCheck(
                    "select avg(l2price(14, ask_size, ask_value)) " +
                            "- avg(l2price(14, bid_size, bid_value))" +
                            " as spread from x",
                    "VirtualRecord\n" +
                            "  functions: [avg1-avg]\n" +
                            "    Async Group By workers: 1\n" +
                            "      values: [avg(l2price([14,bid_size,bid_value])),avg(l2price([14,ask_size,ask_value]))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n"
            );

            assertPlanNoLeakCheck(
                    "select l2price(14, ask_size, ask_value) " +
                            "- l2price(14, bid_size, bid_value)" +
                            " as spread from x",
                    "VirtualRecord\n" +
                            "  functions: [l2price([14,ask_size,ask_value])-l2price([14,bid_size,bid_value])]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );

            assertQuery(
                    "spread\n" +
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
                            " as spread from x"
            );
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LevelTwoPriceFunctionFactory();
    }
}
