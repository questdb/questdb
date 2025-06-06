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

package io.questdb.test.griffin;

import org.junit.Test;

import static io.questdb.test.griffin.PivotTest.*;


public class UnpivotTest extends AbstractSqlParserTest {

    public static String ddlMonthlySales = "CREATE TABLE monthly_sales\n" +
            "    (empid INT, dept SYMBOL, Jan INT, Feb INT, Mar INT, Apr INT, May INT, Jun INT);\n";
    public static String dmlMonthlySales = "INSERT INTO monthly_sales VALUES\n" +
            "    (1, 'electronics', 1, 2, 3, 4, 5, 6),\n" +
            "    (2, 'clothes', 10, 20, 30, 40, 50, 60),\n" +
            "    (3, 'cars', 100, 200, 300, 400, 500, 600),\n" +
            "    (4, 'appliances', 100, NULL, 100, 50, 20, 350);";

    @Test
    public void testBasicUnpivot() throws Exception {
        assertQueryAndPlan(
                "empid\tdept\tmonth\tsales\n",
                "monthly_sales UNPIVOT (\n" +
                        "    sales\n" +
                        "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                        ");",
                ddlMonthlySales,
                null,
                dmlMonthlySales,
                "empid\tdept\tmonth\tsales\n" +
                        "1\telectronics\tJan\t1\n" +
                        "1\telectronics\tFeb\t2\n" +
                        "1\telectronics\tMar\t3\n" +
                        "1\telectronics\tApr\t4\n" +
                        "1\telectronics\tMay\t5\n" +
                        "1\telectronics\tJun\t6\n" +
                        "2\tclothes\tJan\t10\n" +
                        "2\tclothes\tFeb\t20\n" +
                        "2\tclothes\tMar\t30\n" +
                        "2\tclothes\tApr\t40\n" +
                        "2\tclothes\tMay\t50\n" +
                        "2\tclothes\tJun\t60\n" +
                        "3\tcars\tJan\t100\n" +
                        "3\tcars\tFeb\t200\n" +
                        "3\tcars\tMar\t300\n" +
                        "3\tcars\tApr\t400\n" +
                        "3\tcars\tMay\t500\n" +
                        "3\tcars\tJun\t600\n" +
                        "4\tappliances\tJan\t100\n" +
                        "4\tappliances\tMar\t100\n" +
                        "4\tappliances\tApr\t50\n" +
                        "4\tappliances\tMay\t20\n" +
                        "4\tappliances\tJun\t350\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: sales\n" +
                        "  for: month\n" +
                        "  in: [Jan,Feb,Mar,Apr,May,Jun]\n" +
                        "  nulls: excluded\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: monthly_sales\n");
    }

    @Test
    public void testBasicUnpivotExcludeNulls() throws Exception { // default behaviour
        assertQueryAndPlan(
                "empid\tdept\tmonth\tsales\n",
                "monthly_sales UNPIVOT EXCLUDE NULLS (\n" +
                        "    sales\n" +
                        "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                        ");",
                ddlMonthlySales,
                null,
                dmlMonthlySales,
                "empid\tdept\tmonth\tsales\n" +
                        "1\telectronics\tJan\t1\n" +
                        "1\telectronics\tFeb\t2\n" +
                        "1\telectronics\tMar\t3\n" +
                        "1\telectronics\tApr\t4\n" +
                        "1\telectronics\tMay\t5\n" +
                        "1\telectronics\tJun\t6\n" +
                        "2\tclothes\tJan\t10\n" +
                        "2\tclothes\tFeb\t20\n" +
                        "2\tclothes\tMar\t30\n" +
                        "2\tclothes\tApr\t40\n" +
                        "2\tclothes\tMay\t50\n" +
                        "2\tclothes\tJun\t60\n" +
                        "3\tcars\tJan\t100\n" +
                        "3\tcars\tFeb\t200\n" +
                        "3\tcars\tMar\t300\n" +
                        "3\tcars\tApr\t400\n" +
                        "3\tcars\tMay\t500\n" +
                        "3\tcars\tJun\t600\n" +
                        "4\tappliances\tJan\t100\n" +
                        "4\tappliances\tMar\t100\n" +
                        "4\tappliances\tApr\t50\n" +
                        "4\tappliances\tMay\t20\n" +
                        "4\tappliances\tJun\t350\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: sales\n" +
                        "  for: month\n" +
                        "  in: [Jan,Feb,Mar,Apr,May,Jun]\n" +
                        "  nulls: excluded\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: monthly_sales\n");
    }

    @Test
    public void testBasicUnpivotIncludeNulls() throws Exception {
        assertQueryAndPlan(
                "empid\tdept\tmonth\tsales\n",
                "monthly_sales UNPIVOT INCLUDE NULLS (\n" +
                        "    sales\n" +
                        "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                        ");",
                ddlMonthlySales,
                null,
                dmlMonthlySales,
                "empid\tdept\tmonth\tsales\n" +
                        "1\telectronics\tJan\t1\n" +
                        "1\telectronics\tFeb\t2\n" +
                        "1\telectronics\tMar\t3\n" +
                        "1\telectronics\tApr\t4\n" +
                        "1\telectronics\tMay\t5\n" +
                        "1\telectronics\tJun\t6\n" +
                        "2\tclothes\tJan\t10\n" +
                        "2\tclothes\tFeb\t20\n" +
                        "2\tclothes\tMar\t30\n" +
                        "2\tclothes\tApr\t40\n" +
                        "2\tclothes\tMay\t50\n" +
                        "2\tclothes\tJun\t60\n" +
                        "3\tcars\tJan\t100\n" +
                        "3\tcars\tFeb\t200\n" +
                        "3\tcars\tMar\t300\n" +
                        "3\tcars\tApr\t400\n" +
                        "3\tcars\tMay\t500\n" +
                        "3\tcars\tJun\t600\n" +
                        "4\tappliances\tJan\t100\n" +
                        "4\tappliances\tFeb\tnull\n" +
                        "4\tappliances\tMar\t100\n" +
                        "4\tappliances\tApr\t50\n" +
                        "4\tappliances\tMay\t20\n" +
                        "4\tappliances\tJun\t350\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: sales\n" +
                        "  for: month\n" +
                        "  in: [Jan,Feb,Mar,Apr,May,Jun]\n" +
                        "  nulls: included\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: monthly_sales\n");
    }

    @Test
    public void testBasicUnpivotOrderByUnpivoted() throws Exception {
        assertQueryAndPlan(
                "empid\tdept\tmonth\tsales\n",
                "monthly_sales UNPIVOT (\n" +
                        "    sales\n" +
                        "    FOR month IN (jan, feb, mar, apr, may, jun)\n" +
                        ")" +
                        "ORDER BY sales\n;;",
                ddlMonthlySales,
                null,
                dmlMonthlySales,
                "empid\tdept\tmonth\tsales\n" +
                        "1\telectronics\tJan\t1\n" +
                        "1\telectronics\tFeb\t2\n" +
                        "1\telectronics\tMar\t3\n" +
                        "1\telectronics\tApr\t4\n" +
                        "1\telectronics\tMay\t5\n" +
                        "1\telectronics\tJun\t6\n" +
                        "2\tclothes\tJan\t10\n" +
                        "2\tclothes\tFeb\t20\n" +
                        "2\tclothes\tMar\t30\n" +
                        "2\tclothes\tApr\t40\n" +
                        "2\tclothes\tMay\t50\n" +
                        "2\tclothes\tJun\t60\n" +
                        "3\tcars\tJan\t100\n" +
                        "3\tcars\tFeb\t200\n" +
                        "3\tcars\tMar\t300\n" +
                        "3\tcars\tApr\t400\n" +
                        "3\tcars\tMay\t500\n" +
                        "3\tcars\tJun\t600\n" +
                        "4\tappliances\tJan\t100\n" +
                        "4\tappliances\tMar\t100\n" +
                        "4\tappliances\tApr\t50\n" +
                        "4\tappliances\tMay\t20\n" +
                        "4\tappliances\tJun\t350\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: sales\n" +
                        "  for: month\n" +
                        "  in: [Jan,Feb,Mar,Apr,May,Jun]\n" +
                        "  nulls: excluded\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: monthly_sales\n");
    }

    @Test
    public void testPivotUnpivotRoundTripCities() throws Exception {
        assertQueryAndPlan(
                "country\tyear\tsum_population\n",
                "(\n" +
                        "  cities\n" +
                        "    PIVOT (\n" +
                        "      SUM(population)\n" +
                        "      FOR year IN (2000, 2010, 2020)\n" +
                        "      GROUP BY country\n" +
                        "    )\n" +
                        ") UNPIVOT (\n" +
                        "    sum_population\n" +
                        "    FOR year IN (2000, 2010, 2020)\n" +
                        "  );\n",
                ddlCities,
                null,
                dmlCities,
                "country\tyear\tsum_population\n" +
                        "NL\t2000\t1005\n" +
                        "NL\t2010\t1065\n" +
                        "NL\t2020\t1158\n" +
                        "US\t2000\t8579\n" +
                        "US\t2010\t8783\n" +
                        "US\t2020\t9510\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: sum_population\n" +
                        "  for: year\n" +
                        "  in: [2000,2010,2020]\n" +
                        "  nulls: excluded\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [country]\n" +
                        "      values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [country,year]\n" +
                        "          values: [sum(population)]\n" +
                        "          filter: year in [2000,2010,2020]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotUnpivotRoundTripTrades() throws Exception {
        assertQueryAndPlan(
                "symbol\tavg_price\n",
                "(\n" +
                        "  trades\n" +
                        "    PIVOT (\n" +
                        "      avg(price) \n" +
                        "      FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        "    )\n" +
                        ") UNPIVOT (\n" +
                        "    avg_price\n" +
                        "    FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        "  );",
                ddlTrades,
                null,
                dmlTrades,
                "symbol\tavg_price\n" +
                        "BTC-USD\t101500.66363636362\n" +
                        "ETH-USD\t3678.0850000000005\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: avg_price\n" +
                        "  for: symbol\n" +
                        "  in: [BTC-USD,ETH-USD]\n" +
                        "  nulls: excluded\n" +
                        "    GroupBy vectorized: false\n" +
                        "      values: [avg(case([avg,NaN,symbol])),avg(case([avg,NaN,symbol]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [symbol]\n" +
                        "          values: [avg(price)]\n" +
                        "          filter: symbol in [BTC-USD,ETH-USD]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: trades\n");
    }

    @Test
    public void testUnpivotWithPivotAndOrderBy() throws Exception {
        assertQueryAndPlan(
                "side\tsymbol\tprice\n",
                "(\n" +
                        "  trades\n" +
                        "  PIVOT (\n" +
                        "    last(price)\n" +
                        "    FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        "    GROUP BY side\n" +
                        "    ORDER BY side\n" +
                        "  )\n" +
                        ") UNPIVOT (\n" +
                        "  price\n" +
                        "  FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "side\tsymbol\tprice\n" +
                        "buy\tBTC-USD\t101497.6\n" +
                        "buy\tETH-USD\t3678.01\n" +
                        "sell\tBTC-USD\t101497.0\n" +
                        "sell\tETH-USD\t3678.0\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: price\n" +
                        "  for: symbol\n" +
                        "  in: [BTC-USD,ETH-USD]\n" +
                        "  nulls: excluded\n" +
                        "    Sort light\n" +
                        "      keys: [side]\n" +
                        "        GroupBy vectorized: false\n" +
                        "          keys: [side]\n" +
                        "          values: [last_not_null(case([last,NaN,symbol])),last_not_null(case([last,NaN,symbol]))]\n" +
                        "            Async JIT Group By workers: 1\n" +
                        "              keys: [side,symbol]\n" +
                        "              values: [last(price)]\n" +
                        "              filter: symbol in [BTC-USD,ETH-USD]\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: trades\n");
    }

    @Test
    public void testUnpivotWithPivotAndOrderByAndLimit() throws Exception {
        assertQueryAndPlan(
                "side\tsymbol\tprice\n",
                "(\n" +
                        "  trades\n" +
                        "  PIVOT (\n" +
                        "    last(price)\n" +
                        "    FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        "    GROUP BY side\n" +
                        "    ORDER BY side\n" +
                        "    LIMIT 2\n" +
                        "  )\n" +
                        ") UNPIVOT (\n" +
                        "  price\n" +
                        "  FOR symbol IN ('BTC-USD', 'ETH-USD')\n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "side\tsymbol\tprice\n" +
                        "buy\tBTC-USD\t101497.6\n" +
                        "buy\tETH-USD\t3678.01\n" +
                        "sell\tBTC-USD\t101497.0\n" +
                        "sell\tETH-USD\t3678.0\n",
                false,
                false,
                false,
                "Unpivot\n" +
                        "  into: price\n" +
                        "  for: symbol\n" +
                        "  in: [BTC-USD,ETH-USD]\n" +
                        "  nulls: excluded\n" +
                        "    Sort light lo: 2\n" +
                        "      keys: [side]\n" +
                        "        GroupBy vectorized: false\n" +
                        "          keys: [side]\n" +
                        "          values: [last_not_null(case([last,NaN,symbol])),last_not_null(case([last,NaN,symbol]))]\n" +
                        "            Async JIT Group By workers: 1\n" +
                        "              keys: [side,symbol]\n" +
                        "              values: [last(price)]\n" +
                        "              filter: symbol in [BTC-USD,ETH-USD]\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: trades\n");
    }
}



