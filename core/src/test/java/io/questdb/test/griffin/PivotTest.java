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

public class PivotTest extends AbstractSqlParserTest {

    public static String ddlCities = "CREATE TABLE cities (\n" +
            "    country VARCHAR, name VARCHAR, year INT, population INT\n" +
            ");";
    public static String ddlMonthlySales = "CREATE TABLE monthly_sales (empid INT, amount INT, month TEXT);";
    public static String ddlSensors = "CREATE TABLE IF NOT EXISTS sensors (\n" +
            "  timestamp TIMESTAMP,\n" +
            "  vehicle_id SYMBOL,\n" +
            "  sensor_name SYMBOL,\n" +
            "  int_value LONG,\n" +
            "  str_value STRING\n" +
            ") timestamp(timestamp) PARTITION BY DAY;\n";
    public static String ddlTrades = "CREATE TABLE 'trades' (\n" +
            "  symbol SYMBOL,\n" +
            "  side SYMBOL,\n" +
            "  price DOUBLE,\n" +
            "  amount DOUBLE,\n" +
            "  timestamp TIMESTAMP\n" +
            ") timestamp (timestamp) PARTITION BY NONE";
    public static String dmlCities =
            "INSERT INTO cities VALUES\n" +
                    "    ('NL', 'Amsterdam', 2000, 1005),\n" +
                    "    ('NL', 'Amsterdam', 2010, 1065),\n" +
                    "    ('NL', 'Amsterdam', 2020, 1158),\n" +
                    "    ('US', 'Seattle', 2000, 564),\n" +
                    "    ('US', 'Seattle', 2010, 608),\n" +
                    "    ('US', 'Seattle', 2020, 738),\n" +
                    "    ('US', 'New York City', 2000, 8015),\n" +
                    "    ('US', 'New York City', 2010, 8175),\n" +
                    "    ('US', 'New York City', 2020, 8772);";
    public static String dmlMonthlySales = " INSERT INTO monthly_sales VALUES\n" +
            "            (1, 10000, 'JAN'),\n" +
            "    (1, 400, 'JAN'),\n" +
            "            (2, 4500, 'JAN'),\n" +
            "            (2, 35000, 'JAN'),\n" +
            "            (1, 5000, 'FEB'),\n" +
            "            (1, 3000, 'FEB'),\n" +
            "            (2, 200, 'FEB'),\n" +
            "            (2, 90500, 'FEB'),\n" +
            "            (1, 6000, 'MAR'),\n" +
            "            (1, 5000, 'MAR'),\n" +
            "            (2, 2500, 'MAR'),\n" +
            "            (2, 9500, 'MAR'),\n" +
            "            (1, 8000, 'APR'),\n" +
            "            (1, 10000, 'APR'),\n" +
            "            (2, 800, 'APR'),\n" +
            "            (2, 4500, 'APR');";
    public static String dmlSensors = "INSERT INTO sensors\n" +
            "SELECT\n" +
            "    date_trunc('milliseconds', timestamp_sequence('2025-01-01', 1L) + (x / 2000)) AS timestamp,\n" +
            "    'AAA' || lpad(((x / 20) % 100)::string, 3, '0') AS vehicle_id,\n" +
            "    CASE\n" +
            "        WHEN x % 20 < 10 THEN 'i' || lpad((x % 10)::string, 3, '0')\n" +
            "        ELSE 's' || lpad(((x % 10))::string, 3, '0')\n" +
            "    END AS sensor_name,\n" +
            "    CASE WHEN x % 20 < 10 THEN rnd_long() % 1000 ELSE NULL END AS int_value,\n" +
            "    CASE WHEN x % 20 >= 10 THEN 'val_' || rnd_int() % 1000 ELSE NULL END AS str_value\n" +
            "FROM long_sequence(10000) x;";
    public static String dmlTrades = "INSERT INTO trades(symbol,side,price,amount,timestamp) \n" +
            "VALUES ('ADA-USDT','sell',0.9716,94.2581,'2024-12-19T08:10:00.062000Z'),\n" +
            " ('ADA-USD','sell',0.9716,94.2581,'2024-12-19T08:10:00.062000Z'),\n" +
            " ('BTC-USDT','buy',101502.2,5.775E-5,'2024-12-19T08:10:00.136000Z'),\n" +
            " ('BTC-USD','buy',101502.2,5.775E-5,'2024-12-19T08:10:00.136000Z'),\n" +
            " ('BTC-USDT','sell',101502.1,1.4443E-4,'2024-12-19T08:10:00.138000Z'),\n" +
            " ('BTC-USD','sell',101502.1,1.4443E-4,'2024-12-19T08:10:00.138000Z'),\n" +
            " ('BTC-USDT','buy',101502.2,3.4654E-4,'2024-12-19T08:10:00.244000Z'),\n" +
            " ('BTC-USD','buy',101502.2,3.4654E-4,'2024-12-19T08:10:00.244000Z'),\n" +
            " ('DOGE-USDT','sell',0.36051,47.831939,'2024-12-19T08:10:00.322000Z'),\n" +
            " ('DOGE-USD','sell',0.36051,47.831939,'2024-12-19T08:10:00.322000Z'),\n" +
            " ('DOGE-USDT','sell',0.36046,978.95676,'2024-12-19T08:10:00.322000Z'),\n" +
            " ('DOGE-USD','sell',0.36046,978.95676,'2024-12-19T08:10:00.322000Z'),\n" +
            " ('DOGE-USDT','buy',0.36047,8683.359195,'2024-12-19T08:10:00.392000Z'),\n" +
            " ('DOGE-USD','buy',0.36047,8683.359195,'2024-12-19T08:10:00.392000Z'),\n" +
            " ('BTC-USDT','buy',101502.2,9.359E-5,'2024-12-19T08:10:00.424000Z'),\n" +
            " ('BTC-USD','buy',101502.2,9.359E-5,'2024-12-19T08:10:00.424000Z'),\n" +
            " ('USDT-USDC','buy',0.9994,135.321,'2024-12-19T08:10:00.548000Z'),\n" +
            " ('ADA-USDT','sell',0.9716,1763.2036,'2024-12-19T08:10:00.552000Z'),\n" +
            " ('ADA-USD','sell',0.9716,1763.2036,'2024-12-19T08:10:00.552000Z'),\n" +
            " ('ADA-USDT','sell',0.9716,5117.169,'2024-12-19T08:10:00.559000Z'),\n" +
            " ('ADA-USD','sell',0.9716,5117.169,'2024-12-19T08:10:00.559000Z'),\n" +
            " ('BTC-USDT','buy',101502.2,1.4449E-4,'2024-12-19T08:10:00.600000Z'),\n" +
            " ('BTC-USD','buy',101502.2,1.4449E-4,'2024-12-19T08:10:00.600000Z'),\n" +
            " ('BTC-USDT','buy',101502.2,1.7339E-4,'2024-12-19T08:10:00.665999Z'),\n" +
            " ('BTC-USD','buy',101502.2,1.7339E-4,'2024-12-19T08:10:00.665999Z'),\n" +
            " ('BTC-USDT','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.693000Z'),\n" +
            " ('BTC-USD','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.693000Z'),\n" +
            " ('ETH-USDT','sell',3678.25,0.026994,'2024-12-19T08:10:00.700999Z'),\n" +
            " ('ETH-USD','sell',3678.25,0.026994,'2024-12-19T08:10:00.700999Z'),\n" +
            " ('BTC-USDT','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.716999Z'),\n" +
            " ('BTC-USD','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.716999Z'),\n" +
            " ('BTC-USDT','buy',101502.2,8.3806E-4,'2024-12-19T08:10:00.724000Z'),\n" +
            " ('BTC-USD','buy',101502.2,8.3806E-4,'2024-12-19T08:10:00.724000Z'),\n" +
            " ('BTC-USDT','sell',101502.1,0.02973634,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USD','sell',101502.1,0.02973634,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USDT','sell',101502.1,0.06976683,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USD','sell',101502.1,0.06976683,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USDT','sell',101500.9,0.01971311,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USD','sell',101500.9,0.01971311,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USDT','sell',101500.2,0.00621176,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USD','sell',101500.2,0.00621176,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USDT','sell',101500.0,0.04697513,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USD','sell',101500.0,0.04697513,'2024-12-19T08:10:00.732999Z'),\n" +
            " ('BTC-USDT','sell',101500.0,0.02353103,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('BTC-USD','sell',101500.0,0.02353103,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('BTC-USDT','sell',101500.0,0.07167521,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('BTC-USD','sell',101500.0,0.07167521,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('DOGE-USDT','sell',0.36045,3400.0,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('DOGE-USD','sell',0.36045,3400.0,'2024-12-19T08:10:00.733999Z'),\n" +
            " ('BTC-USDT','sell',101500.0,0.01922527,'2024-12-19T08:10:00.734999Z'),\n" +
            " ('BTC-USD','sell',101500.0,0.01922527,'2024-12-19T08:10:00.734999Z'),\n" +
            " ('BTC-USDT','sell',101499.9,4.8784E-4,'2024-12-19T08:10:00.734999Z'),\n" +
            " ('BTC-USD','sell',101499.9,4.8784E-4,'2024-12-19T08:10:00.734999Z'),\n" +
            " ('ETH-USDT','sell',3678.25,0.010916,'2024-12-19T08:10:00.736000Z'),\n" +
            " ('ETH-USD','sell',3678.25,0.010916,'2024-12-19T08:10:00.736000Z'),\n" +
            " ('DOGE-USDT','sell',0.36044,26.0,'2024-12-19T08:10:00.736999Z'),\n" +
            " ('DOGE-USD','sell',0.36044,26.0,'2024-12-19T08:10:00.736999Z'),\n" +
            " ('DOGE-USDT','sell',0.36044,0.050351,'2024-12-19T08:10:00.736999Z'),\n" +
            " ('DOGE-USD','sell',0.36044,0.050351,'2024-12-19T08:10:00.736999Z'),\n" +
            " ('ETH-USDC','sell',3676.0,0.255,'2024-12-19T08:10:00.743000Z'),\n" +
            " ('ETH-USDC','sell',3675.99,0.230546,'2024-12-19T08:10:00.743000Z'),\n" +
            " ('ETH-USDC','sell',3675.99,0.264415,'2024-12-19T08:10:00.743000Z'),\n" +
            " ('ETH-USDC','sell',3675.95,0.221131,'2024-12-19T08:10:00.743000Z'),\n" +
            " ('BTC-USDT','buy',101497.6,8.669E-5,'2024-12-19T08:10:00.744000Z'),\n" +
            " ('BTC-USD','buy',101497.6,8.669E-5,'2024-12-19T08:10:00.744000Z'),\n" +
            " ('ETH-USDC','sell',3675.95,0.064454,'2024-12-19T08:10:00.744999Z'),\n" +
            " ('ETH-USDC','sell',3675.95,0.064454,'2024-12-19T08:10:00.746000Z'),\n" +
            " ('ETH-USDT','sell',3678.0,0.2,'2024-12-19T08:10:00.759000Z'),\n" +
            " ('ETH-USD','sell',3678.0,0.2,'2024-12-19T08:10:00.759000Z'),\n" +
            " ('ETH-USDT','sell',3678.0,1.080001,'2024-12-19T08:10:00.772999Z'),\n" +
            " ('ETH-USD','sell',3678.0,1.080001,'2024-12-19T08:10:00.772999Z'),\n" +
            " ('ETH-USDT','buy',3678.01,0.006046,'2024-12-19T08:10:00.887000Z'),\n" +
            " ('ETH-USD','buy',3678.01,0.006046,'2024-12-19T08:10:00.887000Z'),\n" +
            " ('DOGE-USDT','sell',0.36041,13873.0,'2024-12-19T08:10:00.898000Z'),\n" +
            " ('DOGE-USD','sell',0.36041,13873.0,'2024-12-19T08:10:00.898000Z'),\n" +
            " ('SOL-USDT','sell',210.41,0.037636,'2024-12-19T08:10:00.903000Z'),\n" +
            " ('SOL-USD','sell',210.41,0.037636,'2024-12-19T08:10:00.903000Z'),\n" +
            " ('BTC-USDT','buy',101497.6,4.433E-5,'2024-12-19T08:10:00.926000Z'),\n" +
            " ('BTC-USD','buy',101497.6,4.433E-5,'2024-12-19T08:10:00.926000Z'),\n" +
            " ('BTC-USDT','sell',101497.5,1.2529056,'2024-12-19T08:10:00.932000Z'),\n" +
            " ('BTC-USD','sell',101497.5,1.2529056,'2024-12-19T08:10:00.932000Z'),\n" +
            " ('BTC-USDT','sell',101497.0,9.3655E-4,'2024-12-19T08:10:00.932000Z'),\n" +
            " ('BTC-USD','sell',101497.0,9.3655E-4,'2024-12-19T08:10:00.932000Z'),\n" +
            " ('ETH-USDC','sell',3675.95,0.204168,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.86,0.28142,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.86,0.32258,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.75,0.079,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.73,1.56E-4,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.72,0.083852,'2024-12-19T08:10:00.935000Z'),\n" +
            " ('ETH-USDC','sell',3675.72,0.064412,'2024-12-19T08:10:00.936999Z'),\n" +
            " ('ETH-USDC','sell',3675.72,0.064412,'2024-12-19T08:10:00.937999Z'),\n" +
            " ('ETH-USDT','sell',3678.0,0.2,'2024-12-19T08:10:00.950000Z'),\n" +
            " ('ETH-USD','sell',3678.0,0.2,'2024-12-19T08:10:00.950000Z');";

    @Test
    public void testPivot() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t2010\t2020\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000\t2010\t2020\n" +
                        "NL\t1005\t1065\t1158\n" +
                        "US\t8579\t8783\t9510\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotDefaultNamingRules() throws Exception {
        assertQueryAndPlan(
                "side\tBTC-USD_first_price\tBTC-USD_first_price1\n",
                "trades PIVOT (\n" +
                        "first(price),\n" +
                        "first(price)\n" +
                        "FOR symbol IN ('BTC-USD')\n" +
                        "GROUP BY side\n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "side\tBTC-USD_first_price\tBTC-USD_first_price1\n" +
                        "sell\t101502.1\t101502.1\n" +
                        "buy\t101502.2\t101502.2\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [side]\n" +
                        "  values: [first_not_null(case([first_price,NaN,symbol])),first_not_null(case([first_price1,NaN,symbol]))]\n" +
                        "    SelectedRecord\n" +
                        "        VirtualRecord\n" +
                        "          functions: [side,first_price,symbol]\n" +
                        "            Async JIT Group By workers: 1\n" +
                        "              keys: [side,symbol]\n" +
                        "              values: [first(price)]\n" +
                        "              filter: symbol in [BTC-USD]\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotDefaultNamingRules2() throws Exception {
        assertQueryAndPlan(
                "side\tBTC-USD_first_price\tBTC-USD_first_amount\n",
                "trades PIVOT (\n" +
                        "first(price),\n" +
                        "first(amount)\n" +
                        "FOR symbol IN ('BTC-USD')\n" +
                        "GROUP BY side\n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "side\tBTC-USD_first_price\tBTC-USD_first_amount\n" +
                        "buy\t101502.2\t5.775E-5\n" +
                        "sell\t101502.1\t1.4443E-4\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [side]\n" +
                        "  values: [first_not_null(case([first_price,NaN,symbol])),first_not_null(case([first_amount,NaN,symbol]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [side,symbol]\n" +
                        "      values: [first(price),first(amount)]\n" +
                        "      filter: symbol in [BTC-USD]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotImplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                "2000\t2010\t2020\n" +
                        "null\tnull\tnull\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000\t2010\t2020\n" +
                        "9584\t9848\t10668\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotImplicitGroupByWithAlias() throws Exception {
        assertQueryAndPlan(
                "2000_sum\t2010_sum\t2020_sum\n" +
                        "null\tnull\tnull\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000_sum\t2010_sum\t2020_sum\n" +
                        "9584\t9848\t10668\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotImplicitGroupByWithAliasNoAs() throws Exception {
        assertQueryAndPlan(
                "2000_sum\t2010_sum\t2020_sum\n" +
                        "null\tnull\tnull\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000_sum\t2010_sum\t2020_sum\n" +
                        "9584\t9848\t10668\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotImplicitGroupByWithOrderBy() throws Exception {
        assertQueryAndPlan(
                "2000\t2010\t2020\n" +
                        "null\tnull\tnull\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    ORDER BY \"2000\"\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000\t2010\t2020\n" +
                        "9584\t9848\t10668\n",
                true,
                true,
                false,
                "Sort\n" +
                        "  keys: [2000]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [year]\n" +
                        "          values: [sum(population)]\n" +
                        "          filter: year in [2000,2010,2020]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotOHLC() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);
            drainWalQueue();

            String pivotQuery = "trades PIVOT (\n" +
                    "first(price) as open,\n" +
                    "max(price) as high,\n" +
                    "min(price) as low,\n" +
                    "last(price) as close\n" +
                    "FOR symbol IN ('BTC-USD')\n" +
                    "GROUP BY side\n" +
                    ");";

            String result = "side\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close\n" +
                    "buy\t101502.2\t101502.2\t101497.6\t101497.6\n" +
                    "sell\t101502.1\t101502.1\t101497.0\t101497.0\n";

            assertPlanNoLeakCheck(pivotQuery, "GroupBy vectorized: false\n" +
                    "  keys: [side]\n" +
                    "  values: [first_not_null(case([open,NaN,symbol])),max(case([high,NaN,symbol])),min(case([low,NaN,symbol])),last_not_null(case([close,NaN,symbol]))]\n" +
                    "    Async JIT Group By workers: 1\n" +
                    "      keys: [side,symbol]\n" +
                    "      values: [first(price),max(price),min(price),last(price)]\n" +
                    "      filter: symbol in [BTC-USD]\n" +
                    "        PageFrame\n" +
                    "            Row forward scan\n" +
                    "            Frame forward scan on: trades\n");
            assertSql(result, pivotQuery);
        });
    }

    @Test
    public void testPivotWithAliasedAggregate() throws Exception {
        assertQueryAndPlan(
                "country\t2000_total\t2010_total\t2020_total\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as total\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000_total\t2010_total\t2020_total\n" +
                        "NL\t1005\t1065\t1158\n" +
                        "US\t8579\t8783\t9510\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([total,nullL,year])),sum(case([total,nullL,year])),sum(case([total,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithCTEAndKeyedAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlSensors);
            execute(dmlSensors);

            String query = "\n" +
                    "WITH A AS (\n" +
                    "SELECT * FROM (    \n" +
                    "select * FROM sensors \n" +
                    "PIVOT (\n" +
                    "    avg(int_value) \n" +
                    "    FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%') ELSE 'Others'\n" +
                    "    GROUP BY timestamp, vehicle_id\n" +
                    ") order by timestamp\n" +
                    ")\n" +
                    "), B AS (\n" +
                    "    select * FROM (\n" +
                    "select * FROM sensors \n" +
                    "PIVOT (\n" +
                    "    last(str_value) \n" +
                    "    FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 's%') ELSE 'Others'\n" +
                    "    GROUP BY timestamp, vehicle_id\n" +
                    ")\n" +
                    "order by timestamp\n" +
                    ") \n" +
                    ") select * from A asof join B ON (vehicle_id) LIMIT 10\n" +
                    ";";

            assertQueryNoLeakCheck("timestamp\tvehicle_id\ti009\ti000\ti002\ti004\ti008\ti003\ti007\ti005\ti006\ti001\tOthers\ttimestamp1\tvehicle_id1\ts001\ts005\ts006\ts009\ts003\ts008\ts002\ts004\ts007\ts000\tOthers1\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA003\t-364.0\t64.0\t-360.0\t694.0\t-476.0\t-248.0\t-602.0\t10.0\t778.0\t717.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA003\tval_-835\tval_705\tval_-703\tval_841\tval_-54\tval_-933\tval_263\tval_-908\tval_-393\tval_394\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA014\t-745.0\t309.0\t-804.0\t-671.0\t-721.0\t226.0\t123.0\t-497.0\t-844.0\t296.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA014\tval_-477\tval_903\tval_831\tval_-236\tval_-290\tval_-863\tval_-601\tval_76\tval_412\tval_887\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA044\t-222.0\t-218.0\t-779.0\t322.0\t-959.0\t486.0\t-80.0\t-889.0\t254.0\t-25.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA044\tval_-234\tval_-337\tval_874\tval_725\tval_-661\tval_-950\tval_596\tval_-727\tval_574\tval_-194\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA031\t-39.0\t-813.0\t86.0\t965.0\t481.0\t-170.0\t516.0\t124.0\t-925.0\t412.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA031\tval_-939\tval_590\tval_-719\tval_13\tval_697\tval_-99\tval_-587\tval_156\tval_-718\tval_-921\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA041\t-454.0\t408.0\t-164.0\t904.0\t-698.0\t-299.0\t-419.0\t941.0\t946.0\t-479.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA041\tval_337\tval_468\tval_180\tval_-783\tval_165\tval_-758\tval_248\tval_-872\tval_987\tval_-744\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA039\t412.0\t896.0\t332.0\t-171.0\t327.0\t257.0\t225.0\t-431.0\t-203.0\t-347.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA039\tval_123\tval_450\tval_199\tval_-935\tval_672\tval_616\tval_596\tval_275\tval_-320\tval_450\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA045\t850.0\t315.0\t-332.0\t948.0\t-444.0\t317.0\t-371.0\t-887.0\t-67.0\t428.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA045\tval_-527\tval_212\tval_-916\tval_-836\tval_89\tval_792\tval_-132\tval_769\tval_-464\tval_-88\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA048\t164.0\t436.0\t468.0\t-37.0\t-362.0\t238.0\t224.0\t-686.0\t537.0\t790.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA048\tval_15\tval_-839\tval_500\tval_-887\tval_-439\tval_85\tval_-592\tval_-849\tval_-431\tval_-842\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA022\t-98.0\t804.0\t370.0\t861.0\t-87.0\t90.0\t674.0\t47.0\t908.0\t88.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA022\tval_-614\tval_-367\tval_-547\tval_623\tval_-341\tval_-806\tval_-289\tval_-405\tval_0\tval_-530\t\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA012\t-200.0\t285.0\t-357.0\t412.0\t588.0\t479.0\t-31.0\t807.0\t-726.0\t416.0\tnull\t2025-01-01T00:00:00.000000Z\tAAA012\tval_-31\tval_-86\tval_-648\tval_-36\tval_-113\tval_392\tval_0\tval_815\tval_-192\tval_848\t\n",
                    query,
                    "timestamp",
                    false,
                    true,
                    false);

            assertPlanNoLeakCheck(query,
                    "Limit lo: 10 skip-over-rows: 0 limit: 10\n" +
                            "    SelectedRecord\n" +
                            "        AsOf Join Light\n" +
                            "          condition: B.vehicle_id=A.vehicle_id\n" +
                            "            Radix sort light\n" +
                            "              keys: [timestamp]\n" +
                            "                GroupBy vectorized: false\n" +
                            "                  keys: [timestamp,vehicle_id]\n" +
                            "                  values: [avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([avg,NaN,sensor_name])),avg(case([not (sensor_name in [i009,i000,i002,i004,i008,i003,i007,i005,i006,i001]),avg,null]))]\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [timestamp,vehicle_id,sensor_name]\n" +
                            "                      values: [avg(int_value)]\n" +
                            "                      filter: null\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: sensors\n" +
                            "            Radix sort light\n" +
                            "              keys: [timestamp]\n" +
                            "                GroupBy vectorized: false\n" +
                            "                  keys: [timestamp,vehicle_id]\n" +
                            "                  values: [last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([last,null,sensor_name])),last_not_null(case([not (sensor_name in [s001,s005,s006,s009,s003,s008,s002,s004,s007,s000]),last,null]))]\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [timestamp,vehicle_id,sensor_name]\n" +
                            "                      values: [last(str_value)]\n" +
                            "                      filter: null\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: sensors\n");
        });
    }

    @Test
    public void testPivotWithCTEInsideDynamicSubQuery() throws Exception {
        assertQueryAndPlan(
                "CPDH\t2010\t2017\t2018\t2022\n" +
                        "C1\tnull\t0\t20\t10\n" +
                        "C2\t30\tnull\tnull\t10\n" +
                        "C3\t80\tnull\tnull\tnull\n",
                "WITH CPB AS (\n" +
                        "SELECT 'C1' AS CPDH, 2022 AS NF, 10 AS JG\n" +
                        "UNION ALL\n" +
                        "SELECT 'C1',2018,20\n" +
                        "UNION ALL\n" +
                        "SELECT 'C1',2017,0\n" +
                        "UNION ALL\n" +
                        "SELECT 'C2',2022,10\n" +
                        "UNION ALL\n" +
                        "SELECT 'C2',2010,30\n" +
                        "UNION ALL\n" +
                        "SELECT 'C3',2010,80\n" +
                        ")\n" +
                        "SELECT * FROM CPB PIVOT (sum(jg) FOR nf IN (SELECT DISTINCT NF FROM CPB ORDER BY NF) GROUP BY CPDH ORDER BY CPDH);",
                null,
                null,
                null,
                "CPDH\t2010\t2017\t2018\t2022\n" +
                        "C1\tnull\t0\t20\t10\n" +
                        "C2\t30\tnull\tnull\t10\n" +
                        "C3\t80\tnull\tnull\tnull\n",
                true,
                true,
                false,
                "Sort light\n" +
                        "  keys: [CPDH]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [CPDH]\n" +
                        "      values: [sum(case([sum,nullL,nf])),sum(case([sum,nullL,nf])),sum(case([sum,nullL,nf])),sum(case([sum,nullL,nf]))]\n" +
                        "        GroupBy vectorized: false\n" +
                        "          keys: [CPDH,nf]\n" +
                        "          values: [sum(JG)]\n" +
                        "            Filter filter: NF in [2010,2017,2018,2022]\n" +
                        "                Union All\n" +
                        "                    Union All\n" +
                        "                        Union All\n" +
                        "                            Union All\n" +
                        "                                Union All\n" +
                        "                                    VirtualRecord\n" +
                        "                                      functions: [2022,'C1',10]\n" +
                        "                                        long_sequence count: 1\n" +
                        "                                    VirtualRecord\n" +
                        "                                      functions: [2018,'C1',20]\n" +
                        "                                        long_sequence count: 1\n" +
                        "                                VirtualRecord\n" +
                        "                                  functions: [2017,'C1',0]\n" +
                        "                                    long_sequence count: 1\n" +
                        "                            VirtualRecord\n" +
                        "                              functions: [2022,'C2',10]\n" +
                        "                                long_sequence count: 1\n" +
                        "                        VirtualRecord\n" +
                        "                          functions: [2010,'C2',30]\n" +
                        "                            long_sequence count: 1\n" +
                        "                    VirtualRecord\n" +
                        "                      functions: [2010,'C3',80]\n" +
                        "                        long_sequence count: 1\n");
    }

    @Test
    public void testPivotWithCast() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t2010\t2020\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, '2010'::int, '2020'::long)\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000\t2010\t2020\n" +
                        "NL\t1005\t1065\t1158\n" +
                        "US\t8579\t8783\t9510\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithComplexInitialStatement() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                "(cities\n" +
                        "WHERE (population % 2) = 0)\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country, name\n" +
                        ");",
                ddlCities,
                null,
                dmlCities,
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n" +
                        "NL\tAmsterdam\tnull\tnull\t1158\n" +
                        "US\tSeattle\t564\t608\t738\n" +
                        "US\tNew York City\tnull\tnull\t8772\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country,name]\n" +
                        "  values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [country,name,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: (population%2=0 and year in [2000,2010,2020])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithDynamicInList() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    "cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population)\n" +
                            "    FOR\n" +
                            "        year IN (SELECT DISTINCT year FROM cities ORDER BY year)\n" +
                            "    GROUP BY country\n" +
                            ");\n";

            assertException(query, 60, "query returned no results");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    "country\t2000\t2010\t2020\n" +
                            "NL\t1005\t1065\t1158\n" +
                            "US\t8579\t8783\t9510\n",
                    query,
                    null,
                    true,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [country]\n" +
                            "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [country,year]\n" +
                            "      values: [sum(population)]\n" +
                            "      filter: year in [2000,2010,2020]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cities\n");
        });
    }

    @Test
    public void testPivotWithDynamicInListMultipleFor() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    "cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population)\n" +
                            "    FOR\n" +
                            "        year IN (SELECT DISTINCT year FROM cities ORDER BY year)\n" +
                            "        name IN (SELECT DISTINCT name FROM cities ORDER BY name)\n" +
                            ");\n";

            assertException(query, 60, "query returned no results");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    "2000_Amsterdam\t2000_New York City\t2000_Seattle\t2010_Amsterdam\t2010_New York City\t2010_Seattle\t2020_Amsterdam\t2020_New York City\t2020_Seattle\n" +
                            "1005\t8015\t564\t1065\t8175\t608\t1158\t8772\t738\n",
                    query,
                    null,
                    false,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(case([(year=2000 and name='Amsterdam'),SUM,null])),sum(case([(year=2000 and name='New York City'),SUM,null])),sum(case([(year=2000 and name='Seattle'),SUM,null])),sum(case([(year=2010 and name='Amsterdam'),SUM,null])),sum(case([(year=2010 and name='New York City'),SUM,null])),sum(case([(year=2010 and name='Seattle'),SUM,null])),sum(case([(year=2020 and name='Amsterdam'),SUM,null])),sum(case([(year=2020 and name='New York City'),SUM,null])),sum(case([(year=2020 and name='Seattle'),SUM,null]))]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [year,name]\n" +
                            "      values: [sum(population)]\n" +
                            "      filter: (year in [2000,2010,2020] and name in [Amsterdam,New York City,Seattle])\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cities\n");
        });
    }

    @Test
    public void testPivotWithDynamicInListMultipleForAndMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    "cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population),\n" +
                            "    AVG(population)\n" +
                            "    FOR\n" +
                            "        year IN (SELECT DISTINCT year FROM cities ORDER BY year)\n" +
                            "        name IN (SELECT DISTINCT name FROM cities ORDER BY name)\n" +
                            ");\n";

            assertException(query, 81, "query returned no results");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    "2000_Amsterdam_SUM\t2000_Amsterdam_AVG\t2000_New York City_SUM\t2000_New York City_AVG\t2000_Seattle_SUM\t2000_Seattle_AVG\t2010_Amsterdam_SUM\t2010_Amsterdam_AVG\t2010_New York City_SUM\t2010_New York City_AVG\t2010_Seattle_SUM\t2010_Seattle_AVG\t2020_Amsterdam_SUM\t2020_Amsterdam_AVG\t2020_New York City_SUM\t2020_New York City_AVG\t2020_Seattle_SUM\t2020_Seattle_AVG\n" +
                            "1005\t1005.0\t8015\t8015.0\t564\t564.0\t1065\t1065.0\t8175\t8175.0\t608\t608.0\t1158\t1158.0\t8772\t8772.0\t738\t738.0\n",
                    query,
                    null,
                    false,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(case([(year=2000 and name='Amsterdam'),SUM,null])),avg(case([(year=2000 and name='Amsterdam'),AVG,null])),sum(case([(year=2000 and name='New York City'),SUM,null])),avg(case([(year=2000 and name='New York City'),AVG,null])),sum(case([(year=2000 and name='Seattle'),SUM,null])),avg(case([(year=2000 and name='Seattle'),AVG,null])),sum(case([(year=2010 and name='Amsterdam'),SUM,null])),avg(case([(year=2010 and name='Amsterdam'),AVG,null])),sum(case([(year=2010 and name='New York City'),SUM,null])),avg(case([(year=2010 and name='New York City'),AVG,null])),sum(case([(year=2010 and name='Seattle'),SUM,null])),avg(case([(year=2010 and name='Seattle'),AVG,null])),sum(case([(year=2020 and name='Amsterdam'),SUM,null])),avg(case([(year=2020 and name='Amsterdam'),AVG,null])),sum(case([(year=2020 and name='New York City'),SUM,null])),avg(case([(year=2020 and name='New York City'),AVG,null])),sum(case([(year=2020 and name='Seattle'),SUM,null])),avg(case([(year=2020 and name='Seattle'),AVG,null]))]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [year,name]\n" +
                            "      values: [sum(population),avg(population)]\n" +
                            "      filter: (year in [2000,2010,2020] and name in [Amsterdam,New York City,Seattle])\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cities\n");
        });
    }

    @Test
    public void testPivotWithElse() throws Exception {
        assertQueryAndPlan(
                "country\t2000\tother_years\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000) ELSE other_years\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000\tother_years\n" +
                        "NL\t1005\t2223\n" +
                        "US\t8579\t18293\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([not (year in [2000]),SUM,null]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: null\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithElseMultipleFor() throws Exception {
        assertQueryAndPlan(
                "country\t2000_NL\t2000_USA\tother_years_NL\tother_years_USA\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000) ELSE other_years\n" +
                        "        country IN ('NL') ELSE USA\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000_NL\t2000_USA\tother_years_NL\tother_years_USA\n" +
                        "NL\t1005\tnull\t2223\tnull\n" +
                        "US\tnull\t8579\tnull\t18293\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),SUM,null])),sum(case([(year=2000 and not (country in [NL])),SUM,null])),sum(case([(not (year in [2000]) and country='NL'),SUM,null])),sum(case([(not (year in [2000]) and not (country in [NL])),SUM,null]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: null\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithElseMultipleForAndAggregates() throws Exception {
        assertQueryAndPlan(
                "country\t2000_NL_SUM\t2000_NL_AVG\t2000_USA_SUM\t2000_USA_AVG\tother_years_NL_SUM\tother_years_NL_AVG\tother_years_USA_SUM\tother_years_USA_AVG\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population),\n" +
                        "    AVG(population)" +
                        "    FOR\n" +
                        "        year IN (2000) ELSE other_years\n" +
                        "        country IN ('NL') ELSE USA\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000_NL_SUM\t2000_NL_AVG\t2000_USA_SUM\t2000_USA_AVG\tother_years_NL_SUM\tother_years_NL_AVG\tother_years_USA_SUM\tother_years_USA_AVG\n" +
                        "NL\t1005\t1005.0\tnull\tnull\t2223\t1111.5\tnull\tnull\n" +
                        "US\tnull\tnull\t8579\t4289.5\tnull\tnull\t18293\t4573.25\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),SUM,null])),avg(case([(year=2000 and country='NL'),AVG,null])),sum(case([(year=2000 and not (country in [NL])),SUM,null])),avg(case([(year=2000 and not (country in [NL])),AVG,null])),sum(case([(not (year in [2000]) and country='NL'),SUM,null])),avg(case([(not (year in [2000]) and country='NL'),AVG,null])),sum(case([(not (year in [2000]) and not (country in [NL])),SUM,null])),avg(case([(not (year in [2000]) and not (country in [NL])),AVG,null]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population),avg(population)]\n" +
                        "      filter: null\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithElseMultipleForAndAggregatesOrderedAndLimited() throws Exception {
        assertQueryAndPlan(
                "country\t2000_NL_SUM\t2000_NL_AVG\t2000_USA_SUM\t2000_USA_AVG\tother_years_NL_SUM\tother_years_NL_AVG\tother_years_USA_SUM\tother_years_USA_AVG\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population),\n" +
                        "    AVG(population)" +
                        "    FOR\n" +
                        "        year IN (2000) ELSE other_years\n" +
                        "        country IN ('NL') ELSE USA\n" +
                        "    GROUP BY country\n" +
                        "    ORDER BY country DESC\n" +
                        "    LIMIT 1\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000_NL_SUM\t2000_NL_AVG\t2000_USA_SUM\t2000_USA_AVG\tother_years_NL_SUM\tother_years_NL_AVG\tother_years_USA_SUM\tother_years_USA_AVG\n" +
                        "US\tnull\tnull\t8579\t4289.5\tnull\tnull\t18293\t4573.25\n",
                true,
                true,
                false,
                "Sort light lo: 1\n" +
                        "  keys: [country desc]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [country]\n" +
                        "      values: [sum(case([(year=2000 and country='NL'),SUM,null])),avg(case([(year=2000 and country='NL'),AVG,null])),sum(case([(year=2000 and not (country in [NL])),SUM,null])),avg(case([(year=2000 and not (country in [NL])),AVG,null])),sum(case([(not (year in [2000]) and country='NL'),SUM,null])),avg(case([(not (year in [2000]) and country='NL'),AVG,null])),sum(case([(not (year in [2000]) and not (country in [NL])),SUM,null])),avg(case([(not (year in [2000]) and not (country in [NL])),AVG,null]))]\n" +
                        "        Async Group By workers: 1\n" +
                        "          keys: [country,year]\n" +
                        "          values: [sum(population),avg(population)]\n" +
                        "          filter: null\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithForAliases() throws Exception {
        assertQueryAndPlan(
                "country\tD1\tD2\tD3\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000 as D1, 2010 D2, 2020 as D3)\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\tD1\tD2\tD3\n" +
                        "NL\t1005\t1065\t1158\n" +
                        "US\t8579\t8783\t9510\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithGroupByAndLimit() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country, name\n" +
                        "    LIMIT 1\n" +
                        ");",
                ddlCities,
                null,
                dmlCities,
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n" +
                        "NL\tAmsterdam\t1005\t1065\t1158\n",
                true,
                true,
                false,
                "Limit lo: 1 skip-over-rows: 0 limit: 1\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [country,name]\n" +
                        "      values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [country,name,year]\n" +
                        "          values: [sum(population)]\n" +
                        "          filter: year in [2000,2010,2020]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithGroupByAndOrderBy() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country, name\n" +
                        "    ORDER BY \"2000_sum\"\n" +
                        ");",
                ddlCities,
                null,
                dmlCities,
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n" +
                        "US\tSeattle\t564\t608\t738\n" +
                        "NL\tAmsterdam\t1005\t1065\t1158\n" +
                        "US\tNew York City\t8015\t8175\t8772\n",
                true,
                true,
                false,
                "Radix sort light\n" +
                        "  keys: [2000_sum]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [country,name]\n" +
                        "      values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [country,name,year]\n" +
                        "          values: [sum(population)]\n" +
                        "          filter: year in [2000,2010,2020]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithGroupByAndOrderByAndLimit() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                "SELECT *\n" +
                        "FROM cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as sum\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country, name\n" +
                        "    ORDER BY \"2000_sum\"\n" +
                        "    LIMIT 1\n" +
                        ");",
                ddlCities,
                null,
                dmlCities,
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n" +
                        "US\tSeattle\t564\t608\t738\n",
                true,
                true,
                false,
                "Long top K lo: 1\n" +
                        "  keys: [2000_sum asc]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [country,name]\n" +
                        "      values: [sum(case([sum,nullL,year])),sum(case([sum,nullL,year])),sum(case([sum,nullL,year]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [country,name,year]\n" +
                        "          values: [sum(population)]\n" +
                        "          filter: year in [2000,2010,2020]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithLatestOnGroupBy() throws Exception {
        assertQueryAndPlan(
                "side\tETH-USDT\tBTC-USDT\tDOGE-USDT\n",
                "(select side, symbol, last(price) as price from trades group by side, symbol)\n" +
                        "  pivot (\n" +
                        "    last(price)\n" +
                        "    FOR \"symbol\" IN ('ETH-USDT', 'BTC-USDT', 'DOGE-USDT')\n" +
                        "    GROUP BY side\n" +
                        "    ORDER BY side\n" +
                        "  );",
                ddlTrades,
                null,
                dmlTrades,
                "side\tETH-USDT\tBTC-USDT\tDOGE-USDT\n" +
                        "buy\t3678.01\t101497.6\t0.36047\n" +
                        "sell\t3678.0\t101497.0\t0.36041\n",
                true,
                true,
                false,
                "Sort light\n" +
                        "  keys: [side]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [side]\n" +
                        "      values: [last_not_null(case([last,NaN,symbol])),last_not_null(case([last,NaN,symbol])),last_not_null(case([last,NaN,symbol]))]\n" +
                        "        GroupBy vectorized: false\n" +
                        "          keys: [side,symbol]\n" +
                        "          values: [last(price)]\n" +
                        "            Async JIT Group By workers: 1\n" +
                        "              keys: [side,symbol]\n" +
                        "              values: [last(price)]\n" +
                        "              filter: symbol in [ETH-USDT,BTC-USDT,DOGE-USDT]\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithMultipleAggregates() throws Exception {
        assertQueryAndPlan(
                "country\t2000_SUM\t2000_AVG\t2010_SUM\t2010_AVG\t2020_SUM\t2020_AVG\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population),\n" +
                        "    AVG(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000_SUM\t2000_AVG\t2010_SUM\t2010_AVG\t2020_SUM\t2020_AVG\n" +
                        "NL\t1005\t1005.0\t1065\t1065.0\t1158\t1158.0\n" +
                        "US\t8579\t4289.5\t8783\t4391.5\t9510\t4755.0\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country]\n" +
                        "  values: [sum(case([SUM,nullL,year])),avg(case([AVG,NaN,year])),sum(case([SUM,nullL,year])),avg(case([AVG,NaN,year])),sum(case([SUM,nullL,year])),avg(case([AVG,NaN,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,year]\n" +
                        "      values: [sum(population),avg(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n"
        );
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesExplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                "name\t2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as total,\n" +
                        "    COUNT(population) as count\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010)\n" +
                        "        country IN ('NL', 'US')\n" +
                        "    GROUP BY name\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "name\t2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count\n" +
                        "Amsterdam\t1005\t1\tnull\t0\t1065\t1\tnull\t0\n" +
                        "Seattle\tnull\t0\t564\t1\tnull\t0\t608\t1\n" +
                        "New York City\tnull\t0\t8015\t1\tnull\t0\t8175\t1\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [name]\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),sum(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),sum(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),sum(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [name,year,country]\n" +
                        "      values: [sum(population),count(population)]\n" +
                        "      filter: (year in [2000,2010] and country in [NL,US])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesExplicitGroupByWithForAliases() throws Exception {
        assertQueryAndPlan(
                "name\t2K00_Netherlands_total\t2K00_Netherlands_count\t2K00_United States_total\t2K00_United States_count\t2K10_Netherlands_total\t2K10_Netherlands_count\t2K10_United States_total\t2K10_United States_count\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as total,\n" +
                        "    COUNT(population) as count\n" +
                        "    FOR\n" +
                        "        year IN (2000 AS '2K00', 2010 AS '2K10')\n" +
                        "        country IN ('NL' AS Netherlands, 'US' AS 'United States')\n" +
                        "    GROUP BY name\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "name\t2K00_Netherlands_total\t2K00_Netherlands_count\t2K00_United States_total\t2K00_United States_count\t2K10_Netherlands_total\t2K10_Netherlands_count\t2K10_United States_total\t2K10_United States_count\n" +
                        "Amsterdam\t1005\t1\tnull\t0\t1065\t1\tnull\t0\n" +
                        "Seattle\tnull\t0\t564\t1\tnull\t0\t608\t1\n" +
                        "New York City\tnull\t0\t8015\t1\tnull\t0\t8175\t1\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [name]\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),sum(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),sum(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),sum(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [name,year,country]\n" +
                        "      values: [sum(population),count(population)]\n" +
                        "      filter: (year in [2000,2010] and country in [NL,US])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesImplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                "2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count\n" +
                        "null\tnull\tnull\tnull\tnull\tnull\tnull\tnull\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population) as total,\n" +
                        "    COUNT(population) as count\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010)\n" +
                        "        country IN ('NL', 'US')\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count\n" +
                        "1005\t1\t8579\t2\t1065\t1\t8783\t2\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),sum(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),sum(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),sum(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [year,country]\n" +
                        "      values: [sum(population),count(population)]\n" +
                        "      filter: (year in [2000,2010] and country in [NL,US])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithMultipleForExprs() throws Exception {
        assertQueryAndPlan(
                "name\t2000_NL\t2000_US\t2010_NL\t2010_US\t2020_NL\t2020_US\n",
                "cities PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "        country in ('NL', 'US')\n" +
                        "    GROUP BY name\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "name\t2000_NL\t2000_US\t2010_NL\t2010_US\t2020_NL\t2020_US\n" +
                        "Amsterdam\t1005\tnull\t1065\tnull\t1158\tnull\n" +
                        "Seattle\tnull\t564\tnull\t608\tnull\t738\n" +
                        "New York City\tnull\t8015\tnull\t8175\tnull\t8772\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [name]\n" +
                        "  values: [sum(case([(year=2000 and country='NL'),SUM,null])),sum(case([(year=2000 and country='US'),SUM,null])),sum(case([(year=2010 and country='NL'),SUM,null])),sum(case([(year=2010 and country='US'),SUM,null])),sum(case([(year=2020 and country='NL'),SUM,null])),sum(case([(year=2020 and country='US'),SUM,null]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [name,year,country]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: (year in [2000,2010,2020] and country in [NL,US])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n"
        );
    }

    @Test
    public void testPivotWithMultipleForExprsAndMultipleAggregates() throws Exception {
        assertQueryAndPlan(
                "2000_Amsterdam_NL_SUM\t2000_Amsterdam_NL_COUNT\t2000_Amsterdam_US_SUM\t2000_Amsterdam_US_COUNT\t2000_Seattle_NL_SUM\t2000_Seattle_NL_COUNT\t2000_Seattle_US_SUM\t2000_Seattle_US_COUNT\t2000_New York City_NL_SUM\t2000_New York City_NL_COUNT\t2000_New York City_US_SUM\t2000_New York City_US_COUNT\t2010_Amsterdam_NL_SUM\t2010_Amsterdam_NL_COUNT\t2010_Amsterdam_US_SUM\t2010_Amsterdam_US_COUNT\t2010_Seattle_NL_SUM\t2010_Seattle_NL_COUNT\t2010_Seattle_US_SUM\t2010_Seattle_US_COUNT\t2010_New York City_NL_SUM\t2010_New York City_NL_COUNT\t2010_New York City_US_SUM\t2010_New York City_US_COUNT\t2020_Amsterdam_NL_SUM\t2020_Amsterdam_NL_COUNT\t2020_Amsterdam_US_SUM\t2020_Amsterdam_US_COUNT\t2020_Seattle_NL_SUM\t2020_Seattle_NL_COUNT\t2020_Seattle_US_SUM\t2020_Seattle_US_COUNT\t2020_New York City_NL_SUM\t2020_New York City_NL_COUNT\t2020_New York City_US_SUM\t2020_New York City_US_COUNT\n" +
                        "null\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population),\n" +
                        "    COUNT(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "        name IN ( 'Amsterdam', 'Seattle', 'New York City')\n" +
                        "        country in ('NL', 'US')\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000_Amsterdam_NL_SUM\t2000_Amsterdam_NL_COUNT\t2000_Amsterdam_US_SUM\t2000_Amsterdam_US_COUNT\t2000_Seattle_NL_SUM\t2000_Seattle_NL_COUNT\t2000_Seattle_US_SUM\t2000_Seattle_US_COUNT\t2000_New York City_NL_SUM\t2000_New York City_NL_COUNT\t2000_New York City_US_SUM\t2000_New York City_US_COUNT\t2010_Amsterdam_NL_SUM\t2010_Amsterdam_NL_COUNT\t2010_Amsterdam_US_SUM\t2010_Amsterdam_US_COUNT\t2010_Seattle_NL_SUM\t2010_Seattle_NL_COUNT\t2010_Seattle_US_SUM\t2010_Seattle_US_COUNT\t2010_New York City_NL_SUM\t2010_New York City_NL_COUNT\t2010_New York City_US_SUM\t2010_New York City_US_COUNT\t2020_Amsterdam_NL_SUM\t2020_Amsterdam_NL_COUNT\t2020_Amsterdam_US_SUM\t2020_Amsterdam_US_COUNT\t2020_Seattle_NL_SUM\t2020_Seattle_NL_COUNT\t2020_Seattle_US_SUM\t2020_Seattle_US_COUNT\t2020_New York City_NL_SUM\t2020_New York City_NL_COUNT\t2020_New York City_US_SUM\t2020_New York City_US_COUNT\n" +
                        "1005\t1\tnull\t0\tnull\t0\t564\t1\tnull\t0\t8015\t1\t1065\t1\tnull\t0\tnull\t0\t608\t1\tnull\t0\t8175\t1\t1158\t1\tnull\t0\tnull\t0\t738\t1\tnull\t0\t8772\t1\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([(year=2000 and name='Amsterdam' and country='NL'),SUM,null])),sum(case([(year=2000 and name='Amsterdam' and country='NL'),COUNT,0])),sum(case([(year=2000 and name='Amsterdam' and country='US'),SUM,null])),sum(case([(year=2000 and name='Amsterdam' and country='US'),COUNT,0])),sum(case([(year=2000 and name='Seattle' and country='NL'),SUM,null])),sum(case([(year=2000 and name='Seattle' and country='NL'),COUNT,0])),sum(case([(year=2000 and name='Seattle' and country='US'),SUM,null])),sum(case([(year=2000 and name='Seattle' and country='US'),COUNT,0])),sum(case([(year=2000 and name='New York City' and country='NL'),SUM,null])),sum(case([(year=2000 and name='New York City' and country='NL'),COUNT,0])),sum(case([(year=2000 and name='New York City' and country='US'),SUM,null])),sum(case([(year=2000 and name='New York City' and country='US'),COUNT,0])),sum(case([(year=2010 and name='Amsterdam' and country='NL'),SUM,null])),sum(case([(year=2010 and name='Amsterdam' and country='NL'),COUNT,0])),sum(case([(year=2010 and name='Amsterdam' and country='US'),SUM,null])),sum(case([(year=2010 and name='Amsterdam' and country='US'),COUNT,0])),sum(case([(year=2010 and name='Seattle' and country='NL'),SUM,null])),sum(case([(year=2010 and name='Seattle' and country='NL'),COUNT,0])),sum(case([(year=2010 and name='Seattle' and country='US'),SUM,null])),sum(case([(year=2010 and name='Seattle' and country='US'),COUNT,0])),sum(case([(year=2010 and name='New York City' and country='NL'),SUM,null])),sum(case([(year=2010 and name='New York City' and country='NL'),COUNT,0])),sum(case([(year=2010 and name='New York City' and country='US'),SUM,null])),sum(case([(year=2010 and name='New York City' and country='US'),COUNT,0])),sum(case([(year=2020 and name='Amsterdam' and country='NL'),SUM,null])),sum(case([(year=2020 and name='Amsterdam' and country='NL'),COUNT,0])),sum(case([(year=2020 and name='Amsterdam' and country='US'),SUM,null])),sum(case([(year=2020 and name='Amsterdam' and country='US'),COUNT,0])),sum(case([(year=2020 and name='Seattle' and country='NL'),SUM,null])),sum(case([(year=2020 and name='Seattle' and country='NL'),COUNT,0])),sum(case([(year=2020 and name='Seattle' and country='US'),SUM,null])),sum(case([(year=2020 and name='Seattle' and country='US'),COUNT,0])),sum(case([(year=2020 and name='New York City' and country='NL'),SUM,null])),sum(case([(year=2020 and name='New York City' and country='NL'),COUNT,0])),sum(case([(year=2020 and name='New York City' and country='US'),SUM,null])),sum(case([(year=2020 and name='New York City' and country='US'),COUNT,0]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [year,name,country]\n" +
                        "      values: [sum(population),count(population)]\n" +
                        "      filter: (year in [2000,2010,2020] and name in [Amsterdam,Seattle,New York City] and country in [NL,US])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n"
        );
    }

    @Test
    public void testPivotWithMultipleGroupBy() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000\t2010\t2020\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "        GROUP BY country, name\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\tname\t2000\t2010\t2020\n" +
                        "NL\tAmsterdam\t1005\t1065\t1158\n" +
                        "US\tSeattle\t564\t608\t738\n" +
                        "US\tNew York City\t8015\t8175\t8772\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [country,name]\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [country,name,year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n"
        );
    }

    @Test
    public void testPivotWithNestedCast() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);
            assertException("cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, ('2010'::int)::long, '2020'::long)\n" +
                            "    GROUP BY country\n" +
                            ");\n",
                    85,
                    "only single level of CAST is supported by PIVOT");
        });
    }

    @Test
    public void testPivotWithNonDistinctQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);

            String query = "SELECT *\n" +
                    " FROM (\n" +
                    "SELECT * FROM (\n" +
                    "     SELECT timestamp, symbol, side, AVG(price) price, AVG(amount) amount FROM trades SAMPLE BY 100T\n" +
                    ")\n" +
                    "PIVOT (\n" +
                    "    sum(price)\n" +
                    "    FOR symbol IN (select symbol from trades ORDER BY symbol LIMIT 10)\n" +
                    "        side IN ('buy', 'sell')\n" +
                    "    GROUP BY timestamp\n" +
                    "    ORDER BY timestamp" +
                    ")\n" +
                    ")";

            assertQueryNoLeakCheck("timestamp\tADA-USD_buy\tADA-USD_sell\tADA-USD_buy0\tADA-USD_sell0\tADA-USD_buy1\tADA-USD_sell1\tADA-USDT_buy\tADA-USDT_sell\tADA-USDT_buy0\tADA-USDT_sell0\tADA-USDT_buy1\tADA-USDT_sell1\tBTC-USD_buy\tBTC-USD_sell\tBTC-USD_buy0\tBTC-USD_sell0\tBTC-USD_buy1\tBTC-USD_sell1\tBTC-USD_buy2\tBTC-USD_sell2\n" +
                            "2024-12-19T08:10:00.000000Z\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\n" +
                            "2024-12-19T08:10:00.100000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101502.2\t101502.1\t101502.2\t101502.1\t101502.2\t101502.1\t101502.2\t101502.1\n" +
                            "2024-12-19T08:10:00.200000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\n" +
                            "2024-12-19T08:10:00.400000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\n" +
                            "2024-12-19T08:10:00.500000Z\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\t0.9716\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\n" +
                            "2024-12-19T08:10:00.600000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\t101502.2\tnull\n" +
                            "2024-12-19T08:10:00.700000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101500.66666666667\t101500.57777777778\t101500.66666666667\t101500.57777777778\t101500.66666666667\t101500.57777777778\t101500.66666666667\t101500.57777777778\n" +
                            "2024-12-19T08:10:00.900000Z\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t101497.6\t101497.25\t101497.6\t101497.25\t101497.6\t101497.25\t101497.6\t101497.25\n",
                    query,
                    "timestamp",
                    true,
                    true,
                    false);


            try {
                assertPlanNoLeakCheck(query,
                        "VirtualRecord\n" +
                                "  functions: [timestamp,ADA-USD_buy,ADA-USD_sell,ADA-USD_buy,ADA-USD_sell,ADA-USD_buy,ADA-USD_sell,ADA-USDT_buy,ADA-USDT_sell,ADA-USDT_buy,ADA-USDT_sell,ADA-USDT_buy,ADA-USDT_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell]\n" +
                                "    Radix sort light\n" +
                                "      keys: [timestamp]\n" +
                                "        GroupBy vectorized: false\n" +
                                "          keys: [timestamp]\n" +
                                "          values: [sum(case([(symbol='ADA-USD' and side='buy'),sum,null])),sum(case([(symbol='ADA-USD' and side='sell'),sum,null])),sum(case([(symbol='ADA-USDT' and side='buy'),sum,null])),sum(case([(symbol='ADA-USDT' and side='sell'),sum,null])),sum(case([(symbol='BTC-USD' and side='buy'),sum,null])),sum(case([(symbol='BTC-USD' and side='sell'),sum,null]))]\n" +
                                "            GroupBy vectorized: false\n" +
                                "              keys: [timestamp,symbol,side]\n" +
                                "              values: [sum(price)]\n" +
                                "                Async Group JIT By workers: 1\n" +
                                "                  keys: [timestamp,symbol,side]\n" +
                                "                  values: [avg(price)]\n" +
                                "                  filter: (symbol in [ADA-USD,ADA-USDT,BTC-USD] and side in [buy,sell])\n" +
                                "                    PageFrame\n" +
                                "                        Row forward scan\n" +
                                "                        Frame forward scan on: trades\n");
            } catch (AssertionError ignore) {
                assertPlanNoLeakCheck(query,
                        "VirtualRecord\n" +
                                "  functions: [timestamp,ADA-USD_buy,ADA-USD_sell,ADA-USD_buy,ADA-USD_sell,ADA-USD_buy,ADA-USD_sell,ADA-USDT_buy,ADA-USDT_sell,ADA-USDT_buy,ADA-USDT_sell,ADA-USDT_buy,ADA-USDT_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell,BTC-USD_buy,BTC-USD_sell]\n" +
                                "    Radix sort light\n" +
                                "      keys: [timestamp]\n" +
                                "        GroupBy vectorized: false\n" +
                                "          keys: [timestamp]\n" +
                                "          values: [sum(case([(symbol='ADA-USD' and side='buy'),sum,null])),sum(case([(symbol='ADA-USD' and side='sell'),sum,null])),sum(case([(symbol='ADA-USDT' and side='buy'),sum,null])),sum(case([(symbol='ADA-USDT' and side='sell'),sum,null])),sum(case([(symbol='BTC-USD' and side='buy'),sum,null])),sum(case([(symbol='BTC-USD' and side='sell'),sum,null]))]\n" +
                                "            GroupBy vectorized: false\n" +
                                "              keys: [timestamp,symbol,side]\n" +
                                "              values: [sum(price)]\n" +
                                "                Async Group By workers: 1\n" +
                                "                  keys: [timestamp,symbol,side]\n" +
                                "                  values: [avg(price)]\n" +
                                "                  filter: (symbol in [ADA-USD,ADA-USDT,BTC-USD] and side in [buy,sell])\n" +
                                "                    PageFrame\n" +
                                "                        Row forward scan\n" +
                                "                        Frame forward scan on: trades\n");
            }

        });
    }

    @Test
    public void testPivotWithOrderBy() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t2010\t2020\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        "    GROUP BY country\n" +
                        "    ORDER BY \"2000\"\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "country\t2000\t2010\t2020\n" +
                        "NL\t1005\t1065\t1158\n" +
                        "US\t8579\t8783\t9510\n",
                true,
                true,
                false,
                "Radix sort light\n" +
                        "  keys: [2000]\n" +
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
    public void testPivotWithOrderByNotPresentInForOrGroupBy() throws Exception {
        /*
            If the column does not appear in the FOR list or GROUP BY, it won't be in the output columns.
         */
        assertMemoryLeak(() -> {
            execute(ddlMonthlySales);
            execute(dmlMonthlySales);

            assertException("monthly_sales \n" +
                            "PIVOT (\n" +
                            "  SUM(amount) \n" +
                            "  FOR MONTH IN ('JAN' AS '1', 'FEB' AS '2', 'MAR' AS '3') ELSE '4' \n" +
                            "  ORDER BY EMPID\n" +
                            ");",
                    117,
                    "ORDER BY column is not present in select fields [name=EMPID]");
        });
    }

    @Test
    public void testPivotWithSampleBy() throws Exception {
        assertQueryAndPlan(
                "symbol\tbuy_price\tsell_price\n",
                "(\n" +
                        "  SELECT timestamp, symbol, side, last(price)\n" +
                        "  FROM trades\n" +
                        "  SAMPLE BY 1d\n" +
                        ") PIVOT (\n" +
                        "  sum(last) as price\n" +
                        "  FOR side in ('buy', 'sell')\n" +
                        "  GROUP BY symbol\n" +
                        ");\n",
                ddlTrades,
                null,
                dmlTrades,
                "symbol\tbuy_price\tsell_price\n" +
                        "ETH-USD\t3678.01\t3678.0\n" +
                        "DOGE-USD\t0.36047\t0.36041\n" +
                        "SOL-USDT\tnull\t210.41\n" +
                        "ADA-USD\tnull\t0.9716\n" +
                        "SOL-USD\tnull\t210.41\n" +
                        "BTC-USD\t101497.6\t101497.0\n" +
                        "ETH-USDC\tnull\t3675.72\n" +
                        "ETH-USDT\t3678.01\t3678.0\n" +
                        "BTC-USDT\t101497.6\t101497.0\n" +
                        "USDT-USDC\t0.9994\tnull\n" +
                        "DOGE-USDT\t0.36047\t0.36041\n" +
                        "ADA-USDT\tnull\t0.9716\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [symbol]\n" +
                        "  values: [sum(case([price,NaN,side])),sum(case([price,NaN,side]))]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [symbol,side]\n" +
                        "      values: [sum(last)]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [symbol,side,timestamp]\n" +
                        "          values: [last(price)]\n" +
                        "          filter: side in [buy,sell]\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTimestampGrouping() throws Exception {
        assertQueryAndPlan(
                "2000\t2010\t2020\n" +
                        "null\tnull\tnull\n",
                "cities\n" +
                        "PIVOT (\n" +
                        "    SUM(population)\n" +
                        "    FOR\n" +
                        "        year IN (2000, 2010, 2020)\n" +
                        ");\n",
                ddlCities,
                null,
                dmlCities,
                "2000\t2010\t2020\n" +
                        "9584\t9848\t10668\n",
                false,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  values: [sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year])),sum(case([SUM,nullL,year]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [year]\n" +
                        "      values: [sum(population)]\n" +
                        "      filter: year in [2000,2010,2020]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: cities\n");
    }

    @Test
    public void testPivotWithTradesData() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                "(select * from trades where symbol in 'ETH-USDT')\n" +
                        "  pivot (\n" +
                        "    sum(price)\n" +
                        "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                        "        side in ('buy', 'sell')\n" +
                        "    GROUP BY timestamp\n" +
                        "  );",
                ddlTrades,
                null,
                dmlTrades,
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                        "2024-12-19T08:10:00.700999Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.736000Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.759000Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.772999Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.887000Z\t3678.01\tnull\n" +
                        "2024-12-19T08:10:00.950000Z\tnull\t3678.0\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [timestamp]\n" +
                        "  values: [sum(case([(symbol='ETH-USDT' and side='buy'),sum,null])),sum(case([(symbol='ETH-USDT' and side='sell'),sum,null]))]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [timestamp,symbol,side]\n" +
                        "      values: [sum(price)]\n" +
                        "      filter: (symbol in [ETH-USDT] and symbol in [ETH-USDT] and side in [buy,sell])\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTradesDataAndLimit() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                "trades\n" +
                        "  PIVOT (\n" +
                        "    sum(price)\n" +
                        "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                        "        side in ('buy', 'sell')\n" +
                        "    GROUP BY timestamp\n" +
                        "    LIMIT 3" +
                        "  );",
                ddlTrades,
                null,
                dmlTrades,
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                        "2024-12-19T08:10:00.700999Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.736000Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.759000Z\tnull\t3678.0\n",
                true,
                true,
                false,
                "Limit lo: 3 skip-over-rows: 0 limit: 3\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp]\n" +
                        "      values: [sum(case([(symbol='ETH-USDT' and side='buy'),sum,null])),sum(case([(symbol='ETH-USDT' and side='sell'),sum,null]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [timestamp,symbol,side]\n" +
                        "          values: [sum(price)]\n" +
                        "          filter: (symbol in [ETH-USDT] and side in [buy,sell])\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTradesDataAndOrderByAsc() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                "trades\n" +
                        "  PIVOT (\n" +
                        "    sum(price)\n" +
                        "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                        "        side in ('buy', 'sell')\n" +
                        "    GROUP BY timestamp\n" +
                        "    ORDER BY timestamp ASC\n" +
                        "  );",
                ddlTrades,
                "timestamp###ASC",
                dmlTrades,
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                        "2024-12-19T08:10:00.700999Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.736000Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.759000Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.772999Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.887000Z\t3678.01\tnull\n" +
                        "2024-12-19T08:10:00.950000Z\tnull\t3678.0\n",
                true,
                true,
                false,
                "Radix sort light\n" +
                        "  keys: [timestamp]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp]\n" +
                        "      values: [sum(case([(symbol='ETH-USDT' and side='buy'),sum,null])),sum(case([(symbol='ETH-USDT' and side='sell'),sum,null]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [timestamp,symbol,side]\n" +
                        "          values: [sum(price)]\n" +
                        "          filter: (symbol in [ETH-USDT] and side in [buy,sell])\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: trades\n"
        );
    }

    @Test
    public void testPivotWithTradesDataAndOrderByDesc() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                "trades\n" +
                        "  PIVOT (\n" +
                        "    sum(price)\n" +
                        "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                        "        side in ('buy', 'sell')\n" +
                        "    GROUP BY timestamp\n" +
                        "    ORDER BY timestamp DESC\n" +
                        "  );",
                ddlTrades,
                "timestamp###DESC",
                dmlTrades,
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                        "2024-12-19T08:10:00.950000Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.887000Z\t3678.01\tnull\n" +
                        "2024-12-19T08:10:00.772999Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.759000Z\tnull\t3678.0\n" +
                        "2024-12-19T08:10:00.736000Z\tnull\t3678.25\n" +
                        "2024-12-19T08:10:00.700999Z\tnull\t3678.25\n",
                true,
                true,
                false,
                "Radix sort light\n" +
                        "  keys: [timestamp desc]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp]\n" +
                        "      values: [sum(case([(symbol='ETH-USDT' and side='buy'),sum,null])),sum(case([(symbol='ETH-USDT' and side='sell'),sum,null]))]\n" +
                        "        Async JIT Group By workers: 1\n" +
                        "          keys: [timestamp,symbol,side]\n" +
                        "          values: [sum(price)]\n" +
                        "          filter: (symbol in [ETH-USDT] and side in [buy,sell])\n" +
                        "            PageFrame\n" +
                        "                Row backward scan\n" +
                        "                Frame backward scan on: trades\n"
        );
    }

    @Test
    public void
    testPivotWithTradesDataAndOrderByPositional() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);
            drainWalQueue();

            String pivotQuery =
                    "  trades PIVOT (\n" +
                            "    sum(price)\n" +
                            "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                            "        side in ('buy', 'sell')\n" +
                            "    GROUP BY timestamp\n" +
                            "    ORDER BY timestamp ASC\n" +
                            "  );";

            assertQuery("timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                            "2024-12-19T08:10:00.700999Z\tnull\t3678.25\n" +
                            "2024-12-19T08:10:00.736000Z\tnull\t3678.25\n" +
                            "2024-12-19T08:10:00.759000Z\tnull\t3678.0\n" +
                            "2024-12-19T08:10:00.772999Z\tnull\t3678.0\n" +
                            "2024-12-19T08:10:00.887000Z\t3678.01\tnull\n" +
                            "2024-12-19T08:10:00.950000Z\tnull\t3678.0\n",
                    pivotQuery,
                    "timestamp###ASC",
                    true,
                    true);

            assertException(
                    pivotQuery.replace("GROUP BY timestamp", "GROUP BY 1"),
                    110,
                    "cannot use positional group by inside `PIVOT`");
        });
    }

    @Test
    public void testPivotWithTradesDataAndSubquery() throws Exception {
        assertQueryAndPlan(
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n",
                "SELECT * FROM (\n" +
                        "SELECT * FROM (\n" +
                        "     SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount FROM trades WHERE symbol IN 'BTC-USD'\n" +
                        ")\n" +
                        "PIVOT (\n" +
                        "    sum(price)\n" +
                        "    FOR symbol IN ('BTC-USD')\n" +
                        "        side IN ('buy', 'sell')\n" +
                        "    GROUP BY timestamp\n" +
                        ") \n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n" +
                        "2024-12-19T08:10:00.136000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.138000Z\tnull\t101502.1\n" +
                        "2024-12-19T08:10:00.244000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.424000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.600000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.665999Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.693000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.716999Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.724000Z\t101502.2\tnull\n" +
                        "2024-12-19T08:10:00.732999Z\tnull\t101501.06\n" +
                        "2024-12-19T08:10:00.733999Z\tnull\t101500.0\n" +
                        "2024-12-19T08:10:00.734999Z\tnull\t101499.95\n" +
                        "2024-12-19T08:10:00.744000Z\t101497.6\tnull\n" +
                        "2024-12-19T08:10:00.926000Z\t101497.6\tnull\n" +
                        "2024-12-19T08:10:00.932000Z\tnull\t101497.25\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [timestamp]\n" +
                        "  values: [sum(case([(symbol='BTC-USD' and side='buy'),sum,null])),sum(case([(symbol='BTC-USD' and side='sell'),sum,null]))]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp,symbol,side]\n" +
                        "      values: [sum(price)]\n" +
                        "        Async Group By workers: 1\n" +
                        "          keys: [timestamp,symbol,side]\n" +
                        "          values: [avg(price)]\n" +
                        "          filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])\n" +
                        "            PageFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTradesDataAndWithClause() throws Exception {
        assertQueryAndPlan(
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n",
                "WITH p AS \n" +
                        "(WITH t AS\n" +
                        "(\n" +
                        "\n" +
                        "    SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount FROM trades WHERE symbol IN 'BTC-USD'\n" +
                        "    SAMPLE BY 1m\n" +
                        ")\n" +
                        "SELECT * FROM t\n" +
                        "PIVOT (\n" +
                        "    sum(price)\n" +
                        "    FOR symbol IN ('BTC-USD')    \n" +
                        "    side IN ('buy', 'sell')   \n" +
                        "    GROUP BY timestamp\n" +
                        ") ) SELECT * from p where `BTC-USD_buy` > 25780 or `BTC-USD_sell` > 25780;",
                ddlTrades,
                null,
                dmlTrades,
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n" +
                        "2024-12-19T08:10:00.000000Z\t101501.27999999998\t101500.15000000002\n",
                true,
                false,
                false,
                "Filter filter: (25780<BTC-USD_buy or 25780<BTC-USD_sell)\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp]\n" +
                        "      values: [sum(case([(symbol='BTC-USD' and side='buy'),sum,null])),sum(case([(symbol='BTC-USD' and side='sell'),sum,null]))]\n" +
                        "        GroupBy vectorized: false\n" +
                        "          keys: [timestamp,symbol,side]\n" +
                        "          values: [sum(price)]\n" +
                        "            Radix sort light\n" +
                        "              keys: [timestamp]\n" +
                        "                Async Group By workers: 1\n" +
                        "                  keys: [timestamp,symbol,side]\n" +
                        "                  values: [avg(price)]\n" +
                        "                  filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])\n" +
                        "                    PageFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTradesDataAndWithClause2() throws Exception {
        assertQueryAndPlan(
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n",
                "WITH t AS\n" +
                        "        (\n" +
                        "\n" +
                        "                SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount \n" +
                        "FROM trades WHERE symbol IN 'BTC-USD'\n" +
                        "SAMPLE BY 1m\n" +
                        "), P AS (\n" +
                        "        SELECT * FROM t\n" +
                        "        PIVOT (\n" +
                        "        sum(price)\n" +
                        "FOR symbol IN ('BTC-USD')\n" +
                        "side IN ('buy', 'sell')\n" +
                        "GROUP BY timestamp\n" +
                        ") )\n" +
                        "SELECT * FROM P;",
                ddlTrades,
                null,
                dmlTrades,
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n" +
                        "2024-12-19T08:10:00.000000Z\t101501.27999999998\t101500.15000000002\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [timestamp]\n" +
                        "  values: [sum(case([(symbol='BTC-USD' and side='buy'),sum,null])),sum(case([(symbol='BTC-USD' and side='sell'),sum,null]))]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [timestamp,symbol,side]\n" +
                        "      values: [sum(price)]\n" +
                        "        Radix sort light\n" +
                        "          keys: [timestamp]\n" +
                        "            Async Group By workers: 1\n" +
                        "              keys: [timestamp,symbol,side]\n" +
                        "              values: [avg(price)]\n" +
                        "              filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])\n" +
                        "                PageFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithTradesOHLC() throws Exception {
        assertQueryAndPlan(
                "side\tETH-USD_open\tETH-USD_high\tETH-USD_low\tETH-USD_close\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close\n",
                "trades PIVOT (\n" +
                        "first_not_null(price) as open,\n" +
                        "max(price) as high,\n" +
                        "min(price) as low,\n" +
                        "last_not_null(price) as close\n" +
                        "FOR symbol IN ('ETH-USD', 'BTC-USD')\n" +
                        "GROUP BY side\n" +
                        ");",
                ddlTrades,
                null,
                dmlTrades,
                "side\tETH-USD_open\tETH-USD_high\tETH-USD_low\tETH-USD_close\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close\n" +
                        "buy\t3678.01\t3678.01\t3678.01\t3678.01\t101502.2\t101502.2\t101497.6\t101497.6\n" +
                        "sell\t3678.25\t3678.25\t3678.0\t3678.0\t101502.1\t101502.1\t101497.0\t101497.0\n",
                true,
                true,
                false,
                "GroupBy vectorized: false\n" +
                        "  keys: [side]\n" +
                        "  values: [first_not_null(case([open,NaN,symbol])),max(case([high,NaN,symbol])),min(case([low,NaN,symbol])),last_not_null(case([close,NaN,symbol])),first_not_null(case([open,NaN,symbol])),max(case([high,NaN,symbol])),min(case([low,NaN,symbol])),last_not_null(case([close,NaN,symbol]))]\n" +
                        "    Async JIT Group By workers: 1\n" +
                        "      keys: [side,symbol]\n" +
                        "      values: [first_not_null(price),max(price),min(price),last_not_null(price)]\n" +
                        "      filter: symbol in [ETH-USD,BTC-USD]\n" +
                        "        PageFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: trades\n");
    }

    @Test
    public void testPivotWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlSensors);
            execute(dmlSensors);

            String query = "\n" +
                    "(\n" +
                    "    sensors \n" +
                    "    PIVOT (\n" +
                    "        avg(int_value) \n" +
                    "        FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%' LIMIT 1) ELSE 'Others'\n" +
                    "        GROUP BY timestamp, vehicle_id\n" +
                    "        ORDER BY timestamp\n" +
                    "        LIMIT 10\n" +
                    "    )\n" +
                    ")\n" +
                    "UNION\n" +
                    "(\n" +
                    "    sensors \n" +
                    "    PIVOT (\n" +
                    "        avg(int_value) \n" +
                    "        FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%' LIMIT 1) ELSE 'Others'\n" +
                    "        GROUP BY timestamp, vehicle_id\n" +
                    "        ORDER BY timestamp\n" +
                    "        LIMIT -10\n" +
                    "    )\n" +
                    ");";

            assertQueryNoLeakCheck("timestamp\tvehicle_id\ti009\tOthers\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA012\t-200.0\t208.11111111111111\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA022\t-98.0\t417.22222222222223\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA048\t164.0\t178.66666666666666\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA045\t850.0\t-10.333333333333334\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA039\t412.0\t98.33333333333333\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA041\t-454.0\t126.66666666666667\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA031\t-39.0\t75.11111111111111\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA044\t-222.0\t-209.77777777777777\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA014\t-745.0\t-287.0\n" +
                            "2025-01-01T00:00:00.000000Z\tAAA003\t-364.0\t64.11111111111111\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA082\t320.0\t-67.33333333333333\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA087\t-67.0\t309.6666666666667\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA079\t-24.0\t-294.1111111111111\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA053\t417.0\t-105.55555555555556\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA094\t158.0\t16.11111111111111\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA085\t-773.0\t-287.8888888888889\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA088\t619.0\t-392.6666666666667\n" +
                            "2025-01-01T00:00:00.009000Z\tAAA074\t-946.0\t-34.333333333333336\n" +
                            "2025-01-01T00:00:00.010000Z\tAAA099\tnull\tnull\n" +
                            "2025-01-01T00:00:00.010000Z\tAAA000\tnull\t-636.0\n",
                    query,
                    null,
                    false,
                    false,
                    false);

            assertPlanNoLeakCheck(query,
                    "Union\n" +
                            "    Sort light lo: 10\n" +
                            "      keys: [timestamp]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [timestamp,vehicle_id]\n" +
                            "          values: [avg(case([avg,NaN,sensor_name])),avg(case([not (sensor_name in [i009]),avg,null]))]\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [timestamp,vehicle_id,sensor_name]\n" +
                            "              values: [avg(int_value)]\n" +
                            "              filter: null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: sensors\n" +
                            "    Sort light lo: -10\n" +
                            "      keys: [timestamp]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [timestamp,vehicle_id]\n" +
                            "          values: [avg(case([avg,NaN,sensor_name])),avg(case([not (sensor_name in [i009]),avg,null]))]\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [timestamp,vehicle_id,sensor_name]\n" +
                            "              values: [avg(int_value)]\n" +
                            "              filter: null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: sensors\n");
        });
    }
}



