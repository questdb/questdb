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

import io.questdb.PropertyKey;
import org.junit.Test;

public class PivotTest extends AbstractSqlParserTest {

    public static String ddlCities = """
            CREATE TABLE cities (
                country VARCHAR, name VARCHAR, year INT, population INT
            );""";
    public static String ddlMonthlySales = "CREATE TABLE monthly_sales (empid INT, amount INT, month TEXT);";
    public static String ddlSensors = """
            CREATE TABLE IF NOT EXISTS sensors (
              timestamp TIMESTAMP,
              vehicle_id SYMBOL,
              sensor_name SYMBOL,
              int_value LONG,
              str_value STRING
            ) timestamp(timestamp) PARTITION BY DAY;
            """;
    public static String ddlTrades = """
            CREATE TABLE 'trades' (
              symbol SYMBOL,
              side SYMBOL,
              price DOUBLE,
              amount DOUBLE,
              timestamp TIMESTAMP
            ) timestamp (timestamp) PARTITION BY NONE""";
    public static String dmlCities =
            """
                    INSERT INTO cities VALUES
                        ('NL', 'Amsterdam', 2000, 1005),
                        ('NL', 'Amsterdam', 2010, 1065),
                        ('NL', 'Amsterdam', 2020, 1158),
                        ('US', 'Seattle', 2000, 564),
                        ('US', 'Seattle', 2010, 608),
                        ('US', 'Seattle', 2020, 738),
                        ('US', 'New York City', 2000, 8015),
                        ('US', 'New York City', 2010, 8175),
                        ('US', 'New York City', 2020, 8772);""";
    public static String dmlMonthlySales = """
             INSERT INTO monthly_sales VALUES
                        (1, 10000, 'JAN'),
                (1, 400, 'JAN'),
                        (2, 4500, 'JAN'),
                        (2, 35000, 'JAN'),
                        (1, 5000, 'FEB'),
                        (1, 3000, 'FEB'),
                        (2, 200, 'FEB'),
                        (2, 90500, 'FEB'),
                        (1, 6000, 'MAR'),
                        (1, 5000, 'MAR'),
                        (2, 2500, 'MAR'),
                        (2, 9500, 'MAR'),
                        (1, 8000, 'APR'),
                        (1, 10000, 'APR'),
                        (2, 800, 'APR'),
                        (2, 4500, 'APR');\
            """;
    public static String dmlSensors = """
            INSERT INTO sensors
            SELECT
                date_trunc('milliseconds', timestamp_sequence('2025-01-01', 1L) + (x / 2000)) AS timestamp,
                'AAA' || lpad(((x / 20) % 100)::string, 3, '0') AS vehicle_id,
                CASE
                    WHEN x % 20 < 10 THEN 'i' || lpad((x % 10)::string, 3, '0')
                    ELSE 's' || lpad(((x % 10))::string, 3, '0')
                END AS sensor_name,
                CASE WHEN x % 20 < 10 THEN rnd_long() % 1000 ELSE NULL END AS int_value,
                CASE WHEN x % 20 >= 10 THEN 'val_' || rnd_int() % 1000 ELSE NULL END AS str_value
            FROM long_sequence(10000) x;""";
    public static String dmlTrades = """
            INSERT INTO trades(symbol,side,price,amount,timestamp)\s
            VALUES ('ADA-USDT','sell',0.9716,94.2581,'2024-12-19T08:10:00.062000Z'),
             ('ADA-USD','sell',0.9716,94.2581,'2024-12-19T08:10:00.062000Z'),
             ('BTC-USDT','buy',101502.2,5.775E-5,'2024-12-19T08:10:00.136000Z'),
             ('BTC-USD','buy',101502.2,5.775E-5,'2024-12-19T08:10:00.136000Z'),
             ('BTC-USDT','sell',101502.1,1.4443E-4,'2024-12-19T08:10:00.138000Z'),
             ('BTC-USD','sell',101502.1,1.4443E-4,'2024-12-19T08:10:00.138000Z'),
             ('BTC-USDT','buy',101502.2,3.4654E-4,'2024-12-19T08:10:00.244000Z'),
             ('BTC-USD','buy',101502.2,3.4654E-4,'2024-12-19T08:10:00.244000Z'),
             ('DOGE-USDT','sell',0.36051,47.831939,'2024-12-19T08:10:00.322000Z'),
             ('DOGE-USD','sell',0.36051,47.831939,'2024-12-19T08:10:00.322000Z'),
             ('DOGE-USDT','sell',0.36046,978.95676,'2024-12-19T08:10:00.322000Z'),
             ('DOGE-USD','sell',0.36046,978.95676,'2024-12-19T08:10:00.322000Z'),
             ('DOGE-USDT','buy',0.36047,8683.359195,'2024-12-19T08:10:00.392000Z'),
             ('DOGE-USD','buy',0.36047,8683.359195,'2024-12-19T08:10:00.392000Z'),
             ('BTC-USDT','buy',101502.2,9.359E-5,'2024-12-19T08:10:00.424000Z'),
             ('BTC-USD','buy',101502.2,9.359E-5,'2024-12-19T08:10:00.424000Z'),
             ('USDT-USDC','buy',0.9994,135.321,'2024-12-19T08:10:00.548000Z'),
             ('ADA-USDT','sell',0.9716,1763.2036,'2024-12-19T08:10:00.552000Z'),
             ('ADA-USD','sell',0.9716,1763.2036,'2024-12-19T08:10:00.552000Z'),
             ('ADA-USDT','sell',0.9716,5117.169,'2024-12-19T08:10:00.559000Z'),
             ('ADA-USD','sell',0.9716,5117.169,'2024-12-19T08:10:00.559000Z'),
             ('BTC-USDT','buy',101502.2,1.4449E-4,'2024-12-19T08:10:00.600000Z'),
             ('BTC-USD','buy',101502.2,1.4449E-4,'2024-12-19T08:10:00.600000Z'),
             ('BTC-USDT','buy',101502.2,1.7339E-4,'2024-12-19T08:10:00.665999Z'),
             ('BTC-USD','buy',101502.2,1.7339E-4,'2024-12-19T08:10:00.665999Z'),
             ('BTC-USDT','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.693000Z'),
             ('BTC-USD','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.693000Z'),
             ('ETH-USDT','sell',3678.25,0.026994,'2024-12-19T08:10:00.700999Z'),
             ('ETH-USD','sell',3678.25,0.026994,'2024-12-19T08:10:00.700999Z'),
             ('BTC-USDT','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.716999Z'),
             ('BTC-USD','buy',101502.2,2.889E-5,'2024-12-19T08:10:00.716999Z'),
             ('BTC-USDT','buy',101502.2,8.3806E-4,'2024-12-19T08:10:00.724000Z'),
             ('BTC-USD','buy',101502.2,8.3806E-4,'2024-12-19T08:10:00.724000Z'),
             ('BTC-USDT','sell',101502.1,0.02973634,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USD','sell',101502.1,0.02973634,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USDT','sell',101502.1,0.06976683,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USD','sell',101502.1,0.06976683,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USDT','sell',101500.9,0.01971311,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USD','sell',101500.9,0.01971311,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USDT','sell',101500.2,0.00621176,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USD','sell',101500.2,0.00621176,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USDT','sell',101500.0,0.04697513,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USD','sell',101500.0,0.04697513,'2024-12-19T08:10:00.732999Z'),
             ('BTC-USDT','sell',101500.0,0.02353103,'2024-12-19T08:10:00.733999Z'),
             ('BTC-USD','sell',101500.0,0.02353103,'2024-12-19T08:10:00.733999Z'),
             ('BTC-USDT','sell',101500.0,0.07167521,'2024-12-19T08:10:00.733999Z'),
             ('BTC-USD','sell',101500.0,0.07167521,'2024-12-19T08:10:00.733999Z'),
             ('DOGE-USDT','sell',0.36045,3400.0,'2024-12-19T08:10:00.733999Z'),
             ('DOGE-USD','sell',0.36045,3400.0,'2024-12-19T08:10:00.733999Z'),
             ('BTC-USDT','sell',101500.0,0.01922527,'2024-12-19T08:10:00.734999Z'),
             ('BTC-USD','sell',101500.0,0.01922527,'2024-12-19T08:10:00.734999Z'),
             ('BTC-USDT','sell',101499.9,4.8784E-4,'2024-12-19T08:10:00.734999Z'),
             ('BTC-USD','sell',101499.9,4.8784E-4,'2024-12-19T08:10:00.734999Z'),
             ('ETH-USDT','sell',3678.25,0.010916,'2024-12-19T08:10:00.736000Z'),
             ('ETH-USD','sell',3678.25,0.010916,'2024-12-19T08:10:00.736000Z'),
             ('DOGE-USDT','sell',0.36044,26.0,'2024-12-19T08:10:00.736999Z'),
             ('DOGE-USD','sell',0.36044,26.0,'2024-12-19T08:10:00.736999Z'),
             ('DOGE-USDT','sell',0.36044,0.050351,'2024-12-19T08:10:00.736999Z'),
             ('DOGE-USD','sell',0.36044,0.050351,'2024-12-19T08:10:00.736999Z'),
             ('ETH-USDC','sell',3676.0,0.255,'2024-12-19T08:10:00.743000Z'),
             ('ETH-USDC','sell',3675.99,0.230546,'2024-12-19T08:10:00.743000Z'),
             ('ETH-USDC','sell',3675.99,0.264415,'2024-12-19T08:10:00.743000Z'),
             ('ETH-USDC','sell',3675.95,0.221131,'2024-12-19T08:10:00.743000Z'),
             ('BTC-USDT','buy',101497.6,8.669E-5,'2024-12-19T08:10:00.744000Z'),
             ('BTC-USD','buy',101497.6,8.669E-5,'2024-12-19T08:10:00.744000Z'),
             ('ETH-USDC','sell',3675.95,0.064454,'2024-12-19T08:10:00.744999Z'),
             ('ETH-USDC','sell',3675.95,0.064454,'2024-12-19T08:10:00.746000Z'),
             ('ETH-USDT','sell',3678.0,0.2,'2024-12-19T08:10:00.759000Z'),
             ('ETH-USD','sell',3678.0,0.2,'2024-12-19T08:10:00.759000Z'),
             ('ETH-USDT','sell',3678.0,1.080001,'2024-12-19T08:10:00.772999Z'),
             ('ETH-USD','sell',3678.0,1.080001,'2024-12-19T08:10:00.772999Z'),
             ('ETH-USDT','buy',3678.01,0.006046,'2024-12-19T08:10:00.887000Z'),
             ('ETH-USD','buy',3678.01,0.006046,'2024-12-19T08:10:00.887000Z'),
             ('DOGE-USDT','sell',0.36041,13873.0,'2024-12-19T08:10:00.898000Z'),
             ('DOGE-USD','sell',0.36041,13873.0,'2024-12-19T08:10:00.898000Z'),
             ('SOL-USDT','sell',210.41,0.037636,'2024-12-19T08:10:00.903000Z'),
             ('SOL-USD','sell',210.41,0.037636,'2024-12-19T08:10:00.903000Z'),
             ('BTC-USDT','buy',101497.6,4.433E-5,'2024-12-19T08:10:00.926000Z'),
             ('BTC-USD','buy',101497.6,4.433E-5,'2024-12-19T08:10:00.926000Z'),
             ('BTC-USDT','sell',101497.5,1.2529056,'2024-12-19T08:10:00.932000Z'),
             ('BTC-USD','sell',101497.5,1.2529056,'2024-12-19T08:10:00.932000Z'),
             ('BTC-USDT','sell',101497.0,9.3655E-4,'2024-12-19T08:10:00.932000Z'),
             ('BTC-USD','sell',101497.0,9.3655E-4,'2024-12-19T08:10:00.932000Z'),
             ('ETH-USDC','sell',3675.95,0.204168,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.86,0.28142,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.86,0.32258,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.75,0.079,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.73,1.56E-4,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.72,0.083852,'2024-12-19T08:10:00.935000Z'),
             ('ETH-USDC','sell',3675.72,0.064412,'2024-12-19T08:10:00.936999Z'),
             ('ETH-USDC','sell',3675.72,0.064412,'2024-12-19T08:10:00.937999Z'),
             ('ETH-USDT','sell',3678.0,0.2,'2024-12-19T08:10:00.950000Z'),
             ('ETH-USD','sell',3678.0,0.2,'2024-12-19T08:10:00.950000Z');""";

    @Test
    public void testBasicPivot() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t2010\t2020\n",
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country\t2000\t2010\t2020
                        NL\t1005\t1065\t1158
                        US\t8579\t8783\t9510
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [country]
                          values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [country,year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotDefaultNamingRules() throws Exception {
        assertQueryAndPlan(
                "side\tBTC-USD_first(price)\tBTC-USD_first(price)_2\n",
                """
                        trades PIVOT (
                        first(price),
                        first(price)
                        FOR symbol IN ('BTC-USD')
                        GROUP BY side
                        ) order by side;""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        side\tBTC-USD_first(price)\tBTC-USD_first(price)_2
                        buy\t101502.2\t101502.2
                        sell\t101502.1\t101502.1
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [side]
                            GroupBy vectorized: false
                              keys: [side]
                              values: [first_not_null(case([first(price),NaN,symbol])),first_not_null(case([first(price)_2,NaN,symbol]))]
                                VirtualRecord
                                  functions: [side,first(price),symbol,first(price)]
                                    Async JIT Group By workers: 1
                                      keys: [side,symbol]
                                      values: [first(price)]
                                      filter: symbol in [BTC-USD]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotDefaultNamingRules2() throws Exception {
        assertQueryAndPlan(
                "side\tBTC-USD_first(price)\tBTC-USD_first(amount)\n",
                """
                        trades PIVOT (
                        first(price),
                        first(amount)
                        FOR symbol IN ('BTC-USD')
                        GROUP BY side
                        ) order by side;""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        side\tBTC-USD_first(price)\tBTC-USD_first(amount)
                        buy\t101502.2\t5.775E-5
                        sell\t101502.1\t1.4443E-4
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [side]
                            GroupBy vectorized: false
                              keys: [side]
                              values: [first_not_null(case([first(price),NaN,symbol])),first_not_null(case([first(amount),NaN,symbol]))]
                                Async JIT Group By workers: 1
                                  keys: [side,symbol]
                                  values: [first(price),first(amount)]
                                  filter: symbol in [BTC-USD]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotImplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                """
                        2000\t2010\t2020
                        null\tnull\tnull
                        """,
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000\t2010\t2020
                        9584\t9848\t10668
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotImplicitGroupByWithAlias() throws Exception {
        assertQueryAndPlan(
                """
                        2000_sum\t2010_sum\t2020_sum
                        null\tnull\tnull
                        """,
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000_sum\t2010_sum\t2020_sum
                        9584\t9848\t10668
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotImplicitGroupByWithAliasNoAs() throws Exception {
        assertQueryAndPlan(
                """
                        2000_sum\t2010_sum\t2020_sum
                        null\tnull\tnull
                        """,
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population) sum
                            FOR
                                year IN (2000, 2010, 2020)
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000_sum\t2010_sum\t2020_sum
                        9584\t9848\t10668
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotImplicitGroupByWithOrderBy() throws Exception {
        assertQueryAndPlan(
                """
                        2000\t2010\t2020
                        null\tnull\tnull
                        """,
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                        ) ORDER BY "2000";
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000\t2010\t2020
                        9584\t9848\t10668
                        """,
                true,
                true,
                false,
                """
                        Sort
                          keys: [2000]
                            GroupBy vectorized: false
                              values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotInUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country\t2000\t2010
                            NL\t1005\t1065
                            US\t8579\t8783
                            NL\t1005\t1065
                            US\t8579\t8783
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (2000, 2010)
                                GROUP BY country
                            )
                            UNION ALL
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (2000, 2010)
                                GROUP BY country
                            )
                            """,
                    null,
                    false,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotMaxProducedColumnsExceededByForCombinations() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_PIVOT_MAX_PRODUCED_COLUMNS, 5);
            execute(ddlMonthlySales);
            assertException("""
                    SELECT * FROM monthly_sales
                    PIVOT (
                        SUM(amount)
                        FOR month IN ('JAN', 'FEB', 'MAR', 'APR')
                          empid IN (1, 2)
                    )
                    """, 104, "PIVOT produces too many columns: 8, limit is 5");
        });
    }

    @Test
    public void testPivotMaxProducedColumnsExceededByTotalColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_PIVOT_MAX_PRODUCED_COLUMNS, 10);
            execute(ddlMonthlySales);
            assertException("""
                    SELECT * FROM monthly_sales
                    PIVOT (
                        SUM(amount),
                        AVG(amount),
                        COUNT(amount)
                        FOR month IN ('JAN', 'FEB', 'MAR', 'APR')
                        GROUP BY empid
                    )
                    """, 28, "PIVOT produces too many columns: 12, limit is 10");
        });
    }

    @Test
    public void testPivotNestedPivot() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country	NL	US
                            NL	3228	null
                            US	null	26872
                            """,
                    """
                            SELECT * FROM (
                                SELECT * FROM cities
                                PIVOT (
                                    SUM(population)
                                    FOR year IN (2000, 2010, 2020)
                                    GROUP BY country
                                )
                            ) PIVOT (
                                SUM("2000" + "2010" + "2020")
                                FOR country IN ('NL', 'US')
                                GROUP BY country
                            ) order by country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotOHLC() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);
            drainWalQueue();

            String pivotQuery = """
                    trades PIVOT (
                    first(price) as open,
                    max(price) as high,
                    min(price) as low,
                    last(price) as close
                    FOR symbol IN ('BTC-USD')
                    GROUP BY side
                    ) order by side;""";

            String result = """
                    side\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close
                    buy\t101502.2\t101502.2\t101497.6\t101497.6
                    sell\t101502.1\t101502.1\t101497.0\t101497.0
                    """;

            assertPlanNoLeakCheck(pivotQuery, """
                    Sort light
                      keys: [side]
                        GroupBy vectorized: false
                          keys: [side]
                          values: [first_not_null(case([open,NaN,symbol])),first_not_null(case([high,NaN,symbol])),first_not_null(case([low,NaN,symbol])),first_not_null(case([close,NaN,symbol]))]
                            Async JIT Group By workers: 1
                              keys: [side,symbol]
                              values: [first(price),max(price),min(price),last(price)]
                              filter: symbol in [BTC-USD]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                    """);
            assertSql(result, pivotQuery);
        });
    }

    @Test
    public void testPivotPositionalGroupByNotAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertException(
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (2000, 2010, 2020)
                                GROUP BY 1
                            )
                            """,
                    97,
                    "cannot use positional group by inside `PIVOT`"
            );
        });
    }

    @Test
    public void testPivotSubqueryReturnsEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertException(
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (SELECT year FROM cities WHERE year > 9999)
                                GROUP BY country
                            )
                            """,
                    66,
                    "PIVOT IN subquery returned empty result set"
            );
        });
    }

    @Test
    public void testPivotSubqueryReturnsMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertException(
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (SELECT year, country FROM cities)
                                GROUP BY country
                            )
                            """,
                    66,
                    "PIVOT IN subquery must return exactly one column, got 2"
            );
        });
    }

    @Test
    public void testPivotSubqueryWithArrayType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DOUBLE[], val INT);");
            execute("CREATE TABLE cats (c DOUBLE[]);");
            execute("INSERT INTO cats VALUES (array[1, 2]), (array[3, 4]);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', array[1, 2], 10),
                        ('A', array[3, 4], 20),
                        ('B', array[1, 2], 30),
                        ('B', array[3, 4], 40);
                    """);
            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 56, "unsupported PIVOT FOR column type: DOUBLE[]");
        });
    }

    @Test
    public void testPivotSubqueryWithBinaryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat BINARY, val INT);");
            execute("CREATE TABLE cats (c BINARY);");
            execute("INSERT INTO cats VALUES ('A'::binary), ('B'::binary);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'A'::binary, 10),
                        ('A', 'B'::binary, 20),
                        ('B', 'A'::binary, 30),
                        ('B', 'B'::binary, 40);
                    """);
            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 56, "unsupported PIVOT FOR column type: BINARY");
        });
    }

    @Test
    public void testPivotSubqueryWithBooleanType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat BOOLEAN, val INT);");
            execute("CREATE TABLE cats (c BOOLEAN);");
            execute("INSERT INTO cats VALUES (true), (false);");
            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: BOOLEAN");
        });
    }

    @Test
    public void testPivotSubqueryWithByteType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat BYTE, val INT);");
            execute("CREATE TABLE cats (c BYTE);");
            execute("INSERT INTO cats VALUES (1), (2);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1, 10),
                        ('A', 2, 20),
                        ('B', 1, 30),
                        ('B', 2, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\t1\t2
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithCharType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat CHAR, val INT);");
            execute("CREATE TABLE cats (c CHAR);");
            execute("INSERT INTO cats VALUES ('X'), ('Y');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', 20),
                        ('B', 'X', 30),
                        ('B', 'Y', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\tX\tY
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithDateType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DATE, val INT);");
            execute("CREATE TABLE cats (c DATE);");
            execute("INSERT INTO cats VALUES ('2024-01-01'), ('2024-01-02');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', '2024-01-01', 10),
                        ('A', '2024-01-02', 20),
                        ('B', '2024-01-01', 30),
                        ('B', '2024-01-02', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	"2024-01-01T00:00:00.000Z"	"2024-01-02T00:00:00.000Z"
                            A	10	20
                            B	30	40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal128Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(20, 2), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(20, 2));");
            execute("INSERT INTO cats VALUES (1.50m), (2.75m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.50m, 10),
                        ('A', 2.75m, 20),
                        ('B', 1.50m, 30),
                        ('B', 2.75m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(20,2)");
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal16Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(4, 2), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(4, 2));");
            execute("INSERT INTO cats VALUES (1.50m), (2.75m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.50m, 10),
                        ('A', 2.75m, 20),
                        ('B', 1.50m, 30),
                        ('B', 2.75m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(4,2)");
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal256Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(40, 2), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(40, 2));");
            execute("INSERT INTO cats VALUES (1.50m), (2.75m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.50m, 10),
                        ('A', 2.75m, 20),
                        ('B', 1.50m, 30),
                        ('B', 2.75m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(40,2)");
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal32Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(9, 2), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(9, 2));");
            execute("INSERT INTO cats VALUES (1.50m), (2.75m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.50m, 10),
                        ('A', 2.75m, 20),
                        ('B', 1.50m, 30),
                        ('B', 2.75m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(9,2)");
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal64Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(10, 2), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(10, 2));");
            execute("INSERT INTO cats VALUES (1.50m), (2.75m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.50m, 10),
                        ('A', 2.75m, 20),
                        ('B', 1.50m, 30),
                        ('B', 2.75m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(10,2)");
        });
    }

    @Test
    public void testPivotSubqueryWithDecimal8Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DECIMAL(2, 1), val INT);");
            execute("CREATE TABLE cats (c DECIMAL(2, 1));");
            execute("INSERT INTO cats VALUES (1.5m), (2.5m);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.5m, 10),
                        ('A', 2.5m, 20),
                        ('B', 1.5m, 30),
                        ('B', 2.5m, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: DECIMAL(2,1)");
        });
    }

    @Test
    public void testPivotSubqueryWithDoubleType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat DOUBLE, val INT);");
            execute("CREATE TABLE cats (c DOUBLE);");
            execute("INSERT INTO cats VALUES (1.5), (2.5);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.5, 10),
                        ('A', 2.5, 20),
                        ('B', 1.5, 30),
                        ('B', 2.5, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	"1.5"	"2.5"
                            A	10	20
                            B	30	40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithFewDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val INT);");
            execute("CREATE TABLE cats AS (SELECT 'cat_' || (x%10)::string as c FROM long_sequence(2000000));");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'cat_1', 10),
                        ('A', 'cat_2', 20),
                        ('B', 'cat_1', 30),
                        ('B', 'cat_2', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	cat_1	cat_2	cat_3	cat_4	cat_5	cat_6	cat_7	cat_8	cat_9	cat_0
                            A	10	20	null	null	null	null	null	null	null	null
                            B	30	40	null	null	null	null	null	null	null	null
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithFloatType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat FLOAT, val INT);");
            execute("CREATE TABLE cats (c FLOAT);");
            execute("INSERT INTO cats VALUES (1.5), (2.5);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1.5, 10),
                        ('A', 2.5, 20),
                        ('B', 1.5, 30),
                        ('B', 2.5, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	"1.5"	"2.5"
                            A	10	20
                            B	30	40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithGeoHashType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat GEOHASH(4c), val INT);");
            execute("CREATE TABLE cats (c GEOHASH(4c));");
            execute("INSERT INTO cats VALUES (#u09t), (#u09v);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', #u09t, 10),
                        ('A', #u09v, 20),
                        ('B', #u09t, 30),
                        ('B', #u09v, 40);
                    """);

            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    ) ORDER BY grp
                    """, 48, "there is no matching operator `IN` with the argument type: GEOHASH(4c)");
        });
    }

    @Test
    public void testPivotSubqueryWithIPv4Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat IPv4, val INT);");
            execute("CREATE TABLE cats (c IPv4);");
            execute("INSERT INTO cats VALUES ('192.168.1.1'), ('192.168.1.2');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', '192.168.1.1', 10),
                        ('A', '192.168.1.2', 20),
                        ('B', '192.168.1.1', 30),
                        ('B', '192.168.1.2', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	"192.168.1.1"	"192.168.1.2"
                            A	10	20
                            B	30	40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithIntType() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);
            execute("CREATE TABLE years (y INT);");
            execute("INSERT INTO years VALUES (2000), (2010);");

            assertQueryNoLeakCheck(
                    """
                            country\t2000\t2010
                            NL\t1005\t1065
                            US\t8579\t8783
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (SELECT y FROM years)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithLong128Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat LONG128, val INT);");
            execute("CREATE TABLE cats (c LONG128);");
            execute("INSERT INTO cats VALUES (to_long128(1, 1)), (to_long128(2, 2));");
            execute("""
                    INSERT INTO data VALUES
                        ('A', to_long128(1, 1), 10),
                        ('A', to_long128(2, 2), 20),
                        ('B', to_long128(1, 1), 30),
                        ('B', to_long128(2, 2), 40);
                    """);

            assertException(
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """, 48, "there is no matching operator `IN` with the argument type: LONG128");
        });
    }

    @Test
    public void testPivotSubqueryWithLong256Type() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat LONG256, val INT);");
            execute("CREATE TABLE cats (c LONG256);");
            execute("INSERT INTO cats VALUES " +
                    "(to_long256(1, 1, 1, 1)), " +
                    "(to_long256(2, 2, 2, 2));");
            execute("""
                    INSERT INTO data VALUES
                        ('A', to_long256(1, 1, 1, 1), 10),
                        ('A', to_long256(2, 2, 2, 2), 20),
                        ('B', to_long256(1, 1, 1, 1), 30),
                        ('B', to_long256(2, 2, 2, 2), 40);
                    """);

            assertException(
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """, 56, "unsupported PIVOT FOR column type: LONG256");
        });
    }

    @Test
    public void testPivotSubqueryWithLongType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat LONG, val INT);");
            execute("CREATE TABLE cats (c LONG);");
            execute("INSERT INTO cats VALUES (1000000000000), (2000000000000);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1000000000000, 10),
                        ('A', 2000000000000, 20),
                        ('B', 1000000000000, 30),
                        ('B', 2000000000000, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\t1000000000000\t2000000000000
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val INT);");
            execute("CREATE TABLE cats AS (SELECT 'cat_' || x::string as c FROM long_sequence(100000));");
            assertException("""
                    SELECT * FROM data
                    PIVOT (
                        SUM(val)
                        FOR cat IN (SELECT c FROM cats)
                        GROUP BY grp
                    )
                    """, 56, "PIVOT produces too many columns: 5001, limit is 5000");
        });
    }

    @Test
    public void testPivotSubqueryWithMultipleForColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sales (region SYMBOL, product SYMBOL, quarter SYMBOL, amount INT);");
            execute("CREATE TABLE products AS (SELECT 'prod_' || x::string as p FROM long_sequence(3));");
            execute("CREATE TABLE quarters AS (SELECT 'Q' || x::string as q FROM long_sequence(2));");
            execute("""
                    INSERT INTO sales VALUES
                        ('East', 'prod_1', 'Q1', 100),
                        ('East', 'prod_1', 'Q2', 150),
                        ('East', 'prod_2', 'Q1', 200),
                        ('East', 'prod_2', 'Q2', 250),
                        ('West', 'prod_1', 'Q1', 300),
                        ('West', 'prod_1', 'Q2', 350),
                        ('West', 'prod_2', 'Q1', 400),
                        ('West', 'prod_2', 'Q2', 450);
                    """);

            assertQueryNoLeakCheck(
                    """
                            region\tprod_1_Q1\tprod_1_Q2\tprod_2_Q1\tprod_2_Q2\tprod_3_Q1\tprod_3_Q2
                            East\t100\t150\t200\t250\tnull\tnull
                            West\t300\t350\t400\t450\tnull\tnull
                            """,
                    """
                            SELECT * FROM sales
                            PIVOT (
                                SUM(amount)
                                FOR product IN (SELECT p FROM products)
                                    quarter IN (SELECT q FROM quarters)
                                GROUP BY region
                            ) ORDER BY region
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithNullIntValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat INT, val INT);");
            execute("CREATE TABLE cats (c INT);");
            execute("INSERT INTO cats VALUES (1), (null);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1, 10),
                        ('A', null, 20),
                        ('B', 1, 30),
                        ('B', null, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\t1\tNULL
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val INT);");
            execute("CREATE TABLE cats (c SYMBOL);");
            execute("INSERT INTO cats VALUES ('X'), (null);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', null, 20),
                        ('B', 'X', 30),
                        ('B', null, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\tX\tNULL
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithShortType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat SHORT, val INT);");
            execute("CREATE TABLE cats (c SHORT);");
            execute("INSERT INTO cats VALUES (1), (2);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 1, 10),
                        ('A', 2, 20),
                        ('B', 1, 30),
                        ('B', 2, 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\t1\t2
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithStringType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat STRING, val INT);");
            execute("CREATE TABLE cats (c STRING);");
            execute("INSERT INTO cats VALUES ('X'), ('Y');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', 20),
                        ('B', 'X', 30),
                        ('B', 'Y', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\tX\tY
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithSymbolType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val INT);");
            execute("CREATE TABLE cats (c SYMBOL);");
            execute("INSERT INTO cats VALUES ('X'), ('Y');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', 20),
                        ('B', 'X', 30),
                        ('B', 'Y', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\tX\tY
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithTimestampType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat TIMESTAMP, val INT);");
            execute("CREATE TABLE cats (c TIMESTAMP);");
            execute("INSERT INTO cats VALUES ('2024-01-01T00:00:00.000000Z'), ('2024-01-02T00:00:00.000000Z');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', '2024-01-01T00:00:00.000000Z', 10),
                        ('A', '2024-01-02T00:00:00.000000Z', 20),
                        ('B', '2024-01-01T00:00:00.000000Z', 30),
                        ('B', '2024-01-02T00:00:00.000000Z', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	"2024-01-01T00:00:00.000000Z"	"2024-01-02T00:00:00.000000Z"
                            A	10	20
                            B	30	40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotSubqueryWithVarcharType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat VARCHAR, val INT);");
            execute("CREATE TABLE cats (c VARCHAR);");
            execute("INSERT INTO cats VALUES ('X'), ('Y');");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', 20),
                        ('B', 'X', 30),
                        ('B', 'Y', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp\tX\tY
                            A\t10\t20
                            B\t30\t40
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(val)
                                FOR cat IN (SELECT c FROM cats)
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithAliasedAggregate() throws Exception {
        assertQueryAndPlan(
                "country\t2000_total\t2010_total\t2020_total\n",
                """
                        cities
                        PIVOT (
                            SUM(population) as total
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country
                        ) order by country;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country\t2000_total\t2010_total\t2020_total
                        NL\t1005\t1065\t1158
                        US\t8579\t8783\t9510
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([total,nullL,year])),first_not_null(case([total,nullL,year])),first_not_null(case([total,nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithAllNullColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sparse (grp SYMBOL, cat SYMBOL, val INT);");
            execute("""
                    INSERT INTO sparse VALUES
                        ('G1', 'A', 10),
                        ('G1', 'B', null),
                        ('G2', 'A', 20),
                        ('G2', 'B', null);
                    """);

            assertQueryNoLeakCheck(
                    """
                            grp	A_SUM(val)	A_count(val)	B_SUM(val)	B_count(val)
                            G1	10	1	null	0
                            G2	20	1	null	0
                            """,
                    """
                            SELECT * FROM sparse
                            PIVOT (
                                SUM(val), count(val)
                                FOR cat IN ('A', 'B')
                                GROUP BY grp
                            ) ORDER BY grp
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithCTEAndKeyedAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlSensors);
            execute(dmlSensors);

            String query = """
                    WITH A AS (
                    sensors
                    PIVOT (
                        avg(int_value)
                        FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%')
                        GROUP BY timestamp, vehicle_id
                    ) order by timestamp,vehicle_id
                    ), B AS (
                    sensors
                    PIVOT (
                        last(str_value)
                        FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 's%')
                        GROUP BY timestamp, vehicle_id
                    )
                    order by timestamp,vehicle_id
                    ) select * from A asof join B ON (vehicle_id) LIMIT 10
                    ;""";

            assertQueryNoLeakCheck("""
                            timestamp	vehicle_id	i009	i000	i002	i004	i008	i003	i007	i005	i006	i001	timestamp1	vehicle_id1	s001	s005	s006	s009	s003	s008	s002	s004	s007	s000
                            2025-01-01T00:00:00.000000Z	AAA000	-6.0	null	366.0	-475.0	-881.0	25.0	-998.0	29.0	373.0	856.0	2025-01-01T00:00:00.000000Z	AAA000	val_-516	val_481	val_512	val_-405	val_-714	val_97	val_-972	val_-703	val_116	val_-48
                            2025-01-01T00:00:00.000000Z	AAA001	-727.0	698.0	-893.0	51.0	-904.0	716.0	886.0	-57.0	-16.0	859.0	2025-01-01T00:00:00.000000Z	AAA001	val_-305	val_198	val_-769	val_-723	val_-104	val_228	val_-171	val_-279	val_-127	val_-964
                            2025-01-01T00:00:00.000000Z	AAA002	-951.0	388.0	339.0	-508.0	504.0	697.0	3.0	57.0	518.0	86.0	2025-01-01T00:00:00.000000Z	AAA002	val_-752	val_-300	val_928	val_638	val_-973	val_-319	val_-747	val_-842	val_-463	val_-914
                            2025-01-01T00:00:00.000000Z	AAA003	-364.0	64.0	-360.0	694.0	-476.0	-248.0	-602.0	10.0	778.0	717.0	2025-01-01T00:00:00.000000Z	AAA003	val_-835	val_705	val_-703	val_841	val_-54	val_-933	val_263	val_-908	val_-393	val_394
                            2025-01-01T00:00:00.000000Z	AAA004	203.0	3.0	-123.0	374.0	841.0	290.0	-711.0	-840.0	-155.0	-517.0	2025-01-01T00:00:00.000000Z	AAA004	val_-781	val_524	val_624	val_-574	val_763	val_-352	val_380	val_138	val_-195	val_-136
                            2025-01-01T00:00:00.000000Z	AAA005	93.0	-909.0	422.0	-687.0	932.0	747.0	514.0	-663.0	150.0	-943.0	2025-01-01T00:00:00.000000Z	AAA005	val_-76	val_930	val_681	val_695	val_-128	val_-819	val_-121	val_-59	val_-445	val_-682
                            2025-01-01T00:00:00.000000Z	AAA006	575.0	598.0	-728.0	3.0	25.0	59.0	469.0	-311.0	-842.0	-866.0	2025-01-01T00:00:00.000000Z	AAA006	val_-330	val_-473	val_272	val_-184	val_-113	val_926	val_-740	val_535	val_-671	val_468
                            2025-01-01T00:00:00.000000Z	AAA007	627.0	191.0	87.0	-934.0	-168.0	-820.0	-147.0	485.0	31.0	868.0	2025-01-01T00:00:00.000000Z	AAA007	val_-531	val_-995	val_43	val_75	val_60	val_640	val_-138	val_37	val_782	val_242
                            2025-01-01T00:00:00.000000Z	AAA008	-942.0	-693.0	-472.0	-42.0	-412.0	-964.0	-509.0	-64.0	483.0	-721.0	2025-01-01T00:00:00.000000Z	AAA008	val_17	val_941	val_385	val_795	val_-190	val_-384	val_444	val_692	val_468	val_-67
                            2025-01-01T00:00:00.000000Z	AAA009	-336.0	910.0	451.0	-333.0	-199.0	293.0	-242.0	827.0	834.0	276.0	2025-01-01T00:00:00.000000Z	AAA009	val_407	val_-743	val_988	val_583	val_895	val_435	val_-806	val_460	val_-320	val_889
                            """,
                    query,
                    "timestamp",
                    false,
                    false,
                    false);

            assertPlanNoLeakCheck(query,
                    """
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                SelectedRecord
                                    AsOf Join Light
                                      condition: B.vehicle_id=A.vehicle_id
                                        Sort light
                                          keys: [timestamp, vehicle_id]
                                            GroupBy vectorized: false
                                              keys: [timestamp,vehicle_id]
                                              values: [first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name])),first_not_null(case([avg(int_value),NaN,sensor_name]))]
                                                Async Group By workers: 1
                                                  keys: [timestamp,vehicle_id,sensor_name]
                                                  values: [avg(int_value)]
                                                  filter: sensor_name in [i009,i000,i002,i004,i008,i003,i007,i005,i006,i001]
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: sensors
                                        Sort light
                                          keys: [timestamp, vehicle_id]
                                            GroupBy vectorized: false
                                              keys: [timestamp,vehicle_id]
                                              values: [first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name])),first_not_null(case([last(str_value),null,sensor_name]))]
                                                Async Group By workers: 1
                                                  keys: [timestamp,vehicle_id,sensor_name]
                                                  values: [last(str_value)]
                                                  filter: sensor_name in [s001,s005,s006,s009,s003,s008,s002,s004,s007,s000]
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: sensors
                            """);
        });
    }

    @Test
    public void testPivotWithCTEInsideDynamicSubQuery() throws Exception {
        assertQueryAndPlan(
                """
                        CPDH\t2010\t2017\t2018\t2022
                        C1\tnull\t0\t20\t10
                        C2\t30\tnull\tnull\t10
                        C3\t80\tnull\tnull\tnull
                        """,
                """
                        WITH CPB AS (
                        SELECT 'C1' AS CPDH, 2022 AS NF, 10 AS JG
                        UNION ALL
                        SELECT 'C1',2018,20
                        UNION ALL
                        SELECT 'C1',2017,0
                        UNION ALL
                        SELECT 'C2',2022,10
                        UNION ALL
                        SELECT 'C2',2010,30
                        UNION ALL
                        SELECT 'C3',2010,80
                        )
                        SELECT * FROM CPB PIVOT (sum(jg) FOR nf IN (SELECT NF FROM CPB ORDER BY NF) GROUP BY CPDH) ORDER BY CPDH;""",
                null,
                null,
                null,
                """
                        CPDH\t2010\t2017\t2018\t2022
                        C1\tnull\t0\t20\t10
                        C2\t30\tnull\tnull\t10
                        C3\t80\tnull\tnull\tnull
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [CPDH]
                            GroupBy vectorized: false
                              keys: [CPDH]
                              values: [first_not_null(case([sum(jg),nullL,nf])),first_not_null(case([sum(jg),nullL,nf])),first_not_null(case([sum(jg),nullL,nf])),first_not_null(case([sum(jg),nullL,nf]))]
                                GroupBy vectorized: false
                                  keys: [CPDH,nf]
                                  values: [sum(JG)]
                                    Filter filter: NF in [2010,2017,2018,2022]
                                        Union All
                                            Union All
                                                Union All
                                                    Union All
                                                        Union All
                                                            VirtualRecord
                                                              functions: [2022,'C1',10]
                                                                long_sequence count: 1
                                                            VirtualRecord
                                                              functions: [2018,'C1',20]
                                                                long_sequence count: 1
                                                        VirtualRecord
                                                          functions: [2017,'C1',0]
                                                            long_sequence count: 1
                                                    VirtualRecord
                                                      functions: [2022,'C2',10]
                                                        long_sequence count: 1
                                                VirtualRecord
                                                  functions: [2010,'C2',30]
                                                    long_sequence count: 1
                                            VirtualRecord
                                              functions: [2010,'C3',80]
                                                long_sequence count: 1
                        """);
    }

    @Test
    public void testPivotWithCast() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t'2010'::int\t'2020'::long\n",
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, '2010'::int, '2020'::long)
                            GROUP BY country
                        ) order by country;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country	2000	'2010'::int	'2020'::long
                        NL	1005	1065	1158
                        US	8579	8783	9510
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                                Async Group By workers: 1
                                  keys: [country,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithComplexInitialStatement() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                """
                        (cities
                        WHERE (population % 2) = 0)
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country, name
                        ) order by country;""",
                ddlCities,
                null,
                dmlCities,
                """
                        country\tname\t2000_sum\t2010_sum\t2020_sum
                        NL\tAmsterdam\tnull\tnull\t1158
                        US\tSeattle\t564\t608\t738
                        US\tNew York City\tnull\tnull\t8772
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                                Async Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: (population%2=0 and year in [2000,2010,2020])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithCount() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country	2000_COUNT()	2000_last(population)	2010_COUNT()	2010_last(population)	2020_COUNT()	2020_last(population)	null_COUNT()	null_last(population)	2030_COUNT()	2030_last(population)
                            NL	1	1005	1	1065	1	1158	0	null	0	null
                            US	2	8015	2	8175	2	8772	0	null	0	null
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                COUNT(*),
                                last(population)
                                FOR year IN (2000, 2010, 2020, null, 2030)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithDoubleType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE measurements (sensor SYMBOL, metric SYMBOL, value DOUBLE);");
            execute("""
                    INSERT INTO measurements VALUES
                        ('S1', 'temp', 25.5),
                        ('S1', 'humidity', 60.2),
                        ('S2', 'temp', 28.3),
                        ('S2', 'humidity', 55.8);
                    """);

            assertQueryNoLeakCheck(
                    """
                            sensor	temp	humidity
                            S1	12.75	30.1
                            S2	14.15	27.9
                            """,
                    """
                            SELECT * FROM measurements
                            PIVOT (
                                avg(value) / 2
                                FOR metric IN ('temp', 'humidity')
                                GROUP BY sensor
                            ) ORDER BY sensor
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithDynamicInList() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    """
                            cities
                            PIVOT (
                                SUM(population)
                                FOR
                                    year IN (SELECT DISTINCT year FROM cities ORDER BY year)
                                GROUP BY country
                            ) order by country;
                            """;

            assertException(query, 60, "PIVOT IN subquery returned empty result set");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country\t2000\t2010\t2020
                            NL\t1005\t1065\t1158
                            US\t8579\t8783\t9510
                            """,
                    query,
                    null,
                    true,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    """
                            Sort light
                              keys: [country]
                                GroupBy vectorized: false
                                  keys: [country]
                                  values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                                    Async JIT Group By workers: 1
                                      keys: [country,year]
                                      values: [sum(population)]
                                      filter: year in [2000,2010,2020]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cities
                            """);
        });
    }

    @Test
    public void testPivotWithDynamicInListMultipleFor() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    """
                            cities
                            PIVOT (
                                SUM(population)
                                FOR
                                    year IN (SELECT DISTINCT year FROM cities ORDER BY year)
                                    name IN (SELECT DISTINCT name FROM cities ORDER BY name)
                            );
                            """;

            assertException(query, 60, "PIVOT IN subquery returned empty result set");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            2000_Amsterdam\t2000_New York City\t2000_Seattle\t2010_Amsterdam\t2010_New York City\t2010_Seattle\t2020_Amsterdam\t2020_New York City\t2020_Seattle
                            1005\t8015\t564\t1065\t8175\t608\t1158\t8772\t738
                            """,
                    query,
                    null,
                    false,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    """
                            GroupBy vectorized: false
                              values: [first_not_null(case([(year=2000 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2000 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2000 and name='Seattle'),SUM(population),null])),first_not_null(case([(year=2010 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2010 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2010 and name='Seattle'),SUM(population),null])),first_not_null(case([(year=2020 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2020 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2020 and name='Seattle'),SUM(population),null]))]
                                Async Group By workers: 1
                                  keys: [year,name]
                                  values: [sum(population)]
                                  filter: (year in [2000,2010,2020] and name in [Amsterdam,New York City,Seattle])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                            """);
        });
    }

    @Test
    public void testPivotWithDynamicInListMultipleForAndMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);

            String query =
                    """
                            cities
                            PIVOT (
                                SUM(population),
                                AVG(population)
                                FOR
                                    year IN (SELECT DISTINCT year FROM cities ORDER BY year)
                                    name IN (SELECT DISTINCT name FROM cities ORDER BY name)
                            );
                            """;

            assertException(query, 81, "PIVOT IN subquery returned empty result set");

            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            2000_Amsterdam_SUM(population)	2000_Amsterdam_AVG(population)	2000_New York City_SUM(population)	2000_New York City_AVG(population)	2000_Seattle_SUM(population)	2000_Seattle_AVG(population)	2010_Amsterdam_SUM(population)	2010_Amsterdam_AVG(population)	2010_New York City_SUM(population)	2010_New York City_AVG(population)	2010_Seattle_SUM(population)	2010_Seattle_AVG(population)	2020_Amsterdam_SUM(population)	2020_Amsterdam_AVG(population)	2020_New York City_SUM(population)	2020_New York City_AVG(population)	2020_Seattle_SUM(population)	2020_Seattle_AVG(population)
                            1005	1005.0	8015	8015.0	564	564.0	1065	1065.0	8175	8175.0	608	608.0	1158	1158.0	8772	8772.0	738	738.0
                            """,
                    query,
                    null,
                    false,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    """
                            GroupBy vectorized: false
                              values: [first_not_null(case([(year=2000 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2000 and name='Amsterdam'),AVG(population),null])),first_not_null(case([(year=2000 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2000 and name='New York City'),AVG(population),null])),first_not_null(case([(year=2000 and name='Seattle'),SUM(population),null])),first_not_null(case([(year=2000 and name='Seattle'),AVG(population),null])),first_not_null(case([(year=2010 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2010 and name='Amsterdam'),AVG(population),null])),first_not_null(case([(year=2010 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2010 and name='New York City'),AVG(population),null])),first_not_null(case([(year=2010 and name='Seattle'),SUM(population),null])),first_not_null(case([(year=2010 and name='Seattle'),AVG(population),null])),first_not_null(case([(year=2020 and name='Amsterdam'),SUM(population),null])),first_not_null(case([(year=2020 and name='Amsterdam'),AVG(population),null])),first_not_null(case([(year=2020 and name='New York City'),SUM(population),null])),first_not_null(case([(year=2020 and name='New York City'),AVG(population),null])),first_not_null(case([(year=2020 and name='Seattle'),SUM(population),null])),first_not_null(case([(year=2020 and name='Seattle'),AVG(population),null]))]
                                Async Group By workers: 1
                                  keys: [year,name]
                                  values: [sum(population),avg(population)]
                                  filter: (year in [2000,2010,2020] and name in [Amsterdam,New York City,Seattle])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                            """);
        });
    }

    @Test
    public void testPivotWithEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            assertQueryNoLeakCheck(
                    """
                            country\t2000\t2010\t2020
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (2000, 2010, 2020)
                                GROUP BY country
                            )
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country	0	10
                            NL	1005	1065
                            US	8579	8783
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year - 2000 IN (0, 10)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithForAliases() throws Exception {
        assertQueryAndPlan(
                "country\tD1\tD2\tD3\n",
                """
                        cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000 as D1, 2010 D2, 2020 as D3)
                            GROUP BY country
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country\tD1\tD2\tD3
                        NL\t1005\t1065\t1158
                        US\t8579\t8783\t9510
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [country]
                          values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [country,year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithGroupByAndLimit() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                """
                        cities
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country, name
                        ) order by country, name LIMIT 1 ;""",
                ddlCities,
                null,
                dmlCities,
                """
                        country\tname\t2000_sum\t2010_sum\t2020_sum
                        NL\tAmsterdam\t1005\t1065\t1158
                        """,
                true,
                true,
                false,
                """
                        Sort light lo: 1
                          keys: [country, name]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithGroupByAndOrderBy() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country, name
                        )  ORDER BY "2000_sum";""",
                ddlCities,
                null,
                dmlCities,
                """
                        country\tname\t2000_sum\t2010_sum\t2020_sum
                        US\tSeattle\t564\t608\t738
                        NL\tAmsterdam\t1005\t1065\t1158
                        US\tNew York City\t8015\t8175\t8772
                        """,
                true,
                true,
                false,
                """
                        Radix sort light
                          keys: [2000_sum]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithGroupByAndOrderByAndLimit() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                """
                        SELECT *
                        FROM cities
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country, name
                        ) ORDER BY "2000_sum" LIMIT 1;""",
                ddlCities,
                null,
                dmlCities,
                """
                        country\tname\t2000_sum\t2010_sum\t2020_sum
                        US\tSeattle\t564\t608\t738
                        """,
                true,
                true,
                false,
                """
                        Long Top K lo: 1
                          keys: [2000_sum asc]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);
            execute("CREATE TABLE country_info (code VARCHAR, continent VARCHAR);");
            execute("""
                    INSERT INTO country_info VALUES
                        ('NL', 'Europe'),
                        ('US', 'America');
                    """);

            assertQueryNoLeakCheck(
                    """
                            continent\t2000\t2010\t2020
                            America\t8579\t8783\t9510
                            Europe\t1005\t1065\t1158
                            """,
                    """
                            SELECT continent, "2000", "2010", "2020" FROM (
                                SELECT ci.continent, c.year, c.population
                                FROM cities c
                                JOIN country_info ci ON c.country = ci.code
                            )
                            PIVOT (
                                SUM(population)
                                FOR year IN (2000, 2010, 2020)
                                GROUP BY continent
                            ) ORDER BY continent
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithLatestOnGroupBy() throws Exception {
        assertQueryAndPlan(
                "side\tETH-USDT\tBTC-USDT\tDOGE-USDT\n",
                """
                        (select side, symbol, last(price) as price from trades group by side, symbol)
                          pivot (
                            last(price)
                            FOR "symbol" IN ('ETH-USDT', 'BTC-USDT', 'DOGE-USDT')
                            GROUP BY side
                          ) ORDER BY side;""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        side\tETH-USDT\tBTC-USDT\tDOGE-USDT
                        buy\t3678.01\t101497.6\t0.36047
                        sell\t3678.0\t101497.0\t0.36041
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [side]
                            GroupBy vectorized: false
                              keys: [side]
                              values: [first_not_null(case([last(price),NaN,symbol])),first_not_null(case([last(price),NaN,symbol])),first_not_null(case([last(price),NaN,symbol]))]
                                GroupBy vectorized: false
                                  keys: [side,symbol]
                                  values: [last(price)]
                                    Async JIT Group By workers: 1
                                      keys: [side,symbol]
                                      values: [last(price)]
                                      filter: symbol in [ETH-USDT,BTC-USDT,DOGE-USDT]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithMinMax() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            assertQueryNoLeakCheck(
                    """
                            country\t2000_min\t2000_max\t2010_min\t2010_max\t2020_min\t2020_max
                            NL\t1005\t1005\t1065\t1065\t1158\t1158
                            US\t564\t8015\t608\t8175\t738\t8772
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                MIN(population) AS min, MAX(population) AS max
                                FOR year IN (2000, 2010, 2020)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithMultipleAggregates() throws Exception {
        assertQueryAndPlan(
                "country\t2000_SUM(population)\t2000_AVG(population)\t2010_SUM(population)\t2010_AVG(population)\t2020_SUM(population)\t2020_AVG(population)\n",
                """
                        cities
                        PIVOT (
                            SUM(population),
                            AVG(population)
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country
                        ) order by country;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country	2000_SUM(population)	2000_AVG(population)	2010_SUM(population)	2010_AVG(population)	2020_SUM(population)	2020_AVG(population)
                        NL	1005	1005.0	1065	1065.0	1158	1158.0
                        US	8579	4289.5	8783	4391.5	9510	4755.0
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([AVG(population),NaN,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([AVG(population),NaN,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([AVG(population),NaN,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,year]
                                  values: [sum(population),avg(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """
        );
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesExplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                "name\t2000_NL_total\t2000_NL_count\t2000_NL_count_dis\t2000_US_total\t2000_US_count\t2000_US_count_dis\t2010_NL_total\t2010_NL_count\t2010_NL_count_dis\t2010_US_total\t2010_US_count\t2010_US_count_dis\n",
                """
                        cities
                        PIVOT (
                            SUM(population) as total,
                            COUNT(population) as count,
                            COUNT(distinct population) as count_dis
                            FOR
                                year IN (2000, 2010)
                                country IN ('NL', 'US')
                            GROUP BY name
                        ) order by name;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        name	2000_NL_total	2000_NL_count	2000_NL_count_dis	2000_US_total	2000_US_count	2000_US_count_dis	2010_NL_total	2010_NL_count	2010_NL_count_dis	2010_US_total	2010_US_count	2010_US_count_dis
                        Amsterdam	1005	1	1	null	0	0	1065	1	1	null	0	0
                        New York City	null	0	0	8015	1	1	null	0	0	8175	1	1
                        Seattle	null	0	0	564	1	1	null	0	0	608	1	1
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [name]
                            GroupBy vectorized: false
                              keys: [name]
                              values: [first_not_null(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),sum(case([(year=2000 and country='NL'),count_dis,0])),first_not_null(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),sum(case([(year=2000 and country='US'),count_dis,0])),first_not_null(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),sum(case([(year=2010 and country='NL'),count_dis,0])),first_not_null(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0])),sum(case([(year=2010 and country='US'),count_dis,0]))]
                                Async Group By workers: 1
                                  keys: [name,year,country]
                                  values: [sum(population),count(population),count_distinct(population)]
                                  filter: (year in [2000,2010] and country in [NL,US])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesExplicitGroupByWithForAliases() throws Exception {
        assertQueryAndPlan(
                "name\t2K00_Netherlands_total\t2K00_Netherlands_count\t2K00_United States_total\t2K00_United States_count\t2K10_Netherlands_total\t2K10_Netherlands_count\t2K10_United States_total\t2K10_United States_count\n",
                """
                        cities
                        PIVOT (
                            SUM(population) as total,
                            COUNT(population) as count
                            FOR
                                year IN (2000 AS '2K00', 2010 AS '2K10')
                                country IN ('NL' AS Netherlands, 'US' AS 'United States')
                            GROUP BY name
                        ) order by name;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        name	2K00_Netherlands_total	2K00_Netherlands_count	2K00_United States_total	2K00_United States_count	2K10_Netherlands_total	2K10_Netherlands_count	2K10_United States_total	2K10_United States_count
                        Amsterdam	1005	1	null	0	1065	1	null	0
                        New York City	null	0	8015	1	null	0	8175	1
                        Seattle	null	0	564	1	null	0	608	1
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [name]
                            GroupBy vectorized: false
                              keys: [name]
                              values: [first_not_null(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),first_not_null(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),first_not_null(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),first_not_null(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0]))]
                                Async Group By workers: 1
                                  keys: [name,year,country]
                                  values: [sum(population),count(population)]
                                  filter: (year in [2000,2010] and country in [NL,US])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithMultipleAliasedAggregatesImplicitGroupBy() throws Exception {
        assertQueryAndPlan(
                """
                        2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count
                        null\tnull\tnull\tnull\tnull\tnull\tnull\tnull
                        """,
                """
                        cities
                        PIVOT (
                            SUM(population) as total,
                            COUNT(population) as count
                            FOR
                                year IN (2000, 2010)
                                country IN ('NL', 'US')
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000_NL_total\t2000_NL_count\t2000_US_total\t2000_US_count\t2010_NL_total\t2010_NL_count\t2010_US_total\t2010_US_count
                        1005\t1\t8579\t2\t1065\t1\t8783\t2
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([(year=2000 and country='NL'),total,null])),sum(case([(year=2000 and country='NL'),count,0])),first_not_null(case([(year=2000 and country='US'),total,null])),sum(case([(year=2000 and country='US'),count,0])),first_not_null(case([(year=2010 and country='NL'),total,null])),sum(case([(year=2010 and country='NL'),count,0])),first_not_null(case([(year=2010 and country='US'),total,null])),sum(case([(year=2010 and country='US'),count,0]))]
                            Async Group By workers: 1
                              keys: [year,country]
                              values: [sum(population),count(population)]
                              filter: (year in [2000,2010] and country in [NL,US])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithMultipleFor() throws Exception {
        assertQueryAndPlan(
                "country\t2000_NL\n",
                """
                        cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000)
                                country IN ('NL')
                            GROUP BY country
                        ) order by country;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country	2000_NL
                        NL	1005
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([(year=2000 and country_2='NL'),SUM(population),null]))]
                                SelectedRecord
                                    Async Group By workers: 1
                                      keys: [country,year]
                                      values: [sum(population)]
                                      filter: (year in [2000] and country in [NL])
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithMultipleForAndAggregatesOrderedAndLimited() throws Exception {
        assertQueryAndPlan(
                "country\t2000_NL_SUM(population)\t2000_NL_AVG(population)\n",
                """
                        cities
                        PIVOT (
                            SUM(population),
                            AVG(population)
                            FOR
                                year IN (2000)
                                country IN ('NL')
                            GROUP BY country
                        ) ORDER BY country DESC LIMIT 1;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country	2000_NL_SUM(population)	2000_NL_AVG(population)
                        NL	1005	1005.0
                        """,
                true,
                true,
                false,
                """
                        Sort light lo: 1
                          keys: [country desc]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([(year=2000 and country_2='NL'),SUM(population),null])),first_not_null(case([(year=2000 and country_2='NL'),AVG(population),null]))]
                                SelectedRecord
                                    Async Group By workers: 1
                                      keys: [country,year]
                                      values: [sum(population),avg(population)]
                                      filter: (year in [2000] and country in [NL])
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithMultipleForExprs() throws Exception {
        assertQueryAndPlan(
                "name\t2000_NL\t2000_US\t2010_NL\t2010_US\t2020_NL\t2020_US\n",
                """
                        cities PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                                country in ('NL', 'US')
                            GROUP BY name
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        name\t2000_NL\t2000_US\t2010_NL\t2010_US\t2020_NL\t2020_US
                        Amsterdam\t1005\tnull\t1065\tnull\t1158\tnull
                        Seattle\tnull\t564\tnull\t608\tnull\t738
                        New York City\tnull\t8015\tnull\t8175\tnull\t8772
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [name]
                          values: [first_not_null(case([(year=2000 and country='NL'),SUM(population),null])),first_not_null(case([(year=2000 and country='US'),SUM(population),null])),first_not_null(case([(year=2010 and country='NL'),SUM(population),null])),first_not_null(case([(year=2010 and country='US'),SUM(population),null])),first_not_null(case([(year=2020 and country='NL'),SUM(population),null])),first_not_null(case([(year=2020 and country='US'),SUM(population),null]))]
                            Async Group By workers: 1
                              keys: [name,year,country]
                              values: [sum(population)]
                              filter: (year in [2000,2010,2020] and country in [NL,US])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """
        );
    }

    @Test
    public void testPivotWithMultipleForExprsAndMultipleAggregates() throws Exception {
        assertQueryAndPlan(
                """
                        2000_Amsterdam_NL_SUM(population)	2000_Amsterdam_NL_COUNT(population)	2000_Amsterdam_US_SUM(population)	2000_Amsterdam_US_COUNT(population)	2000_Seattle_NL_SUM(population)	2000_Seattle_NL_COUNT(population)	2000_Seattle_US_SUM(population)	2000_Seattle_US_COUNT(population)	2000_New York City_NL_SUM(population)	2000_New York City_NL_COUNT(population)	2000_New York City_US_SUM(population)	2000_New York City_US_COUNT(population)	2010_Amsterdam_NL_SUM(population)	2010_Amsterdam_NL_COUNT(population)	2010_Amsterdam_US_SUM(population)	2010_Amsterdam_US_COUNT(population)	2010_Seattle_NL_SUM(population)	2010_Seattle_NL_COUNT(population)	2010_Seattle_US_SUM(population)	2010_Seattle_US_COUNT(population)	2010_New York City_NL_SUM(population)	2010_New York City_NL_COUNT(population)	2010_New York City_US_SUM(population)	2010_New York City_US_COUNT(population)	2020_Amsterdam_NL_SUM(population)	2020_Amsterdam_NL_COUNT(population)	2020_Amsterdam_US_SUM(population)	2020_Amsterdam_US_COUNT(population)	2020_Seattle_NL_SUM(population)	2020_Seattle_NL_COUNT(population)	2020_Seattle_US_SUM(population)	2020_Seattle_US_COUNT(population)	2020_New York City_NL_SUM(population)	2020_New York City_NL_COUNT(population)	2020_New York City_US_SUM(population)	2020_New York City_US_COUNT(population)
                        null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null
                        """,
                """
                        cities
                        PIVOT (
                            SUM(population),
                            COUNT(population)
                            FOR
                                year IN (2000, 2010, 2020)
                                name IN ( 'Amsterdam', 'Seattle', 'New York City')
                                country in ('NL', 'US')
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000_Amsterdam_NL_SUM(population)	2000_Amsterdam_NL_COUNT(population)	2000_Amsterdam_US_SUM(population)	2000_Amsterdam_US_COUNT(population)	2000_Seattle_NL_SUM(population)	2000_Seattle_NL_COUNT(population)	2000_Seattle_US_SUM(population)	2000_Seattle_US_COUNT(population)	2000_New York City_NL_SUM(population)	2000_New York City_NL_COUNT(population)	2000_New York City_US_SUM(population)	2000_New York City_US_COUNT(population)	2010_Amsterdam_NL_SUM(population)	2010_Amsterdam_NL_COUNT(population)	2010_Amsterdam_US_SUM(population)	2010_Amsterdam_US_COUNT(population)	2010_Seattle_NL_SUM(population)	2010_Seattle_NL_COUNT(population)	2010_Seattle_US_SUM(population)	2010_Seattle_US_COUNT(population)	2010_New York City_NL_SUM(population)	2010_New York City_NL_COUNT(population)	2010_New York City_US_SUM(population)	2010_New York City_US_COUNT(population)	2020_Amsterdam_NL_SUM(population)	2020_Amsterdam_NL_COUNT(population)	2020_Amsterdam_US_SUM(population)	2020_Amsterdam_US_COUNT(population)	2020_Seattle_NL_SUM(population)	2020_Seattle_NL_COUNT(population)	2020_Seattle_US_SUM(population)	2020_Seattle_US_COUNT(population)	2020_New York City_NL_SUM(population)	2020_New York City_NL_COUNT(population)	2020_New York City_US_SUM(population)	2020_New York City_US_COUNT(population)
                        1005	1	null	0	null	0	564	1	null	0	8015	1	1065	1	null	0	null	0	608	1	null	0	8175	1	1158	1	null	0	null	0	738	1	null	0	8772	1
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([(year=2000 and name='Amsterdam' and country='NL'),SUM(population),null])),sum(case([(year=2000 and name='Amsterdam' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2000 and name='Amsterdam' and country='US'),SUM(population),null])),sum(case([(year=2000 and name='Amsterdam' and country='US'),COUNT(population),0])),first_not_null(case([(year=2000 and name='Seattle' and country='NL'),SUM(population),null])),sum(case([(year=2000 and name='Seattle' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2000 and name='Seattle' and country='US'),SUM(population),null])),sum(case([(year=2000 and name='Seattle' and country='US'),COUNT(population),0])),first_not_null(case([(year=2000 and name='New York City' and country='NL'),SUM(population),null])),sum(case([(year=2000 and name='New York City' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2000 and name='New York City' and country='US'),SUM(population),null])),sum(case([(year=2000 and name='New York City' and country='US'),COUNT(population),0])),first_not_null(case([(year=2010 and name='Amsterdam' and country='NL'),SUM(population),null])),sum(case([(year=2010 and name='Amsterdam' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2010 and name='Amsterdam' and country='US'),SUM(population),null])),sum(case([(year=2010 and name='Amsterdam' and country='US'),COUNT(population),0])),first_not_null(case([(year=2010 and name='Seattle' and country='NL'),SUM(population),null])),sum(case([(year=2010 and name='Seattle' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2010 and name='Seattle' and country='US'),SUM(population),null])),sum(case([(year=2010 and name='Seattle' and country='US'),COUNT(population),0])),first_not_null(case([(year=2010 and name='New York City' and country='NL'),SUM(population),null])),sum(case([(year=2010 and name='New York City' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2010 and name='New York City' and country='US'),SUM(population),null])),sum(case([(year=2010 and name='New York City' and country='US'),COUNT(population),0])),first_not_null(case([(year=2020 and name='Amsterdam' and country='NL'),SUM(population),null])),sum(case([(year=2020 and name='Amsterdam' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2020 and name='Amsterdam' and country='US'),SUM(population),null])),sum(case([(year=2020 and name='Amsterdam' and country='US'),COUNT(population),0])),first_not_null(case([(year=2020 and name='Seattle' and country='NL'),SUM(population),null])),sum(case([(year=2020 and name='Seattle' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2020 and name='Seattle' and country='US'),SUM(population),null])),sum(case([(year=2020 and name='Seattle' and country='US'),COUNT(population),0])),first_not_null(case([(year=2020 and name='New York City' and country='NL'),SUM(population),null])),sum(case([(year=2020 and name='New York City' and country='NL'),COUNT(population),0])),first_not_null(case([(year=2020 and name='New York City' and country='US'),SUM(population),null])),sum(case([(year=2020 and name='New York City' and country='US'),COUNT(population),0]))]
                            Async Group By workers: 1
                              keys: [year,name,country]
                              values: [sum(population),count(population)]
                              filter: (year in [2000,2010,2020] and name in [Amsterdam,Seattle,New York City] and country in [NL,US])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """
        );
    }

    @Test
    public void testPivotWithMultipleGroupBy() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000\t2010\t2020\n",
                """
                        cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                                GROUP BY country, name
                        ) order by country, name;
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country	name	2000	2010	2020
                        NL	Amsterdam	1005	1065	1158
                        US	New York City	8015	8175	8772
                        US	Seattle	564	608	738
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country, name]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """
        );
    }

    @Test
    public void testPivotWithNoMatchingForValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            // DUCKDB result:
            // ┌─────────┬────────┬────────┐
            // │ country │  1990  │  1995  │
            // │ varchar │ int128 │ int128 │
            // ├─────────┼────────┼────────┤
            // │ NL      │   NULL │   NULL │
            // │ US      │   NULL │   NULL │
            // └─────────┴────────┴────────┘
            //
            // We return empty rows because pushPivotFiltersToInnerModel() pushes IN filter
            // conditions down to the inner query, so no rows match when FOR values don't
            // exist in the data (e.g., year IN (1990, 1995) filters out all rows).
            // This behavior is kept for performance reasons.
            assertQueryNoLeakCheck(
                    """
                            country\t1990\t1995
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (1990, 1995)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithNonDistinctQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);

            String query = """
                    SELECT * FROM (
                         SELECT timestamp, symbol, side, AVG(price) price, AVG(amount) amount FROM trades SAMPLE BY 100T
                    )
                    PIVOT (
                        sum(price)
                        FOR symbol IN (select symbol from trades ORDER BY symbol LIMIT 10)
                            side IN ('buy', 'sell')
                        GROUP BY timestamp
                    ) ORDER BY timestamp
                    """;

            assertQueryNoLeakCheck(
                    """
                            timestamp	ADA-USD_buy	ADA-USD_sell	ADA-USDT_buy	ADA-USDT_sell	BTC-USD_buy	BTC-USD_sell
                            2024-12-19T08:10:00.000000Z	null	0.9716	null	0.9716	null	null
                            2024-12-19T08:10:00.100000Z	null	null	null	null	101502.2	101502.1
                            2024-12-19T08:10:00.200000Z	null	null	null	null	101502.2	null
                            2024-12-19T08:10:00.400000Z	null	null	null	null	101502.2	null
                            2024-12-19T08:10:00.500000Z	null	0.9716	null	0.9716	null	null
                            2024-12-19T08:10:00.600000Z	null	null	null	null	101502.2	null
                            2024-12-19T08:10:00.700000Z	null	null	null	null	101500.66666666667	101500.57777777778
                            2024-12-19T08:10:00.900000Z	null	null	null	null	101497.6	101497.25
                            """,
                    query,
                    "timestamp",
                    true,
                    true,
                    false
            );

            assertPlanNoLeakCheck(query,
                    """
                            Radix sort light
                              keys: [timestamp]
                                GroupBy vectorized: false
                                  keys: [timestamp]
                                  values: [first_not_null(case([(symbol='ADA-USD' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ADA-USD' and side='sell'),sum(price),null])),first_not_null(case([(symbol='ADA-USDT' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ADA-USDT' and side='sell'),sum(price),null])),first_not_null(case([(symbol='BTC-USD' and side='buy'),sum(price),null])),first_not_null(case([(symbol='BTC-USD' and side='sell'),sum(price),null]))]
                                    GroupBy vectorized: false
                                      keys: [timestamp,symbol,side]
                                      values: [sum(price)]
                                        Radix sort light
                                          keys: [timestamp]
                                            Async JIT Group By workers: 1
                                              keys: [timestamp,symbol,side]
                                              values: [avg(price)]
                                              filter: (symbol in [ADA-USD,ADA-USDT,BTC-USD] and side in [buy,sell])
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testPivotWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (category SYMBOL, type SYMBOL, value INT);");
            execute("""
                    INSERT INTO data VALUES
                        ('A', 'X', 10),
                        ('A', 'Y', null),
                        ('B', 'X', null),
                        ('B', 'Y', 30),
                        ('C', 'X', 50),
                        ('C', 'Y', 60),
                        ('D', 'y', 70),
                        ('D', null, 80),
                        ('E', 'null', 90);
                    """);

            assertQueryNoLeakCheck(
                    """
                            category	NULL	X	Y	null_2	y_2
                            A	null	21	null	null	null
                            B	null	null	41	null	null
                            C	null	61	71	null	null
                            D	91	null	null	null	81
                            E	null	null	null	101	null
                            """,
                    """
                            SELECT * FROM data
                            PIVOT (
                                SUM(value + 1) + 10
                                FOR type IN (select distinct type from data order by type)
                                GROUP BY category
                            ) ORDER BY category
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithOrderBy() throws Exception {
        assertQueryAndPlan(
                "country\t2000\t2010\t2020\n",
                """
                        cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country
                        )   ORDER BY "2000";
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        country\t2000\t2010\t2020
                        NL\t1005\t1065\t1158
                        US\t8579\t8783\t9510
                        """,
                true,
                true,
                false,
                """
                        Radix sort light
                          keys: [2000]
                            GroupBy vectorized: false
                              keys: [country]
                              values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                                Async JIT Group By workers: 1
                                  keys: [country,year]
                                  values: [sum(population)]
                                  filter: year in [2000,2010,2020]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }

    // Tests for printRecordColumnOrNull - various data types in PIVOT IN subqueries

    @Test
    public void testPivotWithOrderByNotPresentInForOrGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlMonthlySales);
            execute(dmlMonthlySales);

            assertException("""
                            monthly_sales\s
                            PIVOT (
                              SUM(amount)\s
                              FOR MONTH IN ('JAN', 'FEB', 'MAR')\s
                            )ORDER BY EMPID;""",
                    86,
                    "Invalid column: EMPID");

            assertSql("""
                            EMPID	JAN	FEB	MAR
                            1	10400	8000	11000
                            2	39500	90700	12000
                            """,
                    """
                            monthly_sales\s
                            PIVOT (
                              SUM(amount)\s
                              FOR MONTH IN ('JAN', 'FEB', 'MAR')\s
                              GROUP BY EMPID
                            ) ORDER BY EMPID;""");
        });
    }

    @Test
    public void testPivotWithSampleBy() throws Exception {
        assertQueryAndPlan(
                "symbol\tbuy_price\tsell_price\n",
                """
                        (
                          SELECT timestamp, symbol, side, last(price)
                          FROM trades
                          SAMPLE BY 1d
                        ) PIVOT (
                          sum(last) as price
                          FOR side in ('buy', 'sell')
                          GROUP BY symbol
                        ) order by symbol;
                        """,
                ddlTrades,
                null,
                dmlTrades,
                """
                        symbol	buy_price	sell_price
                        ADA-USD	null	0.9716
                        ADA-USDT	null	0.9716
                        BTC-USD	101497.6	101497.0
                        BTC-USDT	101497.6	101497.0
                        DOGE-USD	0.36047	0.36041
                        DOGE-USDT	0.36047	0.36041
                        ETH-USD	3678.01	3678.0
                        ETH-USDC	null	3675.72
                        ETH-USDT	3678.01	3678.0
                        SOL-USD	null	210.41
                        SOL-USDT	null	210.41
                        USDT-USDC	0.9994	null
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [symbol]
                            GroupBy vectorized: false
                              keys: [symbol]
                              values: [first_not_null(case([price,NaN,side])),first_not_null(case([price,NaN,side]))]
                                GroupBy vectorized: false
                                  keys: [symbol,side]
                                  values: [sum(last)]
                                    Radix sort light
                                      keys: [timestamp]
                                        Async JIT Group By workers: 1
                                          keys: [symbol,side,timestamp]
                                          values: [last(price)]
                                          filter: side in [buy,sell]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithStringType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE logs (category SYMBOL, status SYMBOL, message STRING);");
            execute("""
                    INSERT INTO logs VALUES
                        ('error', 'critical', 'msg1'),
                        ('error', 'warning', 'msg2'),
                        ('info', 'critical', 'msg3'),
                        ('info', 'warning', 'msg4');
                    """);

            assertQueryNoLeakCheck(
                    """
                            category\tcritical\twarning
                            error\tmsg1\tmsg2
                            info\tmsg3\tmsg4
                            """,
                    """
                            SELECT * FROM logs
                            PIVOT (
                                last(message)
                                FOR status IN ('critical', 'warning')
                                GROUP BY category
                            ) ORDER BY category
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithSubqueryDuplicateValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);
            execute("CREATE TABLE years (y INT);");
            execute("INSERT INTO years VALUES (2000), (2000), (2010);");

            assertQueryNoLeakCheck(
                    """
                            country\t2000\t2010
                            NL\t1005\t1065
                            US\t8579\t8783
                            """,
                    """
                            SELECT * FROM cities
                            PIVOT (
                                SUM(population)
                                FOR year IN (SELECT y FROM years)
                                GROUP BY country
                            ) ORDER BY country
                            """,
                    null,
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, category SYMBOL, value INT) timestamp(ts);");
            execute("""
                    INSERT INTO events VALUES
                        ('2024-01-01', 'A', 10),
                        ('2024-01-01', 'B', 20),
                        ('2024-01-02', 'A', 30),
                        ('2024-01-02', 'B', 40);
                    """);

            assertQueryNoLeakCheck(
                    """
                            ts\tA\tB
                            2024-01-01T00:00:00.000000Z\t10\t20
                            2024-01-02T00:00:00.000000Z\t30\t40
                            """,
                    """
                            SELECT * FROM events
                            PIVOT (
                                SUM(value)
                                FOR category IN ('A', 'B')
                                GROUP BY ts
                            ) ORDER BY ts
                            """,
                    "ts",
                    true,
                    true,
                    false);
        });
    }

    @Test
    public void testPivotWithTimestampGrouping() throws Exception {
        assertQueryAndPlan(
                """
                        2000\t2010\t2020
                        null\tnull\tnull
                        """,
                """
                        cities
                        PIVOT (
                            SUM(population)
                            FOR
                                year IN (2000, 2010, 2020)
                        );
                        """,
                ddlCities,
                null,
                dmlCities,
                """
                        2000\t2010\t2020
                        9584\t9848\t10668
                        """,
                false,
                true,
                false,
                """
                        GroupBy vectorized: false
                          values: [first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year])),first_not_null(case([SUM(population),nullL,year]))]
                            Async JIT Group By workers: 1
                              keys: [year]
                              values: [sum(population)]
                              filter: year in [2000,2010,2020]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cities
                        """);
    }

    @Test
    public void testPivotWithTradesData() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                """
                        (select * from trades where symbol in 'ETH-USDT')
                          pivot (
                            sum(price)
                            FOR "symbol" IN ('ETH-USDT')
                                side in ('buy', 'sell')
                            GROUP BY timestamp
                          ) order by timestamp;""",
                ddlTrades,
                "timestamp",
                dmlTrades,
                """
                        timestamp\tETH-USDT_buy\tETH-USDT_sell
                        2024-12-19T08:10:00.700999Z\tnull\t3678.25
                        2024-12-19T08:10:00.736000Z\tnull\t3678.25
                        2024-12-19T08:10:00.759000Z\tnull\t3678.0
                        2024-12-19T08:10:00.772999Z\tnull\t3678.0
                        2024-12-19T08:10:00.887000Z\t3678.01\tnull
                        2024-12-19T08:10:00.950000Z\tnull\t3678.0
                        """,
                true,
                true,
                false,
                """
                        Radix sort light
                          keys: [timestamp]
                            GroupBy vectorized: false
                              keys: [timestamp]
                              values: [first_not_null(case([(symbol='ETH-USDT' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ETH-USDT' and side='sell'),sum(price),null]))]
                                Async Group By workers: 1
                                  keys: [timestamp,symbol,side]
                                  values: [sum(price)]
                                  filter: (symbol in [ETH-USDT] and symbol in [ETH-USDT] and side in [buy,sell])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithTradesDataAndLimit() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                """
                        trades
                          PIVOT (
                            sum(price)
                            FOR "symbol" IN ('ETH-USDT')
                                side in ('buy', 'sell')
                            GROUP BY timestamp
                          ) order by timestamp LIMIT 3;""",
                ddlTrades,
                "timestamp",
                dmlTrades,
                """
                        timestamp\tETH-USDT_buy\tETH-USDT_sell
                        2024-12-19T08:10:00.700999Z\tnull\t3678.25
                        2024-12-19T08:10:00.736000Z\tnull\t3678.25
                        2024-12-19T08:10:00.759000Z\tnull\t3678.0
                        """,
                true,
                true,
                false,
                """
                        Long Top K lo: 3
                          keys: [timestamp asc]
                            GroupBy vectorized: false
                              keys: [timestamp]
                              values: [first_not_null(case([(symbol='ETH-USDT' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ETH-USDT' and side='sell'),sum(price),null]))]
                                Async JIT Group By workers: 1
                                  keys: [timestamp,symbol,side]
                                  values: [sum(price)]
                                  filter: (symbol in [ETH-USDT] and side in [buy,sell])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithTradesDataAndOrderByAsc() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                """
                        trades
                          PIVOT (
                            sum(price)
                            FOR "symbol" IN ('ETH-USDT')
                                side in ('buy', 'sell')
                            GROUP BY timestamp
                          ) ORDER BY timestamp ASC;""",
                ddlTrades,
                "timestamp###ASC",
                dmlTrades,
                """
                        timestamp\tETH-USDT_buy\tETH-USDT_sell
                        2024-12-19T08:10:00.700999Z\tnull\t3678.25
                        2024-12-19T08:10:00.736000Z\tnull\t3678.25
                        2024-12-19T08:10:00.759000Z\tnull\t3678.0
                        2024-12-19T08:10:00.772999Z\tnull\t3678.0
                        2024-12-19T08:10:00.887000Z\t3678.01\tnull
                        2024-12-19T08:10:00.950000Z\tnull\t3678.0
                        """,
                true,
                true,
                false,
                """
                        Radix sort light
                          keys: [timestamp]
                            GroupBy vectorized: false
                              keys: [timestamp]
                              values: [first_not_null(case([(symbol='ETH-USDT' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ETH-USDT' and side='sell'),sum(price),null]))]
                                Async JIT Group By workers: 1
                                  keys: [timestamp,symbol,side]
                                  values: [sum(price)]
                                  filter: (symbol in [ETH-USDT] and side in [buy,sell])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """
        );
    }

    @Test
    public void testPivotWithTradesDataAndOrderByDesc() throws Exception {
        assertQueryAndPlan(
                "timestamp\tETH-USDT_buy\tETH-USDT_sell\n",
                """
                        trades
                          PIVOT (
                            sum(price)
                            FOR "symbol" IN ('ETH-USDT')
                                side in ('buy', 'sell')
                            GROUP BY timestamp
                          ) ORDER BY timestamp DESC;""",
                ddlTrades,
                "timestamp###DESC",
                dmlTrades,
                """
                        timestamp\tETH-USDT_buy\tETH-USDT_sell
                        2024-12-19T08:10:00.950000Z\tnull\t3678.0
                        2024-12-19T08:10:00.887000Z\t3678.01\tnull
                        2024-12-19T08:10:00.772999Z\tnull\t3678.0
                        2024-12-19T08:10:00.759000Z\tnull\t3678.0
                        2024-12-19T08:10:00.736000Z\tnull\t3678.25
                        2024-12-19T08:10:00.700999Z\tnull\t3678.25
                        """,
                true,
                true,
                false,
                """
                        Radix sort light
                          keys: [timestamp desc]
                            GroupBy vectorized: false
                              keys: [timestamp]
                              values: [first_not_null(case([(symbol='ETH-USDT' and side='buy'),sum(price),null])),first_not_null(case([(symbol='ETH-USDT' and side='sell'),sum(price),null]))]
                                Async JIT Group By workers: 1
                                  keys: [timestamp,symbol,side]
                                  values: [sum(price)]
                                  filter: (symbol in [ETH-USDT] and side in [buy,sell])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """
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
                    """
                              trades PIVOT (
                                sum(price)
                                FOR "symbol" IN ('ETH-USDT')
                                    side in ('buy', 'sell')
                                GROUP BY timestamp
                              )ORDER BY timestamp ASC;
                            """;

            assertQuery("""
                            timestamp\tETH-USDT_buy\tETH-USDT_sell
                            2024-12-19T08:10:00.700999Z\tnull\t3678.25
                            2024-12-19T08:10:00.736000Z\tnull\t3678.25
                            2024-12-19T08:10:00.759000Z\tnull\t3678.0
                            2024-12-19T08:10:00.772999Z\tnull\t3678.0
                            2024-12-19T08:10:00.887000Z\t3678.01\tnull
                            2024-12-19T08:10:00.950000Z\tnull\t3678.0
                            """,
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
                """
                        SELECT * FROM (
                        SELECT * FROM (
                             SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount FROM trades WHERE symbol IN 'BTC-USD'
                        )
                        PIVOT (
                            sum(price)
                            FOR symbol IN ('BTC-USD')
                                side IN ('buy', 'sell')
                            GROUP BY timestamp
                        )
                        );""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        timestamp\tBTC-USD_buy\tBTC-USD_sell
                        2024-12-19T08:10:00.136000Z\t101502.2\tnull
                        2024-12-19T08:10:00.138000Z\tnull\t101502.1
                        2024-12-19T08:10:00.244000Z\t101502.2\tnull
                        2024-12-19T08:10:00.424000Z\t101502.2\tnull
                        2024-12-19T08:10:00.600000Z\t101502.2\tnull
                        2024-12-19T08:10:00.665999Z\t101502.2\tnull
                        2024-12-19T08:10:00.693000Z\t101502.2\tnull
                        2024-12-19T08:10:00.716999Z\t101502.2\tnull
                        2024-12-19T08:10:00.724000Z\t101502.2\tnull
                        2024-12-19T08:10:00.732999Z\tnull\t101501.06
                        2024-12-19T08:10:00.733999Z\tnull\t101500.0
                        2024-12-19T08:10:00.734999Z\tnull\t101499.95
                        2024-12-19T08:10:00.744000Z\t101497.6\tnull
                        2024-12-19T08:10:00.926000Z\t101497.6\tnull
                        2024-12-19T08:10:00.932000Z\tnull\t101497.25
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [timestamp]
                          values: [first_not_null(case([(symbol='BTC-USD' and side='buy'),sum(price),null])),first_not_null(case([(symbol='BTC-USD' and side='sell'),sum(price),null]))]
                            GroupBy vectorized: false
                              keys: [timestamp,symbol,side]
                              values: [sum(price)]
                                Async Group By workers: 1
                                  keys: [timestamp,symbol,side]
                                  values: [avg(price)]
                                  filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithTradesDataAndWithClause() throws Exception {
        assertQueryAndPlan(
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n",
                """
                        WITH p AS\s
                        (WITH t AS
                        (
                        
                            SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount FROM trades WHERE symbol IN 'BTC-USD'
                            SAMPLE BY 1m
                        )
                        SELECT * FROM t
                        PIVOT (
                            sum(price)
                            FOR symbol IN ('BTC-USD')   \s
                            side IN ('buy', 'sell')  \s
                            GROUP BY timestamp
                        ) ) SELECT * from p where `BTC-USD_buy` > 25780 or `BTC-USD_sell` > 25780;""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        timestamp\tBTC-USD_buy\tBTC-USD_sell
                        2024-12-19T08:10:00.000000Z\t101501.27999999998\t101500.15000000002
                        """,
                true,
                false,
                false,
                """
                        Filter filter: (25780<BTC-USD_buy or 25780<BTC-USD_sell)
                            GroupBy vectorized: false
                              keys: [timestamp]
                              values: [first_not_null(case([(symbol='BTC-USD' and side='buy'),sum(price),null])),first_not_null(case([(symbol='BTC-USD' and side='sell'),sum(price),null]))]
                                GroupBy vectorized: false
                                  keys: [timestamp,symbol,side]
                                  values: [sum(price)]
                                    Radix sort light
                                      keys: [timestamp]
                                        Async Group By workers: 1
                                          keys: [timestamp,symbol,side]
                                          values: [avg(price)]
                                          filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithTradesDataAndWithClause2() throws Exception {
        assertQueryAndPlan(
                "timestamp\tBTC-USD_buy\tBTC-USD_sell\n",
                """
                        WITH t AS
                                (
                        
                                        SELECT timestamp, symbol,  side, AVG(price) price, AVG(amount) amount\s
                        FROM trades WHERE symbol IN 'BTC-USD'
                        SAMPLE BY 1m
                        ), P AS (
                                SELECT * FROM t
                                PIVOT (
                                sum(price)
                        FOR symbol IN ('BTC-USD')
                        side IN ('buy', 'sell')
                        GROUP BY timestamp
                        ) )
                        SELECT * FROM P;""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        timestamp\tBTC-USD_buy\tBTC-USD_sell
                        2024-12-19T08:10:00.000000Z\t101501.27999999998\t101500.15000000002
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [timestamp]
                          values: [first_not_null(case([(symbol='BTC-USD' and side='buy'),sum(price),null])),first_not_null(case([(symbol='BTC-USD' and side='sell'),sum(price),null]))]
                            GroupBy vectorized: false
                              keys: [timestamp,symbol,side]
                              values: [sum(price)]
                                Radix sort light
                                  keys: [timestamp]
                                    Async Group By workers: 1
                                      keys: [timestamp,symbol,side]
                                      values: [avg(price)]
                                      filter: (symbol in [BTC-USD] and symbol in [BTC-USD] and side in [buy,sell])
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithTradesOHLC() throws Exception {
        assertQueryAndPlan(
                "side\tETH-USD_open\tETH-USD_high\tETH-USD_low\tETH-USD_close\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close\n",
                """
                        trades PIVOT (
                        first_not_null(price) as open,
                        max(price) as high,
                        min(price) as low,
                        last_not_null(price) as close
                        FOR symbol IN ('ETH-USD', 'BTC-USD')
                        GROUP BY side
                        );""",
                ddlTrades,
                null,
                dmlTrades,
                """
                        side\tETH-USD_open\tETH-USD_high\tETH-USD_low\tETH-USD_close\tBTC-USD_open\tBTC-USD_high\tBTC-USD_low\tBTC-USD_close
                        buy\t3678.01\t3678.01\t3678.01\t3678.01\t101502.2\t101502.2\t101497.6\t101497.6
                        sell\t3678.25\t3678.25\t3678.0\t3678.0\t101502.1\t101502.1\t101497.0\t101497.0
                        """,
                true,
                true,
                false,
                """
                        GroupBy vectorized: false
                          keys: [side]
                          values: [first_not_null(case([open,NaN,symbol])),first_not_null(case([high,NaN,symbol])),first_not_null(case([low,NaN,symbol])),first_not_null(case([close,NaN,symbol])),first_not_null(case([open,NaN,symbol])),first_not_null(case([high,NaN,symbol])),first_not_null(case([low,NaN,symbol])),first_not_null(case([close,NaN,symbol]))]
                            Async JIT Group By workers: 1
                              keys: [side,symbol]
                              values: [first_not_null(price),max(price),min(price),last_not_null(price)]
                              filter: symbol in [ETH-USD,BTC-USD]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                        """);
    }

    @Test
    public void testPivotWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlSensors);
            execute(dmlSensors);

            String query = """
                    (
                        sensors
                        PIVOT (
                            avg(int_value)
                            FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%' LIMIT 1)
                            GROUP BY timestamp, vehicle_id
                        )
                        ORDER BY timestamp
                        LIMIT 10
                    )
                    UNION
                    (
                        sensors
                        PIVOT (
                            avg(int_value)
                            FOR sensor_name IN (select distinct sensor_name FROM sensors where sensor_name like 'i%' LIMIT 1)
                            GROUP BY timestamp, vehicle_id
                        )
                        ORDER BY timestamp
                        LIMIT -10
                    );""";

            assertQueryNoLeakCheck("""
                            timestamp	vehicle_id	i009
                            2025-01-01T00:00:00.000000Z	AAA000	-6.0
                            2025-01-01T00:00:00.000000Z	AAA001	-727.0
                            2025-01-01T00:00:00.000000Z	AAA002	-951.0
                            2025-01-01T00:00:00.000000Z	AAA003	-364.0
                            2025-01-01T00:00:00.000000Z	AAA004	203.0
                            2025-01-01T00:00:00.000000Z	AAA005	93.0
                            2025-01-01T00:00:00.000000Z	AAA006	575.0
                            2025-01-01T00:00:00.000000Z	AAA007	627.0
                            2025-01-01T00:00:00.000000Z	AAA008	-942.0
                            2025-01-01T00:00:00.000000Z	AAA009	-336.0
                            2025-01-01T00:00:00.009000Z	AAA059	550.0
                            2025-01-01T00:00:00.009000Z	AAA058	-147.0
                            2025-01-01T00:00:00.009000Z	AAA057	958.0
                            2025-01-01T00:00:00.009000Z	AAA056	-584.0
                            2025-01-01T00:00:00.009000Z	AAA055	723.0
                            2025-01-01T00:00:00.009000Z	AAA054	-658.0
                            2025-01-01T00:00:00.009000Z	AAA053	417.0
                            2025-01-01T00:00:00.009000Z	AAA052	118.0
                            2025-01-01T00:00:00.009000Z	AAA051	214.0
                            2025-01-01T00:00:00.009000Z	AAA050	123.0
                            """,
                    query,
                    null,
                    false,
                    false,
                    false);

            assertPlanNoLeakCheck(query,
                    """
                            Union
                                Long Top K lo: 10
                                  keys: [timestamp asc]
                                    GroupBy vectorized: false
                                      keys: [timestamp,vehicle_id]
                                      values: [first_not_null(case([avg(int_value),NaN,sensor_name]))]
                                        Async JIT Group By workers: 1
                                          keys: [timestamp,vehicle_id,sensor_name]
                                          values: [avg(int_value)]
                                          filter: sensor_name in [i009]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: sensors
                                Sort light lo: -10
                                  keys: [timestamp]
                                    GroupBy vectorized: false
                                      keys: [timestamp,vehicle_id]
                                      values: [first_not_null(case([avg(int_value),NaN,sensor_name]))]
                                        Async JIT Group By workers: 1
                                          keys: [timestamp,vehicle_id,sensor_name]
                                          values: [avg(int_value)]
                                          filter: sensor_name in [i009]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: sensors
                            """);
        });
    }

    @Test
    public void testPivotWithWhere() throws Exception {
        assertQueryAndPlan(
                "country\tname\t2000_sum\t2010_sum\t2020_sum\n",
                """
                        cities
                        WHERE (population % 2) = 0
                        PIVOT (
                            SUM(population) as sum
                            FOR
                                year IN (2000, 2010, 2020)
                            GROUP BY country, name
                        ) order by country;""",
                ddlCities,
                null,
                dmlCities,
                """
                        country\tname\t2000_sum\t2010_sum\t2020_sum
                        NL\tAmsterdam\tnull\tnull\t1158
                        US\tSeattle\t564\t608\t738
                        US\tNew York City\tnull\tnull\t8772
                        """,
                true,
                true,
                false,
                """
                        Sort light
                          keys: [country]
                            GroupBy vectorized: false
                              keys: [country,name]
                              values: [first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year])),first_not_null(case([sum,nullL,year]))]
                                Async Group By workers: 1
                                  keys: [country,name,year]
                                  values: [sum(population)]
                                  filter: (year in [2000,2010,2020] and population%2=0)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cities
                        """);
    }
}
