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

import io.questdb.griffin.model.ExecutionModel;
import org.junit.Test;

public class PivotTest extends AbstractSqlParserTest {

    public static String ddlCities = "CREATE TABLE cities (\n" +
            "    country VARCHAR, name VARCHAR, year INT, population INT\n" +
            ");";
    public static String ddlTrades = "CREATE TABLE 'trades' (\n" +
            "  symbol SYMBOL,\n" +
            "  side SYMBOL,\n" +
            "  price DOUBLE,\n" +
            "  amount DOUBLE,\n" +
            "  timestamp TIMESTAMP\n" +
            ") timestamp (timestamp) PARTITION BY DAY WAL;";
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
    public void testStandardPivot() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            String pivotQuery =
                    "SELECT *\n" +
                            "FROM cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 THEN population ELSE 0 END) AS \"2000\",\n" +
                            "    SUM(CASE WHEN year = 2010 THEN population ELSE 0 END) AS \"2010\",\n" +
                            "    SUM(CASE WHEN year = 2020 THEN population ELSE 0 END) AS \"2020\"\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(switch(year,2000,population,0)) 2000, SUM(switch(year,2010,population,0)) 2010, SUM(switch(year,2020,population,0)) 2020 from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000\t2010\t2020\n" +
                    "NL\t1005\t1065\t1158\n" +
                    "US\t8579\t8783\t9510\n";

            assertSql(result, pivotQuery);
            assertSql(result, rewrittenQuery);
        });
    }

    @Test
    public void testStandardPivotWithAliasedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            String pivotQuery =
                    "SELECT *\n" +
                            "FROM cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population) as total\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 THEN population ELSE 0 END) AS \"2000_total\",\n" +
                            "    SUM(CASE WHEN year = 2010 THEN population ELSE 0 END) AS \"2010_total\",\n" +
                            "    SUM(CASE WHEN year = 2020 THEN population ELSE 0 END) AS \"2020_total\"\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(switch(year,2000,population,0)) 2000_total, SUM(switch(year,2010,population,0)) 2010_total, SUM(switch(year,2020,population,0)) 2020_total from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000_total\t2010_total\t2020_total\n" +
                    "NL\t1005\t1065\t1158\n" +
                    "US\t8579\t8783\t9510\n";

            assertSql(result, pivotQuery);
            assertSql(result, rewrittenQuery);
        });
    }

    @Test
    public void testStandardPivotWithMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            String pivotQuery =
                    "SELECT *\n" +
                            "FROM cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population),\n" +
                            "    COUNT(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 THEN population ELSE 0 END) AS \"2000_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2000 THEN population ELSE null END) AS \"2000_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2010 THEN population ELSE 0 END) AS \"2010_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2010 THEN population ELSE null END) AS \"2010_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2020 THEN population ELSE 0 END) AS \"2020_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2020 THEN population ELSE null END) AS \"2020_COUNT\"\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(switch(year,2000,population,0)) 2000_SUM, COUNT(switch(year,2000,population,null)) 2000_COUNT, SUM(switch(year,2010,population,0)) 2010_SUM, COUNT(switch(year,2010,population,null)) 2010_COUNT, SUM(switch(year,2020,population,0)) 2020_SUM, COUNT(switch(year,2020,population,null)) 2020_COUNT from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000_SUM\t2000_COUNT\t2010_SUM\t2010_COUNT\t2020_SUM\t2020_COUNT\n" +
                    "NL\t1005\t1\t1065\t1\t1158\t1\n" +
                    "US\t8579\t2\t8783\t2\t9510\t2\n";

            assertSql(result, pivotQuery);
            assertSql(result, rewrittenQuery);
        });
    }

    @Test
    public void testStandardPivotWithMultipleForExprs() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            String pivotQuery =
                    "SELECT *\n" +
                            "FROM cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "        country in ('NL', 'US')\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 AND country = 'NL' THEN population ELSE 0 END) AS \"2000_NL\",\n" +
                            "    SUM(CASE WHEN year = 2000 AND country = 'US' THEN population ELSE 0 END) AS \"2000_US\",\n" +
                            "    SUM(CASE WHEN year = 2010 AND country = 'NL' THEN population ELSE 0 END) AS \"2010_NL\",\n" +
                            "    SUM(CASE WHEN year = 2010 AND country = 'US' THEN population ELSE 0 END) AS \"2010_US\",\n" +
                            "    SUM(CASE WHEN year = 2020 AND country = 'NL' THEN population ELSE 0 END) AS \"2020_NL\",\n" +
                            "    SUM(CASE WHEN year = 2020 AND country = 'US' THEN population ELSE 0 END) AS \"2020_US\",\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(case(year = 2000 and country = 'NL',population,0)) 2000_NL, SUM(case(year = 2000 and country = 'US',population,0)) 2000_US, SUM(case(year = 2010 and country = 'NL',population,0)) 2010_NL, SUM(case(year = 2010 and country = 'US',population,0)) 2010_US, SUM(case(year = 2020 and country = 'NL',population,0)) 2020_NL, SUM(case(year = 2020 and country = 'US',population,0)) 2020_US from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000_NL\t2000_US\t2010_NL\t2010_US\t2020_NL\t2020_US\n" +
                    "NL\t1005\t0\t1065\t0\t1158\t0\n" +
                    "US\t0\t8579\t0\t8783\t0\t9510\n";

            assertSql(result, pivotQuery);
            assertSql(result, rewrittenQuery);
        });
    }

    @Test
    public void testStandardPivotWithMultipleForExprsAndMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlCities);
            execute(dmlCities);

            String pivotQuery =
                    "SELECT *\n" +
                            "FROM cities\n" +
                            "PIVOT (\n" +
                            "    SUM(population),\n" +
                            "    COUNT(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "        country in ('NL', 'US')\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 AND country = 'NL' THEN population ELSE 0 END) AS \"2000_NL_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2000 AND country = 'NL' THEN population ELSE null END) AS \"2000_NL_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2000 AND country = 'US' THEN population ELSE 0 END) AS \"2000_US_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2000 AND country = 'US' THEN population ELSE null END) AS \"2000_US_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2010 AND country = 'NL' THEN population ELSE 0 END) AS \"2010_NL_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2010 AND country = 'NL' THEN population ELSE null END) AS \"2010_NL_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2010 AND country = 'US' THEN population ELSE 0 END) AS \"2010_US_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2010 AND country = 'US' THEN population ELSE null END) AS \"2010_US_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2020 AND country = 'NL' THEN population ELSE 0 END) AS \"2020_NL_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2020 AND country = 'NL' THEN population ELSE null END) AS \"2020_NL_COUNT\",\n" +
                            "    SUM(CASE WHEN year = 2020 AND country = 'US' THEN population ELSE 0 END) AS \"2020_US_SUM\",\n" +
                            "    COUNT(CASE WHEN year = 2020 AND country = 'US' THEN population ELSE null END) AS \"2020_US_COUNT\",\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(case(year = 2000 and country = 'NL',population,0)) 2000_NL_SUM, COUNT(case(year = 2000 and country = 'NL',population,null)) 2000_NL_COUNT, SUM(case(year = 2000 and country = 'US',population,0)) 2000_US_SUM, COUNT(case(year = 2000 and country = 'US',population,null)) 2000_US_COUNT, SUM(case(year = 2010 and country = 'NL',population,0)) 2010_NL_SUM, COUNT(case(year = 2010 and country = 'NL',population,null)) 2010_NL_COUNT, SUM(case(year = 2010 and country = 'US',population,0)) 2010_US_SUM, COUNT(case(year = 2010 and country = 'US',population,null)) 2010_US_COUNT, SUM(case(year = 2020 and country = 'NL',population,0)) 2020_NL_SUM, COUNT(case(year = 2020 and country = 'NL',population,null)) 2020_NL_COUNT, SUM(case(year = 2020 and country = 'US',population,0)) 2020_US_SUM, COUNT(case(year = 2020 and country = 'US',population,null)) 2020_US_COUNT from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000_NL_SUM\t2000_NL_COUNT\t2000_US_SUM\t2000_US_COUNT\t2010_NL_SUM\t2010_NL_COUNT\t2010_US_SUM\t2010_US_COUNT\t2020_NL_SUM\t2020_NL_COUNT\t2020_US_SUM\t2020_US_COUNT\n" +
                    "NL\t1005\t1\t0\t0\t1065\t1\t0\t0\t1158\t1\t0\t0\n" +
                    "US\t0\t0\t8579\t2\t0\t0\t8783\t2\t0\t0\t9510\t2\n";

            assertSql(result, pivotQuery);
            assertSql(result, rewrittenQuery);
        });
    }

    @Test
    public void testStandardPivotWithTradesData() throws Exception {
        assertMemoryLeak(() -> {
            execute(ddlTrades);
            execute(dmlTrades);
            drainWalQueue();

            String pivotQuery = " select * from (select * from trades where symbol in 'ETH-USDT')\n" +
                    "  pivot (\n" +
                    "    sum(price)\n" +
                    "    FOR \"symbol\" IN ('ETH-USDT')\n" +
                    "        side in ('buy', 'sell')\n" +
                    "    GROUP BY timestamp\n" +
                    "  );";

            assertSql("timestamp\tETH-USDT_buy\tETH-USDT_sell\n" +
                    "2024-12-19T08:10:00.700999Z\t0.0\t3678.25\n" +
                    "2024-12-19T08:10:00.736000Z\t0.0\t3678.25\n" +
                    "2024-12-19T08:10:00.759000Z\t0.0\t3678.0\n" +
                    "2024-12-19T08:10:00.772999Z\t0.0\t3678.0\n" +
                    "2024-12-19T08:10:00.887000Z\t3678.01\t0.0\n" +
                    "2024-12-19T08:10:00.950000Z\t0.0\t3678.0\n", pivotQuery);
        });
    }

    @Test
    public void testStandardPivotWithoutExplicitGroupBy() throws Exception {
        assertException("SELECT *\n" +
                "FROM cities\n" +
                "PIVOT (\n" +
                "    SUM(population) as total\n" +
                "    FOR\n" +
                "        year IN (2000, 2010, 2020)\n" +
                ");\n", 101, "expected `GROUP`");
    }

}
