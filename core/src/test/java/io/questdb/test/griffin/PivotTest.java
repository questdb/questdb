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
                            "    SUM(population)\n" +
                            "    COUNT(population)\n" +
                            "    FOR\n" +
                            "        year IN (2000, 2010, 2020)\n" +
                            "    GROUP BY country\n" +
                            ");\n";
            String rewrittenQuery =
                    "SELECT \n" +
                            "    country,\n" +
                            "    SUM(CASE WHEN year = 2000 THEN population ELSE 0 END) AS \"2000_sum\",\n" +
                            "    COUNT(CASE WHEN year = 2000 THEN population ELSE 0 END) AS \"2000_count\",\n" +
                            "    SUM(CASE WHEN year = 2010 THEN population ELSE 0 END) AS \"2010_sum\",\n" +
                            "    COUNT(CASE WHEN year = 2010 THEN population ELSE 0 END) AS \"2010_count\",\n" +
                            "    SUM(CASE WHEN year = 2020 THEN population ELSE 0 END) AS \"2020_sum\",\n" +
                            "    COUNT(CASE WHEN year = 2020 THEN population ELSE 0 END) AS \"2020_count\"\n" +
                            "FROM cities\n" +
                            "GROUP BY country;";

            String model = "select-group-by country, SUM(switch(year,2000,population,0)) 2000_sum, COUNT(switch(year,2000,population,0)) 2000_count, SUM(switch(year,2010,population,0)) 2010_sum, COUNT(switch(year,2010,population,0)) 2010_count, SUM(switch(year,2020,population,0)) 2020_sum, COUNT(switch(year,2020,population,0)) 2020_count from (select [country, population, year] from cities)";
            assertModel(model, pivotQuery, ExecutionModel.QUERY);
            assertModel(model, rewrittenQuery, ExecutionModel.QUERY);

            String result = "country\t2000_sum\t2000_count\t2010_sum\t2010_count\t2020_sum\t2020_count\n" +
                    "NL\t1005\t1065\t1158\n" +
                    "US\t8579\t8783\t9510\n";

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
