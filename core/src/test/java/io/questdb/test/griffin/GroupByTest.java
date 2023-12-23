/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupByTest extends AbstractCairoTest {

    @Test
    public void test1GroupByWithoutAggregateFunctionsReturnsUniqueKeys() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query1 = "select l, s from t group by l,s";
            assertPlan(
                    query1,
                    "Async Group By workers: 1\n" +
                            "  keys: [l,s]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
            assertQuery("l\ts\n1\ta\n", query1, null, true, true);

            String query2 = "select l as l1, s as s1 from t group by l,s";
            // virtual model must be used here to change aliases
            assertPlan(
                    query2,
                    "VirtualRecord\n" +
                            "  functions: [l,s]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery("l1\ts1\n1\ta\n", query2, null, true, true);
        });
    }

    @Test
    public void test2FailOnAggregateFunctionAliasInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            assertError(
                    "select x, avg(x) as agx, avg(y) from t group by agx ",
                    "[48] aggregate functions are not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void test2FailOnAggregateFunctionColumnIndexInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            assertError(
                    "select x, avg(x) as agx, avg(y) from t group by 2 ",
                    "[48] aggregate functions are not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void test2FailOnAggregateFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, avg(x) ";
            assertError(query, "[36] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, concat('a', 'b', 'c', first(x)) ";
            assertError(query, "[58] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, case when x > 0 then 1 else first(x) end ";
            assertError(query, "[64] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, strpos('123', '1' || first(x)::string)";
            assertError(query, "[57] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggregateFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, y+avg(x) ";
            assertError(query, "[38] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithNonAggregateNonKeyColumnReference() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, x+y from t group by x ";
            assertError(query, "[12] column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void test2FailOnNonAggregateNonKeyColumnReference() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, y from t group by x ";
            assertError(query, "[10] column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void test2FailOnSelectAliasUsedInGroupByExpression() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            final String errorMessage = "[48] Invalid column: agx";

            assertError("select x, abs(x) as agx, avg(y) from t group by agx+1 ", errorMessage);
            assertError("select x, x+5    as agx, avg(y) from t group by agx+1 ", errorMessage);
            assertError("select x, avg(x)    agx, avg(y) from t group by agx+1 ", errorMessage);
        });
    }

    @Test
    public void test2FailOnWindowFunctionAliasInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, row_number() as z from t group by x, z ";
            assertError(query, "[47] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionColumnIndexInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, row_number() as z from t group by x, 2 ";
            assertError(query, "[47] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, row_number() ";
            assertError(query, "[36] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionNestedInFunctionAliasInGroupByClause1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y), abs(row_number() ) z from t group by x, z";
            assertError(query, "[58] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionNestedInFunctionAliasInGroupByClause2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            String query = "select x, avg(y), case when x > 0 then 1 else row_number() over (partition by x) end as z from t group by x, z";
            assertError(query, "[75] Invalid column: by");
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");

            String query = "select x+1, count(*) " +
                    "from t " +
                    "group by x+1 ";

            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [column,count]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQuery(
                    "column\tcount\n" +
                            "2\t2\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");

            String query = "select case when x < 0 then -1 when x = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by case when x < 0 then -1 when x = 0 then 0 else 1 end ";

            assertPlan(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [case]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertQuery(
                    "case\tcount\n" +
                            "1\t2\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");

            String query = "select case when x+1 < 0 then -1 when x+1 = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by x+1";

            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [case([column<0,-1,column=0,0,1]),count]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQuery(
                    "case\tcount\n" +
                            "1\t2\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test // expressions based on group by clause expressions should go to outer model
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");

            String query = "select x, avg(y), avg(y) + min(y), x+10, avg(x), avg(x) + 10 " +
                    "from t " +
                    "group by x ";

            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x,avg,avg+min,x+10,avg1,avg1+10]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y),min(y),avg(x)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery(
                    "x\tavg\tcolumn\tcolumn1\tavg1\tcolumn2\n" +
                            "1\t11.5\t22.5\t11\t1.0\t11.0\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumnsAndBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");

            bindVariableService.clear();
            bindVariableService.setStr("bv", "x");
            String query = "select x, avg(y), :bv " +
                    "from t " +
                    "group by x ";

            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x,avg,:bv::string]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery(
                    "x\tavg\t:bv\n" +
                            "1\t11.5\tx\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2SuccessOnSelectWithExplicitGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t group by x ";
            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x*10,x+avg,min]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y),min(y)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery(
                    "column\tcolumn1\tmin\n" +
                            "10\t12.5\t11\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2SuccessOnSelectWithoutExplicitGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x long, y long);");
            insert("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t";
            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [column,x+avg,min]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column,x]\n" +
                            "      values: [avg(y),min(y)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery(
                    "column\tcolumn1\tmin\n" +
                            "10\t12.5\t11\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test3GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE weather ( " +
                    "timestamp TIMESTAMP, windDir INT, windSpeed INT, windGust INT, \n" +
                    "cloudCeiling INT, skyCover SYMBOL, visMiles DOUBLE, tempF INT, \n" +
                    "dewpF INT, rain1H DOUBLE, rain6H DOUBLE, rain24H DOUBLE, snowDepth INT) " +
                    "timestamp (timestamp)");

            String query = "select  windSpeed, avg(windSpeed), avg + 10  " +
                    "from weather " +
                    "group by windSpeed " +
                    "order by windSpeed";

            assertError(query, "[35] Invalid column: avg");
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQuery(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by date_report " + // no alias used here
                    "order by ordr.date_report";
            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQuery(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, count(*) " +//date_report used with no alias
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQuery(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, ordr.date_report,  count(*) " +
                    "from dat ordr " +
                    "group by date_report, ordr.date_report " +
                    "order by ordr.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report1]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,date_report,count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQuery(
                    "date_report\tdate_report1\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, dateadd('d', -1, ordr.date_report) as minusday, dateadd('d', 1, date_report) as plusday, " +
                    "concat('1', ordr.date_report, '3'), count(*) " +
                    "from dat ordr " +
                    "group by dateadd('d', -1, date_report), ordr.date_report " +
                    "order by ordr.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,dateadd,dateadd('d',1,date_report),concat(['1',date_report,'3']),count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report,dateadd]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQuery(
                    "date_report\tminusday\tplusday\tconcat\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t103\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t1864000000003\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\t11728000000003\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test5GroupByWithNonAggregateExpressionUsingKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, to_str(ordr.date_report, 'dd.MM.yyyy') as dt, " +
                    "dateadd('d', 1, date_report) as plusday, dateadd('d', -1, ordr.date_report) as minusday, count(*)\n" +
                    "from dat ordr\n" +
                    "group by ordr.date_report\n" +
                    "order by ordr.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,to_str(date_report),dateadd('d',1,date_report),dateadd('d',-1,date_report),count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQuery(
                    "date_report\tdt\tplusday\tminusday\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t01.01.1970\t1970-01-02T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t02.01.1970\t1970-01-03T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t03.01.1970\t1970-01-04T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            compile("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

            String query = "select details.date_report, " +
                    " to_str(details.date_report, 'dd.MM.yyyy') as dt, " +
                    " dateadd('d', 1, details.date_report) as plusday, " +
                    " min(details.x), " +
                    " count(*), " +
                    " min(dateadd('d', -1, ordr.date_report)) as minminusday " +
                    "from ord ordr " +
                    "join det details on ordr.x = details.x " +
                    "group by details.date_report " +
                    "order by details.date_report";

            assertPlan(
                    query,
                    "Sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,to_str(date_report),dateadd('d',1,date_report),min,count,minminusday]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [date_report]\n" +
                            "          values: [min(x),count(*),min(dateadd('d',-1,date_report1))]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: details.x=ordr.x\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: ord\n" +
                            "                    Hash\n" +
                            "                        DataFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: det\n"
            );

            assertQuery(
                    "date_report\tdt\tplusday\tmin\tcount\tminminusday\n" +
                            "1970-01-11T00:00:00.000000Z\t11.01.1970\t1970-01-12T00:00:00.000000Z\t3\t3\t1969-12-31T00:00:00.000000Z\n" +
                            "1970-01-12T00:00:00.000000Z\t12.01.1970\t1970-01-13T00:00:00.000000Z\t1\t4\t1970-01-01T00:00:00.000000Z\n" +
                            "1970-01-13T00:00:00.000000Z\t13.01.1970\t1970-01-14T00:00:00.000000Z\t2\t3\t1970-01-02T00:00:00.000000Z\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            compile("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

            String query = "select details.date_report, to_str(date_report, 'dd.MM.yyyy') as dt, min(details.x), count(*) " +
                    "from ord ordr " +
                    "join det details on ordr.x = details.x " +
                    "group by details.date_report " +
                    "order by details.date_report";
            assertError(query, "[35] Ambiguous column [name=date_report]");
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            compile("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

            String query = "select details.date_report, dateadd('d', 1, date_report), min(details.x), count(*) " +
                    "from ord ordr " +
                    "join det details on ordr.x = details.x " +
                    "group by details.date_report " +
                    "order by details.date_report";
            assertError(query, "[44] Ambiguous column [name=date_report]");
        });
    }

    @Test
    public void testGroupByAliasInDifferentOrder1() throws Exception {
        assertQuery(
                "k1\tk2\tcount\n" +
                        "0\t2\t3\n" +
                        "0\t0\t2\n" +
                        "1\t1\t3\n" +
                        "1\t3\t2\n",
                "select key1 as k1, key2 as k2, count(*) from t group by k2, k1 order by 1",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10)); ",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByAliasInDifferentOrder2() throws Exception {
        assertQuery(
                "k1\tk2\tcount\n" +
                        "1\t2\t3\n" +
                        "1\t0\t2\n" +
                        "2\t1\t3\n" +
                        "2\t3\t2\n",
                "select key1+1 as k1, key2 as k2, count(*) from t group by k2, k1 order by 1",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByAllIndexedColumns() throws Exception {
        assertQuery(
                "time\ts1\tfirst\tfirst1\tfirst2\n" +
                        "2023-05-16T00:00:00.000000Z\ta\tfoo\tNaN\t0.08486964232560668\n" +
                        "2023-05-16T00:02:00.000000Z\tb\tfoo\t0.8899286912289663\t0.6254021542412018\n" +
                        "2023-05-16T00:05:00.000000Z\tc\tfoo\t0.1985581797355932\t0.33608255572515877\n",
                "SELECT first(ts) as time, s1, first(s2), first(d1), first(d2) " +
                        "FROM x " +
                        "WHERE ts BETWEEN '2023-05-16T00:00:00.00Z' AND '2023-05-16T00:10:00.00Z' " +
                        "AND s2 = ('foo') " +
                        "GROUP BY s1, s2;",
                "create table x as " +
                        "(" +
                        "select" +
                        "   rnd_symbol('a','b','c') s1," +
                        "   rnd_symbol('foo','bar') s2," +
                        "   rnd_double(1) d1," +
                        "   rnd_double(1) d2," +
                        "   timestamp_sequence('2023-05-16T00:00:00.00000Z', 60*1000000L) ts" +
                        "   from long_sequence(100)" +
                        "), index(s1), index(s2) timestamp(ts) partition by DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx1() throws Exception {
        assertQuery(
                "key\tcount\n" +
                        "0\t50\n" +
                        "1\t50\n",
                "select key, count(*) from t group by 1 order by 1",
                "create table t as ( select x%2 as key, x as value from long_sequence(100))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx2() throws Exception {
        assertQuery(
                "key\tcount\n" +
                        "0\t50\n" +
                        "1\t50\n",
                "select key, count(*) from t group by 1, 1 order by 1",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx3() throws Exception {
        assertQuery(
                "key\tcount\n" +
                        "0\t50\n" +
                        "1\t50\n",
                "select key, count(*) from t group by key, 1 order by 1",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx4() throws Exception {
        assertQuery(
                "column\tcount\n" +
                        "1\t50\n" +
                        "2\t50\n",
                "select key+1, count(*) from t group by key, 1 order by key+1",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx5() throws Exception {
        assertQuery(
                "z\tcount\n" +
                        "1\t50\n" +
                        "2\t50\n",
                "select key+1 as z, count(*) from t group by key, 1 order by z",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx6() throws Exception {
        assertQuery(
                "column\tcount\n" +
                        "1\t50\n" +
                        "2\t50\n",
                "select key+1, count(*) from t group by key, 1 order by 1",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx7() throws Exception {
        assertQuery(
                "column\tcount\n" +
                        "2\t50\n" +
                        "1\t50\n",
                "select key+1, count(*) from t group by key, 1 order by key+3 desc",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByColumnIdx8() throws Exception {
        assertQuery(
                "column\tkey\tkey1\tcount\n" +
                        "1\t0\t0\t50\n" +
                        "2\t1\t1\t50\n",
                "select key+1, key, key, count(*) from t group by key order by 1,2,3 desc",
                "create table t as ( select x%2 as key, x as value from long_sequence(100));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByDuplicateColumn() throws Exception {
        assertQuery(
                "k1\tk2\tcount\n" +
                        "0\t0\t2\n" +
                        "0\t2\t3\n" +
                        "1\t1\t3\n" +
                        "1\t3\t2\n",
                "select key1 as k1, key2 as k2, count(*) from t group by k2, k1, k2 order by 1, 2",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByExpressionAndLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l, s, l+1 from t group by l+1,s, l, l+2";
            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [l,s,column]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s,column,column1]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQuery(
                    "l\ts\tcolumn\n" +
                            "1\ta\t2\n",
                    query,
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testGroupByIndexOutsideSelectList() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab as (select x, x%2 as y from long_sequence(2))");
            assertError(
                    "select * from tab group by 5",
                    "[27] GROUP BY position 5 is not in select list"
            );
        });
    }

    @Test
    public void testGroupByInvalidOrderByExpression() throws Exception {
        assertException(
                "SELECT ts AS ref0 FROM x WHERE 1=1 GROUP BY ts ORDER BY (ts) NOT IN ('{}') LIMIT 1;",
                "CREATE TABLE x (ts TIMESTAMP, event SHORT, origin SHORT) TIMESTAMP(ts);",
                69,
                "Invalid date"
        );
    }

    @Test
    public void testGroupByNonPartitioned() throws Exception {
        assertQuery(
                "k\tsum\n" +
                        "BBBE\t0.7453598685393461\n" +
                        "BBBI\t0.7394866029725212\n" +
                        "BBBK\t1.370328208214273\n",
                "SELECT k, sum(val) FROM tab ORDER BY k LIMIT 3;",
                "CREATE TABLE tab AS (SELECT rnd_str(4, 4, 0) k, rnd_double() val FROM long_sequence(100000));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByOrderByExpression() throws Exception {
        assertQuery(
                "ref0\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n",
                "SELECT ts AS ref0 FROM x WHERE 1=1 GROUP BY ts ORDER BY (ts) NOT IN ('1970-01-01T00:00:00.000002Z');",
                "CREATE TABLE x AS (SELECT x::timestamp AS ts, x::short AS event, x::short AS origin FROM long_sequence(2)) TIMESTAMP(ts);",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByWithAliasClash1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "    select 1 as l, 'a' as s, -1 max " +
                    "    union all " +
                    "    select 1, 'a', -2 )");

            String query = "select s, max, max(l) from t group by s, max";
            assertPlan(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [s,max]\n" +
                            "  values: [max(l)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertQuery(
                    "s\tmax\tmax1\n" +
                            "a\t-1\t1\n" +
                            "a\t-2\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x, x%2 as y from long_sequence(2))");
            compile("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), t2.x " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x";

            assertQuery(
                    "x\tmax\tx1\n" +
                            "1\t1\t1\n" +
                            "2\t0\t2\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x, x%2 as y from long_sequence(2))");
            compile("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), case when t1.x > 1 then 100*t1.x else 10*t2.x end " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x";

            assertPlan(query, "VirtualRecord\n" +
                    "  functions: [x,max,case([1<x,100*x,10*x1])]\n" +
                    "    GroupBy vectorized: false\n" +
                    "      keys: [x,x1]\n" +
                    "      values: [max(y)]\n" +
                    "        SelectedRecord\n" +
                    "            Hash Join Light\n" +
                    "              condition: t2.y=t1.y\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t1\n" +
                    "                Hash\n" +
                    "                    DataFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: t2\n");

            assertQuery(
                    "x\tmax\tcase\n" +
                            "1\t1\t10\n" +
                            "2\t0\t200\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x, x%2 as y from long_sequence(2))");
            compile("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), case when t1.x > 1 then 30*t1.x else 20*t2.x end " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, case when t1.x > 1 then 30*t1.x else 20*t2.x end";

            assertPlan(query, "VirtualRecord\n" +
                    "  functions: [x,max,case]\n" +
                    "    GroupBy vectorized: false\n" +
                    "      keys: [x,case,x1]\n" +
                    "      values: [max(y)]\n" +
                    "        VirtualRecord\n" +
                    "          functions: [x,y,case([1<x,30*x,20*x1]),x1]\n" +
                    "            SelectedRecord\n" +
                    "                Hash Join Light\n" +
                    "                  condition: t2.y=t1.y\n" +
                    "                    DataFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: t1\n" +
                    "                    Hash\n" +
                    "                        DataFrame\n" +
                    "                            Row forward scan\n" +
                    "                            Frame forward scan on: t2\n");

            assertQuery(
                    "x\tmax\tcase\n" +
                            "1\t1\t20\n" +
                            "2\t0\t60\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x::int as x, x%2 as y from long_sequence(2))");
            compile("create table t2 as (select x::int as x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), dateadd('d', t1.x, '2023-03-01T00:00:00')::long + t2.x " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, dateadd('d', t1.x, '2023-03-01T00:00:00') ";

            assertPlan(query, "VirtualRecord\n" +
                    "  functions: [x,max,dateadd::long+x1]\n" +
                    "    GroupBy vectorized: false\n" +
                    "      keys: [x,x1,dateadd]\n" +
                    "      values: [max(y)]\n" +
                    "        VirtualRecord\n" +
                    "          functions: [x,y,x1,dateadd('d',1677628800000000,x)]\n" +
                    "            SelectedRecord\n" +
                    "                Hash Join Light\n" +
                    "                  condition: t2.y=t1.y\n" +
                    "                    DataFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: t1\n" +
                    "                    Hash\n" +
                    "                        DataFrame\n" +
                    "                            Row forward scan\n" +
                    "                            Frame forward scan on: t2\n");

            assertQuery(
                    "x\tmax\tcolumn\n" +
                            "1\t1\t1677715200000001\n" +
                            "2\t0\t1677801600000002\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash6() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x::int as x, x%2 as y from long_sequence(2))");
            compile("create table t2 as (select x::int as x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), dateadd('s', max(t2.y)::int, dateadd('d', t1.x, '2023-03-01T00:00:00') ) " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, dateadd('d', t1.x, '2023-03-01T00:00:00') ";

            assertPlan(query, "VirtualRecord\n" +
                    "  functions: [x,max,dateadd('s',dateadd,max::int)]\n" +
                    "    GroupBy vectorized: false\n" +
                    "      keys: [x,dateadd,x1]\n" +
                    "      values: [max(y)]\n" +
                    "        VirtualRecord\n" +
                    "          functions: [x,y,dateadd('d',1677628800000000,x),x1]\n" +
                    "            SelectedRecord\n" +
                    "                Hash Join Light\n" +
                    "                  condition: t2.y=t1.y\n" +
                    "                    DataFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: t1\n" +
                    "                    Hash\n" +
                    "                        DataFrame\n" +
                    "                            Row forward scan\n" +
                    "                            Frame forward scan on: t2\n");

            assertQuery(
                    "x\tmax\tdateadd\n" +
                            "1\t1\t2023-03-02T00:00:01.000000Z\n" +
                            "2\t0\t2023-03-03T00:00:00.000000Z\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithDuplicateSelectColumn() throws Exception {
        assertQuery(
                "k1\tkey2\tkey21\tcount\n" +
                        "0\t0\t0\t2\n" +
                        "0\t2\t2\t3\n" +
                        "1\t1\t1\t3\n" +
                        "1\t3\t3\t2\n",
                "select key1 as k1, key2, key2, count(*) from t group by key2, k1 order by 1, 2",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByWithNonConstantSelectClauseExpression() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l,s,rnd_int(0,1,0)/10 from t group by l,s";
            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [l,s,rnd_int(0,1,0)/10]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s]\n" +
                            "      filter: null\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQuery(
                    "l\ts\tcolumn\n" +
                            "1\ta\t0\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (s1 symbol, s2 symbol, l long, ts timestamp) timestamp(ts) partition by day;");
            insert(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select s2, sum(l) from t where s2 in ('c') latest on ts partition by s1";
            assertPlan(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [s2]\n" +
                            "  values: [sum(l)]\n" +
                            "    LatestByDeferredListValuesFiltered\n" +
                            "      filter: s2 in [c]\n" +
                            "        Frame backward scan on: t\n"
            );
            assertQuery(
                    "s2\tsum\n" +
                            "c\t25\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (s1 symbol index, s2 symbol index, l long, ts timestamp) timestamp(ts) partition by day;");
            insert(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select s2, sum(l) from t where s2 in ('c', 'd') latest on ts partition by s1";
            assertPlan(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [s2]\n" +
                            "  values: [sum(l)]\n" +
                            "    LatestByDeferredListValuesFiltered\n" +
                            "      filter: s2 in [c,d]\n" +
                            "        Frame backward scan on: t\n"
            );
            assertQuery(
                    "s2\tsum\n" +
                            "c\t14\n" +
                            "d\t12\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (s1 symbol index, s2 symbol index, l long, ts timestamp) timestamp(ts) partition by day;");
            insert(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select concat('_', s2, '_'), sum(l) from t where s2 in ('d') latest on ts partition by s1";
            assertPlan(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [concat]\n" +
                            "  values: [sum(l)]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [concat(['_',s2,'_']),l]\n" +
                            "        LatestByDeferredListValuesFiltered\n" +
                            "          filter: s2 in [d]\n" +
                            "            Frame backward scan on: t\n"
            );
            assertQuery(
                    "concat\tsum\n" +
                            "_d_\t25\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNestedGroupByWithExplicitGroupByClause() throws Exception {
        assertQuery(
                "url\tu_count\tcnt\tavg_m_sum\n" +
                        "RXPEHNRXGZ\t4\t4\t414.25\n" +
                        "DXYSBEOUOJ\t1\t1\t225.0\n" +
                        "SXUXIBBTGP\t2\t2\t379.5\n" +
                        "GWFFYUDEYY\t5\t5\t727.2\n" +
                        "LOFJGETJRS\t2\t2\t524.5\n" +
                        "ZSRYRFBVTM\t2\t2\t337.0\n" +
                        "VTJWCPSWHY\t1\t1\t660.0\n" +
                        "HGOOZZVDZJ\t1\t1\t540.0\n" +
                        "SHRUEDRQQU\t2\t2\t468.0\n",
                "WITH x_sample AS (\n" +
                        "  SELECT id, uuid, url, sum(metric) m_sum\n" +
                        "  FROM x\n" +
                        "  WHERE ts >= '1023-03-31T00:00:00' and ts <= '2023-04-02T23:59:59'\n" +
                        "  GROUP BY id, uuid, url\n" +
                        ")\n" +
                        "SELECT url, count_distinct(uuid) u_count, count() cnt, avg(m_sum) avg_m_sum\n" +
                        "FROM x_sample\n" +
                        "GROUP BY url",
                "create table x as (\n" +
                        "select timestamp_sequence(100000000, 100000000) ts,\n" +
                        "  rnd_int(0, 10, 0) id,\n" +
                        "  rnd_uuid4() uuid,\n" +
                        "  rnd_str(10, 10, 10, 0) url,\n" +
                        "  rnd_long(0, 1000, 0) metric\n" +
                        "from long_sequence(20)) timestamp(ts)",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByOnAliasedColumnAfterGroupBy() throws Exception {
        ddl("create table tst ( ts timestamp ) timestamp(ts);");
        insert("insert into tst values ('2023-05-29T15:30:00.000000Z')");

        ddl("create table data ( dts timestamp, s symbol ) timestamp(dts);");
        insert("insert into data values ('2023-05-29T15:29:59.000000Z', 'USD')");

        // single table
        assertQuery(
                "ref0\n2023-05-29T15:30:00.000000Z\n",
                "SELECT ts AS ref0 " +
                        "FROM tst " +
                        "GROUP BY ts " +
                        "ORDER BY ts",
                "",
                true,
                true
        );

        assertQuery(
                "ref0\n2023-05-29T15:30:00.000000Z\n",
                "SELECT tst.ts AS ref0 " +
                        "FROM tst " +
                        "GROUP BY tst.ts " +
                        "ORDER BY tst.ts",
                "",
                true,
                true
        );

        assertQuery(
                "ref0\n2023-05-29T15:30:00.000000Z\n",
                "SELECT tst.ts AS ref0 " +
                        "FROM tst " +
                        "GROUP BY ts " +
                        "ORDER BY tst.ts",
                "",
                true,
                true
        );

        assertQuery(
                "ref0\n2023-05-29T15:30:00.000000Z\n",
                "SELECT ts AS ref0 " +
                        "FROM tst " +
                        "GROUP BY tst.ts " +
                        "ORDER BY ts",
                "",
                true,
                true
        );

        // joins
        for (String join : Arrays.asList("LT JOIN data ", "ASOF JOIN data ", "LEFT JOIN data on (tst.ts > data.dts) ", "INNER JOIN data on (tst.ts > data.dts) ", "CROSS JOIN data ")) {
            assertQuery(
                    "ref0\tdts\n2023-05-29T15:30:00.000000Z\t2023-05-29T15:29:59.000000Z\n",
                    "SELECT ts AS ref0, dts " +
                            "FROM tst " +
                            join +
                            "GROUP BY tst.ts, data.dts " +
                            "ORDER BY ts",
                    "",
                    true,
                    true
            );

            assertQuery(
                    "ref0\tdts\n2023-05-29T15:30:00.000000Z\t2023-05-29T15:29:59.000000Z\n",
                    "SELECT ts AS ref0, dts " +
                            "FROM tst " +
                            join +
                            "GROUP BY ts, data.dts " +
                            "ORDER BY tst.ts",
                    "",
                    true,
                    true
            );
        }
    }

    @Test
    public void testSelectDistinctOnAliasedColumnWithOrderBy() throws Exception {
        ddl("create table tab (created timestamp, i int) timestamp(created)");
        insert("insert into tab select x::timestamp, x from long_sequence(3)");
        drainWalQueue();

        String query = "SELECT DISTINCT tab.created AS ref0 " +
                "FROM tab " +
                "WHERE (tab.created) IS NOT NULL " +
                "GROUP BY tab.created " +
                "ORDER BY tab.created";

        assertPlan(
                query,
                "Sort light\n" +
                        "  keys: [ref0]\n" +
                        "    Distinct\n" +
                        "      keys: ref0\n" +
                        "        VirtualRecord\n" +
                        "          functions: [created]\n" +
                        "            Async Group By workers: 1\n" +
                        "              keys: [created]\n" +
                        "              filter: created!=null\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: tab\n"
        );

        assertQuery(
                "ref0\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000003Z\n",
                query,
                null,
                true,
                false
        );
    }

    @Test
    public void testSelectDistinctOnExpressionWithOrderBy() throws Exception {
        ddl("create table tab (created timestamp, i int) timestamp(created)");
        insert("insert into tab select x::timestamp, x from long_sequence(3)");
        drainWalQueue();

        String query = "SELECT DISTINCT dateadd('h', 1, tab.created) AS ref0 " +
                "FROM tab " +
                "WHERE (tab.created) IS NOT NULL " +
                "GROUP BY tab.created " +
                "ORDER BY dateadd('h', 1, tab.created)";

        assertPlan(
                query,
                "Sort light\n" +
                        "  keys: [ref0]\n" +
                        "    Distinct\n" +
                        "      keys: ref0\n" +
                        "        VirtualRecord\n" +
                        "          functions: [dateadd('h',1,created)]\n" +
                        "            Async Group By workers: 1\n" +
                        "              keys: [created]\n" +
                        "              filter: created!=null\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: tab\n"
        );

        assertQuery(
                "ref0\n" +
                        "1970-01-01T01:00:00.000001Z\n" +
                        "1970-01-01T01:00:00.000002Z\n" +
                        "1970-01-01T01:00:00.000003Z\n",
                query,
                null,
                true,
                false
        );
    }

    @Test
    public void testSelectDistinctOnUnaliasedColumnWithOrderBy() throws Exception {
        ddl("create table tab (created timestamp, i int) timestamp(created)");
        insert("insert into tab select x::timestamp, x from long_sequence(3)");
        drainWalQueue();

        String query = "SELECT DISTINCT tab.created " +
                "FROM tab " +
                "WHERE (tab.created) IS NOT NULL " +
                "GROUP BY tab.created " +
                "ORDER BY tab.created";

        assertPlan(
                query,
                "Sort light\n" +
                        "  keys: [created]\n" +
                        "    Distinct\n" +
                        "      keys: created\n" +
                        "        Async Group By workers: 1\n" +
                        "          keys: [created]\n" +
                        "          filter: created!=null\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: tab\n"
        );

        assertQuery(
                "created\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000003Z\n",
                query,
                null,
                true,
                false
        );
    }

    @Test
    public void testSelectMatchingButInDifferentOrderThanGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (" +
                    "    sym symbol," +
                    "    bid double, " +
                    "    ts timestamp " +
                    ") timestamp(ts) partition by DAY");
            ddl("insert into x " +
                    " select rnd_symbol('A', 'B'), rnd_double(), dateadd('m', x::int, 0::timestamp) " +
                    " from long_sequence(20)");

            String query = "select sym, hour(ts), avg(bid) avgBid from x group by hour(ts), sym ";
            assertPlan(query, "VirtualRecord\n" +
                    "  functions: [sym,hour,avgBid]\n" +
                    "    GroupBy vectorized: false\n" +
                    "      keys: [sym,hour]\n" +
                    "      values: [avg(bid)]\n" +
                    "        VirtualRecord\n" +
                    "          functions: [sym,hour(ts),bid]\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: x\n");
            assertQuery(
                    "sym\thour\tavgBid\n" +
                            "A\t0\t0.4922298136511458\n" +
                            "B\t0\t0.4796420804429589\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testStarIsNotAllowedInGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab as (select x, x%2 as y from long_sequence(2))");
            assertError(
                    "select * from tab group by tab.*",
                    "[27] '*' is not allowed in GROUP BY"
            );
        });
    }

    private void assertError(String query, String errorMessage) {
        try {
            assertQuery(null, query,
                    null, true, true
            );
            Assert.fail();
        } catch (SqlException sqle) {
            Assert.assertEquals(errorMessage, sqle.getMessage());
        }
    }
}
