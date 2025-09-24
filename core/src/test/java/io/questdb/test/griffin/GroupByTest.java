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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupByTest extends AbstractCairoTest {

    @Test
    public void test1GroupByWithoutAggregateFunctionsReturnsUniqueKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query1 = "select l, s from t group by l,s";
            assertPlanNoLeakCheck(
                    query1,
                    "Async Group By workers: 1\n" +
                            "  keys: [l,s]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck("l\ts\n1\ta\n", query1, null, true, true);

            String query2 = "select l as l1, s as s1 from t group by l,s";
            // virtual model must be used here to change aliases
            assertPlanNoLeakCheck(
                    query2,
                    "VirtualRecord\n" +
                            "  functions: [l,s]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck("l1\ts1\n1\ta\n", query2, null, true, true);
        });
    }

    @Test
    public void test2FailOnAggregateFunctionAliasInGroupByClause1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            assertError(
                    "select x, avg(x) as agx, avg(y) from t group by agx ",
                    "[48] aggregate functions are not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void test2FailOnAggregateFunctionAliasInGroupByClause2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            assertError(
                    "select x, 2*avg(y) agy from t group by agy;",
                    "[39] aggregate functions are not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void test2FailOnAggregateFunctionColumnIndexInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            assertError(
                    "select x, avg(x) as agx, avg(y) from t group by 2 ",
                    "[48] aggregate functions are not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void test2FailOnAggregateFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, avg(x) ";
            assertError(query, "[36] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, concat('a', 'b', 'c', first(x)) ";
            assertError(query, "[58] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, case when x > 0 then 1 else first(x) end ";
            assertError(query, "[64] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggFunctionNestedInFunctionInGroupByClause3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, strpos('123', '1' || first(x)::string)";
            assertError(query, "[57] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithAggregateFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, y+avg(x) ";
            assertError(query, "[38] aggregate functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnExpressionWithNonAggregateNonKeyColumnReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, x+y from t group by x ";
            assertError(query, "[12] column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void test2FailOnNonAggregateNonKeyColumnReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, y from t group by x ";
            assertError(query, "[10] column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void test2FailOnSelectAliasUsedInGroupByExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            final String errorMessage = "[48] Invalid column: agx";

            assertError("select x, abs(x) as agx, avg(y) from t group by agx+1 ", errorMessage);
            assertError("select x, x+5    as agx, avg(y) from t group by agx+1 ", errorMessage);
            assertError("select x, avg(x)    agx, avg(y) from t group by agx+1 ", errorMessage);
        });
    }

    @Test
    public void test2FailOnWindowFunctionAliasInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, row_number() as z from t group by x, z ";
            assertError(query, "[47] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionColumnIndexInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, row_number() as z from t group by x, 2 ";
            assertError(query, "[47] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionInGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y) from t group by x, row_number() ";
            assertError(query, "[36] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionNestedInFunctionAliasInGroupByClause1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y), abs(row_number() ) z from t group by x, z";
            assertError(query, "[58] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2FailOnWindowFunctionNestedInFunctionAliasInGroupByClause2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            String query = "select x, avg(y), case when x > 0 then 1 else row_number() over (partition by x) end as z from t group by x, z";
            assertError(query, "[59] Nested window functions are not currently supported.");
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select x+1, count(*) " +
                    "from t " +
                    "group by x+1 ";

            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [column,count]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select case when x < 0 then -1 when x = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by case when x < 0 then -1 when x = 0 then 0 else 1 end ";

            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [case]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select case when x+1 < 0 then -1 when x+1 = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by x+1";

            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [case([column<0,-1,column=0,0,1]),count]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select x, avg(y), avg(y) + min(y), x+10, avg(x), avg(x) + 10 " +
                    "from t " +
                    "group by x ";

            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x,avg,avg+min,x+10,avg1,avg1+10]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y),min(y),avg(x)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            bindVariableService.clear();
            bindVariableService.setStr("bv", "x");
            String query = "select x, avg(y), :bv " +
                    "from t " +
                    "group by x ";

            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x,avg,:bv::string]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t group by x ";
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x*10,x+avg,min]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [x]\n" +
                            "      values: [avg(y),min(y)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t";
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [column,x+avg,min]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column,x]\n" +
                            "      values: [avg(y),min(y)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("CREATE TABLE weather ( " +
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
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQueryNoLeakCheck(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by date_report " + // no alias used here
                    "order by ordr.date_report";
            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQueryNoLeakCheck(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, count(*) " +//date_report used with no alias
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [date_report]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: dat\n"
            );
            assertQueryNoLeakCheck(
                    "date_report\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, ordr.date_report,  count(*) " +
                    "from dat ordr " +
                    "group by date_report, ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report1]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,date_report,count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQueryNoLeakCheck(
                    "date_report\tdate_report1\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t3\n",
                    query,
                    "date_report1",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, dateadd('d', -1, ordr.date_report) as minusday, dateadd('d', 1, date_report) as plusday, " +
                    "concat('1', ordr.date_report, '3'), count(*) " +
                    "from dat ordr " +
                    "group by dateadd('d', -1, date_report), ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,dateadd,dateadd('d',1,date_report),concat(['1',date_report,'3']),count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report,dateadd]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQueryNoLeakCheck(
                    "date_report\tminusday\tplusday\tconcat\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t11970-01-01T00:00:00.000000Z3\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t11970-01-02T00:00:00.000000Z3\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\t11970-01-03T00:00:00.000000Z3\t3\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test5GroupByWithNonAggregateExpressionUsingKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, to_str(ordr.date_report, 'dd.MM.yyyy') as dt, " +
                    "dateadd('d', 1, date_report) as plusday, dateadd('d', -1, ordr.date_report) as minusday, count(*)\n" +
                    "from dat ordr\n" +
                    "group by ordr.date_report\n" +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,to_str(date_report),dateadd('d',1,date_report),dateadd('d',-1,date_report),count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [date_report]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: dat\n"
            );

            assertQueryNoLeakCheck(
                    "date_report\tdt\tplusday\tminusday\tcount\n" +
                            "1970-01-01T00:00:00.000000Z\t01.01.1970\t1970-01-02T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t3\n" +
                            "1970-01-02T00:00:00.000000Z\t02.01.1970\t1970-01-03T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t4\n" +
                            "1970-01-03T00:00:00.000000Z\t03.01.1970\t1970-01-04T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t3\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            execute("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

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

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [date_report]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [date_report,to_str(date_report),dateadd('d',1,date_report),min,count,minminusday]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [date_report]\n" +
                            "          values: [min(x),count(*),min(dateadd('d',-1,date_report1))]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: details.x=ordr.x\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: ord\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: det\n"
            );

            assertQueryNoLeakCheck(
                    "date_report\tdt\tplusday\tmin\tcount\tminminusday\n" +
                            "1970-01-11T00:00:00.000000Z\t11.01.1970\t1970-01-12T00:00:00.000000Z\t3\t3\t1969-12-31T00:00:00.000000Z\n" +
                            "1970-01-12T00:00:00.000000Z\t12.01.1970\t1970-01-13T00:00:00.000000Z\t1\t4\t1970-01-01T00:00:00.000000Z\n" +
                            "1970-01-13T00:00:00.000000Z\t13.01.1970\t1970-01-14T00:00:00.000000Z\t2\t3\t1970-01-02T00:00:00.000000Z\n",
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            execute("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

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
            execute("create table ord as ( select cast(86400000000*(x%3) as timestamp) as date_report, x from long_sequence(10))");
            execute("create table det as ( select cast(86400000000*(10+x%3) as timestamp) as date_report, x from long_sequence(10))");

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
                        "0\t0\t2\n" +
                        "0\t2\t3\n" +
                        "1\t1\t3\n" +
                        "1\t3\t2\n",
                "select key1 as k1, key2 as k2, count(*) from t group by k2, k1 order by 1, 2",
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
                        "1\t0\t2\n" +
                        "1\t2\t3\n" +
                        "2\t1\t3\n" +
                        "2\t3\t2\n",
                "select key1+1 as k1, key2 as k2, count(*) from t group by k2, k1 order by 1, 2",
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
                        "2023-05-16T00:00:00.000000Z\ta\tfoo\tnull\t0.08486964232560668\n" +
                        "2023-05-16T00:02:00.000000Z\tb\tfoo\t0.8899286912289663\t0.6254021542412018\n" +
                        "2023-05-16T00:05:00.000000Z\tc\tfoo\t0.1985581797355932\t0.33608255572515877\n",
                "SELECT first(ts) as time, s1, first(s2), first(d1), first(d2) " +
                        "FROM x " +
                        "WHERE ts BETWEEN '2023-05-16T00:00:00.00Z' AND '2023-05-16T00:10:00.00Z' " +
                        "AND s2 = ('foo') " +
                        "GROUP BY s1, s2 " +
                        "ORDER BY s1, s2;",
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
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l, s, l+1 from t group by l+1,s, l, l+2";
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [l,s,column]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s,column,column1]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQueryNoLeakCheck(
                    "l\ts\tcolumn\n" +
                            "1\ta\t2\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByIndexOutsideSelectList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select x, x%2 as y from long_sequence(2))");
            assertError(
                    "select * from tab group by 5",
                    "[27] GROUP BY position 5 is not in select list"
            );
        });
    }

    @Test
    public void testGroupByInterval1() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "i\tcount\n" +
                        "('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\t2\n" +
                        "\t1\n",
                "select i, count() from (" +
                        "  (select interval(100000,200000) i) " +
                        "  union all " +
                        "  (select interval(100000,200000) i) " +
                        "  union all " +
                        "  (select null::interval i)" +
                        ")"
        ));
    }

    @Test
    public void testGroupByInterval2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "i\ts\tcount\n" +
                        "('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tfoobar\t2\n" +
                        "\t\t1\n",
                "select i, s, count() from (" +
                        "  (select interval(100000,200000) i, 'foobar' s) " +
                        "  union all " +
                        "  (select interval(100000,200000) i, 'foobar' s) " +
                        "  union all " +
                        "  (select null::interval i, null::string s)" +
                        ")"
        ));
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
    public void testGroupBySingleVarcharKeyFromSampleByWithFill() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (vch varchar, l long, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into t values \n" +
                    "('USD', 1, '2021-11-17T17:00:00.000000Z'),\n" +
                    "('USD', 2, '2021-11-17T17:35:02.000000Z'),\n" +
                    "('EUR', 3, '2021-11-17T17:45:02.000000Z'),\n" +
                    "('USD', 1, '2021-11-17T19:04:00.000000Z'),\n" +
                    "('USD', 2, '2021-11-17T19:35:02.000000Z'),\n" +
                    "('USD', 3, '2021-11-17T19:45:02.000000Z');");

            String query = "with samp as (\n" +
                    "  select ts, vch, min(l), max(l)\n" +
                    "  from t\n" +
                    "  sample by 1h fill(prev)\n" +
                    ")\n" +
                    "select vch, sum(min)\n" +
                    "from samp;";

            assertQueryNoLeakCheck(
                    "vch\tsum\n" +
                            "EUR\t9\n" +
                            "USD\t3\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupBySingleVarcharKeyFromUnionAllAndFunction() throws Exception {
        // grouping by a single varchar key uses a fast-path which assume most of the time keys are just stable
        // pointers into mmaped memory. This test verifies that the fast-path is not broken when some grouping
        // keys are indeed stable and some are not. to_uppercase() produces a varchar which is not stable.

        assertMemoryLeak(() -> {
            execute("create table tab1 as (select (x % 5)::varchar as vch, x, now() as ts from long_sequence(20)) \n" +
                    "timestamp(ts) PARTITION by day");

            String query = "with \n" +
                    "  w1 as (\n" +
                    "    select * from tab1\n" +
                    "    order by vch asc\n" +
                    "  ), \n" +
                    "  w2 as (\n" +
                    "    select * from tab1\n" +
                    "    order by vch desc\n" +
                    "  ),\n" +
                    "  u as (select * from w1\n" +
                    "    UNION all w2\n" +
                    "  ),\n" +
                    "  uo as (\n" +
                    "    select * from u order by vch\n" +
                    "  ),\n" +
                    "  grouped as (\n" +
                    "    select vch, count(vch)\n" +
                    "    from uo\n" +
                    "    order by vch\n" +
                    "  ),\n" +
                    "  nested as (\n" +
                    "    select vch, sum(count) from grouped\n" +
                    "    group by vch\n" +
                    "    order by vch\n" +
                    "  )\n" +
                    "select tab1.vch, tab1.x, nested.vch as nested_vch, sum\n" +
                    "from tab1\n" +
                    "join nested on nested.vch = tab1.vch;";
            assertQueryNoLeakCheck(
                    "vch\tx\tnested_vch\tsum\n" +
                            "1\t1\t1\t8\n" +
                            "2\t2\t2\t8\n" +
                            "3\t3\t3\t8\n" +
                            "4\t4\t4\t8\n" +
                            "0\t5\t0\t8\n" +
                            "1\t6\t1\t8\n" +
                            "2\t7\t2\t8\n" +
                            "3\t8\t3\t8\n" +
                            "4\t9\t4\t8\n" +
                            "0\t10\t0\t8\n" +
                            "1\t11\t1\t8\n" +
                            "2\t12\t2\t8\n" +
                            "3\t13\t3\t8\n" +
                            "4\t14\t4\t8\n" +
                            "0\t15\t0\t8\n" +
                            "1\t16\t1\t8\n" +
                            "2\t17\t2\t8\n" +
                            "3\t18\t3\t8\n" +
                            "4\t19\t4\t8\n" +
                            "0\t20\t0\t8\n",
                    query,
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testGroupByVarchar() throws Exception {
        assertQuery(
                "key\tmax\n" +
                        "0\t100\n" +
                        "1\t91\n" +
                        "2\t92\n" +
                        "3\t93\n" +
                        "4\t94\n" +
                        "5\t95\n" +
                        "6\t96\n" +
                        "7\t97\n" +
                        "8\t98\n" +
                        "9\t99\n",
                "select key, max(value) from t group by key order by key",
                "create table t as ( select (x%10)::varchar key, x as value from long_sequence(100)); ",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByWithAliasClash1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t as (" +
                            "    select 1 as l, 'a' as s, -1 max " +
                            "    union all " +
                            "    select 1, 'a', -2" +
                            "    )"
            );

            String query = "select s, max, max(l) from t group by s, max order by s, max";
            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [s, max]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [s,max]\n" +
                            "      values: [max(l)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );

            assertQueryNoLeakCheck(
                    "s\tmax\tmax1\n" +
                            "a\t-2\t1\n" +
                            "a\t-1\t1\n",
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
            execute("create table t1 as (select x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), t2.x " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x " +
                    "order by t1.x, t2.x";

            assertQueryNoLeakCheck(
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
            execute("create table t1 as (select x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), case when t1.x > 1 then 100*t1.x else 10*t2.x end " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x " +
                    "order by t1.x, t2.x";

            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Sort light\n" +
                            "      keys: [x, x1]\n" +
                            "        VirtualRecord\n" +
                            "          functions: [x,max,case([1<x,100*x,10*x1]),x1]\n" +
                            "            GroupBy vectorized: false\n" +
                            "              keys: [x,x1]\n" +
                            "              values: [max(y)]\n" +
                            "                SelectedRecord\n" +
                            "                    Hash Join Light\n" +
                            "                      condition: t2.y=t1.y\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: t1\n" +
                            "                        Hash\n" +
                            "                            PageFrame\n" +
                            "                                Row forward scan\n" +
                            "                                Frame forward scan on: t2\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t1 as (select x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), case when t1.x > 1 then 30*t1.x else 20*t2.x end " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, case when t1.x > 1 then 30*t1.x else 20*t2.x end";

            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [x,max,case]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [x,case,x1]\n" +
                            "      values: [max(y)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: t2.y=t1.y\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: t1\n" +
                            "                Hash\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: t2\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t1 as (select x::int as x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x::int as x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), dateadd('d', t1.x, '2023-03-01T00:00:00')::long + t2.x " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, dateadd('d', t1.x, '2023-03-01T00:00:00') " +
                    "order by 1";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [x]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [x,max,dateadd::long+x1]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [x,x1,dateadd]\n" +
                            "          values: [max(y)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: t2.y=t1.y\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: t1\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: t2\n"
            );

            assertQueryNoLeakCheck(
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
            execute("create table t1 as (select x::int as x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x::int as x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), dateadd('s', max(t2.y)::int, dateadd('d', t1.x, '2023-03-01T00:00:00') ) " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, dateadd('d', t1.x, '2023-03-01T00:00:00') " +
                    "order by 1, 2, 3";

            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [x, max, dateadd]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [x,max,dateadd('s',max::int,dateadd)]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [x,dateadd,x1]\n" +
                            "          values: [max(y)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: t2.y=t1.y\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: t1\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: t2\n"
            );

            assertQueryNoLeakCheck(
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
    public void testGroupByWithLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table dim_apTemperature as (" +
                            "  select x::int id," +
                            "         rnd_str('a','b','c') as category," +
                            "         rnd_float() aparent_temperature" +
                            "  from long_sequence(10)" +
                            ");"
            );
            execute(
                    "create table fact_table as (" +
                            "  select x::int id_aparent_temperature," +
                            "         (x * 120000000)::timestamp date_time," +
                            "         rnd_float() radiation," +
                            "         rnd_float() energy_power" +
                            "  from long_sequence(10)" +
                            ");"
            );

            final String expectedResult = "dim_ap_temperature__category\tfact_table__date_time_day\tfact_table__avg_radiation\tfact_table__energy_power\n" +
                    "c\t1970-01-01T00:00:00.000000Z\t0.5421442091464996\t0.6145070195198059\n" +
                    "b\t1970-01-01T00:00:00.000000Z\t0.525111973285675\t0.6171746551990509\n" +
                    "a\t1970-01-01T00:00:00.000000Z\t0.33940355479717255\t0.47865718603134155\n";

            // With GROUP BY clause
            // This query is generated by Cube.js
            final String query1 = "SELECT\n" +
                    "  \"dim_ap_temperature\".category \"dim_ap_temperature__category\",\n" +
                    "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC')) \"fact_table__date_time_day\",\n" +
                    "  avg(\"fact_table\".radiation) \"fact_table__avg_radiation\",\n" +
                    "  avg(\"fact_table\".energy_power) \"fact_table__energy_power\"\n" +
                    "FROM\n" +
                    "  fact_table AS \"fact_table\"\n" +
                    "  LEFT JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                    "GROUP BY\n" +
                    "  \"dim_ap_temperature__category\",\n" +
                    "  \"fact_table__date_time_day\"\n" +
                    "ORDER BY\n" +
                    "  \"fact_table__avg_radiation\" DESC\n" +
                    "LIMIT\n" +
                    "  10000;";
            assertSql(expectedResult, query1);
            assertPlanNoLeakCheck(
                    query1,
                    "Sort light lo: 10000\n" +
                            "  keys: [fact_table__avg_radiation desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [dim_ap_temperature__category,fact_table__date_time_day,fact_table__avg_radiation,fact_table__energy_power]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [dim_ap_temperature__category,fact_table__date_time_day]\n" +
                            "          values: [avg(radiation),avg(energy_power)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Outer Join Light\n" +
                            "                  condition: dim_ap_temperature.id=fact_table.id_aparent_temperature\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: fact_table\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: dim_apTemperature\n"
            );

            // With no aliases in GROUP BY clause - 1
            final String query2 = "SELECT\n" +
                    "  \"dim_ap_temperature\".category \"dim_ap_temperature__category\",\n" +
                    "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC')) \"fact_table__date_time_day\",\n" +
                    "  avg(\"fact_table\".radiation) \"fact_table__avg_radiation\",\n" +
                    "  avg(\"fact_table\".energy_power) \"fact_table__energy_power\"\n" +
                    "FROM\n" +
                    "  fact_table AS \"fact_table\"\n" +
                    "  LEFT JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                    "GROUP BY\n" +
                    "  \"dim_ap_temperature\".category,\n" +
                    "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC'))\n" +
                    "ORDER BY\n" +
                    "  \"fact_table__avg_radiation\" DESC\n" +
                    "LIMIT\n" +
                    "  10000;";
            assertSql(expectedResult, query2);
            assertPlanNoLeakCheck(
                    query2,
                    "Sort light lo: 10000\n" +
                            "  keys: [fact_table__avg_radiation desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [category,timestamp_floor,fact_table__avg_radiation,fact_table__energy_power]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [category,timestamp_floor]\n" +
                            "          values: [avg(radiation),avg(energy_power)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Outer Join Light\n" +
                            "                  condition: dim_ap_temperature.id=fact_table.id_aparent_temperature\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: fact_table\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: dim_apTemperature\n"
            );

            // With no aliases in GROUP BY clause - 2
            final String query3 = "SELECT\n" +
                    "  category \"dim_ap_temperature__category\",\n" +
                    "  timestamp_floor('d', to_timezone(date_time, 'UTC')) \"fact_table__date_time_day\",\n" +
                    "  avg(radiation) \"fact_table__avg_radiation\",\n" +
                    "  avg(energy_power) \"fact_table__energy_power\"\n" +
                    "FROM\n" +
                    "  fact_table AS \"fact_table\"\n" +
                    "  LEFT JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                    "GROUP BY\n" +
                    "  category,\n" +
                    "  timestamp_floor('d', to_timezone(date_time, 'UTC'))\n" +
                    "ORDER BY\n" +
                    "  \"fact_table__avg_radiation\" DESC\n" +
                    "LIMIT\n" +
                    "  10000;";
            assertSql(expectedResult, query3);
            assertPlanNoLeakCheck(
                    query3,
                    "Sort light lo: 10000\n" +
                            "  keys: [fact_table__avg_radiation desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [category,timestamp_floor,fact_table__avg_radiation,fact_table__energy_power]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [category,timestamp_floor]\n" +
                            "          values: [avg(radiation),avg(energy_power)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Outer Join Light\n" +
                            "                  condition: dim_ap_temperature.id=fact_table.id_aparent_temperature\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: fact_table\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: dim_apTemperature\n"
            );

            // Without GROUP BY clause
            final String query4 = "SELECT\n" +
                    "  \"dim_ap_temperature\".category \"dim_ap_temperature__category\",\n" +
                    "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC')) \"fact_table__date_time_day\",\n" +
                    "  avg(\"fact_table\".radiation) \"fact_table__avg_radiation\",\n" +
                    "  avg(\"fact_table\".energy_power) \"fact_table__energy_power\"\n" +
                    "FROM\n" +
                    "  fact_table AS \"fact_table\"\n" +
                    "  LEFT JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                    "ORDER BY\n" +
                    "  \"fact_table__avg_radiation\" DESC\n" +
                    "LIMIT\n" +
                    "  10000;";
            assertSql(expectedResult, query4);
            assertPlanNoLeakCheck(
                    query4,
                    "Sort light lo: 10000\n" +
                            "  keys: [fact_table__avg_radiation desc]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [dim_ap_temperature__category,fact_table__date_time_day]\n" +
                            "      values: [avg(radiation),avg(energy_power)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Outer Join Light\n" +
                            "              condition: dim_ap_temperature.id=fact_table.id_aparent_temperature\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: fact_table\n" +
                            "                Hash\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: dim_apTemperature\n"
            );
        });
    }

    @Test
    public void testGroupByWithNonConstantSelectClauseExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l,s,rnd_int(0,1,0)/10 from t group by l,s";
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [l,s,memoize(rnd_int(0,1,0)/10)]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [l,s]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            assertQueryNoLeakCheck(
                    "l\ts\tcolumn\n" +
                            "1\ta\t0\n",
                    query,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithTimestampKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (\n" +
                    "  timestamp TIMESTAMP,\n" +
                    "  bar INT\n" +
                    ") TIMESTAMP (timestamp)\n" +
                    "PARTITION BY DAY;");
            execute("INSERT INTO foo VALUES ('2020', 0);");
            String query = "SELECT\n" +
                    "  timestamp AS time,\n" +
                    "  TO_STR(timestamp, 'yyyy-MM-dd'),\n" +
                    "  SUM(1) \n" +
                    "FROM foo;";
            assertQueryNoLeakCheck(
                    "time\tTO_STR\tSUM\n" +
                            "2020-01-01T00:00:00.000000Z\t2020-01-01\t1\n",
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
            execute("create table t (s1 symbol, s2 symbol, l long, ts timestamp) timestamp(ts) partition by day;");
            execute(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select s2, sum(l) from t where s2 in ('c') latest on ts partition by s1";
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [s2]\n" +
                            "  values: [sum(l)]\n" +
                            "    LatestByDeferredListValuesFiltered\n" +
                            "      filter: s2 in [c]\n" +
                            "        Frame backward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table t (s1 symbol index, s2 symbol index, l long, ts timestamp) timestamp(ts) partition by day;");
            execute(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select s2, sum(l) from t where s2 in ('c', 'd') latest on ts partition by s1 order by s2";
            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [s2]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [s2]\n" +
                            "      values: [sum(l)]\n" +
                            "        LatestByDeferredListValuesFiltered\n" +
                            "          filter: s2 in [c,d]\n" +
                            "            Frame backward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table t (s1 symbol index, s2 symbol index, l long, ts timestamp) timestamp(ts) partition by day;");
            execute(
                    "insert into t values " +
                            "('a', 'c', 11, '2021-11-17T17:35:01.000000Z')," +
                            "('a', 'd', 12, '2021-11-17T17:35:02.000000Z')," +
                            "('b', 'd', 13, '2021-11-17T17:35:01.000000Z')," +
                            "('b', 'c', 14, '2021-11-17T17:35:02.000000Z');"
            );
            String query = "select concat('_', s2, '_'), sum(l) from t where s2 in ('d') latest on ts partition by s1";
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [concat]\n" +
                            "  values: [sum(l)]\n" +
                            "    LatestByDeferredListValuesFiltered\n" +
                            "      filter: s2 in [d]\n" +
                            "        Frame backward scan on: t\n"
            );
            assertQueryNoLeakCheck(
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
    public void testLiftAliasesFromInnerSelect1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    "select a, b, c as z, count(*) as views\n" +
                            "from x\n" +
                            "where a = 1\n" +
                            "group by a,b,z\n";
            assertPlanNoLeakCheck(
                    query,
                    "Async JIT Group By workers: 1\n" +
                            "  keys: [a,b,z]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: a=1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tb\tz\tviews\n" +
                            "1\t2\t3\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect10() throws Exception {
        // test that it properly handles max(ts) ts on lhs and data.ts ts on rhs
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            execute("insert into tab " +
                    "select (100000+x)::timestamp, " +
                    "rnd_long(1,20,10), " +
                    "rnd_long(1,1000,5) " +
                    "from long_sequence(1000000)");

            String query1 = "select max(ts) as ts, i, avg(j) as avg, sum(j::double) as sum, first(j::double) as first_value " +
                    "from (" +
                    "  select data.ts, data.i, data.j " +
                    "  from (select i, max(ts) as max from tab group by i) cnt " +
                    "  join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                    "  order by data.i, ts" +
                    ")" +
                    "group by i " +
                    "order by i ";

            // cross-check with re-write using aggregate functions
            assertSql(
                    "ts\ti\tavg\tsum\tfirst_value\n" +
                            "1970-01-01T00:00:01.099967Z\tnull\t495.40261282660333\t1668516.0\t481.0\n" +
                            "1970-01-01T00:00:01.099995Z\t1\t495.08707124010556\t1688742.0\tnull\n" +
                            "1970-01-01T00:00:01.099973Z\t2\t506.5011448196909\t1769715.0\t697.0\n" +
                            "1970-01-01T00:00:01.099908Z\t3\t505.95267958950967\t1774882.0\t16.0\n" +
                            "1970-01-01T00:00:01.099977Z\t4\t501.16155593412833\t1765091.0\t994.0\n" +
                            "1970-01-01T00:00:01.099994Z\t5\t494.87667161961366\t1665260.0\t701.0\n" +
                            "1970-01-01T00:00:01.099991Z\t6\t500.67453098351336\t1761373.0\t830.0\n" +
                            "1970-01-01T00:00:01.099998Z\t7\t497.7231450719823\t1797776.0\t293.0\n" +
                            "1970-01-01T00:00:01.099997Z\t8\t498.6340425531915\t1757685.0\t868.0\n" +
                            "1970-01-01T00:00:01.099992Z\t9\t499.1758750361585\t1725651.0\t528.0\n" +
                            "1970-01-01T00:00:01.099989Z\t10\t500.3242937853107\t1771148.0\t936.0\n" +
                            "1970-01-01T00:00:01.099976Z\t11\t501.4019192774485\t1776467.0\t720.0\n" +
                            "1970-01-01T00:00:01.099984Z\t12\t489.8953058321479\t1721982.0\t949.0\n" +
                            "1970-01-01T00:00:01.099952Z\t13\t500.65723270440253\t1751299.0\t518.0\n" +
                            "1970-01-01T00:00:01.099996Z\t14\t506.8769141866513\t1754301.0\tnull\n" +
                            "1970-01-01T00:00:01.100000Z\t15\t497.0794058840331\t1740275.0\t824.0\n" +
                            "1970-01-01T00:00:01.099979Z\t16\t499.3338209479228\t1706723.0\t38.0\n" +
                            "1970-01-01T00:00:01.099951Z\t17\t492.7804469273743\t1764154.0\t698.0\n" +
                            "1970-01-01T00:00:01.099999Z\t18\t501.4806333050608\t1773737.0\t204.0\n" +
                            "1970-01-01T00:00:01.099957Z\t19\t501.01901034386356\t1792145.0\t712.0\n" +
                            "1970-01-01T00:00:01.099987Z\t20\t498.1350566366541\t1715079.0\t188.0\n",
                    query1
            );

            assertPlanNoLeakCheck(
                    query1,
                    "Radix sort light\n" +
                            "  keys: [i]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [ts,i,avg,sum,first_value]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [i]\n" +
                            "          values: [max(ts),avg(j),sum(j::double),first(j::double)]\n" +
                            "            Sort\n" +
                            "              keys: [i, ts]\n" +
                            "                SelectedRecord\n" +
                            "                    Filter filter: data.ts>=cnt.max-80000\n" +
                            "                        Hash Join Light\n" +
                            "                          condition: data.i=cnt.i\n" +
                            "                            Async Group By workers: 1\n" +
                            "                              keys: [i]\n" +
                            "                              values: [max(ts)]\n" +
                            "                              filter: null\n" +
                            "                                PageFrame\n" +
                            "                                    Row forward scan\n" +
                            "                                    Frame forward scan on: tab\n" +
                            "                            Hash\n" +
                            "                                PageFrame\n" +
                            "                                    Row forward scan\n" +
                            "                                    Frame forward scan on: tab\n"
            );

            String query2 = "select last(ts) as ts, " +
                    "i, " +
                    "last(avg) as avg, " +
                    "last(sum) as sum, " +
                    "last(first_value) as first_value " +
                    "from (  " +
                    "  select * from (" +
                    "    select ts, i, " +
                    "    avg(j) over (partition by i order by ts range between 80000 preceding and current row) avg, " +
                    "    sum(j) over (partition by i order by ts range between 80000 preceding and current row) sum, " +
                    "    first_value(j) over (partition by i order by ts range between 80000 preceding and current row) first_value, " +
                    "    from tab ) " +
                    "  limit -100 )" +
                    "order by i";

            assertQueryNoLeakCheck(
                    "ts\ti\tavg\tsum\tfirst_value\n" +
                            "1970-01-01T00:00:01.099967Z\tnull\t495.40261282660333\t1668516.0\t481\n" +
                            "1970-01-01T00:00:01.099995Z\t1\t495.08707124010556\t1688742.0\tnull\n" +
                            "1970-01-01T00:00:01.099973Z\t2\t506.5011448196909\t1769715.0\t697\n" +
                            "1970-01-01T00:00:01.099908Z\t3\t505.95267958950967\t1774882.0\t16\n" +
                            "1970-01-01T00:00:01.099977Z\t4\t501.16155593412833\t1765091.0\t994\n" +
                            "1970-01-01T00:00:01.099994Z\t5\t494.87667161961366\t1665260.0\t701\n" +
                            "1970-01-01T00:00:01.099991Z\t6\t500.67453098351336\t1761373.0\t830\n" +
                            "1970-01-01T00:00:01.099998Z\t7\t497.7231450719823\t1797776.0\t293\n" +
                            "1970-01-01T00:00:01.099997Z\t8\t498.6340425531915\t1757685.0\t868\n" +
                            "1970-01-01T00:00:01.099992Z\t9\t499.1758750361585\t1725651.0\t528\n" +
                            "1970-01-01T00:00:01.099989Z\t10\t500.3242937853107\t1771148.0\t936\n" +
                            "1970-01-01T00:00:01.099976Z\t11\t501.4019192774485\t1776467.0\t720\n" +
                            "1970-01-01T00:00:01.099984Z\t12\t489.8953058321479\t1721982.0\t949\n" +
                            "1970-01-01T00:00:01.099952Z\t13\t500.65723270440253\t1751299.0\t518\n" +
                            "1970-01-01T00:00:01.099996Z\t14\t506.8769141866513\t1754301.0\tnull\n" +
                            "1970-01-01T00:00:01.100000Z\t15\t497.0794058840331\t1740275.0\t824\n" +
                            "1970-01-01T00:00:01.099979Z\t16\t499.3338209479228\t1706723.0\t38\n" +
                            "1970-01-01T00:00:01.099951Z\t17\t492.7804469273743\t1764154.0\t698\n" +
                            "1970-01-01T00:00:01.099999Z\t18\t501.4806333050608\t1773737.0\t204\n" +
                            "1970-01-01T00:00:01.099957Z\t19\t501.01901034386356\t1792145.0\t712\n" +
                            "1970-01-01T00:00:01.099987Z\t20\t498.1350566366541\t1715079.0\t188\n",
                    query2,
                    null,
                    true,
                    true,
                    false
            );

            assertPlanNoLeakCheck(
                    query2,
                    "Radix sort light\n" +
                            "  keys: [i]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [i]\n" +
                            "      values: [last(ts),last(avg),last(sum),last(first_value)]\n" +
                            "        Limit lo: -100 skip-over-rows: 999900 limit: 100\n" +
                            "            Window\n" +
                            "              functions: [avg(j) over (partition by [i] range between 80000 preceding and current row),sum(j) over (partition by [i] range between 80000 preceding and current row),first_value(j) over (partition by [i] range between 80000 preceding and current row)]\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect11() throws Exception {
        // test output naming
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    "select a, b as B, c as z, count(*) as views\n" +
                            "from x\n" +
                            "where a = 1\n" +
                            "group by a,b,z\n";
            assertPlanNoLeakCheck(
                    query,
                    "Async JIT Group By workers: 1\n" +
                            "  keys: [a,b,z]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: a=1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tb\tz\tviews\n" +
                            "1\t2\t3\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect12() throws Exception {
        // test output naming
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    "select a, b as B, c as z, count(*) as views\n" +
                            "from x\n" +
                            "where a = 1\n" +
                            "group by a,B,z\n";
            assertPlanNoLeakCheck(
                    query,
                    "Async JIT Group By workers: 1\n" +
                            "  keys: [a,B,z]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: a=1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tB\tz\tviews\n" +
                            "1\t2\t3\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    "select a, b, c as z, count(*) as views\n" +
                            "from x\n" +
                            "where a = 1\n" +
                            "group by a,b,c\n";
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [a,b,c,views]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [a,b,c]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: a=1\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tb\tz\tviews\n"
                            + "1\t2\t3\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    "select a, b, c, count(*) as views\n" +
                            "from x\n" +
                            "where a = 1\n" +
                            "group by a,b,c\n";
            assertPlanNoLeakCheck(
                    query,
                    "Async JIT Group By workers: 1\n" +
                            "  keys: [a,b,c]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: a=1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tb\tc\tviews\n" +
                            "1\t2\t3\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect4() throws Exception {
        // Check that if a select-choose node is elided by the factory, the WHERE condition can
        // be retrieved from the inner PartitionFrame.
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select" +
                    " timestamp_sequence(0, 15*60*1000000L) delivery_start_utc," +
                    " rnd_symbol('sf', null) seller," +
                    " rnd_symbol('sf', null) buyer," +
                    " rnd_double() volume_mw" +
                    " from long_sequence(100)" +
                    "), index(seller), index(buyer) timestamp(delivery_start_utc)");
            String expected = "y_utc_15m\ty_sf_position_mw\n" +
                    "1970-01-01T00:00:00.000000Z\t-0.2246301342497259\n" +
                    "1970-01-01T00:30:00.000000Z\t-0.6508594025855301\n" +
                    "1970-01-01T00:45:00.000000Z\t-0.9856290845874263\n" +
                    "1970-01-01T01:00:00.000000Z\t-0.5093827001617407\n" +
                    "1970-01-01T01:30:00.000000Z\t0.5599161804800813\n" +
                    "1970-01-01T01:45:00.000000Z\t0.2390529010846525\n" +
                    "1970-01-01T02:00:00.000000Z\t-0.6778564558839208\n" +
                    "1970-01-01T02:15:00.000000Z\t0.38539947865244994\n" +
                    "1970-01-01T02:30:00.000000Z\t-0.33608255572515877\n" +
                    "1970-01-01T02:45:00.000000Z\t0.7675673070796104\n" +
                    "1970-01-01T03:00:00.000000Z\t0.6217326707853098\n" +
                    "1970-01-01T03:15:00.000000Z\t0.6381607531178513\n" +
                    "1970-01-01T03:45:00.000000Z\t0.12026122412833129\n" +
                    "1970-01-01T04:00:00.000000Z\t-0.8912587536603974\n" +
                    "1970-01-01T04:15:00.000000Z\t-0.42281342727402726\n" +
                    "1970-01-01T04:30:00.000000Z\t-0.7664256753596138\n" +
                    "1970-01-01T05:15:00.000000Z\t-0.8847591603509142\n" +
                    "1970-01-01T05:30:00.000000Z\t0.931192737286751\n" +
                    "1970-01-01T05:45:00.000000Z\t0.8001121139739173\n" +
                    "1970-01-01T06:00:00.000000Z\t0.92050039469858\n" +
                    "1970-01-01T06:15:00.000000Z\t0.456344569609078\n" +
                    "1970-01-01T06:30:00.000000Z\t0.40455469747939254\n" +
                    "1970-01-01T06:45:00.000000Z\t0.5659429139861241\n" +
                    "1970-01-01T07:00:00.000000Z\t-0.6821660861001273\n" +
                    "1970-01-01T07:30:00.000000Z\t-0.11585982949541473\n" +
                    "1970-01-01T07:45:00.000000Z\t0.8164182592467494\n" +
                    "1970-01-01T08:00:00.000000Z\t0.5449155021518948\n" +
                    "1970-01-01T08:30:00.000000Z\t0.49428905119584543\n" +
                    "1970-01-01T08:45:00.000000Z\t-0.6551335839796312\n" +
                    "1970-01-01T09:15:00.000000Z\t0.9540069089049732\n" +
                    "1970-01-01T09:30:00.000000Z\t-0.03167026265669903\n" +
                    "1970-01-01T09:45:00.000000Z\t-0.19751370382305056\n" +
                    "1970-01-01T10:00:00.000000Z\t0.6806873134626418\n" +
                    "1970-01-01T10:15:00.000000Z\t-0.24008362859107102\n" +
                    "1970-01-01T10:30:00.000000Z\t-0.9455893004802433\n" +
                    "1970-01-01T10:45:00.000000Z\t-0.6247427794126656\n" +
                    "1970-01-01T11:00:00.000000Z\t-0.3901731258748704\n" +
                    "1970-01-01T11:15:00.000000Z\t-0.10643046345788132\n" +
                    "1970-01-01T11:30:00.000000Z\t0.07246172621937097\n" +
                    "1970-01-01T11:45:00.000000Z\t-0.3679848625908545\n" +
                    "1970-01-01T12:00:00.000000Z\t0.6697969295620055\n" +
                    "1970-01-01T12:15:00.000000Z\t-0.26369335635512836\n" +
                    "1970-01-01T12:45:00.000000Z\t-0.19846258365662472\n" +
                    "1970-01-01T13:00:00.000000Z\t-0.8595900073631431\n" +
                    "1970-01-01T13:15:00.000000Z\t0.7458169804091256\n" +
                    "1970-01-01T13:30:00.000000Z\t0.4274704286353759\n" +
                    "1970-01-01T14:00:00.000000Z\t-0.8291193369353376\n" +
                    "1970-01-01T14:30:00.000000Z\t0.2711532808184136\n" +
                    "1970-01-01T15:00:00.000000Z\t-0.8189713915910615\n" +
                    "1970-01-01T15:15:00.000000Z\t0.7365115215570027\n" +
                    "1970-01-01T15:30:00.000000Z\t-0.9418719455092096\n" +
                    "1970-01-01T16:00:00.000000Z\t-0.05024615679069011\n" +
                    "1970-01-01T16:15:00.000000Z\t-0.8952510116133903\n" +
                    "1970-01-01T16:30:00.000000Z\t-0.029227696942726644\n" +
                    "1970-01-01T16:45:00.000000Z\t-0.7668146556860689\n" +
                    "1970-01-01T17:00:00.000000Z\t-0.05158459929273784\n" +
                    "1970-01-01T17:15:00.000000Z\t-0.06846631555382798\n" +
                    "1970-01-01T17:30:00.000000Z\t-0.5708643723875381\n" +
                    "1970-01-01T17:45:00.000000Z\t0.7260468106076399\n" +
                    "1970-01-01T18:15:00.000000Z\t-0.1010501916946902\n" +
                    "1970-01-01T18:30:00.000000Z\t-0.05094182589333662\n" +
                    "1970-01-01T18:45:00.000000Z\t-0.38402128906440336\n" +
                    "1970-01-01T19:15:00.000000Z\t0.7694744648762927\n" +
                    "1970-01-01T19:45:00.000000Z\t0.6901976778065181\n" +
                    "1970-01-01T20:00:00.000000Z\t-0.5913874468544745\n" +
                    "1970-01-01T20:30:00.000000Z\t-0.14261321308606745\n" +
                    "1970-01-01T20:45:00.000000Z\t0.4440250924606578\n" +
                    "1970-01-01T21:00:00.000000Z\t-0.09618589590900506\n" +
                    "1970-01-01T21:15:00.000000Z\t-0.08675950660182763\n" +
                    "1970-01-01T21:30:00.000000Z\t-0.741970173888595\n" +
                    "1970-01-01T21:45:00.000000Z\t0.4167781163798937\n" +
                    "1970-01-01T22:00:00.000000Z\t-0.05514933756198426\n" +
                    "1970-01-01T22:30:00.000000Z\t-0.2093569947644236\n" +
                    "1970-01-01T22:45:00.000000Z\t-0.8439276969435359\n" +
                    "1970-01-01T23:00:00.000000Z\t-0.03973283003449557\n" +
                    "1970-01-01T23:15:00.000000Z\t-0.8551850405049611\n" +
                    "1970-01-01T23:45:00.000000Z\t0.6226001464598434\n" +
                    "1970-01-02T00:00:00.000000Z\t-0.7195457109208119\n" +
                    "1970-01-02T00:15:00.000000Z\t-0.23493793601747937\n" +
                    "1970-01-02T00:30:00.000000Z\t-0.6334964081687151\n";
            String query = "SELECT\n" +
                    "    delivery_start_utc as y_utc_15m,\n" +
                    "    sum(case\n" +
                    "            when seller='sf' then -1.0*volume_mw\n" +
                    "            when buyer='sf' then 1.0*volume_mw\n" +
                    "            else 0.0\n" +
                    "        end)\n" +
                    "    as y_sf_position_mw\n" +
                    "FROM (\n" +
                    "    SELECT delivery_start_utc, seller, buyer, volume_mw FROM trades\n" +
                    "    WHERE\n" +
                    "        (seller = 'sf' OR buyer = 'sf')\n" +
                    "    )\n" +
                    "group by y_utc_15m " +
                    "order by y_utc_15m";

            assertQueryNoLeakCheck(
                    expected,
                    query,
                    "y_utc_15m",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [y_utc_15m]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [y_utc_15m]\n" +
                            "      values: [sum(case([seller='sf',-1.0*volume_mw,buyer='sf',1.0*volume_mw,0.0]))]\n" +
                            "        SelectedRecord\n" +
                            "            Async JIT Filter workers: 1\n" +
                            "              filter: (seller='sf' or buyer='sf') [pre-touch]\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: trades\n"
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect5() throws Exception {
        // Test aliasing a function name
        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            execute("insert into x values (1, 5, '4', now()), (1, 3, '1', now())");
            drainWalQueue();
            String query = "select a, sum(b) sum, c as z, count(*) views\n" +
                    "from x\n" +
                    "where a = 1\n" +
                    "group by a,b,z\n" +
                    "order by a,b,z\n";
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Sort light\n" +
                            "      keys: [a, b, z]\n" +
                            "        VirtualRecord\n" +
                            "          functions: [a,sum,z,views,b]\n" +
                            "            Async JIT Group By workers: 1\n" +
                            "              keys: [a,z,b]\n" +
                            "              values: [sum(b),count(*)]\n" +
                            "              filter: a=1\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
                    "a\tsum\tz\tviews\n" +
                            "1\t2\t3\t1\n" +
                            "1\t3\t1\t1\n" +
                            "1\t5\t4\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect6() throws Exception {
        // Test ClickBench Q39 plan
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hits\n" +
                    "(\n" +
                    "    URL string,\n" +
                    "    Referer string,\n" +
                    "    TraficSourceID int,\n" +
                    "    SearchEngineID short,\n" +
                    "    AdvEngineID short,\n" +
                    "    EventTime timestamp,\n" +
                    "    CounterID int,\n" +
                    "    IsRefresh short\n" +
                    ") TIMESTAMP(EventTime) PARTITION BY DAY;");
            String query1 =
                    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews\n" +
                            "FROM hits\n" +
                            "WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0\n" +
                            "GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst\n" +
                            "ORDER BY PageViews DESC\n" +
                            "LIMIT 1000, 1010;";
            assertPlanNoLeakCheck(
                    query1,
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: (CounterID=62 and IsRefresh=0)\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: hits\n" +
                            "                  intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            );
            String query2 =
                    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL, COUNT(*) AS PageViews\n" +
                            "FROM hits\n" +
                            "WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0\n" +
                            "GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, URL\n" +
                            "ORDER BY PageViews DESC\n" +
                            "LIMIT 1000, 1010;";
            assertPlanNoLeakCheck(
                    query2,
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,PageViews]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: (CounterID=62 and IsRefresh=0)\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: hits\n" +
                            "                  intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            );
            String query3 = "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL, COUNT(*) AS PageViews, concat(lpad(cast(TraficSourceId as string), 10, '0'), lpad(cast(Referer as string), 32, '0')) as cat\n" +
                    "FROM hits\n" +
                    "WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0\n" +
                    "GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, URL, cat\n" +
                    "ORDER BY PageViews DESC\n" +
                    "LIMIT 1000, 1010;";
            assertPlanNoLeakCheck(
                    query3,
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,PageViews,cat]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,cat]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: (CounterID=62 and IsRefresh=0)\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: hits\n" +
                            "                  intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect7() throws Exception {
        // test duplicate key ordering
        assertMemoryLeak(() -> {
            execute("create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));");
            String query = "select key1 as k1, key2, key2, count(*) from t group by key2, k1 order by 1, 2";
            assertQueryNoLeakCheck(
                    "k1\tkey2\tkey21\tcount\n" +
                            "0\t0\t0\t2\n" +
                            "0\t2\t2\t3\n" +
                            "1\t1\t1\t3\n" +
                            "1\t3\t3\t2\n",
                    query,
                    null,
                    true,
                    true
            );
            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [k1, key2]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [k1,key2,key2,count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [k1,key2]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect8() throws Exception {
        // test ordering by number
        assertMemoryLeak(() -> {
            execute("create table t as ( select x%2 as key, x as value from long_sequence(100));");
            String query = "select key+1, key, key, count(*) from t group by key order by 1,2,3 desc";
            assertQueryNoLeakCheck(
                    "column\tkey\tkey1\tcount\n" +
                            "1\t0\t0\t50\n" +
                            "2\t1\t1\t50\n",
                    query,
                    null,
                    true,
                    true
            );
            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [column, key, key1 desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [key+1,key,key,count]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [key]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect9() throws Exception {
        // test args requiring de-aliasing when moved to rhs
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x::int as x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x::int as x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), dateadd('s', max(t2.y)::int, dateadd('d', t1.x, '2023-03-01T00:00:00') ) " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, dateadd('d', t1.x, '2023-03-01T00:00:00') " +
                    "order by 1, 2, 3";

            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [x, max, dateadd]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [x,max,dateadd('s',max::int,dateadd)]\n" +
                            "        GroupBy vectorized: false\n" +
                            "          keys: [x,dateadd,x1]\n" +
                            "          values: [max(y)]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: t2.y=t1.y\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: t1\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: t2\n"
            );

            assertQueryNoLeakCheck(
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
    public void testNestedGroupByWithExplicitGroupByClause() throws Exception {
        String expected = "url\tu_count\tcnt\tavg_m_sum\n" +
                "RXPEHNRXGZ\t4\t4\t414.25\n" +
                "DXYSBEOUOJ\t1\t1\t225.0\n" +
                "SXUXIBBTGP\t2\t2\t379.5\n" +
                "GWFFYUDEYY\t5\t5\t727.2\n" +
                "LOFJGETJRS\t2\t2\t524.5\n" +
                "ZSRYRFBVTM\t2\t2\t337.0\n" +
                "VTJWCPSWHY\t1\t1\t660.0\n" +
                "HGOOZZVDZJ\t1\t1\t540.0\n" +
                "SHRUEDRQQU\t2\t2\t468.0\n";
        assertQuery(
                expected,
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
        assertSql(
                expected,
                "WITH x_sample AS (\n" +
                        "  SELECT id, uuid, url, sum(metric) m_sum\n" +
                        "  FROM x\n" +
                        "  WHERE ts >= '1023-03-31T00:00:00' and ts <= '2023-04-02T23:59:59'\n" +
                        "  GROUP BY id, uuid, url\n" +
                        ")\n" +
                        "SELECT url, count(distinct uuid) u_count, count() cnt, avg(m_sum) avg_m_sum\n" +
                        "FROM x_sample\n" +
                        "GROUP BY url"
        );
    }

    @Test
    public void testOrderByOnAliasedColumnAfterGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tst ( ts timestamp ) timestamp(ts);");
            execute("insert into tst values ('2023-05-29T15:30:00.000000Z')");

            execute("create table data ( dts timestamp, s symbol ) timestamp(dts);");
            execute("insert into data values ('2023-05-29T15:29:59.000000Z', 'USD')");

            // single table
            assertQueryNoLeakCheck(
                    "ref0\n2023-05-29T15:30:00.000000Z\n",
                    "SELECT ts AS ref0 " +
                            "FROM tst " +
                            "GROUP BY ts " +
                            "ORDER BY ts",
                    "ref0",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ref0\n2023-05-29T15:30:00.000000Z\n",
                    "SELECT tst.ts AS ref0 " +
                            "FROM tst " +
                            "GROUP BY tst.ts " +
                            "ORDER BY tst.ts",
                    "ref0",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ref0\n2023-05-29T15:30:00.000000Z\n",
                    "SELECT tst.ts AS ref0 " +
                            "FROM tst " +
                            "GROUP BY ts " +
                            "ORDER BY tst.ts",
                    "ref0",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ref0\n2023-05-29T15:30:00.000000Z\n",
                    "SELECT ts AS ref0 " +
                            "FROM tst " +
                            "GROUP BY tst.ts " +
                            "ORDER BY ts",
                    "ref0",
                    true,
                    true
            );

            // joins
            for (String join : Arrays.asList("LT JOIN data ", "ASOF JOIN data ", "LEFT JOIN data on (tst.ts > data.dts) ", "INNER JOIN data on (tst.ts > data.dts) ", "CROSS JOIN data ")) {
                assertQueryNoLeakCheck(
                        "ref0\tdts\n2023-05-29T15:30:00.000000Z\t2023-05-29T15:29:59.000000Z\n",
                        "SELECT ts AS ref0, dts " +
                                "FROM tst " +
                                join +
                                "GROUP BY tst.ts, data.dts " +
                                "ORDER BY ts",
                        "ref0",
                        true,
                        true
                );

                assertQueryNoLeakCheck(
                        "ref0\tdts\n2023-05-29T15:30:00.000000Z\t2023-05-29T15:29:59.000000Z\n",
                        "SELECT ts AS ref0, dts " +
                                "FROM tst " +
                                join +
                                "GROUP BY ts, data.dts " +
                                "ORDER BY tst.ts",
                        "ref0",
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testSelectDistinctOnAliasedColumnWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (created timestamp, i int) timestamp(created)");
            execute("insert into tab select x::timestamp, x from long_sequence(3)");
            drainWalQueue();

            String query = "SELECT DISTINCT tab.created AS ref0 " +
                    "FROM tab " +
                    "WHERE (tab.created) IS NOT NULL " +
                    "GROUP BY tab.created " +
                    "ORDER BY tab.created";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [ref0]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [created]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [created]\n" +
                            "          filter: null!=created\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertQueryNoLeakCheck(
                    "ref0\n" +
                            "1970-01-01T00:00:00.000001Z\n" +
                            "1970-01-01T00:00:00.000002Z\n" +
                            "1970-01-01T00:00:00.000003Z\n",
                    query,
                    "ref0",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectDistinctOnExpressionWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (created timestamp, i int) timestamp(created)");
            execute("insert into tab select x::timestamp, x from long_sequence(3)");
            drainWalQueue();

            String query = "SELECT DISTINCT dateadd('h', 1, tab.created) AS ref0 " +
                    "FROM tab " +
                    "WHERE (tab.created) IS NOT NULL " +
                    "GROUP BY tab.created " +
                    "ORDER BY dateadd('h', 1, tab.created)";


            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [ref0]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [dateadd('h',1,created)]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [created]\n" +
                            "          filter: null!=created\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertQueryNoLeakCheck(
                    "ref0\n" +
                            "1970-01-01T01:00:00.000001Z\n" +
                            "1970-01-01T01:00:00.000002Z\n" +
                            "1970-01-01T01:00:00.000003Z\n",
                    query,
                    "ref0",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectDistinctOnUnaliasedColumnWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (created timestamp, i int) timestamp(created)");
            execute("insert into tab select x::timestamp, x from long_sequence(3)");
            drainWalQueue();

            String query = "SELECT DISTINCT tab.created " +
                    "FROM tab " +
                    "WHERE (tab.created) IS NOT NULL " +
                    "GROUP BY tab.created " +
                    "ORDER BY tab.created";

            assertPlanNoLeakCheck(
                    query,
                    "Radix sort light\n" +
                            "  keys: [created]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [created]\n" +
                            "      filter: null!=created\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertQueryNoLeakCheck(
                    "created\n" +
                            "1970-01-01T00:00:00.000001Z\n" +
                            "1970-01-01T00:00:00.000002Z\n" +
                            "1970-01-01T00:00:00.000003Z\n",
                    query,
                    "created",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectMatchingButInDifferentOrderThanGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "    sym symbol," +
                    "    bid double, " +
                    "    ts timestamp " +
                    ") timestamp(ts) partition by DAY");
            execute("insert into x " +
                    " select rnd_symbol('A', 'B'), rnd_double(), dateadd('m', x::int, 0::timestamp) " +
                    " from long_sequence(20)");

            String query = "select sym, hour(ts), avg(bid) avgBid from x group by hour(ts), sym order by hour(ts), sym";
            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [hour, sym]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sym,hour,avgBid]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [sym,hour]\n" +
                            "          values: [avg(bid)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: x\n"
            );
            assertQueryNoLeakCheck(
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
            execute("create table tab as (select x, x%2 as y from long_sequence(2))");
            assertError(
                    "select * from tab group by tab.*",
                    "[27] '*' is not allowed in GROUP BY"
            );
        });
    }

    @Test
    public void testSumOverSumColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"avg\" as (" +
                    "select rnd_symbol('A', 'B', 'C') category, " +
                    "rnd_double() sum, " +
                    "rnd_double() count, " +
                    "timestamp_sequence(0, 100000000000) timestamp " +
                    "from long_sequence(20)" +
                    ") timestamp(timestamp) partition by DAY");

            String query = "select sum(\"sum\"), sum(\"count\"), \"category\" from \"avg\" group by \"category\" order by 3";
            assertQueryNoLeakCheck(
                    "sum\tsum1\tcategory\n" +
                            "1.920104572218119\t0.9826178313717698\tA\n" +
                            "2.0117879412419453\t3.362073294894596\tB\n" +
                            "6.020496469863701\t5.702005218155505\tC\n",
                    query,
                    null,
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    "Sort light\n" +
                            "  keys: [category]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [sum,sum1,category]\n" +
                            "        GroupBy vectorized: true workers: 1\n" +
                            "          keys: [category]\n" +
                            "          values: [sum(sum),sum(count)]\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: avg\n"
            );
        });
    }

    private void assertError(String query, String errorMessage) throws Exception {
        try {
            assertQuery(
                    null,
                    query,
                    null,
                    true,
                    true
            );
            Assert.fail();
        } catch (SqlException sqle) {
            Assert.assertEquals(errorMessage, sqle.getMessage());
        }
    }
}
