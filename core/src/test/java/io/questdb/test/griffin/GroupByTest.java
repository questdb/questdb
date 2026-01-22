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
package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupByTest extends AbstractCairoTest {

    @Test
    public void test1GroupByWithoutAggregateFunctionsReturnsUniqueKeys() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query1 = "select l, s from t group by l,s";
            assertPlanNoLeakCheck(
                    query1,
                    """
                            Async Group By workers: 1
                              keys: [l,s]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck("l\ts\n1\ta\n", query1, null, true, true);

            String query2 = "select l as l1, s as s1 from t group by l,s";
            // virtual model must be used here to change aliases
            assertPlanNoLeakCheck(
                    query2,
                    """
                            VirtualRecord
                              functions: [l,s]
                                Async Group By workers: 1
                                  keys: [l,s]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
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
            assertError(query, "[109] window functions are not allowed in GROUP BY");
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns1() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select x+1, count(*) " +
                    "from t " +
                    "group by x+1 ";

            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [column,count]
                                Async Group By workers: 1
                                  keys: [column]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            column\tcount
                            2\t2
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select case when x < 0 then -1 when x = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by case when x < 0 then -1 when x = 0 then 0 else 1 end ";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [case]
                              values: [count(*)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            case\tcount
                            1\t2
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumns3() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");

            String query = "select case when x+1 < 0 then -1 when x+1 = 0 then 0 else 1 end, count(*) " +
                    "from t " +
                    "group by x+1";

            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [case([column<0,-1,column=0,0,1]),count]
                                Async Group By workers: 1
                                  keys: [column]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            case\tcount
                            1\t2
                            """,
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
                    """
                            VirtualRecord
                              functions: [x,avg,avg+min,x+10,avg1,avg1+10]
                                Async Group By workers: 1
                                  keys: [x]
                                  values: [avg(y),min(y),avg(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            x\tavg\tcolumn\tcolumn1\tavg1\tcolumn2
                            1\t11.5\t22.5\t11\t1.0\t11.0
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2GroupByWithNonAggregateExpressionsOnKeyColumnsAndBindVariable() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            VirtualRecord
                              functions: [x,avg,:bv::string]
                                Async Group By workers: 1
                                  keys: [x]
                                  values: [avg(y)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            x\tavg\t:bv
                            1\t11.5\tx
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2SuccessOnSelectWithExplicitGroupBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t group by x ";
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x*10,x+avg,min]
                                Async Group By workers: 1
                                  keys: [x]
                                  values: [avg(y),min(y)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            column\tcolumn1\tmin
                            10\t12.5\t11
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test2SuccessOnSelectWithoutExplicitGroupBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (x long, y long);");
            execute("insert into t values (1, 11), (1, 12);");
            String query = "select x*10, x+avg(y), min(y) from t";
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [column,x+avg,min]
                                Async Group By workers: 1
                                  keys: [column,x]
                                  values: [avg(y),min(y)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            column\tcolumn1\tmin
                            10\t12.5\t11
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void test3GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE weather ( \
                    timestamp TIMESTAMP, windDir INT, windSpeed INT, windGust INT,\s
                    cloudCeiling INT, skyCover SYMBOL, visMiles DOUBLE, tempF INT,\s
                    dewpF INT, rain1H DOUBLE, rain6H DOUBLE, rain24H DOUBLE, snowDepth INT) \
                    timestamp (timestamp)""");

            String query = "select  windSpeed, avg(windSpeed), avg + 10  " +
                    "from weather " +
                    "group by windSpeed " +
                    "order by windSpeed";

            assertError(query, "[35] Invalid column: avg");
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report]
                                Async Group By workers: 1
                                  keys: [date_report]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: dat
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            date_report\tcount
                            1970-01-01T00:00:00.000000Z\t3
                            1970-01-02T00:00:00.000000Z\t4
                            1970-01-03T00:00:00.000000Z\t3
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select ordr.date_report, count(*) " +
                    "from dat ordr " +
                    "group by date_report " + // no alias used here
                    "order by ordr.date_report";
            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report]
                                Async Group By workers: 1
                                  keys: [date_report]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: dat
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            date_report\tcount
                            1970-01-01T00:00:00.000000Z\t3
                            1970-01-02T00:00:00.000000Z\t4
                            1970-01-03T00:00:00.000000Z\t3
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel3() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, count(*) " +//date_report used with no alias
                    "from dat ordr " +
                    "group by ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report]
                                Async Group By workers: 1
                                  keys: [date_report]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: dat
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            date_report\tcount
                            1970-01-01T00:00:00.000000Z\t3
                            1970-01-02T00:00:00.000000Z\t4
                            1970-01-03T00:00:00.000000Z\t3
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel4() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, ordr.date_report,  count(*) " +
                    "from dat ordr " +
                    "group by date_report, ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report1]
                                VirtualRecord
                                  functions: [date_report,date_report,count]
                                    Async Group By workers: 1
                                      keys: [date_report]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: dat
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            date_report\tdate_report1\tcount
                            1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t3
                            1970-01-02T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t4
                            1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t3
                            """,
                    query,
                    "date_report1",
                    true,
                    true
            );
        });
    }

    @Test
    public void test4GroupByWithNonAggregateExpressionUsingAliasDefinedOnSameLevel5() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = "select date_report, dateadd('d', -1, ordr.date_report) as minusday, dateadd('d', 1, date_report) as plusday, " +
                    "concat('1', ordr.date_report, '3'), count(*) " +
                    "from dat ordr " +
                    "group by dateadd('d', -1, date_report), ordr.date_report " +
                    "order by ordr.date_report";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report]
                                VirtualRecord
                                  functions: [date_report,dateadd,dateadd('d',1,date_report),concat(['1',date_report,'3']),count]
                                    Async Group By workers: 1
                                      keys: [date_report,dateadd]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: dat
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            date_report\tminusday\tplusday\tconcat\tcount
                            1970-01-01T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t11970-01-01T00:00:00.000000Z3\t3
                            1970-01-02T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\t11970-01-02T00:00:00.000000Z3\t4
                            1970-01-03T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\t11970-01-03T00:00:00.000000Z3\t3
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test5GroupByWithNonAggregateExpressionUsingKeyColumn() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table dat as ( select cast(86400000000*(x%3) as timestamp) as date_report from long_sequence(10))");
            String query = """
                    select ordr.date_report, to_str(ordr.date_report, 'dd.MM.yyyy') as dt, \
                    dateadd('d', 1, date_report) as plusday, dateadd('d', -1, ordr.date_report) as minusday, count(*)
                    from dat ordr
                    group by ordr.date_report
                    order by ordr.date_report""";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [date_report]
                                VirtualRecord
                                  functions: [date_report,to_str(date_report),dateadd('d',1,date_report),dateadd('d',-1,date_report),count]
                                    Async Group By workers: 1
                                      keys: [date_report]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: dat
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            date_report\tdt\tplusday\tminusday\tcount
                            1970-01-01T00:00:00.000000Z\t01.01.1970\t1970-01-02T00:00:00.000000Z\t1969-12-31T00:00:00.000000Z\t3
                            1970-01-02T00:00:00.000000Z\t02.01.1970\t1970-01-03T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t4
                            1970-01-03T00:00:00.000000Z\t03.01.1970\t1970-01-04T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z\t3
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn1() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Radix sort light
                              keys: [date_report]
                                VirtualRecord
                                  functions: [date_report,to_str(date_report),dateadd('d',1,date_report),min,count,minminusday]
                                    GroupBy vectorized: false
                                      keys: [date_report]
                                      values: [min(x),count(*),min(dateadd('d',-1,date_report1))]
                                        SelectedRecord
                                            Hash Join Light
                                              condition: details.x=ordr.x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: ord
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: det
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            date_report\tdt\tplusday\tmin\tcount\tminminusday
                            1970-01-11T00:00:00.000000Z\t11.01.1970\t1970-01-12T00:00:00.000000Z\t3\t3\t1969-12-31T00:00:00.000000Z
                            1970-01-12T00:00:00.000000Z\t12.01.1970\t1970-01-13T00:00:00.000000Z\t1\t4\t1970-01-01T00:00:00.000000Z
                            1970-01-13T00:00:00.000000Z\t13.01.1970\t1970-01-14T00:00:00.000000Z\t2\t3\t1970-01-02T00:00:00.000000Z
                            """,
                    query,
                    "date_report",
                    true,
                    true
            );
        });
    }

    @Test
    public void test6GroupByWithNonAggregateExpressionUsingKeyColumn2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                """
                        k1\tk2\tcount
                        0\t0\t2
                        0\t2\t3
                        1\t1\t3
                        1\t3\t2
                        """,
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
                """
                        k1\tk2\tcount
                        1\t0\t2
                        1\t2\t3
                        2\t1\t3
                        2\t3\t2
                        """,
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
                """
                        time\ts1\tfirst\tfirst1\tfirst2
                        2023-05-16T00:00:00.000000Z\ta\tfoo\tnull\t0.08486964232560668
                        2023-05-16T00:02:00.000000Z\tb\tfoo\t0.8899286912289663\t0.6254021542412018
                        2023-05-16T00:05:00.000000Z\tc\tfoo\t0.1985581797355932\t0.33608255572515877
                        """,
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
                """
                        key\tcount
                        0\t50
                        1\t50
                        """,
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
                """
                        key\tcount
                        0\t50
                        1\t50
                        """,
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
                """
                        key\tcount
                        0\t50
                        1\t50
                        """,
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
                """
                        column\tcount
                        1\t50
                        2\t50
                        """,
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
                """
                        z\tcount
                        1\t50
                        2\t50
                        """,
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
                """
                        column\tcount
                        1\t50
                        2\t50
                        """,
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
                """
                        column\tcount
                        2\t50
                        1\t50
                        """,
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
                """
                        column\tkey\tkey1\tcount
                        1\t0\t0\t50
                        2\t1\t1\t50
                        """,
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
                """
                        k1\tk2\tcount
                        0\t0\t2
                        0\t2\t3
                        1\t1\t3
                        1\t3\t2
                        """,
                "select key1 as k1, key2 as k2, count(*) from t group by k2, k1, k2 order by 1, 2",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByExpressionAndLiteral() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l, s, l+1 from t group by l+1,s, l, l+2";
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [l,s,column]
                                Async Group By workers: 1
                                  keys: [l,s,column,column1]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            l\ts\tcolumn
                            1\ta\t2
                            """,
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
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                """
                        i\tcount
                        ('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\t2
                        \t1
                        """,
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
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                """
                        i\ts\tcount
                        ('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tfoobar\t2
                        \t\t1
                        """,
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
                """
                        k\tsum
                        BBBE\t0.7453598685393461
                        BBBI\t0.7394866029725212
                        BBBK\t1.370328208214273
                        """,
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
                """
                        ref0
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        """,
                "SELECT ts AS ref0 FROM x WHERE 1=1 GROUP BY ts ORDER BY (ts) NOT IN ('1970-01-01T00:00:00.000002Z');",
                "CREATE TABLE x AS (SELECT x::timestamp AS ts, x::short AS event, x::short AS origin FROM long_sequence(2)) TIMESTAMP(ts);",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupBySingleVarcharKeyFromSampleByWithFill() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t (vch varchar, l long, ts timestamp) timestamp(ts) partition by day;");
            execute("""
                    insert into t values\s
                    ('USD', 1, '2021-11-17T17:00:00.000000Z'),
                    ('USD', 2, '2021-11-17T17:35:02.000000Z'),
                    ('EUR', 3, '2021-11-17T17:45:02.000000Z'),
                    ('USD', 1, '2021-11-17T19:04:00.000000Z'),
                    ('USD', 2, '2021-11-17T19:35:02.000000Z'),
                    ('USD', 3, '2021-11-17T19:45:02.000000Z');""");

            String query = """
                    with samp as (
                      select ts, vch, min(l), max(l)
                      from t
                      sample by 1h fill(prev)
                    )
                    select vch, sum(min)
                    from samp;""";

            assertQueryNoLeakCheck(
                    """
                            vch\tsum
                            EUR\t9
                            USD\t3
                            """,
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

            String query = """
                    with\s
                      w1 as (
                        select * from tab1
                        order by vch asc
                      ),\s
                      w2 as (
                        select * from tab1
                        order by vch desc
                      ),
                      u as (select * from w1
                        UNION all w2
                      ),
                      uo as (
                        select * from u order by vch
                      ),
                      grouped as (
                        select vch, count(vch)
                        from uo
                        order by vch
                      ),
                      nested as (
                        select vch, sum(count) from grouped
                        group by vch
                        order by vch
                      )
                    select tab1.vch, tab1.x, nested.vch as nested_vch, sum
                    from tab1
                    join nested on nested.vch = tab1.vch;""";
            assertQueryNoLeakCheck(
                    """
                            vch\tx\tnested_vch\tsum
                            1\t1\t1\t8
                            2\t2\t2\t8
                            3\t3\t3\t8
                            4\t4\t4\t8
                            0\t5\t0\t8
                            1\t6\t1\t8
                            2\t7\t2\t8
                            3\t8\t3\t8
                            4\t9\t4\t8
                            0\t10\t0\t8
                            1\t11\t1\t8
                            2\t12\t2\t8
                            3\t13\t3\t8
                            4\t14\t4\t8
                            0\t15\t0\t8
                            1\t16\t1\t8
                            2\t17\t2\t8
                            3\t18\t3\t8
                            4\t19\t4\t8
                            0\t20\t0\t8
                            """,
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
                """
                        key\tmax
                        0\t100
                        1\t91
                        2\t92
                        3\t93
                        4\t94
                        5\t95
                        6\t96
                        7\t97
                        8\t98
                        9\t99
                        """,
                "select key, max(value) from t group by key order by key",
                "create table t as ( select (x%10)::varchar key, x as value from long_sequence(100)); ",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByWithAliasClash1() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Sort light
                              keys: [s, max]
                                Async Group By workers: 1
                                  keys: [s,max]
                                  values: [max(l)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            s\tmax\tmax1
                            a\t-2\t1
                            a\t-1\t1
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t1 as (select x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), t2.x " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x " +
                    "order by t1.x, t2.x";

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tx1
                            1\t1\t1
                            2\t0\t2
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash3() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            SelectedRecord
                                Sort light
                                  keys: [x, x1]
                                    VirtualRecord
                                      functions: [x,max,case([1<x,100*x,10*x1]),x1]
                                        GroupBy vectorized: false
                                          keys: [x,x1]
                                          values: [max(y)]
                                            SelectedRecord
                                                Hash Join Light
                                                  condition: t2.y=t1.y
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t1
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: t2
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tcase
                            1\t1\t10
                            2\t0\t200
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash4() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table t1 as (select x, x%2 as y from long_sequence(2))");
            execute("create table t2 as (select x, x%2 as y from long_sequence(2))");

            String query = "select t1.x, max(t2.y), case when t1.x > 1 then 30*t1.x else 20*t2.x end " +
                    "from t1 " +
                    "join t2 on t1.y = t2.y  " +
                    "group by t1.x, t2.x, case when t1.x > 1 then 30*t1.x else 20*t2.x end";

            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x,max,case]
                                GroupBy vectorized: false
                                  keys: [x,case,x1]
                                  values: [max(y)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: t2.y=t1.y
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t1
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t2
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tcase
                            1\t1\t20
                            2\t0\t60
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash5() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Radix sort light
                              keys: [x]
                                VirtualRecord
                                  functions: [x,max,dateadd::long+x1]
                                    GroupBy vectorized: false
                                      keys: [x,x1,dateadd]
                                      values: [max(y)]
                                        SelectedRecord
                                            Hash Join Light
                                              condition: t2.y=t1.y
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t2
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tcolumn
                            1\t1\t1677715200000001
                            2\t0\t1677801600000002
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithAliasClash6() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Sort light
                              keys: [x, max, dateadd]
                                VirtualRecord
                                  functions: [x,max,dateadd('s',max::int,dateadd)]
                                    GroupBy vectorized: false
                                      keys: [x,dateadd,x1]
                                      values: [max(y)]
                                        SelectedRecord
                                            Hash Join Light
                                              condition: t2.y=t1.y
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t2
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tdateadd
                            1\t1\t2023-03-02T00:00:01.000000Z
                            2\t0\t2023-03-03T00:00:00.000000Z
                            """,
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
                """
                        k1\tkey2\tkey21\tcount
                        0\t0\t0\t2
                        0\t2\t2\t3
                        1\t1\t1\t3
                        1\t3\t3\t2
                        """,
                "select key1 as k1, key2, key2, count(*) from t group by key2, k1 order by 1, 2",
                "create table t as ( select x%2 key1, x%4 key2, x as value from long_sequence(10));",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupByWithLeftJoin() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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

            final String expectedResult = """
                    dim_ap_temperature__category\tfact_table__date_time_day\tfact_table__avg_radiation\tfact_table__energy_power
                    c\t1970-01-01T00:00:00.000000Z\t0.5421442091464996\t0.6145070195198059
                    b\t1970-01-01T00:00:00.000000Z\t0.525111973285675\t0.6171746551990509
                    a\t1970-01-01T00:00:00.000000Z\t0.33940355479717255\t0.47865718603134155
                    """;

            String[] joinTypes = new String[]{"Left", "Right", "Full"};
            // With GROUP BY clause
            // This query is generated by Cube.js
            for (String joinType : joinTypes) {
                final String query1 = "SELECT\n" +
                        "  \"dim_ap_temperature\".category \"dim_ap_temperature__category\",\n" +
                        "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC')) \"fact_table__date_time_day\",\n" +
                        "  avg(\"fact_table\".radiation) \"fact_table__avg_radiation\",\n" +
                        "  avg(\"fact_table\".energy_power) \"fact_table__energy_power\"\n" +
                        "FROM\n" +
                        "  fact_table AS \"fact_table\"\n" +
                        "  " + joinType + " JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                        "GROUP BY\n" +
                        "  \"dim_ap_temperature__category\",\n" +
                        "  \"fact_table__date_time_day\"\n" +
                        "ORDER BY\n" +
                        "  \"fact_table__avg_radiation\" DESC\n" +
                        "LIMIT\n" +
                        "  10000;";
                assertQueryNoLeakCheck(expectedResult, query1);
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
                                "                Hash " + joinType + " Outer Join Light\n" +
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
                        "  " + joinType + " JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                        "GROUP BY\n" +
                        "  \"dim_ap_temperature\".category,\n" +
                        "  timestamp_floor('d', to_timezone(\"fact_table\".date_time, 'UTC'))\n" +
                        "ORDER BY\n" +
                        "  \"fact_table__avg_radiation\" DESC\n" +
                        "LIMIT\n" +
                        "  10000;";
                assertQueryNoLeakCheck(expectedResult, query2);
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
                                "                Hash " + joinType + " Outer Join Light\n" +
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
                        "  " + joinType + " JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                        "GROUP BY\n" +
                        "  category,\n" +
                        "  timestamp_floor('d', to_timezone(date_time, 'UTC'))\n" +
                        "ORDER BY\n" +
                        "  \"fact_table__avg_radiation\" DESC\n" +
                        "LIMIT\n" +
                        "  10000;";
                assertQueryNoLeakCheck(expectedResult, query3);
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
                                "                Hash " + joinType + " Outer Join Light\n" +
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
                        "  " + joinType + " JOIN dim_apTemperature AS \"dim_ap_temperature\" ON \"fact_table\".id_aparent_temperature = \"dim_ap_temperature\".id\n" +
                        "ORDER BY\n" +
                        "  \"fact_table__avg_radiation\" DESC\n" +
                        "LIMIT\n" +
                        "  10000;";
                assertQueryNoLeakCheck(expectedResult, query4);
                assertPlanNoLeakCheck(
                        query4,
                        "Sort light lo: 10000\n" +
                                "  keys: [fact_table__avg_radiation desc]\n" +
                                "    GroupBy vectorized: false\n" +
                                "      keys: [dim_ap_temperature__category,fact_table__date_time_day]\n" +
                                "      values: [avg(radiation),avg(energy_power)]\n" +
                                "        SelectedRecord\n" +
                                "            Hash " + joinType + " Outer Join Light\n" +
                                "              condition: dim_ap_temperature.id=fact_table.id_aparent_temperature\n" +
                                "                PageFrame\n" +
                                "                    Row forward scan\n" +
                                "                    Frame forward scan on: fact_table\n" +
                                "                Hash\n" +
                                "                    PageFrame\n" +
                                "                        Row forward scan\n" +
                                "                        Frame forward scan on: dim_apTemperature\n"
                );
            }
        });
    }

    @Test
    public void testGroupByWithNonConstantSelectClauseExpression() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "    select 1 as l, 'a' as s " +
                    "    union all " +
                    "    select 1, 'a' )");

            String query = "select l,s,rnd_int(0,1,0)/10 from t group by l,s";
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [l,s,memoize(rnd_int(0,1,0)/10)]
                                Async Group By workers: 1
                                  keys: [l,s]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            l\ts\tcolumn
                            1\ta\t0
                            """,
                    query,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByWithTimestampKey() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE foo (
                      timestamp TIMESTAMP,
                      bar INT
                    ) TIMESTAMP (timestamp)
                    PARTITION BY DAY;""");
            execute("INSERT INTO foo VALUES ('2020', 0);");
            String query = """
                    SELECT
                      timestamp AS time,
                      TO_STR(timestamp, 'yyyy-MM-dd'),
                      SUM(1)\s
                    FROM foo;""";
            assertQueryNoLeakCheck(
                    """
                            time\tTO_STR\tSUM
                            2020-01-01T00:00:00.000000Z\t2020-01-01\t1
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy1() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            GroupBy vectorized: false
                              keys: [s2]
                              values: [sum(l)]
                                LatestByDeferredListValuesFiltered
                                  filter: s2 in [c]
                                    Frame backward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            s2\tsum
                            c\t25
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Sort light
                              keys: [s2]
                                GroupBy vectorized: false
                                  keys: [s2]
                                  values: [sum(l)]
                                    LatestByDeferredListValuesFiltered
                                      filter: s2 in [c,d]
                                        Frame backward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            s2\tsum
                            c\t14
                            d\t12
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByImplicitGroupBy3() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            GroupBy vectorized: false
                              keys: [concat]
                              values: [sum(l)]
                                LatestByDeferredListValuesFiltered
                                  filter: s2 in [d]
                                    Frame backward scan on: t
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            concat\tsum
                            _d_\t25
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect1() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    """
                            select a, b, c as z, count(*) as views
                            from x
                            where a = 1
                            group by a,b,z
                            """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async JIT Group By workers: 1
                              keys: [a,b,z]
                              values: [count(*)]
                              filter: a=1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tb\tz\tviews
                            1\t2\t3\t1
                            """,
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
            assertQueryNoLeakCheck(
                    """
                            ts\ti\tavg\tsum\tfirst_value
                            1970-01-01T00:00:01.099967Z\tnull\t495.40261282660333\t1668516.0\t481.0
                            1970-01-01T00:00:01.099995Z\t1\t495.08707124010556\t1688742.0\tnull
                            1970-01-01T00:00:01.099973Z\t2\t506.5011448196909\t1769715.0\t697.0
                            1970-01-01T00:00:01.099908Z\t3\t505.95267958950967\t1774882.0\t16.0
                            1970-01-01T00:00:01.099977Z\t4\t501.16155593412833\t1765091.0\t994.0
                            1970-01-01T00:00:01.099994Z\t5\t494.87667161961366\t1665260.0\t701.0
                            1970-01-01T00:00:01.099991Z\t6\t500.67453098351336\t1761373.0\t830.0
                            1970-01-01T00:00:01.099998Z\t7\t497.7231450719823\t1797776.0\t293.0
                            1970-01-01T00:00:01.099997Z\t8\t498.6340425531915\t1757685.0\t868.0
                            1970-01-01T00:00:01.099992Z\t9\t499.1758750361585\t1725651.0\t528.0
                            1970-01-01T00:00:01.099989Z\t10\t500.3242937853107\t1771148.0\t936.0
                            1970-01-01T00:00:01.099976Z\t11\t501.4019192774485\t1776467.0\t720.0
                            1970-01-01T00:00:01.099984Z\t12\t489.8953058321479\t1721982.0\t949.0
                            1970-01-01T00:00:01.099952Z\t13\t500.65723270440253\t1751299.0\t518.0
                            1970-01-01T00:00:01.099996Z\t14\t506.8769141866513\t1754301.0\tnull
                            1970-01-01T00:00:01.100000Z\t15\t497.0794058840331\t1740275.0\t824.0
                            1970-01-01T00:00:01.099979Z\t16\t499.3338209479228\t1706723.0\t38.0
                            1970-01-01T00:00:01.099951Z\t17\t492.7804469273743\t1764154.0\t698.0
                            1970-01-01T00:00:01.099999Z\t18\t501.4806333050608\t1773737.0\t204.0
                            1970-01-01T00:00:01.099957Z\t19\t501.01901034386356\t1792145.0\t712.0
                            1970-01-01T00:00:01.099987Z\t20\t498.1350566366541\t1715079.0\t188.0
                            """,
                    query1
            );

            assertPlanNoLeakCheck(
                    query1,
                    """
                            Radix sort light
                              keys: [i]
                                VirtualRecord
                                  functions: [ts,i,avg,sum,first_value]
                                    GroupBy vectorized: false
                                      keys: [i]
                                      values: [max(ts),avg(j),sum(j::double),first(j::double)]
                                        Sort
                                          keys: [i, ts]
                                            SelectedRecord
                                                Filter filter: data.ts>=cnt.max-80000
                                                    Hash Join Light
                                                      condition: data.i=cnt.i
                                                        Async Group By workers: 1
                                                          keys: [i]
                                                          values: [max(ts)]
                                                          filter: null
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tab
                                                        Hash
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tab
                            """
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
                    """
                            ts\ti\tavg\tsum\tfirst_value
                            1970-01-01T00:00:01.099967Z\tnull\t495.40261282660333\t1668516.0\t481
                            1970-01-01T00:00:01.099995Z\t1\t495.08707124010556\t1688742.0\tnull
                            1970-01-01T00:00:01.099973Z\t2\t506.5011448196909\t1769715.0\t697
                            1970-01-01T00:00:01.099908Z\t3\t505.95267958950967\t1774882.0\t16
                            1970-01-01T00:00:01.099977Z\t4\t501.16155593412833\t1765091.0\t994
                            1970-01-01T00:00:01.099994Z\t5\t494.87667161961366\t1665260.0\t701
                            1970-01-01T00:00:01.099991Z\t6\t500.67453098351336\t1761373.0\t830
                            1970-01-01T00:00:01.099998Z\t7\t497.7231450719823\t1797776.0\t293
                            1970-01-01T00:00:01.099997Z\t8\t498.6340425531915\t1757685.0\t868
                            1970-01-01T00:00:01.099992Z\t9\t499.1758750361585\t1725651.0\t528
                            1970-01-01T00:00:01.099989Z\t10\t500.3242937853107\t1771148.0\t936
                            1970-01-01T00:00:01.099976Z\t11\t501.4019192774485\t1776467.0\t720
                            1970-01-01T00:00:01.099984Z\t12\t489.8953058321479\t1721982.0\t949
                            1970-01-01T00:00:01.099952Z\t13\t500.65723270440253\t1751299.0\t518
                            1970-01-01T00:00:01.099996Z\t14\t506.8769141866513\t1754301.0\tnull
                            1970-01-01T00:00:01.100000Z\t15\t497.0794058840331\t1740275.0\t824
                            1970-01-01T00:00:01.099979Z\t16\t499.3338209479228\t1706723.0\t38
                            1970-01-01T00:00:01.099951Z\t17\t492.7804469273743\t1764154.0\t698
                            1970-01-01T00:00:01.099999Z\t18\t501.4806333050608\t1773737.0\t204
                            1970-01-01T00:00:01.099957Z\t19\t501.01901034386356\t1792145.0\t712
                            1970-01-01T00:00:01.099987Z\t20\t498.1350566366541\t1715079.0\t188
                            """,
                    query2,
                    null,
                    true,
                    true,
                    false
            );

            assertPlanNoLeakCheck(
                    query2,
                    """
                            Radix sort light
                              keys: [i]
                                GroupBy vectorized: false
                                  keys: [i]
                                  values: [last(ts),last(avg),last(sum),last(first_value)]
                                    Limit value: -100 skip-rows: 999900 take-rows: 100
                                        Window
                                          functions: [avg(j) over (partition by [i] range between 80000 preceding and current row),sum(j) over (partition by [i] range between 80000 preceding and current row),first_value(j) over (partition by [i] range between 80000 preceding and current row)]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                            """
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
                    """
                            select a, b as B, c as z, count(*) as views
                            from x
                            where a = 1
                            group by a,b,z
                            """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async JIT Group By workers: 1
                              keys: [a,b,z]
                              values: [count(*)]
                              filter: a=1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tb\tz\tviews
                            1\t2\t3\t1
                            """,
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
                    """
                            select a, b as B, c as z, count(*) as views
                            from x
                            where a = 1
                            group by a,B,z
                            """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async JIT Group By workers: 1
                              keys: [a,B,z]
                              values: [count(*)]
                              filter: a=1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tB\tz\tviews
                            1\t2\t3\t1
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect2() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    """
                            select a, b, c as z, count(*) as views
                            from x
                            where a = 1
                            group by a,b,c
                            """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [a,b,c,views]
                                Async JIT Group By workers: 1
                                  keys: [a,b,c]
                                  values: [count(*)]
                                  filter: a=1
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tb\tz\tviews
                            1\t2\t3\t1
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLiftAliasesFromInnerSelect3() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table x ( a int, b int, c symbol, ts timestamp ) timestamp(ts) partition by DAY WAL;");
            execute("insert into x values (1,2,'3', now()), (2,3, '3', now()), (5,6,'4', now())");
            drainWalQueue();
            String query =
                    """
                            select a, b, c, count(*) as views
                            from x
                            where a = 1
                            group by a,b,c
                            """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async JIT Group By workers: 1
                              keys: [a,b,c]
                              values: [count(*)]
                              filter: a=1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tb\tc\tviews
                            1\t2\t3\t1
                            """,
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
            String expected = """
                    y_utc_15m\ty_sf_position_mw
                    1970-01-01T00:00:00.000000Z\t-0.2246301342497259
                    1970-01-01T00:30:00.000000Z\t-0.6508594025855301
                    1970-01-01T00:45:00.000000Z\t-0.9856290845874263
                    1970-01-01T01:00:00.000000Z\t-0.5093827001617407
                    1970-01-01T01:30:00.000000Z\t0.5599161804800813
                    1970-01-01T01:45:00.000000Z\t0.2390529010846525
                    1970-01-01T02:00:00.000000Z\t-0.6778564558839208
                    1970-01-01T02:15:00.000000Z\t0.38539947865244994
                    1970-01-01T02:30:00.000000Z\t-0.33608255572515877
                    1970-01-01T02:45:00.000000Z\t0.7675673070796104
                    1970-01-01T03:00:00.000000Z\t0.6217326707853098
                    1970-01-01T03:15:00.000000Z\t0.6381607531178513
                    1970-01-01T03:45:00.000000Z\t0.12026122412833129
                    1970-01-01T04:00:00.000000Z\t-0.8912587536603974
                    1970-01-01T04:15:00.000000Z\t-0.42281342727402726
                    1970-01-01T04:30:00.000000Z\t-0.7664256753596138
                    1970-01-01T05:15:00.000000Z\t-0.8847591603509142
                    1970-01-01T05:30:00.000000Z\t0.931192737286751
                    1970-01-01T05:45:00.000000Z\t0.8001121139739173
                    1970-01-01T06:00:00.000000Z\t0.92050039469858
                    1970-01-01T06:15:00.000000Z\t0.456344569609078
                    1970-01-01T06:30:00.000000Z\t0.40455469747939254
                    1970-01-01T06:45:00.000000Z\t0.5659429139861241
                    1970-01-01T07:00:00.000000Z\t-0.6821660861001273
                    1970-01-01T07:30:00.000000Z\t-0.11585982949541473
                    1970-01-01T07:45:00.000000Z\t0.8164182592467494
                    1970-01-01T08:00:00.000000Z\t0.5449155021518948
                    1970-01-01T08:30:00.000000Z\t0.49428905119584543
                    1970-01-01T08:45:00.000000Z\t-0.6551335839796312
                    1970-01-01T09:15:00.000000Z\t0.9540069089049732
                    1970-01-01T09:30:00.000000Z\t-0.03167026265669903
                    1970-01-01T09:45:00.000000Z\t-0.19751370382305056
                    1970-01-01T10:00:00.000000Z\t0.6806873134626418
                    1970-01-01T10:15:00.000000Z\t-0.24008362859107102
                    1970-01-01T10:30:00.000000Z\t-0.9455893004802433
                    1970-01-01T10:45:00.000000Z\t-0.6247427794126656
                    1970-01-01T11:00:00.000000Z\t-0.3901731258748704
                    1970-01-01T11:15:00.000000Z\t-0.10643046345788132
                    1970-01-01T11:30:00.000000Z\t0.07246172621937097
                    1970-01-01T11:45:00.000000Z\t-0.3679848625908545
                    1970-01-01T12:00:00.000000Z\t0.6697969295620055
                    1970-01-01T12:15:00.000000Z\t-0.26369335635512836
                    1970-01-01T12:45:00.000000Z\t-0.19846258365662472
                    1970-01-01T13:00:00.000000Z\t-0.8595900073631431
                    1970-01-01T13:15:00.000000Z\t0.7458169804091256
                    1970-01-01T13:30:00.000000Z\t0.4274704286353759
                    1970-01-01T14:00:00.000000Z\t-0.8291193369353376
                    1970-01-01T14:30:00.000000Z\t0.2711532808184136
                    1970-01-01T15:00:00.000000Z\t-0.8189713915910615
                    1970-01-01T15:15:00.000000Z\t0.7365115215570027
                    1970-01-01T15:30:00.000000Z\t-0.9418719455092096
                    1970-01-01T16:00:00.000000Z\t-0.05024615679069011
                    1970-01-01T16:15:00.000000Z\t-0.8952510116133903
                    1970-01-01T16:30:00.000000Z\t-0.029227696942726644
                    1970-01-01T16:45:00.000000Z\t-0.7668146556860689
                    1970-01-01T17:00:00.000000Z\t-0.05158459929273784
                    1970-01-01T17:15:00.000000Z\t-0.06846631555382798
                    1970-01-01T17:30:00.000000Z\t-0.5708643723875381
                    1970-01-01T17:45:00.000000Z\t0.7260468106076399
                    1970-01-01T18:15:00.000000Z\t-0.1010501916946902
                    1970-01-01T18:30:00.000000Z\t-0.05094182589333662
                    1970-01-01T18:45:00.000000Z\t-0.38402128906440336
                    1970-01-01T19:15:00.000000Z\t0.7694744648762927
                    1970-01-01T19:45:00.000000Z\t0.6901976778065181
                    1970-01-01T20:00:00.000000Z\t-0.5913874468544745
                    1970-01-01T20:30:00.000000Z\t-0.14261321308606745
                    1970-01-01T20:45:00.000000Z\t0.4440250924606578
                    1970-01-01T21:00:00.000000Z\t-0.09618589590900506
                    1970-01-01T21:15:00.000000Z\t-0.08675950660182763
                    1970-01-01T21:30:00.000000Z\t-0.741970173888595
                    1970-01-01T21:45:00.000000Z\t0.4167781163798937
                    1970-01-01T22:00:00.000000Z\t-0.05514933756198426
                    1970-01-01T22:30:00.000000Z\t-0.2093569947644236
                    1970-01-01T22:45:00.000000Z\t-0.8439276969435359
                    1970-01-01T23:00:00.000000Z\t-0.03973283003449557
                    1970-01-01T23:15:00.000000Z\t-0.8551850405049611
                    1970-01-01T23:45:00.000000Z\t0.6226001464598434
                    1970-01-02T00:00:00.000000Z\t-0.7195457109208119
                    1970-01-02T00:15:00.000000Z\t-0.23493793601747937
                    1970-01-02T00:30:00.000000Z\t-0.6334964081687151
                    """;
            String query = """
                    SELECT
                        delivery_start_utc as y_utc_15m,
                        sum(case
                                when seller='sf' then -1.0*volume_mw
                                when buyer='sf' then 1.0*volume_mw
                                else 0.0
                            end)
                        as y_sf_position_mw
                    FROM (
                        SELECT delivery_start_utc, seller, buyer, volume_mw FROM trades
                        WHERE
                            (seller = 'sf' OR buyer = 'sf')
                        )
                    group by y_utc_15m \
                    order by y_utc_15m""";

            assertQueryNoLeakCheck(
                    expected,
                    query,
                    "y_utc_15m",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [y_utc_15m]
                                GroupBy vectorized: false
                                  keys: [y_utc_15m]
                                  values: [sum(case([seller='sf',-1.0*volume_mw,buyer='sf',1.0*volume_mw,0.0]))]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: (seller='sf' or buyer='sf')
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                            """
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
            String query = """
                    select a, sum(b) sum, c as z, count(*) views
                    from x
                    where a = 1
                    group by a,b,z
                    order by a,b,z
                    """;
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Sort light
                                  keys: [a, b, z]
                                    VirtualRecord
                                      functions: [a,sum,z,views,b]
                                        Async JIT Group By workers: 1
                                          keys: [a,z,b]
                                          values: [sum(b),count(*)]
                                          filter: a=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            a\tsum\tz\tviews
                            1\t2\t3\t1
                            1\t3\t1\t1
                            1\t5\t4\t1
                            """,
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
            execute("""
                    CREATE TABLE hits
                    (
                        URL string,
                        Referer string,
                        TraficSourceID int,
                        SearchEngineID short,
                        AdvEngineID short,
                        EventTime timestamp,
                        CounterID int,
                        IsRefresh short
                    ) TIMESTAMP(EventTime) PARTITION BY DAY;""");
            String query1 =
                    """
                            SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews
                            FROM hits
                            WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0
                            GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst
                            ORDER BY PageViews DESC
                            LIMIT 1000, 1010;""";
            assertPlanNoLeakCheck(
                    query1,
                    """
                            Sort light lo: 1000 hi: 1010
                              keys: [PageViews desc]
                                VirtualRecord
                                  functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews]
                                    Async JIT Group By workers: 1
                                      keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst]
                                      values: [count(*)]
                                      filter: (CounterID=62 and IsRefresh=0)
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: hits
                                              intervals: [("2013-07-01T00:00:00.000000Z","2013-07-31T23:59:59.000000Z")]
                            """
            );
            String query2 =
                    """
                            SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL, COUNT(*) AS PageViews
                            FROM hits
                            WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0
                            GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, URL
                            ORDER BY PageViews DESC
                            LIMIT 1000, 1010;""";
            assertPlanNoLeakCheck(
                    query2,
                    """
                            Sort light lo: 1000 hi: 1010
                              keys: [PageViews desc]
                                VirtualRecord
                                  functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,PageViews]
                                    Async JIT Group By workers: 1
                                      keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL]
                                      values: [count(*)]
                                      filter: (CounterID=62 and IsRefresh=0)
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: hits
                                              intervals: [("2013-07-01T00:00:00.000000Z","2013-07-31T23:59:59.000000Z")]
                            """
            );
            String query3 = """
                    SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL, COUNT(*) AS PageViews, concat(lpad(cast(TraficSourceId as string), 10, '0'), lpad(cast(Referer as string), 32, '0')) as cat
                    FROM hits
                    WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0
                    GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, URL, cat
                    ORDER BY PageViews DESC
                    LIMIT 1000, 1010;""";
            assertPlanNoLeakCheck(
                    query3,
                    """
                            Sort light lo: 1000 hi: 1010
                              keys: [PageViews desc]
                                VirtualRecord
                                  functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,PageViews,cat]
                                    Async JIT Group By workers: 1
                                      keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,URL,cat]
                                      values: [count(*)]
                                      filter: (CounterID=62 and IsRefresh=0)
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: hits
                                              intervals: [("2013-07-01T00:00:00.000000Z","2013-07-31T23:59:59.000000Z")]
                            """
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
                    """
                            k1\tkey2\tkey21\tcount
                            0\t0\t0\t2
                            0\t2\t2\t3
                            1\t1\t1\t3
                            1\t3\t3\t2
                            """,
                    query,
                    null,
                    true,
                    true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light
                              keys: [k1, key2]
                                VirtualRecord
                                  functions: [k1,key2,key2,count]
                                    Async Group By workers: 1
                                      keys: [k1,key2]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
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
                    """
                            column\tkey\tkey1\tcount
                            1\t0\t0\t50
                            2\t1\t1\t50
                            """,
                    query,
                    null,
                    true,
                    true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light
                              keys: [column, key, key1 desc]
                                VirtualRecord
                                  functions: [key+1,key,key,count]
                                    Async Group By workers: 1
                                      keys: [key]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
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
                    """
                            Sort light
                              keys: [x, max, dateadd]
                                VirtualRecord
                                  functions: [x,max,dateadd('s',max::int,dateadd)]
                                    GroupBy vectorized: false
                                      keys: [x,dateadd,x1]
                                      values: [max(y)]
                                        SelectedRecord
                                            Hash Join Light
                                              condition: t2.y=t1.y
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t2
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tmax\tdateadd
                            1\t1\t2023-03-02T00:00:01.000000Z
                            2\t0\t2023-03-03T00:00:00.000000Z
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLimitedOrderByLongConstant() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

        assertMemoryLeak(() -> {
            execute("create table x (sym symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values ('1','2023-01-01T00:00:00'),('1','2023-01-01T00:00:01'),('2','2023-01-01T00:00:03')");
            assertQueryNoLeakCheck(
                    """
                            sym\tlatest
                            1\t0
                            2\t0
                            """,
                    "SELECT sym, first((0::timestamp)::long) latest " +
                            "FROM x " +
                            "WHERE ts IN '2023-01-01' " +
                            "ORDER BY latest DESC " +
                            "LIMIT 10;"
            );
        });
    }

    @Test
    public void testManyAggregatesFallbackUpdater() throws Exception {
        // This test verifies that GROUP BY queries with many aggregate functions
        // don't fail with "Bytecode is too long" error. See issue #3326.
        assertMemoryLeak(() -> {
            // change to 6k to reproduce the OG issue
            final int functionCount = 1000;
            execute("create table t (value double)");
            execute("insert into t values (1.5)");

            StringBuilder query = new StringBuilder("select ");
            StringBuilder expectedHeader = new StringBuilder();
            StringBuilder expectedValues = new StringBuilder();
            for (int i = 0; i < functionCount; i++) {
                query.append("avg(value + ").append(i).append(") avg").append(i);
                expectedHeader.append("avg").append(i);
                expectedValues.append(1.5 + i);
                if (i != functionCount - 1) {
                    query.append(", ");
                    expectedHeader.append("\t");
                    expectedValues.append("\t");
                }
            }
            query.append(" from t;");
            expectedHeader.append("\n");
            expectedValues.append("\n");

            // assertQueryNoLeakCheck's cursor memory verification is too strict, so we're using assertSql;
            // that's because non-keyed group by cursors only close their map value when the factory is closed
            assertSql(
                    expectedHeader + expectedValues.toString(),
                    query.toString()
            );
        });
    }

    @Test
    public void testManyAggregatesFallbackUpdaterNoAliases() throws Exception {
        // Same as testManyAggregatesFallbackUpdater, but without aliases.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final boolean aliasExprEnabled = rnd.nextBoolean();
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, Boolean.toString(aliasExprEnabled));
        assertMemoryLeak(() -> {
            final int functionCount = 1000;
            execute("create table t (value double)");
            execute("insert into t values (1.5)");

            StringBuilder query = new StringBuilder("select ");
            StringBuilder expectedHeader = new StringBuilder();
            StringBuilder expectedValues = new StringBuilder();
            for (int i = 0; i < functionCount; i++) {
                query.append("avg(value + ").append(i).append(")");
                if (aliasExprEnabled) {
                    expectedHeader.append("avg(value + ").append(i).append(")");
                } else {
                    expectedHeader.append("avg");
                    if (i > 0) {
                        expectedHeader.append(i);
                    }
                }
                expectedValues.append(1.5 + i);
                if (i != functionCount - 1) {
                    query.append(", ");
                    expectedHeader.append("\t");
                    expectedValues.append("\t");
                }
            }
            query.append(" from t;");
            expectedHeader.append("\n");
            expectedValues.append("\n");

            // assertQueryNoLeakCheck's cursor memory verification is too strict, so we're using assertSql;
            // that's because non-keyed group by cursors only close their map value when the factory is closed
            assertSql(
                    expectedHeader + expectedValues.toString(),
                    query.toString()
            );
        });
    }

    @Test
    public void testNestedGroupByWithExplicitGroupByClause() throws Exception {
        String expected = """
                url\tu_count\tcnt\tavg_m_sum
                RXPEHNRXGZ\t4\t4\t414.25
                DXYSBEOUOJ\t1\t1\t225.0
                SXUXIBBTGP\t2\t2\t379.5
                GWFFYUDEYY\t5\t5\t727.2
                LOFJGETJRS\t2\t2\t524.5
                ZSRYRFBVTM\t2\t2\t337.0
                VTJWCPSWHY\t1\t1\t660.0
                HGOOZZVDZJ\t1\t1\t540.0
                SHRUEDRQQU\t2\t2\t468.0
                """;
        assertQuery(
                expected,
                """
                        WITH x_sample AS (
                          SELECT id, uuid, url, sum(metric) m_sum
                          FROM x
                          WHERE ts >= '1023-03-31T00:00:00' and ts <= '2023-04-02T23:59:59'
                          GROUP BY id, uuid, url
                        )
                        SELECT url, count_distinct(uuid) u_count, count() cnt, avg(m_sum) avg_m_sum
                        FROM x_sample
                        GROUP BY url""",
                """
                        create table x as (
                        select timestamp_sequence(100000000, 100000000) ts,
                          rnd_int(0, 10, 0) id,
                          rnd_uuid4() uuid,
                          rnd_str(10, 10, 10, 0) url,
                          rnd_long(0, 1000, 0) metric
                        from long_sequence(20)) timestamp(ts)""",
                null,
                true,
                true
        );
        assertQueryNoLeakCheck(
                expected,
                """
                        WITH x_sample AS (
                          SELECT id, uuid, url, sum(metric) m_sum
                          FROM x
                          WHERE ts >= '1023-03-31T00:00:00' and ts <= '2023-04-02T23:59:59'
                          GROUP BY id, uuid, url
                        )
                        SELECT url, count(distinct uuid) u_count, count() cnt, avg(m_sum) avg_m_sum
                        FROM x_sample
                        GROUP BY url"""
        );
    }

    @Test
    public void testOrderByOnAliasedColumnAfterGroupBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Radix sort light
                              keys: [ref0]
                                VirtualRecord
                                  functions: [created]
                                    Async JIT Group By workers: 1
                                      keys: [created]
                                      filter: null!=created
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ref0
                            1970-01-01T00:00:00.000001Z
                            1970-01-01T00:00:00.000002Z
                            1970-01-01T00:00:00.000003Z
                            """,
                    query,
                    "ref0",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectDistinctOnExpressionWithOrderBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Radix sort light
                              keys: [ref0]
                                VirtualRecord
                                  functions: [dateadd('h',1,created)]
                                    Async JIT Group By workers: 1
                                      keys: [created]
                                      filter: null!=created
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ref0
                            1970-01-01T01:00:00.000001Z
                            1970-01-01T01:00:00.000002Z
                            1970-01-01T01:00:00.000003Z
                            """,
                    query,
                    "ref0",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectDistinctOnUnaliasedColumnWithOrderBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Radix sort light
                              keys: [created]
                                Async JIT Group By workers: 1
                                  keys: [created]
                                  filter: null!=created
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            created
                            1970-01-01T00:00:00.000001Z
                            1970-01-01T00:00:00.000002Z
                            1970-01-01T00:00:00.000003Z
                            """,
                    query,
                    "created",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSelectMatchingButInDifferentOrderThanGroupBy() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            Sort light
                              keys: [hour, sym]
                                VirtualRecord
                                  functions: [sym,hour,avgBid]
                                    Async Group By workers: 1
                                      keys: [sym,hour]
                                      values: [avg(bid)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );
            assertQueryNoLeakCheck(
                    """
                            sym\thour\tavgBid
                            A\t0\t0.4922298136511458
                            B\t0\t0.4796420804429589
                            """,
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
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));

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
                    """
                            sum\tsum1\tcategory
                            1.920104572218119\t0.9826178313717698\tA
                            2.0117879412419453\t3.362073294894596\tB
                            6.020496469863701\t5.702005218155505\tC
                            """,
                    query,
                    null,
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light
                              keys: [category]
                                VirtualRecord
                                  functions: [sum,sum1,category]
                                    GroupBy vectorized: true workers: 1
                                      keys: [category]
                                      values: [sum(sum),sum(count)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: avg
                            """
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
