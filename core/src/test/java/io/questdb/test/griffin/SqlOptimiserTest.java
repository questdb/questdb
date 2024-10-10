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

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Misc;
import io.questdb.test.griffin.engine.groupby.SampleByTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlOptimiser.aliasAppearsInFuncArgs;
import static org.junit.Assert.assertEquals;

public class SqlOptimiserTest extends AbstractSqlParserTest {
    private static final String orderByAdviceDdl = "CREATE TABLE t (\n" +
            "  s SYMBOL index,\n" +
            "  ts TIMESTAMP\n" +
            ") timestamp(ts) PARTITION BY DAY;";
    private static final String orderByAdviceDml =
            "INSERT INTO t (s, ts) VALUES" +
                    " ('a', '2023-09-01T00:00:00.000Z')," +
                    " ('a', '2023-09-01T00:10:00.000Z')," +
                    " ('a', '2023-09-01T00:20:00.000Z')," +
                    " ('b', '2023-09-01T00:05:00.000Z')," +
                    " ('b', '2023-09-01T00:15:00.000Z')," +
                    " ('b', '2023-09-01T00:25:00.000Z')," +
                    " ('c', '2023-09-01T01:00:00.000Z')," +
                    " ('c', '2023-09-01T02:00:00.000Z')," +
                    " ('c', '2023-09-01T03:00:00.000Z')";

    @Test
    public void testAliasAppearsInFuncArgs1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x1]\n" +
                            "  values: [sum(x1)]\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select concat(lpad(cast(x1 as string), 5)), x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals(
                    "select-group-by concat(lpad(cast(x1,string),5)) concat, x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [concat,x1]\n" +
                            "  values: [sum(x1)]\n" +
                            "  filter: null\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select concat(lpad(cast(x1 as string), 5)), x1 from (select x x1 from y) group by x1";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-virtual concat(lpad(cast(x1,string),5)) concat, x1 from (select-group-by [x1] x1 from (select-choose [x x1] x x1 from (select [x] from y)))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [concat([lpad(x1::string,5)]),x1]\n" +
                            "    GroupBy vectorized: true workers: 1\n" +
                            "      keys: [x1]\n" +
                            "      values: [count(*)]\n" +
                            "        SelectedRecord\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs4() throws Exception {
        // check aliases are case-insensitive
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select x1, sum(x1), max(X1) from (select x X1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum, max(x1) max from (select-choose [x X1] x X1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [X1]\n" +
                            "  values: [sum(X1),max(X1)]\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs5() throws Exception {
        // test function on its own is caught
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [sum(x1)]\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs6() throws Exception {
        // test that col on its own works
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select x1 from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose x1 from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert !aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testAliasForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testAliasForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testAliasForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testConstantInGroupByDoesNotPreventOptimisation() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table hits (\n" +
                    "  URL string, ts timestamp\n" +
                    ") timestamp(ts) partition by day wal");

            insert("insert into hits (URL, ts) values ('abc', 0), ('abc', 1), ('def', 2), ('ghi', 3)");
            drainWalQueue();

            String q1 = "SELECT 1, URL, COUNT(*) AS c FROM hits ORDER BY c DESC LIMIT 10;";
            String q2 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;";
            String q3 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY URL, 1 ORDER BY c DESC LIMIT 10;";
            String q4 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, 2 ORDER BY c DESC LIMIT 10;";

            String expectedSql = "1\tURL\tc\n" +
                    "1\tabc\t2\n" +
                    "1\tghi\t1\n" +
                    "1\tdef\t1\n";
            String expectedPlan = "Sort light lo: 10\n" +
                    "  keys: [c desc]\n" +
                    "    VirtualRecord\n" +
                    "      functions: [1,URL,c]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [URL]\n" +
                    "          values: [count(*)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: hits\n";

            assertSql(expectedSql, q1);
            assertSql(expectedSql, q2);
            assertSql(expectedSql, q3);
            assertSql(expectedSql, q4);

            assertPlanNoLeakCheck(q1, expectedPlan);
            assertPlanNoLeakCheck(q2, expectedPlan);
            assertPlanNoLeakCheck(q3, expectedPlan);
            assertPlanNoLeakCheck(q4, expectedPlan);

        });
    }

    @Test
    public void testFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts FIRST from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testFirstAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by FIRST(x) FIRST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  values: [first(x)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testJoinAndUnionQueryWithJoinOnDesignatedTimestampColumnWithLastFunction() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            ddl("create table y1 ( x int, ts timestamp) timestamp(ts);");
            ddl("create table y2 ( x int, ts timestamp) timestamp(ts);");
            final String query = "select  * from y \n" +
                    "left join \n" +
                    "y1 on \n" +
                    "y1.x = y.x\n" +
                    "INNER join (select LAST(ts) from y2) as y2 \n" +
                    "on y2.LAST = y1.ts";
            String queryNew = query + " union \n" + query;
            final QueryModel model = compileModel(queryNew);
            assertEquals(
                    "select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x, " +
                            "y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) left join " +
                            "select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from (select-choose " +
                            "[ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc limit 1) y2 on " +
                            "y2.LAST = y1.ts) union select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x," +
                            " y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) " +
                            "left join select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from " +
                            "(select-choose [ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc " +
                            "limit 1) y2 on y2.LAST = y1.ts)",
                    model.toString0()
            );
            // TODO: there's a forward scan on y2 whereas it should be a backward scan;
            //       it could have something to do with SqlOptimiser.optimiseOrderBy()
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: y2.LAST=y1.ts\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: y1.x=y.x\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n" +
                            "            Hash\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y1\n" +
                            "        Hash\n" +
                            "            Sort light lo: 1\n" +
                            "              keys: [LAST desc]\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y2\n"
            );
        });
    }

    @Test
    public void testJoinWithSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y (x long, ts timestamp) timestamp(ts);");
            ddl("create table y1 (x long, ts timestamp) timestamp(ts);");
            final String queryA = "select * from y \n" +
                    "inner join (select count_distinct(x) c from y1) as y1 \n" +
                    "on y.x = y1.c";
            final String queryB = "select * from y \n" +
                    "inner join (select count(distinct x) c from y1) as y1 \n" +
                    "on y.x = y1.c";

            String expectedModel = "select-choose y.x x, y.ts ts, y1.c c from (select [x, ts] from y timestamp (ts) " +
                    "join select [c] from (select-group-by [count() c] count() c from (select-group-by x from " +
                    "(select [x] from y1 timestamp (ts) where null != x))) y1 on y1.c = y.x)";
            assertEquals(
                    expectedModel,
                    compileModel(queryA).toString0()
            );
            assertEquals(
                    expectedModel,
                    compileModel(queryB).toString0()
            );

            String expectedPlan = "SelectedRecord\n" +
                    "    Hash Join\n" +
                    "      condition: y1.c=y.x\n" +
                    "        PageFrame\n" +
                    "            Row forward scan\n" +
                    "            Frame forward scan on: y\n" +
                    "        Hash\n" +
                    "            Count\n" +
                    "                Async JIT Group By workers: 1\n" +
                    "                  keys: [x]\n" +
                    "                  filter: x!=null\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: y1\n";
            assertPlanNoLeakCheck(
                    queryA,
                    expectedPlan
            );
            assertPlanNoLeakCheck(
                    queryB,
                    expectedPlan
            );
        });
    }

    @Test
    public void testLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testLastAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by LAST(x) LAST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  values: [last(x)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select max(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts max from (select [ts] from y timestamp (ts)) order by max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testMaxAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by MAX(x) MAX from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [max(x)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select min(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts min from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testMinAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by MIN(x) MIN from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [min(x)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testNestedFirstFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testNestedLastFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select LAST(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose LAST from " +
                            "(select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1)",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testNestedMaxFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MAX(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose MAX from " +
                            "(select-choose [ts MAX] ts MAX from (select [ts] from y timestamp (ts)) order by MAX desc limit 1)",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testNestedMinFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MIN(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose MIN from (select-choose [ts MIN] ts MIN from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testNestedUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y union select LAST(ts) from y union select min(ts) from y  " +
                    "union select max(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select" +
                            " [ts] from y timestamp (ts)) limit 1 union select-choose [ts LAST] ts LAST from (select " +
                            "[ts] from y timestamp (ts)) order by LAST desc limit 1 union select-choose [ts min] ts min " +
                            "from (select [ts] from y timestamp (ts)) limit 1 union select-choose [ts max] ts max from " +
                            "(select [ts] from y timestamp (ts)) order by max desc limit 1)",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "Union\n" +
                            "    Union\n" +
                            "        Union\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row backward scan\n" +
                            "                        Frame backward scan on: y\n" +
                            "        Limit lo: 1\n" +
                            "            SelectedRecord\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y\n" +
                            "    Limit lo: 1\n" +
                            "        SelectedRecord\n" +
                            "            PageFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromDifferentTables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab1 (\n" +
                    "    id int,\n" +
                    "    a int\n" +
                    "  );\n");

            ddl("create table tab2 (\n" +
                    "    id int,\n" +
                    "    b int\n" +
                    "  );");

            assertPlanNoLeakCheck(
                    "select a, b\n" +
                            "            from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "            order by a, b",
                    "Sort\n" +
                            "  keys: [a, b]\n" +
                            "    SelectedRecord\n" +
                            "        Hash Join Light\n" +
                            "          condition: tab2.id=tab1.id\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab1\n" +
                            "            Hash\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab2\n"
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromOneTableWithOrderingAlias() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab1 (\n" +
                    "    id int,\n" +
                    "    a int," +
                    "    ts timestamp\n" +
                    "  ) timestamp(ts);\n");

            ddl("create table tab2 (\n" +
                    "    id int,\n" +
                    "    b int," +
                    "    ts timestamp\n" +
                    "  ) timestamp(ts);");

            // No top level sort needed, sort is by tab1.ts
            assertPlanNoLeakCheck(
                    "select tab1.id, tab1.ts as b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by b",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: tab2.id=tab1.id\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab1\n" +
                            "        Hash\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab2\n"
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromOneTableWithOrderingPosition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab1 (\n" +
                    "    id int,\n" +
                    "    a int," +
                    "    ts timestamp\n" +
                    "  ) timestamp(ts);\n");

            ddl("create table tab2 (\n" +
                    "    id int,\n" +
                    "    b int," +
                    "    ts timestamp\n" +
                    "  ) timestamp(ts);");

            // No top level sort needed, sort is by tab1.ts
            assertPlanNoLeakCheck(
                    "select tab1.id, tab1.ts as b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by 2",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: tab2.id=tab1.id\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab1\n" +
                            "        Hash\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab2\n"
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromTheSameTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab1 (\n" +
                    "    id int,\n" +
                    "    a int\n" +
                    "  );\n");

            ddl("create table tab2 (\n" +
                    "    id int,\n" +
                    "    b int\n" +
                    "  );");

            assertPlanNoLeakCheck(
                    "select a, b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by a desc " +
                            "limit 10",
                    "Limit lo: 10\n" +
                            "    Sort\n" +
                            "      keys: [a desc]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tab2.id=tab1.id\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab1\n" +
                            "                Hash\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab2\n"
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromTheSameTableWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab1 (\n" +
                    "    id int,\n" +
                    "    a int,\n" +
                    "    ts timestamp\n" +
                    "  ) timestamp(ts);\n");

            ddl("create table tab2 (\n" +
                    "    id int,\n" +
                    "    b int\n" +
                    "  );");

            assertPlanNoLeakCheck(
                    "select a, b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by a desc, ts " +
                            "limit 10",
                    "SelectedRecord\n" +
                            "    Limit lo: 10\n" +
                            "        Sort\n" +
                            "          keys: [a desc, ts]\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: tab2.id=tab1.id\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab1\n" +
                            "                    Hash\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: tab2\n"
            );
        });
    }

    @Test
    public void testOrderByAdviceWithMultipleJoinsAndFilters() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE 'WorkflowEvent' (\n" +
                    "  CreateDate timestamp,\n" +
                    "  Id uuid,\n" +
                    "  TenantId int,\n" +
                    "  UserId int,\n" +
                    "  EventTypeId int\n" +
                    ") timestamp (CreateDate) partition by hour wal;");

            ddl("CREATE TABLE 'WorkflowEventAction' (\n" +
                    "  CreateDate TIMESTAMP,\n" +
                    "  WorkflowEventId UUID,\n" +
                    "  ActionTypeId INT,\n" +
                    "  Message STRING\n" +
                    ") timestamp (CreateDate) PARTITION BY HOUR WAL;");

            insert("insert into WorkflowEventAction (CreateDate, WorkflowEventId, ActionTypeId, Message) values" +
                    " ('2016-01-01T00:00:00Z', to_uuid(1, 1), 13, '2')");
            insert("insert into WorkflowEvent (CreateDate, Id, TenantId, UserId, EventTypeId) values ('2016-01-01T00:00:00Z', to_uuid(1, 1), 24024, 19, 1)");
            drainWalQueue();

            assertPlanNoLeakCheck(
                    "SELECT  1\n" +
                            "FROM    WorkflowEvent el\n" +
                            "\n" +
                            "LEFT JOIN WorkflowEventAction ep0\n" +
                            "  ON    el.CreateDate = ep0.CreateDate\n" +
                            "  and   el.Id = ep0.WorkflowEventId\n" +
                            "  and   ep0.ActionTypeId = 13\n" +
                            "  and   ep0.Message = '2'\n" +
                            "\n" +
                            "LEFT JOIN    WorkflowEventAction ep\n" +
                            "  on    el.CreateDate = ep.CreateDate\n" +
                            "  and   el.Id = ep.WorkflowEventId\n" +
                            "  and   ep.ActionTypeId = 8\n" +
                            "\n" +
                            "WHERE   el.UserId = 19\n" +
                            "  and   el.TenantId = 24024\n" +
                            "  and   el.EventTypeId = 1\n" +
                            "  and   el.CreateDate >= to_timestamp('2016-01-01T00:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')\n" +
                            "  and   el.CreateDate <= to_timestamp('2016-01-01T10:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')",
                    "VirtualRecord\n" +
                            "  functions: [1]\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: ep.WorkflowEventId=el.Id and ep.CreateDate=el.CreateDate\n" +
                            "      filter: ep.ActionTypeId=8\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: ep0.WorkflowEventId=el.Id and ep0.CreateDate=el.CreateDate\n" +
                            "          filter: (ep0.ActionTypeId=13 and ep0.Message='2')\n" +
                            "            Empty table\n" +
                            "            Hash\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: WorkflowEventAction\n" +
                            "        Hash\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: WorkflowEventAction\n"
            );

            assertQuery("select-virtual 1 1 from (select [Id, CreateDate, UserId, TenantId, EventTypeId] from WorkflowEvent el timestamp (CreateDate) join (select [WorkflowEventId, CreateDate, ActionTypeId, Message] from WorkflowEventAction ep0 timestamp (CreateDate) where ActionTypeId = 13 and Message = '2') ep0 on ep0.WorkflowEventId = el.Id and ep0.CreateDate = el.CreateDate join (select [WorkflowEventId, CreateDate, ActionTypeId] from WorkflowEventAction ep timestamp (CreateDate) where ActionTypeId = 8) ep on ep.WorkflowEventId = el.Id and ep.CreateDate = el.CreateDate where UserId = 19 and TenantId = 24024 and EventTypeId = 1 and CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z','yyyy-MM-ddTHH:mm:ss.SSSUUUZ') and CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z','yyyy-MM-ddTHH:mm:ss.SSSUUUZ')) el", "SELECT  1\n" +
                    "FROM    WorkflowEvent el\n" +
                    "\n" +
                    "JOIN    WorkflowEventAction ep0\n" +
                    "  ON    el.CreateDate = ep0.CreateDate\n" +
                    "  and   el.Id = ep0.WorkflowEventId\n" +
                    "  and   ep0.ActionTypeId = 13\n" +
                    "  and   ep0.Message = '2'\n" +
                    "\n" +
                    "join    WorkflowEventAction ep\n" +
                    "  on    el.CreateDate = ep.CreateDate\n" +
                    "  and   el.Id = ep.WorkflowEventId\n" +
                    "  and   ep.ActionTypeId = 8\n" +
                    "\n" +
                    "WHERE   el.UserId = 19\n" +
                    "  and   el.TenantId = 24024\n" +
                    "  and   el.EventTypeId = 1\n" +
                    "  and   el.CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')\n" +
                    "  and   el.CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')");

            assertSql("1\n", "SELECT  1\n" +
                    "FROM    WorkflowEvent el\n" +
                    "\n" +
                    "JOIN    WorkflowEventAction ep0\n" +
                    "  ON    el.CreateDate = ep0.CreateDate\n" +
                    "  and   el.Id = ep0.WorkflowEventId\n" +
                    "  and   ep0.ActionTypeId = 13\n" +
                    "  and   ep0.Message = '2'\n" +
                    "\n" +
                    "JOIN    WorkflowEventAction ep\n" +
                    "  on    el.CreateDate = ep.CreateDate\n" +
                    "  and   el.Id = ep.WorkflowEventId\n" +
                    "  and   ep.ActionTypeId = 8\n" +
                    "\n" +
                    "WHERE   el.UserId = 19\n" +
                    "  and   el.TenantId = 24024\n" +
                    "  and   el.EventTypeId = 1\n" +
                    "  and   el.CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ') \n" +
                    "  and   el.CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')");


            assertSql("CreateDate\tId\tTenantId\tUserId\tEventTypeId\tCreateDate1\tWorkflowEventId\tActionTypeId\tMessage\tCreateDate2\tWorkflowEventId1\tActionTypeId1\tMessage1\n" +
                            "2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t24024\t19\t1\t2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t13\t2\t\t\tnull\t\n",

                    "SELECT  *\n" +
                            "FROM    WorkflowEvent el\n" +
                            "\n" +
                            "LEFT JOIN WorkflowEventAction ep0\n" +
                            "  ON    el.CreateDate = ep0.CreateDate\n" +
                            "  and   el.Id = ep0.WorkflowEventId\n" +
                            "  and   ep0.ActionTypeId = 13\n" +
                            "  and   ep0.Message = '2'\n" +
                            "\n" +
                            "LEFT JOIN WorkflowEventAction ep\n" +
                            "  on    el.CreateDate = ep.CreateDate\n" +
                            "  and   el.Id = ep.WorkflowEventId\n" +
                            "  and   ep.ActionTypeId = 8\n" +
                            "\n" +
                            "WHERE   el.UserId = 19\n" +
                            "  and   el.TenantId = 24024\n" +
                            "  and   el.EventTypeId = 1\n" +
                            "  and   el.CreateDate >= '2016-01-01T00:00:00Z'\n" +
                            "  and   el.CreateDate <= '2016-01-01T10:00:00Z'");
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin1() throws Exception {
        // Case when order by is one table and not timestamp first
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Fast Scan\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin2() throws Exception {
        // Case when order by is one table and timestamp first
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.ts, t1.s\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by ts, s limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Fast Scan\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin3() throws Exception {
        // Case when order by is for more than one table prefix
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "    FROM t1\n" +
                    "    ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "    ORDER BY t1.s, t2.ts\n" +
                    "    LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Fast Scan\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin4() throws Exception {
        // Case when ordering by secondary table
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t2.s, t2.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s1, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s1, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Fast Scan\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin5() throws Exception {
        // Case when order by is one table and not timestamp first
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Fast Scan\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1a() throws Exception {
        // case when ordering by symbol, then timestamp - we expect to use the symbol index
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1b() throws Exception {
        // case when ordering by symbol, then timestamp - we expect to use the symbol index
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1c() throws Exception {
        // case when by columns from both tables - expect it to use the sort
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts, t2.ts, t2.s\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts, ts1, s1 limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts, ts1, s1]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin2() throws Exception {
        // case when ordering by just symbol - we expect to use the symbol index
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin3() throws Exception {
        // case when ordering by timestamp, then symbol - we expect to not use the symbol index
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.ts, t1.s\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by ts, s limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin4() throws Exception {
        // case when ordering by timestamp, then symbol - we expect to not use the symbol index
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "CROSS JOIN t2\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.ts, t2.s, t1.s, t2.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by ts, s1, s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s1, s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithLtJoin() throws Exception {
        // Case when order by is for more than one table prefix
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "    FROM t1\n" +
                    "    LT JOIN t2 ON t1.s = t2.s\n" +
                    "    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "    ORDER BY t1.s, t2.ts\n" +
                    "    LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) lt join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            Lt Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
                    "a\t2023-09-01T00:00:00.000000Z\t\t\n" +
                    "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                    "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                    "b\t2023-09-01T00:05:00.000000Z\t\t\n" +
                    "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                    "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                    "c\t2023-09-01T01:00:00.000000Z\t\t\n", query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithRegularJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));
            ddl(orderByAdviceDdl.replace(" t ", " t2 "));
            insert(orderByAdviceDml.replace(" t ", " t1 "));
            insert(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts, t2.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1000000\n" +
                            "    Sort\n" +
                            "      keys: [s, ts, ts1]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: t2.s=t1.s\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Interval forward scan on: t1\n" +
                            "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                            "                Hash\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: t2\n"
            );
            assertSql(
                    "s\tts\ts1\tts1\n" +
                            "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                            "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                            "a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                            "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                            "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                            "a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                            "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z\n" +
                            "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z\n" +
                            "a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z\n" +
                            "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                            "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                            "b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                            "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                            "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                            "b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                            "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z\n" +
                            "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z\n" +
                            "b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z\n" +
                            "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z\n" +
                            "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z\n" +
                            "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n",
                    query
            );
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin1() throws Exception {
        // Case when no join
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = "SELECT s, ts\n" +
                    "    FROM t1\n" +
                    "    ORDER BY t1.s, t1.ts\n" +
                    "    LIMIT 1000000;";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query, "Sort light lo: 1000000\n" +
                    "  keys: [s, ts]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin2() throws Exception {
        // Test with reverse limit, expect sort due to symbol first
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = "SELECT s, ts\n" +
                    "    FROM t1\n" +
                    "    ORDER BY t1.s, t1.ts\n" +
                    "    LIMIT -10;";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by s, ts limit -(10)", query);
            assertPlanNoLeakCheck(query, "Sort light lo: -10\n" +
                    "  keys: [s, ts]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin3() throws Exception {
        // Test with reverse limit
        assertMemoryLeak(() -> {
            ddl(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = "SELECT s, ts\n" +
                    "    FROM t1\n" +
                    "    ORDER BY t1.ts, t1.s\n" +
                    "    LIMIT -10;";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by ts, s limit -(10)", query);
            assertPlanNoLeakCheck(query, "Sort light lo: -10 partiallySorted: true\n" +
                    "  keys: [ts, s]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    @Test
    public void testOrderingOfSortsInSingleTimestampCase() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            insert("insert into a select x::int as i, x::timestamp as ts from long_sequence(10000)");

            assertPlanNoLeakCheck(
                    "select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts desc " +
                            " limit 10" +
                            ") " +
                            "order by ts desc",
                    "Limit lo: 10\n" +
                            "    SelectedRecord\n" +
                            "        Cross Join\n" +
                            "            PageFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: a\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n"
            );

            assertQueryNoLeakCheck("i\tts\ti1\tts1\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t2\t1970-01-01T00:00:00.000002Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t3\t1970-01-01T00:00:00.000003Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t4\t1970-01-01T00:00:00.000004Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t5\t1970-01-01T00:00:00.000005Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t6\t1970-01-01T00:00:00.000006Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t7\t1970-01-01T00:00:00.000007Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t8\t1970-01-01T00:00:00.000008Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t9\t1970-01-01T00:00:00.000009Z\n" +
                            "10000\t1970-01-01T00:00:00.010000Z\t10\t1970-01-01T00:00:00.000010Z\n",
                    "select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts desc " +
                            " limit 10" +
                            ") " +
                            "order by ts desc",
                    "ts"
            );
        });
    }

    @Test
    public void testQueryPlanForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts FIRST from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForFirstAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by FIRST(x) FIRST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  values: [first(x)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForJoinAndUnionQueryWithJoinOnDesignatedTimestampColumnWithLastFunction() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            ddl("create table y1 ( x int, ts timestamp) timestamp(ts);");
            ddl("create table y2 ( x int, ts timestamp) timestamp(ts);");
            final String query = "select  * from y \n" +
                    "left join \n" +
                    "y1 on \n" +
                    "y1.x = y.x\n" +
                    "INNER join (select LAST(ts) from y2) as y2 \n" +
                    "on y2.LAST = y1.ts";
            String queryNew = query + " union \n" + query;
            final QueryModel model = compileModel(queryNew);
            TestUtils.assertEquals("select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x, " +
                    "y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) left join " +
                    "select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from (select-choose " +
                    "[ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc limit 1) y2 on " +
                    "y2.LAST = y1.ts) union select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x," +
                    " y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) " +
                    "left join select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from " +
                    "(select-choose [ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc " +
                    "limit 1) y2 on y2.LAST = y1.ts)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: y2.LAST=y1.ts\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: y1.x=y.x\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n" +
                            "            Hash\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y1\n" +
                            "        Hash\n" +
                            "            Sort light lo: 1\n" +
                            "              keys: [LAST desc]\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y2\n");

        });
    }

    @Test
    public void testQueryPlanForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForLastAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by LAST(x) LAST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  values: [last(x)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select max(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts max from (select [ts] from y timestamp (ts)) order by max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForMaxAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MAX(x) MAX from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [max(x)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select min(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts min from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForMinAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MIN(x) MIN from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [min(x)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForNestedFirstFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForNestedLastFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select LAST(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose LAST from (select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForNestedMaxFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MAX(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose MAX from (select-choose [ts MAX] ts MAX from (select [ts] from y timestamp (ts)) order by MAX desc limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForNestedMinFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MIN(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose MIN from (select-choose [ts MIN] ts MIN from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForNestedUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y union select LAST(ts) from y union select min(ts) from y  " +
                    "union select max(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select" +
                    " [ts] from y timestamp (ts)) limit 1 union select-choose [ts LAST] ts LAST from (select " +
                    "[ts] from y timestamp (ts)) order by LAST desc limit 1 union select-choose [ts min] ts min " +
                    "from (select [ts] from y timestamp (ts)) limit 1 union select-choose [ts max] ts max from " +
                    "(select [ts] from y timestamp (ts)) order by max desc limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Union\n" +
                            "    Union\n" +
                            "        Union\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row backward scan\n" +
                            "                        Frame backward scan on: y\n" +
                            "        Limit lo: 1\n" +
                            "            SelectedRecord\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y\n" +
                            "    Limit lo: 1\n" +
                            "        SelectedRecord\n" +
                            "            PageFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingFirstFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, FIRST(ts) FIRST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [first(ts)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingLastFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, LAST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, LAST(ts) LAST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [last(ts)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingMaxFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MAX(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, MAX(ts) MAX from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [max(ts)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingMinFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MIN(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, MIN(ts) MIN from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [min(ts)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y union select LAST(ts) from y union select min(ts) from y  union select max(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts))" +
                    " limit 1 union select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by " +
                    "LAST desc limit 1 union select-choose [ts min] ts min from (select [ts] from y timestamp (ts)) " +
                    "limit 1 union select-choose [ts max] ts max from (select [ts] from y timestamp (ts)) order by" +
                    " max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Union\n" +
                            "    Union\n" +
                            "        Union\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row backward scan\n" +
                            "                        Frame backward scan on: y\n" +
                            "        Limit lo: 1\n" +
                            "            SelectedRecord\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y\n" +
                            "    Limit lo: 1\n" +
                            "        SelectedRecord\n" +
                            "            PageFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: y\n");

        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithFirstAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by FIRST(ts) FIRST from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [first(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithLastAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by LAST(ts) LAST from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [last(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithMaxAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MAX(ts) MAX from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [max(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithMinAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MIN(ts) MIN from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [min(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithFirstAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts FIRST from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithLastAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts LAST from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                    "by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithMaxAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts MAX from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                    "by MAX desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithMinAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts MIN from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanWithAliasForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanWithAliasForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanWithAliasForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n");
        });
    }

    @Test
    public void testQueryPlanWithAliasForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 1\n" +
                            "    SelectedRecord\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testRangeFillWhenThereAreNoRecords() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE 'trades' (\n" +
                    "  symbol SYMBOL capacity 256 CACHE,\n" +
                    "  side SYMBOL capacity 256 CACHE,\n" +
                    "  price DOUBLE,\n" +
                    "  amount DOUBLE,\n" +
                    "  timestamp TIMESTAMP\n" +
                    ") timestamp (timestamp) PARTITION BY DAY WAL;");
            drainWalQueue();

            String query = "with a as (\n" +
                    "SELECT \n" +
                    "    timestamp,\n" +
                    "    vwap(price, amount) AS vwap_price,\n" +
                    "    sum(amount) AS volume\n" +
                    "FROM trades\n" +
                    "WHERE symbol = 'MEH' AND timestamp > dateadd('d', -1, now())\n" +
                    "SAMPLE by 1h FILL(NULL)\n" +
                    ")\n" +
                    "select * from a;";

            assertSql("timestamp\tvwap_price\tvolume\n", query);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationBetween() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'";
            final String target = "select ts, avg(x) from fromto\n" +
                    "where ts >= '2017-12-20' and ts < '2018-01-31'\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20' and ts < '2018-01-31') sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
            assertModel(model, target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationGreaterThanOrEqualTo() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' align to calendar with offset '10:00'";
            final String target = "select ts, avg(x) from fromto\n" +
                    "where ts >= '2017-12-20'\n" +
                    "sample by 5d from '2017-12-20' align to calendar with offset '10:00'";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20') sample by 5d from '2017-12-20' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
            assertModel(model, target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationLesserThan() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d to '2018-01-31' align to calendar with offset '10:00'";
            final String target = "select ts, avg(x) from fromto\n" +
                    "where ts < '2018-01-31'\n" +
                    "sample by 5d to '2018-01-31' align to calendar with offset '10:00'";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts < '2018-01-31') sample by 5d to '2018-01-31' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
            assertModel(model, target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereBetween() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "where s != '5'\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'\n";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts >= '2017-12-20' and ts < '2018-01-31' and s != '5') sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereGreaterThanOrEqualTo() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "where s != '5'\n" +
                    "sample by 5d from '2017-12-20' align to calendar with offset '10:00'\n";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts >= '2017-12-20' and s != '5') sample by 5d from '2017-12-20' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereLesserThan() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "where s != '5'\n" +
                    "sample by 5d to '2018-01-31' align to calendar with offset '10:00'\n";

            final String model = "select-group-by ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts < '2018-01-31' and s != '5') sample by 5d to '2018-01-31' align to calendar with offset '10:00'";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToCheckingColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query =
                    "select ts, avg(x), " +
                            "string_agg(s, ',')," +
                            "avg(b)," +
                            "avg(e)," +
                            "avg(i)," +
                            "avg(f)," +
                            "avg(d)," +
                            "string_agg(str, ',')," +
                            "avg(a::double)," +
                            "avg(k::double)," +
                            "avg(t::double)," +
                            "avg(n::double)," +
                            "from fromto sample by 5d from '2018-01-01' to '2018-01-31' fill(null)";

            assertSql("ts\tavg\tstring_agg\tavg1\tavg2\tavg3\tavg4\tavg5\tstring_agg1\tavg6\tavg7\tavg8\tavg9\n" +
                    "2018-01-01T00:00:00.000000Z\t120.5\t1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240\t-0.03333333333333333\t120.5\t120.5\t120.5\t120.5\t1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240\t4.5\t120.5\t1.0\t120.5\n" +
                    "2018-01-06T00:00:00.000000Z\t360.5\t241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480\t1.0333333333333334\t360.5\t360.5\t360.5\t360.5\t241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480\t4.5\t360.5\t1.0\t360.5\n" +
                    "2018-01-11T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull\tnull\n" +
                    "2018-01-16T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull\tnull\n" +
                    "2018-01-21T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull\tnull\n" +
                    "2018-01-26T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToDisallowedQueryWithKey() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            assertException("SELECT ts, count, s\n" +
                    "FROM fromto\n" +
                    "SAMPLE BY 5d FROM '2018-01-01' TO '2019-01-01'\n" +
                    "LIMIT 6", 0, "are not supported for keyed SAMPLE BY");
        });
    }

    @Test
    public void testSampleByFromToFillNullWithExtraColumns() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x), sum(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null,null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x),sum(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");
            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToNotEnoughFillValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query =
                    "select ts, avg(x), " +
                            "string_agg(s, ',')," +
                            "avg(b)," +
                            "avg(e)," +
                            "avg(i)," +
                            "avg(f)," +
                            "avg(d)," +
                            "string_agg(str, ',')," +
                            "avg(a::double)," +
                            "avg(k::double)," +
                            "avg(t::double)," +
                            "avg(n::double)," +
                            "from fromto sample by 5d from '2018-01-01' to '2018-01-31' fill(42)";

            assertException(query, -1, "not enough fill values");
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewrite() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");
            assertSql("ts\tavg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteMultipleFills() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x), sum(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(42, 41)";

            assertPlanNoLeakCheck(query, "Sample By\n" +
                    "  fill: value\n" +
                    "  range: ('2017-12-20','2018-01-31')\n" +
                    "  values: [avg(x),sum(x)]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: fromto\n" +
                    "          intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");
            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\t42.0\t41\n" +
                    "2017-12-25T00:00:00.000000Z\t42.0\t41\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\t42.0\t41\n" +
                    "2018-01-19T00:00:00.000000Z\t42.0\t41\n" +
                    "2018-01-24T00:00:00.000000Z\t42.0\t41\n" +
                    "2018-01-29T00:00:00.000000Z\t42.0\t41\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewritePostfill() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d to '2018-01-31' fill(null)";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: (null,'2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"MIN\",\"2018-01-30T23:59:59.999999Z\")]\n");
            assertSql("ts\tavg\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewritePrefill() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) ";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20',null)\n" +
                    "      stride: '5d'\n" +
                    "      values: [null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"MAX\")]\n");
            assertSql("ts\tavg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithExcept() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            ddl(SampleByTest.DDL_FROMTO.replace("fromto", "fromto2"));

            final String exceptAllQuery = "select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n" +
                    "except all\n" +
                    "select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            final String exceptQuery = exceptAllQuery.replace("except all", "except");

            assertPlanNoLeakCheck(exceptAllQuery, "Except All\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Hash\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto2\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n", exceptAllQuery);

            assertPlanNoLeakCheck(exceptQuery, "Except\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Hash\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto2\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n", exceptQuery);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithIntersect() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            ddl(SampleByTest.DDL_FROMTO.replace("fromto", "fromto2"));

            final String intersectAllQuery = "select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n" +
                    "intersect all\n" +
                    "select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            final String intersectQuery = intersectAllQuery.replace("intersect all", "intersect");

            assertPlanNoLeakCheck(intersectAllQuery, "Intersect All\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Hash\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto2\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n", intersectAllQuery);

            assertPlanNoLeakCheck(intersectQuery, "Intersect\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Hash\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto2\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n", intersectQuery);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            ddl(SampleByTest.DDL_FROMTO.replace("fromto", "fromto2"));


            final String query = "select fromto.ts, avg(fromto.x)\n" +
                    "from fromto\n" +
                    "asof join fromto2\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null]\n" +
                    "        GroupBy vectorized: false\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x)]\n" +
                    "            SelectedRecord\n" +
                    "                AsOf Join Fast Scan\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Interval forward scan on: fromto\n" +
                    "                          intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: fromto2\n");
            assertSql("ts\tavg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithJoin2() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            ddl(SampleByTest.DDL_FROMTO.replace("fromto", "fromto2"));

            final String query = "(select ts as five_days, avg(x) as five_days_avg from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null))\n" +
                    "asof join\n" +
                    "(select ts as ten_days, avg(x) as ten_days_avg from fromto2 sample by 10d from '2017-12-20' to '2018-01-31' fill(null))\n";

            assertPlanNoLeakCheck(query, "SelectedRecord\n" +
                    "    AsOf Join\n" +
                    "        Sort\n" +
                    "          keys: [five_days]\n" +
                    "            Fill Range\n" +
                    "              range: ('2017-12-20','2018-01-31')\n" +
                    "              stride: '5d'\n" +
                    "              values: [null]\n" +
                    "                Async Group By workers: 1\n" +
                    "                  keys: [five_days]\n" +
                    "                  values: [avg(x)]\n" +
                    "                  filter: null\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Interval forward scan on: fromto\n" +
                    "                          intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "        Sort\n" +
                    "          keys: [ten_days]\n" +
                    "            Fill Range\n" +
                    "              range: ('2017-12-20','2018-01-31')\n" +
                    "              stride: '10d'\n" +
                    "              values: [null]\n" +
                    "                Async Group By workers: 1\n" +
                    "                  keys: [ten_days]\n" +
                    "                  values: [avg(x)]\n" +
                    "                  filter: null\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Interval forward scan on: fromto2\n" +
                    "                          intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");
            assertSql("five_days\tfive_days_avg\tten_days\tten_days_avg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\t2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\t2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t2017-12-30T00:00:00.000000Z\t192.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t2017-12-30T00:00:00.000000Z\t192.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\t2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\t2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\t2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\t2018-01-29T00:00:00.000000Z\tnull\n", query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithKeys() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String shouldFail1a = "select ts, avg(x), s from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) ";

            final String shouldFail1b = "select ts, avg(x), s from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) align to calendar with offset '10:00'";


            final String shouldFail2a = "select ts, avg(x), sum(x), concat('1', s) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) ";


            final String shouldFail2b = "select ts, avg(x), sum(x), concat('1', s) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) align to calendar with offset '10:00'";

            assertException(shouldFail1a, 0, "FROM-TO");
            assertException(shouldFail1b, 0, "FROM-TO");
            assertException(shouldFail2a, 0, "FROM-TO");
            assertException(shouldFail2b, 0, "FROM-TO");

            final String shouldSucceedParallel = "select ts, avg(x), sum(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) ";

            final String shouldSucceedSequential = "select ts, avg(x), sum(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) align to calendar with offset '10:00'";

            final String shouldSucceedResult = "ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n";

            assertPlanNoLeakCheck(shouldSucceedParallel, "Sort\n" +
                    "  keys: [ts]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20',null)\n" +
                    "      stride: '5d'\n" +
                    "      values: [null,null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x),sum(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"MAX\")]\n");
            assertSql(shouldSucceedResult, shouldSucceedParallel);

            assertPlanNoLeakCheck(shouldSucceedSequential, "Sample By\n" +
                    "  fill: null\n" +
                    "  range: ('2017-12-20',null)\n" +
                    "  values: [avg(x),sum(x)]\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: fromto\n" +
                    "          intervals: [(\"2017-12-20T00:00:00.000000Z\",\"MAX\")]\n");
            assertSql(shouldSucceedResult, shouldSucceedSequential);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            ddl(SampleByTest.DDL_FROMTO.replace("fromto", "fromto2"));

            final String unionAllQuery = "select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n" +
                    "union all\n" +
                    "select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)\n";

            final String unionQuery = unionAllQuery.replace("union all", "union");

            assertPlanNoLeakCheck(unionAllQuery, "Union All\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null,null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x),sum(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto2\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n", unionAllQuery);

            assertPlanNoLeakCheck(unionQuery, "Union\n" +
                    "    Sort\n" +
                    "      keys: [ts]\n" +
                    "        Fill Range\n" +
                    "          range: ('2017-12-20','2018-01-31')\n" +
                    "          stride: '5d'\n" +
                    "          values: [null,null]\n" +
                    "            Async Group By workers: 1\n" +
                    "              keys: [ts]\n" +
                    "              values: [avg(x),sum(x)]\n" +
                    "              filter: null\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: fromto\n" +
                    "                      intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null,null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [ts]\n" +
                    "          values: [avg(x),sum(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto2\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("ts\tavg\tsum\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\t10440\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\t63480\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\t41520\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\tnull\n", unionQuery);
        });
    }

    @Test
    public void testSampleByFromToParallelSequentialEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);

            final String parallel = "select ts, avg(x) from fromto\n" +
                    "sample by 1w from '2017-12-20' to '2018-01-31' fill(null)";

            // offset is ignored
            final String sequential = parallel + " align to calendar with offset '10:00'";

            final String result = "ts\tavg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-27T00:00:00.000000Z\t48.5\n" +
                    "2018-01-03T00:00:00.000000Z\t264.5\n" +
                    "2018-01-10T00:00:00.000000Z\t456.5\n" +
                    "2018-01-17T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\n";

            assertSql(result, parallel);
            assertSql(result, sequential);
        });
    }

    @Test
    public void testSampleByFromToPlansWithRewrite() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl (\n" +
                    "  ts timestamp,\n" +
                    "  price double\n" +
                    ") timestamp(ts) partition by day wal;");
            drainWalQueue();
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m from '2018-01-01' to '2019-01-01'",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [avg(price)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: tbl\n" +
                            "              intervals: [(\"2018-01-01T00:00:00.000000Z\",\"2018-12-31T23:59:59.999999Z\")]\n"
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m from '2018-01-01'",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [avg(price)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: tbl\n" +
                            "              intervals: [(\"2018-01-01T00:00:00.000000Z\",\"MAX\")]\n"
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m to '2019-01-01'",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [avg(price)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: tbl\n" +
                            "              intervals: [(\"MIN\",\"2018-12-31T23:59:59.999999Z\")]\n"
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [avg(price)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tbl\n"
            );
        });

    }

    @Test
    public void testSampleByFromToWithAliases() throws Exception {
        assertMemoryLeak(() -> {
            ddl(SampleByTest.DDL_FROMTO);
            final String query = "select ts as five_days, avg(x) as five_days_avg from fromto \n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(null)";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [five_days]\n" +
                    "    Fill Range\n" +
                    "      range: ('2017-12-20','2018-01-31')\n" +
                    "      stride: '5d'\n" +
                    "      values: [null]\n" +
                    "        Async Group By workers: 1\n" +
                    "          keys: [five_days]\n" +
                    "          values: [avg(x)]\n" +
                    "          filter: null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: fromto\n" +
                    "                  intervals: [(\"2017-12-20T00:00:00.000000Z\",\"2018-01-30T23:59:59.999999Z\")]\n");

            assertSql("five_days\tfive_days_avg\n" +
                    "2017-12-20T00:00:00.000000Z\tnull\n" +
                    "2017-12-25T00:00:00.000000Z\tnull\n" +
                    "2017-12-30T00:00:00.000000Z\t72.5\n" +
                    "2018-01-04T00:00:00.000000Z\t264.5\n" +
                    "2018-01-09T00:00:00.000000Z\t432.5\n" +
                    "2018-01-14T00:00:00.000000Z\tnull\n" +
                    "2018-01-19T00:00:00.000000Z\tnull\n" +
                    "2018-01-24T00:00:00.000000Z\tnull\n" +
                    "2018-01-29T00:00:00.000000Z\tnull\n", query);
        });
    }

    @Test
    public void testSampleByGroupByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE 'trades' (\n" +
                    "  symbol SYMBOL capacity 256 CACHE,\n" +
                    "  side SYMBOL capacity 256 CACHE,\n" +
                    "  price DOUBLE,\n" +
                    "  amount DOUBLE,\n" +
                    "  timestamp TIMESTAMP\n" +
                    ") timestamp (timestamp) PARTITION BY DAY WAL;");

            assertPlanNoLeakCheck("SELECT last(price) value, symbol, timestamp \n" +
                            "FROM trades\n" +
                            "WHERE timestamp >= '2024-08-11T10:13:00' \n" +
                            "AND timestamp < '2024-08-11T10:16:00' \n" +
                            "AND (symbol LIKE ('BTC-USD')) \n" +
                            "SAMPLE BY 1m FILL(NONE) ALIGN TO CALENDAR",
                    "Radix sort light\n" +
                            "  keys: [timestamp]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [symbol,timestamp]\n" +
                            "      values: [last(price)]\n" +
                            "      filter: symbol ~ BTC-USD\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: trades\n" +
                            "              intervals: [(\"2024-08-11T10:13:00.000000Z\",\"2024-08-11T10:15:59.999999Z\")]\n");
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingLastFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, LAST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, LAST(ts) LAST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [last(ts)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingMaxFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MAX(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, MAX(ts) MAX from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [max(ts)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingMinFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MIN(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, MIN(ts) MIN from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [min(ts)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testSelectingMultipleColumnsIncludingFirstFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, FIRST(ts) FIRST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    "Async Group By workers: 1\n" +
                            "  keys: [x]\n" +
                            "  values: [first(ts)]\n" +
                            "  filter: null\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y (x int, ts timestamp) timestamp(ts) partition by day;");
            final String queryA = "select count_distinct(x) from y;";
            final String queryB = "select count(distinct x) from y;";
            String expectedModel = "select-group-by count() count_distinct from (select-group-by x from (select [x] from y timestamp (ts) where null != x))";
            assertEquals(
                    expectedModel,
                    compileModel(queryA).toString0()
            );
            String expectedPlan = "Count\n" +
                    "    Async JIT Group By workers: 1\n" +
                    "      keys: [x]\n" +
                    "      filter: x!=null\n" +
                    "        PageFrame\n" +
                    "            Row forward scan\n" +
                    "            Frame forward scan on: y\n";
            assertPlanNoLeakCheck(
                    queryA,
                    expectedPlan
            );
            assertPlanNoLeakCheck(
                    queryB,
                    expectedPlan
            );
        });
    }

    @Test
    public void testUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y union select LAST(ts) from y union select min(ts) from y  union select max(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts))" +
                            " limit 1 union select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by " +
                            "LAST desc limit 1 union select-choose [ts min] ts min from (select [ts] from y timestamp (ts)) " +
                            "limit 1 union select-choose [ts max] ts max from (select [ts] from y timestamp (ts)) order by" +
                            " max desc limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "Union\n" +
                            "    Union\n" +
                            "        Union\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: y\n" +
                            "            Limit lo: 1\n" +
                            "                SelectedRecord\n" +
                            "                    PageFrame\n" +
                            "                        Row backward scan\n" +
                            "                        Frame backward scan on: y\n" +
                            "        Limit lo: 1\n" +
                            "            SelectedRecord\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y\n" +
                            "    Limit lo: 1\n" +
                            "        SelectedRecord\n" +
                            "            PageFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testUnionQueryOnSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y (x int, z int);");
            final String queryA = "select count_distinct(x) from y union select count_distinct(z) from y;";
            final String queryB = "select count(distinct x) from y union select count_distinct(z) from y;";
            String expectedModel = "select-group-by [count() count_distinct] count() count_distinct from (select-group-by x from (select [x] from y where null != x)) " +
                    "union " +
                    "select-group-by [count() count_distinct] count() count_distinct from (select-group-by z from (select [z] from y where null != z))";
            assertEquals(
                    expectedModel,
                    compileModel(queryA).toString0()
            );
            assertEquals(
                    expectedModel,
                    compileModel(queryB).toString0()
            );
            String expectedPlan = "Union\n" +
                    "    Count\n" +
                    "        Async JIT Group By workers: 1\n" +
                    "          keys: [x]\n" +
                    "          filter: x!=null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: y\n" +
                    "    Count\n" +
                    "        Async JIT Group By workers: 1\n" +
                    "          keys: [z]\n" +
                    "          filter: z!=null\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: y\n";
            assertPlanNoLeakCheck(
                    queryA,
                    expectedPlan
            );
            assertPlanNoLeakCheck(
                    queryB,
                    expectedPlan
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithFirstAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by FIRST(ts) FIRST from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [first(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithLastAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by LAST(ts) LAST from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [last(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithMaxAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by MAX(ts) MAX from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [max(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithMinAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by MIN(ts) MIN from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [min(ts)]\n" +
                            "    SelectedRecord\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: x=3\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseWithFirstAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts FIRST from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseWithLastAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts LAST from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                            "by LAST desc limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseWithMaxAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts MAX from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                            "by MAX desc limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: y\n"
            );
        });
    }

    @Test
    public void testWhereClauseWithMinAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts MIN from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 1\n" +
                            "      filter: x=3\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n"
            );
        });
    }

    protected QueryModel compileModel(String query) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            assertEquals(model.getModelType(), ExecutionModel.QUERY);
            return (QueryModel) model;
        }
    }
}
