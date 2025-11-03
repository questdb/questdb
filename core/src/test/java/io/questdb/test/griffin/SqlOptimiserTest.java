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
import java.util.Arrays;
import java.util.List;

import static io.questdb.griffin.SqlOptimiser.aliasAppearsInFuncArgs;
import static org.junit.Assert.assertEquals;

public class SqlOptimiserTest extends AbstractSqlParserTest {
    private static final String orderByAdviceDdl = """
            CREATE TABLE t (
              s SYMBOL index,
              ts TIMESTAMP
            ) timestamp(ts) PARTITION BY DAY;""";
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
    private static final List<String> outerJoinTypes = Arrays.asList("left", "right", "full");
    private static final String tradesDdl = """
            CREATE TABLE 'trades' (
              symbol SYMBOL,
              side SYMBOL,
              price DOUBLE,
              amount DOUBLE,
              timestamp TIMESTAMP
            ) timestamp (timestamp) PARTITION BY DAY WAL;""";
    private static final String tripsDdl = """
            
            CREATE TABLE 'trips' (
              cab_type SYMBOL capacity 12,
              vendor_id SYMBOL capacity 8,
              pickup_datetime TIMESTAMP
            ) timestamp (pickup_datetime) PARTITION BY MONTH WAL;""";

    @Test
    public void testAliasAppearsInFuncArgs1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [x1]
                              values: [sum(x1)]
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select concat(lpad(x1::string, 5)), x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals(
                    "select-group-by concat(lpad(x1::string, 5)) concat, x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [concat,x1]
                              values: [sum(x1)]
                              filter: null
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select concat(lpad(x1::string, 5)), x1 from (select x x1 from y) group by x1";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-virtual concat(lpad(x1::string, 5)) concat, x1 from (select-group-by [x1] x1 from (select-choose [x x1] x x1 from (select [x] from y)))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [concat([lpad(x1::string,5)]),x1]
                                GroupBy vectorized: true workers: 1
                                  keys: [x1]
                                  values: [count(*)]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs4() throws Exception {
        // check aliases are case-insensitive
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select x1, sum(x1), max(X1) from (select x X1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum, max(x1) max from (select-choose [x X1] x X1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [X1]
                              values: [sum(X1),max(X1)]
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs5() throws Exception {
        // test function on its own is caught
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              values: [sum(x1)]
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs6() throws Exception {
        // test that col on its own works
        assertMemoryLeak(() -> {
            execute("create table y ( x int );");
            final String query = "select x1 from (select x x1 from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose x1 from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert !aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testAliasForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testAliasForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testAliasForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testAliasForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testConstantInGroupByDoesNotPreventOptimisation() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table hits (
                      URL string, ts timestamp
                    ) timestamp(ts) partition by day wal""");

            execute("insert into hits (URL, ts) values ('abc', 0), ('abc', 1), ('def', 2), ('ghi', 3)");
            drainWalQueue();

            String q1 = "SELECT 1, URL, COUNT(*) AS c FROM hits ORDER BY c DESC LIMIT 10;";
            String q2 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;";
            String q3 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY URL, 1 ORDER BY c DESC LIMIT 10;";
            String q4 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, 2 ORDER BY c DESC LIMIT 10;";

            String expectedSql = """
                    1\tURL\tc
                    1\tabc\t2
                    1\tdef\t1
                    1\tghi\t1
                    """;
            String expectedPlan = """
                    Long Top K lo: 10
                      keys: [c desc]
                        VirtualRecord
                          functions: [1,URL,c]
                            Async Group By workers: 1
                              keys: [URL]
                              values: [count(*)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: hits
                    """;

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
    public void testDuplicateColumnsInWindowModel() throws SqlException {
        execute("create table cpu_ts ( hostname symbol, usage_system double, ts timestamp) timestamp(ts);");
        execute("insert into cpu_ts select rnd_symbol('A', 'B', 'C'), x, x::timestamp from long_sequence(3)");
        String q1 = "select rank() over(), t1.usage_system, t1.usage_system from cpu_ts t1 join cpu_ts t2 on t1.ts > t2.ts";

        assertPlanNoLeakCheck(
                q1,
                """
                        Window
                          functions: [rank() over ()]
                            SelectedRecord
                                Filter filter: t2.ts<t1.ts
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                rank\tusage_system\tusage_system1
                1\t2.0\t2.0
                1\t3.0\t3.0
                1\t3.0\t3.0
                """, q1);

        String q2 = "select rank() over(partition by t1.hostname order by t1.ts), t2.usage_system, t2.usage_system from cpu_ts t1 join cpu_ts t2 on t1.ts > t2.ts";

        assertPlanNoLeakCheck(
                q2,
                """
                        Window
                          functions: [rank() over (partition by [hostname])]
                            SelectedRecord
                                Filter filter: t2.ts<t1.ts
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                rank\tusage_system\tusage_system1
                1\t1.0\t1.0
                1\t1.0\t1.0
                1\t2.0\t2.0
                """, q2);

        // useInnerModel
        String q3 = "select rank() over(partition by t1.hostname order by t1.ts), t2.usage_system, t2.usage_system, t1.usage_system + 10 from cpu_ts t1 join cpu_ts t2 on t1.ts > t2.ts";

        assertPlanNoLeakCheck(
                q3,
                """
                        Window
                          functions: [rank() over (partition by [hostname])]
                            VirtualRecord
                              functions: [hostname,ts,usage_system,usage_system1+10]
                                SelectedRecord
                                    Filter filter: t2.ts<t1.ts
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: cpu_ts
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                rank\tusage_system\tusage_system1\tcolumn
                1\t1.0\t1.0\t12.0
                1\t1.0\t1.0\t13.0
                1\t2.0\t2.0\t13.0
                """, q3);
    }

    @Test
    public void testFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts FIRST from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testFirstAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by FIRST(x) FIRST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              values: [first(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testJoinAndUnionQueryWithJoinOnDesignatedTimestampColumnWithLastFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            execute("create table y1 ( x int, ts timestamp) timestamp(ts);");
            execute("create table y2 ( x int, ts timestamp) timestamp(ts);");
            for (String joinType : outerJoinTypes) {
                final String query = ("""
                        select  * from y\s
                        #JOIN_TYPE join\s
                        y1 on\s
                        y1.x = y.x
                        INNER join (select LAST(ts) from y2) as y2\s
                        on y2.LAST = y1.ts""").replaceAll("#JOIN_TYPE", joinType);
                String queryNew = query + " union \n" + query;
                final QueryModel model = compileModel(queryNew);
                assertEquals(
                        ("select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x, " +
                                "y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) #JOIN_TYPE join " +
                                "select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from (select-choose " +
                                "[ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc limit 1) y2 on " +
                                "y2.LAST = y1.ts) union select-choose [y.x x, y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST] y.x x," +
                                " y.ts ts, y1.x x1, y1.ts ts1, y2.LAST LAST from (select [x, ts] from y timestamp (ts) " +
                                "#JOIN_TYPE join select [x, ts] from y1 timestamp (ts) on y1.x = y.x join select [LAST] from " +
                                "(select-choose [ts LAST] ts LAST from (select [ts] from y2 timestamp (ts)) order by LAST desc " +
                                "limit 1) y2 on y2.LAST = y1.ts)").replaceAll("#JOIN_TYPE", joinType),
                        model.toString0()
                );
                // TODO: there's a forward scan on y2 whereas it should be a backward scan;
                //       it could have something to do with SqlOptimiser.optimiseOrderBy()
                assertPlanNoLeakCheck(
                        query,
                        "SelectedRecord\n" +
                                "    Hash Join Light\n" +
                                "      condition: y2.LAST=y1.ts\n" +
                                "        Hash #JOIN_TYPE Outer Join Light\n".replaceAll("#JOIN_TYPE", Character.toUpperCase(joinType.charAt(0)) + joinType.substring(1)) +
                                "          condition: y1.x=y.x\n" +
                                "            PageFrame\n" +
                                "                Row forward scan\n" +
                                "                Frame forward scan on: y\n" +
                                "            Hash\n" +
                                "                PageFrame\n" +
                                "                    Row forward scan\n" +
                                "                    Frame forward scan on: y1\n" +
                                "        Hash\n" +
                                "            Async Top K lo: 1 workers: 1\n" +
                                "              filter: null\n" +
                                "              keys: [LAST desc]\n" +
                                "                SelectedRecord\n" +
                                "                    PageFrame\n" +
                                "                        Row forward scan\n" +
                                "                        Frame forward scan on: y2\n"
                );
            }
        });
    }

    @Test
    public void testJoinWithSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (x long, ts timestamp) timestamp(ts);");
            execute("create table y1 (x long, ts timestamp) timestamp(ts);");
            final String queryA = """
                    select * from y\s
                    inner join (select count_distinct(x) c from y1) as y1\s
                    on y.x = y1.c""";
            final String queryB = """
                    select * from y\s
                    inner join (select count(distinct x) c from y1) as y1\s
                    on y.x = y1.c""";

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

            String expectedPlan = """
                    SelectedRecord
                        Hash Join
                          condition: y1.c=y.x
                            PageFrame
                                Row forward scan
                                Frame forward scan on: y
                            Hash
                                Count
                                    Async JIT Group By workers: 1
                                      keys: [x]
                                      filter: x!=null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y1
                    """;
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
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testLastAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by LAST(x) LAST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              values: [last(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select max(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts max from (select [ts] from y timestamp (ts)) order by max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testMaxAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by MAX(x) MAX from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              values: [max(x)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select min(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose ts min from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testMinAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(x) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by MIN(x) MIN from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              values: [min(x)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNestedFirstFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNestedLastFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select LAST(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose LAST from " +
                            "(select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1)",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNestedMaxFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MAX(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose MAX from " +
                            "(select-choose [ts MAX] ts MAX from (select [ts] from y timestamp (ts)) order by MAX desc limit 1)",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNestedMinFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MIN(ts) from y)";
            final QueryModel model = compileModel(query);
            assertEquals("select-choose MIN from (select-choose [ts MIN] ts MIN from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNestedUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
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
                    """
                            Union
                                Union
                                    Union
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: y
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: y
                                    Limit lo: 1 skip-over-rows: 0 limit: 0
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                Limit lo: 1 skip-over-rows: 0 limit: 0
                                    SelectedRecord
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromDifferentTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab1 (
                        id int,
                        a int
                      );
                    """);

            execute("""
                    create table tab2 (
                        id int,
                        b int
                      );""");

            assertPlanNoLeakCheck(
                    """
                            select a, b
                                        from tab1 join tab2 on tab1.id = tab2.id
                                        order by a, b""",
                    """
                            Sort
                              keys: [a, b]
                                SelectedRecord
                                    Hash Join Light
                                      condition: tab2.id=tab1.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab1
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab2
                            """
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromOneTableWithOrderingAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab1 (
                        id int,
                        a int,\
                        ts timestamp
                      ) timestamp(ts);
                    """);

            execute("""
                    create table tab2 (
                        id int,
                        b int,\
                        ts timestamp
                      ) timestamp(ts);""");

            // No top level sort needed, sort is by tab1.ts
            assertPlanNoLeakCheck(
                    """
                            select tab1.id, tab1.ts as b
                            from tab1 join tab2 on tab1.id = tab2.id
                            order by b""",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: tab2.id=tab1.id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab2
                            """
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromOneTableWithOrderingPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab1 (
                        id int,
                        a int,\
                        ts timestamp
                      ) timestamp(ts);
                    """);

            execute("""
                    create table tab2 (
                        id int,
                        b int,\
                        ts timestamp
                      ) timestamp(ts);""");

            // No top level sort needed, sort is by tab1.ts
            assertPlanNoLeakCheck(
                    """
                            select tab1.id, tab1.ts as b
                            from tab1 join tab2 on tab1.id = tab2.id
                            order by 2""",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: tab2.id=tab1.id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab2
                            """
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromTheSameTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab1 (
                        id int,
                        a int
                      );
                    """);

            execute("""
                    create table tab2 (
                        id int,
                        b int
                      );""");

            assertPlanNoLeakCheck(
                    """
                            select a, b
                            from tab1 join tab2 on tab1.id = tab2.id
                            order by a desc \
                            limit 10""",
                    """
                            Limit lo: 10 skip-over-rows: 0 limit: 10
                                Sort
                                  keys: [a desc]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tab2.id=tab1.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab1
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab2
                            """
            );
        });
    }

    @Test
    public void testNonPrefixedAdviceFromTheSameTableWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab1 (
                        id int,
                        a int,
                        ts timestamp
                      ) timestamp(ts);
                    """);

            execute("""
                    create table tab2 (
                        id int,
                        b int
                      );""");

            assertPlanNoLeakCheck(
                    """
                            select a, b
                            from tab1 join tab2 on tab1.id = tab2.id
                            order by a desc, ts \
                            limit 10""",
                    """
                            SelectedRecord
                                Limit lo: 10 skip-over-rows: 0 limit: 10
                                    Sort
                                      keys: [a desc, ts]
                                        SelectedRecord
                                            Hash Join Light
                                              condition: tab2.id=tab1.id
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab1
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tab2
                            """
            );
        });
    }

    @Test
    public void testOrderByAdviceWithMultipleJoinsAndFilters() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'WorkflowEvent' (
                      CreateDate timestamp,
                      Id uuid,
                      TenantId int,
                      UserId int,
                      EventTypeId int
                    ) timestamp (CreateDate) partition by hour wal;""");

            execute("""
                    CREATE TABLE 'WorkflowEventAction' (
                      CreateDate TIMESTAMP,
                      WorkflowEventId UUID,
                      ActionTypeId INT,
                      Message STRING
                    ) timestamp (CreateDate) PARTITION BY HOUR WAL;""");

            execute("insert into WorkflowEventAction (CreateDate, WorkflowEventId, ActionTypeId, Message) values" +
                    " ('2016-01-01T00:00:00Z', to_uuid(1, 1), 13, '2')");
            execute("insert into WorkflowEvent (CreateDate, Id, TenantId, UserId, EventTypeId) values ('2016-01-01T00:00:00Z', to_uuid(1, 1), 24024, 19, 1)");
            drainWalQueue();

            assertPlanNoLeakCheck(
                    """
                            SELECT  1
                            FROM    WorkflowEvent el
                            
                            LEFT JOIN WorkflowEventAction ep0
                              ON    el.CreateDate = ep0.CreateDate
                              and   el.Id = ep0.WorkflowEventId
                              and   ep0.ActionTypeId = 13
                              and   ep0.Message = '2'
                            
                            LEFT JOIN    WorkflowEventAction ep
                              on    el.CreateDate = ep.CreateDate
                              and   el.Id = ep.WorkflowEventId
                              and   ep.ActionTypeId = 8
                            
                            WHERE   el.UserId = 19
                              and   el.TenantId = 24024
                              and   el.EventTypeId = 1
                              and   el.CreateDate >= to_timestamp('2016-01-01T00:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')
                              and   el.CreateDate <= to_timestamp('2016-01-01T10:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')""",
                    """
                            VirtualRecord
                              functions: [1]
                                Hash Left Outer Join Light
                                  condition: ep.WorkflowEventId=el.Id and ep.CreateDate=el.CreateDate
                                  filter: ep.ActionTypeId=8
                                    Hash Left Outer Join Light
                                      condition: ep0.WorkflowEventId=el.Id and ep0.CreateDate=el.CreateDate
                                      filter: (ep0.ActionTypeId=13 and ep0.Message='2')
                                        Empty table
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: WorkflowEventAction
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: WorkflowEventAction
                            """
            );

            assertPlanNoLeakCheck(
                    """
                            SELECT  1
                            FROM    WorkflowEvent el
                            
                            Right JOIN WorkflowEventAction ep0
                              ON    el.CreateDate = ep0.CreateDate
                              and   el.Id = ep0.WorkflowEventId
                              and   ep0.ActionTypeId = 13
                              and   ep0.Message = '2'
                            
                            Right JOIN    WorkflowEventAction ep
                              on    el.CreateDate = ep.CreateDate
                              and   el.Id = ep.WorkflowEventId
                              and   ep.ActionTypeId = 8
                            
                            WHERE   el.UserId = 19
                              and   el.TenantId = 24024
                              and   el.EventTypeId = 1
                              and   el.CreateDate >= to_timestamp('2016-01-01T00:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')
                              and   el.CreateDate <= to_timestamp('2016-01-01T10:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')""",
                    """
                            VirtualRecord
                              functions: [1]
                                Hash Right Outer Join Light
                                  condition: ep.WorkflowEventId=el.Id and ep.CreateDate=el.CreateDate
                                  filter: ep.ActionTypeId=8
                                    Hash Right Outer Join Light
                                      condition: ep0.WorkflowEventId=el.Id and ep0.CreateDate=el.CreateDate
                                      filter: (ep0.ActionTypeId=13 and ep0.Message='2')
                                        Empty table
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: WorkflowEventAction
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: WorkflowEventAction
                            """
            );

            assertPlanNoLeakCheck(
                    """
                            SELECT  1
                            FROM    WorkflowEvent el
                            
                            FULL JOIN WorkflowEventAction ep0
                              ON    el.CreateDate = ep0.CreateDate
                              and   el.Id = ep0.WorkflowEventId
                              and   ep0.ActionTypeId = 13
                              and   ep0.Message = '2'
                            
                            FULL JOIN    WorkflowEventAction ep
                              on    el.CreateDate = ep.CreateDate
                              and   el.Id = ep.WorkflowEventId
                              and   ep.ActionTypeId = 8
                            
                            WHERE   el.UserId = 19
                              and   el.TenantId = 24024
                              and   el.EventTypeId = 1
                              and   el.CreateDate >= to_timestamp('2016-01-01T00:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')
                              and   el.CreateDate <= to_timestamp('2016-01-01T10:00:00Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')""",
                    """
                            VirtualRecord
                              functions: [1]
                                Hash Full Outer Join Light
                                  condition: ep.WorkflowEventId=el.Id and ep.CreateDate=el.CreateDate
                                  filter: ep.ActionTypeId=8
                                    Hash Full Outer Join Light
                                      condition: ep0.WorkflowEventId=el.Id and ep0.CreateDate=el.CreateDate
                                      filter: (ep0.ActionTypeId=13 and ep0.Message='2')
                                        Empty table
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: WorkflowEventAction
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: WorkflowEventAction
                            """
            );

            assertQuery("select-virtual 1 1 from (select [Id, CreateDate, UserId, TenantId, EventTypeId] from WorkflowEvent el timestamp (CreateDate) join (select [WorkflowEventId, CreateDate, ActionTypeId, Message] from WorkflowEventAction ep0 timestamp (CreateDate) where ActionTypeId = 13 and Message = '2') ep0 on ep0.WorkflowEventId = el.Id and ep0.CreateDate = el.CreateDate join (select [WorkflowEventId, CreateDate, ActionTypeId] from WorkflowEventAction ep timestamp (CreateDate) where ActionTypeId = 8) ep on ep.WorkflowEventId = el.Id and ep.CreateDate = el.CreateDate where UserId = 19 and TenantId = 24024 and EventTypeId = 1 and CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ') and CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')) el", """
                    SELECT  1
                    FROM    WorkflowEvent el
                    
                    JOIN    WorkflowEventAction ep0
                      ON    el.CreateDate = ep0.CreateDate
                      and   el.Id = ep0.WorkflowEventId
                      and   ep0.ActionTypeId = 13
                      and   ep0.Message = '2'
                    
                    join    WorkflowEventAction ep
                      on    el.CreateDate = ep.CreateDate
                      and   el.Id = ep.WorkflowEventId
                      and   ep.ActionTypeId = 8
                    
                    WHERE   el.UserId = 19
                      and   el.TenantId = 24024
                      and   el.EventTypeId = 1
                      and   el.CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')
                      and   el.CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')""");

            assertSql("1\n", """
                    SELECT  1
                    FROM    WorkflowEvent el
                    
                    JOIN    WorkflowEventAction ep0
                      ON    el.CreateDate = ep0.CreateDate
                      and   el.Id = ep0.WorkflowEventId
                      and   ep0.ActionTypeId = 13
                      and   ep0.Message = '2'
                    
                    JOIN    WorkflowEventAction ep
                      on    el.CreateDate = ep.CreateDate
                      and   el.Id = ep.WorkflowEventId
                      and   ep.ActionTypeId = 8
                    
                    WHERE   el.UserId = 19
                      and   el.TenantId = 24024
                      and   el.EventTypeId = 1
                      and   el.CreateDate >= to_timestamp('2024-01-26T18:26:14.000000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')\s
                      and   el.CreateDate <= to_timestamp('2024-01-26T18:47:49.994262Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')""");


            assertSql("""
                            CreateDate\tId\tTenantId\tUserId\tEventTypeId\tCreateDate1\tWorkflowEventId\tActionTypeId\tMessage\tCreateDate2\tWorkflowEventId1\tActionTypeId1\tMessage1
                            2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t24024\t19\t1\t2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t13\t2\t\t\tnull\t
                            """,

                    """
                            SELECT  *
                            FROM    WorkflowEvent el
                            
                            LEFT JOIN WorkflowEventAction ep0
                              ON    el.CreateDate = ep0.CreateDate
                              and   el.Id = ep0.WorkflowEventId
                              and   ep0.ActionTypeId = 13
                              and   ep0.Message = '2'
                            
                            LEFT JOIN WorkflowEventAction ep
                              on    el.CreateDate = ep.CreateDate
                              and   el.Id = ep.WorkflowEventId
                              and   ep.ActionTypeId = 8
                            
                            WHERE   el.UserId = 19
                              and   el.TenantId = 24024
                              and   el.EventTypeId = 1
                              and   el.CreateDate >= '2016-01-01T00:00:00Z'
                              and   el.CreateDate <= '2016-01-01T10:00:00Z'""");

            assertSql("""
                            CreateDate\tId\tTenantId\tUserId\tEventTypeId\tCreateDate1\tWorkflowEventId\tActionTypeId\tMessage
                            2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t24024\t19\t1\t2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t13\t2
                            """,

                    """
                            SELECT  *
                            FROM    WorkflowEvent el
                            
                            RIGHT JOIN WorkflowEventAction ep0
                              ON    el.CreateDate = ep0.CreateDate
                              and   el.Id = ep0.WorkflowEventId
                              and   ep0.ActionTypeId = 13
                              and   ep0.Message = '2'
                            
                            WHERE   el.UserId = 19
                              and   el.TenantId = 24024
                              and   el.EventTypeId = 1
                              and   el.CreateDate >= '2016-01-01T00:00:00Z'
                              and   el.CreateDate <= '2016-01-01T10:00:00Z'""");
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin1() throws Exception {
        // Case when order by is one table and not timestamp first
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    ASOF JOIN t2 ON t1.s = t2.s
                    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts]
                                    SelectedRecord
                                        AsOf Join Fast Scan
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T00:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin2() throws Exception {
        // Case when order by is one table and timestamp first
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    ASOF JOIN t2 ON t1.s = t2.s
                    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.ts, t1.s
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by ts, s limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [ts, s]
                                    SelectedRecord
                                        AsOf Join Fast Scan
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T00:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin3() throws Exception {
        // Case when order by is for more than one table prefix
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                        FROM t1
                        ASOF JOIN t2 ON t1.s = t2.s
                        WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'
                        ORDER BY t1.s, t2.ts
                        LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts1]
                                    SelectedRecord
                                        AsOf Join Fast Scan
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T00:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin4() throws Exception {
        // Case when ordering by secondary table
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    ASOF JOIN t2 ON t1.s = t2.s
                    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'
                    ORDER BY t2.s, t2.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s1, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s1, ts1]
                                    SelectedRecord
                                        AsOf Join Fast Scan
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T00:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsOfJoin5() throws Exception {
        // Case when order by is one table and not timestamp first
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    ASOF JOIN t2 ON t1.s = t2.s
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) asof join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts]
                                    SelectedRecord
                                        AsOf Join Fast Scan
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1a() throws Exception {
        // case when ordering by symbol, then timestamp - we expect to use the symbol index
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts in '2023-09-01T00:00:00.000Z' and ts <= '2023-09-01T01:00:00.000Z') order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                SelectedRecord
                                    Cross Join
                                        SortedSymbolIndex
                                            Index forward scan on: s
                                              symbolOrder: asc
                                            Interval forward scan on: t1
                                              intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T00:00:00.000000Z")]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1b() throws Exception {
        // case when ordering by symbol, then timestamp - we expect to use the symbol index
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                SelectedRecord
                                    Cross Join
                                        SortedSymbolIndex
                                            Index forward scan on: s
                                              symbolOrder: asc
                                            Interval forward scan on: t1
                                              intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t2
                            """
            );
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin1c() throws Exception {
        // case when by columns from both tables - expect it to use the sort
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts, t2.ts, t2.s
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s, ts, ts1, s1 limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts, ts1, s1]
                                    SelectedRecord
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin2() throws Exception {
        // case when ordering by just symbol - we expect to use the symbol index
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                SelectedRecord
                                    Cross Join
                                        SortedSymbolIndex
                                            Index forward scan on: s
                                              symbolOrder: asc
                                            Interval forward scan on: t1
                                              intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin3() throws Exception {
        // case when ordering by timestamp, then symbol - we expect to not use the symbol index
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.ts, t1.s
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by ts, s limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [ts, s]
                                    SelectedRecord
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithCrossJoin4() throws Exception {
        // case when ordering by timestamp, then symbol - we expect to not use the symbol index
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    CROSS JOIN t2
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.ts, t2.s, t1.s, t2.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) cross join select [s, ts] from t2 timestamp (ts) where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by ts, s1, s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [ts, s1, s, ts1]
                                    SelectedRecord
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """
            );
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:10:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:15:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithLtJoin() throws Exception {
        // Case when order by is for more than one table prefix
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                        FROM t1
                        LT JOIN t2 ON t1.s = t2.s
                        WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                        ORDER BY t1.s, t2.ts
                        LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) lt join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts1]
                                    SelectedRecord
                                        Lt Join Light
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """);
            assertSql("""
                    s\tts\ts1\tts1
                    a\t2023-09-01T00:00:00.000000Z\t\t
                    a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                    a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                    b\t2023-09-01T00:05:00.000000Z\t\t
                    b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                    b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                    c\t2023-09-01T01:00:00.000000Z\t\t
                    """, query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithRegularJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));
            execute(orderByAdviceDdl.replace(" t ", " t2 "));
            execute(orderByAdviceDml.replace(" t ", " t1 "));
            execute(orderByAdviceDml.replace(" t ", " t2 "));

            final String query = """
                    SELECT t1.s, t1.ts, t2.s, t2.ts
                    FROM t1
                    JOIN t2 ON t1.s = t2.s
                    WHERE t1.ts BETWEEN '2023-09-01T00:00:00.000Z' AND '2023-09-01T01:00:00.000Z'
                    ORDER BY t1.s, t1.ts, t2.ts
                    LIMIT 1000000;""";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z', '2023-09-01T01:00:00.000Z')) order by s, ts, ts1 limit 1000000", query);
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1000000 skip-over-rows: 0 limit: 1000000
                                Sort
                                  keys: [s, ts, ts1]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: t2.s=t1.s
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t1
                                                  intervals: [("2023-09-01T00:00:00.000000Z","2023-09-01T01:00:00.000000Z")]
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t2
                            """
            );
            assertSql(
                    """
                            s\tts\ts1\tts1
                            a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                            a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                            a\t2023-09-01T00:00:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                            a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                            a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                            a\t2023-09-01T00:10:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                            a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:00:00.000000Z
                            a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:10:00.000000Z
                            a\t2023-09-01T00:20:00.000000Z\ta\t2023-09-01T00:20:00.000000Z
                            b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                            b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                            b\t2023-09-01T00:05:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                            b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                            b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                            b\t2023-09-01T00:15:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                            b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:05:00.000000Z
                            b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:15:00.000000Z
                            b\t2023-09-01T00:25:00.000000Z\tb\t2023-09-01T00:25:00.000000Z
                            c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T01:00:00.000000Z
                            c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T02:00:00.000000Z
                            c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z
                            """,
                    query
            );
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin1() throws Exception {
        // Case when no join
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = """
                    SELECT s, ts
                        FROM t1
                        ORDER BY t1.s, t1.ts
                        LIMIT 1000000;""";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by s, ts limit 1000000", query);
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Top K lo: 1000000 workers: 1
                              filter: null
                              keys: [s, ts]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t1
                            """
            );

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin2() throws Exception {
        // Test with reverse limit, expect sort due to symbol first
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = """
                    SELECT s, ts
                        FROM t1
                        ORDER BY t1.s, t1.ts
                        LIMIT -10;""";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by s, ts limit -(10)", query);
            assertPlanNoLeakCheck(query, """
                    Sort light lo: -10
                      keys: [s, ts]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: t1
                    """);

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    @Test
    public void testOrderByAdviceWorksWithoutJoin3() throws Exception {
        // Test with reverse limit
        assertMemoryLeak(() -> {
            execute(orderByAdviceDdl.replace(" t ", " t1 "));

            final String query = """
                    SELECT s, ts
                        FROM t1
                        ORDER BY t1.ts, t1.s
                        LIMIT -10;""";

            assertQuery("select-choose s, ts from (select [s, ts] from t1 timestamp (ts)) order by ts, s limit -(10)", query);
            assertPlanNoLeakCheck(query, """
                    Sort light lo: -10 partiallySorted: true
                      keys: [ts, s]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: t1
                    """);

            Misc.free(select(query, sqlExecutionContext));
        });
    }

    // issue 6256
    @Test
    public void testOrderByJoinCursorFuncNpe() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP);");
            execute("insert into tango values(100000)");
            execute("insert into tango values(100001)");
            assertQueryNoLeakCheck("""
                            column
                            1970-01-01T00:00:00.100001Z
                            1970-01-01T00:00:00.100002Z
                            """,
                    "SELECT ts + x  FROM tango CROSS JOIN (SELECT x FROM long_sequence(1)) ORDER BY x;",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderByNotChooseByParent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (f1 float, f2 float, ts timestamp) timestamp(ts)");
            execute("insert into tab VALUES(1, 10, '2024-12-24T00:11:00.000Z'), (2, 20, '2024-12-24T00:11:00.000Z')");
            String q1 = "select f2 - f1 as p1, f1, f2 from tab order by ts desc";
            assertPlanNoLeakCheck(q1, """
                    SelectedRecord
                        VirtualRecord
                          functions: [f2-f1,f1,f2,ts]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                    """);
            assertQueryNoLeakCheck(
                    """
                            p1\tf1\tf2
                            18.0\t2.0\t20.0
                            9.0\t1.0\t10.0
                            """,
                    q1
            );

            String q2 = "select f2 - f1, f1, f2 from tab order by ts desc";
            assertPlanNoLeakCheck(q2, """
                    SelectedRecord
                        VirtualRecord
                          functions: [f2-f1,f1,f2,ts]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                    """);
            assertQueryNoLeakCheck("""
                    column\tf1\tf2
                    18.0\t2.0\t20.0
                    9.0\t1.0\t10.0
                    """, q2);
        });
    }

    @Test
    public void testOrderingOfSortsInSingleTimestampCase() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x::int as i, x::timestamp as ts from long_sequence(10000)");

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
                    """
                            Limit lo: 10 skip-over-rows: 0 limit: 10
                                SelectedRecord
                                    Cross Join
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """
            );

            assertQueryNoLeakCheck("""
                            i\tts\ti1\tts1
                            10000\t1970-01-01T00:00:00.010000Z\t1\t1970-01-01T00:00:00.000001Z
                            10000\t1970-01-01T00:00:00.010000Z\t2\t1970-01-01T00:00:00.000002Z
                            10000\t1970-01-01T00:00:00.010000Z\t3\t1970-01-01T00:00:00.000003Z
                            10000\t1970-01-01T00:00:00.010000Z\t4\t1970-01-01T00:00:00.000004Z
                            10000\t1970-01-01T00:00:00.010000Z\t5\t1970-01-01T00:00:00.000005Z
                            10000\t1970-01-01T00:00:00.010000Z\t6\t1970-01-01T00:00:00.000006Z
                            10000\t1970-01-01T00:00:00.010000Z\t7\t1970-01-01T00:00:00.000007Z
                            10000\t1970-01-01T00:00:00.010000Z\t8\t1970-01-01T00:00:00.000008Z
                            10000\t1970-01-01T00:00:00.010000Z\t9\t1970-01-01T00:00:00.000009Z
                            10000\t1970-01-01T00:00:00.010000Z\t10\t1970-01-01T00:00:00.000010Z
                            """,
                    "select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts desc " +
                            " limit 10" +
                            ") " +
                            "order by ts desc",
                    "ts###desc",
                    false,
                    true
            );
        });
    }

    @Test
    public void testPushDownLimitFromChooseToNone() throws Exception {
        assertMemoryLeak(() -> {
            execute(tripsDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select cab_type, vendor_id, pickup_datetime from trips \n" +
                    "order by pickup_datetime desc, cab_type desc limit 100;", """
                    Sort light lo: 100 partiallySorted: true
                      keys: [pickup_datetime desc, cab_type desc]
                        PageFrame
                            Row backward scan
                            Frame backward scan on: trips
                    """);
        });
    }

    @Test
    public void testPushDownLimitFromChooseToNoneWithHiLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(tripsDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select cab_type, vendor_id, pickup_datetime from trips \n" +
                    "order by pickup_datetime desc, cab_type desc limit 100, 110;", """
                    Sort light lo: 100 hi: 110
                      keys: [pickup_datetime desc, cab_type desc]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: trips
                    """);
        });
    }

    @Test
    public void testQueryPlanForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts FIRST from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForFirstAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by FIRST(x) FIRST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              values: [first(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForJoinAndUnionQueryWithJoinOnDesignatedTimestampColumnWithLastFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            execute("create table y1 ( x int, ts timestamp) timestamp(ts);");
            execute("create table y2 ( x int, ts timestamp) timestamp(ts);");
            final String query = """
                    select  * from y\s
                    left join\s
                    y1 on\s
                    y1.x = y.x
                    INNER join (select LAST(ts) from y2) as y2\s
                    on y2.LAST = y1.ts""";
            String queryNew = query + " union \n" + query;
            final QueryModel model = compileModel(queryNew);
            TestUtils.assertEquals(
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
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: y2.LAST=y1.ts
                                    Hash Left Outer Join Light
                                      condition: y1.x=y.x
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y1
                                    Hash
                                        Async Top K lo: 1 workers: 1
                                          filter: null
                                          keys: [LAST desc]
                                            SelectedRecord
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: y2
                            """
            );
        });
    }

    @Test
    public void testQueryPlanForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForLastAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by LAST(x) LAST from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              values: [last(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select max(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts max from (select [ts] from y timestamp (ts)) order by max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForMaxAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MAX(x) MAX from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              values: [max(x)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select min(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts min from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForMinAggregateFunctionOnNonDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(x) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MIN(x) MIN from (select [x] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              values: [min(x)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForNestedFirstFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select FIRST(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose FIRST from (select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForNestedLastFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select LAST(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose LAST from (select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by LAST desc limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForNestedMaxFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MAX(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose MAX from (select-choose [ts MAX] ts MAX from (select [ts] from y timestamp (ts)) order by MAX desc limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForNestedMinFunctionOptimisationOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select * from (select MIN(ts) from y)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose MIN from (select-choose [ts MIN] ts MIN from (select [ts] from y timestamp (ts)) limit 1)", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForNestedUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
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
                    """
                            Union
                                Union
                                    Union
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: y
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: y
                                    Limit lo: 1 skip-over-rows: 0 limit: 0
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                Limit lo: 1 skip-over-rows: 0 limit: 0
                                    SelectedRecord
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingFirstFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, FIRST(ts) FIRST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [x]
                              values: [first(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingLastFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, LAST(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, LAST(ts) LAST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [x]
                              values: [last(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingMaxFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MAX(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, MAX(ts) MAX from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [max(ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForSelectingMultipleColumnsIncludingMinFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MIN(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by x, MIN(ts) MIN from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [min(ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """);

        });
    }

    @Test
    public void testQueryPlanForUnionQueryOnForMinMaxFirstLastOnAggregateTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y union select LAST(ts) from y union select min(ts) from y  union select max(ts) from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose [ts FIRST] ts FIRST from (select [ts] from y timestamp (ts))" +
                    " limit 1 union select-choose [ts LAST] ts LAST from (select [ts] from y timestamp (ts)) order by " +
                    "LAST desc limit 1 union select-choose [ts min] ts min from (select [ts] from y timestamp (ts)) " +
                    "limit 1 union select-choose [ts max] ts max from (select [ts] from y timestamp (ts)) order by" +
                    " max desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Union
                                Union
                                    Union
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: y
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: y
                                    Limit lo: 1 skip-over-rows: 0 limit: 0
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                Limit lo: 1 skip-over-rows: 0 limit: 0
                                    SelectedRecord
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithFirstAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by FIRST(ts) FIRST from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [first(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithLastAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by LAST(ts) LAST from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [last(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithMaxAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MAX(ts) MAX from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [max(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseOnNestedModelWithMinAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-group-by MIN(ts) MIN from (select-choose [ts] x, ts from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [min(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithFirstAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts FIRST from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithLastAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts LAST from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                    "by LAST desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithMaxAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts MAX from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) order " +
                    "by MAX desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanForWhereClauseWithMinAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts MIN from " +
                    "(select [ts, x] from y timestamp (ts) where x = 3) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanWithAliasForFirstAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanWithAliasForLastAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanWithAliasForMaxAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) order by ts1 desc limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """);
        });
    }

    @Test
    public void testQueryPlanWithAliasForMinAggregateFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) as ts1 from y";
            final QueryModel model = compileModel(query);
            TestUtils.assertEquals("select-choose ts ts1 from (select [ts] from y timestamp (ts)) limit 1", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit lo: 1 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """);
        });
    }

    @Test
    public void testRangeFillWhenThereAreNoRecords() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'trades' (
                      symbol SYMBOL capacity 256 CACHE,
                      side SYMBOL capacity 256 CACHE,
                      price DOUBLE,
                      amount DOUBLE,
                      timestamp TIMESTAMP
                    ) timestamp (timestamp) PARTITION BY DAY WAL;""");
            drainWalQueue();

            String query = """
                    with a as (
                    SELECT\s
                        timestamp,
                        vwap(price, amount) AS vwap_price,
                        sum(amount) AS volume
                    FROM trades
                    WHERE symbol = 'MEH' AND timestamp > dateadd('d', -1, now())
                    SAMPLE by 1h FILL(NULL)
                    )
                    select * from a;""";

            assertSql("timestamp\tvwap_price\tvolume\n", query);
        });
    }

    @Test
    public void testRewriteNegativeLimitAndHiLimitAvoidsJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck(
                    """
                            SELECT trades.timestamp, * FROM trades
                            ASOF JOIN (SELECT * from trades) trades2
                             LIMIT -3, -10;""",
                    """
                            Limit lo: -3 hi: -10 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    AsOf Join Fast Scan
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitAvoidsJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck(
                    """
                            SELECT trades.timestamp, * FROM trades
                            ASOF JOIN (SELECT * from trades) trades2
                             LIMIT -3;""",
                    """
                            Limit lo: -3 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    AsOf Join Fast Scan
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitHandleTimestampAndAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck(
                    "select timestamp ts1, timestamp ts2 from trades limit -3",
                    """
                            Limit lo: -3 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """
            );
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesExistingOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck(
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC LIMIT -3;",
                    """
                            Sort light
                              keys: [timestamp, side desc]
                                Sort light lo: 3 partiallySorted: true
                                  keys: [timestamp desc, side]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: trades
                            """
            );
            execute("insert into trades (timestamp, side, symbol) values " +
                    "(0, 'sell', 'abc'), (0, 'buy', 'abc'), (0, 'buy', 'def'), (0, 'buy', 'fgh'), (1, 'sell', 'abc')");
            drainWalQueue();
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tsell
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC; ");
            // last 3 rows of the result set above
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC LIMIT -3;"
            );
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesExistingOrderByAscAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side ASC LIMIT -3;",
                    """
                            Sort light
                              keys: [timestamp, side]
                                Sort light lo: 3 partiallySorted: true
                                  keys: [timestamp desc, side desc]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: trades
                            """);
            execute("insert into trades (timestamp, side, symbol) values " +
                    "(0, 'sell', 'abc'), (0, 'buy', 'abc'), (0, 'buy', 'def'), (0, 'buy', 'fgh'), (1, 'sell', 'abc')");
            drainWalQueue();
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tsell
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side ASC; ");
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tsell
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side ASC LIMIT -3;");
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesExistingOrderByCaseCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("SELECT timestamp, side FROM tRaDEs ORDER BY tiMesTAmP ASC, sIDe DESC LIMIT -3;",
                    """
                            Sort light
                              keys: [timestamp, side desc]
                                Sort light lo: 3 partiallySorted: true
                                  keys: [timestamp desc, side]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: trades
                            """);
            execute("insert into trades (timestamp, side, symbol) values " +
                    "(0, 'sell', 'abc'), (0, 'buy', 'abc'), (0, 'buy', 'def'), (0, 'buy', 'fgh'), (1, 'sell', 'abc')");
            drainWalQueue();
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tsell
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC; ");
            // last 3 roes of the result set above
            assertSql("""
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC LIMIT -3;");

        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesExistingOrderByThreeTerms() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck(
                    "SELECT timestamp, side, symbol FROM trades ORDER BY timestamp ASC, side DESC, symbol ASC LIMIT -3;",
                    """
                            Sort light
                              keys: [timestamp, side desc, symbol]
                                Sort light lo: 3 partiallySorted: true
                                  keys: [timestamp desc, side, symbol desc]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: trades
                            """
            );

            execute(
                    "insert into trades (timestamp, side, symbol) values " +
                            "(0, 'sell', 'abc'), (0, 'buy', 'abc'), (0, 'buy', 'def'), (0, 'buy', 'fgh'), (1, 'sell', 'abc')"
            );
            drainWalQueue();
            assertSql("""
                            timestamp\tside\tsymbol
                            1970-01-01T00:00:00.000000Z\tsell\tabc
                            1970-01-01T00:00:00.000000Z\tbuy\tabc
                            1970-01-01T00:00:00.000000Z\tbuy\tdef
                            1970-01-01T00:00:00.000000Z\tbuy\tfgh
                            1970-01-01T00:00:00.000001Z\tsell\tabc
                            """,
                    "SELECT timestamp, side, symbol FROM trades ORDER BY timestamp ASC, side DESC, symbol ASC;");
            assertSql(
                    """
                            timestamp\tside\tsymbol
                            1970-01-01T00:00:00.000000Z\tbuy\tdef
                            1970-01-01T00:00:00.000000Z\tbuy\tfgh
                            1970-01-01T00:00:00.000001Z\tsell\tabc
                            """,
                    "SELECT timestamp, side, symbol FROM trades ORDER BY timestamp ASC, side DESC, symbol ASC LIMIT -3;"
            );
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesExistingOrderByWithHiLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC LIMIT -1, -3;",
                    """
                            Sort light lo: -1 hi: -3 partiallySorted: true
                              keys: [timestamp, side desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """);
            execute("insert into trades (timestamp, side, symbol) values " +
                    "(0, 'sell1', 'abc'), (0, 'buy', 'abc'), (0, 'buy', 'def'), (0, 'buy', 'fgh'), (1, 'sell', 'abc')");
            drainWalQueue();
            assertSql(
                    """
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tsell1
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC;"
            );
            assertSql(
                    """
                            timestamp\tside
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000000Z\tbuy
                            1970-01-01T00:00:00.000001Z\tsell
                            """,
                    "SELECT timestamp, side FROM trades ORDER BY timestamp ASC, side DESC LIMIT -3;"
            );
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesWildcards() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select timestamp, * from trades limit -3",
                    """
                            Limit lo: -3 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesWildcardsManualAliasing() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select *, timestamp ts1, timestamp ts2 from trades limit -3",
                    """
                            Limit lo: -3 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitHandlesWildcardsTimestampNotFirst() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select *, timestamp from trades limit -3",
                    """
                            Limit lo: -3 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitWithHiLimitHandleTimestampAndAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select timestamp ts1, timestamp ts2 from trades limit -3, -10",
                    """
                            Limit lo: -3 hi: -10 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitWithHiLimitHandlesWildcardsManualAliasing() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select *, timestamp ts1, timestamp ts2 from trades limit -3, -10;",
                    """
                            Limit lo: -3 hi: -10 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteNegativeLimitWithHiLimitHandlesWildcardsTimestampNotFirst() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertPlanNoLeakCheck("select *, timestamp from trades limit -3, -10;",
                    """
                            Limit lo: -3 hi: -10 skip-over-rows: 0 limit: 0
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteTrivialExpressions() throws Exception {
        testRewriteTrivialExpressions(false);
    }

    @Test
    public void testRewriteTrivialExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                id0 long,
                                id1 long,
                                ts timestamp
                            ) TIMESTAMP(ts) PARTITION BY DAY;"""
            );
            execute("INSERT INTO x VALUES(1, 1, '2021-01-01T12:34:00');");
            execute("INSERT INTO x VALUES(2, 2, '2021-01-01T12:35:00');");

            final String query = "SELECT id0, id1, 2 - id0 + 1 id0_1, 2 / (id1 * 2) id1_1, count(*) AS c " +
                    "FROM x " +
                    "GROUP BY id0, id1, id0 / 42, 1 / (id1 * 42) " +
                    "ORDER BY c DESC";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Radix sort light
                              keys: [c desc]
                                VirtualRecord
                                  functions: [id0,id1,2-id0+1,2/id1*2,c]
                                    Async Group By workers: 1
                                      keys: [id0,id1,column,column1]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            id0\tid1\tid0_1\tid1_1\tc
                            2\t2\t1\t0\t1
                            1\t1\t2\t1\t1
                            """,
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testRewriteTrivialExpressionsInOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                id long,
                                ts timestamp
                            ) TIMESTAMP(ts) PARTITION BY DAY;"""
            );
            execute("INSERT INTO x VALUES(1, '2021-01-01T12:34:00');");
            execute("INSERT INTO x VALUES(2, '2021-01-01T12:35:00');");

            final String query = "WITH x1 AS ( " +
                    "  SELECT id id0, id / 2 id1, count(*) AS c " +
                    "  FROM x " +
                    "  GROUP BY id, id / 2 " +
                    "  ORDER BY c DESC" +
                    ") " +
                    "SELECT * " +
                    "FROM x x0 " +
                    "JOIN x1 ON x0.id = x1.id0;";

            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: x1.id0=x0.id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                                    Hash
                                        Radix sort light
                                          keys: [c desc]
                                            VirtualRecord
                                              functions: [id,id/2,c]
                                                Async Group By workers: 1
                                                  keys: [id]
                                                  values: [count(*)]
                                                  filter: null
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: x
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            id\tts\tid0\tid1\tc
                            1\t2021-01-01T12:34:00.000000Z\t1\t0\t1
                            2\t2021-01-01T12:35:00.000000Z\t2\t1\t1
                            """,
                    query,
                    "ts"
            );
        });
    }

    @Test
    public void testRewriteTrivialExpressionsInOuterJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                id long,
                                ts timestamp
                            ) TIMESTAMP(ts) PARTITION BY DAY;"""
            );
            execute("INSERT INTO x VALUES(1, '2021-01-01T12:34:00');");
            execute("INSERT INTO x VALUES(2, '2021-01-01T12:35:00');");

            final String query = "WITH x1 AS ( " +
                    "  SELECT id id0, id / 2 id1, id / 2 + 1 id2, count(*) AS c " +
                    "  FROM x " +
                    "  GROUP BY id, id / 2, id / 2 + 1 " +
                    "  ORDER BY c DESC" +
                    ") " +
                    "SELECT * " +
                    "FROM x x0 " +
                    "JOIN x1 ON x0.id = x1.id0;";

            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: x1.id0=x0.id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                                    Hash
                                        Radix sort light
                                          keys: [c desc]
                                            VirtualRecord
                                              functions: [id,id/2,id/2+1,c]
                                                Async Group By workers: 1
                                                  keys: [id]
                                                  values: [count(*)]
                                                  filter: null
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: x
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            id\tts\tid0\tid1\tid2\tc
                            1\t2021-01-01T12:34:00.000000Z\t1\t0\t1\t1
                            2\t2021-01-01T12:35:00.000000Z\t2\t1\t2\t1
                            """,
                    query,
                    "ts"
            );
        });
    }

    @Test
    public void testRewriteTrivialExpressionsInOuterUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                id long,
                                ts timestamp
                            ) TIMESTAMP(ts) PARTITION BY DAY;"""
            );
            execute("INSERT INTO x VALUES(1, '2021-01-01T12:34:00');");
            execute("INSERT INTO x VALUES(2, '2021-01-01T12:35:00');");

            final String query = "SELECT 1 id0, 2 id1, 1 c FROM long_sequence(1) " +
                    "UNION ALL " +
                    "(SELECT id id0, id * 2 id1, count(*) AS c " +
                    "FROM x " +
                    "GROUP BY id, id * 2 " +
                    "ORDER BY c DESC);";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Union All
                                VirtualRecord
                                  functions: [1,2,1]
                                    long_sequence count: 1
                                Radix sort light
                                  keys: [c desc]
                                    VirtualRecord
                                      functions: [id,id*2,c]
                                        Async Group By workers: 1
                                          keys: [id]
                                          values: [count(*)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            id0\tid1\tc
                            1\t2\t1
                            2\t4\t1
                            1\t2\t1
                            """,
                    query,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testRewriteTrivialExpressions_aliasExpressionsEnabled() throws Exception {
        testRewriteTrivialExpressions(true);
    }

    @Test
    public void testSampleByExpressionDependOtherColumn() throws Exception {
        execute("""
                create table t (
                  timestamp TIMESTAMP,
                  symbol SYMBOL capacity 256 CACHE,
                  side SYMBOL CAPACITY 256 CACHE,
                  price double
                ) timestamp(timestamp) partition by day;""");

        execute("INSERT INTO t (timestamp, symbol, side, price) VALUES" +
                " ('2023-09-01T00:00:00.000Z', 'ETH-USD', 'buyer', 3240.0)," +
                " ('2023-09-01T01:00:00.000Z', 'ETH-USD', 'buyer', 3241.0)," +
                " ('2023-09-01T02:00:00.000Z', 'ETH-USD', 'buyer', 3242.0)," +
                " ('2023-09-01T03:00:00.000Z', 'ETH-USD', 'buyer', 3243.0)," +
                " ('2023-09-01T04:00:00.000Z', 'ETH-USD', 'buyer', 3244.0)," +
                " ('2023-09-01T05:00:00.000Z', 'ETH-USD', 'seller', 5.0)");
        assertMemoryLeak(() -> {
            final String query = "select timestamp, symbol, side, CASE WHEN price > 3240  THEN avg(price) END as price_today, CASE WHEN price < 3240  THEN avg(price) END as price_yesterday " +
                    "from t where timestamp >= '2023-09-01T00:00:00.000Z' and symbol = 'ETH-USD' sample by 1h";

            final String result = """
                    timestamp\tsymbol\tside\tprice_today\tprice_yesterday
                    2023-09-01T00:00:00.000000Z\tETH-USD\tbuyer\tnull\tnull
                    2023-09-01T01:00:00.000000Z\tETH-USD\tbuyer\t3241.0\tnull
                    2023-09-01T02:00:00.000000Z\tETH-USD\tbuyer\t3242.0\tnull
                    2023-09-01T03:00:00.000000Z\tETH-USD\tbuyer\t3243.0\tnull
                    2023-09-01T04:00:00.000000Z\tETH-USD\tbuyer\t3244.0\tnull
                    2023-09-01T05:00:00.000000Z\tETH-USD\tseller\tnull\t5.0
                    """;

            assertSql(result, query);
        });

        assertMemoryLeak(() -> {
            final String query = "select timestamp, symbol, side, CASE WHEN extract('day', timestamp) =14  THEN avg(price) END as price_today, CASE WHEN true  THEN avg(price) END as price_yesterday " +
                    "from t where timestamp >= '2023-09-01T00:00:00.000Z' and symbol = 'ETH-USD' sample by 1h";

            final String result = """
                    timestamp\tsymbol\tside\tprice_today\tprice_yesterday
                    2023-09-01T00:00:00.000000Z\tETH-USD\tbuyer\tnull\t3240.0
                    2023-09-01T01:00:00.000000Z\tETH-USD\tbuyer\tnull\t3241.0
                    2023-09-01T02:00:00.000000Z\tETH-USD\tbuyer\tnull\t3242.0
                    2023-09-01T03:00:00.000000Z\tETH-USD\tbuyer\tnull\t3243.0
                    2023-09-01T04:00:00.000000Z\tETH-USD\tbuyer\tnull\t3244.0
                    2023-09-01T05:00:00.000000Z\tETH-USD\tseller\tnull\t5.0
                    """;

            assertSql(result, query);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);

            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'";
            final String model = "select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20' and ts < '2018-01-31' from '2017-12-20' to '2018-01-31' stride 5d) order by ts";
            assertModel(model, query, ExecutionModel.QUERY);

            final String target = """
                    select ts, avg(x) from fromto
                    where ts >= '2017-12-20' and ts < '2018-01-31'
                    sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'""";

            final String tmodel = "select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20' and ts < '2018-01-31' and ts >= '2017-12-20' and ts < '2018-01-31' from '2017-12-20' to '2018-01-31' stride 5d) order by ts";
            assertModel(tmodel, target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationGreaterThanOrEqualTo() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' align to calendar with offset '10:00'";

            assertModel("select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20' from '2017-12-20' stride 5d) order by ts", query, ExecutionModel.QUERY);

            final String target = """
                    select ts, avg(x) from fromto
                    where ts >= '2017-12-20'
                    sample by 5d from '2017-12-20' align to calendar with offset '10:00'""";

            assertModel("select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-20' and ts >= '2017-12-20' from '2017-12-20' stride 5d) order by ts", target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationLesserThan() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d to '2018-01-31' align to calendar with offset '10:00'";

            final String model = "select-group-by timestamp_floor('5d', ts, null, '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts < '2018-01-31' to '2018-01-31' stride 5d) order by ts";
            assertModel(model, query, ExecutionModel.QUERY);

            final String target = """
                    select ts, avg(x) from fromto
                    where ts < '2018-01-31'
                    sample by 5d to '2018-01-31' align to calendar with offset '10:00'""";

            final String targetModel = "select-group-by timestamp_floor('5d', ts, null, '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts < '2018-01-31' and ts < '2018-01-31' to '2018-01-31' stride 5d) order by ts";
            assertModel(targetModel, target, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationNarrowing() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String fromNarrow = """
                    select ts, avg(x) from fromto
                    where ts >= '2017-12-20'
                    sample by 5d from '2017-12-22' align to calendar with offset '10:00'""";

            assertModel("select-group-by timestamp_floor('5d', ts, '2017-12-22', '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts >= '2017-12-22' and ts >= '2017-12-20' from '2017-12-22' stride 5d) order by ts", fromNarrow, ExecutionModel.QUERY);

            final String toNarrow = """
                    select ts, avg(x) from fromto
                    where ts >= '2017-12-20'
                    sample by 5d TO '2017-12-22' align to calendar with offset '10:00'""";

            assertModel("select-group-by timestamp_floor('5d', ts, null, '10:00', null) ts, avg(x) avg from (select [ts, x] from fromto timestamp (ts) where ts < '2017-12-22' and ts >= '2017-12-20' to '2017-12-22' stride 5d) order by ts", toNarrow, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = """
                    select ts, avg(x) from fromto
                    where s != '5'
                    sample by 5d from '2017-12-20' to '2018-01-31' align to calendar with offset '10:00'
                    """;

            final String model = "select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts >= '2017-12-20' and ts < '2018-01-31' and s != '5' from '2017-12-20' to '2018-01-31' stride 5d) order by ts";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereGreaterThanOrEqualTo() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = """
                    select ts, avg(x) from fromto
                    where s != '5'
                    sample by 5d from '2017-12-20' align to calendar with offset '10:00'
                    """;

            final String model = "select-group-by timestamp_floor('5d', ts, '2017-12-20', '10:00', null) ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts >= '2017-12-20' and s != '5' from '2017-12-20' stride 5d) order by ts";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToBasicWhereOptimisationWithExistingWhereLesserThan() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = """
                    select ts, avg(x) from fromto
                    where s != '5'
                    sample by 5d to '2018-01-31' align to calendar with offset '10:00'
                    """;

            final String model = "select-group-by timestamp_floor('5d', ts, null, '10:00', null) ts, avg(x) avg from (select [ts, x, s] from fromto timestamp (ts) where ts < '2018-01-31' and s != '5' to '2018-01-31' stride 5d) order by ts";

            assertModel(model, query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSampleByFromToCheckingColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query =
                    "select ts, avg(x), " +
                            "string_agg(s, ',')," +
                            "avg(b)," +
                            "avg(e)," +
                            "avg(i)," +
                            "avg(f)," +
                            "avg(d)," +
                            "string_agg(str, ',')," +
                            "avg(k::double)," +
                            "avg(t::double)," +
                            "avg(n::double)," +
                            "from fromto sample by 5d from '2018-01-01' to '2018-01-31' fill(null)";

            assertSql("""
                    ts\tavg\tstring_agg\tavg1\tavg2\tavg3\tavg4\tavg5\tstring_agg1\tavg6\tavg7\tavg8
                    2018-01-01T00:00:00.000000Z\t120.5\t1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240\t-0.03333333333333333\t120.5\t120.5\t120.5\t120.5\t1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240\t120.5\t1.0\t120.5
                    2018-01-06T00:00:00.000000Z\t360.5\t241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480\t1.0333333333333334\t360.5\t360.5\t360.5\t360.5\t241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480\t360.5\t1.0\t360.5
                    2018-01-11T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull
                    2018-01-16T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull
                    2018-01-21T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull
                    2018-01-26T00:00:00.000000Z\tnull\t\tnull\tnull\tnull\tnull\tnull\t\tnull\tnull\tnull
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToDisallowedQueryWithKey() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            assertException("""
                    SELECT ts, count, s
                    FROM fromto
                    SAMPLE BY 5d FROM '2018-01-01' TO '2019-01-01'
                    LIMIT 6""", 0, "are not supported for keyed SAMPLE BY");
        });
    }

    @Test
    public void testSampleByFromToFillNullWithExtraColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = """
                    select ts, avg(x), sum(x) from fromto
                    sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    """;

            assertPlanNoLeakCheck(query, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: ('2017-12-20','2018-01-31')
                          stride: '5d'
                          values: [null,null]
                            Async Group By workers: 1
                              keys: [ts]
                              values: [avg(x),sum(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: fromto
                                      intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);
            assertSql("""
                    ts\tavg\tsum
                    2017-12-20T00:00:00.000000Z\tnull\tnull
                    2017-12-25T00:00:00.000000Z\tnull\tnull
                    2017-12-30T00:00:00.000000Z\t72.5\t10440
                    2018-01-04T00:00:00.000000Z\t264.5\t63480
                    2018-01-09T00:00:00.000000Z\t432.5\t41520
                    2018-01-14T00:00:00.000000Z\tnull\tnull
                    2018-01-19T00:00:00.000000Z\tnull\tnull
                    2018-01-24T00:00:00.000000Z\tnull\tnull
                    2018-01-29T00:00:00.000000Z\tnull\tnull
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToNotEnoughFillValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
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
    public void testSampleByFromToParallelDeduceTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 't' (" +
                            "  name SYMBOL capacity 256 CACHE," +
                            "  timestamp TIMESTAMP" +
                            ") timestamp (timestamp) PARTITION BY DAY;"
            );
            execute(
                    "INSERT INTO t (name, timestamp) VALUES" +
                            " ('a', '2023-09-01T00:00:00.000Z')," +
                            " ('a', '2023-09-01T00:10:00.000Z')"
            );

            assertSql(
                    """
                            timestamp\textra_column\textra_column2\tfirst
                            2023-09-01T00:01:00.000000Z\t0\t0\ta
                            2023-09-01T00:11:00.000000Z\t0\t0\ta
                            """,
                    """
                            SELECT timestamp+60000000 as 'timestamp', 0 AS extra_column, 0 AS extra_column2, first(name)\s
                            FROM t
                            WHERE name = 'a'
                            SAMPLE BY (1m);
                            """
            );

            assertSql(
                    """
                            timestamp\tname\tcount
                            2023-09-02T00:00:00.000000Z\ta\t1
                            2023-09-02T00:10:00.000000Z\ta\t1
                            """,
                    "select dateadd('d', 1, timestamp) timestamp, name, count() from t sample by 10m"
            );

            assertSql(
                    """
                            timestamp\ttimestamp2\tname\tcount
                            2023-09-02T00:00:00.000000Z\t2023-09-03T00:00:00.000000Z\ta\t1
                            2023-09-02T00:10:00.000000Z\t2023-09-03T00:10:00.000000Z\ta\t1
                            """,
                    "select dateadd('d', 1, timestamp) timestamp, dateadd('d', 2, timestamp) timestamp2, name, count() from t sample by 10m"
            );

            assertSql(
                    """
                            timestamp\ttimestamp1\tcount
                            2023-09-01T00:01:00.000000Z\t2023-09-01T00:00:00.000000Z\t1
                            2023-09-01T00:11:00.000000Z\t2023-09-01T00:10:00.000000Z\t1
                            """,
                    "select timestamp + 60000000 as 'timestamp', timestamp, count() from t where name = 'a' sample by (1m)"
            );

            assertSql(
                    """
                            timestamp1\ttimestamp\tcount
                            2023-09-01T00:01:00.000000Z\t2023-09-01T00:00:00.000000Z\t1
                            2023-09-01T00:11:00.000000Z\t2023-09-01T00:10:00.000000Z\t1
                            """,
                    "select timestamp + 60000000 as 'timestamp1', timestamp, count() from t where name = 'a' sample by (1m)"
            );

            assertSql(
                    """
                            timestamp\ttimestamp1\tcount
                            2023-09-01T00:01:00.000000Z\t2023-09-01T00:00:00.000000Z\t1
                            2023-09-01T00:11:00.000000Z\t2023-09-01T00:10:00.000000Z\t1
                            """,
                    "select timestamp + 60000000 as 'timestamp', timestamp as 'timestamp1', count() from t where name = 'a' sample by (1m)"
            );
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewrite() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = """
                    select ts, avg(x) from fromto
                    sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    """;

            assertPlanNoLeakCheck(query, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: ('2017-12-20','2018-01-31')
                          stride: '5d'
                          values: [null]
                            Async Group By workers: 1
                              keys: [ts]
                              values: [avg(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: fromto
                                      intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);
            assertSql("""
                    ts\tavg
                    2017-12-20T00:00:00.000000Z\tnull
                    2017-12-25T00:00:00.000000Z\tnull
                    2017-12-30T00:00:00.000000Z\t72.5
                    2018-01-04T00:00:00.000000Z\t264.5
                    2018-01-09T00:00:00.000000Z\t432.5
                    2018-01-14T00:00:00.000000Z\tnull
                    2018-01-19T00:00:00.000000Z\tnull
                    2018-01-24T00:00:00.000000Z\tnull
                    2018-01-29T00:00:00.000000Z\tnull
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteMultipleFills() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts, avg(x), sum(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(42, 41)";

            assertPlanNoLeakCheck(query, """
                    Sample By
                      fill: value
                      range: ('2017-12-20','2018-01-31')
                      values: [avg(x),sum(x)]
                        PageFrame
                            Row forward scan
                            Interval forward scan on: fromto
                              intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);
            assertSql("""
                    ts\tavg\tsum
                    2017-12-20T00:00:00.000000Z\t42.0\t41
                    2017-12-25T00:00:00.000000Z\t42.0\t41
                    2017-12-30T00:00:00.000000Z\t72.5\t10440
                    2018-01-04T00:00:00.000000Z\t264.5\t63480
                    2018-01-09T00:00:00.000000Z\t432.5\t41520
                    2018-01-14T00:00:00.000000Z\t42.0\t41
                    2018-01-19T00:00:00.000000Z\t42.0\t41
                    2018-01-24T00:00:00.000000Z\t42.0\t41
                    2018-01-29T00:00:00.000000Z\t42.0\t41
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewritePostfill() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d to '2018-01-31' fill(null)";

            assertPlanNoLeakCheck(query, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: (,'2018-01-31')
                          stride: '5d'
                          values: [null]
                            Async Group By workers: 1
                              keys: [ts]
                              values: [avg(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: fromto
                                      intervals: [("MIN","2018-01-30T23:59:59.999999Z")]
                    """);
            assertSql("""
                    ts\tavg
                    2017-12-30T00:00:00.000000Z\t72.5
                    2018-01-04T00:00:00.000000Z\t264.5
                    2018-01-09T00:00:00.000000Z\t432.5
                    2018-01-14T00:00:00.000000Z\tnull
                    2018-01-19T00:00:00.000000Z\tnull
                    2018-01-24T00:00:00.000000Z\tnull
                    2018-01-29T00:00:00.000000Z\tnull
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewritePrefill() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts, avg(x) from fromto\n" +
                    "sample by 5d from '2017-12-20' fill(null) ";

            assertPlanNoLeakCheck(query, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: ('2017-12-20',)
                          stride: '5d'
                          values: [null]
                            Async Group By workers: 1
                              keys: [ts]
                              values: [avg(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: fromto
                                      intervals: [("2017-12-20T00:00:00.000000Z","MAX")]
                    """);
            assertSql("""
                    ts\tavg
                    2017-12-20T00:00:00.000000Z\tnull
                    2017-12-25T00:00:00.000000Z\tnull
                    2017-12-30T00:00:00.000000Z\t72.5
                    2018-01-04T00:00:00.000000Z\t264.5
                    2018-01-09T00:00:00.000000Z\t432.5
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithExcept() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            execute(SampleByTest.FROM_TO_DDL.replace("fromto", "fromto2"));

            final String exceptAllQuery = """
                    select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    except all
                    select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    """;

            final String exceptQuery = exceptAllQuery.replace("except all", "except");

            assertPlanNoLeakCheck(exceptAllQuery, """
                    Except All
                        Sort
                          keys: [ts]
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                        Hash
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto2
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);

            assertSql("ts\tavg\tsum\n", exceptAllQuery);

            assertPlanNoLeakCheck(exceptQuery, """
                    Except
                        Sort
                          keys: [ts]
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                        Hash
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto2
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);

            assertSql("ts\tavg\tsum\n", exceptQuery);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithIntersect() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            execute(SampleByTest.FROM_TO_DDL.replace("fromto", "fromto2"));

            final String intersectAllQuery = """
                    select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    intersect all
                    select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    """;

            final String intersectQuery = intersectAllQuery.replace("intersect all", "intersect");

            assertPlanNoLeakCheck(intersectAllQuery, """
                    Intersect All
                        Sort
                          keys: [ts]
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                        Hash
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto2
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);

            assertSql("""
                    ts\tavg\tsum
                    2017-12-20T00:00:00.000000Z\tnull\tnull
                    2017-12-25T00:00:00.000000Z\tnull\tnull
                    2017-12-30T00:00:00.000000Z\t72.5\t10440
                    2018-01-04T00:00:00.000000Z\t264.5\t63480
                    2018-01-09T00:00:00.000000Z\t432.5\t41520
                    2018-01-14T00:00:00.000000Z\tnull\tnull
                    2018-01-19T00:00:00.000000Z\tnull\tnull
                    2018-01-24T00:00:00.000000Z\tnull\tnull
                    2018-01-29T00:00:00.000000Z\tnull\tnull
                    """, intersectAllQuery);

            assertPlanNoLeakCheck(intersectQuery, """
                    Intersect
                        Sort
                          keys: [ts]
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                        Hash
                            Fill Range
                              range: ('2017-12-20','2018-01-31')
                              stride: '5d'
                              values: [null,null]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(x),sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: fromto2
                                          intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                    """);

            assertSql("""
                    ts\tavg\tsum
                    2017-12-20T00:00:00.000000Z\tnull\tnull
                    2017-12-25T00:00:00.000000Z\tnull\tnull
                    2017-12-30T00:00:00.000000Z\t72.5\t10440
                    2018-01-04T00:00:00.000000Z\t264.5\t63480
                    2018-01-09T00:00:00.000000Z\t432.5\t41520
                    2018-01-14T00:00:00.000000Z\tnull\tnull
                    2018-01-19T00:00:00.000000Z\tnull\tnull
                    2018-01-24T00:00:00.000000Z\tnull\tnull
                    2018-01-29T00:00:00.000000Z\tnull\tnull
                    """, intersectQuery);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            execute(SampleByTest.FROM_TO_DDL.replace("fromto", "fromto2"));


            final String query = """
                    select fromto.ts, avg(fromto.x)
                    from fromto
                    asof join fromto2
                    sample by 5d from '2017-12-20' to '2018-01-31' fill(null)
                    """;

            assertPlanNoLeakCheck(query, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: ('2017-12-20','2018-01-31')
                          stride: '5d'
                          values: [null]
                            GroupBy vectorized: false
                              keys: [ts]
                              values: [avg(x)]
                                SelectedRecord
                                    AsOf Join Fast Scan
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: fromto
                                              intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: fromto2
                    """);
            assertSql("""
                    ts\tavg
                    2017-12-20T00:00:00.000000Z\tnull
                    2017-12-25T00:00:00.000000Z\tnull
                    2017-12-30T00:00:00.000000Z\t72.5
                    2018-01-04T00:00:00.000000Z\t264.5
                    2018-01-09T00:00:00.000000Z\t432.5
                    2018-01-14T00:00:00.000000Z\tnull
                    2018-01-19T00:00:00.000000Z\tnull
                    2018-01-24T00:00:00.000000Z\tnull
                    2018-01-29T00:00:00.000000Z\tnull
                    """, query);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            execute(SampleByTest.FROM_TO_DDL.replace("fromto", "fromto2"));

            final String query = """
                    (select ts as five_days, avg(x) as five_days_avg from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null))
                    asof join
                    (select ts as ten_days, avg(x) as ten_days_avg from fromto2 sample by 10d from '2017-12-20' to '2018-01-31' fill(null))
                    """;

            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                AsOf Join
                                    Sort
                                      keys: [five_days]
                                        Fill Range
                                          range: ('2017-12-20','2018-01-31')
                                          stride: '5d'
                                          values: [null]
                                            Async Group By workers: 1
                                              keys: [five_days]
                                              values: [avg(x)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Interval forward scan on: fromto
                                                      intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                                    Sort
                                      keys: [ten_days]
                                        Fill Range
                                          range: ('2017-12-20','2018-01-31')
                                          stride: '10d'
                                          values: [null]
                                            Async Group By workers: 1
                                              keys: [ten_days]
                                              values: [avg(x)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Interval forward scan on: fromto2
                                                      intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                            """
            );
            assertSql(
                    """
                            five_days\tfive_days_avg\tten_days\tten_days_avg
                            2017-12-20T00:00:00.000000Z\tnull\t2017-12-20T00:00:00.000000Z\tnull
                            2017-12-25T00:00:00.000000Z\tnull\t2017-12-20T00:00:00.000000Z\tnull
                            2017-12-30T00:00:00.000000Z\t72.5\t2017-12-30T00:00:00.000000Z\t192.5
                            2018-01-04T00:00:00.000000Z\t264.5\t2017-12-30T00:00:00.000000Z\t192.5
                            2018-01-09T00:00:00.000000Z\t432.5\t2018-01-09T00:00:00.000000Z\t432.5
                            2018-01-14T00:00:00.000000Z\tnull\t2018-01-09T00:00:00.000000Z\t432.5
                            2018-01-19T00:00:00.000000Z\tnull\t2018-01-19T00:00:00.000000Z\tnull
                            2018-01-24T00:00:00.000000Z\tnull\t2018-01-19T00:00:00.000000Z\tnull
                            2018-01-29T00:00:00.000000Z\tnull\t2018-01-29T00:00:00.000000Z\tnull
                            """,
                    query
            );
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
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

            final String shouldSucceedResult = """
                    ts\tavg\tsum
                    2017-12-20T00:00:00.000000Z\tnull\tnull
                    2017-12-25T00:00:00.000000Z\tnull\tnull
                    2017-12-30T00:00:00.000000Z\t72.5\t10440
                    2018-01-04T00:00:00.000000Z\t264.5\t63480
                    2018-01-09T00:00:00.000000Z\t432.5\t41520
                    """;

            assertPlanNoLeakCheck(shouldSucceedParallel, """
                    Sort
                      keys: [ts]
                        Fill Range
                          range: ('2017-12-20',)
                          stride: '5d'
                          values: [null,null]
                            Async Group By workers: 1
                              keys: [ts]
                              values: [avg(x),sum(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: fromto
                                      intervals: [("2017-12-20T00:00:00.000000Z","MAX")]
                    """);
            assertSql(shouldSucceedResult, shouldSucceedParallel);

            assertPlanNoLeakCheck(shouldSucceedSequential, """
                    Sample By
                      fill: null
                      range: ('2017-12-20',)
                      values: [avg(x),sum(x)]
                        PageFrame
                            Row forward scan
                            Interval forward scan on: fromto
                              intervals: [("2017-12-20T00:00:00.000000Z","MAX")]
                    """);
            assertSql(shouldSucceedResult, shouldSucceedSequential);
        });
    }

    @Test
    public void testSampleByFromToParallelSampleByRewriteWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            execute(SampleByTest.FROM_TO_DDL.replace("fromto", "fromto2"));

            final String unionAllQuery = "select *" +
                    "from (" +
                    "  select ts, avg(x), sum(x) from fromto sample by 5d from '2017-12-20' to '2018-01-31' fill(null) " +
                    "  union all " +
                    "  select ts, avg(x), sum(x) from fromto2 sample by 5d from '2017-12-20' to '2018-01-31' fill(null)" +
                    ")" +
                    "order by ts";

            final String unionQuery = unionAllQuery.replace("union all", "union");

            assertPlanNoLeakCheck(
                    unionAllQuery,
                    """
                            Sort
                              keys: [ts]
                                Union All
                                    Fill Range
                                      range: ('2017-12-20','2018-01-31')
                                      stride: '5d'
                                      values: [null,null]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          values: [avg(x),sum(x)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: fromto
                                                  intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                                    Fill Range
                                      range: ('2017-12-20','2018-01-31')
                                      stride: '5d'
                                      values: [null,null]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          values: [avg(x),sum(x)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: fromto2
                                                  intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tavg\tsum
                            2017-12-20T00:00:00.000000Z\tnull\tnull
                            2017-12-20T00:00:00.000000Z\tnull\tnull
                            2017-12-25T00:00:00.000000Z\tnull\tnull
                            2017-12-25T00:00:00.000000Z\tnull\tnull
                            2017-12-30T00:00:00.000000Z\t72.5\t10440
                            2017-12-30T00:00:00.000000Z\t72.5\t10440
                            2018-01-04T00:00:00.000000Z\t264.5\t63480
                            2018-01-04T00:00:00.000000Z\t264.5\t63480
                            2018-01-09T00:00:00.000000Z\t432.5\t41520
                            2018-01-09T00:00:00.000000Z\t432.5\t41520
                            2018-01-14T00:00:00.000000Z\tnull\tnull
                            2018-01-14T00:00:00.000000Z\tnull\tnull
                            2018-01-19T00:00:00.000000Z\tnull\tnull
                            2018-01-19T00:00:00.000000Z\tnull\tnull
                            2018-01-24T00:00:00.000000Z\tnull\tnull
                            2018-01-24T00:00:00.000000Z\tnull\tnull
                            2018-01-29T00:00:00.000000Z\tnull\tnull
                            2018-01-29T00:00:00.000000Z\tnull\tnull
                            """,
                    unionAllQuery,
                    "ts",
                    true,
                    false
            );

            assertPlanNoLeakCheck(
                    unionQuery,
                    """
                            Sort
                              keys: [ts]
                                Union
                                    Fill Range
                                      range: ('2017-12-20','2018-01-31')
                                      stride: '5d'
                                      values: [null,null]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          values: [avg(x),sum(x)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: fromto
                                                  intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                                    Fill Range
                                      range: ('2017-12-20','2018-01-31')
                                      stride: '5d'
                                      values: [null,null]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          values: [avg(x),sum(x)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: fromto2
                                                  intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tavg\tsum
                            2017-12-20T00:00:00.000000Z\tnull\tnull
                            2017-12-25T00:00:00.000000Z\tnull\tnull
                            2017-12-30T00:00:00.000000Z\t72.5\t10440
                            2018-01-04T00:00:00.000000Z\t264.5\t63480
                            2018-01-09T00:00:00.000000Z\t432.5\t41520
                            2018-01-14T00:00:00.000000Z\tnull\tnull
                            2018-01-19T00:00:00.000000Z\tnull\tnull
                            2018-01-24T00:00:00.000000Z\tnull\tnull
                            2018-01-29T00:00:00.000000Z\tnull\tnull
                            """,
                    unionQuery,
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSampleByFromToParallelSequentialEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);

            final String parallel = "select ts, avg(x) from fromto\n" +
                    "sample by 1w from '2017-12-20' to '2018-01-31' fill(null)";

            // offset is ignored
            final String sequential = parallel + " align to calendar with offset '10:00'";

            final String result = """
                    ts\tavg
                    2017-12-20T00:00:00.000000Z\tnull
                    2017-12-27T00:00:00.000000Z\t48.5
                    2018-01-03T00:00:00.000000Z\t264.5
                    2018-01-10T00:00:00.000000Z\t456.5
                    2018-01-17T00:00:00.000000Z\tnull
                    2018-01-24T00:00:00.000000Z\tnull
                    """;

            assertSql(result, parallel);
            assertSql(result, sequential);
        });
    }

    @Test
    public void testSampleByFromToPlansWithRewrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tbl (
                      ts timestamp,
                      price double
                    ) timestamp(ts) partition by day wal;""");
            drainWalQueue();
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m from '2018-01-01' to '2019-01-01'",
                    """
                            Radix sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: tbl
                                          intervals: [("2018-01-01T00:00:00.000000Z","2018-12-31T23:59:59.999999Z")]
                            """
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m from '2018-01-01'",
                    """
                            Radix sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: tbl
                                          intervals: [("2018-01-01T00:00:00.000000Z","MAX")]
                            """
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m to '2019-01-01'",
                    """
                            Radix sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: tbl
                                          intervals: [("MIN","2018-12-31T23:59:59.999999Z")]
                            """
            );
            assertPlanNoLeakCheck(
                    "select ts, avg(price) from tbl sample by 5m",
                    """
                            Radix sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tbl
                            """
            );
        });

    }

    @Test
    public void testSampleByFromToWithAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute(SampleByTest.FROM_TO_DDL);
            final String query = "select ts as five_days, avg(x) as five_days_avg from fromto \n" +
                    "sample by 5d from '2017-12-20' to '2018-01-31' fill(null)";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort
                              keys: [five_days]
                                Fill Range
                                  range: ('2017-12-20','2018-01-31')
                                  stride: '5d'
                                  values: [null]
                                    Async Group By workers: 1
                                      keys: [five_days]
                                      values: [avg(x)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: fromto
                                              intervals: [("2017-12-20T00:00:00.000000Z","2018-01-30T23:59:59.999999Z")]
                            """
            );
            assertSql(
                    """
                            five_days\tfive_days_avg
                            2017-12-20T00:00:00.000000Z\tnull
                            2017-12-25T00:00:00.000000Z\tnull
                            2017-12-30T00:00:00.000000Z\t72.5
                            2018-01-04T00:00:00.000000Z\t264.5
                            2018-01-09T00:00:00.000000Z\t432.5
                            2018-01-14T00:00:00.000000Z\tnull
                            2018-01-19T00:00:00.000000Z\tnull
                            2018-01-24T00:00:00.000000Z\tnull
                            2018-01-29T00:00:00.000000Z\tnull
                            """,
                    query
            );
        });
    }

    @Test
    public void testSampleByGroupByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'trades' (
                      symbol SYMBOL capacity 256 CACHE,
                      side SYMBOL capacity 256 CACHE,
                      price DOUBLE,
                      amount DOUBLE,
                      timestamp TIMESTAMP
                    ) timestamp (timestamp) PARTITION BY DAY WAL;""");

            assertPlanNoLeakCheck("""
                            SELECT last(price) value, symbol, timestamp\s
                            FROM trades
                            WHERE timestamp >= '2024-08-11T10:13:00'\s
                            AND timestamp < '2024-08-11T10:16:00'\s
                            AND (symbol LIKE ('BTC-USD'))\s
                            SAMPLE BY 1m FILL(NONE) ALIGN TO CALENDAR""",
                    """
                            Radix sort light
                              keys: [timestamp]
                                Async Group By workers: 1
                                  keys: [symbol,timestamp]
                                  values: [last(price)]
                                  filter: symbol ~ BTC-USD [state-shared]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2024-08-11T10:13:00.000000Z","2024-08-11T10:15:59.999999Z")]
                            """);
        });
    }

    @Test
    public void testSampleByTimezone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (x int, ts timestamp) timestamp(ts);");
            final String query = "select ts, avg(x) from y\n" +
                    "sample by 5d from '2017-12-20' align to calendar time zone 'Europe/London' with offset '10:00'";

            assertModel("select-virtual to_utc(ts, 'Europe/London') ts, avg from (select-group-by [timestamp_floor('5d', ts, '2017-12-20', '10:00', 'Europe/London') ts, avg(x) avg] timestamp_floor('5d', ts, '2017-12-20', '10:00', 'Europe/London') ts, avg(x) avg from (select [ts, x] from y timestamp (ts) where ts >= '2017-12-20' from '2017-12-20' stride 5d)) timestamp (ts) order by ts", query, ExecutionModel.QUERY);
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingLastFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, LAST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, LAST(ts) LAST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [x]
                              values: [last(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingMaxFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MAX(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, MAX(ts) MAX from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [max(ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testSelectMultipleColumnsIncludingMinFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, MIN(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, MIN(ts) MIN from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [min(ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testSelectingMultipleColumnsIncludingFirstFunctionOnDesignatedTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select x, FIRST(ts) from y";
            final QueryModel model = compileModel(query);
            assertEquals("select-group-by x, FIRST(ts) FIRST from (select [x, ts] from y timestamp (ts))", model.toString0());
            assertPlanNoLeakCheck(
                    query,
                    """
                            Async Group By workers: 1
                              keys: [x]
                              values: [first(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (x int, ts timestamp) timestamp(ts) partition by day;");
            final String queryA = "select count_distinct(x) from y;";
            final String queryB = "select count(distinct x) from y;";
            String expectedModel = "select-group-by count() count_distinct from (select-group-by x from (select [x] from y timestamp (ts) where null != x))";
            assertEquals(
                    expectedModel,
                    compileModel(queryA).toString0()
            );
            String expectedPlan = """
                    Count
                        Async JIT Group By workers: 1
                          keys: [x]
                          filter: x!=null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: y
                    """;
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
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
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
                    """
                            Union
                                Union
                                    Union
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: y
                                        Limit lo: 1 skip-over-rows: 0 limit: 0
                                            SelectedRecord
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: y
                                    Limit lo: 1 skip-over-rows: 0 limit: 0
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                                Limit lo: 1 skip-over-rows: 0 limit: 0
                                    SelectedRecord
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testUnionQueryOnSingleCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (x int, z int);");
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
            String expectedPlan = """
                    Union
                        Count
                            Async JIT Group By workers: 1
                              keys: [x]
                              filter: x!=null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                        Count
                            Async JIT Group By workers: 1
                              keys: [z]
                              filter: z!=null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                    """;
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
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by FIRST(ts) FIRST from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [first(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithLastAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select LAST(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by LAST(ts) LAST from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [last(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithMaxAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MAX(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by MAX(ts) MAX from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [max(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseOnNestedModelWithMinAggregateFunctionOnParentModel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from (select * from y where x = 3)";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-group-by MIN(ts) MIN from (select-choose [ts] x, ts from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3))",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            GroupBy vectorized: false
                              values: [min(ts)]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: x=3
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseWithFirstAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select FIRST(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts FIRST from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseWithLastAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
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
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseWithMaxAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
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
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWhereClauseWithMinAggregateFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y ( x int, ts timestamp) timestamp(ts);");
            final String query = "select MIN(ts) from y where x = 3";
            final QueryModel model = compileModel(query);
            assertEquals(
                    "select-choose ts MIN from " +
                            "(select [ts, x] from y timestamp (ts) where x = 3) limit 1",
                    model.toString0()
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: x=3
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: y
                            """
            );
        });
    }

    @Test
    public void testWindowRangeFrameDependOnSubqueryOrderBy() throws SqlException {
        execute("create table cpu_ts ( hostname symbol, usage_system double, ts1 timestamp, ts2 timestamp) timestamp(ts1);");
        execute("insert into cpu_ts select rnd_symbol('A', 'B', 'C'), x, x::timestamp, x::timestamp + 6000000 from long_sequence(10)");
        String q1 = "SELECT * from " +
                "( " +
                "SELECT ts2, hostname, usage_system, " +
                "max(usage_system) OVER ( partition by hostname ORDER BY ts2 ASC RANGE BETWEEN 3 seconds preceding and current row ) AS max_usage_system " +
                "from ( " +
                "select * FROM cpu_ts WHERE ts2 >= '1970-01-01T00:00:00.000001Z' ORDER BY ts2)" +
                ") order by hostname, ts2 LIMIT 40;";

        assertPlanNoLeakCheck(
                q1,
                """
                        Limit lo: 40 skip-over-rows: 0 limit: 40
                            Sort
                              keys: [hostname, ts2]
                                Window
                                  functions: [max(usage_system) over (partition by [hostname] range between 3000000 preceding and current row)]
                                    Radix sort light
                                      keys: [ts2]
                                        Async JIT Filter workers: 1
                                          filter: ts2>=1970-01-01T00:00:00.000001Z
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts2\thostname\tusage_system\tmax_usage_system
                1970-01-01T00:00:06.000001Z\tA\t1.0\t1.0
                1970-01-01T00:00:06.000002Z\tA\t2.0\t2.0
                1970-01-01T00:00:06.000009Z\tA\t9.0\t9.0
                1970-01-01T00:00:06.000003Z\tB\t3.0\t3.0
                1970-01-01T00:00:06.000008Z\tB\t8.0\t8.0
                1970-01-01T00:00:06.000010Z\tB\t10.0\t10.0
                1970-01-01T00:00:06.000004Z\tC\t4.0\t4.0
                1970-01-01T00:00:06.000005Z\tC\t5.0\t5.0
                1970-01-01T00:00:06.000006Z\tC\t6.0\t6.0
                1970-01-01T00:00:06.000007Z\tC\t7.0\t7.0
                """, q1);

        String q2 = "SELECT ts2, hostname, usage_system, " +
                "max(usage_system) OVER ( partition by hostname ORDER BY ts2 ASC RANGE BETWEEN 3 seconds preceding and current row ) AS max_usage_system " +
                "from ( " +
                "select * FROM cpu_ts WHERE ts2 >= '1970-01-01T00:00:00.000001Z' ORDER BY ts2)";
        assertPlanNoLeakCheck(
                q2,
                """
                        Window
                          functions: [max(usage_system) over (partition by [hostname] range between 3000000 preceding and current row)]
                            Radix sort light
                              keys: [ts2]
                                Async JIT Filter workers: 1
                                  filter: ts2>=1970-01-01T00:00:00.000001Z
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts2\thostname\tusage_system\tmax_usage_system
                1970-01-01T00:00:06.000001Z\tA\t1.0\t1.0
                1970-01-01T00:00:06.000002Z\tA\t2.0\t2.0
                1970-01-01T00:00:06.000003Z\tB\t3.0\t3.0
                1970-01-01T00:00:06.000004Z\tC\t4.0\t4.0
                1970-01-01T00:00:06.000005Z\tC\t5.0\t5.0
                1970-01-01T00:00:06.000006Z\tC\t6.0\t6.0
                1970-01-01T00:00:06.000007Z\tC\t7.0\t7.0
                1970-01-01T00:00:06.000008Z\tB\t8.0\t8.0
                1970-01-01T00:00:06.000009Z\tA\t9.0\t9.0
                1970-01-01T00:00:06.000010Z\tB\t10.0\t10.0
                """, q2);

        String q3 = "SELECT * FROM (" +
                "SELECT ts1, hostname, usage_system, " +
                "max(usage_system) OVER ( partition by hostname ORDER BY ts1 ASC RANGE BETWEEN 3 seconds preceding and current row ) AS max_usage_system " +
                "from cpu_ts order by ts1)" +
                "order by ts1 desc";
        assertPlanNoLeakCheck(
                q3,
                """
                        Sort
                          keys: [ts1 desc]
                            Limit lo: 9223372036854775807L skip-over-rows: 0 limit: 10
                                Window
                                  functions: [max(usage_system) over (partition by [hostname] range between 3000000 preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts1\thostname\tusage_system\tmax_usage_system
                1970-01-01T00:00:00.000010Z\tB\t10.0\t10.0
                1970-01-01T00:00:00.000009Z\tA\t9.0\t9.0
                1970-01-01T00:00:00.000008Z\tB\t8.0\t8.0
                1970-01-01T00:00:00.000007Z\tC\t7.0\t7.0
                1970-01-01T00:00:00.000006Z\tC\t6.0\t6.0
                1970-01-01T00:00:00.000005Z\tC\t5.0\t5.0
                1970-01-01T00:00:00.000004Z\tC\t4.0\t4.0
                1970-01-01T00:00:00.000003Z\tB\t3.0\t3.0
                1970-01-01T00:00:00.000002Z\tA\t2.0\t2.0
                1970-01-01T00:00:00.000001Z\tA\t1.0\t1.0
                """, q3);

        String q4 = "SELECT * FROM (" +
                "SELECT ts1, hostname, usage_system, " +
                "first_value(usage_system) OVER ( partition by hostname) AS first_usage_system " +
                "from cpu_ts order by ts2)" +
                "order by ts1 desc";
        assertPlanNoLeakCheck(
                q4,
                """
                        Radix sort light
                          keys: [ts1 desc]
                            SelectedRecord
                                Limit lo: 9223372036854775807L skip-over-rows: 0 limit: 10
                                    Sort
                                      keys: [ts2]
                                        Window
                                          functions: [first_value(usage_system) over (partition by [hostname])]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts1\thostname\tusage_system\tfirst_usage_system
                1970-01-01T00:00:00.000010Z\tB\t10.0\t3.0
                1970-01-01T00:00:00.000009Z\tA\t9.0\t1.0
                1970-01-01T00:00:00.000008Z\tB\t8.0\t3.0
                1970-01-01T00:00:00.000007Z\tC\t7.0\t4.0
                1970-01-01T00:00:00.000006Z\tC\t6.0\t4.0
                1970-01-01T00:00:00.000005Z\tC\t5.0\t4.0
                1970-01-01T00:00:00.000004Z\tC\t4.0\t4.0
                1970-01-01T00:00:00.000003Z\tB\t3.0\t3.0
                1970-01-01T00:00:00.000002Z\tA\t2.0\t1.0
                1970-01-01T00:00:00.000001Z\tA\t1.0\t1.0
                """, q4);

        String q5 = "SELECT * from " +
                "( " +
                "SELECT ts2, hostname, usage_system, " +
                "max(usage_system) OVER ( partition by hostname ORDER BY ts2 ASC RANGE BETWEEN 3 seconds preceding and current row ) AS max_usage_system " +
                "from ( " +
                "select * FROM cpu_ts WHERE ts2 >= '1970-01-01T00:00:00.000001Z' ORDER BY ts2)" +
                ") order by ts2, hostname LIMIT 40;";

        assertPlanNoLeakCheck(
                q5,
                """
                        Limit lo: 40 skip-over-rows: 0 limit: 40
                            Sort
                              keys: [ts2, hostname]
                                Window
                                  functions: [max(usage_system) over (partition by [hostname] range between 3000000 preceding and current row)]
                                    Radix sort light
                                      keys: [ts2]
                                        Async JIT Filter workers: 1
                                          filter: ts2>=1970-01-01T00:00:00.000001Z
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts2\thostname\tusage_system\tmax_usage_system
                1970-01-01T00:00:06.000001Z\tA\t1.0\t1.0
                1970-01-01T00:00:06.000002Z\tA\t2.0\t2.0
                1970-01-01T00:00:06.000003Z\tB\t3.0\t3.0
                1970-01-01T00:00:06.000004Z\tC\t4.0\t4.0
                1970-01-01T00:00:06.000005Z\tC\t5.0\t5.0
                1970-01-01T00:00:06.000006Z\tC\t6.0\t6.0
                1970-01-01T00:00:06.000007Z\tC\t7.0\t7.0
                1970-01-01T00:00:06.000008Z\tB\t8.0\t8.0
                1970-01-01T00:00:06.000009Z\tA\t9.0\t9.0
                1970-01-01T00:00:06.000010Z\tB\t10.0\t10.0
                """, q5);

        String q6 = "SELECT * FROM (" +
                "SELECT ts1, hostname, usage_system, " +
                "row_number() OVER (partition by hostname order by ts1 desc RANGE BETWEEN 3 seconds preceding and current row), " +
                "rank() OVER (partition by hostname order by ts1 desc RANGE BETWEEN 3 seconds preceding and current row), " +
                "lead(usage_system) OVER (partition by hostname order by ts1 desc RANGE BETWEEN 3 seconds preceding and current row), " +
                "lag(usage_system) OVER (partition by hostname order by ts1 desc RANGE BETWEEN 3 seconds preceding and current row), " +
                "dense_rank() OVER (partition by hostname order by ts1 desc RANGE BETWEEN 3 seconds preceding and current row) " +
                "from (select * from cpu_ts order by ts1 desc))" +
                "order by ts1 desc";
        assertPlanNoLeakCheck(
                q6,
                """
                        CachedWindow
                          unorderedFunctions: [row_number() over (partition by [hostname]),rank() over (partition by [hostname]),lead(usage_system, 1, NULL) over (partition by [hostname]),lag(usage_system, 1, NULL) over (partition by [hostname]),dense_rank() over (partition by [hostname])]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: cpu_ts
                        """
        );
        assertSql("""
                ts1\thostname\tusage_system\trow_number\trank\tlead\tlag\tdense_rank
                1970-01-01T00:00:00.000010Z\tB\t10.0\t1\t1\t8.0\tnull\t1
                1970-01-01T00:00:00.000009Z\tA\t9.0\t1\t1\t2.0\tnull\t1
                1970-01-01T00:00:00.000008Z\tB\t8.0\t2\t2\t3.0\t10.0\t2
                1970-01-01T00:00:00.000007Z\tC\t7.0\t1\t1\t6.0\tnull\t1
                1970-01-01T00:00:00.000006Z\tC\t6.0\t2\t2\t5.0\t7.0\t2
                1970-01-01T00:00:00.000005Z\tC\t5.0\t3\t3\t4.0\t6.0\t3
                1970-01-01T00:00:00.000004Z\tC\t4.0\t4\t4\tnull\t5.0\t4
                1970-01-01T00:00:00.000003Z\tB\t3.0\t3\t3\tnull\t8.0\t3
                1970-01-01T00:00:00.000002Z\tA\t2.0\t2\t2\t1.0\t9.0\t2
                1970-01-01T00:00:00.000001Z\tA\t1.0\t3\t3\tnull\t2.0\t3
                """, q6);
    }

    private void testRewriteTrivialExpressions(boolean aliasExpressionsEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(aliasExpressionsEnabled));
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE hits (
                                ClientIP ipv4,
                                EventTime timestamp
                            ) TIMESTAMP(EventTime) PARTITION BY DAY;"""
            );
            execute("INSERT INTO hits VALUES('198.162.0.11', '2021-01-01T12:34:00');");
            execute("INSERT INTO hits VALUES('198.162.0.12', '2021-01-01T12:35:00');");

            final String query = "SELECT ClientIP, clientip - 1, ClientIP - 2, -3 + Clientip cip3, count(*) AS c " +
                    "FROM hits " +
                    "GROUP BY ClientIP, clientip - 1, clientIP - 2, -3 + Clientip " +
                    "ORDER BY c DESC " +
                    "LIMIT 10;";

            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [ClientIP,ClientIP-1,ClientIP-2,-3+ClientIP,c]
                                Long Top K lo: 10
                                  keys: [c desc]
                                    Async Group By workers: 1
                                      keys: [ClientIP]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: hits
                            """
            );

            final String expectedColumns = aliasExpressionsEnabled
                    ? "ClientIP\tclientip - 1\tClientIP - 2\tcip3\tc\n"
                    : "ClientIP\tcolumn\tcolumn1\tcip3\tc\n";
            assertQueryNoLeakCheck(
                    expectedColumns +
                            "198.162.0.11\t198.162.0.10\t198.162.0.9\t198.162.0.8\t1\n" +
                            "198.162.0.12\t198.162.0.11\t198.162.0.10\t198.162.0.9\t1\n",
                    query,
                    null,
                    true,
                    true
            );
        });
    }

    protected QueryModel compileModel(String query) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            assertEquals(ExecutionModel.QUERY, model.getModelType());
            return (QueryModel) model;
        }
    }
}
