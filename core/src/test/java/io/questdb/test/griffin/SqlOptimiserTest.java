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

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlOptimiser.aliasAppearsInFuncArgs;

public class SqlOptimiserTest extends AbstractSqlParserTest {

    @Test
    public void testAliasAppearsInFuncArgs1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<ExpressionNode>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [x1]\n" +
                            "  values: [sum(x1)]\n" +
                            "    SelectedRecord\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select concat(lpad(cast(x1 as string), 5)), x1, sum(x1) from (select x x1 from y)";
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-group-by concat, x1, sum(x1) sum from (select-virtual [concat(lpad(cast(x1,string),5)) concat, x1] concat(lpad(cast(x1,string),5)) concat, x1 from (select-choose [x x1] x x1 from (select [x] from y)))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assert aliasAppearsInFuncArgs(model.getNestedModel(), "x1", sqlNodeStack);
            assert !aliasAppearsInFuncArgs(model.getNestedModel().getNestedModel(), "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  keys: [concat,x1]\n" +
                            "  values: [sum(x1)]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [concat([lpad(x1::string,5)]),x1]\n" +
                            "        SelectedRecord\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select concat(lpad(cast(x1 as string), 5)), x1 from (select x x1 from y) group by x1";
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-virtual concat(lpad(cast(x1,string),5)) concat, x1 from (select-group-by [x1] x1 from (select-choose [x x1] x x1 from (select [x] from y)))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<ExpressionNode>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "VirtualRecord\n" +
                            "  functions: [concat([lpad(x1::string,5)]),x1]\n" +
                            "    GroupBy vectorized: true workers: 1\n" +
                            "      keys: [x1]\n" +
                            "      values: [count(*)]\n" +
                            "        SelectedRecord\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: y\n");
        });
    }

    @Test
    public void testAliasAppearsInFuncArgs4() throws Exception {
        // check aliases are case insensitive
        assertMemoryLeak(() -> {
            ddl("create table y ( x int );");
            final String query = "select x1, sum(x1), max(X1) from (select x X1 from y)";
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum, max(x1) max from (select-choose [x X1] x X1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<ExpressionNode>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
                            "  keys: [X1]\n" +
                            "  values: [sum(X1),max(X1)]\n" +
                            "    SelectedRecord\n" +
                            "        DataFrame\n" +
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
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-group-by sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<ExpressionNode>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "GroupBy vectorized: true\n" +
                            "  values: [sum(x1)]\n" +
                            "    SelectedRecord\n" +
                            "        DataFrame\n" +
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
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-choose x1 from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<ExpressionNode>();
            assert !aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "SelectedRecord\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: y\n");
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            ddl("CREATE TABLE t2 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            compile(query);
        });
    }

    @Test
    public void testOrderByAdviceWorksWithAsofJoin2() throws Exception {
        // Case when order by is for more than one table prefix
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            ddl("CREATE TABLE t2 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "    FROM t1\n" +
                    "    ASOF JOIN t2 ON t1.s = t2.s\n" +
                    "    WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "    ORDER BY t1.s, t2.ts\n" +
                    "    LIMIT 1000000;";

            select(query);
        });
    }





    @Test
    public void testOrderByAdviceWorksWithRegularJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            ddl("CREATE TABLE t2 (\n" +
                    "  s SYMBOL index,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp(ts) PARTITION BY DAY;");

            final String query = "SELECT t1.s, t1.ts, t2.s, t2.ts\n" +
                    "FROM t1\n" +
                    "JOIN t2 ON t1.s = t2.s\n" +
                    "WHERE t1.ts in '2023-09-01T00:00:00.000Z' AND t1.ts <= '2023-09-01T01:00:00.000Z'\n" +
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            compile(query);
        });
    }

    protected QueryModel compileModel(String query, int modelType) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            Assert.assertEquals(model.getModelType(), modelType);
            return (QueryModel)model;
        }
    }
}


