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

import io.questdb.cairo.sql.RecordCursorFactory;
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

    final String orderByAdviceDdl = "CREATE TABLE t (\n" +
            "  s SYMBOL index,\n" +
            "  ts TIMESTAMP\n" +
            ") timestamp(ts) PARTITION BY DAY;";

    final String orderByAdviceDml =
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
            final QueryModel model = compileModel(query, ExecutionModel.QUERY);
            TestUtils.assertEquals("select-group-by x1, sum(x1) sum from (select-choose [x x1] x x1 from (select [x] from y))", model.toString0());
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
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
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
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
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
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
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
            assert aliasAppearsInFuncArgs(model, "x1", sqlNodeStack);
            assertPlan(
                    query,
                    "GroupBy vectorized: true workers: 1\n" +
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
            ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
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
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: hits\n";

            assertSql(expectedSql, q1);
            assertSql(expectedSql, q2);
            assertSql(expectedSql, q3);
            assertSql(expectedSql, q4);

            assertPlan(q1, expectedPlan);
            assertPlan(q2, expectedPlan);
            assertPlan(q3, expectedPlan);
            assertPlan(q4, expectedPlan);

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

            assertPlan(
                    "select a, b\n" +
                            "            from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "            order by a, b",
                    "Sort\n" +
                            "  keys: [a, b]\n" +
                            "    SelectedRecord\n" +
                            "        Hash Join Light\n" +
                            "          condition: tab2.id=tab1.id\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab1\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
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
            assertPlan(
                    "select tab1.id, tab1.ts as b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by b",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: tab2.id=tab1.id\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab1\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
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
            assertPlan(
                    "select tab1.id, tab1.ts as b\n" +
                            "from tab1 join tab2 on tab1.id = tab2.id\n" +
                            "order by 2",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: tab2.id=tab1.id\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab1\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
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

            assertPlan(
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
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab1\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
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

            assertPlan(
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
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab1\n" +
                            "                    Hash\n" +
                            "                        DataFrame\n" +
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

            assertPlan(
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
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: WorkflowEventAction\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
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
                            "2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t24024\t19\t1\t2016-01-01T00:00:00.000000Z\t00000000-0000-0001-0000-000000000001\t13\t2\t\t\tNaN\t\n",

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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s1, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts]\n" +
                    "        SelectedRecord\n" +
                    "            AsOf Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T00:00:00.000000Z\")]\n" +
                    "            DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "            DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts, ts1, s1]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Cross Join\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "            DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [ts, s1, s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            Cross Join\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    Sort\n" +
                    "      keys: [s, ts1]\n" +
                    "        SelectedRecord\n" +
                    "            Lt Join Light\n" +
                    "              condition: t2.s=t1.s\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Interval forward scan on: t1\n" +
                    "                      intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "                DataFrame\n" +
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
                    "ORDER BY t1.s, t1.ts\n" +
                    "LIMIT 1000000;";

            assertQuery("select-choose t1.s s, t1.ts ts, t2.s s1, t2.ts ts1 from (select [s, ts] from t1 timestamp (ts) join select [s, ts] from t2 timestamp (ts) on t2.s = t1.s where ts between ('2023-09-01T00:00:00.000Z','2023-09-01T01:00:00.000Z')) order by s, ts limit 1000000", query);
            assertPlan(query, "Limit lo: 1000000\n" +
                    "    SelectedRecord\n" +
                    "        Hash Join Light\n" +
                    "          condition: t2.s=t1.s\n" +
                    "            SortedSymbolIndex\n" +
                    "                Index forward scan on: s\n" +
                    "                  symbolOrder: asc\n" +
                    "                Interval forward scan on: t1\n" +
                    "                  intervals: [(\"2023-09-01T00:00:00.000000Z\",\"2023-09-01T01:00:00.000000Z\")]\n" +
                    "            Hash\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n");
            assertSql("s\tts\ts1\tts1\n" +
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
                    "c\t2023-09-01T01:00:00.000000Z\tc\t2023-09-01T03:00:00.000000Z\n", query);
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
            assertPlan(query, "Sort light lo: 1000000\n" +
                    "  keys: [s, ts]\n" +
                    "    DataFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            try (RecordCursorFactory ignored = select(query, sqlExecutionContext)) {
            }
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
            assertPlan(query, "Sort light lo: -10\n" +
                    "  keys: [s, ts]\n" +
                    "    DataFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            try (RecordCursorFactory ignored = select(query, sqlExecutionContext)) {
            }
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
            assertPlan(query, "Sort light lo: -10 partiallySorted: true\n" +
                    "  keys: [ts, s]\n" +
                    "    DataFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: t1\n");

            try (RecordCursorFactory ignored = select(query, sqlExecutionContext)) {
            }
        });
    }

    @Test
    public void testOrderingOfSortsInSingleTimestampCase() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            insert("insert into a select x::int as i, x::timestamp as ts from long_sequence(10000)");

            assertPlan(
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
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n"
            );

            assertQuery("i\tts\ti1\tts1\n" +
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

    protected QueryModel compileModel(String query, int modelType) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            Assert.assertEquals(model.getModelType(), modelType);
            return (QueryModel) model;
        }
    }
}
