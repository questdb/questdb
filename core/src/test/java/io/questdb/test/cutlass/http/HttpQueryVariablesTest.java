/*+*****************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The {@code variables} parameter of {@code /exec}: variables a client declares outside the
 * query's text, which the query reads as a DECLARE block that opens it.
 */
public class HttpQueryVariablesTest extends AbstractTest {
    private static final VariablesHttpClient testHttpClient = new VariablesHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        testHttpClient.setKeepConnection(false);
    }

    @Test
    public void testCachedQueryWithOtherValues() throws Exception {
        // The select cache keeps a compiled query by its text. The compiled query holds the
        // values of the variables, so the same text with other values must not reuse it.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            engine.execute("INSERT INTO t VALUES (1), (2), (3)", sqlExecutionContext);
            final String sql = "SELECT v FROM t WHERE v > @min";
            for (int i = 0; i < 2; i++) {
                assertGet(
                        "{\"query\":\"SELECT v FROM t WHERE v > @min\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[2],[3]],\"count\":2}",
                        sql,
                        "[{\"name\":\"min\",\"value\":\"1\"}]"
                );
                assertGet(
                        "{\"query\":\"SELECT v FROM t WHERE v > @min\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[3]],\"count\":1}",
                        sql,
                        "[{\"name\":\"min\",\"value\":\"2\"}]"
                );
            }
            // the text without variables is another query again
            assertGet(
                    "{\"query\":\"SELECT v FROM t WHERE v > @min\",\"error\":\"Invalid column: @min\",\"position\":26}",
                    sql,
                    null
            );
        }));
    }

    @Test
    public void testCreateIgnoresVariables() throws Exception {
        // CREATE TABLE AS and views keep their text, without the variables: each answers as it
        // does without the parameter.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final String variables = "[{\"name\":\"a\",\"value\":\"1\"}]";
            assertSameResponse("CREATE TABLE x AS (SELECT @a v)", variables);
            assertSameResponse("CREATE VIEW vw AS (SELECT @a v)", variables);
            assertSameResponse("CREATE VIEW vw AS SELECT @a v", variables);
            final Utf8StringSink sink = new Utf8StringSink();
            testHttpClient.get("CREATE TABLE x AS (SELECT @a v)", variables, sink);
            TestUtils.assertContains(sink.toString(), "\"error\"");
        }));
    }

    @Test
    public void testErrorWithoutPosition() throws Exception {
        // an error in another parameter carries no position, with the variables as without
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
            params.put("cols", "y");
            final Utf8StringSink without = new Utf8StringSink();
            testHttpClient.get("SELECT 1 x", null, params, without);
            final Utf8StringSink with = new Utf8StringSink();
            testHttpClient.get("SELECT 1 x", "[{\"name\":\"a\",\"value\":\"1\"}]", params, with);
            TestUtils.assertContains(with.toString(), "column not found: 'y'");
            TestUtils.assertEquals(without.toString(), with.toString());
        }));
    }

    @Test
    public void testErrorWhileRowsStream() throws Exception {
        // simulate_crash('0') fails at the first row with the fixed position 222
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final String sql = "SELECT simulate_crash('0') FROM long_sequence(1)";
            final String prefix = "{\"query\":\"SELECT simulate_crash('0') FROM long_sequence(1)\",\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}],\"timestamp\":-1,\"dataset\":[[]],\"count\":1,\"error\":\"simulated cairo exception\", ";
            // a position past the variables' text is left as it is
            assertGet(prefix + "\"errorPos\":222}", sql, "[{\"name\":\"pad\",\"value\":\"1\"}]");
            // Within it, it is one in the variable's value. The assignment "@pad := '...'" starts
            // past the query and a newline, at 49, and its value 8 characters later, so 222 is
            // 222 - 49 - 8 = 165 characters into the value.
            final String pad = "'" + "x".repeat(200) + "'";
            assertGet(
                    prefix + "\"errorPos\":165,\"errorSource\":\"variables\",\"variable\":\"pad\"}",
                    sql,
                    "[{\"name\":\"pad\",\"value\":\"" + pad + "\"}]"
            );
        }));
    }

    @Test
    public void testErrorInQueryKeepsItsPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> assertGet(
                "{\"query\":\"SELECT @a, nope_fn()\",\"error\":\"unknown function name: nope_fn()\",\"position\":11}",
                "SELECT @a, nope_fn()",
                "[{\"name\":\"a\",\"value\":\"1\"}]"
        )));
    }

    @Test
    public void testErrorInVariableValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // the position is in the value of the variable the error lies in
            assertGet(
                    "{\"query\":\"SELECT @a + @b\",\"error\":\"unknown function name: nope_fn()\",\"position\":4,\"errorSource\":\"variables\",\"variable\":\"b\"}",
                    "SELECT @a + @b",
                    "[{\"name\":\"a\",\"value\":\"1\"},{\"name\":\"b\",\"value\":\"1 + nope_fn()\"}]"
            );
            // a value that reads a later variable
            assertGet(
                    "{\"query\":\"SELECT @a\",\"error\":\"tried to use undeclared variable `@b`\",\"position\":0,\"errorSource\":\"variables\",\"variable\":\"a\"}",
                    "SELECT @a",
                    "[{\"name\":\"a\",\"value\":\"@b\"},{\"name\":\"b\",\"value\":\"1\"}]"
            );
            // a value with a query after it
            assertGet(
                    "{\"query\":\"SELECT @a\",\"error\":\"unexpected token [SELECT]\",\"position\":2,\"errorSource\":\"variables\",\"variable\":\"a\"}",
                    "SELECT @a",
                    "[{\"name\":\"a\",\"value\":\"1 SELECT 2\"}]"
            );
        }));
    }

    @Test
    public void testInsertReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT, s VARCHAR)", sqlExecutionContext);
            final String variables = "[{\"name\":\"v\",\"value\":\"40 + 2\"},{\"name\":\"s\",\"value\":\"'x' || @v\"}]";
            assertGet("{\"dml\":\"OK\"}", "INSERT INTO t SELECT @v, @s", variables);
            assertGet("{\"dml\":\"OK\"}", "WITH c AS (SELECT @v v) INSERT INTO t SELECT v, 'cte' FROM c", variables);
            assertGet(
                    "{\"query\":\"SELECT * FROM t\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"},{\"name\":\"s\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[42,\"x42\"],[42,\"cte\"]],\"count\":2}",
                    "SELECT * FROM t",
                    null
            );
        }));
    }

    @Test
    public void testInvalidVariablesParameter() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // the position is in the parameter's text; no variable is named
            final Utf8StringSink sink = new Utf8StringSink();
            Assert.assertEquals("400", testHttpClient.get("SELECT 1", "[{\"name\":\"1a\",\"value\":\"1\"}]", sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT 1\",\"error\":\"invalid variable name [name=1a]; a name starts with a letter or '_' and holds letters, digits and '_' only\",\"position\":9,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
            Assert.assertEquals("400", testHttpClient.get("SELECT 1", "{\"a\":1}", sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT 1\",\"error\":\"variables must be a JSON array of {\\\"name\\\": ..., \\\"value\\\": ...} objects\",\"position\":0,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
        }));
    }

    @Test
    public void testInvalidUtf8InVariablesParameter() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final CharSequenceObjHashMap<Utf8Sequence> params = new CharSequenceObjHashMap<>();
            final Utf8StringSink badUtf8 = new Utf8StringSink();
            badUtf8.put("[").putAny((byte) 0xC3).putAny((byte) 0x28);
            params.put("variables", badUtf8);
            // this path sends the parameters as they are, unencoded
            final Utf8StringSink sql = new Utf8StringSink();
            sql.put("SELECT%201");
            testHttpClient.assertGet(
                    "/query",
                    "{\"query\":\"SELECT 1\",\"error\":\"variables are not valid UTF-8\",\"position\":0,\"errorSource\":\"variables\"}",
                    sql,
                    params
            );
        }));
    }

    @Test
    public void testSelectReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // the same as DECLARE @from := ..., @to := dateadd('d', 1, @from) SELECT ...
            assertGet(
                    "{\"query\":\"SELECT @from f, @to t\",\"columns\":[{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"t\",\"type\":\"TIMESTAMP\"}],\"timestamp\":-1,\"dataset\":[[\"2026-11-11T00:00:00.000000Z\",\"2026-11-12T00:00:00.000000Z\"]],\"count\":1}",
                    "SELECT @from f, @to t",
                    "[{\"name\":\"from\",\"value\":\"'2026-11-11'::TIMESTAMP\"},{\"name\":\"to\",\"value\":\"dateadd('d', 1, @from)\"}]"
            );
            // a value with a double quote and a line comment
            assertGet(
                    "{\"query\":\"SELECT @a x\",\"columns\":[{\"name\":\"x\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"say \\\"hi\\\"\"]],\"count\":1}",
                    "SELECT @a x",
                    "[{\"name\":\"a\",\"value\":\"'say \\\"hi\\\"' -- quoted\"}]"
            );
            // an empty array declares nothing
            assertGet(
                    "{\"query\":\"SELECT 1 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    "SELECT 1 x",
                    "[]"
            );
        }));
    }

    @Test
    public void testUpdateIgnoresVariables() throws Exception {
        // The WAL applies an UPDATE by parsing its text again, without the variables: UPDATE
        // answers as it does without the parameter.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            final String variables = "[{\"name\":\"a\",\"value\":\"1\"}]";
            assertSameResponse("UPDATE t SET v = @a", variables);
            assertSameResponse("WITH c AS (SELECT @a x) UPDATE t SET v = c.x FROM c", variables);
        }));
    }

    private static void assertGet(CharSequence expected, CharSequence sql, @Nullable String variables) {
        final Utf8StringSink sink = new Utf8StringSink();
        testHttpClient.get(sql, variables, sink);
        TestUtils.assertEquals(expected, sink.toString());
    }

    // the statement answers with the variables as it does without them
    private static void assertSameResponse(String sql, String variables) {
        final Utf8StringSink without = new Utf8StringSink();
        final String statusWithout = testHttpClient.get(sql, null, without);
        final Utf8StringSink with = new Utf8StringSink();
        final String statusWith = testHttpClient.get(sql, variables, with);
        Assert.assertEquals(statusWithout, statusWith);
        TestUtils.assertEquals(without.toString(), with.toString());
    }

    private static class VariablesHttpClient extends TestHttpClient {
        // returns the status code
        String get(CharSequence sql, @Nullable String variables, Utf8StringSink sink) {
            return get(sql, variables, null, sink);
        }

        String get(CharSequence sql, @Nullable String variables, @Nullable CharSequenceObjHashMap<String> params, Utf8StringSink sink) {
            try {
                final HttpClient.Request req = getHttpClient().newRequest("localhost", 9001);
                req.GET().url("/query").query("query", sql);
                if (variables != null) {
                    req.query("variables", variables);
                }
                if (params != null) {
                    for (int i = 0, n = params.size(); i < n; i++) {
                        final CharSequence name = params.keys().getQuick(i);
                        req.query(name, params.get(name));
                    }
                }
                final String status = reqToSink(req, sink, null, null, null, null);
                Assert.assertTrue(Utf8s.toString(sink), sink.size() > 0);
                return status;
            } finally {
                disconnect();
            }
        }
    }
}
