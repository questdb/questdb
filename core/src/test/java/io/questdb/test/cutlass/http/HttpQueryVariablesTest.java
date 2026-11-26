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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorState;
import io.questdb.cutlass.http.processors.SqlValidationProcessorState;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * The {@code variables} parameter of {@code /exec}, {@code /exp} and
 * {@code /api/v1/sql/validate}: variables a client declares outside the query's text, as the
 * assignments of a DECLARE block, which the query reads as a DECLARE block that opens it.
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
    public void testAlterExpirePolicyIgnoresVariables() throws Exception {
        // ALTER checks a window EXPIRE ROWS policy with a query of its own, which compiles while the
        // statement compiles. The view keeps the policy as text, without the variables, so the
        // statement fails as it does without the parameter, and the view keeps no policy.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", sqlExecutionContext);
            engine.execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)", sqlExecutionContext);
            final String sql = "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k) - @k";
            for (String url : new String[]{"/query", "/api/v1/sql/validate"}) {
                final String response = assertSameResponse(url, sql, "@k := 1.0");
                TestUtils.assertContains(response, "\"error\":\"invalid EXPIRE ROWS policy: Invalid column: @k\"");
            }
            // The view takes a policy from its WAL, and every read of the view reads the policy.
            // One the statement stored would read @k, which nothing declares there, and fail the read.
            drainWalQueue(engine);
            assertGet(
                    "{\"query\":\"SELECT count() FROM mv\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[0]],\"count\":1}",
                    "SELECT count() FROM mv",
                    null
            );
        }));
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
                        "@min := 1"
                );
                assertGet(
                        "{\"query\":\"SELECT v FROM t WHERE v > @min\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[3]],\"count\":1}",
                        sql,
                        "@min := 2"
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
    public void testCachedViewReadWithAndWithoutVariables() throws Exception {
        // A view's OVERRIDABLE variable makes the same text compile with and without the
        // parameter, to queries that return other rows. The select cache that /exec and /exp
        // share keeps them apart, whichever request compiles the text first.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE k (l LONG)", sqlExecutionContext);
            engine.execute("INSERT INTO k VALUES (1), (2), (3)", sqlExecutionContext);
            engine.execute("CREATE VIEW v_ovl AS (DECLARE OVERRIDABLE @lim := 2 SELECT l FROM k WHERE l <= @lim)", sqlExecutionContext);
            final String sql = "SELECT * FROM v_ovl";
            final String variables = "@lim := 1";
            final String plainJson = "{\"query\":\"SELECT * FROM v_ovl\",\"columns\":[{\"name\":\"l\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1],[2]],\"count\":2}";
            final String overriddenJson = "{\"query\":\"SELECT * FROM v_ovl\",\"columns\":[{\"name\":\"l\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}";
            for (int i = 0; i < 2; i++) {
                // without the parameter first, then with it, on each endpoint
                assertGet(plainJson, sql, null);
                assertGet(overriddenJson, sql, variables);
                assertSameExport("SELECT l FROM k WHERE l <= 2", sql, null, null);
                assertSameExport("SELECT l FROM k WHERE l <= 1", sql, variables, null);
            }
        }));
    }

    @Test
    public void testCreateIgnoresVariables() throws Exception {
        // CREATE TABLE AS and views keep their text, without the variables: each answers as it
        // does without the parameter.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final String variables = "@a := 1";
            assertSameResponse("CREATE TABLE x AS (SELECT @a v)", variables);
            assertSameResponse("CREATE VIEW vw AS (SELECT @a v)", variables);
            assertSameResponse("CREATE VIEW vw AS SELECT @a v", variables);
            final Utf8StringSink sink = new Utf8StringSink();
            testHttpClient.get("CREATE TABLE x AS (SELECT @a v)", variables, sink);
            TestUtils.assertContains(sink.toString(), "\"error\"");
            // they do not parse the variables either: a parameter that holds no assignment
            // fails no CREATE
            final String response = assertSameResponse("/query", "CREATE TABLE IF NOT EXISTS z (a INT)", "a := 1");
            TestUtils.assertEquals("{\"ddl\":\"OK\"}", response);
        }));
    }

    @Test
    public void testErrorInQueryKeepsItsPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            assertGet(
                    "{\"query\":\"SELECT @a, nope_fn()\",\"error\":\"unknown function name: nope_fn()\",\"position\":11}",
                    "SELECT @a, nope_fn()",
                    "@a := 1"
            );
            // A query cannot have both GROUP BY and SAMPLE BY, whatever the variables hold. The
            // error points at GROUP BY in the query, also when a variable supplies the key.
            engine.execute("CREATE TABLE t (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
            final String sql = "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY @k";
            for (String url : new String[]{"/query", "/exp", "/api/v1/sql/validate"}) {
                final Utf8StringSink sink = new Utf8StringSink();
                Assert.assertEquals("400", testHttpClient.get(url, sql, "@k := s", null, sink));
                TestUtils.assertEquals(
                        "{\"query\":\"" + sql + "\",\"error\":\"SELECT query must not contain both GROUP BY and SAMPLE BY\",\"position\":"
                                + sql.indexOf("GROUP BY") + '}',
                        sink.toString()
                );
            }
        }));
    }

    @Test
    public void testErrorInVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // the position is in the variables parameter: "@a := 1, @b := 1 + " is 19 characters
            assertGet(
                    "{\"query\":\"SELECT @a + @b\",\"error\":\"unknown function name: nope_fn()\",\"position\":19,\"errorSource\":\"variables\"}",
                    "SELECT @a + @b",
                    "@a := 1, @b := 1 + nope_fn()"
            );
            // a value that reads a later variable
            assertGet(
                    "{\"query\":\"SELECT @a\",\"error\":\"tried to use undeclared variable `@b`\",\"position\":6,\"errorSource\":\"variables\"}",
                    "SELECT @a",
                    "@a := @b, @b := 1"
            );
            // a value with a query after it
            assertGet(
                    "{\"query\":\"SELECT @a\",\"error\":\"unexpected token [SELECT]\",\"position\":8,\"errorSource\":\"variables\"}",
                    "SELECT @a",
                    "@a := 1 SELECT 2"
            );
            // after characters of 2, 3 and 4 bytes in UTF-8, the position counts UTF-16 characters,
            // as positions in the query do
            final String multiByte = "@s := 'é€\uD83D\uDE00', @b := nope_fn()";
            assertGet(
                    "{\"query\":\"SELECT @s s, @b b\",\"error\":\"unknown function name: nope_fn()\",\"position\":"
                            + multiByte.indexOf("nope_fn") + ",\"errorSource\":\"variables\"}",
                    "SELECT @s s, @b b",
                    multiByte
            );
            // at the very end of the variables, 19 characters into "@a := now() at time"
            assertGet(
                    "{\"query\":\"SELECT 1\",\"error\":\"did you mean 'at time zone <tz>'?\",\"position\":19,\"errorSource\":\"variables\"}",
                    "SELECT 1",
                    "@a := now() at time"
            );
            // at the very end of the query, which is no position in the variables
            assertGet(
                    "{\"query\":\"SELECT * FROM\",\"error\":\"table name or sub-query expected\",\"position\":13}",
                    "SELECT * FROM",
                    "@a := 1"
            );
            // not an assignment
            final Utf8StringSink sink = new Utf8StringSink();
            Assert.assertEquals("400", testHttpClient.get("SELECT 1", "a := 1", sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT 1\",\"error\":\"unexpected token [a]\",\"position\":0,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
        }));
    }

    @Test
    public void testErrorInViewIsNoErrorInVariables() throws Exception {
        // An error in the body of a view the query reads carries a position in the view's text,
        // past the end of the query's here. It is no error in the variables, however long they
        // are: each endpoint answers as it does without the parameter.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (a INT, b INT, c INT, k INT)", sqlExecutionContext);
            engine.execute("CREATE VIEW v AS (SELECT a, b, c, k FROM t)", sqlExecutionContext);
            engine.execute("ALTER TABLE t DROP COLUMN k", sqlExecutionContext);
            engine.execute("CREATE TABLE t2 (u CHAR, n INT, ts TIMESTAMP)", sqlExecutionContext);
            engine.execute("INSERT INTO t2 VALUES ('d', null, '2024-01-01')", sqlExecutionContext);
            engine.execute("CREATE VIEW vd AS (SELECT dateadd(u, n, ts) d FROM t2)", sqlExecutionContext);
            final String variables = "@pad := '" + "x".repeat(100) + "'";
            for (String url : new String[]{"/query", "/exp", "/api/v1/sql/validate"}) {
                // the view's body reads a column that is gone, which the compile finds
                final String response = assertSameResponse(url, "SELECT * FROM v", variables);
                TestUtils.assertContains(response, "\"error\":\"Invalid column: k\",\"position\":16}");
            }
            // a function of the view's body fails on the first row
            final String response = assertSameResponse("/query", "SELECT * FROM vd", variables);
            TestUtils.assertContains(response, "\"error\":\"`null` is not a valid stride\", \"errorPos\":18}");
        }));
    }

    @Test
    public void testErrorWhileRowsStream() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // A function from the variables fails at the first row, after the response has begun:
            // the error points into the variables parameter. dateadd() reports a NULL stride at the
            // stride, n here.
            engine.execute("CREATE TABLE t2 (u CHAR, n INT, ts TIMESTAMP)", sqlExecutionContext);
            engine.execute("INSERT INTO t2 VALUES ('d', null, '2024-01-01')", sqlExecutionContext);
            final String variables = "@d := dateadd(u, n, ts)";
            assertGet(
                    "{\"query\":\"SELECT @d d FROM t2\",\"columns\":[{\"name\":\"d\",\"type\":\"TIMESTAMP\"}],\"timestamp\":-1,\"dataset\":[[]],\"count\":1,\"error\":\"`null` is not a valid stride\", \"errorPos\":"
                            + variables.indexOf("n, ts") + ",\"errorSource\":\"variables\"}",
                    "SELECT @d d FROM t2",
                    variables
            );
            // simulate_crash('0') fails at the first row with the fixed position 222, which is no
            // position in the variables, however long they are
            final String sql = "SELECT simulate_crash('0') FROM long_sequence(1)";
            final String expected = "{\"query\":\"SELECT simulate_crash('0') FROM long_sequence(1)\",\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}],\"timestamp\":-1,\"dataset\":[[]],\"count\":1,\"error\":\"simulated cairo exception\", \"errorPos\":222}";
            assertGet(expected, sql, "@pad := 1");
            assertGet(expected, sql, "@pad := '" + "x".repeat(200) + "'");
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
            testHttpClient.get("SELECT 1 x", "@a := 1", params, with);
            TestUtils.assertContains(with.toString(), "column not found: 'y'");
            TestUtils.assertEquals(without.toString(), with.toString());
        }));
    }

    @Test
    public void testExportCsvReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            // as the same query with the values written in it
            assertSameExport(
                    "SELECT 1 x, 'a&b?c=d+e#f%g' y FROM long_sequence(2)",
                    "SELECT @a x, @b y FROM long_sequence(2)",
                    "@a := 1, @b := 'a&b?c=d+e#f%g'",
                    null
            );
            // the select cache keeps the variables apart
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            engine.execute("INSERT INTO t VALUES (1), (2), (3)", sqlExecutionContext);
            for (int i = 0; i < 2; i++) {
                assertSameExport("SELECT v FROM t WHERE v > 1", "SELECT v FROM t WHERE v > @min", "@min := 1", null);
                assertSameExport("SELECT v FROM t WHERE v > 2", "SELECT v FROM t WHERE v > @min", "@min := 2", null);
            }
            // the same text without the variables reads no @min, cached or not
            final Utf8StringSink sink = new Utf8StringSink();
            Assert.assertEquals("400", testHttpClient.get("/exp", "SELECT v FROM t WHERE v > @min", null, null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT v FROM t WHERE v > @min\",\"error\":\"Invalid column: @min\",\"position\":26}",
                    sink.toString()
            );
        }));
    }

    @Test
    public void testExportErrorInVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final Utf8StringSink sink = new Utf8StringSink();
            Assert.assertEquals("400", testHttpClient.get("/exp", "SELECT @a x FROM long_sequence(1)", "@a := nope_fn()", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT @a x FROM long_sequence(1)\",\"error\":\"unknown function name: nope_fn()\",\"position\":6,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
            // an error in the query keeps its position
            Assert.assertEquals("400", testHttpClient.get("/exp", "SELECT @a x, nope_fn()", "@a := 1", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT @a x, nope_fn()\",\"error\":\"unknown function name: nope_fn()\",\"position\":13}",
                    sink.toString()
            );
        }));
    }

    @Test
    public void testExportParquetReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final CharSequenceObjHashMap<String> parquet = new CharSequenceObjHashMap<>();
            parquet.put("fmt", "parquet");
            // streamed from the query's cursor
            assertSameExport("SELECT 7 v FROM long_sequence(3)", "SELECT @a v FROM long_sequence(3)", "@a := 7", parquet);
            // A computed BINARY column makes the export go through a temp table, which is
            // CREATE TABLE AS on the query's text: the variables reach it too.
            engine.execute("CREATE TABLE b AS (SELECT x, rnd_bin(4, 4, 0) bin FROM long_sequence(3))", sqlExecutionContext);
            assertSameExport(
                    "SELECT CASE WHEN x > 1 THEN bin END bin, 1 v FROM b",
                    "SELECT CASE WHEN x > @a THEN bin END bin, @a v FROM b",
                    "@a := 1",
                    parquet
            );
        }));
    }

    @Test
    public void testExportParquetResumesWithVariables() throws Exception {
        // A send buffer far smaller than the file makes the export stop whenever the buffer is
        // full and resume once the client has read it. The temp table, CREATE TABLE AS on the
        // query's text, reads the variables once, and the resumed export streams the whole file.
        TestUtils.assertMemoryLeak(() -> getSimpleTester()
                .withSendBufferSize(1024)
                .withForceSendFragmentationChunkSize(1024)
                .run((engine, sqlExecutionContext) -> {
                    final CharSequenceObjHashMap<String> parquet = new CharSequenceObjHashMap<>();
                    parquet.put("fmt", "parquet");
                    engine.execute("CREATE TABLE b AS (SELECT x, rnd_bin(64, 64, 0) bin FROM long_sequence(2_000))", sqlExecutionContext);
                    final Utf8StringSink file = assertSameExport(
                            "SELECT CASE WHEN x > 1 THEN bin END bin, 1 v FROM b",
                            "SELECT CASE WHEN x > @a THEN bin END bin, @a v FROM b",
                            "@a := 1",
                            parquet
                    );
                    // An export cut short would match a reference cut short at the same place. A
                    // whole file, here over a hundred send buffers, opens and closes with PAR1.
                    Assert.assertTrue(file.size() > 100 * 1024);
                    Assert.assertTrue(Utf8s.startsWithAscii(file, "PAR1"));
                    Assert.assertTrue(Utf8s.endsWithAscii(file, "PAR1"));
                }));
    }

    @Test
    public void testExportRecompilesStaleQueryWithVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            engine.execute("INSERT INTO t VALUES (1), (2), (3)", sqlExecutionContext);
            final String reference = "SELECT v FROM t WHERE v > 1";
            final String sql = "SELECT v FROM t WHERE v > @min";
            // the select cache keeps the compiled query
            assertSameExport(reference, sql, "@min := 1", null);
            // The new column makes the cached query stale: opening its cursor fails, and /exp
            // compiles the query again, with the variables.
            engine.execute("ALTER TABLE t ADD COLUMN extra INT", sqlExecutionContext);
            assertSameExport(reference, sql, "@min := 1", null);
        }));
    }

    @Test
    public void testInsertReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT, s VARCHAR)", sqlExecutionContext);
            final String variables = "@v := 40 + 2, @s := 'x' || @v";
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
    public void testInsertValuesIgnoresVariables() throws Exception {
        // VALUES lists the rows of an INSERT without a query to read the variables, with a WITH
        // clause ahead of it too, also one glued to a comment: each statement answers as it does
        // without the parameter, and does not parse it, so a parameter that holds no assignment
        // fails none
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            final String[] statements = {
                    "INSERT INTO t VALUES (1)",
                    "WITH c AS (SELECT 1 x) INSERT INTO t VALUES (2)",
                    "WITH-- comment\nc AS (SELECT 1 x) INSERT INTO t VALUES (3)",
            };
            for (String sql : statements) {
                TestUtils.assertEquals("{\"dml\":\"OK\"}", assertSameResponse("/query", sql, "a := 1"));
                TestUtils.assertContains(assertSameResponse("/api/v1/sql/validate", sql, "a := 1"), "\"queryType\":\"INSERT\"");
            }
            // /query ran each statement twice
            assertGet(
                    "{\"query\":\"SELECT v FROM t\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1],[1],[2],[2],[3],[3]],\"count\":6}",
                    "SELECT v FROM t",
                    null
            );
        }));
    }

    @Test
    public void testInvalidUtf8InVariablesParameter() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final CharSequenceObjHashMap<Utf8Sequence> params = new CharSequenceObjHashMap<>();
            final Utf8StringSink badUtf8 = new Utf8StringSink();
            badUtf8.put("@a%20%3A%3D%20").putAny((byte) 0xC3).putAny((byte) 0x28);
            params.put("variables", badUtf8);
            // this path sends the parameters as they are, unencoded
            final Utf8StringSink sql = new Utf8StringSink();
            sql.put("SELECT%201");
            for (String url : new String[]{"/query", "/exp", "/api/v1/sql/validate"}) {
                testHttpClient.assertGet(
                        url,
                        "{\"query\":\"SELECT 1\",\"error\":\"variables are not valid UTF-8\",\"position\":0,\"errorSource\":\"variables\"}",
                        sql,
                        params
                );
            }
        }));
    }

    @Test
    public void testLogsShowErrorPositions() throws Exception {
        // The log lines that show the position of an error show the one the response reports: an
        // error in the variables at its position in them, marked as the response marks it. The
        // loggers of the asserted lines wait for room in a full log ring instead of dropping them.
        LogFactory.enableGuaranteedLogging(QueryProgress.class, ExportQueryProcessor.class);
        final LogCapture capture = new LogCapture();
        try {
            TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
                capture.start();
                final String unknownFunction = "unknown function name: nope_fn()";
                for (String url : new String[]{"/query", "/exp"}) {
                    // the alias keeps the statements of the two endpoints apart in the log
                    final String alias = url.equals("/exp") ? "e" : "q";
                    // query, variables, error, its position in the query or the variables, and
                    // whether it lies in the variables
                    final Object[][] cases = {
                            // after the 6 characters of "@a := "
                            {"SELECT @a " + alias + 1, "@a := nope_fn()", unknownFunction, 6, true},
                            // at the first character of the variables
                            {"SELECT @a " + alias + 2, "a := 1", "unexpected token [a]", 0, true},
                            // in the query, with and without variables
                            {"SELECT @a " + alias + "3, nope_fn()", "@a := 1", unknownFunction, 14, false},
                            {"SELECT nope_fn() " + alias + 4, null, unknownFunction, 7, false},
                    };
                    for (Object[] c : cases) {
                        final String sql = (String) c[0];
                        final String variables = (String) c[1];
                        final String error = (String) c[2];
                        final int position = (int) c[3];
                        final boolean isInVariables = (boolean) c[4];
                        final Utf8StringSink sink = new Utf8StringSink();
                        Assert.assertEquals("400", testHttpClient.get(url, sql, variables, null, sink));
                        TestUtils.assertEquals(
                                "{\"query\":\"" + sql + "\",\"error\":\"" + error + "\",\"position\":" + position
                                        + (isInVariables ? ",\"errorSource\":\"variables\"" : "") + '}',
                                sink.toString()
                        );
                        final String logVariables = variables != null ? ", vars=`" + Pattern.quote(variables) + '`' : "";
                        final String logPosition = position + (isInVariables ? ", errorSource=variables" : "");
                        // the compile's error line, on both endpoints
                        final String errLine = "err \\[id=-1, sql=`" + Pattern.quote(sql) + '`' + logVariables
                                + ", principal=.*, msg=" + Pattern.quote(error) + ", errno=0, pos=" + logPosition + ']';
                        capture.waitForRegex(errLine);
                        capture.assertOnlyOnce(errLine);
                        if (url.equals("/exp")) {
                            final String syntaxErrorLine = "syntax-error \\[q=`" + Pattern.quote(sql) + '`' + logVariables
                                    + ", at=" + logPosition + ", message=`" + Pattern.quote(error) + "`]";
                            capture.waitForRegex(syntaxErrorLine);
                            capture.assertOnlyOnce(syntaxErrorLine);
                        }
                    }
                }
            }));
        } finally {
            capture.stop();
            LogFactory.disableGuaranteedLogging(QueryProgress.class, ExportQueryProcessor.class);
        }
    }

    @Test
    public void testLogsShowVariables() throws Exception {
        // The log lines that show a statement's text show the variables it ran with, so the log
        // tells what ran. A request without variables logs as before. The loggers of the asserted
        // lines wait for room in a full log ring instead of dropping them.
        LogFactory.enableGuaranteedLogging(QueryProgress.class, JsonQueryProcessorState.class, SqlValidationProcessorState.class);
        final LogCapture capture = new LogCapture();
        try {
            TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
                capture.start();
                engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
                // a query: when it starts and when it ends, and the timings line
                assertGet(
                        "{\"query\":\"SELECT @a + 1 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[2]],\"count\":1}",
                        "SELECT @a + 1 x",
                        "@a := 1"
                );
                // an INSERT, which runs after its compile
                assertGet("{\"dml\":\"OK\"}", "INSERT INTO t SELECT @v", "@v := 42");
                // an error
                assertGet(
                        "{\"query\":\"SELECT @a FROM nope\",\"error\":\"table does not exist [table=nope]\",\"position\":15}",
                        "SELECT @a FROM nope",
                        "@a := 1"
                );
                // the export and the validation endpoints
                assertSameExport("SELECT 3 z", "SELECT @c z", "@c := 3", null);
                final Utf8StringSink sink = new Utf8StringSink();
                Assert.assertEquals("200", testHttpClient.get("/api/v1/sql/validate", "SELECT @d w", "@d := 4", null, sink));
                // a request without variables after them
                assertGet(
                        "{\"query\":\"SELECT 2 y\",\"columns\":[{\"name\":\"y\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[2]],\"count\":1}",
                        "SELECT 2 y",
                        null
                );
                // the last line the requests log: one logging thread delivers them in order, so
                // every line asserted below has arrived once this one has
                capture.waitForRegex("timings \\[compiler: \\d+, count: \\d+, execute: \\d+, q=`SELECT 2 y`]");

                capture.assertOnlyOnce("exe \\[id=\\d+, sql=`SELECT @a \\+ 1 x`, vars=`@a := 1`, principal=");
                capture.assertOnlyOnce("fin \\[id=\\d+, sql=`SELECT @a \\+ 1 x`, vars=`@a := 1`, principal=");
                capture.assertOnlyOnce("timings \\[compiler: \\d+, count: \\d+, execute: \\d+, q=`SELECT @a \\+ 1 x`, vars=`@a := 1`]");
                capture.assertOnlyOnce("fin \\[id=\\d+, sql=`INSERT INTO t SELECT @v`, vars=`@v := 42`, principal=");
                capture.assertOnlyOnce("err \\[id=-1, sql=`SELECT @a FROM nope`, vars=`@a := 1`, principal=.*, msg=table does not exist \\[table=nope], errno=0, pos=15");
                capture.assertOnlyOnce("fin \\[id=\\d+, sql=`SELECT @c z`, vars=`@c := 3`, principal=");
                capture.assertOnlyOnce("timings \\[compiler: \\d+, q=`SELECT @d w`, vars=`@d := 4`]");
                capture.assertOnlyOnce("fin \\[id=\\d+, sql=`SELECT 2 y`, principal=");
                capture.assertOnlyOnce("timings \\[compiler: \\d+, count: \\d+, execute: \\d+, q=`SELECT 2 y`]");
                // the reference export ran without variables
                capture.assertOnlyOnce("fin \\[id=\\d+, sql=`SELECT 3 z`, principal=");
            }));
        } finally {
            capture.stop();
            LogFactory.disableGuaranteedLogging(QueryProgress.class, JsonQueryProcessorState.class, SqlValidationProcessorState.class);
        }
    }

    @Test
    public void testLogsShowVariablesOfErrorWithoutPosition() throws Exception {
        // The error line shows the request's variables for an error that carries no position as
        // well, such as the OutOfMemoryError that simulate_crash('M') throws, and shows JIT off.
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        final LogCapture capture = new LogCapture();
        try {
            TestUtils.assertMemoryLeak(() -> getSimpleTester().withJitMode(SqlJitMode.JIT_MODE_DISABLED).run((engine, sqlExecutionContext) -> {
                capture.start();
                // the filter runs while the cursor looks for its first row, so the cursor sees the error
                final String sql = "SELECT x FROM long_sequence(1) WHERE simulate_crash('M')";
                final Utf8StringSink sink = new Utf8StringSink();
                Assert.assertEquals("500", testHttpClient.get(sql, "@a := 1", sink));
                // an internal error reports position 0, which lies in the query, not in the variables
                TestUtils.assertEquals(
                        "{\"query\":\"" + sql + "\",\"error\":\"simulated OOM\",\"position\":0}",
                        sink.toString()
                );
                final String errLine = "err \\[id=\\d+, sql=`" + Pattern.quote(sql) + "`, vars=`@a := 1`, principal=.*, jit=false, time=\\d+, exception=java.lang.OutOfMemoryError: simulated OOM";
                capture.waitForRegex(errLine);
                capture.assertOnlyOnce(errLine);
            }));
        } finally {
            capture.stop();
            LogFactory.disableGuaranteedLogging(QueryProgress.class);
        }
    }

    @Test
    public void testNulLedQueryTakesNoCachedPlan() throws Exception {
        // The select cache keys a query sent with variables by a NUL, a 'V', the variables and the
        // text. A query text that starts with a NUL and spells that key compiles on its own, as it
        // does while the cache holds nothing.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            engine.execute("INSERT INTO t VALUES (1), (2)", sqlExecutionContext);
            // the key of "SELECT v FROM t WHERE v = @a" with "@a := 1", sent URL-encoded as the text
            final Utf8StringSink spelled = new Utf8StringSink();
            spelled.put("%00V7%3A%40a%20%3A%3D%201-SELECT%20v%20FROM%20t%20WHERE%20v%20%3D%20%40a");
            final String error = "{\"query\":\"\\u0000V7:@a := 1-SELECT v FROM t WHERE v = @a\",\"error\":\"too few arguments for ':=' [found=1,expected=2]\",\"position\":7}";
            testHttpClient.assertGet("/query", error, spelled, null);
            final String rows = "{\"query\":\"SELECT v FROM t WHERE v = @a\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}";
            for (int i = 0; i < 2; i++) {
                // the query with its variables leaves its compiled plan in the cache
                assertGet(rows, "SELECT v FROM t WHERE v = @a", "@a := 1");
                testHttpClient.assertGet("/query", error, spelled, null);
                testHttpClient.assertGet("/exp", error, spelled, null);
            }
        }));
    }

    @Test
    public void testRedefineViewIgnoresVariables() throws Exception {
        // ALTER VIEW and CREATE OR REPLACE VIEW over an existing view compile the new body while
        // the statement compiles. The body is stored as text, without the variables, so it fails
        // as it does without the parameter, and the view keeps its definition.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE VIEW vw AS (SELECT 1 v)", sqlExecutionContext);
            final String variables = "@a := 1";
            final String[] statements = {
                    "ALTER VIEW vw AS (SELECT @a v)",
                    "ALTER VIEW vw AS SELECT @a v",
                    "CREATE OR REPLACE VIEW vw AS (SELECT @a v)",
            };
            for (String url : new String[]{"/query", "/api/v1/sql/validate"}) {
                for (String sql : statements) {
                    final String response = assertSameResponse(url, sql, variables);
                    TestUtils.assertContains(response, "\"error\":\"Invalid column: @a\"");
                }
            }
            assertGet(
                    "{\"query\":\"SELECT * FROM vw\",\"columns\":[{\"name\":\"v\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    "SELECT * FROM vw",
                    null
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
                    "@from := '2026-11-11'::TIMESTAMP, @to := dateadd('d', 1, @from)"
            );
            // a value with a double quote and a line comment
            assertGet(
                    "{\"query\":\"SELECT @a x\",\"columns\":[{\"name\":\"x\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"say \\\"hi\\\"\"]],\"count\":1}",
                    "SELECT @a x",
                    "@a := 'say \"hi\"' -- quoted"
            );
            // the characters URL encoding gives a meaning arrive as they were sent
            assertGet(
                    "{\"query\":\"SELECT @s x\",\"columns\":[{\"name\":\"x\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"a&b?c=d+e#f%g h\"]],\"count\":1}",
                    "SELECT @s x",
                    "@s := 'a&b?c=d+e#f%g h'"
            );
            // characters of 2, 3 and 4 bytes in UTF-8 arrive as they were sent, and length() counts
            // their UTF-16 characters
            final String multiByte = "héllo€\uD83D\uDE00";
            assertGet(
                    "{\"query\":\"SELECT @s x, length(@s) l\",\"columns\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"l\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[\""
                            + multiByte + "\"," + multiByte.length() + "]],\"count\":1}",
                    "SELECT @s x, length(@s) l",
                    "@s := '" + multiByte + "'"
            );
            // an empty parameter declares nothing
            assertGet(
                    "{\"query\":\"SELECT 1 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    "SELECT 1 x",
                    ""
            );
        }));
    }

    @Test
    public void testUpdateIgnoresVariables() throws Exception {
        // The WAL applies an UPDATE by parsing its text again, without the variables: UPDATE
        // answers as it does without the parameter.
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            final String variables = "@a := 1";
            assertSameResponse("UPDATE t SET v = @a", variables);
            assertSameResponse("WITH c AS (SELECT @a x) UPDATE t SET v = c.x FROM c", variables);
            // nor does it parse them: a parameter that holds no assignment fails no UPDATE, also
            // after a WITH glued to a comment
            final String[] statements = {
                    "UPDATE t SET v = 1",
                    "WITH c AS (SELECT 1 x) UPDATE t SET v = c.x FROM c",
                    "WITH/* c */c AS (SELECT 1 x) UPDATE t SET v = c.x FROM c",
            };
            for (String sql : statements) {
                final String response = assertSameResponse("/query", sql, "a := 1");
                TestUtils.assertEquals("{\"dml\":\"OK\",\"updated\":0}", response);
            }
        }));
    }

    @Test
    public void testValidateReadsVariables() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            final String url = "/api/v1/sql/validate";
            final Utf8StringSink sink = new Utf8StringSink();
            Assert.assertEquals("200", testHttpClient.get(url, "SELECT @a x, @b y", "@a := 1, @b := 'zz'", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT @a x, @b y\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"},{\"name\":\"y\",\"type\":\"STRING\"}],\"timestamp\":-1}",
                    sink.toString()
            );
            // without them, the query reads no variables
            Assert.assertEquals("400", testHttpClient.get(url, "SELECT @a x", null, null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT @a x\",\"error\":\"Invalid column: @a\",\"position\":7}",
                    sink.toString()
            );
            // an error in the variables
            Assert.assertEquals("400", testHttpClient.get(url, "SELECT @a x", "@a := nope_fn()", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT @a x\",\"error\":\"unknown function name: nope_fn()\",\"position\":6,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
            // an unfinished query, as the console validates it while the user types: the error at
            // the end of the query is no error in the variables
            Assert.assertEquals("400", testHttpClient.get(url, "SELECT * FROM", "@a := 1", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT * FROM\",\"error\":\"table name or sub-query expected\",\"position\":13}",
                    sink.toString()
            );
            // and one at the end of the variables is
            Assert.assertEquals("400", testHttpClient.get(url, "SELECT 1", "@a := now() at time", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"SELECT 1\",\"error\":\"did you mean 'at time zone <tz>'?\",\"position\":19,\"errorSource\":\"variables\"}",
                    sink.toString()
            );
            // an INSERT reads them; an UPDATE does not, as on /exec
            engine.execute("CREATE TABLE t (v INT)", sqlExecutionContext);
            Assert.assertEquals("200", testHttpClient.get(url, "INSERT INTO t SELECT @a", "@a := 1", null, sink));
            TestUtils.assertEquals("{\"queryType\":\"INSERT AS SELECT\"}", sink.toString());
            Assert.assertEquals("400", testHttpClient.get(url, "UPDATE t SET v = @a", "@a := 1", null, sink));
            TestUtils.assertEquals(
                    "{\"query\":\"UPDATE t SET v = @a\",\"error\":\"Invalid column: @a\",\"position\":17}",
                    sink.toString()
            );
        }));
    }

    private static void assertGet(CharSequence expected, CharSequence sql, @Nullable String variables) {
        final Utf8StringSink sink = new Utf8StringSink();
        testHttpClient.get(sql, variables, sink);
        TestUtils.assertEquals(expected, sink.toString());
    }

    // The export of the query with the variables is the export of the reference without them.
    // Returns the export.
    private static Utf8StringSink assertSameExport(String reference, String sql, String variables, @Nullable CharSequenceObjHashMap<String> params) {
        final Utf8StringSink expected = new Utf8StringSink();
        Assert.assertEquals("200", testHttpClient.get("/exp", reference, null, params, expected));
        final Utf8StringSink actual = new Utf8StringSink();
        final String status = testHttpClient.get("/exp", sql, variables, params, actual);
        if (!"200".equals(status)) {
            Assert.fail("status " + status + ": " + Utf8s.toString(actual));
        }
        // a parquet file is binary, so compare the bytes
        Assert.assertTrue(actual.size() > 0);
        Assert.assertTrue(Utf8s.equals(expected, actual));
        return actual;
    }

    // the statement answers with the variables as it does without them
    private static void assertSameResponse(String sql, String variables) {
        assertSameResponse("/query", sql, variables);
    }

    // returns the response, the same with the variables as without them
    private static String assertSameResponse(String url, String sql, String variables) {
        final Utf8StringSink without = new Utf8StringSink();
        final String statusWithout = testHttpClient.get(url, sql, null, null, without);
        final Utf8StringSink with = new Utf8StringSink();
        final String statusWith = testHttpClient.get(url, sql, variables, null, with);
        Assert.assertEquals(statusWithout, statusWith);
        TestUtils.assertEquals(without.toString(), with.toString());
        return with.toString();
    }

    private static class VariablesHttpClient extends TestHttpClient {
        // returns the status code
        String get(CharSequence sql, @Nullable String variables, Utf8StringSink sink) {
            return get(sql, variables, null, sink);
        }

        String get(CharSequence sql, @Nullable String variables, @Nullable CharSequenceObjHashMap<String> params, Utf8StringSink sink) {
            return get("/query", sql, variables, params, sink);
        }

        String get(CharSequence url, CharSequence sql, @Nullable String variables, @Nullable CharSequenceObjHashMap<String> params, Utf8StringSink sink) {
            try {
                final HttpClient.Request req = getHttpClient().newRequest("localhost", port);
                req.GET().url(url).query("query", sql);
                if (variables != null) {
                    req.query("variables", variables);
                }
                final String status = reqToSink(req, sink, null, null, null, params);
                Assert.assertTrue(sink.size() > 0);
                return status;
            } finally {
                disconnect();
            }
        }
    }
}
