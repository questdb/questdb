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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class InStrTest extends AbstractCairoTest {

    @Test
    public void testAllConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQuery("test where a in NULL")
                    .noLeakCheck()
                    .returns("a\tts\n");
            assertQuery("test where a in ('16', NULL)")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            16\t1970-01-01T00:00:15.000000Z
                            """);
            assertQuery("test where a in '3'")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """);
            assertQuery("test where a in ('3', '22', '79')")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """);
        });
    }

    @Test
    public void testBindVarTypeChange() throws Exception {
        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "simple",
                """
                        x\ta
                        26\tSUW
                        69\tWZLUOG
                        87\tGQZVK
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "WZLUOG");
                    bindVariableService.setStr(1, "GQZVK");
                    bindVariableService.setStr(2, "SUW");
                }
        ));

        cases.add(BindVarTuple.fails(
                "undefined bind variable",
                20,
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setStr(0, "ELLKK");
                    bindVariableService.setStr(2, "GNJJILL");
                }
        ));

        cases.add(BindVarTuple.fails(
                "bad type",
                20,
                "cannot compare STRING with type INT",
                bindVariableService -> {
                    bindVariableService.setVarchar(0, new Utf8String("ELLKK"));
                    bindVariableService.setInt(1, 30);
                    bindVariableService.setStr(2, "GNJJILL");
                }
        ));

        cases.add(BindVarTuple.ok(
                "with nulls",
                """
                        x\ta
                        2\t
                        4\tNRXG
                        19\t
                        24\t
                        29\t
                        30\t
                        32\t
                        33\t
                        35\t
                        36\t
                        38\t
                        39\t
                        40\t
                        41\t
                        43\t
                        49\t
                        55\t
                        62\t
                        66\t
                        68\t
                        70\t
                        73\t
                        84\t
                        93\tNPIW
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "NPIW");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "NRXG");
                }
        ));

        assertQuery("test where a in ($1,$2,$3)")
                .ddl("create table test as (select x, rnd_str(3,6,1) a from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testBindVarTypeNotConvertible() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.fails(
                "mixed",
                33,
                "cannot compare STRING with type FLOAT",
                bindVariableService -> {
                    bindVariableService.setVarchar(0, new Utf8String("52"));
                    bindVariableService.setFloat(1, (float) 4567.6);
                }
        ));

        assertQuery("test where a in ('81', $1, null, $2)")
                .ddl("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQuery("test where a in '3'::char")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """);
            assertQuery("test where a in ('3'::char, '22', '79')")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """);

            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "char test",
                    """
                            a\tts
                            4\t1970-01-01T00:00:03.000000Z
                            15\t1970-01-01T00:00:14.000000Z
                            77\t1970-01-01T00:01:16.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setChar(0, '4');
                        bindVariableService.setStr(1, "15");
                        bindVariableService.setVarchar(2, new Utf8String("77"));
                    }
            ));

            assertQuery("test where a in ($1, $2, $3)")
                    .noLeakCheck()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testColumn() throws Exception {
        assertQuery("test where a in ('abcde', '81', b)")
                .ddl("create table test as (select cast(x as string) a, x b, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .fails(32, "constant expected");
    }

    @Test
    public void testCastSymbol() throws Exception {
        assertMemoryLeak(() -> {
            for (String cacheFlag : new String[]{"CACHE", "NOCACHE"}) {
                execute("CREATE TABLE x (s SYMBOL " + cacheFlag + ", str STRING, v INT)");
                execute("""
                        INSERT INTO x VALUES
                            ('abc', 'abc', 1),
                            ('b', 'b', 2),
                            (NULL, NULL, 3),
                            ('', '', 4),
                            ('é€😀', 'é€😀', 5),
                            ('abc', 'abc', 6)
                        """);
                assertCastSymbolIn("s::STRING IN ('abc', 'zz')", "1\n6\n");
                assertCastSymbolIn("s::STRING IN ('abc', NULL)", "1\n3\n6\n");
                assertCastSymbolIn("s::STRING IN (NULL)", "3\n");
                assertCastSymbolIn("s::STRING IN ('')", "4\n");
                assertCastSymbolIn("s::STRING IN ('é€😀')", "5\n");
                assertCastSymbolIn("s::STRING IN ('zz')", "");
                assertCastSymbolIn("s::STRING IN ('b')", "2\n");
                assertCastSymbolIn("s::STRING IN ('b'::VARCHAR, 'abc'::SYMBOL)", "1\n2\n6\n");
                assertCastSymbolIn("CAST(s AS STRING) IN ('b')", "2\n");
                assertCastSymbolIn("NOT s::STRING IN ('abc', NULL)", "2\n4\n5\n");
                assertCastSymbolIn("str::SYMBOL::STRING IN ('abc', NULL)", "1\n3\n6\n");
                assertQuery("SELECT v, s::STRING IN ('b') m FROM x WHERE v < 3")
                        .noLeakCheck()
                        .returns("""
                                v\tm
                                1\tfalse
                                2\ttrue
                                """);
                execute("DROP TABLE x");
            }
        });
    }

    @Test
    public void testCastSymbolAddedAfterData() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v INT)");
            execute("INSERT INTO x VALUES (1), (2)");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            execute("INSERT INTO x VALUES (3, 'abc')");
            assertCastSymbolIn("s::STRING IN (NULL)", "1\n2\n");
            assertCastSymbolIn("s::STRING IN ('abc')", "3\n");
        });
    }

    @Test
    public void testCastSymbolBindVariableOfWrongType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v INT)");
            execute("INSERT INTO x VALUES ('abc', 1)");
            assertWrongBindVariableType("s::VARCHAR::STRING", 45, "cannot compare STRING with type INT");
            assertWrongBindVariableType("s::STRING", 36, "inconvertible types: INT -> SYMBOL");
        });
    }

    @Test
    public void testCastSymbolBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v INT)");
            execute("INSERT INTO x VALUES ('abc', 1), ('b', 2), (NULL, 3)");
            execute("ALTER TABLE x ADD COLUMN str STRING");
            execute("UPDATE x SET str = s");
            for (String lhs : new String[]{"s::STRING", "str::SYMBOL::STRING"}) {
                final String sql = "SELECT v FROM x WHERE " + lhs + " IN ('b', :p1)";
                sqlExecutionContext.getBindVariableService().setStr("p1", "abc");
                assertQuery(sql).noLeakCheck().returns("v\n1\n2\n");
                sqlExecutionContext.getBindVariableService().setStr("p1", null);
                assertQuery(sql).noLeakCheck().returns("v\n2\n3\n");
                sqlExecutionContext.getBindVariableService().setStr("p1", "zz");
                assertQuery(sql).noLeakCheck().returns("v\n2\n");
                sqlExecutionContext.getBindVariableService().setVarchar("p1", new Utf8String("abc"));
                assertQuery(sql).noLeakCheck().returns("v\n1\n2\n");
            }
        });
    }

    @Test
    public void testCastSymbolComparesKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v INT)");
            execute("INSERT INTO x VALUES ('abc', 1), ('b', 2)");
            assertQuery("SELECT v FROM x WHERE s::STRING IN ('b')")
                    .noLeakCheck()
                    .assertsPlanContaining("filter: s in [b]");
        });
    }

    @Test
    public void testCastSymbolInCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, str STRING, v INT)");
            execute("CREATE TABLE y (s SYMBOL)");
            execute("INSERT INTO x VALUES ('abc', 'abc', 1), ('b', 'b', 2), (NULL, NULL, 3)");
            execute("INSERT INTO y VALUES ('zz'), ('b'), (NULL)");
            assertCastSymbolIn("s::STRING IN (SELECT s FROM y)", "2\n3\n");
            assertCastSymbolIn("s::STRING IN (SELECT s FROM y WHERE s = 'b')", "2\n");
            assertCastSymbolIn("s::STRING IN (SELECT s FROM y WHERE s = 'none')", "");
            assertCastSymbolIn("s::STRING IN (SELECT NULL)", "3\n");
            assertCastSymbolIn("str::SYMBOL::STRING IN (SELECT s FROM y)", "2\n3\n");
        });
    }

    @Test
    public void testCastSymbolMatchesTextComparison() throws Exception {
        assertMemoryLeak(() -> assertCastSymbolMatchesTextComparison(engine, sqlExecutionContext));
    }

    @Test
    public void testCastSymbolMatchesTextComparisonWithWorkers() throws Exception {
        TestUtils.execute(
                new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG))),
                (engine, _, context) -> assertCastSymbolMatchesTextComparison(engine, context),
                configuration,
                LOG
        );
    }

    @Test
    public void testCastSymbolRejectsBadConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL)");
            assertException("SELECT * FROM x WHERE s::STRING IN ('a', 10)", 41, "STRING constant expected");
        });
    }

    @Test
    public void testCastSymbolReusedFactoryResolvesKeysPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v INT)");
            execute("INSERT INTO x VALUES ('a', 1)");
            try (RecordCursorFactory factory = select("SELECT v FROM x WHERE s::STRING IN ('b', NULL)")) {
                assertFactoryRows("v\n", factory);
                execute("INSERT INTO x VALUES ('b', 2), (NULL, 3)");
                assertFactoryRows("v\n2\n3\n", factory);
                execute("TRUNCATE TABLE x");
                assertFactoryRows("v\n", factory);
                execute("INSERT INTO x VALUES ('c', 4), ('b', 5)");
                assertFactoryRows("v\n5\n", factory);
            }
        });
    }

    @Test
    public void testCastSymbolRuntimeConstantArgument() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v INT)");
            execute("INSERT INTO x VALUES (1), (2)");
            for (String lhs : new String[]{":p1::SYMBOL::VARCHAR::STRING", ":p1::SYMBOL::STRING"}) {
                final String sql = "SELECT v FROM x WHERE " + lhs + " IN ('a', :p2)";
                bindVariableService.setStr("p1", "a");
                bindVariableService.setStr("p2", "b");
                assertQuery(sql).noLeakCheck().expectSize().returns("v\n1\n2\n");
                bindVariableService.setStr("p1", "c");
                assertQuery(sql).noLeakCheck().expectSize().returns("v\n");
                bindVariableService.setStr("p2", "c");
                assertQuery(sql).noLeakCheck().expectSize().returns("v\n1\n2\n");
                bindVariableService.setStr("p1", null);
                assertQuery(sql).noLeakCheck().expectSize().returns("v\n");
                bindVariableService.setStr("p2", null);
                assertQuery(sql).noLeakCheck().expectSize().returns("v\n1\n2\n");
            }
        });
    }

    @Test
    public void testConstAndBindVarMixed() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "mixed",
                """
                        a\tts
                        6\t1970-01-01T00:00:05.000000Z
                        27\t1970-01-01T00:00:26.000000Z
                        52\t1970-01-01T00:00:51.000000Z
                        81\t1970-01-01T00:01:20.000000Z
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "27");
                    bindVariableService.setVarchar(1, new Utf8String("52"));
                }
        ));

        assertQuery("test where a in ('6', '81', $1, null, $2)")
                .ddl("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testConstConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))");
            assertQuery("test where 'a' in ('a', 'b')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """);
            assertQuery("test where NULL::string in ('a', 'b')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tts\n");
            assertQuery("test where NULL::string in ('a', NULL)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """);
        });
    }

    private void assertCastSymbolIn(String predicate, String expectedRows) throws Exception {
        assertQuery("SELECT v FROM x WHERE " + predicate)
                .noLeakCheck()
                .returns("v\n" + expectedRows);
    }

    private void assertFactoryRows(String expected, RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(expected, cursor, factory.getMetadata(), true);
        }
    }

    private void assertWrongBindVariableType(String lhs, int position, String message) throws SqlException {
        bindVariableService.clear();
        try (RecordCursorFactory factory = select("SELECT v FROM x WHERE " + lhs + " IN ($1, $2)")) {
            bindVariableService.clear();
            bindVariableService.setInt(0, 1);
            bindVariableService.setStr(1, "abc");
            try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        }
    }

    private static void assertCastSymbolMatchesTextComparison(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        engine.execute("CREATE TABLE t (k INT, s SYMBOL, n SYMBOL NOCACHE, ix SYMBOL INDEX, str STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("""
                INSERT INTO t
                SELECT x::INT, v, v, v, v, (x * 3_600_000_000)::TIMESTAMP
                FROM (
                    SELECT x, CASE x % 6 WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL WHEN 4 THEN '' WHEN 5 THEN 'é€😀' ELSE 'c' END v
                    FROM long_sequence(2_000)
                )
                """, context);
        engine.execute("ALTER TABLE t ADD COLUMN late SYMBOL", context);
        engine.execute("""
                INSERT INTO t (k, s, late, ts)
                SELECT (x + 2_000)::INT, 'a', CASE x % 2 WHEN 0 THEN 'b' ELSE 'a' END, ((x + 2_000) * 3_600_000_000)::TIMESTAMP
                FROM long_sequence(100)
                """, context);
        // First-seen order differs from t, so the same text has different keys in each table.
        engine.execute("CREATE TABLE u (k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("""
                INSERT INTO u
                SELECT (x * 2)::INT, CASE x % 5 WHEN 1 THEN 'é€😀' WHEN 2 THEN 'b' WHEN 3 THEN 'a' WHEN 4 THEN NULL ELSE 'z' END, (x * 7_200_000_000)::TIMESTAMP
                FROM long_sequence(500)
                """, context);

        final String[] queries = {
                "SELECT k FROM t WHERE s::STRING IN ('a', 'é€😀', NULL)",
                "SELECT k FROM t WHERE n::STRING IN ('a', 'é€😀', NULL)",
                "SELECT k FROM t WHERE ix::STRING IN ('a', 'é€😀', NULL)",
                "SELECT k FROM t WHERE late::STRING IN ('a', NULL)",
                "SELECT k FROM t WHERE s::STRING IN ('', 'missing')",
                "SELECT k FROM t WHERE s::STRING IN ('a'::CHAR, 'b'::SYMBOL, 'c'::VARCHAR)",
                "SELECT k FROM t WHERE NOT s::STRING IN ('a', '')",
                "SELECT k, s::STRING IN ('b') m FROM t",
                "SELECT k FROM t WHERE str::SYMBOL::STRING IN ('a', NULL)",
                "SELECT k FROM t WHERE s::STRING IN ('a', 'b') AND k % 2 = 0 ORDER BY k DESC LIMIT 3",
                "SELECT k FROM t WHERE s::STRING IN ('a', 'b') ORDER BY k LIMIT -3",
                "SELECT k, s FROM t WHERE s::STRING IN ('a', 'b') LATEST ON ts PARTITION BY s",
                "SELECT t.k, u.k uk FROM t LEFT JOIN u ON t.k = u.k WHERE u.s::STRING IN ('a', NULL) ORDER BY t.k",
                "SELECT t.k, u.k uk FROM t JOIN u ON t.k = u.k WHERE t.s::STRING IN ('a', 'b') AND u.s::STRING IN ('b', 'a') ORDER BY t.k",
                "SELECT t.k, u.k uk, t.s::STRING IN ('a') ml, u.s::STRING IN ('a') mr FROM t LEFT JOIN u ON t.k = u.k ORDER BY t.k",
                "SELECT t.k, u.k uk FROM t ASOF JOIN u WHERE u.s::STRING IN ('a', NULL)",
                "SELECT k FROM (SELECT k, s FROM t UNION ALL SELECT k, s FROM u) WHERE s::STRING IN ('a', NULL) ORDER BY k",
                "SELECT s::STRING IN ('a') m, count() c FROM t ORDER BY m",
                "SELECT s, c FROM (SELECT s, count() c FROM t) WHERE s::STRING IN ('a', NULL) ORDER BY s",
                "SELECT d, f, l FROM (SELECT k % 2 d, first(s) f, last(s) l FROM t) WHERE f::STRING IN ('a', NULL) OR l::STRING IN ('a') ORDER BY d",
                "SELECT k, m FROM (SELECT k, mode(s) m FROM t) WHERE m::STRING IN ('a', NULL) ORDER BY k",
                "SELECT ts, f FROM (SELECT ts, first(s) f FROM t SAMPLE BY 30m FILL(NULL)) WHERE f::STRING IN (NULL, 'a')",
                "SELECT ts, f FROM (SELECT ts, first(s) f FROM t SAMPLE BY 30m FILL(PREV)) WHERE f::STRING IN ('a', 'b')",
                "SELECT k, l FROM (SELECT k, lag(s) OVER (ORDER BY ts) l FROM t) WHERE l::STRING IN ('a', NULL)",
                "SELECT k, l::STRING IN ('a', NULL) m FROM (SELECT k, lead(s) OVER (ORDER BY ts) l FROM t)",
                "SELECT k FROM (SELECT k, s FROM t ORDER BY s, k) WHERE s::STRING IN ('a', 'b')",
                "SELECT k, m FROM (SELECT k, s x, s::STRING IN ('a') m FROM t) WHERE x::STRING IN ('a', 'b')",
                "SELECT k FROM t WHERE nullif(s, 'a')::STRING IN ('b', NULL)",
                "SELECT k FROM t WHERE coalesce(s, 'x')::STRING IN ('x', 'b')",
                "SELECT k FROM t WHERE (CASE WHEN k > 3 THEN s ELSE n END)::STRING IN ('a', NULL)",
                "SELECT d, v FROM (SELECT k % 2 d, nullif(first(s), last(s)) v FROM t) WHERE v::STRING IN ('a', NULL) ORDER BY d",
                "SELECT k FROM t WHERE s::STRING <> ALL('{a,é€😀}'::text[])",
                "SELECT k FROM t WHERE n::STRING <> ALL('{a,missing}'::text[])",
                "SELECT k FROM t WHERE ix::STRING <> ALL('{a}'::text[])",
                "SELECT k FROM t WHERE late::STRING <> ALL('{a}'::text[])",
                "SELECT k FROM t WHERE s::STRING <> ALL('{}'::text[])",
                "SELECT k FROM t WHERE s::STRING != ALL('{a}'::text[])",
                "SELECT k FROM t WHERE NOT s::STRING <> ALL('{a,b}'::text[])",
                "SELECT k, s::STRING <> ALL('{b}'::text[]) m FROM t",
                "SELECT k FROM t WHERE str::SYMBOL::STRING <> ALL('{a}'::text[])",
                "SELECT k FROM t WHERE coalesce(s, 'x')::STRING <> ALL('{x}'::text[])",
                "SELECT k FROM (SELECT k, s FROM t UNION ALL SELECT k, s FROM u) WHERE s::STRING <> ALL('{a}'::text[]) ORDER BY k",
                "SELECT t.k, u.k uk FROM t LEFT JOIN u ON t.k = u.k WHERE u.s::STRING <> ALL('{a}'::text[]) ORDER BY t.k",
                "SELECT string_distinct_agg(s::STRING, ',') v FROM t",
                "SELECT string_distinct_agg(n::STRING, ',') v FROM t",
                "SELECT string_distinct_agg(ix::STRING, ',') v FROM t",
                "SELECT string_distinct_agg(late::STRING, ',') v FROM t",
                "SELECT string_agg(DISTINCT s::STRING, ',') v FROM t",
                "SELECT k % 3 d, string_distinct_agg(s::STRING, ',') v FROM t ORDER BY d",
                "SELECT string_distinct_agg(s::STRING, ',') v FROM (SELECT k, s FROM t UNION ALL SELECT k, s FROM u)",
                "SELECT t.k % 2 d, string_distinct_agg(u.s::STRING, ',') v FROM t JOIN u ON t.k = u.k ORDER BY d",
                "SELECT ts, string_distinct_agg(s::STRING, ',') v FROM t SAMPLE BY 1d",
                "SELECT string_distinct_agg(coalesce(s, 'x')::STRING, ',') v FROM t",
                "SELECT string_distinct_agg(nullif(s, 'a')::STRING, ',') v FROM t",
                "SELECT count_distinct(s::STRING) c FROM t",
                "SELECT count_distinct(n::STRING) c FROM t",
                "SELECT count_distinct(ix::STRING) c FROM t",
                "SELECT count_distinct(late::STRING) c FROM t",
                "SELECT count(DISTINCT s::STRING) c FROM t",
                "SELECT COUNT_DISTINCT(s::STRING) c FROM t",
                "SELECT k % 3 d, count_distinct(s::STRING) c FROM t ORDER BY d",
                "SELECT count_distinct(s::STRING) c FROM (SELECT k, s FROM t UNION ALL SELECT k, s FROM u)",
                "SELECT t.k % 2 d, count_distinct(u.s::STRING) c FROM t JOIN u ON t.k = u.k ORDER BY d",
                "SELECT ts, count_distinct(s::STRING) c FROM t SAMPLE BY 1d",
                "SELECT count_distinct(coalesce(s, 'x')::STRING) c FROM t",
                "SELECT count_distinct(nullif(s, 'a')::STRING) c FROM t",
                "SELECT t.k, count_distinct(u.s::STRING) c FROM t WINDOW JOIN u RANGE BETWEEN 2 HOUR PRECEDING AND 2 HOUR FOLLOWING",
                "SELECT t.k, string_distinct_agg(u.s::STRING, ',') v FROM t WINDOW JOIN u RANGE BETWEEN 2 HOUR PRECEDING AND 2 HOUR FOLLOWING",
                "SELECT t.k, count_distinct(u.s::STRING) c FROM t WINDOW JOIN u ON (t.s = u.s) RANGE BETWEEN 4 HOUR PRECEDING AND 4 HOUR FOLLOWING",
        };
        for (int run = 0; run < 2; run++) {
            for (String query : queries) {
                TestUtils.assertSqlCursors(engine, context, query.replace("::STRING", "::VARCHAR::STRING"), query, LOG);
            }
            if (run == 0) {
                engine.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-02-01'", context);
                engine.execute("ALTER TABLE u CONVERT PARTITION TO PARQUET WHERE ts < '1970-02-01'", context);
            }
        }
    }
}
