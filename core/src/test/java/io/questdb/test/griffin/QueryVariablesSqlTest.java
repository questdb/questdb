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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Variables a client declares outside the statement's text, see
 * {@link io.questdb.griffin.SqlExecutionContext#getQueryVariables()}: the assignments of a DECLARE
 * block, which the statement reads as a DECLARE block that opens it. An expectation holds the
 * result of the same statement with the variables written as that block, the result of the
 * statement without the variables, where it ignores them, or a result worked out by hand.
 * <p>
 * The parser reads the variables at {@link SqlParser#QUERY_VARIABLES_POSITION}, so an error in
 * them points there plus the offset in the variables, and every other error points where it does
 * without them.
 */
public class QueryVariablesSqlTest extends AbstractCairoTest {

    @Test
    public void testCommentGluedToWith() throws Exception {
        assertMemoryLeak(() -> {
            // A comment glued to WITH leaves the statement as it is with a blank there: UPDATE and
            // INSERT ... VALUES ignore the variables, a query reads them. The second comment holds
            // the start of a statement that ignores them.
            execute("CREATE TABLE w (k SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO w VALUES ('a', 1, '2024-01-01'), ('b', 2, '2024-01-02')");
            drainWalQueue();
            for (String comment : new String[]{"/**/", "/* c AS (SELECT 1) UPDATE */", "/*+ hint */", "-- comment\n"}) {
                final String with = "WITH" + comment + "c AS ";
                execute("CREATE TABLE x (a INT)");
                // "a := 1" is no assignment: a statement that ignores the variables does not parse them
                executeWithVariables("a := 1", with + "(SELECT 1 y) INSERT INTO x VALUES (10)");
                executeWithVariables("a := 1", with + "(SELECT 1 y) UPDATE x SET a = a + 1");
                final String explain = "EXPLAIN " + with + "(SELECT 1 y) INSERT INTO x VALUES (20)";
                assertSamePrint(explain, explain, "a := 1");
                // the query of an INSERT reads them, as a query does
                executeWithVariables("@a := 7", with + "(SELECT @a y) INSERT INTO x SELECT y FROM c");
                assertQuery("SELECT * FROM x").noLeakCheck().expectSize().returns("a\n11\n7\n");
                final String select = with + "(SELECT @a y) SELECT y FROM c";
                withVariables("@a := 7", select, () -> assertQuery(select).noLeakCheck().expectSize().returns("y\n7\n"));
                // The WAL applies an UPDATE by parsing its text again, without the variables. An
                // UPDATE that reads one fails as it does without them, and never reaches the WAL.
                assertFailsAsWithoutVariables(with + "(SELECT 1 y) UPDATE w SET v = 9 WHERE v = @a", "@a := 1");
                execute("DROP TABLE x");
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            assertQuery("SELECT k, v FROM w").noLeakCheck().expectSize().returns("k\tv\na\t1\nb\t2\n");
        });
    }

    @Test
    public void testContextWithoutVariablesDeclaresNone() {
        // A context that implements the interface without SqlExecutionContextImpl, as other
        // contexts may, carries no variables: the interface's defaults return none.
        final SqlExecutionContext context = new SqlExecutionContextStub(engine);
        Assert.assertNull(context.getQueryVariables());
        Assert.assertNull(context.getQueryVariablesSqlText());
        Assert.assertNull(context.getRequestVariables());
    }

    @Test
    public void testErrorInValuePointsIntoVariables() throws Exception {
        // the 6 characters of "@a := " into "@a := nope_fn()"
        final String sql = "SELECT @a";
        withVariables("@a := nope_fn()", sql, () -> assertQuery(sql)
                .fails(SqlParser.QUERY_VARIABLES_POSITION + 6, "unknown function name: nope_fn()"));
    }

    @Test
    public void testErrorInViewPointsWhereItDoesWithoutVariables() throws Exception {
        assertMemoryLeak(() -> {
            // An error in the body of a view carries a position in the view's text, which can lie
            // past the statement's text. It lies before the variables all the same, so the error
            // is no error in them, however long they are.
            execute("CREATE TABLE t (a INT, b INT, c INT, k INT)");
            execute("CREATE VIEW v AS (SELECT a, b, c, k FROM t)");
            execute("CREATE TABLE t2 (u CHAR, n INT, ts TIMESTAMP)");
            execute("INSERT INTO t2 VALUES ('d', null, '2024-01-01')");
            execute("CREATE VIEW vd AS (SELECT dateadd(u, n, ts) d FROM t2)");
            drainWalAndViewQueues();
            execute("ALTER TABLE t DROP COLUMN k");
            drainWalAndViewQueues();
            final String variables = "@pad := '" + "x".repeat(100) + "'";
            // the view's body reads a column that is gone, which the compile finds
            assertFailsWithVariablesAsWithout("SELECT * FROM v", variables, "Invalid column: k");
            // a function of the view's body fails on the first row
            assertFailsWithVariablesAsWithout("SELECT * FROM vd", variables, "`null` is not a valid stride");
        });
    }

    @Test
    public void testErrorLogWithoutTextShowsVariables() throws Exception {
        // A plan that a compiler generates from a model rather than from a statement's text has
        // no text to log: its error line shows **subquery** in its place, followed by the
        // variables of the request, as any text is. simulate_crash('0') fails the filter at the
        // first row with the fixed position 222.
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                capture.start();
                final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                context.setRequestVariables("@a := 1");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(
                            "SELECT x FROM long_sequence(1) WHERE simulate_crash('0')",
                            context
                    );
                    try (
                            RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, context, true);
                            RecordCursor cursor = factory.getCursor(context)
                    ) {
                        cursor.hasNext();
                        Assert.fail("expected the filter to fail");
                    } catch (CairoException e) {
                        TestUtils.assertEquals("simulated cairo exception", e.getFlyweightMessage());
                    }
                } finally {
                    context.setRequestVariables(null);
                }
                final String line = "err \\[id=\\d+, sql=`\\*\\*subquery\\*\\*`, vars=`@a := 1`, principal=.*, msg=simulated cairo exception, errno=1, pos=222]";
                capture.waitForRegex(line);
                capture.assertOnlyOnce(line);
            });
        } finally {
            capture.stop();
            LogFactory.disableGuaranteedLogging(QueryProgress.class);
        }
    }

    @Test
    public void testExplainInsertAndWithReadVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            final String[][] statements = {
                    // no DECLARE block can open an INSERT, so the reference holds the value itself
                    {"EXPLAIN INSERT INTO t SELECT 7", "EXPLAIN INSERT INTO t SELECT @a"},
                    {"EXPLAIN DECLARE @a := 7 WITH c AS (SELECT @a x) SELECT * FROM c", "EXPLAIN WITH c AS (SELECT @a x) SELECT * FROM c"},
            };
            for (String[] statement : statements) {
                assertSamePrint(statement[0], statement[1], "@a := 7");
            }
        });
    }

    @Test
    public void testExplainReadsVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            // the statement with the assignments as a DECLARE block that opens it, and the statement
            // sent with them; EXPLAIN parses a statement that opens with SELECT apart from one that
            // opens with a table name, its own DECLARE block or a bracket
            final String[][] statements = {
                    {"EXPLAIN DECLARE @lim := 3 SELECT v FROM t WHERE v > @lim", "EXPLAIN SELECT v FROM t WHERE v > @lim"},
                    {"EXPLAIN DECLARE @lim := 3 t WHERE v > @lim", "EXPLAIN t WHERE v > @lim"},
                    {"EXPLAIN DECLARE @lim := 3, @b := @lim + 1 SELECT v FROM t WHERE v > @b", "EXPLAIN DECLARE @b := @lim + 1 SELECT v FROM t WHERE v > @b"},
                    {"EXPLAIN DECLARE @lim := 3 (SELECT v FROM t WHERE v > @lim)", "EXPLAIN (SELECT v FROM t WHERE v > @lim)"},
            };
            for (String[] statement : statements) {
                TestUtils.assertContains(assertSamePrint(statement[0], statement[1], "@lim := 3"), "filter:");
            }
        });
    }

    @Test
    public void testGroupByWithSampleByPointsAsDeclareBlockDoes() throws Exception {
        assertMemoryLeak(() -> {
            // A query cannot have both GROUP BY and SAMPLE BY. The error points at the first GROUP BY
            // key where the key follows GROUP BY in the text, and at GROUP BY otherwise. A key that a
            // variable supplies sits in the variables, which read as a DECLARE block ahead of the
            // statement: the error points at GROUP BY, as it does with that block, and lies in the
            // statement, not in the variables.
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String message = "SELECT query must not contain both GROUP BY and SAMPLE BY";
            // variables, statement, and the text in the statement that the error points at
            final String[][] cases = {
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY @k", "GROUP BY"},
                    {"@k := concat(s1, 'x')", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY @k", "GROUP BY"},
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY @k, s2", "GROUP BY"},
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY ROLLUP(@k)", "GROUP BY"},
                    // the first key is the statement's own
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY s2, @k", "s2, @k"},
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY ROLLUP(s2, @k)", "s2, @k"},
                    // no key at all
                    {"@k := s1", "SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY GROUPING SETS (())", "GROUP BY"},
            };
            for (String[] c : cases) {
                final String variables = c[0];
                final String sql = c[1];
                final int position = sql.indexOf(c[2]);
                final String declared = "DECLARE " + variables + ' ' + sql;
                assertQuery(declared).noLeakCheck().fails(declared.length() - sql.length() + position, message);
                withVariables(variables, sql, () -> assertQuery(sql).noLeakCheck().fails(position, message));
            }
            // The query with both clauses sits in a sub-query that a variable holds, so the error lies
            // in the variables, at the token it points at in the same DECLARE block: the first key,
            // which follows GROUP BY in the variables, or GROUP BY for a key an earlier variable
            // supplies. Variables, and the text in them that the error points at.
            final String[][] inVariables = {
                    {"@q := (SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY s1)", "s1)"},
                    {"@q := (SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY s1, s2)", "s1, s2"},
                    {"@q := (SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY ROLLUP(s1))", "s1))"},
                    {"@k := s1, @q := (SELECT ts, count() FROM t SAMPLE BY 1h GROUP BY @k)", "GROUP BY"},
            };
            final String sql = "SELECT * FROM @q";
            for (String[] c : inVariables) {
                final String variables = c[0];
                final int offset = variables.indexOf(c[1]);
                assertQuery("DECLARE " + variables + ' ' + sql).noLeakCheck().fails("DECLARE ".length() + offset, message);
                withVariables(variables, sql, () -> assertQuery(sql).noLeakCheck().fails(SqlParser.QUERY_VARIABLES_POSITION + offset, message));
            }
        });
    }

    @Test
    public void testInsertSelectReadsVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, s VARCHAR)");
            executeWithVariables("@v := 41 + 1, @s := 'x' || @v", "INSERT INTO t SELECT @v, @s");
            executeWithVariables("@v := 7", "WITH c AS (SELECT @v v) INSERT INTO t SELECT v, 'cte' FROM c");
            drainWalQueue();
            assertQuery("SELECT * FROM t").noLeakCheck().expectSize().returns("""
                    v\ts
                    42\tx42
                    7\tcte
                    """);
        });
    }

    @Test
    public void testLegacyPrecedenceAdvisoryShowsVariables() throws Exception {
        // In legacy operator precedence mode, the parser logs an advisory for an expression that
        // the current precedence reads otherwise: the text the expression sits in, and the
        // position of the operator that reads otherwise, AND here. For an expression in the
        // variables, that is the variables and the position in them.
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE, "true");
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                // a compiler of its own takes the mode, see DeclareOperatorPrecedenceTest
                try (SqlCompiler compiler = new SqlCompilerImpl(engine)) {
                    capture.start();
                    // the same assignment in a DECLARE block, where AND sits 28 characters in
                    final String reference = "DECLARE @a := true OR false AND false SELECT @a value";
                    assertQuery(reference).withCompiler(compiler).noLeakCheck().expectSize().returns("value\nfalse\n");
                    capture.waitFor(advisory(reference, 28));
                    // AND sits 20 characters into the variables
                    final String variables = "@a := true OR false AND false";
                    final String sql = "SELECT @a value";
                    withVariables(variables, sql, () -> assertQuery(sql).withCompiler(compiler).noLeakCheck().expectSize().returns("value\nfalse\n"));
                    capture.waitFor(advisory(variables, 20));
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testNestedCompilesIgnoreVariables() throws Exception {
        assertMemoryLeak(() -> {
            // These statements compile SQL other than their own text: a query that checks an
            // EXPIRE ROWS policy, or a definition the statement stores. Those texts carry no
            // DECLARE block of the statement, so they read no variables either, and each
            // statement fails as it does without them.
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 3.0, '2024-01-01T01:00:00.000000Z')""");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)");
            drainWalAndMatViewQueues();
            final String variables = "@k := 1.0";
            final String[] statements = {
                    "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k) - @k",
                    "CREATE MATERIALIZED VIEW mv2 AS (SELECT * FROM base) EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k) - @k",
                    "CREATE MATERIALIZED VIEW mv3 AS (SELECT k, v + @k v, ts FROM base)",
                    "CREATE TABLE ctas AS (SELECT @k x)",
                    "CREATE VIEW vw AS (SELECT @k x)",
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, v, count(*) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE v > @k",
            };
            for (String sql : statements) {
                assertFailsAsWithoutVariables(sql, variables);
            }
            drainWalAndMatViewQueues();
            // the view keeps no policy
            assertQuery("SELECT k, v FROM mv").noLeakCheck().expectSize().returns("""
                    k\tv
                    A\t1.0
                    A\t3.0
                    """);
        });
    }

    @Test
    public void testNoAssignmentsDeclareNothing() throws Exception {
        assertMemoryLeak(() -> {
            // as SELECT @a without the variables
            for (String variables : new String[]{"", "  \n\t", "-- no variables", "/* none */"}) {
                withVariables(variables, "SELECT @a", () -> assertQuery("SELECT @a").noLeakCheck().fails(7, "Invalid column: @a"));
            }
        });
    }

    @Test
    public void testOnlyAssignments() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "SELECT @a";
            // Positions in the assignments, from QUERY_VARIABLES_POSITION on. A malformed
            // assignment fails as it does in a DECLARE block that opens the statement, at the same
            // place, which the statement with that block checks.
            final String declare = "DECLARE ";
            final Object[][] malformed = {
                    {"@a := 1,,", 8, "missing declaration"},
                    {"@a := 1 @b := 2", 8, "',' expected"},
                    {"@a 1", 3, "expected variable assignment operator `:=`"},
                    {"@a :=", 3, "too few arguments for ':=' [found=1,expected=2]"},
                    {"@a := @b", 6, "tried to use undeclared variable `@b`"},
            };
            for (Object[] c : malformed) {
                final String assignments = (String) c[0];
                final int offset = (int) c[1];
                final String message = (String) c[2];
                assertQuery(declare + assignments + ' ' + sql).noLeakCheck().fails(declare.length() + offset, message);
                withVariables(assignments, sql, () -> assertQuery(sql).noLeakCheck().fails(SqlParser.QUERY_VARIABLES_POSITION + offset, message));
            }
            // These refusals are the parameter's own. Such a block hands a token that opens no
            // assignment to the statement, reports DECLARE as a second block, and accepts the
            // OVERRIDABLE and AUDITED markers, which mean nothing for a client's variables. No
            // statement follows the variables, so the error names what belongs where the token
            // sits: an assignment at the start and after a comma, and a comma or the end of the
            // variables after a value, also when the token ends the variables. A refused marker is
            // reported where it sits, also when the variable it marks is declared again without
            // it, and of two markers the first is reported.
            final String assignmentExpected = "variable assignment expected, such as @name := value";
            final String commaExpected = "',' or end of variables expected";
            final String declareRefused = "variables are the assignments of a DECLARE block, without the DECLARE keyword";
            final Object[][] refused = {
                    // where an assignment belongs
                    {"1", 0, "unexpected token [1] - " + assignmentExpected},
                    {"a := 1", 0, "unexpected token [a] - " + assignmentExpected},
                    {"(@a := 1)", 0, "unexpected token [(] - " + assignmentExpected},
                    {")", 0, "unexpected token [)] - " + assignmentExpected},
                    {"overridable", 0, "unexpected token [overridable] - " + assignmentExpected},
                    {"@a := 1, b := 2", 9, "unexpected token [b] - " + assignmentExpected},
                    {"@a := 1, SELECT 2", 9, "unexpected token [SELECT] - " + assignmentExpected},
                    {"@a := 1, audited", 9, "unexpected token [audited] - " + assignmentExpected},
                    // after a value, with more text after the token and without
                    {"@a := 1 SELECT 2", 8, "unexpected token [SELECT] - " + commaExpected},
                    {"@a := 1 SELECT", 8, "unexpected token [SELECT] - " + commaExpected},
                    {"@a := 1 b c", 8, "unexpected token [b] - " + commaExpected},
                    {"@a := 1 b", 8, "unexpected token [b] - " + commaExpected},
                    {"@a := 1 overridable x", 8, "unexpected token [overridable] - " + commaExpected},
                    {"@a := 1 overridable", 8, "unexpected token [overridable] - " + commaExpected},
                    {"@a := 1;", 7, "unexpected token [;] - " + commaExpected},
                    {"@a := (1))", 9, "unexpected token [)] - " + commaExpected},
                    // DECLARE, wherever it sits
                    {"DECLARE @a := 1", 0, declareRefused},
                    {"@a := 1, DECLARE @b := 2", 9, declareRefused},
                    {"@a := 1 DECLARE @b := 2", 8, declareRefused},
                    {"@a := 1 DECLARE", 8, declareRefused},
                    {"OVERRIDABLE @a := 1", 0, "variables cannot be OVERRIDABLE"},
                    {"@b := 1, OVERRIDABLE @a := 1", 9, "variables cannot be OVERRIDABLE"},
                    {"/* c */ OVERRIDABLE @a := 1", 8, "variables cannot be OVERRIDABLE"},
                    {"OVERRIDABLE @a := 1, @a := 2", 0, "variables cannot be OVERRIDABLE"},
                    {"OVERRIDABLE AUDITED @a := 1", 0, "variables cannot be OVERRIDABLE"},
                    {"AUDITED @a := 1", 0, "variables cannot be AUDITED"},
                    {"@b := 1, AUDITED @a := 1", 9, "variables cannot be AUDITED"},
                    {"/* c */ AUDITED @a := 1", 8, "variables cannot be AUDITED"},
                    {"AUDITED @a := 1, @a := 2", 0, "variables cannot be AUDITED"},
                    {"AUDITED OVERRIDABLE @a := 1", 0, "variables cannot be AUDITED"},
            };
            for (Object[] c : refused) {
                withVariables((String) c[0], sql, () -> assertQuery(sql).noLeakCheck().fails(SqlParser.QUERY_VARIABLES_POSITION + (int) c[1], (String) c[2]));
            }
            // the statement's own block reports a second DECLARE as it does without the variables,
            // at that DECLARE, 16 characters in
            final String twoBlocks = "DECLARE @b := 1 DECLARE @c := 2 SELECT @b";
            withVariables("@a := 1", twoBlocks, () -> assertQuery(twoBlocks).noLeakCheck().fails(16, "unexpected token [DECLARE] - Multiple DECLARE statements are not allowed"));
        });
    }

    @Test
    public void testParseBudgetCountsVariables() throws Exception {
        assertMemoryLeak(() -> {
            // The parse budget of a statement grows with the text it parses once, the variables'
            // text among it, not with the position the parser reads the variables at. Each CTE
            // reads the one before it twice, so a later reference parses the chain again, and the
            // parse runs out of query models at a second read of a CTE, which would parse it
            // again, see WithClauseTest.testCteRefusedByParseBudgetLeavesNoLexerStash.
            execute("CREATE TABLE t AS (SELECT x::INT x FROM long_sequence(5))");
            final StringBuilder chain = new StringBuilder("WITH c0 AS (SELECT x FROM t)");
            for (int i = 1; i <= 12; i++) {
                chain.append(", c").append(i)
                        .append(" AS (SELECT a.x FROM c").append(i - 1)
                        .append(" a JOIN c").append(i - 1).append(" b ON a.x = b.x)");
            }
            final String sql = chain.append(" SELECT * FROM c12").toString();
            final String variables = "@a := 1";
            withVariables(variables, sql, () -> {
                try {
                    assertExceptionNoLeakCheck(sql);
                } catch (SqlException e) {
                    assertTooComplexToParseMessage(
                            e.getFlyweightMessage(),
                            "models",
                            SqlParser.MAX_QUERY_MODELS + (long) SqlParser.MAX_QUERY_MODELS_PER_CHAR * (sql.length() + variables.length())
                    );
                    // at the second read of a CTE, in the text of the next one
                    Assert.assertTrue(sql.startsWith("JOIN c", e.getPosition() - "JOIN ".length()));
                }
            });
        });
    }

    @Test
    public void testQueriesOfEveryShapeReadVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1), (2), (3)");
            drainWalQueue();
            final String variables = "@a := 2";
            // implicit SELECT
            final String implicitSelect = "t WHERE v >= @a";
            withVariables(variables, implicitSelect, () -> assertQuery(implicitSelect).noLeakCheck().returns("""
                    v
                    2
                    3
                    """));
            // CTE, sub-query in FROM, set operation branches, IN list
            final String cte = "WITH c AS (SELECT @a x) SELECT x FROM c";
            withVariables(variables, cte, () -> assertQuery(cte).noLeakCheck().expectSize().returns("x\n2\n"));
            final String subQuery = "SELECT * FROM (SELECT v FROM t WHERE v < @a)";
            withVariables(variables, subQuery, () -> assertQuery(subQuery).noLeakCheck().returns("v\n1\n"));
            final String union = "SELECT @a x UNION ALL SELECT @a + 1";
            withVariables(variables, union, () -> assertQuery(union).noLeakCheck().expectSize().noRandomAccess().returns("x\n2\n3\n"));
            final String in = "t WHERE v IN (@a, 3)";
            withVariables(variables, in, () -> assertQuery(in).noLeakCheck().returns("v\n2\n3\n"));
        });
    }

    @Test
    public void testRedefineViewIgnoresVariables() throws Exception {
        assertMemoryLeak(() -> {
            // ALTER VIEW and CREATE OR REPLACE VIEW over an existing view compile the new body while
            // the statement compiles. The view keeps the body as text, which no reader parses with
            // the variables, so the body fails as it does without them.
            execute("CREATE VIEW v AS (SELECT 1 x)");
            final String[] statements = {
                    "ALTER VIEW v AS (SELECT @a x)",
                    "ALTER VIEW v AS SELECT @a x",
                    "CREATE OR REPLACE VIEW v AS (SELECT @a x)",
            };
            for (String sql : statements) {
                Assert.assertEquals("Invalid column: @a", assertFailsAsWithoutVariables(sql, "@a := 1"));
            }
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v").noLeakCheck().expectSize().returns("x\n1\n");
            // a body that declares the variable itself still reads the client's value when it lets
            // callers override it
            executeWithVariables("@a := 2", "ALTER VIEW v AS (DECLARE OVERRIDABLE @a := 1 SELECT @a x)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v").noLeakCheck().expectSize().returns("x\n1\n");
            withVariables("@a := 2", "SELECT * FROM v", () -> assertQuery("SELECT * FROM v").noLeakCheck().expectSize().returns("x\n2\n"));
        });
    }

    @Test
    public void testRetryParsesTheStatementWithVariablesAgain() throws Exception {
        assertMemoryLeak(() -> {
            // A change of EXPIRE ROWS policies during the compile makes INSERT ... SELECT parse its
            // text again, which reads the variables again.
            execute("CREATE TABLE dst (v INT)");
            final AtomicInteger attempts = new AtomicInteger();
            SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(() -> {
                if (attempts.getAndIncrement() == 0) {
                    engine.getMetadataCache().publishExpiryPolicyUpdate();
                }
            });
            try {
                executeWithVariables("@v := 42", "INSERT INTO dst SELECT @v");
            } finally {
                SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(null);
            }
            Assert.assertEquals(2, attempts.get());
            assertQuery("SELECT * FROM dst").noLeakCheck().expectSize().returns("v\n42\n");
        });
    }

    @Test
    public void testStatementDeclarationReadsVariables() throws Exception {
        // the statement's own block reads the client's variables, as its own earlier
        // declarations; DECLARE @a := 1, @b := @a + 10 SELECT @b
        final String sql = "DECLARE @b := @a + 10 SELECT @b b";
        withVariables("@a := 1", sql, () -> assertQuery(sql).expectSize().returns("b\n11\n"));
    }

    @Test
    public void testStatementDeclarationWins() throws Exception {
        // DECLARE @a := 1, @b := @a + 1, @a := 5 SELECT @a, @b: the statement's @a is the
        // last one declared, and @b keeps the value it was declared with
        final String sql = "DECLARE @a := 5 SELECT @a a, @b b";
        withVariables("@a := 1, @b := @a + 1", sql, () -> assertQuery(sql).expectSize().returns("a\tb\n5\t2\n"));
    }

    @Test
    public void testStatementsThatIgnoreVariablesDoNotParseThem() throws Exception {
        assertMemoryLeak(() -> {
            // "a := 1" is no assignment: a statement that reads the variables fails on it, one
            // that does not read them runs as it does without the parameter
            final String variables = "a := 1";
            final String[] readers = {
                    "SELECT 1 x",
                    "INSERT INTO x SELECT 1",
                    "WITH c AS (SELECT 1 y) SELECT y FROM c",
                    "WITH c AS (SELECT 1 y) INSERT INTO x SELECT y FROM c",
                    "WITH c AS (SELECT 1 y) INSERT BATCH 10 o3MaxLag 1s INTO x (a) SELECT y FROM c",
            };
            for (String sql : readers) {
                withVariables(variables, sql, () -> assertExceptionNoLeakCheck(sql, SqlParser.QUERY_VARIABLES_POSITION, "unexpected token [a] - variable assignment expected, such as @name := value"));
            }

            final String[] statements = {
                    "CREATE TABLE x (a INT)",
                    "INSERT INTO x VALUES (1)",
                    "UPDATE x SET a = a + 1",
                    "WITH c AS (SELECT 10 y) UPDATE x SET a = a + c.y FROM c",
                    // VALUES lists the rows of an INSERT without a query to read them, with a WITH
                    // clause ahead of it too, whatever options and columns the INSERT names
                    "WITH c AS (SELECT 1 y) INSERT INTO x VALUES (20)",
                    "WITH c AS (SELECT 1 y), d AS (SELECT (2) z) INSERT BATCH 10 o3MaxLag 1s INTO x (a) VALUES (30), (40)",
                    "CREATE TABLE y AS (SELECT a FROM x)",
                    "CREATE VIEW vx AS (SELECT a FROM x)",
            };
            for (String sql : statements) {
                executeWithVariables(variables, sql);
            }
            for (String explain : new String[]{"EXPLAIN UPDATE x SET a = 0", "EXPLAIN WITH c AS (SELECT 1 y) INSERT INTO x VALUES (50)"}) {
                assertSamePrint(explain, explain, variables);
            }

            drainWalAndViewQueues();
            final String rows = """
                    a
                    12
                    20
                    30
                    40
                    """;
            assertQuery("SELECT * FROM x").noLeakCheck().expectSize().returns(rows);
            assertQuery("SELECT * FROM y").noLeakCheck().expectSize().returns(rows);
            assertQuery("SELECT * FROM vx").noLeakCheck().expectSize().returns(rows);
        });
    }

    @Test
    public void testSubQueryValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            drainWalQueue();
            withVariables("@q := (SELECT v FROM t WHERE v > 1)", "SELECT * FROM @q", () -> assertQuery("SELECT * FROM @q")
                    .noLeakCheck()
                    .returns("v\n2\n"));
            // "@q := (SELECT * FROM nope)": nope sits 21 characters into the assignments
            final String sql = "SELECT * FROM @q";
            withVariables("@q := (SELECT * FROM nope)", sql, () -> assertQuery(sql)
                    .noLeakCheck()
                    .fails(SqlParser.QUERY_VARIABLES_POSITION + 21, "table does not exist [table=nope]"));
        });
    }

    @Test
    public void testSubQueryValueReadInExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sym SYMBOL, v INT)");
            execute("INSERT INTO s VALUES ('a', 1), ('b', 2), ('c', 3)");
            execute("CREATE TABLE r (v INT)");
            execute("CREATE TABLE r2 (sym SYMBOL)");
            execute("CREATE VIEW vw AS (DECLARE OVERRIDABLE @m := (SELECT min(v) FROM s) SELECT v FROM s WHERE v = @m)");
            drainWalAndViewQueues();
            final String variables = "@q := (SELECT sym FROM s WHERE v > 1), @m := (SELECT max(v) FROM s)";
            // The first read of a sub-query value in an expression takes the sub-query the
            // declaration parsed, later reads parse copies. Each statement returns what it
            // returns with the assignments as a DECLARE block that opens it.
            // statement, result, whether its cursor supports random access
            final Object[][] cases = {
                    {"SELECT sym, v FROM s WHERE sym IN @q", "sym\tv\nb\t2\nc\t3\n", true},
                    {"SELECT v FROM s WHERE v = @m", "v\n3\n", true},
                    {"SELECT v FROM s WHERE v = @m UNION ALL SELECT v FROM s WHERE v = @m", "v\n3\n3\n", false},
                    {"SELECT * FROM (SELECT v FROM s WHERE v = @m)", "v\n3\n", true},
                    {"WITH c AS (SELECT sym FROM s WHERE sym IN @q) SELECT * FROM c", "sym\nb\nc\n", true},
                    {"SELECT sym FROM s WHERE sym IN @q AND v < @m", "sym\nb\n", true},
                    // the caller's value for the view's OVERRIDABLE variable
                    {"SELECT * FROM vw", "v\n3\n", true},
            };
            for (Object[] c : cases) {
                final String sql = (String) c[0];
                final String result = (String) c[1];
                final boolean isRandomAccess = (boolean) c[2];
                assertQuery("DECLARE " + variables + ' ' + sql).noLeakCheck().supportsRandomAccess(isRandomAccess).returns(result);
                withVariables(variables, sql, () -> assertQuery(sql).noLeakCheck().supportsRandomAccess(isRandomAccess).returns(result));
            }

            assertSamePrint(
                    "EXPLAIN DECLARE " + variables + " SELECT sym, v FROM s WHERE sym IN @q",
                    "EXPLAIN SELECT sym, v FROM s WHERE sym IN @q",
                    variables
            );

            executeWithVariables(variables, "INSERT INTO r SELECT v FROM s WHERE v = @m");
            executeWithVariables(variables, "WITH c AS (SELECT sym FROM s WHERE sym IN @q) INSERT INTO r2 SELECT sym FROM c");
            drainWalQueue();
            assertQuery("SELECT * FROM r").noLeakCheck().expectSize().returns("v\n3\n");
            assertQuery("SELECT * FROM r2").noLeakCheck().expectSize().returns("sym\nb\nc\n");
        });
    }

    @Test
    public void testTimeSeriesQueriesReadVariables() throws Exception {
        assertMemoryLeak(() -> {
            // The shapes a notebook reads a time series with: a day and a list of symbols, a SAMPLE BY
            // over a range, LATEST ON, a window frame and a negative LIMIT, on a WAL table. Each
            // statement returns the rows worked out by hand, as it does with the assignments as a
            // DECLARE block that opens it, and plans as it does with that block.
            execute("CREATE TABLE trades (sym SYMBOL INDEX, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO trades VALUES
                    ('BTC-USD', 10.0, '2026-11-10T23:00:00.000000Z'),
                    ('BTC-USD', 11.0, '2026-11-11T00:00:00.000000Z'),
                    ('ETH-USD', 20.0, '2026-11-11T01:30:00.000000Z'),
                    ('BTC-USD', 12.0, '2026-11-11T03:00:00.000000Z'),
                    ('SOL-USD', 30.0, '2026-11-11T23:59:59.999999Z'),
                    ('BTC-USD', 13.0, '2026-11-12T00:00:00.000000Z')""");
            drainWalQueue();
            final String variables = """
                    @day := '2026-11-11', @syms := ('BTC-USD', 'ETH-USD'),
                    @from := '2026-11-11T00:00:00.000000Z', @to := '2026-11-11T04:00:00.000000Z',
                    @n := 1, @last := -2""";
            // statement, result, whether its cursor knows its size and supports random access
            final Object[][] cases = {
                    // the day and the symbols
                    {
                            "SELECT sym, price, ts FROM trades WHERE ts IN @day AND sym IN @syms",
                            """
                            sym\tprice\tts
                            BTC-USD\t11.0\t2026-11-11T00:00:00.000000Z
                            ETH-USD\t20.0\t2026-11-11T01:30:00.000000Z
                            BTC-USD\t12.0\t2026-11-11T03:00:00.000000Z
                            """,
                            false, true
                    },
                    // the hours from @from up to @to, the empty one filled
                    {
                            "SELECT ts, sum(price) s FROM trades WHERE sym IN @syms SAMPLE BY 1h FROM @from TO @to FILL(NULL)",
                            """
                            ts\ts
                            2026-11-11T00:00:00.000000Z\t11.0
                            2026-11-11T01:00:00.000000Z\t20.0
                            2026-11-11T02:00:00.000000Z\tnull
                            2026-11-11T03:00:00.000000Z\t12.0
                            """,
                            false, false
                    },
                    // the last trade of each symbol on the day
                    {
                            "SELECT sym, price, ts FROM trades WHERE ts IN @day LATEST ON ts PARTITION BY sym",
                            """
                            sym\tprice\tts
                            ETH-USD\t20.0\t2026-11-11T01:30:00.000000Z
                            BTC-USD\t12.0\t2026-11-11T03:00:00.000000Z
                            SOL-USD\t30.0\t2026-11-11T23:59:59.999999Z
                            """,
                            true, true
                    },
                    // each price with the @n before it
                    {
                            "SELECT ts, price, sum(price) OVER (ORDER BY ts ROWS BETWEEN @n PRECEDING AND CURRENT ROW) s FROM trades WHERE sym = 'BTC-USD'",
                            """
                            ts\tprice\ts
                            2026-11-10T23:00:00.000000Z\t10.0\t10.0
                            2026-11-11T00:00:00.000000Z\t11.0\t21.0
                            2026-11-11T03:00:00.000000Z\t12.0\t23.0
                            2026-11-12T00:00:00.000000Z\t13.0\t25.0
                            """,
                            false, false
                    },
                    // the last two trades
                    {
                            "SELECT sym, price, ts FROM trades LIMIT @last",
                            """
                            sym\tprice\tts
                            SOL-USD\t30.0\t2026-11-11T23:59:59.999999Z
                            BTC-USD\t13.0\t2026-11-12T00:00:00.000000Z
                            """,
                            true, true
                    },
            };
            for (Object[] c : cases) {
                final String sql = (String) c[0];
                final String result = (String) c[1];
                final boolean isSizeKnown = (boolean) c[2];
                final boolean isRandomAccess = (boolean) c[3];
                final String declared = "DECLARE " + variables + ' ' + sql;
                assertQuery(declared).noLeakCheck().timestamp("ts")
                        .expectSize(isSizeKnown).supportsRandomAccess(isRandomAccess).returns(result);
                withVariables(variables, sql, () -> assertQuery(sql).noLeakCheck().timestamp("ts")
                        .expectSize(isSizeKnown).supportsRandomAccess(isRandomAccess).returns(result));
                assertSamePrint("EXPLAIN " + declared, "EXPLAIN " + sql, variables);
            }

            // the scan reads the day alone
            final StringSink plan = new StringSink();
            final String explain = "EXPLAIN " + cases[0][0];
            withVariables(variables, explain, () -> printSql(explain, plan));
            TestUtils.assertContains(plan, "intervals: [(\"2026-11-11T00:00:00.000000Z\",\"2026-11-11T23:59:59.999999Z\")]");
        });
    }

    @Test
    public void testUnusedSubQueryValueIsCompiled() throws Exception {
        // As in a DECLARE block that opens the statement, a sub-query in a value compiles even
        // when the statement does not read its variable. nope sits 29 characters into
        // "DECLARE @q := (SELECT * FROM nope) SELECT 1 x", and 21 into the assignments.
        assertQuery("DECLARE @q := (SELECT * FROM nope) SELECT 1 x").fails(29, "table does not exist [table=nope]");
        final String sql = "SELECT 1 x";
        withVariables(
                "@q := (SELECT * FROM nope)",
                sql,
                () -> assertQuery(sql).fails(SqlParser.QUERY_VARIABLES_POSITION + 21, "table does not exist [table=nope]")
        );
    }

    @Test
    public void testUpdateDoesNotReadViewsOfVariables() throws Exception {
        assertMemoryLeak(() -> {
            // A view that only the variables read is no view the UPDATE reads: dropping it
            // between the compile and the execution of the UPDATE leaves the UPDATE as it is
            // without the parameter. The table is not WAL, so the compiled UPDATE runs as is.
            final String variables = "@x := (SELECT max(a) FROM vw)";
            final String[][] cases = {
                    {"UPDATE t SET v = 5", "a\tv\n1\t5\n2\t5\n"},
                    {"WITH c AS (SELECT 7 x) UPDATE t SET v = c.x FROM c", "a\tv\n1\t7\n2\t7\n"},
            };
            for (String[] c : cases) {
                final String sql = c[0];
                execute("CREATE TABLE t (a INT, v INT)");
                execute("INSERT INTO t VALUES (1, 0), (2, 0)");
                execute("CREATE VIEW vw AS (SELECT a FROM t)");
                drainWalAndViewQueues();
                final SCSequence eventSubSequence = new SCSequence();
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery[] cq = new CompiledQuery[1];
                    withVariables(variables, sql, () -> cq[0] = compiler.compile(sql, sqlExecutionContext));
                    Assert.assertEquals(CompiledQuery.UPDATE, cq[0].getType());
                    execute("DROP VIEW vw");
                    try (OperationFuture fut = cq[0].execute(eventSubSequence)) {
                        fut.await();
                        Assert.assertEquals(2, fut.getAffectedRowsCount());
                    }
                }
                assertQuery("SELECT * FROM t").noLeakCheck().expectSize().returns(c[1]);
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testUpdateIgnoresVariables() throws Exception {
        assertMemoryLeak(() -> {
            // The WAL applies an UPDATE by parsing its text again, without the variables.
            execute("CREATE TABLE t (v INT)");
            // as without the variables
            final String update = "UPDATE t SET v = @a";
            withVariables("@a := 1", update, () -> assertExceptionNoLeakCheck(update, 17, "Invalid column: @a"));
            final String withUpdate = "WITH c AS (SELECT @a x) UPDATE t SET v = c.x FROM c";
            withVariables("@a := 1", withUpdate, () -> assertExceptionNoLeakCheck(withUpdate, 18, "Invalid column: @a"));
        });
    }

    @Test
    public void testValueEndingWithLineComment() throws Exception {
        final String sql = "SELECT @a + @b x";
        withVariables("@a := 1 -- one\n, @b := 2", sql, () -> assertQuery(sql).expectSize().returns("x\n3\n"));
    }

    @Test
    public void testValueMayNotHoldAQuery() throws Exception {
        final String sql = "SELECT @a";
        // "@a := 1 SELECT 2": SELECT sits 8 characters into the assignments
        withVariables("@a := 1 SELECT 2", sql, () -> assertQuery(sql)
                .fails(SqlParser.QUERY_VARIABLES_POSITION + 8, "unexpected token [SELECT] - ',' or end of variables expected"));
    }

    @Test
    public void testVariablesReadEarlierVariables() throws Exception {
        assertMemoryLeak(() -> {
            final String variables = "@a := 1, @b := @a + 1, @c := ('x', 'y')";
            final String scalars = "SELECT @a a, @b b";
            withVariables(variables, scalars, () -> assertQuery(scalars).noLeakCheck().expectSize().returns("a\tb\n1\t2\n"));
            final String list = "SELECT 'x' IN @c a, 'z' IN @c b";
            withVariables(variables, list, () -> assertQuery(list).noLeakCheck().expectSize().returns("a\tb\ntrue\tfalse\n"));
        });
    }

    @Test
    public void testVariablesTooLong() throws Exception {
        // The variables end before the last position an int holds, or the positions past them
        // would wrap. The parser refuses longer variables before it copies them: these blanks
        // take no memory.
        final int max = Integer.MAX_VALUE - SqlParser.QUERY_VARIABLES_POSITION;
        final CharSequence variables = new CharSequence() {
            @Override
            public char charAt(int index) {
                return ' ';
            }

            @Override
            public int length() {
                return max + 1;
            }

            @Override
            public @NotNull CharSequence subSequence(int start, int end) {
                throw new UnsupportedOperationException();
            }
        };
        final String sql = "SELECT 1 x";
        withVariables(variables, sql, () -> assertQuery(sql).fails(SqlParser.QUERY_VARIABLES_POSITION, "variables are too long [max=" + max + ']'));
    }

    @Test
    public void testViewReadsVariableItLetsCallersOverride() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            execute("CREATE VIEW v_ovl AS (DECLARE OVERRIDABLE @lim := 2 SELECT l FROM k WHERE l <= @lim)");
            execute("CREATE VIEW v_fixed AS (DECLARE @lim := 2 SELECT l FROM k WHERE l <= @lim)");
            // the body's own block keeps both markers, which the client's block refuses
            execute("CREATE VIEW v_aud AS (DECLARE AUDITED OVERRIDABLE @lim := 2 SELECT l FROM k WHERE l <= @lim)");
            drainWalAndViewQueues();
            // as with DECLARE @lim := 1 SELECT * FROM v_ovl
            final String overridable = "SELECT * FROM v_ovl";
            withVariables("@lim := 1", overridable, () -> assertQuery(overridable).noLeakCheck().returns("l\n1\n"));
            final String audited = "SELECT * FROM v_aud";
            withVariables("@lim := 1", audited, () -> assertQuery(audited).noLeakCheck().returns("l\n1\n"));
            assertQuery("DECLARE @lim := 1 SELECT * FROM v_aud").noLeakCheck().returns("l\n1\n");
            // "@lim := 1": the error points at ":=", 5 characters into the assignments
            final String fixed = "SELECT * FROM v_fixed";
            withVariables("@lim := 1", fixed, () -> assertQuery(fixed).noLeakCheck().fails(SqlParser.QUERY_VARIABLES_POSITION + 5, "variable is not overridable: @lim"));
            assertQuery("DECLARE @lim := 1 SELECT * FROM v_ovl").noLeakCheck().returns("l\n1\n");
        });
    }

    @Test
    public void testWithClauseLeavesVariablesToTheRoot() throws Exception {
        assertMemoryLeak(() -> {
            // A WITH clause parses its CTEs ahead of the statement's root and hands each body the
            // variables' declarations. A body copies them, and its own DECLARE block declares over
            // the copy: only the root takes the model the variables were parsed into, with the
            // sub-queries of their values. Each statement answers as it does with the assignments
            // as a DECLARE block that opens it.
            execute("CREATE TABLE s (v INT)");
            execute("INSERT INTO s VALUES (1), (2), (3)");
            execute("CREATE TABLE r (a INT, v INT)");
            drainWalQueue();
            // variables, statement, result, whether its cursor knows its size and supports random
            // access
            final Object[][] cases = {
                    // a CTE that declares a variable of the client's name keeps it to itself
                    {"@a := 1", "WITH c AS (DECLARE @a := 5 SELECT @a v) SELECT @a a, v FROM c", "a\tv\n1\t5\n", true, true},
                    {"@a := 1", "WITH c AS (DECLARE @a := 5 SELECT @a v), d AS (SELECT @a w) SELECT * FROM c CROSS JOIN d", "v\tw\n5\t1\n", true, false},
                    // the root reads a sub-query value that no CTE reads
                    {"@m := (SELECT max(v) FROM s)", "WITH c AS (SELECT 1 y) SELECT v FROM s WHERE v = @m", "v\n3\n", false, true},
            };
            for (Object[] c : cases) {
                final String variables = (String) c[0];
                final String sql = (String) c[1];
                final String result = (String) c[2];
                final boolean isSizeKnown = (boolean) c[3];
                final boolean isRandomAccess = (boolean) c[4];
                assertQuery("DECLARE " + variables + ' ' + sql).noLeakCheck()
                        .expectSize(isSizeKnown).supportsRandomAccess(isRandomAccess).returns(result);
                withVariables(variables, sql, () -> assertQuery(sql).noLeakCheck()
                        .expectSize(isSizeKnown).supportsRandomAccess(isRandomAccess).returns(result));
            }

            // A sub-query value that nothing reads still compiles, see
            // testUnusedSubQueryValueIsCompiled. nope sits 29 characters into the DECLARE form,
            // and 21 into the assignments.
            final String unread = "WITH c AS (SELECT 1 y) SELECT 1 x";
            final String nope = "@q := (SELECT * FROM nope)";
            assertQuery("DECLARE " + nope + ' ' + unread).noLeakCheck().fails(29, "table does not exist [table=nope]");
            withVariables(nope, unread, () -> assertQuery(unread).noLeakCheck()
                    .fails(SqlParser.QUERY_VARIABLES_POSITION + 21, "table does not exist [table=nope]"));

            // the query of an INSERT reads the client's value, and the CTE its own
            executeWithVariables("@a := 1", "WITH c AS (DECLARE @a := 5 SELECT @a v) INSERT INTO r SELECT @a, v FROM c");
            drainWalQueue();
            assertQuery("SELECT * FROM r").noLeakCheck().expectSize().returns("a\tv\n1\t5\n");
        });
    }

    @Test
    public void testWithClauseOfEveryShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1)");
            drainWalQueue();
            // brackets inside a CTE body, and a list of CTEs
            final String select = "WITH c AS (SELECT ((@a)) x), d AS (SELECT x + 1 y FROM c) SELECT y FROM d";
            withVariables("@a := 2", select, () -> assertQuery(select).noLeakCheck().expectSize().returns("y\n3\n"));
            // an UPDATE after brackets and a list of CTEs still skips the variables
            final String update = "WITH c AS (SELECT (1) x), d AS (SELECT @a y) UPDATE t SET v = d.y FROM d";
            withVariables("@a := 2", update, () -> assertExceptionNoLeakCheck(update, 39, "Invalid column: @a"));
            // a WITH clause the parser rejects fails as it does without the variables
            final String[] malformed = {
                    "WITH",
                    "WITH/**/",
                    "WITH-- comment",
                    "WITH c",
                    "WITH c x (SELECT 1)",
                    "WITH c AS",
                    "WITH c AS SELECT 1",
                    "WITH c AS (SELECT 1",
                    "WITH c AS (SELECT 1)",
                    "WITH c AS (SELECT 1),",
                    "WITH c AS (SELECT 1) INSERT INTO t",
                    "WITH c AS (SELECT 1) INSERT INTO t VALUES",
                    // an unclosed quote after an earlier error: the earlier error is reported
                    "WITH c AS (SELECT , \"x) SELECT * FROM c",
                    "WITH c AS (SELECT 1 x), d AS (SELECT , \"y) UPDATE t SET v = d.y FROM d",
            };
            for (String sql : malformed) {
                assertFailsAsWithoutVariables(sql, "@a := 2");
            }
        });
    }

    @Test
    public void testWithClauseReadingViewReadsVariables() throws Exception {
        assertMemoryLeak(() -> {
            // A WITH clause parses its CTEs before the statement's root takes the variables, and a
            // view that a CTE reads parses its body as a top-level query. The body takes a model of
            // its own all the same: the view reads the variables as a caller's values, and keeps its
            // own declarations to itself. Each statement answers as it does with the assignments as
            // a DECLARE block that opens it.
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            execute("CREATE VIEW v AS (SELECT l FROM k)");
            execute("CREATE VIEW v_ovl AS (DECLARE OVERRIDABLE @lim := 2, @y := 42 SELECT l FROM k WHERE l <= @lim)");
            drainWalAndViewQueues();

            final String cte = "WITH c AS (SELECT l, @x x FROM v) SELECT * FROM c";
            final String rows = """
                    l\tx
                    1\t1
                    2\t1
                    3\t1
                    """;
            assertQuery("DECLARE @x := 1 " + cte).noLeakCheck().expectSize().returns(rows);
            withVariables("@x := 1", cte, () -> assertQuery(cte).noLeakCheck().expectSize().returns(rows));

            // the caller's value for the view's OVERRIDABLE variable
            final String overridden = "WITH c AS (SELECT * FROM v_ovl) SELECT * FROM c";
            assertQuery("DECLARE @lim := 1 " + overridden).noLeakCheck().returns("l\n1\n");
            withVariables("@lim := 1", overridden, () -> assertQuery(overridden).noLeakCheck().returns("l\n1\n"));

            // the view's own @y is no variable of the statement
            final String own = "WITH c AS (SELECT * FROM v_ovl) SELECT @y y FROM c";
            final String undeclared = "tried to use undeclared variable `@y`";
            final String declare = "DECLARE @lim := 1 ";
            assertQuery(declare + own).noLeakCheck().fails(declare.length() + own.indexOf("@y"), undeclared);
            withVariables("@lim := 1", own, () -> assertQuery(own).noLeakCheck().fails(own.indexOf("@y"), undeclared));
        });
    }

    // the advisory the parser logs in legacy operator precedence mode, see
    // testLegacyPrecedenceAdvisoryShowsVariables
    private static String advisory(String text, int position) {
        return "operator precedence compat mode: detected expression parsing behaviour change for query=[" + text + "] at position=" + position + Misc.EOL;
    }

    // The statement fails with the variables as it does without them. Returns the error message.
    private static String assertFailsAsWithoutVariables(String sql, String variables) throws Exception {
        final int position;
        final String message;
        try {
            execute(sql);
            throw new AssertionError("expected an error for " + sql);
        } catch (Throwable e) {
            if (!(e instanceof FlyweightMessageContainer container)) {
                throw e;
            }
            position = container.getPosition();
            message = container.getFlyweightMessage().toString();
        }
        withVariables(variables, sql, () -> assertExceptionNoLeakCheck(sql, position, message));
        return message;
    }

    // The statement fails with the variables as it does without them, at a position before the
    // variables: no error in them. Compiling or reading the statement reports the error.
    private static void assertFailsWithVariablesAsWithout(String sql, String variables, String message) throws Exception {
        final int position;
        try {
            assertExceptionNoLeakCheck(sql);
            throw new AssertionError("expected an error for " + sql);
        } catch (Throwable e) {
            if (!(e instanceof FlyweightMessageContainer container)) {
                throw e;
            }
            TestUtils.assertEquals(message, container.getFlyweightMessage());
            position = container.getPosition();
        }
        // past the statement's text: only QUERY_VARIABLES_POSITION tells it from an error in them
        Assert.assertTrue(position > sql.length());
        Assert.assertTrue(position < SqlParser.QUERY_VARIABLES_POSITION);
        withVariables(variables, sql, () -> assertExceptionNoLeakCheck(sql, position, message));
    }

    // The statement sent with the variables prints what the reference prints without them: for
    // EXPLAIN, the same plan. Returns the print.
    private static String assertSamePrint(String reference, String sql, String variables) throws Exception {
        final StringSink expected = new StringSink();
        printSql(reference, expected);
        final StringSink actual = new StringSink();
        withVariables(variables, sql, () -> printSql(sql, actual));
        TestUtils.assertEquals(expected, actual);
        return expected.toString();
    }

    private static void executeWithVariables(String variables, String sql) throws Exception {
        withVariables(variables, sql, () -> execute(sql));
    }

    // Runs the code with the variables a client sent with the statement's text, as /exec does.
    // DeclareListFuzzTest sends its declared lists this way too.
    static void withVariables(CharSequence variables, String sql, TestUtils.LeakProneCode code) throws Exception {
        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
        context.setQueryVariables(variables, sql);
        try {
            code.run();
        } finally {
            context.setQueryVariables(null, null);
        }
    }
}
