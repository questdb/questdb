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

import io.questdb.griffin.QueryVariables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Variables a client declares outside the statement's text, see {@link QueryVariables}. The
 * statement reads them as a DECLARE block that opens it. Each expectation holds the result of
 * the same statement with the variables written as that block, which the test also runs.
 */
public class QueryVariablesSqlTest extends AbstractCairoTest {

    @Test
    public void testErrorInValuePointsPastTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            // The value is parsed from the statement's text, a newline and "@a := nope_fn()", so
            // its position lies past the statement: 10 + the 6 characters of "@a := ".
            final String sql = "SELECT @a";
            withVariables("[{\"name\":\"a\",\"value\":\"nope_fn()\"}]", () -> assertQuery(sql)
                    .fails(sql.length() + 1 + 6, "unknown function name: nope_fn()"));
        });
    }

    @Test
    public void testExplainReadsVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            final StringSink expected = new StringSink();
            printSql("EXPLAIN DECLARE @lim := 3 SELECT v FROM t WHERE v > @lim", expected);
            final StringSink actual = new StringSink();
            withVariables("[{\"name\":\"lim\",\"value\":\"3\"}]", () -> printSql("EXPLAIN SELECT v FROM t WHERE v > @lim", actual));
            TestUtils.assertContains(expected, "filter:");
            TestUtils.assertEquals(expected, actual);
        });
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
                final StringSink expected = new StringSink();
                printSql(statement[0], expected);
                final StringSink actual = new StringSink();
                withVariables("[{\"name\":\"a\",\"value\":\"7\"}]", () -> printSql(statement[1], actual));
                TestUtils.assertEquals(expected, actual);
            }
        });
    }

    @Test
    public void testWithClauseOfEveryShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1)");
            drainWalQueue();
            withVariables("[{\"name\":\"a\",\"value\":\"2\"}]", () -> {
                // brackets inside a CTE body, and a list of CTEs
                assertQuery("WITH c AS (SELECT ((@a)) x), d AS (SELECT x + 1 y FROM c) SELECT y FROM d").expectSize().returns("y\n3\n");
                // an UPDATE after brackets and a list of CTEs still skips the variables
                assertExceptionNoLeakCheck("WITH c AS (SELECT (1) x), d AS (SELECT @a y) UPDATE t SET v = d.y FROM d", 39, "Invalid column: @a");
            });
            // a WITH clause the parser rejects fails as it does without the variables
            final String[] malformed = {
                    "WITH",
                    "WITH c",
                    "WITH c x (SELECT 1)",
                    "WITH c AS",
                    "WITH c AS SELECT 1",
                    "WITH c AS (SELECT 1",
                    "WITH c AS (SELECT 1)",
                    "WITH c AS (SELECT 1),",
            };
            for (String sql : malformed) {
                final int position;
                final String message;
                try {
                    execute(sql);
                    throw new AssertionError("expected an error for " + sql);
                } catch (SqlException e) {
                    position = e.getPosition();
                    message = e.getFlyweightMessage().toString();
                }
                withVariables("[{\"name\":\"a\",\"value\":\"2\"}]", () -> assertExceptionNoLeakCheck(sql, position, message));
            }
        });
    }

    @Test
    public void testInsertSelectReadsVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, s VARCHAR)");
            withVariables(
                    "[{\"name\":\"v\",\"value\":\"41 + 1\"},{\"name\":\"s\",\"value\":\"'x' || @v\"}]",
                    () -> execute("INSERT INTO t SELECT @v, @s")
            );
            withVariables(
                    "[{\"name\":\"v\",\"value\":\"7\"}]",
                    () -> execute("WITH c AS (SELECT @v v) INSERT INTO t SELECT v, 'cte' FROM c")
            );
            drainWalQueue();
            assertQuery("SELECT * FROM t").expectSize().returns("""
                    v\ts
                    42\tx42
                    7\tcte
                    """);
        });
    }

    @Test
    public void testNoVariablesWithEmptyArray() throws Exception {
        assertMemoryLeak(() -> withVariables("[]", () -> assertQuery("SELECT @a")
                // as SELECT @a without the variables
                .fails(7, "Invalid column: @a")));
    }

    @Test
    public void testQueriesOfEveryShapeReadVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1), (2), (3)");
            drainWalQueue();
            final String variables = "[{\"name\":\"a\",\"value\":\"2\"}]";
            withVariables(variables, () -> {
                // implicit SELECT
                assertQuery("t WHERE v >= @a").returns("""
                        v
                        2
                        3
                        """);
                // CTE, sub-query in FROM, set operation branches, IN list
                assertQuery("WITH c AS (SELECT @a x) SELECT x FROM c").expectSize().returns("x\n2\n");
                assertQuery("SELECT * FROM (SELECT v FROM t WHERE v < @a)").returns("v\n1\n");
                assertQuery("SELECT @a x UNION ALL SELECT @a + 1").expectSize().noRandomAccess().returns("x\n2\n3\n");
                assertQuery("t WHERE v IN (@a, 3)").returns("v\n2\n3\n");
            });
        });
    }

    @Test
    public void testStatementDeclarationReadsVariables() throws Exception {
        assertMemoryLeak(() -> withVariables(
                "[{\"name\":\"a\",\"value\":\"1\"}]",
                // the statement's own block reads the client's variables, as its own earlier
                // declarations; DECLARE @a := 1, @b := @a + 10 SELECT @b
                () -> assertQuery("DECLARE @b := @a + 10 SELECT @b b").expectSize().returns("b\n11\n")
        ));
    }

    @Test
    public void testStatementDeclarationWins() throws Exception {
        assertMemoryLeak(() -> withVariables(
                "[{\"name\":\"a\",\"value\":\"1\"},{\"name\":\"b\",\"value\":\"@a + 1\"}]",
                // DECLARE @a := 1, @b := @a + 1, @a := 5 SELECT @a, @b: the statement's @a is the
                // last one declared, and @b keeps the value it was declared with
                () -> assertQuery("DECLARE @a := 5 SELECT @a a, @b b").expectSize().returns("a\tb\n5\t2\n")
        ));
    }

    @Test
    public void testSubQueryValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            drainWalQueue();
            withVariables("[{\"name\":\"q\",\"value\":\"(SELECT v FROM t WHERE v > 1)\"}]", () -> assertQuery("SELECT * FROM @q")
                    .returns("v\n2\n"));
            // "@q := (SELECT * FROM nope)": nope sits 21 characters into the assignments
            final String sql = "SELECT * FROM @q";
            withVariables("[{\"name\":\"q\",\"value\":\"(SELECT * FROM nope)\"}]", () -> assertQuery(sql)
                    .fails(sql.length() + 1 + 21, "table does not exist [table=nope]"));
        });
    }

    @Test
    public void testUnusedSubQueryValueIsNotCompiled() throws Exception {
        assertMemoryLeak(() -> withVariables(
                // Unlike the same value in the statement's own DECLARE block, a variable the
                // statement does not read never reaches the compiler, so its sub-query may read a
                // table that does not exist.
                "[{\"name\":\"q\",\"value\":\"(SELECT * FROM nope)\"}]",
                () -> assertQuery("SELECT 1 x").expectSize().returns("x\n1\n")
        ));
    }

    @Test
    public void testUpdateIgnoresVariables() throws Exception {
        assertMemoryLeak(() -> {
            // The WAL applies an UPDATE by parsing its text again, without the variables.
            execute("CREATE TABLE t (v INT)");
            withVariables("[{\"name\":\"a\",\"value\":\"1\"}]", () -> {
                // as without the variables
                assertExceptionNoLeakCheck("UPDATE t SET v = @a", 17, "Invalid column: @a");
                assertExceptionNoLeakCheck("WITH c AS (SELECT @a x) UPDATE t SET v = c.x FROM c", 18, "Invalid column: @a");
            });
        });
    }

    @Test
    public void testValueEndingWithLineComment() throws Exception {
        assertMemoryLeak(() -> withVariables(
                "[{\"name\":\"a\",\"value\":\"1 -- one\"},{\"name\":\"b\",\"value\":\"2\"}]",
                () -> assertQuery("SELECT @a + @b x").expectSize().returns("x\n3\n")
        ));
    }

    @Test
    public void testValueMayNotHoldAnotherAssignment() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "SELECT @a";
            // "@a := 1, @z := 2": @z sits 9 characters into the assignments
            withVariables("[{\"name\":\"a\",\"value\":\"1, @z := 2\"}]", () -> assertQuery(sql)
                    .fails(sql.length() + 1 + 9, "a variable value must be a single expression"));
        });
    }

    @Test
    public void testValueMayNotHoldAQuery() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "SELECT @a";
            // "@a := 1 SELECT 2": SELECT sits 8 characters into the assignments
            withVariables("[{\"name\":\"a\",\"value\":\"1 SELECT 2\"}]", () -> assertQuery(sql)
                    .fails(sql.length() + 1 + 8, "unexpected token [SELECT]"));
        });
    }

    @Test
    public void testVariablesReadEarlierVariables() throws Exception {
        assertMemoryLeak(() -> withVariables(
                "[{\"name\":\"a\",\"value\":\"1\"},{\"name\":\"b\",\"value\":\"@a + 1\"},{\"name\":\"c\",\"value\":\"('x', 'y')\"}]",
                () -> {
                    assertQuery("SELECT @a a, @b b").expectSize().returns("a\tb\n1\t2\n");
                    assertQuery("SELECT 'x' IN @c a, 'z' IN @c b").expectSize().returns("a\tb\ntrue\tfalse\n");
                }
        ));
    }

    @Test
    public void testViewReadsVariableItLetsCallersOverride() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            execute("CREATE VIEW v_ovl AS (DECLARE OVERRIDABLE @lim := 2 SELECT l FROM k WHERE l <= @lim)");
            execute("CREATE VIEW v_fixed AS (DECLARE @lim := 2 SELECT l FROM k WHERE l <= @lim)");
            drainWalAndViewQueues();
            // as with DECLARE @lim := 1 SELECT * FROM v_ovl
            withVariables("[{\"name\":\"lim\",\"value\":\"1\"}]", () -> {
                assertQuery("SELECT * FROM v_ovl").noLeakCheck().returns("l\n1\n");
                // "@lim := 1": the error points at ":=", 5 characters into the assignments
                assertQuery("SELECT * FROM v_fixed").noLeakCheck().fails("SELECT * FROM v_fixed".length() + 1 + 5, "variable is not overridable: @lim");
            });
            assertQuery("DECLARE @lim := 1 SELECT * FROM v_ovl").noLeakCheck().returns("l\n1\n");
        });
    }

    private static void withVariables(String json, VariablesCode code) throws Exception {
        final QueryVariables variables = new QueryVariables();
        variables.of(json);
        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
        context.setQueryVariables(variables);
        try {
            code.run();
        } finally {
            context.setQueryVariables(null);
        }
    }

    @FunctionalInterface
    private interface VariablesCode {
        void run() throws Exception;
    }
}
