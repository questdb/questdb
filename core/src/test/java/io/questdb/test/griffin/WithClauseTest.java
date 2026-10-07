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
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WithClauseTest extends AbstractCairoTest {

    @Test
    public void testCteBodySubQueryDoesNotReadLaterCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("CREATE TABLE y (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            execute("INSERT INTO y VALUES ('a')");
            // A CTE body sees the CTEs defined before it, not the ones after it, so y is the table
            // in the body of u, as it is for a FROM clause there. The first reference to u takes
            // the model the definition parsed, which read the table. The second parses the body
            // again, and its sub-query used to read the CTE y, which returned b.
            assertQuery("WITH u AS (SELECT x FROM t WHERE x IN (SELECT x FROM y)), y AS (SELECT 'b'::SYMBOL x) SELECT * FROM u UNION ALL SELECT * FROM u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            a
                            a
                            """);
            assertQuery("WITH u AS (SELECT x FROM y), y AS (SELECT 'b'::SYMBOL x) SELECT * FROM u UNION ALL SELECT * FROM u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x
                            a
                            a
                            """);
        });
    }

    @Test
    public void testCteBodySubQueryNamingItsOwnCteReadsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("CREATE TABLE y (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            execute("INSERT INTO y VALUES ('a')");
            // The body of y does not see y itself, so its sub-query reads the table. The second
            // reference parses the body again, and its sub-query used to read the CTE y, whose
            // body it parsed again, until the stack overflowed.
            assertQuery("WITH y AS (SELECT x FROM t WHERE x IN (SELECT x FROM y)) SELECT * FROM y UNION ALL SELECT * FROM y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            a
                            a
                            """);
        });
    }

    @Test
    public void testCteDenseChainHasNodeBudget() throws Exception {
        assertMemoryLeak(() -> {
            // A copy of a CTE takes as many expression nodes as its text holds, but only one or two
            // query models, so a chain over a CTE with dense text copies far more nodes than models.
            // A statement may take 10,000 nodes, plus 20 for each character of its text. Five levels
            // over a sum of 250 terms parse c0 32 times, 16,263 nodes of their 26,780.
            assertQuery(denseCteChain(5))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            32\t8000
                            """);
            // Six levels would parse c0 64 times. The parser refuses the first copy that finds the
            // nodes spent, at the reference that would parse it, a read of c0 in the text of c1.
            final String sixLevels = denseCteChain(6);
            assertTooComplexToParse(
                    sixLevels,
                    sixLevels.indexOf("UNION ALL SELECT * FROM c0") + "UNION ALL SELECT * FROM ".length(),
                    "nodes",
                    27_840
            );
            // In 825 characters, CTEs that each read the one before four times would parse c0 16,384
            // times. While only query models counted, the models stayed within budget until the
            // copies had taken 365,336 nodes, so the parser kept about 140 MB before it refused the
            // statement.
            final String fanOut = fanOutCteChain();
            assertTooComplexToParse(
                    fanOut,
                    fanOut.indexOf("c0,c0 a") + "c0,".length(),
                    "nodes",
                    26_500
            );
        });
    }

    @Test
    public void testCteDoublingChainHasModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            // A reference to a CTE after the one that takes the definition's model parses a copy
            // of the CTE's text, and the copy parses a copy of every CTE it reads, whose models
            // their first references took. So when each CTE reads the one before it twice, the
            // query models the parse takes double with every level: 382 at six levels, over 6,000
            // at ten and over six million at twenty, every model kept by a pool that never shrinks
            // while the compiler lives. A statement may take 1,000 models, plus 2 for each
            // character of its text. Eight levels take 1,534 of their 2,000.
            assertQuery(doublingCteChain(8))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            768\t1536
                            """);
            // Nine levels would take over 3,000 of their 2,106. The parser refuses the first copy
            // that finds the budget spent, at the reference that would parse it, a read of q0 in
            // the text of q1.
            final String nineLevels = doublingCteChain(9);
            assertTooComplexToParse(
                    nineLevels,
                    nineLevels.indexOf("UNION ALL SELECT * FROM q0") + "UNION ALL SELECT * FROM ".length(),
                    "models",
                    2106
            );
            // Twenty levels stop as soon as the parse has taken their 3,336.
            final String twentyLevels = doublingCteChain(20);
            assertTooComplexToParse(
                    twentyLevels,
                    twentyLevels.indexOf("UNION ALL SELECT * FROM q0") + "UNION ALL SELECT * FROM ".length(),
                    "models",
                    3336
            );
        });
    }

    @Test
    public void testCteLiteralChainHasCharacterBudget() throws Exception {
        assertMemoryLeak(() -> {
            // A reference to a declared variable copies the variable's value, and the parser writes
            // the alias of every column without one to the character store. Without expression
            // aliases, each of the 29 repeated names in c0 copies the 800-character literal twice, so
            // a parse of c0 writes about 46,000 characters. A statement may write 250,000 characters,
            // plus 500 for each character of its text. One level that reads c0 four times writes
            // 185,801 of its 753,500.
            assertQuery(declaredLiteralCteChain(1))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
            // Seven levels would parse c0 16,384 times. While the store did not count, the parse
            // wrote 24 million characters, about 100 MB, before the node budget refused the
            // statement. The parser now refuses the first copy that finds the characters spent, at
            // a read of c0 in the text of c1.
            final String sevenLevels = declaredLiteralCteChain(7);
            assertTooComplexToParse(
                    sevenLevels,
                    sevenLevels.indexOf("c0 d"),
                    "chars",
                    864_500
            );
        });
    }

    @Test
    public void testCteLiteralChainHasCharacterBudgetWithExpressionAliases() throws Exception {
        // The server's default configuration names a column without an alias after its expression,
        // so each such column in c0 writes the whole 800-character literal to the character store,
        // and a parse of c0 writes about 50,000 characters.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            // One level that reads c0 four times writes 200,173 of the 753,500 characters its text
            // allows.
            assertQuery(declaredLiteralCteChain(1))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count()
                            1
                            """);
            // Seven levels used to write 25.8 million characters, about 90 MB, before the node
            // budget refused the statement.
            final String sevenLevels = declaredLiteralCteChain(7);
            assertTooComplexToParse(
                    sevenLevels,
                    sevenLevels.indexOf("c0 b"),
                    "chars",
                    864_500
            );
        });
    }

    @Test
    public void testCteReadMoreThanOnceKeepsGeneratedColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            // The test configuration names an unaliased constant with a dot in it column1, column2
            // and so on, from a counter the parser keeps for the whole statement. The first
            // reference to w takes the model the definition parsed, and every later reference
            // parses the text of w again. That parse used to go on counting where the statement
            // had got to: the second reference saw w as column2 and column3, and returned 1.5 for
            // column2.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT column2 FROM w UNION ALL SELECT column2 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            // The second reference had no column1 at all, and the third no column2.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT column1 FROM w UNION ALL SELECT column1 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1
                            1.5
                            1.5
                            """);
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT column2 FROM w UNION ALL SELECT column2 FROM w UNION ALL SELECT column2 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            2.5
                            """);
            // The references expose the same names, so a join of them suffixes the second
            // reference's. The second reference used to expose column2 and column3, which the join
            // named column21 and column3.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM w a CROSS JOIN w b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21
                            1.5	2.5	1.5	2.5
                            """);
            // A CTE that reads w twice, read twice itself.
            assertQuery("WITH w AS (SELECT 1.5, 2.5), u AS (SELECT column2 FROM w UNION ALL SELECT column2 FROM w) SELECT * FROM u UNION ALL SELECT * FROM u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            2.5
                            2.5
                            """);
            // A nested WITH in a CTE read twice: every parse of the body of u defines w from the
            // same count.
            assertQuery("WITH u AS (WITH w AS (SELECT 1.5, 2.5) SELECT * FROM w a CROSS JOIN w b) SELECT column21 FROM u UNION ALL SELECT column21 FROM u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column21
                            2.5
                            2.5
                            """);
            // The definition skips the number an alias written beside the constants has taken,
            // and so does every later reference.
            assertQuery("WITH w AS (SELECT 1.5 column1, 2.5, 3.5) SELECT column3 FROM w UNION ALL SELECT column3 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column3
                            3.5
                            3.5
                            """);
            // The statement's own constants after the references get the names they get after one
            // reference: a later reference leaves the count where it found it, as taking the
            // model does. The definition of w takes the count to 2, and 7.5 and 8.5, column2 and
            // column3, take it to 3. So 5.5 and 6.5 are column3 and column4, which the join names
            // column31 and column4. Parsing w again used to move the count on, and named them
            // column4 and column5.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (SELECT 7.5, 8.5) x CROSS JOIN (SELECT * FROM w UNION ALL SELECT * FROM w) a CROSS JOIN (SELECT 5.5, 6.5) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column3	column1	column21	column31	column4
                            7.5	8.5	1.5	2.5	5.5	6.5
                            7.5	8.5	1.5	2.5	5.5	6.5
                            """);
            // CREATE TABLE AS keeps the names the references expose.
            execute("CREATE TABLE two_reads AS (WITH w AS (SELECT 1.5, 2.5) SELECT * FROM w a CROSS JOIN w b)");
            assertQuery("SELECT \"column\" FROM table_columns('two_reads')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column11
                            column21
                            """);
        });
    }

    @Test
    public void testCteReadsWithinModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('1'), ('2'), ('3'), ('4'), ('5')");
            // Each read of c after the first parses a copy of c's text, as writing that text out
            // again would, and the budget of a statement grows with its text: these 150 reads take
            // 602 query models of their 9,252.
            assertQuery(readsOfCte(150))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            3
                            """);
            // A copy of c parses copies of b and a inside it, which take models like any other:
            // 52 reads take 418 of their 3,924.
            final String pipeline = "WITH a AS (SELECT x FROM long_sequence(3)), b AS (SELECT x FROM a), c AS (SELECT x FROM b) ";
            assertQuery(pipeline + "SELECT count(), sum(x) FROM (" + unionOfReads("c", 52) + ")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            156\t312
                            """);
            // A generated statement of 27 KB that reads c 1,000 times takes 4,002 models, more than
            // the 1,000 every statement may take, and its text allows 55,152.
            assertQuery(readsOfCte(1_000))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            3
                            """);
            // The allowance of one statement does not carry over to the next one.
            final String nineLevels = doublingCteChain(9);
            assertTooComplexToParse(
                    nineLevels,
                    nineLevels.indexOf("UNION ALL SELECT * FROM q0") + "UNION ALL SELECT * FROM ".length(),
                    "models",
                    2106
            );
        });
    }

    @Test
    public void testCteReadBySubQueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL, y INT)");
            execute("CREATE TABLE t2 (x SYMBOL, y INT)");
            execute("INSERT INTO t VALUES ('a', 1), ('b', 2)");
            final String expected = """
                    x\ty
                    a\t1
                    """;
            assertQuery("WITH a AS (SELECT 'a'::SYMBOL x) SELECT * FROM t WHERE x IN (SELECT x FROM a)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("WITH a AS (SELECT 'a'::SYMBOL x) SELECT * FROM (SELECT * FROM t WHERE x IN (SELECT x FROM a))")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("WITH a AS (SELECT 'a'::SYMBOL x) SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x IN (SELECT x FROM a))")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("WITH a AS (SELECT 'a'::SYMBOL x), b AS (SELECT * FROM t WHERE x IN (SELECT x FROM a)) SELECT * FROM b UNION ALL SELECT * FROM b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            a\t1
                            a\t1
                            """);
            assertQuery("WITH a AS (SELECT 'a'::SYMBOL x) SELECT * FROM t WHERE x IN (SELECT x FROM a) UNION ALL SELECT * FROM t WHERE x NOT IN (SELECT x FROM a)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            a\t1
                            b\t2
                            """);
            execute("WITH a AS (SELECT 'a'::SYMBOL x) INSERT INTO t2 SELECT * FROM t WHERE x IN (SELECT x FROM a)");
            execute("WITH a AS (SELECT 'a'::SYMBOL x) UPDATE t2 SET y = 5 WHERE x IN (SELECT x FROM a)");
            execute("CREATE TABLE t3 AS (WITH a AS (SELECT 'a'::SYMBOL x) SELECT * FROM t WHERE x IN (SELECT x FROM a))");
            assertQuery("SELECT * FROM t2 UNION ALL SELECT * FROM t3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\ty
                            a\t5
                            a\t1
                            """);
        });
    }

    @Test
    public void testCteWideChainHasColumnBudget() throws Exception {
        assertMemoryLeak(() -> {
            // A copy of a CTE that selects x 300 times takes 300 query columns. A statement may take
            // 5,000 columns, plus 10 for each character of its text. Five levels parse c0 32 times,
            // 9,663 columns of their 14,290.
            assertQuery(wideCteChain(5))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            32
                            """);
            // Six levels would parse c0 64 times. The parser refuses the first copy that finds the
            // columns spent, at the reference that would parse it, a read of c1 in the text of c2.
            final String sixLevels = wideCteChain(6);
            assertTooComplexToParse(
                    sixLevels,
                    sixLevels.indexOf("UNION ALL SELECT * FROM c1") + "UNION ALL SELECT * FROM ".length(),
                    "columns",
                    14_820
            );
        });
    }

    @Test
    public void testNestedCteReadBySubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("CREATE TABLE y (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            execute("INSERT INTO y VALUES ('a')");
            // A sub-query in an expression resolves a name as a FROM clause at the same place
            // does, so it sees the WITH of the query it sits in, wherever that query sits. It used
            // to see the top-level WITH only: the nested CTE was missing, or the table of the same
            // name stood in for it.
            final String expected = """
                    x
                    b
                    """;
            assertQuery("SELECT * FROM (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM c))")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT * FROM (WITH y AS (SELECT 'b'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM y))")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @v := 1 WITH y AS (SELECT 'b'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM y)")
                    .noLeakCheck()
                    .returns(expected);
            // The same query at the top level has always read the CTE.
            assertQuery("WITH y AS (SELECT 'b'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM y)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT * FROM (WITH c AS (SELECT 'b'::SYMBOL x), d AS (SELECT x FROM t WHERE x IN (SELECT x FROM c)) SELECT * FROM d UNION ALL SELECT * FROM d)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            b
                            b
                            """);
            // A sub-query that opens with DECLARE can have a WITH of its own, which sees the CTEs
            // around the sub-query, in its body and in its definitions alike.
            assertQuery("WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM t WHERE x IN (DECLARE @v := 1 WITH d AS (SELECT x FROM c) SELECT x FROM d UNION ALL SELECT x FROM c)")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testNestedWithDuplicateName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            // A nested WITH may reuse the name of a CTE around it, but one WITH still cannot
            // define a name twice, whether or not that name shadows a CTE around it.
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM (WITH c AS (SELECT 'b'::SYMBOL x), c AS (SELECT 'c'::SYMBOL x) SELECT x FROM c)")
                    .noLeakCheck()
                    .fails(82, "duplicate name");
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM (WITH d AS (SELECT 'b'::SYMBOL x), c AS (SELECT x FROM d), d AS (SELECT 'c'::SYMBOL x) SELECT x FROM c)")
                    .noLeakCheck()
                    .fails(106, "duplicate name");
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (DECLARE @v := 1 WITH c AS (SELECT 'b'::SYMBOL x), c AS (SELECT 'c'::SYMBOL x) SELECT x FROM c)")
                    .noLeakCheck()
                    .fails(111, "duplicate name");
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x), c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c")
                    .noLeakCheck()
                    .fails(34, "duplicate name");
        });
    }

    @Test
    public void testNestedWithShadowsEnclosingCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            // A nested WITH may reuse the name of a CTE visible around it, as standard SQL allows.
            // Inside the nested query the name binds to the inner CTE, and everywhere else to the
            // outer one. A sub-query in FROM used to fail with "duplicate name".
            final String inner = """
                    x
                    b
                    """;
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(inner);
            assertQuery("SELECT * FROM (WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c))")
                    .noLeakCheck()
                    .expectSize()
                    .returns(inner);
            // a sub-query in JOIN, next to a read of the outer CTE
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT c.x, i.y FROM c CROSS JOIN (WITH c AS (SELECT 'b'::SYMBOL y) SELECT y FROM c) i")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\ty
                            a\tb
                            """);
            // a CTE body, next to a read of the outer CTE
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x), d AS (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c) SELECT * FROM d UNION ALL SELECT * FROM c")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x
                            b
                            a
                            """);
            // Sub-queries in expressions. The one that opens with DECLARE read the inner CTE
            // before as well; the others failed with "duplicate name" once they saw the CTEs
            // around them.
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (DECLARE @v := 1 WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c)")
                    .noLeakCheck()
                    .returns(inner);
            assertQuery("SELECT * FROM (DECLARE @v := 1 WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (DECLARE @w := 1 WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c))")
                    .noLeakCheck()
                    .returns(inner);
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c))")
                    .noLeakCheck()
                    .returns(inner);
            assertQuery("SELECT * FROM (WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (SELECT x FROM (WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c)))")
                    .noLeakCheck()
                    .returns(inner);
            // After the nested query, the name binds to the outer CTE again.
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT x FROM t WHERE x IN (DECLARE @v := 1 WITH c AS (SELECT 'b'::SYMBOL x) SELECT x FROM c) UNION ALL SELECT x FROM t WHERE x IN (SELECT x FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            b
                            a
                            """);
        });
    }

    @Test
    public void testSetOperationBranchWithShadowsEarlierCte() throws Exception {
        assertMemoryLeak(() -> {
            // A set-operation branch that opens with its own WITH may reuse the name of a CTE that
            // an earlier branch or the statement's WITH defines. The parser carries a branch's CTEs
            // into the branches after it, so those branches read the redefinition, not the CTE it
            // shadows.
            final String earlierThenRedefinedTwice = """
                    x
                    a
                    b
                    b
                    """;
            assertQuery("SELECT * FROM (WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM c UNION ALL WITH c AS (SELECT 'b'::SYMBOL x) SELECT * FROM c UNION ALL SELECT * FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(earlierThenRedefinedTwice);
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM c UNION ALL WITH c AS (SELECT 'b'::SYMBOL x) SELECT * FROM c UNION ALL SELECT * FROM c")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(earlierThenRedefinedTwice);
        });
    }

    @Test
    public void testShadowingCteNamingItsOwnNameReadsOuterCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            // A CTE body sees only what was visible before its definition, so a CTE that shadows
            // an outer one and names itself reads the outer CTE, on every parse of the body.
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH c AS (SELECT 'b'::SYMBOL x UNION ALL SELECT x FROM c) SELECT * FROM c UNION ALL SELECT * FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x
                            b
                            a
                            b
                            a
                            """);
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH c AS (SELECT x FROM t WHERE x IN (SELECT x FROM c)) SELECT * FROM c UNION ALL SELECT * FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x
                            a
                            a
                            """);
        });
    }

    @Test
    public void testShadowingCteReadTwiceKeepsBindings() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b')");
            // Every reference to a CTE after the first parses its body again, against the CTEs
            // visible at its definition. A CTE defined before a shadowing CTE of the same WITH reads
            // the outer binding on every parse, and one defined after it reads the inner binding.
            final String outerTwiceThenInner = """
                    x
                    a
                    a
                    b
                    """;
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH d AS (SELECT x FROM c), c AS (SELECT 'b'::SYMBOL x) SELECT * FROM d UNION ALL SELECT * FROM d UNION ALL SELECT * FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(outerTwiceThenInner);
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH d AS (SELECT x FROM t WHERE x IN (SELECT x FROM c)), c AS (SELECT 'b'::SYMBOL x) SELECT * FROM d UNION ALL SELECT * FROM d UNION ALL SELECT * FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(outerTwiceThenInner);
            assertQuery("WITH c AS (SELECT 'a'::SYMBOL x) SELECT * FROM (WITH c AS (SELECT 'b'::SYMBOL x), d AS (SELECT x FROM c) SELECT * FROM d UNION ALL SELECT * FROM d)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x
                            b
                            b
                            """);
        });
    }

    @Test
    public void testWithAliasOverridingTable1() throws Exception {
        assertMemoryLeak(() -> assertQuery("WITH balance as ( SELECT * FROM balance WHERE address = 1 ) " +
                "SELECT * FROM balance ")
                .ddl("""
                        CREATE TABLE balance (
                          address LONG,
                          balance DOUBLE
                        );""")
                .mutateWith("insert into balance values ( 1, 1.0 ), (2, 2.0);")
                .returns("address\tbalance\n", "address\tbalance\n1\t1.0\n"));
    }

    @Test
    public void testWithAliasOverridingTable2() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                WITH balance as ( SELECT * FROM balance WHERE address = 1 ),\s
                     balance_other as ( SELECT * FROM balance )
                SELECT * FROM balance\s""")
                .ddl("""
                        CREATE TABLE balance (
                          address LONG,
                          balance DOUBLE
                        );""")
                .mutateWith("insert into balance values ( 1, 1.0 ), (2, 2.0);")
                .returns("address\tbalance\n", "address\tbalance\n1\t1.0\n"));
    }

    @Test
    public void testWithAliasOverridingTable3() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                WITH balance as ( SELECT * FROM balance WHERE address = 1 )\s
                SELECT * FROM ( \
                WITH balance_other AS ( SELECT * FROM balance )
                SELECT * FROM balance \
                 ) ORDER BY 1\s""")
                .ddl("""
                        CREATE TABLE balance (
                          address LONG,
                          balance DOUBLE
                        );""")
                .mutateWith("insert into balance values ( 1, 1.0 ), (2, 2.0);")
                .returns("address\tbalance\n", "address\tbalance\n1\t1.0\n"));
    }

    @Test
    public void testWithAliasOverridingTable4() throws Exception {
        assertMemoryLeak(() -> {//to force 2nd balance with clause parsing
            assertQuery("WITH balance2 as ( SELECT * FROM balance WHERE address = 2 ) " +
                    "SELECT * FROM (" +
                    "(" +
                    "WITH balance as (select * from balance where address = 1) " +
                    "SELECT b1.*, b2.* " +
                    "FROM balance b1 " +
                    "JOIN balance2 b2 on b1.address = b2.address " +
                    "JOIN balance b3 on b1.address = b3.address " +//to force 2nd balance with clause parsing
                    ") UNION ALL  " +
                    "SELECT * " +
                    "FROM balance b1 " +
                    "JOIN balance2 b2 on b1.address = b2.address " +
                    ")")
                    .ddl("""
                            CREATE TABLE balance (
                              address LONG,
                              balance DOUBLE
                            );""")
                    .mutateWith("insert into balance values ( 1, 1.0 ), (2, 2.0);")
                    .noRandomAccess()
                    .returns("address\tbalance\taddress1\tbalance1\n", "address\tbalance\taddress1\tbalance1\n2\t2.0\t2\t2.0\n");
        });
    }

    @Test
    public void testWithLatestByFilterGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table contact_events2 as (
                      select cast(x as SYMBOL) _id,
                        rnd_symbol('c1', 'c2', 'c3', 'c4') contactid,\s
                        CAST(x as Timestamp) timestamp,\s
                        rnd_symbol('g1', 'g2', 'g3', 'g4') groupId\s
                    from long_sequence(500))\s
                    timestamp(timestamp)""");

            // this is deliberately shuffled column in select to check that correct metadata is used on filtering
            // latest by queries
            TestUtils.printSql(
                    engine,
                    sqlExecutionContext,
                    "select groupId, _id, contactid, timestamp, _id from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp",
                    sink
            );
            String expected = sink.toString();
            Assert.assertTrue(expected.length() > 100);

            assertQuery("""
                    with eventlist as (
                        select * from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp
                    )
                    select groupId, _id, contactid, timestamp, _id from eventlist where groupId = 'g1'\s
                    """)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .sizeMayVary()
                    .returns(expected);
        });
    }

    @Test
    public void testWithSelectTwoWheres() throws Exception {
        assertQuery("with example as (select * from long_sequence(1))\n" +
                "select * from example where true where false;")
                .fails(82, "unexpected token [where]");
    }

    // Asserts that the parse budget refuses the statement at the position. The error reports how
    // much of the spent part the parse had taken, a count that moves with any change to how the
    // parser allocates, so the assertion pins the spent part and its maximum, which follows from
    // the length of the text, but not the count.
    private static void assertTooComplexToParse(CharSequence sql, int position, String spentPart, long max) throws Exception {
        try {
            assertExceptionNoLeakCheck(sql);
        } catch (SqlException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "statement is too complex to parse [" + spentPart + '=');
            TestUtils.assertContains(e.getFlyweightMessage(), ", max=" + max + ']');
        }
    }

    // DECLARE @s := '<800 characters>' WITH c0 AS(SELECT @s,@s,...,@s FROM long_sequence(1)), with
    // 30 reads of @s, then the fan-out of fanOutCteChain(); 1,229 characters at seven levels
    private static String declaredLiteralCteChain(int levels) {
        final StringBuilder c0 = new StringBuilder("DECLARE @s := '").append("x".repeat(800)).append("' WITH c0 AS(SELECT @s");
        for (int i = 1; i < 30; i++) {
            c0.append(",@s");
        }
        return fanOutCteChain(c0.append(" FROM long_sequence(1))"), levels);
    }

    // A doubling chain over c0 = SELECT 1+1+...+1 a FROM long_sequence(1), a sum of 250 terms
    private static String denseCteChain(int levels) {
        final StringBuilder c0 = new StringBuilder("SELECT 1");
        for (int i = 1; i < 250; i++) {
            c0.append("+1");
        }
        return doublingChainOver(c0.append(" a FROM long_sequence(1)").toString(), levels, "count(), sum(a)");
    }

    // WITH c0 AS (<c0>), c1 AS (SELECT * FROM c0 UNION ALL SELECT * FROM c0), ...
    // SELECT <projection> FROM c<levels>
    private static String doublingChainOver(String c0, int levels, String projection) {
        final StringBuilder sql = new StringBuilder("WITH c0 AS (").append(c0).append(')');
        for (int i = 1; i <= levels; i++) {
            sql.append(", c").append(i)
                    .append(" AS (SELECT * FROM c").append(i - 1)
                    .append(" UNION ALL SELECT * FROM c").append(i - 1).append(')');
        }
        return sql.append(" SELECT ").append(projection).append(" FROM c").append(levels).toString();
    }

    // WITH q0 AS (SELECT x l FROM long_sequence(3)), q1 AS (SELECT * FROM q0 UNION ALL SELECT *
    // FROM q0), ... SELECT count(), sum(l) FROM q<levels>
    private static String doublingCteChain(int levels) {
        final StringBuilder sql = new StringBuilder("WITH q0 AS (SELECT x l FROM long_sequence(3))");
        for (int i = 1; i <= levels; i++) {
            sql.append(", q").append(i)
                    .append(" AS (SELECT * FROM q").append(i - 1)
                    .append(" UNION ALL SELECT * FROM q").append(i - 1).append(')');
        }
        return sql.append(" SELECT count(), sum(l) FROM q").append(levels).toString();
    }

    // 825 characters: WITH c0 AS (SELECT 1+1+...+1 a FROM long_sequence(1)),
    // c1 AS(SELECT*FROM c0,c0 a,c0 b,c0 d), ..., c7 AS(...) SELECT count() FROM c7
    private static String fanOutCteChain() {
        final StringBuilder c0 = new StringBuilder("WITH c0 AS (SELECT 1");
        for (int i = 1; i < 250; i++) {
            c0.append("+1");
        }
        return fanOutCteChain(c0.append(" a FROM long_sequence(1))"), 7);
    }

    // <c0>,c1 AS(SELECT*FROM c0,c0 a,c0 b,c0 d), ..., SELECT count() FROM c<levels>, where <c0>
    // opens the WITH and defines c0
    private static String fanOutCteChain(StringBuilder c0, int levels) {
        for (int i = 1; i <= levels; i++) {
            final String previous = "c" + (i - 1);
            c0.append(",c").append(i).append(" AS(SELECT*FROM ").append(previous)
                    .append(',').append(previous).append(" a,")
                    .append(previous).append(" b,")
                    .append(previous).append(" d)");
        }
        return c0.append(" SELECT count() FROM c").append(levels).toString();
    }

    // WITH c AS (SELECT x::STRING s FROM long_sequence(3)) SELECT count() FROM k
    // WHERE s IN (SELECT s FROM c) AND ..., <reads> times
    private static String readsOfCte(int reads) {
        final StringBuilder sql = new StringBuilder("WITH c AS (SELECT x::STRING s FROM long_sequence(3)) SELECT count() FROM k WHERE s IN (SELECT s FROM c)");
        for (int i = 1; i < reads; i++) {
            sql.append(" AND s IN (SELECT s FROM c)");
        }
        return sql.toString();
    }

    // SELECT * FROM <name> UNION ALL SELECT * FROM <name> ..., <reads> times
    private static String unionOfReads(String name, int reads) {
        final StringBuilder sql = new StringBuilder();
        for (int i = 0; i < reads; i++) {
            if (i > 0) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM ").append(name);
        }
        return sql.toString();
    }

    // A doubling chain over c0 = SELECT x,x,...,x FROM long_sequence(1), with 300 columns
    private static String wideCteChain(int levels) {
        final StringBuilder c0 = new StringBuilder("SELECT x");
        for (int i = 1; i < 300; i++) {
            c0.append(",x");
        }
        return doublingChainOver(c0.append(" FROM long_sequence(1)").toString(), levels, "count()");
    }
}
