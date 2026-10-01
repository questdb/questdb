/*******************************************************************************
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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Ownership of the cursor factories {@code SqlOptimiser#parseFunctionAndEnumerateColumns} instantiates for
 * FROM/JOIN table functions, on the compile paths that reject a statement AFTER {@code optimise()} returned
 * and BEFORE code generation has taken those factories over.
 * <p>
 * {@code CreateMatViewTest} covers the materialized-view path; this covers the general one in
 * {@code SqlCompilerImpl#compileExecutionModel}, which rejects INSERT and UPDATE models without ever
 * generating. Both close counts are asserted exactly, in both directions: a miss leaks the factory and a
 * second close is a use-after-free.
 * <p>
 * The optimiser also instantiates the factory of every sub-query it optimises, and code generation takes
 * over only the ones the plan reads. The {@code testSubQueryNeverGenerated*} tests cover the factories
 * generation never reaches, on the compile paths that return and on the ones that throw. The
 * {@code testSubQueryNeverGeneratedCloseFailure*} tests cover a close that throws on those paths: the
 * failure has to reach the statement's error, and the sweep has to carry on to the factories it has
 * not closed yet.
 * <p>
 * The {@code testPivotInSubQuery*} tests cover the sub-query of a {@code PIVOT ... FOR ... IN (SELECT ...)}:
 * the optimiser opens its table function and borrows a second compiler to generate and run it.
 */
public class TableFunctionOwnershipTest extends AbstractCairoTest {
    private static final String FAILING_FUNCTION_NAME = "failing_cursor";
    private static final String FUNCTION_NAME = "owned_cursor";
    private static final String SECOND_FAILING_FUNCTION_NAME = "second_failing_cursor";
    // owned_cursor() returns no rows, so each sub-query is 0 and no x equals it. The outer queries
    // below select a, or a and c: the optimiser prunes the columns they leave out, and code
    // generation never generates the sub-query of a pruned column.
    private static final String SUB_QUERIES = "(SELECT x a, x = (SELECT count() FROM " + FUNCTION_NAME + "()) b, x = (SELECT count() FROM " + FUNCTION_NAME + "()) c FROM long_sequence(2))";
    // The outer queries over this one select a, c and d and leave b out, so code generation never
    // generates the sub-query over failing_cursor(), whose factories throw from close(). The
    // generated tree owns the other two factories: owned_cursor() counts the tree's close, and
    // read_parquet() holds native memory until the tree closes it.
    private static final String SUB_QUERIES_WITH_CLOSE_FAILURE = "(SELECT x a, x = (SELECT count() FROM " + FAILING_FUNCTION_NAME + "()) b, x = (SELECT count() FROM " + FUNCTION_NAME + "()) c, x = (SELECT k FROM read_parquet('p.parquet') LIMIT 1) d FROM long_sequence(2))";
    // The outer queries over this one select a and leave the other four columns out, so code
    // generation takes over none of their factories and one sweep closes all four. The optimiser
    // opens them in the order of the columns (optimiseExpressionModels() walks the sub-queries in
    // the order the parser met them) and the sweep closes them in the order the optimiser opened
    // them, so the two closes that throw, each a failure of its own, come first. Behind them,
    // owned_cursor() counts its close, and read_parquet() holds native memory until something
    // closes it.
    private static final String SUB_QUERIES_WITH_TWO_CLOSE_FAILURES = "(SELECT x a, x = (SELECT count() FROM " + FAILING_FUNCTION_NAME + "()) b, x = (SELECT count() FROM " + SECOND_FAILING_FUNCTION_NAME + "()) c, x = (SELECT count() FROM " + FUNCTION_NAME + "()) d, x = (SELECT k FROM read_parquet('p.parquet') LIMIT 1) e FROM long_sequence(2))";

    @Test
    public void testInsertAsSelectRejectedAfterOptimiseClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dest (a varchar, b varchar)");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            final String functionName = "owned_cursor";
            TableFunctionTestUtils.register(engine, functionName, SqlExecutionRequirements.NONE, factories);
            try {
                // validateAndOptimiseInsertAsSelect() optimises FIRST and only then compares the column
                // count, so the rejection lands in exactly the window this test is about: the optimiser
                // has instantiated and is holding the table function, and generation - which would have
                // taken it over - never runs.
                assertExceptionNoLeakCheck(
                        "insert into dest (a, b) select * from " + functionName + "()",
                        12,
                        "column count mismatch"
                );

                assertEquals(1, factories.size());
                assertEquals(1, factories.getQuick(0).getCloseCount());

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close the factory a second time.
                execute("create table other (x long)");
                assertEquals(1, factories.getQuick(0).getCloseCount());
            } finally {
                TableFunctionTestUtils.unregister(engine, functionName);
            }
        });
    }

    @Test
    public void testInsertAsSelectSuccessLeavesTableFunctionFactoryToTheGeneratedTree() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dest (a varchar)");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            final String functionName = "owned_cursor";
            TableFunctionTestUtils.register(engine, functionName, SqlExecutionRequirements.NONE, factories);
            try {
                // The counterpart of the rejection above: when the statement is accepted, generation owns
                // every instantiated factory and the compiled tree - nothing else - closes it, once. Without
                // this direction the test above would still pass if the compile path closed the factory
                // unconditionally rather than only on the throw.
                execute("insert into dest (a) select * from " + functionName + "()");

                assertTrue(factories.size() > 0);
                for (int i = 0, n = factories.size(); i < n; i++) {
                    assertEquals(1, factories.getQuick(i).getCloseCount());
                }

                execute("create table other (x long)");
                for (int i = 0, n = factories.size(); i < n; i++) {
                    assertEquals(1, factories.getQuick(i).getCloseCount());
                }
            } finally {
                TableFunctionTestUtils.unregister(engine, functionName);
            }
        });
    }

    @Test
    public void testPivotInSubQueryOverTableFunction() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            try {
                TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
                // The IN values name the columns a PIVOT returns, so the optimiser runs the IN
                // sub-query while it optimises the statement. It opens the sub-query's table
                // function itself and borrows a second compiler to generate the sub-query, whose
                // plan takes the factory over. The optimiser closes that plan once it read the
                // values, and its own sweep has to leave the factory alone afterwards.
                //
                // The loop repeats each statement: a compile borrows a pooled compiler, and a
                // reference the previous compile left in it must not close or leak a factory.
                for (int i = 0; i < 3; i++) {
                    // the IN sub-query reads the file
                    assertQuery("""
                            SELECT * FROM src
                            PIVOT (
                                sum(v)
                                FOR k IN (SELECT DISTINCT k FROM read_parquet('p.parquet') ORDER BY k)
                                GROUP BY g
                            ) ORDER BY g
                            """)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    g\t1\t2\t3
                                    a\t10\t20\t30
                                    b\t40\t50\t60
                                    """);
                    // The pivoted source reads the file too: the statement's own plan takes
                    // that factory over.
                    assertQuery("""
                            SELECT * FROM read_parquet('p.parquet')
                            PIVOT (
                                sum(v)
                                FOR k IN (SELECT DISTINCT k FROM read_parquet('p.parquet') WHERE k > 1 ORDER BY k)
                                GROUP BY g
                            ) ORDER BY g
                            """)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    g\t2\t3
                                    a\t20\t30
                                    b\t50\t60
                                    """);

                    // owned_cursor() counts the closes of every factory it hands out, which the
                    // memory check cannot do: a factory ignores a second close. It returns no
                    // rows, so the second branch of the union supplies the IN value.
                    factories.clear();
                    assertQuery("""
                            SELECT * FROM src
                            PIVOT (
                                sum(v)
                                FOR g IN (SELECT permission FROM owned_cursor() UNION ALL SELECT 'a'::VARCHAR FROM long_sequence(1))
                                GROUP BY k
                            ) ORDER BY k
                            """)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    k\ta
                                    1\t10
                                    2\t20
                                    3\t30
                                    """);
                    assertEachClosedOnce(factories, 1);

                    // The pivoted source is owned_cursor(). It has no rows to aggregate, so
                    // each produced column is NULL.
                    factories.clear();
                    assertQuery("""
                            SELECT * FROM owned_cursor()
                            PIVOT (
                                count()
                                FOR permission IN (SELECT DISTINCT g FROM read_parquet('p.parquet') ORDER BY g)
                            )
                            """)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    a\tb
                                    null\tnull
                                    """);
                    assertEachClosedOnce(factories, 1);
                }

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close a factory a second time.
                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 1);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testPivotInSubQueryOverTableFunctionFailingCompile() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            // The optimiser rejects the statement after the borrowed compiler generated the IN
            // sub-query, so it still holds the table function of the pivoted source, which no
            // plan took over.
            final String emptyInList = """
                    SELECT * FROM read_parquet('p.parquet')
                    PIVOT (
                        sum(v)
                        FOR k IN (SELECT k FROM read_parquet('p.parquet') WHERE k > 3)
                        GROUP BY g
                    )
                    """;
            final String twoColumnInList = """
                    SELECT * FROM read_parquet('p.parquet')
                    PIVOT (
                        sum(v)
                        FOR k IN (SELECT k, v FROM read_parquet('p.parquet'))
                        GROUP BY g
                    )
                    """;
            for (int i = 0; i < 3; i++) {
                assertQuery(emptyInList)
                        .noLeakCheck()
                        .fails(emptyInList.indexOf("SELECT k"), "PIVOT IN subquery returned empty result set");
                assertQuery(twoColumnInList)
                        .noLeakCheck()
                        .fails(twoColumnInList.indexOf("SELECT k"), "PIVOT IN subquery must return exactly one column, got 2");
            }
            // a compile that succeeds after the failed ones reads the file
            assertQuery("""
                    SELECT * FROM read_parquet('p.parquet')
                    PIVOT (
                        sum(v)
                        FOR k IN (SELECT k FROM read_parquet('p.parquet') WHERE k > 2)
                        GROUP BY g
                    ) ORDER BY g
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\t3
                            a\t30
                            b\t60
                            """);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureDoesNotStopTheSweep() throws Exception {
        assertWithCloseFailures(fixture -> {
            // The outer query selects a alone: generation succeeds and the sweep that follows
            // finds all four factories without an owner. The first two closes throw, and the
            // sweep still closes the other two factories. The statement fails with the first
            // close failure, which carries the second one as suppressed.
            try (RecordCursorFactory ignored = select("SELECT a FROM " + SUB_QUERIES_WITH_TWO_CLOSE_FAILURES)) {
                fail("the close failure must fail the statement");
            } catch (RuntimeException e) {
                assertSame(fixture.closeFailure, e);
                assertSuppressedOnce(e, fixture.secondCloseFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.secondFailingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureDoesNotStopTheSweepWhenGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            // The outer query selects a and f, and generation fails on f: the sweep in the catch
            // block finds all four factories without an owner. The first two closes throw, and
            // the sweep still closes the other two factories. The statement reports why
            // generation failed, and both close failures travel with that error as suppressed.
            try (RecordCursorFactory ignored = select("SELECT a, sin(a, a) f FROM " + SUB_QUERIES_WITH_TWO_CLOSE_FAILURES)) {
                fail("generation must fail");
            } catch (SqlException e) {
                assertEquals(10, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "wrong number of arguments for function `sin`");
                assertSuppressedOnce(e, fixture.closeFailure);
                assertSuppressedOnce(e, fixture.secondCloseFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.secondFailingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureFailsTheStatement() throws Exception {
        assertWithCloseFailures(fixture -> {
            // Generation succeeds and takes over the factories c and d read. The sweep that
            // follows closes the factory of the pruned b, which throws. The statement fails
            // with that failure, and the compiler closes the tree it has just generated.
            try (RecordCursorFactory ignored = select("SELECT a, c, d FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE)) {
                fail("the close failure must fail the statement");
            } catch (RuntimeException e) {
                assertSame(fixture.closeFailure, e);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenAlterViewGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            execute("CREATE VIEW v_ok AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();

            // An ALTER VIEW borrows a second compiler to optimise and generate the new body, and
            // no catch block sweeps that compiler once generation has thrown. The sweep in the
            // catch block of generateSelectOneShot() is the only one that closes the factory of
            // the pruned b and attaches its failure to the generation error.
            try {
                execute("ALTER VIEW v_ok AS (SELECT a, c, d, sin(a, a) e FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE + ")");
                fail("generation must fail");
            } catch (SqlException e) {
                assertEquals(35, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "wrong number of arguments for function `sin`");
                assertSuppressedOnce(e, fixture.closeFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenCreateViewGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            // A CREATE VIEW compiles into an operation, and that operation optimises and
            // generates the view's body when it executes. The statement's own compile has
            // returned by then, so its catch block, which sweeps a second time when a plain
            // SELECT fails, does not run. The sweep in the catch block of
            // generateSelectOneShot() is the only one that closes the factory of the pruned b
            // and attaches its failure to the generation error.
            try {
                execute("CREATE VIEW v_bad AS (SELECT a, c, d, sin(a, a) e FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE + ")");
                fail("generation must fail");
            } catch (SqlException e) {
                assertEquals(38, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "wrong number of arguments for function `sin`");
                assertSuppressedOnce(e, fixture.closeFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            // Generation fails on e. The sweep closes the factory of the pruned b, which
            // throws, and the statement still reports why generation failed: the close
            // failure travels with that error as suppressed.
            try (RecordCursorFactory ignored = select("SELECT a, c, d, sin(a, a) e FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE)) {
                fail("generation must fail");
            } catch (SqlException e) {
                assertEquals(16, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "wrong number of arguments for function `sin`");
                assertSuppressedOnce(e, fixture.closeFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE upd AS (SELECT x, 0L v FROM long_sequence(2))");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
            try {
                // b and c are pruned: generation takes over neither factory
                assertQuery("SELECT a FROM " + SUB_QUERIES)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a
                                1
                                2
                                """);
                assertEachClosedOnce(factories, 2);

                // b is pruned and c is read: generation takes over one factory, which the compiled
                // tree closes, and leaves the other
                factories.clear();
                assertQuery("SELECT a, c FROM " + SUB_QUERIES)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a\tc
                                1\tfalse
                                2\tfalse
                                """);
                assertEachClosedOnce(factories, 2);

                // EXPLAIN generates the plan it prints
                factories.clear();
                assertQuery("SELECT a, c FROM " + SUB_QUERIES)
                        .noLeakCheck()
                        .assertsPlanContaining("long_sequence count: 2");
                assertEachClosedOnce(factories, 2);

                // an UPDATE generates the nested model that selects the rows
                factories.clear();
                execute("UPDATE upd SET v = s.a FROM " + SUB_QUERIES + " s WHERE upd.x = s.a");
                assertEachClosedOnce(factories, 2);
                assertQuery("SELECT * FROM upd")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x\tv
                                1\t1
                                2\t2
                                """);

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close a factory a second time.
                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 2);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testSubQueryNeverGeneratedClosesTableFunctionFactoryOnceWhenGenerationFails() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
            try {
                // Generation takes over the factory c reads, then fails on d. Its cleanup walks the
                // models it can reach from the statement's model, which the model of the pruned b
                // is not among.
                assertExceptionNoLeakCheck(
                        "SELECT a, c, sin(a, a) d FROM " + SUB_QUERIES,
                        13,
                        "wrong number of arguments for function `sin`"
                );
                assertEachClosedOnce(factories, 2);

                // a CREATE VIEW generates its body outside the statement's own compile
                factories.clear();
                assertExceptionNoLeakCheck(
                        "CREATE VIEW v_bad AS (SELECT a, c, sin(a, a) d FROM " + SUB_QUERIES + ")",
                        35,
                        "wrong number of arguments for function `sin`"
                );
                assertEachClosedOnce(factories, 2);

                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 2);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testSubQueryNeverGeneratedClosesTableFunctionFactoryOnceWhenRejectedAfterOptimise() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW v_plain AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
            try {
                // The compiler rejects an INSERT into a view after it optimised the SELECT and before
                // it generates it, so the optimiser still holds both factories.
                assertExceptionNoLeakCheck(
                        "INSERT INTO v_plain SELECT a FROM " + SUB_QUERIES,
                        12,
                        "cannot modify view [view=v_plain]"
                );
                assertEquals(2, factories.size());
                assertEachClosedOnce(factories, 2);

                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 2);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testSubQueryNeverGeneratedClosesTableFunctionFactoryOnceWhenRetried() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(2))");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
            // The first attempt generates against a table that changed after the optimiser read it,
            // so the compiler discards the attempt's models and compiles the statement again. The
            // outer query does not select b, so neither attempt generates its sub-query, and each
            // attempt instantiates a factory of its own.
            try (StaleTableCompiler compiler = new StaleTableCompiler("ALTER TABLE t ADD COLUMN y INT")) {
                assertQuery("SELECT a FROM (SELECT x a, x = (SELECT count() FROM " + FUNCTION_NAME + "()) b FROM t)")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a
                                1
                                2
                                """);
                assertEquals("the compile must retry once", 2, compiler.attemptCount);
                assertEquals(2, factories.size());
                assertEachClosedOnce(factories, 2);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    // Covers every factory the fixture's functions handed out. A test asserts how many it expects
    // of each function.
    private static void assertEachClosedOnce(CloseFailureFixture fixture) {
        assertEachClosedOnce(fixture.failingFactories, 0);
        assertEachClosedOnce(fixture.secondFailingFactories, 0);
        assertEachClosedOnce(fixture.factories, 0);
    }

    private static void assertEachClosedOnce(ObjList<CloseCountingRecordCursorFactory> factories, int minFactoryCount) {
        assertTrue("instantiated factories: " + factories.size(), factories.size() >= minFactoryCount);
        for (int i = 0, n = factories.size(); i < n; i++) {
            assertEquals("factory " + i + " of " + n, 1, factories.getQuick(i).getCloseCount());
        }
    }

    private static void assertSuppressedOnce(Throwable failure, Throwable expectedSuppressed) {
        int count = 0;
        final Throwable[] suppressed = failure.getSuppressed();
        for (int i = 0, n = suppressed.length; i < n; i++) {
            if (suppressed[i] == expectedSuppressed) {
                count++;
            }
        }
        assertEquals("suppressed failures: " + suppressed.length, 1, count);
    }

    // Runs a close-failure test under the leak check, with p.parquet written and the fixture's
    // three table functions registered. Once the test's statement has failed, a second statement
    // compiles: it borrows the same pooled compiler and clears its optimiser state, and a
    // reference the failed compile left behind there must not close a factory a second time.
    private static void assertWithCloseFailures(CloseFailureCode code) throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();

            final CloseFailureFixture fixture = new CloseFailureFixture();
            try {
                TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, fixture.factories);
                TableFunctionTestUtils.register(engine, FAILING_FUNCTION_NAME, SqlExecutionRequirements.NONE, fixture.failingFactories, fixture.closeFailure);
                TableFunctionTestUtils.register(engine, SECOND_FAILING_FUNCTION_NAME, SqlExecutionRequirements.NONE, fixture.secondFailingFactories, fixture.secondCloseFailure);

                code.run(fixture);

                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(fixture);
            } finally {
                TableFunctionTestUtils.unregister(engine, SECOND_FAILING_FUNCTION_NAME);
                TableFunctionTestUtils.unregister(engine, FAILING_FUNCTION_NAME);
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    // Writes src to p.parquet, the file the read_parquet() calls in this class read.
    private static void createParquetFile() throws Exception {
        execute("CREATE TABLE src (g VARCHAR, k LONG, v LONG)");
        execute("""
                INSERT INTO src VALUES
                    ('a', 1, 10),
                    ('a', 2, 20),
                    ('a', 3, 30),
                    ('b', 1, 40),
                    ('b', 2, 50),
                    ('b', 3, 60)
                """);
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("src")
        ) {
            path.of(root).concat("p.parquet");
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
        }
        inputRoot = root;
    }

    @FunctionalInterface
    private interface CloseFailureCode {
        void run(CloseFailureFixture fixture) throws Exception;
    }

    // What the close-failure tests register: owned_cursor(), whose factories count their closes,
    // and two functions whose factories also throw from close(), each a failure of its own. Each
    // list holds the factories its function handed out.
    private static class CloseFailureFixture {
        private final RuntimeException closeFailure = new RuntimeException("injected close failure");
        private final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
        private final ObjList<CloseCountingRecordCursorFactory> failingFactories = new ObjList<>();
        private final RuntimeException secondCloseFailure = new RuntimeException("second injected close failure");
        private final ObjList<CloseCountingRecordCursorFactory> secondFailingFactories = new ObjList<>();
    }

    // Changes a table between the optimiser's read of it and the first generation attempt.
    private static class StaleTableCompiler extends SqlCompilerImpl {
        private final String ddl;
        private int attemptCount;

        private StaleTableCompiler(String ddl) {
            super(AbstractCairoTest.engine);
            this.ddl = ddl;
        }

        @Override
        protected RecordCursorFactory generateSelectOneShot(IQueryModel model, SqlExecutionContext context, boolean isProgressLogger) throws SqlException {
            if (attemptCount++ == 0) {
                AbstractCairoTest.engine.execute(ddl, context);
            }
            return super.generateSelectOneShot(model, context, isProgressLogger);
        }
    }
}
