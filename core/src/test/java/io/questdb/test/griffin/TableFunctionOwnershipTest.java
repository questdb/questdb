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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Ownership of the cursor factories {@code SqlOptimiser#parseFunctionAndEnumerateColumns} instantiates for
 * FROM/JOIN table functions. The optimiser holds such a factory until code generation takes it over, so a
 * compile that rejects the statement in between, or never generates the model, has to close it. The tests
 * assert each close count exactly, in both directions: a missed close leaks the factory, and a second close
 * is a use-after-free.
 * <p>
 * The tests sweep these compile paths:
 * <ul>
 *     <li>The catch block of {@code SqlCompilerImpl#compileExecutionModel}. That method optimises INSERT and
 *     UPDATE models without generating them, and an INSERT ... SELECT whose column count does not match
 *     fails inside it ({@code testInsertAsSelectRejectedAfterOptimiseClosesTableFunctionFactoryOnce}, and
 *     the INSERT in {@code testPivotInSubQueryFailingCompileClosesTableFunctionFactoryOnce}).</li>
 *     <li>The catch block of {@code SqlCompilerImpl#compileUsingModel}, which walks the statement's models
 *     and then sweeps. The view modification check fails an INSERT into a view there, after the optimiser
 *     returned ({@code testInsertIntoView*} and the {@code *WhenRejectedAfterOptimise} tests).</li>
 *     <li>{@code SqlCompilerImpl#generateSelectOneShot}, which sweeps the factories the plan does not read,
 *     whether generation returns or throws. Every plan the tests generate passes through it: a SELECT, the
 *     SELECT of an INSERT, an EXPLAIN, the model of an UPDATE that selects its rows, each attempt of
 *     {@code generateSelectWithRetries()} when a stale table makes the compiler retry, and the body a
 *     CREATE VIEW, CREATE OR REPLACE VIEW, ALTER VIEW or CREATE MATERIALIZED VIEW operation generates when
 *     it executes ({@code testInsertAsSelectSuccessLeavesTableFunctionFactoryToTheGeneratedTree}, and the
 *     {@code testSubQueryNeverGenerated*} tests the previous item does not name).</li>
 *     <li>The plan of a {@code PIVOT ... FOR ... IN (SELECT ...)} sub-query. The optimiser opens the
 *     sub-query's table function, and {@code SqlOptimiser#preparePivotForSelectSubquery} borrows a second
 *     compiler to generate the sub-query through {@code generateSelectWithoutRetries()}, runs it, and
 *     closes the plan. The catch block of {@code SqlOptimiser#optimise} sweeps when the statement fails
 *     afterwards ({@code testPivotInSubQuery*}).</li>
 *     <li>{@code SqlCodeGenerator#generateFunctionQuery}, which takes over a factory that projects its
 *     columns, as {@code read_parquet()} does, then builds the projected metadata, and closes the factory
 *     when that throws ({@code *ProjectionFailure*}).</li>
 * </ul>
 * {@code CreateMatViewTest} covers the rejection of a materialized view's query in
 * {@code SqlCompilerImpl#compileMatViewQuery}.
 * <p>
 * The optimiser also instantiates the factory of every sub-query it optimises, and code generation takes
 * over only the ones the plan reads. The {@code testSubQueryNeverGenerated*} tests cover the factories
 * generation never reaches, on the compile paths that return and on the ones that throw. The
 * {@code testSubQueryNeverGeneratedCloseFailure*} tests cover a close that throws on those paths: the
 * failure has to reach the statement's error, and the sweep has to carry on to the factories it has
 * not closed yet.
 */
public class TableFunctionOwnershipTest extends AbstractCairoTest {
    private static final String FAILING_FUNCTION_NAME = "failing_cursor";
    private static final String FUNCTION_NAME = "owned_cursor";
    // The IN sub-query of a PIVOT over owned_cursor(). owned_cursor() returns no rows, so the
    // second branch of the union supplies the one IN value.
    private static final String PIVOT_IN_ONE_VALUE = "SELECT permission FROM %s() UNION ALL SELECT 'a'::VARCHAR FROM long_sequence(1)"
            .formatted(FUNCTION_NAME);
    private static final String PROJECTED_FUNCTION_NAME = "projected_cursor";
    private static final String RETYPED_FUNCTION_NAME = "retyped_cursor";
    private static final String SECOND_FAILING_FUNCTION_NAME = "second_failing_cursor";
    // owned_cursor() returns no rows, so each sub-query is 0 and no x equals it. The outer queries
    // below select a, or a and c: the optimiser prunes the columns they leave out, and code
    // generation never generates the sub-query of a pruned column.
    private static final String SUB_QUERIES = """
            (
                SELECT
                    x a,
                    x = (SELECT count() FROM %s()) b,
                    x = (SELECT count() FROM %s()) c
                FROM long_sequence(2)
            )""".formatted(FUNCTION_NAME, FUNCTION_NAME);
    // The outer queries over this one select a, c and d and leave b out, so code generation never
    // generates the sub-query over failing_cursor(), whose factories throw from close(). The
    // generated tree owns the other two factories: owned_cursor() counts the tree's close, and
    // read_parquet() holds native memory until the tree closes it.
    private static final String SUB_QUERIES_WITH_CLOSE_FAILURE = """
            (
                SELECT
                    x a,
                    x = (SELECT count() FROM %s()) b,
                    x = (SELECT count() FROM %s()) c,
                    x = (SELECT k FROM read_parquet('p.parquet') LIMIT 1) d
                FROM long_sequence(2)
            )""".formatted(FAILING_FUNCTION_NAME, FUNCTION_NAME);
    // The outer queries over this one select a and leave the other four columns out, so code
    // generation takes over none of their factories and one sweep closes all four. The optimiser
    // opens them in the order of the columns (optimiseExpressionModels() walks the sub-queries in
    // the order the parser met them) and the sweep closes them in the order the optimiser opened
    // them, so the two closes that throw, each a failure of its own, come first. Behind them,
    // owned_cursor() counts its close, and read_parquet() holds native memory until something
    // closes it.
    private static final String SUB_QUERIES_WITH_TWO_CLOSE_FAILURES = """
            (
                SELECT
                    x a,
                    x = (SELECT count() FROM %s()) b,
                    x = (SELECT count() FROM %s()) c,
                    x = (SELECT count() FROM %s()) d,
                    x = (SELECT k FROM read_parquet('p.parquet') LIMIT 1) e
                FROM long_sequence(2)
            )""".formatted(FAILING_FUNCTION_NAME, SECOND_FAILING_FUNCTION_NAME, FUNCTION_NAME);
    // While set, every compiler the engine hands out refuses each plan it generates, see
    // setUpStatic().
    private static boolean isPlanRefused;

    /**
     * Installs a compiler that refuses plans as Enterprise does: Enterprise generates a plan,
     * then refuses it when it cannot write the plan's audit, freeing the plan and throwing. Every
     * compiler the engine pools is one, including the one PIVOT borrows to run its
     * {@code FOR ... IN (SELECT ...)} sub-query. It stays inert until a test sets
     * {@link #isPlanRefused}.
     */
    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return PlanRefusingCompiler::new;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

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
    public void testInsertIntoViewRejectedAfterOptimiseClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW v_plain AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
            try {
                // The compiler rejects an INSERT into a view after it optimised the SELECT and before
                // it generates it. The SELECT reads owned_cursor() in its FROM clause, so the catch
                // block's walk of the statement's models closes that factory and detaches it from
                // its model. The sweep that follows must leave it alone: closing everything the
                // optimiser opened would close it a second time.
                assertExceptionNoLeakCheck(
                        "INSERT INTO v_plain SELECT * FROM " + FUNCTION_NAME + "()",
                        12,
                        "cannot modify view [view=v_plain]"
                );
                assertEquals(1, factories.size());
                assertEachClosedOnce(factories, 1);

                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 1);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testPivotInSubQueryCloseFailureIsSuppressedOnceWhenCompileFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            // The plan of the IN sub-query takes failing_cursor() over, and the optimiser's close
            // of that plan throws. The statement fails and carries that failure as suppressed.
            // The cleanup of the failed compile must leave the factory alone: a second close
            // would throw again and attach the same failure twice.
            final String sql = "SELECT * FROM src PIVOT (sum(v) FOR g IN (SELECT permission, permission p FROM failing_cursor()) GROUP BY k)";
            try (RecordCursorFactory ignored = select(sql)) {
                fail("the IN sub-query must be rejected");
            } catch (SqlException e) {
                assertEquals(sql.indexOf("SELECT permission"), e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "PIVOT IN subquery must return exactly one column, got 2");
                assertSuppressedOnce(e, fixture.closeFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testPivotInSubQueryFailingCompileClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            execute("CREATE TABLE dest (a LONG)");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            try {
                TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
                // The optimiser opens the table function of the IN sub-query, and the plan the
                // borrowed compiler generates for the sub-query takes it over. The optimiser
                // closes that plan once it has read the IN values. Each statement below then
                // fails, and the cleanup of the failed compile must close only the factories no
                // plan took over, or it closes this one a second time.
                final String oneColumn = """
                        SELECT * FROM src
                        PIVOT (
                            sum(v)
                            FOR g IN (SELECT permission, permission p FROM owned_cursor())
                            GROUP BY k
                        )
                        """;
                assertQuery(oneColumn)
                        .noLeakCheck()
                        .fails(oneColumn.indexOf("SELECT permission"), "PIVOT IN subquery must return exactly one column, got 2");
                assertEachClosedOnce(factories, 1);

                final String emptyInList = """
                        SELECT * FROM src
                        PIVOT (
                            sum(v)
                            FOR g IN (SELECT permission FROM owned_cursor())
                            GROUP BY k
                        )
                        """;
                assertQuery(emptyInList)
                        .noLeakCheck()
                        .fails(emptyInList.indexOf("SELECT permission"), "PIVOT IN subquery returned empty result set");
                assertEachClosedOnce(factories, 2);

                // The IN sub-query succeeds, and the optimiser rejects the statement later.
                assertQuery("SELECT nope FROM (SELECT * FROM src PIVOT (sum(v) FOR g IN (" + PIVOT_IN_ONE_VALUE + ") GROUP BY k))")
                        .noLeakCheck()
                        .fails(7, "Invalid column: nope");
                assertEachClosedOnce(factories, 3);

                // The generation of the IN sub-query fails, and the generator's cleanup closes
                // both factories of the join.
                final String failingGeneration = """
                        SELECT * FROM src
                        PIVOT (
                            sum(v)
                            FOR g IN (SELECT l.permission FROM owned_cursor() l ASOF JOIN owned_cursor() r)
                            GROUP BY k
                        )
                        """;
                assertQuery(failingGeneration)
                        .noLeakCheck()
                        .fails(failingGeneration.indexOf("ASOF"), "left side of time series join has no timestamp");
                assertEachClosedOnce(factories, 5);

                // The compiler rejects the INSERT after the optimiser returned.
                assertExceptionNoLeakCheck(
                        "INSERT INTO dest (a) SELECT * FROM src PIVOT (sum(v) FOR g IN (" + PIVOT_IN_ONE_VALUE + ") GROUP BY k)",
                        12,
                        "column count mismatch"
                );
                assertEachClosedOnce(factories, 6);

                // The IN sub-query returns more values than the limit allows.
                node1.setProperty(PropertyKey.CAIRO_SQL_PIVOT_MAX_PRODUCED_COLUMNS, 1);
                final String tooManyColumns = """
                        SELECT * FROM src
                        PIVOT (
                            sum(v)
                            FOR k IN (SELECT permission::LONG FROM owned_cursor() UNION ALL SELECT k FROM src)
                            GROUP BY g
                        )
                        """;
                assertQuery(tooManyColumns)
                        .noLeakCheck()
                        .fails(tooManyColumns.indexOf("SELECT permission"), "PIVOT produces too many columns: 2, limit is 1");
                assertEachClosedOnce(factories, 7);

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close a factory a second time.
                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 7);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
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
                // The list keeps every factory owned_cursor() has handed out, so the assertion
                // behind each statement also covers the factories of the statements before it.
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
                    // each earlier iteration left at least two factories in the list
                    assertEachClosedOnce(factories, 2 * i);

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
                    assertEachClosedOnce(factories, 2 * i);

                    // owned_cursor() counts the closes of every factory it hands out, which the
                    // memory check cannot do: a factory ignores a second close. It returns no
                    // rows, so the second branch of the union supplies the IN value.
                    int factoryCount = factories.size();
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
                    assertEachClosedOnce(factories, factoryCount + 1);

                    // The pivoted source is owned_cursor(). It has no rows to aggregate, so
                    // each produced column is NULL.
                    factoryCount = factories.size();
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
                    assertEachClosedOnce(factories, factoryCount + 1);
                }

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close a factory a second time.
                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 6);
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
    public void testPivotInSubQueryProjectionFailureClosesTableFunctionFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (g VARCHAR, k LONG, v LONG)");
            execute("INSERT INTO src VALUES ('a', 1, 10), ('b', 2, 20)");

            final ObjList<ProjectedCursorFactory> factories = new ObjList<>();
            try {
                registerProjectedCursor(PROJECTED_FUNCTION_NAME, false, factories);
                // The optimiser opens the table function of the IN sub-query, and the borrowed
                // compiler generates the sub-query. The optimiser calls the column col.zx zx, a
                // name the factory's metadata does not resolve, so the generation fails while it
                // projects the factory's columns. The factory holds native memory until it
                // closes, so the leak check fails if the failed statement leaves it open.
                final String sql = "SELECT * FROM src PIVOT (sum(v) FOR g IN (SELECT zx FROM " + PROJECTED_FUNCTION_NAME + "()) GROUP BY k)";
                for (int i = 0; i < 3; i++) {
                    assertExceptionNoLeakCheck(sql, 0, "Invalid column: zx");
                    assertEquals(i + 1, factories.size());
                }

                // The IN sub-query compiles when it reads a column the factory resolves. The
                // factory returns no rows, so the second branch of the union supplies the one IN
                // value.
                assertQuery("SELECT * FROM src PIVOT (sum(v) FOR g IN (SELECT ts::VARCHAR FROM " + PROJECTED_FUNCTION_NAME + "() UNION ALL SELECT 'a'::VARCHAR FROM long_sequence(1)) GROUP BY k)")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\ta
                                1\t10
                                """);
                assertEquals(4, factories.size());
            } finally {
                TableFunctionTestUtils.unregister(engine, PROJECTED_FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testPivotInSubQueryRefusedPlanClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            try {
                TableFunctionTestUtils.register(engine, FUNCTION_NAME, SqlExecutionRequirements.NONE, factories);
                // The borrowed compiler generates the plan of the IN sub-query, which takes the
                // factory over, then refuses the plan and frees it. The optimiser fails with the
                // refusal, and its cleanup must not close the factory a second time.
                final String sql = "SELECT * FROM src PIVOT (sum(v) FOR g IN (" + PIVOT_IN_ONE_VALUE + ") GROUP BY k)";
                isPlanRefused = true;
                try {
                    assertExceptionNoLeakCheck(sql, 0, "refused plan");
                } finally {
                    isPlanRefused = false;
                }
                assertEachClosedOnce(factories, 1);

                // the same statement compiles once nothing refuses its plans
                final int factoryCount = factories.size();
                assertQuery(sql + " ORDER BY k")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\ta
                                1\t10
                                2\t20
                                3\t30
                                """);
                assertEachClosedOnce(factories, factoryCount + 1);

                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, factoryCount + 1);
            } finally {
                TableFunctionTestUtils.unregister(engine, FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testProjectionFailureClosesReinstantiatedTableFunctionFactory() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<ProjectedCursorFactory> factories = new ObjList<>();
            try {
                registerProjectedCursor(RETYPED_FUNCTION_NAME, true, factories);
                // The optimiser reads max(ts) as the timestamp of the last row. The model it
                // builds for that read names the table function, but does not take over the
                // factory the optimiser opened, so generation opens the function a second time.
                // The second factory types ts as LONG, as a file rewritten in between would, and
                // generation fails while it projects that factory's columns, before any plan
                // takes the factory over.
                final String sql = "SELECT max(ts) FROM " + RETYPED_FUNCTION_NAME + "() TIMESTAMP(ts)";
                assertExceptionNoLeakCheck(sql, sql.lastIndexOf("ts)"), "not a TIMESTAMP");
                assertEquals(2, factories.size());
            } finally {
                TableFunctionTestUtils.unregister(engine, RETYPED_FUNCTION_NAME);
            }
        });
    }

    @Test
    public void testProjectionFailureClosesTableFunctionFactory() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<ProjectedCursorFactory> factories = new ObjList<>();
            try {
                registerProjectedCursor(PROJECTED_FUNCTION_NAME, false, factories);
                // The optimiser calls the column col.zx zx, a name the factory's metadata does
                // not resolve, so generation fails while it projects the factory's columns.
                // Generation has taken the factory over from its model by then, and nothing
                // else would close it.
                for (int i = 0; i < 3; i++) {
                    assertExceptionNoLeakCheck("SELECT * FROM " + PROJECTED_FUNCTION_NAME + "()", 0, "Invalid column: zx");
                    assertEquals(i + 1, factories.size());
                }

                // The plan takes the factory over when the statement reads a column the factory
                // resolves. The factory returns no rows, so its cursor never checks the circuit
                // breaker.
                assertQuery("SELECT ts FROM " + PROJECTED_FUNCTION_NAME + "()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .noCircuitBreakerCheck()
                        .expectSize()
                        .returns("ts\n");
            } finally {
                TableFunctionTestUtils.unregister(engine, PROJECTED_FUNCTION_NAME);
            }
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
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenCreateMatViewGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // A materialized view rejects a sub-query its plan reads, so the sub-query over
            // failing_cursor() is the value of a variable the body declares and never reads:
            // the optimiser opens its factory and generation never reaches it. Generation
            // fails on m, and the generator's own cleanup closes the factory of the joined
            // owned_cursor().
            //
            // A CREATE MATERIALIZED VIEW compiles into an operation, as a CREATE VIEW does, and
            // that operation optimises and generates the body when it executes. The statement's
            // own compile has returned by then, so its catch block, which sweeps a second time
            // when a plain SELECT fails, does not run. The sweep in the catch block of
            // generateSelectOneShot() is the only one that closes the factory of @unread and
            // attaches its failure to the generation error.
            final String sql = """
                    CREATE MATERIALIZED VIEW mv_bad AS (
                        DECLARE @unread := (SELECT count() FROM %s())
                        SELECT t.ts, max(sin(t.x, t.x)) m
                        FROM base t CROSS JOIN %s()
                        SAMPLE BY 1d
                    ) PARTITION BY DAY
                    """.formatted(FAILING_FUNCTION_NAME, FUNCTION_NAME);
            try {
                execute(sql);
                fail("generation must fail");
            } catch (SqlException e) {
                assertEquals(sql.indexOf("sin("), e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "wrong number of arguments for function `sin`");
                assertSuppressedOnce(e, fixture.closeFailure);
            }
            assertEquals(1, fixture.failingFactories.size());
            assertEquals(1, fixture.factories.size());
            assertEachClosedOnce(fixture);
        });
    }

    @Test
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenCreateOrReplaceViewGenerationFails() throws Exception {
        assertWithCloseFailures(fixture -> {
            execute("CREATE VIEW v_ok AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();

            // A CREATE OR REPLACE VIEW that names an existing view takes the route of an ALTER
            // VIEW: compileCreate() hands the new body to alterViewExecution(), which borrows a
            // second compiler to optimise and generate it. No catch block sweeps that compiler
            // once generation has thrown, so the sweep in the catch block of
            // generateSelectOneShot() is the only one that closes the factory of the pruned b
            // and attaches its failure to the generation error.
            try {
                execute("CREATE OR REPLACE VIEW v_ok AS (SELECT a, c, d, sin(a, a) e FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE + ")");
                fail("generation must fail");
            } catch (SqlException e) {
                // alterViewExecution() reports the position of the character before sin, as it
                // does for an ALTER VIEW
                assertEquals(47, e.getPosition());
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
    public void testSubQueryNeverGeneratedCloseFailureIsSuppressedWhenRejectedAfterOptimise() throws Exception {
        assertWithCloseFailures(fixture -> {
            execute("CREATE VIEW v_plain AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();

            // The compiler rejects an INSERT into a view after it optimised the SELECT and before
            // it generates it. The catch block's walk of the statement's models does not reach
            // the sub-queries in its columns, so the sweep that follows closes all three of their
            // factories, and the close of b's throws. The statement still reports why it was
            // rejected, and the close failure travels with that error as suppressed.
            try {
                execute("INSERT INTO v_plain SELECT a, c, d FROM " + SUB_QUERIES_WITH_CLOSE_FAILURE);
                fail("the insert must be rejected");
            } catch (SqlException e) {
                assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot modify view [view=v_plain]");
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

    // Registers a table function whose factories project their columns, as read_parquet() does,
    // and hold native memory until they close, so the leak check catches a factory nothing
    // closed. A factory names its columns ts and col.zx. The optimiser keeps no dot in a column
    // name and calls the second one zx, a name the factory's metadata does not resolve. When
    // isTimestampRetyped is set, every factory after the first types ts as LONG instead of
    // TIMESTAMP.
    private static void registerProjectedCursor(
            String functionName,
            boolean isTimestampRetyped,
            ObjList<ProjectedCursorFactory> factories
    ) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return functionName + "()";
            }

            @Override
            public boolean isCursor() {
                return true;
            }

            @Override
            public Function newInstance(
                    int position,
                    ObjList<Function> args,
                    IntList argPositions,
                    CairoConfiguration configuration,
                    SqlExecutionContext executionContext
            ) {
                final boolean isRetyped = isTimestampRetyped && factories.size() > 0;
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("ts", isRetyped ? ColumnType.LONG : ColumnType.TIMESTAMP));
                metadata.add(new TableColumnMetadata("col.zx", ColumnType.LONG));
                final ProjectedCursorFactory factory = new ProjectedCursorFactory(metadata);
                factories.add(factory);
                return new CursorFunction(factory);
            }
        }));
        assertNull(engine.getFunctionFactoryCache().getFactories().get(functionName));
        engine.getFunctionFactoryCache().getFactories().put(functionName, descriptors);
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

    // Refuses each plan it generates while isPlanRefused is set: it frees the plan and throws.
    private static class PlanRefusingCompiler extends SqlCompilerImpl {
        private PlanRefusingCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        protected RecordCursorFactory generateSelectOneShot(
                IQueryModel selectQueryModel,
                SqlExecutionContext executionContext,
                boolean generateProgressLogger
        ) throws SqlException {
            final RecordCursorFactory factory = super.generateSelectOneShot(selectQueryModel, executionContext, generateProgressLogger);
            if (isPlanRefused) {
                Misc.free(factory);
                throw SqlException.$(0, "refused plan");
            }
            return factory;
        }
    }

    // Holds native memory from construction until it closes, and returns no rows.
    private static class ProjectedCursorFactory extends ProjectableRecordCursorFactory {
        private static final long MEMORY_SIZE = 64;
        private long memory;

        private ProjectedCursorFactory(RecordMetadata metadata) {
            super(metadata);
            memory = Unsafe.malloc(MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return EmptyTableRecordCursor.INSTANCE;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        protected void _close() {
            memory = Unsafe.free(memory, MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
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
