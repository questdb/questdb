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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
 * generation never reaches, on the compile paths that return and on the ones that throw.
 */
public class TableFunctionOwnershipTest extends AbstractCairoTest {
    private static final String FUNCTION_NAME = "owned_cursor";
    // owned_cursor() returns no rows, so each sub-query is 0 and no x equals it. The outer queries
    // below select a, or a and c: the optimiser prunes the columns they leave out, and code
    // generation never generates the sub-query of a pruned column.
    private static final String SUB_QUERIES = "(SELECT x a, x = (SELECT count() FROM " + FUNCTION_NAME + "()) b, x = (SELECT count() FROM " + FUNCTION_NAME + "()) c FROM long_sequence(2))";

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

    private static void assertEachClosedOnce(ObjList<CloseCountingRecordCursorFactory> factories, int minFactoryCount) {
        assertTrue("instantiated factories: " + factories.size(), factories.size() >= minFactoryCount);
        for (int i = 0, n = factories.size(); i < n; i++) {
            assertEquals("factory " + i + " of " + n, 1, factories.getQuick(i).getCloseCount());
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
