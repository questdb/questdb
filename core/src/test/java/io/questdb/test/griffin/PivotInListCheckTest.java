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
 *  distributed under the License is distributed active an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A PIVOT whose FOR values come from a subquery compiles to a plan that a query cache may keep:
 * every later execution of the plan runs the subquery again and refuses with
 * {@link TableReferenceOutOfDateException}, the cache's signal to recompile, when the values are
 * not the ones the plan was compiled with. New data under the same values keeps the plan.
 */
public class PivotInListCheckTest extends AbstractCairoTest {
    private static final String PIVOT = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats WHERE active ORDER BY c) GROUP BY grp) ORDER BY grp";

    @Test
    public void testCompileFailureFreesTheSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the subquery has run when the unknown column fails the compile: its plan must not leak
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("SELECT * FROM data PIVOT (sum(nope) FOR cat IN (SELECT c FROM cats WHERE active) GROUP BY grp)", sqlExecutionContext);
                Assert.fail("the compile must fail");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "nope");
            }
        });
    }

    @Test
    public void testFixedValueListPlanUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT * FROM data PIVOT (sum(val) FOR cat IN ('A', 'B') GROUP BY grp)", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    Assert.assertTrue(cq.isCacheable());
                    Assert.assertFalse(factory instanceof io.questdb.griffin.engine.PivotInListCheckFactory);
                }
            }
        });
    }

    @Test
    public void testPlanIsCacheable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(PIVOT, sqlExecutionContext);
                try (RecordCursorFactory ignore = cq.getRecordCursorFactory()) {
                    Assert.assertTrue(cq.isCacheable());
                }
            }
            // the check is not in the plan: it reads as the same PIVOT over the values as a list
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            printSql("EXPLAIN " + PIVOT.replace("(SELECT c FROM cats WHERE active ORDER BY c)", "('A', 'B')"), expected);
            printSql("EXPLAIN " + PIVOT, actual);
            TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testReusedPlanReadsNewDataUnderSameValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("INSERT INTO data VALUES ('g2', 'B', 5), ('g3', 'A', 7), ('g3', 'C', 1)");
                // C is in the data but not among the values: the plan stays
                for (int i = 0; i < 3; i++) {
                    assertCursor(factory, """
                            grp\tA\tB
                            g1\t10\t20
                            g2\t30\t5
                            g3\t7\tnull
                            """);
                }
            }
        });
    }

    @Test
    public void testReusedPlanRefusesChangedValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("UPDATE cats SET c = 'C' WHERE c = 'B'");
                assertStale(factory, "[value=C]");
            }
            assertFresh("""
                    grp\tA\tC
                    g1\t10\tnull
                    g2\t30\tnull
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesExtraValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("INSERT INTO cats VALUES ('C', true, 3)");
                execute("INSERT INTO data VALUES ('g1', 'C', 3)");
                assertStale(factory, "[value=C]");
                // and keeps refusing: it never adopts the new values
                assertStale(factory, "[value=C]");
            }
            assertFresh("""
                    grp\tA\tB\tC
                    g1\t10\t20\t3
                    g2\t30\tnull\tnull
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesMissingValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("UPDATE cats SET active = false WHERE c = 'B'");
                assertStale(factory, "[value=B]");
            }
            assertFresh("""
                    grp\tA
                    g1\t10
                    g2\t30
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesReorderedValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String pivot = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats WHERE active ORDER BY rank) GROUP BY grp) ORDER BY grp";
            try (RecordCursorFactory factory = select(pivot)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                // the same values in another order name the columns in another order
                execute("UPDATE cats SET rank = 3 WHERE c = 'A'");
                assertStale(factory, "[value=B]");
            }
            assertFresh("""
                    grp\tB\tA
                    g1\t20\t10
                    g2\tnull\t30
                    """, pivot);
        });
    }

    private static void assertCursor(RecordCursorFactory factory, String expected) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, sink);
        }
        TestUtils.assertEquals(expected, sink);
    }

    // a fresh compile, as a cache's recompile does
    private static void assertFresh(String expected, String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            assertCursor(factory, expected);
        }
    }

    private static void assertStale(RecordCursorFactory factory, String fragment) throws SqlException {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("the reused plan must refuse");
        } catch (TableReferenceOutOfDateException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "PIVOT IN subquery returns other values");
            TestUtils.assertContains(e.getFlyweightMessage(), fragment);
        }
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val LONG)");
        execute("CREATE TABLE cats (c SYMBOL, active BOOLEAN, rank INT)");
        execute("INSERT INTO cats VALUES ('A', true, 1), ('B', true, 2), ('X', false, 0)");
        execute("INSERT INTO data VALUES ('g1', 'A', 10), ('g1', 'B', 20), ('g2', 'A', 30)");
    }
}
