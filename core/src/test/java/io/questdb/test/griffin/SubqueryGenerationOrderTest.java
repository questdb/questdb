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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestCloseCounterFunctionFactory;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Binding builds sub-query consumers over the plan's metadata, every query of the statement is optimised and
 * authorized before any factory is generated, and a sub-query generates where its consumer reads it: binding errors,
 * including a sub-query's static errors and a call that fails to resolve over a sub-query argument, precede
 * generation errors, which surface in generation order, and a sub-query the optimiser drops is never generated. The
 * per-worker clones of a parallel consumer take the sub-query's value from their owner and never generate it again.
 * PIVOT IN and table-function arguments generate their sub-query while the statement binds.
 */
public class SubqueryGenerationOrderTest extends AbstractCairoTest {
    private static final String GENERATION_ERROR_SUBQUERY = "(SELECT p.price FROM (SELECT * FROM prices ORDER BY ts DESC) p ASOF JOIN trades t)";
    private static final String STATIC_ERROR_SUBQUERY = "(SELECT sum(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym)"
            + " RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING)";
    private static final String UNFRAMED_TOUCH = "touch(SELECT * FROM trades LATEST ON ts PARTITION BY sym)";

    @Test
    public void testDroppedSubqueryIsNeverGenerated() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("INSERT INTO trades VALUES ('a', 1.5, '2024-01-01T00:00:00.000000Z')");
            assertQuery("SELECT price FROM trades WHERE true OR price = " + GENERATION_ERROR_SUBQUERY)
                    .expectSize()
                    .returns("""
                            price
                            1.5
                            """);
        });
    }

    @Test
    public void testDroppedSubqueryStaticErrorSurfacesAtBind() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE true OR price = " + STATIC_ERROR_SUBQUERY)
                    .fails(159, "WINDOW join hi value cannot be less than lo value");
        });
    }

    @Test
    public void testCallErrorPrecedesSubqueryArgumentError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE EXISTS " + GENERATION_ERROR_SUBQUERY)
                    .fails(31, "unknown function name: EXISTS(CURSOR)");
            assertQuery("SELECT price FROM trades WHERE price = abs(1, 2) OR price = " + GENERATION_ERROR_SUBQUERY)
                    .fails(39, "there is no matching function `abs` with the argument types: (INT, INT)");
            assertQuery("SELECT price FROM trades WHERE EXISTS (SELECT price FROM prices)")
                    .fails(31, "unknown function name: EXISTS(CURSOR)");
        });
    }

    @Test
    public void testGenerationFailureOfSecondSubqueryFreesFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE price = (SELECT price FROM prices LIMIT 1) AND price = " + GENERATION_ERROR_SUBQUERY)
                    .fails(149, "left side of time series join doesn't have ASC timestamp order");
        });
    }

    @Test
    public void testAuthorizationPrecedesSubqueryGeneration() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE secret (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1).with(new DenySecretSecurityContext());
                    SqlCompilerImpl compiler = new ScanAuthorizingCompiler(engine)
            ) {
                final String query = "SELECT price FROM trades WHERE price = (SELECT p.price FROM (SELECT * FROM secret ORDER BY ts DESC) p"
                        + " ASOF JOIN prices t)";
                try (RecordCursorFactory ignore = compiler.compile(query, executionContext).getRecordCursorFactory()) {
                    Assert.fail("authorization must reject the sub-query");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "permission denied [table=secret]");
                }
                try (RecordCursorFactory ignore = compiler.compile("SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY,
                        executionContext).getRecordCursorFactory()) {
                    Assert.fail("the sub-query must fail generation");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "left side of time series join doesn't have ASC timestamp order");
                }
                final String staticErrorQuery = "SELECT price FROM trades WHERE price = (SELECT sum(p.price) FROM secret t WINDOW JOIN prices p"
                        + " ON (t.sym = p.sym) RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING)";
                try (RecordCursorFactory ignore = compiler.compile(staticErrorQuery, executionContext).getRecordCursorFactory()) {
                    Assert.fail("binding must reject the sub-query");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "WINDOW join hi value cannot be less than lo value");
                }
            }
        });
    }

    @Test
    public void testBindErrorPrecedesSubqueryError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY + " AND missing = 1")
                    .fails(126, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = (SELECT price FROM prices WHERE price = " + GENERATION_ERROR_SUBQUERY + ") AND missing = 1")
                    .fails(167, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY + " AND price = (SELECT missing FROM prices)")
                    .fails(142, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY + " ORDER BY missing")
                    .fails(131, "Invalid column: missing");
        });
    }

    @Test
    public void testGenerationErrorsSurfaceInGenerationOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY + " AND price = " + GENERATION_ERROR_SUBQUERY)
                    .fails(102, "left side of time series join doesn't have ASC timestamp order");
        });
    }

    @Test
    public void testPivotInSubqueryGenerationFailureFreesResources() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM trades PIVOT (sum(price) FOR price IN " + GENERATION_ERROR_SUBQUERY + " GROUP BY sym)")
                    .fails(115, "left side of time series join doesn't have ASC timestamp order");
        });
    }

    @Test
    public void testPivotInSubqueryReadsNestedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("""
                    INSERT INTO trades VALUES
                        ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                        ('a', 2.0, '2024-01-01T00:00:01.000000Z'),
                        ('b', 2.0, '2024-01-01T00:00:02.000000Z')
                    """);
            assertQuery("""
                    SELECT * FROM trades
                    PIVOT (
                        count()
                        FOR price IN (SELECT DISTINCT price FROM trades WHERE price = (SELECT max(price) FROM trades))
                        GROUP BY sym
                    )
                    ORDER BY sym
                    """)
                    .expectSize()
                    .returns("""
                            sym	2.0
                            a	1
                            b	1
                            """);
        });
    }

    @Test
    public void testCreateViewAndInsertSelectReportSubqueryErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException("CREATE VIEW v1 AS (SELECT price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY + ")",
                    121, "left side of time series join doesn't have ASC timestamp order");
            assertException("INSERT INTO trades(sym, price) SELECT sym, price FROM trades WHERE price = " + GENERATION_ERROR_SUBQUERY,
                    138, "left side of time series join doesn't have ASC timestamp order");
            assertException("CREATE VIEW v1 AS (SELECT price FROM trades WHERE price = " + STATIC_ERROR_SUBQUERY + ")",
                    170, "WINDOW join hi value cannot be less than lo value");
            assertException("INSERT INTO trades(sym, price) SELECT sym, price FROM trades WHERE price = " + STATIC_ERROR_SUBQUERY,
                    187, "WINDOW join hi value cannot be less than lo value");
        });
    }

    @Test
    public void testParallelWorkerClonesDoNotGenerateSubquery() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tb2 (rts TIMESTAMP, v INT, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
            execute("CREATE TABLE tc2 (cts TIMESTAMP, v INT) TIMESTAMP(cts) PARTITION BY DAY");
            execute("CREATE TABLE tsym (s SYMBOL, ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tb2 VALUES ('2020-01-01T00:20', 5, 10), ('2020-01-01T01:20', 0, 20), ('2020-01-01T03:20', 100, 30)");
            execute("INSERT INTO tc2 VALUES ('2020-01-01T00:20', 5), ('2020-01-01T01:20', 0), ('2020-01-01T03:20', 100)");
            execute("INSERT INTO tsym VALUES ('10g', '2020-01-01T00:00', 10), ('20g', '2020-01-01T01:00', 20), ('30g', '2020-01-01T02:00', 30)");
            final String subquery = """
                    SELECT concat(m, test_close_counter('g'))::SYMBOL FROM (
                        SELECT v, max(c1) m FROM tb2
                        WHERE rts > (SELECT m FROM (SELECT v, min(cts) m FROM tc2 GROUP BY v) ORDER BY m LIMIT 1)
                        GROUP BY v
                    )""";
            final String keyQuery = "SELECT s IN (" + subquery + ") k, count() c, sum(v) sv FROM tsym GROUP BY k ORDER BY k";
            final String aggregateQuery = "SELECT count() c, sum(CASE WHEN s IN (" + subquery + ") THEN v ELSE 0 END) sv FROM tsym";
            final String filterQuery = "SELECT count() c, sum(v) sv FROM tsym WHERE v > 0 AND s IN (" + subquery + ")";
            for (int workerCount : new int[]{1, 4, 16}) {
                try (SqlExecutionContextImpl executionContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                    TestCloseCounterFunctionFactory.reset();
                    final long rosti = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
                    final int subqueryCreated;
                    final long subqueryRosti;
                    try (RecordCursorFactory ignore = engine.select(subquery, executionContext)) {
                        subqueryCreated = TestCloseCounterFunctionFactory.created("g");
                        subqueryRosti = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI) - rosti;
                    }
                    Assert.assertTrue(subqueryRosti > 0);
                    assertSubqueryGeneratedOnce(keyQuery, executionContext, subqueryCreated, subqueryRosti);
                    assertQuery(keyQuery)
                            .withContext(executionContext)
                            .expectSize()
                            .returns("""
                                    k\tc\tsv
                                    false\t1\t10
                                    true\t2\t50
                                    """);
                    assertSubqueryGeneratedOnce(aggregateQuery, executionContext, subqueryCreated, subqueryRosti);
                    assertQuery(aggregateQuery)
                            .withContext(executionContext)
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    c\tsv
                                    3\t50
                                    """);
                    assertSubqueryGeneratedOnce(filterQuery, executionContext, subqueryCreated, subqueryRosti);
                    assertQuery(filterQuery)
                            .withContext(executionContext)
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    c\tsv
                                    2\t50
                                    """);
                }
            }
        });
    }

    @Test
    public void testTableFunctionArgumentSubqueryGeneratesWithNestedSubquery() throws Exception {
        assertQuery("SELECT x FROM long_sequence((SELECT x FROM long_sequence(3) WHERE x = (SELECT 2)))")
                .fails(29, "constant expected");
    }

    @Test
    public void testTouchValidatesGeneratedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT x FROM (SELECT " + UNFRAMED_TOUCH + " x, price FROM trades)")
                    .fails(28, "query does not support framing execution and cannot be pre-touched");
            assertQuery("SELECT " + UNFRAMED_TOUCH + ", missing FROM trades")
                    .fails(66, "Invalid column: missing");
            assertQuery("SELECT price FROM (SELECT " + UNFRAMED_TOUCH + " x, price FROM trades)")
                    .returns("price\n");
        });
    }

    /**
     * Compiling the query creates the sub-query's counter and allocates its vectorized GROUP BY maps as often as
     * compiling the sub-query alone does.
     */
    private static void assertSubqueryGeneratedOnce(String query, SqlExecutionContext executionContext, int subqueryCreated, long subqueryRosti) throws SqlException {
        TestCloseCounterFunctionFactory.reset();
        final long rosti = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
        try (RecordCursorFactory ignore = engine.select(query, executionContext)) {
            Assert.assertEquals(subqueryCreated, TestCloseCounterFunctionFactory.created("g"));
            Assert.assertEquals(subqueryRosti, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI) - rosti);
        }
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private static final class DenySecretSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            if (Chars.equals(tableToken.getTableName(), "secret")) {
                throw CairoException.nonCritical().put("permission denied [table=").put(tableToken.getTableName()).put(']');
            }
        }
    }

    // Authorizes the columns each scan of a level reads, as the enterprise compiler does.
    private static final class ScanAuthorizingCompiler extends SqlCompilerImpl {
        private final ObjList<CharSequence> columnNames = new ObjList<>();

        private ScanAuthorizingCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        protected void authorizeColumnAccess(SqlExecutionContext executionContext, LogicalPlan root) {
            if (root instanceof ScanPlan scan) {
                final OutputSchema output = scan.getOutput();
                columnNames.clear();
                for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                    columnNames.add(output.getColumnName(i));
                }
                executionContext.getSecurityContext().authorizeSelect(scan.getTableToken(), columnNames);
                return;
            }
            for (int i = 0, n = root.inputCount(); i < n; i++) {
                authorizeColumnAccess(executionContext, root.inputAt(i));
            }
        }
    }
}
