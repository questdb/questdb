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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Binding builds sub-query consumers over the plan's metadata and the sub-queries are generated once the statement is
 * bound: binding errors precede sub-query generation errors, which surface in bind order, as on master.
 */
public class SubqueryGenerationOrderTest extends AbstractCairoTest {
    private static final String BAD_SUBQUERY = "(SELECT sum(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym)"
            + " RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING)";
    private static final String UNFRAMED_TOUCH = "touch(SELECT * FROM trades LATEST ON ts PARTITION BY sym)";

    @Test
    public void testDroppedSubqueryErrorSurfaces() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE true OR price = " + BAD_SUBQUERY)
                    .fails(159, "WINDOW join hi value cannot be less than lo value");
        });
    }

    @Test
    public void testSubqueryArgumentErrorPrecedesCallError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE EXISTS " + BAD_SUBQUERY)
                    .fails(150, "WINDOW join hi value cannot be less than lo value");
            assertQuery("SELECT price FROM trades WHERE EXISTS (SELECT price FROM prices)")
                    .fails(31, "unknown function name: EXISTS(CURSOR)");
        });
    }

    @Test
    public void testEarlierSubqueryErrorWins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT price FROM trades WHERE price = (SELECT price FROM prices LIMIT 1) AND price = " + BAD_SUBQUERY)
                    .fails(198, "WINDOW join hi value cannot be less than lo value");
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
                final String query = "SELECT price FROM trades WHERE price = (SELECT sum(p.price) FROM secret t WINDOW JOIN prices p"
                        + " ON (t.sym = p.sym) RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING)";
                try (RecordCursorFactory ignore = compiler.compile(query, executionContext).getRecordCursorFactory()) {
                    Assert.fail("authorization must reject the sub-query");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "permission denied [table=secret]");
                }
                try (RecordCursorFactory ignore = compiler.compile("SELECT price FROM trades WHERE price = " + BAD_SUBQUERY,
                        executionContext).getRecordCursorFactory()) {
                    Assert.fail("the sub-query must fail generation");
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
            assertQuery("SELECT price FROM trades WHERE price = " + BAD_SUBQUERY + " AND missing = 1")
                    .fails(175, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = (SELECT price FROM prices WHERE price = " + BAD_SUBQUERY + ") AND missing = 1")
                    .fails(216, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = " + BAD_SUBQUERY + " AND price = (SELECT missing FROM prices)")
                    .fails(191, "Invalid column: missing");
            assertQuery("SELECT price FROM trades WHERE price = " + BAD_SUBQUERY + " ORDER BY missing")
                    .fails(180, "Invalid column: missing");
        });
    }

    @Test
    public void testStatementsWithoutQueryGenerationSurfaceSubqueryError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException("CREATE VIEW v1 AS (SELECT price FROM trades WHERE price = " + BAD_SUBQUERY + ")",
                    170, "WINDOW join hi value cannot be less than lo value");
            assertException("INSERT INTO trades(sym, price) SELECT sym, price FROM trades WHERE price = " + BAD_SUBQUERY,
                    187, "WINDOW join hi value cannot be less than lo value");
        });
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
