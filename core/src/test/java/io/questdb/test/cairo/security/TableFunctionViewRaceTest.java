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

package io.questdb.test.cairo.security;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A table-name function or a SHOW statement written in a view reads the object it names through the
 * view, see SqlExecutionContext.isTableFunctionVisible(). Another session may change the view while a
 * statement reading it compiles, or after StaleViewCheckFactory verified it and before the function's
 * cursor opens. Such a change makes the plan stale, so it must surface as
 * TableReferenceOutOfDateException, which the select caches answer by recompiling, never as the
 * named object missing. The interleavings are forced with hooks, never with timing.
 */
public class TableFunctionViewRaceTest extends AbstractCairoTest {
    // DDL that another session commits right after the compile captures the view a function reads through
    private static final AtomicReference<String[]> DDL_AFTER_VIEW_CAPTURE = new AtomicReference<>();
    // DDL that another session commits right after StaleViewCheckFactory verified the view
    private static final AtomicReference<String[]> DDL_AFTER_VIEW_CHECK = new AtomicReference<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public void verifyViewToken(TableToken tableToken, long expectedTxn) {
                super.verifyViewToken(tableToken, expectedTxn);
                final String[] ddl = DDL_AFTER_VIEW_CHECK.getAndSet(null);
                if (ddl != null) {
                    executeAsOtherSession(this, ddl);
                }
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAlterViewAfterStaleCheckRecompilesCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_columns AS (SELECT \"column\" FROM table_columns('t'))");
            drainWalAndViewQueues();
            // the view's cursor opens inside the join's cursor, after the statement verified the view
            assertStalePlanAfterViewCheck(
                    "SELECT count() FROM t CROSS JOIN v_columns",
                    "ALTER VIEW v_columns AS (SELECT \"column\" FROM table_columns('t2'))"
            );
            assertQuery("SELECT count() FROM t CROSS JOIN v_columns").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    @Test
    public void testAlterViewAfterStaleCheckRecompilesShowCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_show_create AS (SELECT * FROM (SHOW CREATE TABLE t))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCheck(
                    "SELECT count() FROM v_show_create",
                    "ALTER VIEW v_show_create AS (SELECT * FROM (SHOW CREATE TABLE t2))"
            );
            assertQuery("SELECT count() FROM v_show_create").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testAlterViewAfterStaleCheckRecompilesTableColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_columns AS (SELECT \"column\" FROM table_columns('t'))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCheck(
                    "SELECT * FROM v_columns",
                    "ALTER VIEW v_columns AS (SELECT \"column\" FROM table_columns('t2'))"
            );
            assertQuery("SELECT * FROM v_columns").noLeakCheck().noRandomAccess().returns("column\nts\na\nb\n");
        });
    }

    @Test
    public void testAlterViewAfterStaleCheckRecompilesWalTransactions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_txns AS (SELECT * FROM wal_transactions('t'))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCheck(
                    "SELECT count() FROM v_txns",
                    "ALTER VIEW v_txns AS (SELECT * FROM wal_transactions('t2'))"
            );
            assertQuery("SELECT count() FROM v_txns").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testAlterViewWhileCompilingShowColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_show AS (SELECT \"column\" FROM (SHOW COLUMNS FROM t))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCapture(
                    "SELECT * FROM v_show",
                    "ALTER VIEW v_show AS (SELECT \"column\" FROM (SHOW COLUMNS FROM t2))"
            );
            assertQuery("SELECT * FROM v_show").noLeakCheck().noRandomAccess().returns("column\nts\na\nb\n");
        });
    }

    @Test
    public void testAlterViewWhileCompilingTableColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_columns AS (SELECT \"column\" FROM table_columns('t'))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCapture(
                    "SELECT * FROM v_columns",
                    "ALTER VIEW v_columns AS (SELECT \"column\" FROM table_columns('t2'))"
            );
            assertQuery("SELECT * FROM v_columns").noLeakCheck().noRandomAccess().returns("column\nts\na\nb\n");
        });
    }

    @Test
    public void testDropViewAfterStaleCheckIsStalePlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_columns AS (SELECT \"column\" FROM table_columns('t'))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCheck("SELECT * FROM v_columns", "DROP VIEW v_columns");
            // the recompile, not the stale plan, reports the view missing
            assertException("SELECT * FROM v_columns", 14, "table does not exist [table=v_columns]");
        });
    }

    @Test
    public void testRecreateViewAfterStaleCheckRecompilesShowColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_show AS (SELECT \"column\" FROM (SHOW COLUMNS FROM t))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCheck(
                    "SELECT * FROM v_show",
                    "DROP VIEW v_show",
                    "CREATE VIEW v_show AS (SELECT \"column\" FROM (SHOW COLUMNS FROM t2))"
            );
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_show").noLeakCheck().noRandomAccess().returns("column\nts\na\nb\n");
        });
    }

    @Test
    public void testRecreateViewWhileCompilingShowCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_show_create AS (SELECT * FROM (SHOW CREATE TABLE t))");
            drainWalAndViewQueues();
            assertStalePlanAfterViewCapture(
                    "SELECT count() FROM v_show_create",
                    "DROP VIEW v_show_create",
                    "CREATE VIEW v_show_create AS (SELECT * FROM (SHOW CREATE TABLE t2))"
            );
            drainWalAndViewQueues();
            assertQuery("SELECT count() FROM v_show_create").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testReplaceViewAfterStaleCheckRecompilesTablePartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_parts AS (SELECT * FROM table_partitions('t'))");
            drainWalAndViewQueues();
            assertQuery("SELECT count() FROM v_parts").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertStalePlanAfterViewCheck(
                    "SELECT count() FROM v_parts",
                    "CREATE OR REPLACE VIEW v_parts AS (SELECT * FROM table_partitions('t2'))"
            );
            assertQuery("SELECT count() FROM v_parts").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    private static void assertStalePlan(RecordCursorFactory factory, String sql) throws SqlException {
        try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("a plan compiled against a changed view must not open: " + sql);
        } catch (TableReferenceOutOfDateException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used");
        }
    }

    // Another session changes the view right after the compile captured the view a table-name function or
    // a SHOW statement reads through. The compile must not report the named object missing.
    private static void assertStalePlanAfterViewCapture(String sql, String... ddl) throws SqlException {
        try (SqlExecutionContextImpl capturingContext = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public void setTableFunctionView(TableFunctionView view) {
                super.setTableFunctionView(view);
                if (view != null) {
                    final String[] pending = DDL_AFTER_VIEW_CAPTURE.getAndSet(null);
                    if (pending != null) {
                        executeAsOtherSession(engine, pending);
                    }
                }
            }
        }) {
            capturingContext.with(AllowAllSecurityContext.INSTANCE);
            DDL_AFTER_VIEW_CAPTURE.set(ddl);
            try (RecordCursorFactory factory = select(sql, capturingContext)) {
                Assert.assertNull("the view change must have happened during the compile", DDL_AFTER_VIEW_CAPTURE.get());
                assertStalePlan(factory, sql);
            } finally {
                DDL_AFTER_VIEW_CAPTURE.set(null);
            }
        }
    }

    // Another session changes the view right after StaleViewCheckFactory verified it, before the cursor of
    // the table-name function or SHOW statement opens. The plan is stale: it must not report the named
    // object missing, nor read through the old definition.
    private static void assertStalePlanAfterViewCheck(String sql, String... ddl) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            DDL_AFTER_VIEW_CHECK.set(ddl);
            try {
                assertStalePlan(factory, sql);
                Assert.assertNull("the view change must have happened after the view check", DDL_AFTER_VIEW_CHECK.get());
            } finally {
                DDL_AFTER_VIEW_CHECK.set(null);
            }
        }
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE t2 (ts TIMESTAMP, a INT, b INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES ('2024-01-01', 1)");
        drainWalQueue();
    }

    private static void executeAsOtherSession(CairoEngine engine, String[] ddl) {
        try (SqlExecutionContextImpl otherSession = new SqlExecutionContextImpl(engine, 1)) {
            otherSession.with(AllowAllSecurityContext.INSTANCE);
            for (String sql : ddl) {
                engine.execute(sql, otherSession);
            }
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }
}
