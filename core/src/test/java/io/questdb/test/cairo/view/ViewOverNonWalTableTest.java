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

package io.questdb.test.cairo.view;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A view reads a non-WAL table, and an ALTER TABLE changes the table's columns. The writer applies
 * the change outside the WAL, so no WAL apply job queues the recompile of the views that read the
 * table: the writer has to queue it. Without the recompile, the view keeps its old columns in the
 * catalogue, and stays valid after a change that breaks it (#7792).
 */
public class ViewOverNonWalTableTest extends AbstractViewTest {
    // 2025-06-19T10:00:00.000000Z
    private static final long CREATE_MICROS = 1_750_327_200_000_000L;
    // 2025-06-19T11:00:00.000000Z
    private static final long ALTER_MICROS = 1_750_330_800_000_000L;

    @Test
    public void testAddColumnFromBusyWriterUpdatesViewColumns() throws Exception {
        // Another thread holds the writer, so the statement queues the change for it. The view
        // keeps its columns until the writer applies the change.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            try (
                    TableWriter writer = getWriter("t");
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext)
                            .execute(new SCSequence())
            ) {
                Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, future.getStatus());
                drainViewQueue();
                assertViewColumns("v", "a:INT");

                writer.tick(true);
                future.await();
            }
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT");
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAddColumnUpdatesViewColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            createView("v_dep", "SELECT * FROM v");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ADD COLUMN b INT");
            execute("INSERT INTO t VALUES (3, 30)");
            drainViewQueue();

            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
            // SHOW COLUMNS reads the metadata cache, which only the view compiler updates
            assertQuery("SHOW COLUMNS FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            b\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
            assertQuery("v_dep")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            3\t30
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAddColumnWhenWriterFreesUpUpdatesViewColumns() throws Exception {
        // The writer is busy when the statement starts and free by the time it would queue the
        // change, so the statement applies the change in the writer itself.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            final TableWriter[] holder = {getWriter("t")};
            OperationDispatcher.setAsyncEnqueueObserver(() -> holder[0] = Misc.free(holder[0]));
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext)
                            .execute(new SCSequence())
            ) {
                Assert.assertNull(holder[0]);
                future.await();
            } finally {
                OperationDispatcher.setAsyncEnqueueObserver(null);
                Misc.free(holder[0]);
            }
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT");
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAlterColumnTypeUpdatesViewColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ALTER COLUMN a TYPE LONG");
            drainViewQueue();

            assertViewColumns("v", "a:LONG");
            assertQuery("SELECT a, typeOf(a) FROM v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\ttypeOf
                            1\tLONG
                            2\tLONG
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testDropColumnInvalidatesView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, c INT)");
            execute("INSERT INTO t VALUES (1, 10)");
            createView("v", "SELECT a, c FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t DROP COLUMN c");
            drainViewQueue();

            assertViewState("v", "Invalid column: c");
            assertViewStatus("v", "invalid", "Invalid column: c", "2025-06-19T11:00:00.000000Z");

            // the column is back, and so is the view
            execute("ALTER TABLE t ADD COLUMN c INT");
            execute("INSERT INTO t VALUES (2, 20)");
            drainViewQueue();

            assertViewState("v");
            assertViewColumns("v", "a:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tc
                            1\tnull
                            2\t20
                            """);
        });
    }

    @Test
    public void testFailedAlterKeepsView() throws Exception {
        // A concurrent change adds the column after the statement compiled, so the writer
        // rejects the statement. The failed statement must not recompile the view, and the
        // table and the view must keep working.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            try (
                    TableWriter writer = getWriter("t");
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final CompiledQuery compiledQuery = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext);
                writer.addColumn("b", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                try (OperationFuture future = compiledQuery.execute(new SCSequence())) {
                    writer.tick(true);
                    try {
                        future.await();
                        Assert.fail("expected the writer to reject the duplicate column");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "duplicate column [name=b]");
                    }
                }
            }
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");

            execute("ALTER TABLE t ADD COLUMN c INT");
            execute("INSERT INTO t VALUES (3, 30, 300)");
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            1\tnull\tnull
                            2\tnull\tnull
                            3\t30\t300
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testNonStructuralAlterDoesNotRecompileView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (s SYMBOL, a INT)");
            execute("INSERT INTO t VALUES ('x', 1)");
            createView("v", "SELECT * FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ALTER COLUMN s ADD INDEX");
            execute("ALTER TABLE t ALTER COLUMN s NOCACHE");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");

            execute("ALTER TABLE t ALTER COLUMN s DROP INDEX");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
        });
    }

    @Test
    public void testRenameColumnInvalidatesView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, c INT)");
            execute("INSERT INTO t VALUES (1, 10)");
            createView("v", "SELECT a, c FROM t");
            createView("v_all", "SELECT * FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t RENAME COLUMN c TO d");
            drainViewQueue();

            assertViewState("v", "Invalid column: c");
            assertViewStatus("v", "invalid", "Invalid column: c", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("v_all", "a:INT, d:INT");

            execute("ALTER TABLE t RENAME COLUMN d TO c");
            drainViewQueue();

            assertViewState("v");
            assertViewColumns("v_all", "a:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tc
                            1\t10
                            """);
        });
    }

    private static void assertViewColumns(String viewName, String expectedColumns) {
        final StringSink sink = new StringSink();
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(viewName))) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i)));
            }
        }
        Assert.assertEquals(expectedColumns, sink.toString());
    }

    private void assertViewStatus(String viewName, String status, String reason, String updateTime) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, view_status_update_time FROM views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize(false)
                .returns("view_status\tinvalidation_reason\tview_status_update_time\n"
                        + status + '\t' + reason + '\t' + updateTime + '\n');
    }

    // creates a non-partitioned table, which is never a WAL table, and a view over it at CREATE_MICROS
    private void createTableAndView(String tableName, String viewName) throws SqlException {
        setCurrentMicros(CREATE_MICROS);
        execute("CREATE TABLE " + tableName + " (a INT)");
        execute("INSERT INTO " + tableName + " VALUES (1), (2)");
        createView(viewName, "SELECT * FROM " + tableName);
        assertViewColumns(viewName, "a:INT");
        Assert.assertFalse(engine.verifyTableName(tableName).isWal());
    }
}
