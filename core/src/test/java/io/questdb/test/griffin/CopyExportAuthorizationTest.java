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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class CopyExportAuthorizationTest extends AbstractCairoTest {
    private static final String COPY_SQL = "COPY t TO 'out' WITH FORMAT PARQUET";
    private static final String EXPECTED_ROWS = """
            ts\tx\tsecret
            2024-01-02T00:00:00.000000Z\t2\t42
            2024-01-02T01:00:00.000000Z\tnull\t43
            """;
    private final boolean isWal;

    public CopyExportAuthorizationTest(WalMode walMode) {
        isWal = walMode == WalMode.WITH_WAL;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<WalMode> data() {
        return List.of(WalMode.WITH_WAL, WalMode.NO_WAL);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        // Compilers capture the retry limit at construction; each test may override it.
        engine.getSqlCompilerPool().releaseAll();
        final File output = new File(exportRoot, "out.parquet");
        Assert.assertTrue(!output.exists() || output.delete());
    }

    @Test
    public void testCompileDoesNotRetryDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            createOriginalTable();
            final AllowAllSecurityContext securityContext = new AllowAllSecurityContext() {
                private boolean hasDropped;

                @Override
                public boolean isTableVisible(TableToken tableToken) {
                    Assert.assertFalse("a missing table must not trigger another metadata attempt", hasDropped);
                    hasDropped = true;
                    TestUtils.unchecked(() -> execute("DROP TABLE t"));
                    return true;
                }
            };
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                assertCairoFailure(context, "table does not exist [table=t]");
                createReplacementTable();
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testCompileDoesNotRetryWhenDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 0);
        assertRetryExhaustion();
    }

    @Test
    public void testCompileRechecksColumnPermissionsAfterRecreate() throws Exception {
        assertMemoryLeak(() -> {
            createOriginalTable();
            final RecreatingSecurityContext securityContext = new RecreatingSecurityContext(1) {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    super.authorizeSelect(tableToken, columnNames);
                    if (columnNames.contains("secret")) {
                        throw CairoException.authorization().put("permission denied [table=t, column=secret]");
                    }
                }
            };
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                assertCairoFailure(context, "permission denied [table=t, column=secret]");
                Assert.assertEquals("[ts,x,secret]", securityContext.authorizedColumns.toString());
                Assert.assertEquals(engine.verifyTableName("t"), securityContext.authorizedTableToken);
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testCompileRechecksVisibilityAfterRecreate() throws Exception {
        assertMemoryLeak(() -> {
            createOriginalTable();
            final TableToken originalToken = engine.verifyTableName("t");
            final RecreatingSecurityContext securityContext = new RecreatingSecurityContext(1) {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    Assert.fail("COPY must check visibility before column permissions");
                }

                @Override
                public boolean isTableVisible(TableToken tableToken) {
                    super.isTableVisible(tableToken);
                    return tableToken.equals(originalToken);
                }
            };
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                assertCairoFailure(context, "table does not exist [table=t]");
                Assert.assertNotEquals(originalToken, engine.verifyTableName("t"));
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testCompileRetriesRecreatedTableAtDefaultLimit() throws Exception {
        assertMemoryLeak(() -> {
            createOriginalTable();
            assertRecreatedTableExport(COPY_SQL, configuration.getMaxSqlRecompileAttempts());
        });
    }

    @Test
    public void testCompileRetriesRecreatedTableFunction() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 1);
        assertMemoryLeak(() -> {
            createOriginalTable();
            assertRecreatedTableExport("COPY t() TO 'out' WITH FORMAT PARQUET", 1);
        });
    }

    @Test
    public void testCompileStopsAtDefaultRetryLimit() throws Exception {
        assertRetryExhaustion();
    }

    private static void assertCairoFailure(SqlExecutionContext context, String expectedMessage) throws SqlException {
        try (RecordCursorFactory ignored = select(COPY_SQL, context)) {
            Assert.fail("COPY compilation must fail");
        } catch (CairoException e) {
            TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
        }
    }

    private static SqlExecutionContextImpl newContext(AllowAllSecurityContext securityContext) {
        return new SqlExecutionContextImpl(engine, 1)
                .with(securityContext, null, null, -1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    private void assertExport(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertNotNull(cursor.getRecord().getStrA(0));
            Assert.assertFalse(cursor.hasNext());
        }
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            Assert.assertTrue("the export must reach the job", job.run());
        }
        assertQuery("SELECT status, num_exported_files, export_path FROM \"sys.copy_export_log\" LIMIT -1")
                .noLeakCheck()
                .expectSize()
                .returns("status\tnum_exported_files\texport_path\nfinished\t1\t" + exportRoot + File.separator + "out.parquet\n");
        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "out.parquet')")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(EXPECTED_ROWS);
        assertQuery("SELECT * FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(EXPECTED_ROWS);
    }

    private void assertNoExportAndRecover(SqlExecutionContextImpl context) throws Exception {
        Assert.assertFalse(new File(exportRoot, "out.parquet").exists());
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            Assert.assertFalse("failed compilation must not queue an export", job.run());
        }
        Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
        Assert.assertEquals(0, engine.getBusyReaderCount());
        Assert.assertEquals(0, engine.getBusyWriterCount());
        context.with(AllowAllSecurityContext.INSTANCE);
        try (RecordCursorFactory factory = select(COPY_SQL, context)) {
            assertExport(factory, context);
        }
    }

    private void assertRecreatedTableExport(String sql, int recreations) throws Exception {
        final RecreatingSecurityContext securityContext = new RecreatingSecurityContext(recreations);
        try (
                SqlExecutionContext context = newContext(securityContext);
                RecordCursorFactory factory = select(sql, context)
        ) {
            Assert.assertEquals(0, securityContext.recreationsLeft);
            Assert.assertEquals("[ts,x,secret]", securityContext.authorizedColumns.toString());
            Assert.assertEquals(engine.verifyTableName("t"), securityContext.authorizedTableToken);
            assertExport(factory, context);
        }
    }

    private void assertRetryExhaustion() throws Exception {
        assertMemoryLeak(() -> {
            createOriginalTable();
            final RecreatingSecurityContext securityContext = new RecreatingSecurityContext(configuration.getMaxSqlRecompileAttempts() + 1);
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                try (RecordCursorFactory ignored = select(COPY_SQL, context)) {
                    Assert.fail("COPY must stop retrying when it reaches the configured limit");
                } catch (SqlException e) {
                    Assert.assertEquals(5, e.getPosition());
                    TestUtils.assertEquals("table schema changed too many times [table=t]", e.getFlyweightMessage());
                }
                Assert.assertEquals(0, securityContext.recreationsLeft);
                Assert.assertNull(securityContext.authorizedTableToken);
                assertNoExportAndRecover(context);
            }
        });
    }

    private void createOriginalTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO t VALUES ('2024-01-01', 1)");
        drainWalQueue();
    }

    private void createReplacementTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT, secret INT) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("""
                INSERT INTO t VALUES
                    ('2024-01-02T00:00:00', 2, 42),
                    ('2024-01-02T01:00:00', NULL, 43)
                """);
        drainWalQueue();
    }

    private class RecreatingSecurityContext extends AllowAllSecurityContext {
        private final ObjList<CharSequence> authorizedColumns = new ObjList<>();
        private TableToken authorizedTableToken;
        private int recreationsLeft;

        private RecreatingSecurityContext(int recreations) {
            recreationsLeft = recreations;
        }

        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            authorizedTableToken = tableToken;
            authorizedColumns.clear();
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                authorizedColumns.add(Chars.toString(columnNames.getQuick(i)));
            }
        }

        @Override
        public boolean isTableVisible(TableToken tableToken) {
            // Force another session's DROP/CREATE between name resolution and metadata acquisition.
            if (recreationsLeft > 0) {
                recreationsLeft--;
                TestUtils.unchecked(() -> {
                    execute("DROP TABLE t");
                    createReplacementTable();
                });
            }
            return true;
        }
    }
}
