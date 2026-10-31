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
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
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

    @Test
    public void testExportJobDeniesColumnAddedAfterCompile() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            final TableToken token = engine.verifyTableName("t");
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext() {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    super.authorizeSelect(tableToken, columnNames);
                    if (columnNames.contains("extra")) {
                        throw CairoException.authorization().put("permission denied [table=t, column=extra]");
                    }
                }
            };
            securityContext.grantedTableId = token.getTableId();
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                try (RecordCursorFactory factory = select(COPY_SQL, context)) {
                    execute("ALTER TABLE t ADD COLUMN extra INT");
                    drainWalQueue();
                    securityContext.isEnforced = true;
                    runExportJob(factory, context);
                }
                Assert.assertEquals(token, securityContext.jobTableToken);
                Assert.assertEquals("[ts,x,secret,extra]", securityContext.jobColumns.toString());
                assertExportFailed("permission denied [table=t, column=extra]");
                execute("ALTER TABLE t DROP COLUMN extra");
                drainWalQueue();
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testExportJobDeniesRevokedSelect() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext();
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                exportAfterEnforcing(securityContext, context);
                Assert.assertEquals(engine.verifyTableName("t"), securityContext.jobTableToken);
                Assert.assertEquals("[ts,x,secret]", securityContext.jobColumns.toString());
                assertExportFailed("permission denied [table=t]");
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testExportJobExportsAuthorizedSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            final TableToken token = engine.verifyTableName("t");
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext();
            securityContext.grantedTableId = token.getTableId();
            // A column added after the export job took its reader stays out of both the check and the file.
            securityContext.concurrentChange = () -> {
                execute("ALTER TABLE t ADD COLUMN extra INT");
                drainWalQueue();
            };
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                exportAfterEnforcing(securityContext, context);
            }
            TestUtils.assertEquals("", securityContext.refusedChange);
            Assert.assertEquals(token, securityContext.jobTableToken);
            Assert.assertEquals("[ts,x,secret]", securityContext.jobColumns.toString());
            assertExportSucceeded();
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tsecret\textra
                            2024-01-02T00:00:00.000000Z\t2\t42\tnull
                            2024-01-02T01:00:00.000000Z\tnull\t43\tnull
                            """);
        });
    }

    @Test
    public void testExportJobRejectsTableDroppedDuringCheck() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            final TableToken token = engine.verifyTableName("t");
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext();
            securityContext.concurrentChange = () -> execute("DROP TABLE t");
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                exportAfterEnforcing(securityContext, context);
                assertRejectedExport(securityContext, token, "table does not exist [table=t]");
                if (isWal) {
                    createReplacementTable();
                }
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testExportJobRejectsTableRenamedDuringCheck() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            final TableToken token = engine.verifyTableName("t");
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext();
            securityContext.concurrentChange = () -> execute("RENAME TABLE t TO t_old");
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                exportAfterEnforcing(securityContext, context);
                assertRejectedExport(securityContext, token, "table does not exist [table=t]");
                if (isWal) {
                    execute("RENAME TABLE t_old TO t");
                }
                assertNoExportAndRecover(context);
            }
        });
    }

    @Test
    public void testExportJobRejectsTableSwappedDuringCheck() throws Exception {
        assertMemoryLeak(() -> {
            createReplacementTable();
            execute("CREATE TABLE t_new (ts TIMESTAMP, y INT) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
            final TableToken token = engine.verifyTableName("t");
            final TableToken newToken = engine.verifyTableName("t_new");
            final JobCheckSecurityContext securityContext = new JobCheckSecurityContext();
            // the principal may read only the table that takes over the name
            securityContext.grantedTableId = newToken.getTableId();
            securityContext.concurrentChange = () -> {
                execute("RENAME TABLE t TO t_old");
                execute("RENAME TABLE t_new TO t");
            };
            try (SqlExecutionContextImpl context = newContext(securityContext)) {
                exportAfterEnforcing(securityContext, context);
                assertRejectedExport(
                        securityContext,
                        token,
                        "cached query plan cannot be used because table schema has changed [table=t', expectedTableId="
                                + token.getTableId() + ", actualTableId=" + newToken.getTableId()
                                + ", expectedMetadataVersion=" + newToken.getTableId() + ", actualMetadataVersion=-1]"
                );
                if (isWal) {
                    execute("DROP TABLE t");
                    execute("RENAME TABLE t_old TO t");
                    drainWalQueue();
                }
                assertNoExportAndRecover(context);
            }
        });
    }

    private static void assertCairoFailure(SqlExecutionContext context, String expectedMessage) throws SqlException {
        try (RecordCursorFactory ignored = select(COPY_SQL, context)) {
            Assert.fail("COPY compilation must fail");
        } catch (CairoException e) {
            TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
        }
    }

    private static void exportAfterEnforcing(JobCheckSecurityContext securityContext, SqlExecutionContext context) throws Exception {
        try (RecordCursorFactory factory = select(COPY_SQL, context)) {
            // Grants can change between compilation and the export job, e.g. for a prepared statement or a queued export.
            securityContext.isEnforced = true;
            runExportJob(factory, context);
        }
    }

    private static SqlExecutionContextImpl newContext(AllowAllSecurityContext securityContext) {
        return new SqlExecutionContextImpl(engine, 1)
                .with(securityContext, null, null, -1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    private static void runExportJob(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertNotNull(cursor.getRecord().getStrA(0));
            Assert.assertFalse(cursor.hasNext());
        }
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            Assert.assertTrue("the export must reach the job", job.run());
        }
    }

    private void assertExport(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        runExportJob(factory, context);
        assertExportSucceeded();
        assertQuery("SELECT * FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(EXPECTED_ROWS);
    }

    private void assertExportFailed(String expectedMessage) throws Exception {
        assertQuery("SELECT status, message FROM \"sys.copy_export_log\" LIMIT -1")
                .noLeakCheck()
                .expectSize()
                .returns("status\tmessage\nfailed\t" + expectedMessage + '\n');
    }

    private void assertExportSucceeded() throws Exception {
        assertQuery("SELECT status, num_exported_files, export_path FROM \"sys.copy_export_log\" LIMIT -1")
                .noLeakCheck()
                .expectSize()
                .returns("status\tnum_exported_files\texport_path\nfinished\t1\t" + exportRoot + File.separator + "out.parquet\n");
        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "out.parquet')")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(EXPECTED_ROWS);
    }

    private void assertNoExportAndRecover(SqlExecutionContextImpl context) throws Exception {
        Assert.assertFalse(new File(exportRoot, "out.parquet").exists());
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            Assert.assertFalse("no export may remain queued", job.run());
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

    private void assertRejectedExport(JobCheckSecurityContext securityContext, TableToken token, String walFailure) throws Exception {
        Assert.assertEquals(token, securityContext.jobTableToken);
        Assert.assertEquals("[ts,x,secret]", securityContext.jobColumns.toString());
        if (isWal) {
            // RENAME and DROP of a WAL table do not wait for the reader the export job holds
            TestUtils.assertEquals("", securityContext.refusedChange);
            assertExportFailed(walFailure);
        } else {
            // the export job's reader blocks RENAME and DROP of a non-WAL table, so the check judges t itself
            TestUtils.assertContains(securityContext.refusedChange, "busyReader");
            assertExportFailed("permission denied [table=t]");
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

    // Allows everything until a test enforces the grants for the export job. From then on it judges the table that
    // holds the name when the check runs, as the Enterprise permission check does: the granted table passes, and so
    // does a name that no table holds. The concurrent change runs inside the check, after the job took its reader.
    private static class JobCheckSecurityContext extends AllowAllSecurityContext {
        private final ObjList<CharSequence> jobColumns = new ObjList<>();
        private final StringSink refusedChange = new StringSink();
        private TestUtils.CheckedRunnable concurrentChange;
        private int grantedTableId = -1;
        private boolean isEnforced;
        private TableToken jobTableToken;

        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            if (!isEnforced) {
                return;
            }
            jobTableToken = tableToken;
            jobColumns.clear();
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                jobColumns.add(Chars.toString(columnNames.getQuick(i)));
            }
            if (concurrentChange != null) {
                final TestUtils.CheckedRunnable change = concurrentChange;
                concurrentChange = null;
                try {
                    change.run();
                } catch (Throwable th) {
                    refusedChange.put(th instanceof FlyweightMessageContainer f ? f.getFlyweightMessage() : th.toString());
                }
            }
            final TableToken nameHolder = engine.getTableTokenIfExists(tableToken.getTableName());
            if (nameHolder != null && nameHolder.getTableId() != grantedTableId) {
                throw CairoException.authorization().put("permission denied [table=").put(tableToken.getTableName()).put(']');
            }
        }
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
