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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class ViewCompilationRetryTest extends AbstractViewTest {
    private static final String EXPECTED_ROWS = """
            v\tts2\tc
            11\t1970-01-01T00:00:00.000000Z\t5
            null\t1970-01-01T01:00:00.000000Z\t7
            """;
    private static final String OLD_VIEW_SQL = "SELECT 42 AS value FROM long_sequence(1)";
    private static final String VIEW_SQL = """
            DECLARE OVERRIDABLE @c := a
            SELECT t.v, t.ts2, u.c
            FROM (SELECT k, ts2, @c AS v FROM x) t ASOF JOIN y u ON (k)
            """;
    private final String wal;

    public ViewCompilationRetryTest(WalMode walMode) {
        wal = walMode == WalMode.WITH_WAL ? " WAL" : " BYPASS WAL";
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<WalMode> data() {
        return List.of(WalMode.WITH_WAL, WalMode.NO_WAL);
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // Compilers capture the retry limit at construction, while tests change the configuration.
        engine.getSqlCompilerPool().releaseAll();
    }

    @Test
    public void testAlterCursorRetryPreservesMetadataProbe() throws Exception {
        assertReplacementRetry("ALTER VIEW", true, false);
    }

    @Test
    public void testAlterInvalidColumnAfterPlanRetry() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables(false);
            createView("v", OLD_VIEW_SQL);
            final RacingSecurityContext securityContext = new RacingSecurityContext(false, true, 1);
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, null, null, -1, null)) {
                assertExceptionNoLeakCheck(
                        "ALTER VIEW v AS (SELECT a * 10 FROM x ASOF JOIN y ON (k))",
                        16,
                        "invalid column name [name=a * 10, position=0]",
                        context
                );
                Assert.assertEquals(1, securityContext.changeCount);
                Assert.assertEquals(2, securityContext.yVisits);
                Assert.assertFalse(context.isMetadataProbe());
                assertNothingBusy();
                assertViewDefinition("v", OLD_VIEW_SQL);
                assertViewDefinitionFile("v", OLD_VIEW_SQL);
                assertQuery("SELECT * FROM v").noLeakCheck().expectSize().returns("value\n42\n");
                execute("ALTER VIEW v AS (" + VIEW_SQL + ")", context);
            }
            drainWalAndViewQueues();
            assertCurrentDependencies(getViewDefinition("v"));
            try (SqlExecutionContext reader = viewOnlyContext()) {
                assertRead(reader, false);
            }
        });
    }

    @Test
    public void testAlterNegativeRetryLimit() throws Exception {
        assertRetryLimit("ALTER VIEW", Integer.MIN_VALUE);
    }

    @Test
    public void testAlterPlanRetry() throws Exception {
        assertReplacementRetry("ALTER VIEW", false, false);
    }

    @Test
    public void testAlterPlanRetryEmpty() throws Exception {
        assertReplacementRetry("ALTER VIEW", false, true);
    }

    @Test
    public void testAlterRetryLimit() throws Exception {
        assertRetryLimit("ALTER VIEW", 2);
    }

    @Test
    public void testCreateNegativeRetryLimit() throws Exception {
        assertRetryLimit("CREATE VIEW", -1);
    }

    @Test
    public void testCreateOrReplacePlanRetry() throws Exception {
        assertReplacementRetry("CREATE OR REPLACE VIEW", false, false);
    }

    @Test
    public void testCreateOrReplacePlanRetryEmpty() throws Exception {
        assertReplacementRetry("CREATE OR REPLACE VIEW", false, true);
    }

    @Test
    public void testCreateOrReplaceRetryLimit() throws Exception {
        assertRetryLimit("CREATE OR REPLACE VIEW", 2);
    }

    @Test
    public void testCreatePlanRetry() throws Exception {
        assertReplacementRetry("CREATE VIEW", false, false);
    }

    @Test
    public void testCreatePlanRetryEmpty() throws Exception {
        assertReplacementRetry("CREATE VIEW", false, true);
    }

    @Test
    public void testCreateRetryLimitZero() throws Exception {
        assertRetryLimit("CREATE VIEW", 0);
    }

    @Test
    public void testLastAlterAttemptSucceeds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 1);
        assertReplacementRetry("ALTER VIEW", false, false);
    }

    private static void assertCurrentDependencies(ViewDefinition definition) {
        Assert.assertEquals(2, definition.getDependencies().size());
        final LowerCaseCharSequenceHashSet x = definition.getDependencies().get("x");
        Assert.assertNotNull(x);
        Assert.assertEquals(3, x.size());
        Assert.assertTrue(x.contains("a"));
        Assert.assertTrue(x.contains("k"));
        Assert.assertTrue(x.contains("ts2"));
        final LowerCaseCharSequenceHashSet y = definition.getDependencies().get("y");
        Assert.assertNotNull(y);
        Assert.assertEquals(3, y.size());
        Assert.assertTrue(y.contains("c"));
        Assert.assertTrue(y.contains("k"));
        Assert.assertTrue(y.contains("ts"));
    }

    private static void assertNothingBusy() {
        Assert.assertEquals(0, engine.getBusyReaderCount());
        Assert.assertEquals(0, engine.getBusyWriterCount());
    }

    private static SqlExecutionContext viewOnlyContext() {
        return new SqlExecutionContextImpl(engine, 1).with(new AllowAllSecurityContext() {
            @Override
            public void authorizeSelect(TableToken token, @NotNull ObjList<CharSequence> columns) {
                throw CairoException.nonCritical().put("base table SELECT denied [table=")
                        .put(token.getTableName()).put(", columns=").put(columns).put(']');
            }
        }, null, null, -1, null);
    }

    private void assertRead(SqlExecutionContext context, boolean isEmpty) throws Exception {
        // The successful plan can have a different designated timestamp from the discarded plan.
        assertQuery("SELECT * FROM v").withContext(context).noLeakCheck()
                .inferTimestamp().inferRandomAccess().sizeMayVary()
                .returns(isEmpty ? "v\tts2\tc\n" : EXPECTED_ROWS);
    }

    private void assertReplacementRetry(String statement, boolean isCursorOpen, boolean isEmpty) throws Exception {
        assertMemoryLeak(() -> {
            createTables(isEmpty);
            if (!statement.equals("CREATE VIEW")) {
                createView("v", OLD_VIEW_SQL);
            }
            final RacingSecurityContext securityContext = new RacingSecurityContext(isCursorOpen, true, 1);
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, null, null, -1, null)) {
                // A nested metadata probe must survive both the failed open and its retry.
                context.setMetadataProbe(isCursorOpen);
                execute(statement + " v AS (" + VIEW_SQL + ")", context);
                Assert.assertEquals(isCursorOpen, context.isMetadataProbe());
                Assert.assertEquals(1, securityContext.changeCount);
                Assert.assertEquals(2, securityContext.yVisits);
            }
            drainWalAndViewQueues();
            assertViewState("v");
            try (SqlExecutionContext reader = viewOnlyContext()) {
                assertRead(reader, isEmpty);
                assertQuery("SELECT ts FROM x").withContext(reader).noLeakCheck()
                        .failsWith("base table SELECT denied [table=x, columns=[ts]]");
                assertQuery("DECLARE @c := ts SELECT * FROM v").withContext(reader).noLeakCheck()
                        .failsWith("base table SELECT denied [table=x, columns=[ts]]");
                assertCurrentDependencies(getViewDefinition("v"));
                try (BlockFileReader file = new BlockFileReader(configuration); Path path = new Path()) {
                    path.of(configuration.getDbRoot());
                    final ViewDefinition stored = new ViewDefinition();
                    ViewDefinition.readFrom(stored, file, path, path.size(), engine.verifyTableName("v"));
                    assertCurrentDependencies(stored);
                }
                compileView("v");
                assertCurrentDependencies(getViewDefinition("v"));
                assertRead(reader, isEmpty);
                assertQuery("DECLARE @c := ts SELECT * FROM v").withContext(reader).noLeakCheck()
                        .failsWith("base table SELECT denied [table=x, columns=[ts]]");
            }
            assertNothingBusy();
        });
    }

    private void assertRetryLimit(String statement, int maxRetries) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, maxRetries);
        final int allowedRetries = Math.max(0, maxRetries);
        assertMemoryLeak(() -> {
            createTables(false);
            final boolean isCreate = statement.equals("CREATE VIEW");
            if (!isCreate) {
                createView("v", OLD_VIEW_SQL);
            }
            final int tableId = engine.verifyTableName("x").getTableId();
            final RacingSecurityContext securityContext = new RacingSecurityContext(false, false, allowedRetries + 1);
            final String sql = statement + " v AS (" + VIEW_SQL + ")";
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, null, null, -1, null)) {
                try {
                    execute(sql, context);
                    Assert.fail("expected the view compilation to exhaust its retries");
                } catch (SqlException e) {
                    Assert.assertEquals(isCreate ? 0 : sql.indexOf('('), e.getPosition());
                    Assert.assertEquals(
                            "cached query plan cannot be used because table schema has changed [table=x', expectedTableId="
                                    + tableId + ", actualTableId=" + tableId + ", expectedMetadataVersion="
                                    + allowedRetries + ", actualMetadataVersion=" + (allowedRetries + 1) + "]",
                            e.getFlyweightMessage().toString()
                    );
                }
                Assert.assertEquals(allowedRetries + 1, securityContext.changeCount);
                Assert.assertEquals(allowedRetries + 1, securityContext.yVisits);
                Assert.assertFalse(context.isMetadataProbe());
                assertNothingBusy();
                if (isCreate) {
                    Assert.assertNull(engine.getTableTokenIfExists("v"));
                } else {
                    assertViewDefinition("v", OLD_VIEW_SQL);
                    assertViewDefinitionFile("v", OLD_VIEW_SQL);
                    Assert.assertEquals(0, getViewDefinition("v").getDependencies().size());
                    assertQuery("SELECT * FROM v").noLeakCheck().expectSize().returns("value\n42\n");
                }
                // The same context and compiler pool remain usable once the schema settles.
                execute(sql, context);
            }
            drainWalAndViewQueues();
            assertViewState("v");
            try (SqlExecutionContext reader = viewOnlyContext()) {
                assertRead(reader, false);
            }
            Assert.assertEquals(allowedRetries + 1, securityContext.changeCount);
            assertNothingBusy();
        });
    }

    private void createTables(boolean isEmpty) throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY" + wal);
        execute("CREATE TABLE y (ts TIMESTAMP, k SYMBOL, c LONG) TIMESTAMP(ts) PARTITION BY DAY" + wal);
        execute("CREATE TABLE x2 (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts2) PARTITION BY DAY" + wal);
        // Force code generation to reject the metadata version that the first plan captured.
        execute("ALTER TABLE x2 ADD COLUMN pad INT");
        if (!isEmpty) {
            execute("INSERT INTO x VALUES (0, 0, 'k1', 11, 12), (3_600_000_000, 3_600_000_000, 'k1', NULL, 14)");
            execute("INSERT INTO y VALUES (0, 'k1', 5), (3_600_000_000, 'k1', 7)");
            execute("""
                    INSERT INTO x2 (ts, ts2, k, a, b) VALUES
                        (10, 0, 'k1', 11, 12), (3_600_000_010, 3_600_000_000, 'k1', NULL, 14)
                    """);
        }
        drainWalAndViewQueues();
        try (TableReader x = engine.getReader("x"); TableReader x2 = engine.getReader("x2")) {
            Assert.assertEquals(0, x.getMetadataVersion());
            Assert.assertEquals(1, x2.getMetadataVersion());
        }
    }

    // Visibility checks pause optimisation after x's metadata; SELECT checks pause cursor opening.
    private static final class RacingSecurityContext extends AllowAllSecurityContext {
        private final boolean isCursorOpen;
        private final boolean isReplacement;
        private int changeCount;
        private int changesRemaining;
        private int yVisits;

        private RacingSecurityContext(boolean isCursorOpen, boolean isReplacement, int changesRemaining) {
            this.isCursorOpen = isCursorOpen;
            this.isReplacement = isReplacement;
            this.changesRemaining = changesRemaining;
        }

        @Override
        public void authorizeSelect(TableToken token, @NotNull ObjList<CharSequence> columns) {
            if (isCursorOpen && token.getTableName().equals("x")) {
                changeSchema();
            }
        }

        @Override
        public boolean isTableVisible(TableToken token) {
            if (token.getTableName().equals("y")) {
                yVisits++;
                if (!isCursorOpen) {
                    changeSchema();
                }
            }
            return true;
        }

        private void changeSchema() {
            if (changesRemaining == 0) {
                return;
            }
            changesRemaining--;
            changeCount++;
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);
            final Thread ddl = new Thread(() -> {
                try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                    if (isReplacement) {
                        execute("RENAME TABLE x TO x_replaced", context);
                        execute("RENAME TABLE x2 TO x", context);
                    } else {
                        execute("ALTER TABLE x ADD COLUMN extra" + changeCount + " INT", context);
                    }
                } catch (Throwable e) {
                    error.set(e);
                } finally {
                    Path.clearThreadLocals();
                    done.countDown();
                }
            }, "view-schema-change");
            ddl.setDaemon(true);
            ddl.start();
            try {
                Assert.assertTrue("DDL must finish before compilation resumes", done.await(30, TimeUnit.SECONDS));
                ddl.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            if (error.get() != null) {
                throw new AssertionError("concurrent DDL failed", error.get());
            }
            drainWalQueue();
        }
    }
}
