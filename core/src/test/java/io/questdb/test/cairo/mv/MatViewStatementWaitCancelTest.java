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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Statements that wait inside the engine stop when the statement is cancelled or its timeout expires, write
 * nothing, leave the view's latch free, and succeed when run again:
 * <ul>
 *     <li>ALTER ... WITH BASE waiting for the view's refresh latch;</li>
 *     <li>REFRESH ... FULL, INCREMENTAL and RANGE waiting for the view's WAL to apply a pending ALTER ... AS;</li>
 *     <li>view ADD COLUMN, DROP COLUMN, ALTER ... AS and ALTER ... WITH BASE ... AS waiting for the view's WAL to apply
 *     a pending ALTER ... AS;</li>
 *     <li>RESUME waiting for the view's WAL to apply.</li>
 * </ul>
 */
public class MatViewStatementWaitCancelTest extends AbstractCairoTest {
    // the SQL of an ALTER ... WITH BASE base2 AS of the view
    private static final String BASE2_SQL = "SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h";
    // the SQL of a view ALTER ... AS that follows NEW_SQL
    private static final String MIN_SQL = "SELECT ts, sym, min(price) AS price FROM base SAMPLE BY 1h";
    private static final String NEW_SQL = "SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h";
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String VIEW_SQL = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";

    @Test
    public void testAddColumnStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertAddColumnWaitForDefinitionStops(true);
    }

    @Test
    public void testAddColumnStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertAddColumnWaitForDefinitionStops(false);
    }

    @Test
    public void testAlterAsStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertAlterAsWaitForDefinitionStops(true);
    }

    @Test
    public void testAlterAsStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertAlterAsWaitForDefinitionStops(false);
    }

    @Test
    public void testAlterWithBaseStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertAlterWithBaseWaitForDefinitionStops(true);
    }

    @Test
    public void testAlterWithBaseStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertAlterWithBaseWaitForDefinitionStops(false);
    }

    @Test
    public void testBaseChangeStopsWhenCancelledWhileWaitingForLatch() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final String alter = "ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, max(price) AS price FROM base2 SAMPLE BY 1h)";
            Assert.assertTrue(state.tryLock());
            try {
                assertCancelled(alter, circuitBreaker -> state.setOnResumeLatchBusyForTesting(circuitBreaker::cancel));
            } finally {
                state.setOnResumeLatchBusyForTesting(null);
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken(), state, false);
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals("base", engine.getDependentViewGraph().getViewDefinition(viewToken()).getBaseTableName());

            // run again, the statement changes the base
            execute(alter);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", engine.getDependentViewGraph().getViewDefinition(viewToken()).getBaseTableName());
            Assert.assertFalse(state.isLocked());
        });
    }

    @Test
    public void testDropColumnStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertDropColumnWaitForDefinitionStops(true);
    }

    @Test
    public void testDropColumnStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertDropColumnWaitForDefinitionStops(false);
    }

    @Test
    public void testFullRefreshStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h FULL", true);
    }

    @Test
    public void testFullRefreshStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h FULL", false);
    }

    @Test
    public void testIncrementalRefreshStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL", true);
    }

    @Test
    public void testIncrementalRefreshStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL", false);
    }

    @Test
    public void testRangeRefreshStopsWhenCancelledWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-01T02:00:00.000000Z'", true);
    }

    @Test
    public void testRangeRefreshStopsWhenTimedOutWhileWaitingForDefinition() throws Exception {
        assertRefreshWaitForDefinitionStops("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-01T02:00:00.000000Z'", false);
    }

    @Test
    public void testResumeStopsWhenCancelledWhileWaitingForViewWal() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("IMMEDIATE");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            // a view WAL transaction that RESUME waits for
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            final long viewSeqTxn = viewSeqTxn();
            try {
                assertCancelled("REFRESH MATERIALIZED VIEW price_1h RESUME", circuitBreaker -> viewState().setOnResumeWalWaitForTesting(circuitBreaker::cancel));
            } finally {
                viewState().setOnResumeWalWaitForTesting(null);
            }
            Assert.assertFalse(viewState().isLocked());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");

            // run again, RESUME catches the view up
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    2024-01-01T02:00:00.000000Z\tc\t3.0\tnull
                    """);
        });
    }

    // Asserts the error of a statement that the circuit breaker stopped: a cancellation or a timeout.
    private static void assertStopped(CairoException e, boolean isCancel) {
        if (isCancel) {
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
        } else {
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_TIMEOUT, e.getInterruptionReason());
            TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
        }
    }

    // Runs the statement with a circuit breaker that the given hook trips while the statement waits; the statement
    // fails with the cancellation.
    private static void assertCancelled(String sql, Consumer<NetworkSqlExecutionCircuitBreaker> armHook) throws Exception {
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
             NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
            context.with(circuitBreaker);
            circuitBreaker.resetTimer();
            armHook.accept(circuitBreaker);
            try {
                execute(sql, context);
                Assert.fail("the statement must stop: " + sql);
            } catch (CairoException e) {
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
            }
        }
    }

    // A MANUAL view whose ALTER ... AS waits in the view's WAL, and a base row the view has not refreshed. A REFRESH
    // that stops while it waits for the definition enqueues nothing: once the definition applies and the refresh
    // job drains, the view still holds its old rows. Run again, the REFRESH writes the new SQL's rows.
    private void assertRefreshWaitForDefinitionStops(String refreshSql, boolean isCancel) throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("INSERT INTO base VALUES ('a', 3.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW price_1h AS (" + NEW_SQL + ")");
            final long viewSeqTxn = viewSeqTxn();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                viewState().setOnDefinitionWaitForTesting(isCancel
                        ? circuitBreaker::cancel
                        : () -> circuitBreaker.setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK));
                try {
                    execute(refreshSql, context);
                    Assert.fail("the statement must stop: " + refreshSql);
                } catch (CairoException e) {
                    if (isCancel) {
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                        TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                    } else {
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_TIMEOUT, e.getInterruptionReason());
                        TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                    }
                } finally {
                    viewState().setOnDefinitionWaitForTesting(null);
                }
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(NEW_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            assertViewRows(VIEW_ROWS);

            execute(refreshSql);
            drainWalAndMatViewQueues();
            // a RANGE reports no view status of its own until the next refresh
            Assert.assertFalse(viewState().isInvalid());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """.replace("\tb\t2.0", refreshSql.endsWith("INCREMENTAL") ? "\tb\t2.0" : "\tb\t20.0"));
        });
    }

    private void assertAddColumnWaitForDefinitionStops(boolean isCancel) throws Exception {
        assertViewAlterWaitForDefinitionStops("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE", isCancel, () -> {
            assertViewColumns("ts", "sym", "price", "extra");
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
        });
    }

    private void assertAlterAsWaitForDefinitionStops(boolean isCancel) throws Exception {
        assertViewAlterWaitForDefinitionStops("ALTER MATERIALIZED VIEW price_1h AS (" + MIN_SQL + ")", isCancel, () -> {
            Assert.assertEquals(MIN_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            assertViewStatus("valid", "");
        });
    }

    private void assertAlterWithBaseWaitForDefinitionStops(boolean isCancel) throws Exception {
        assertViewAlterWaitForDefinitionStops("ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (" + BASE2_SQL + ")", isCancel, () -> {
            Assert.assertEquals("base2", engine.getDependentViewGraph().getViewDefinition(viewToken()).getBaseTableName());
            Assert.assertEquals(BASE2_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            assertViewStatus("invalid", "base table changed");
        });
    }

    private void assertDropColumnWaitForDefinitionStops(boolean isCancel) throws Exception {
        assertViewAlterWaitForDefinitionStops("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price", isCancel, () -> {
            assertViewColumns("ts", "sym");
            assertViewRows("""
                    ts\tsym
                    2024-01-01T00:00:00.000000Z\ta
                    2024-01-01T01:00:00.000000Z\tb
                    """);
        });
    }

    // A MANUAL view whose ALTER ... AS waits in the view's WAL. A view ALTER that stops while it waits for the
    // definition writes nothing: once the definition applies, the view keeps its columns, base table and rows. Run
    // again, the statement succeeds, which the check asserts once the view's WAL applied it.
    private void assertViewAlterWaitForDefinitionStops(String alterSql, boolean isCancel, ViewCheck rerunCheck) throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView("MANUAL");
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW price_1h AS (" + NEW_SQL + ")");
            final long viewSeqTxn = viewSeqTxn();
            final AtomicBoolean isWaitSeen = new AtomicBoolean();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                 NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                viewState().setOnDefinitionWaitForTesting(() -> {
                    isWaitSeen.set(true);
                    if (isCancel) {
                        circuitBreaker.cancel();
                    } else {
                        circuitBreaker.setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK);
                    }
                });
                try {
                    execute(alterSql, context);
                    Assert.fail("the statement must stop: " + alterSql);
                } catch (CairoException e) {
                    assertStopped(e, isCancel);
                } finally {
                    viewState().setOnDefinitionWaitForTesting(null);
                }
            }
            Assert.assertTrue(isWaitSeen.get());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(viewState().isLocked());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(NEW_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            Assert.assertEquals("base", engine.getDependentViewGraph().getViewDefinition(viewToken()).getBaseTableName());
            assertViewColumns("ts", "sym", "price");
            assertViewRows(VIEW_ROWS);

            // run again, the statement succeeds
            execute(alterSql);
            drainWalAndMatViewQueues();
            rerunCheck.run();
            Assert.assertFalse(viewState().isLocked());
        });
    }

    private void assertViewColumns(String... expected) throws Exception {
        final StringBuilder columns = new StringBuilder("column\n");
        for (String column : expected) {
            columns.append(column).append('\n');
        }
        assertQuery("SELECT \"column\" FROM table_columns('price_1h')").noLeakCheck().noRandomAccess().returns(columns.toString());
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck().noRandomAccess().returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
    }

    // Base rows a@00:10 (1.0) and b@01:10 (2.0), and a view refreshed from them.
    private void createBaseWithRefreshedView(String refreshType) throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T01:10:00.000000Z')
                """);
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
        if (!"IMMEDIATE".equals(refreshType)) {
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(VIEW_ROWS);
        assertViewStatus("valid", "");
    }

    private static NetworkSqlExecutionCircuitBreaker newCircuitBreaker() {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        }) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private static MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private static TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }

    @FunctionalInterface
    private interface ViewCheck {
        void run() throws Exception;
    }
}
