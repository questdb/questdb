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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.griffin.SqlExecutionSuspension;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * RESUME, ALTER ... WITH BASE, REFRESH and a view ADD COLUMN wait inside the statement on a server whose network pool
 * has one worker, which serves pg-wire, HTTP and QWP. While a statement that runs on a network fiber waits for the view
 * latch that the test holds, it leaves the worker to other connections: a SELECT 1 on a new connection returns before
 * the statement has an outcome. The statement completes once the test frees the latch, and its connection keeps
 * working. A view ADD COLUMN right after ALTER ... AS waits the same way for the definition event, which a server
 * without a WAL apply job leaves in the view's WAL until the test applies it.
 * <p>
 * The statement cannot end before the test frees the latch, since its timeout is 120 s; every SELECT 1 waits at most
 * {@link #GUARD_MILLIS}, which a statement that held the worker would make it hit.
 */
public class MatViewStatementWaitFiberTest extends AbstractBootstrapTest {
    // a view ADD COLUMN of the column x that X_SQL produces, which waits for an ALTER ... AS to X_SQL
    private static final String ADD_X = "ALTER MATERIALIZED VIEW v ADD COLUMN x DOUBLE";
    private static final String BASE2_ROWS = "x,10.0\ny,20.0\n";
    private static final String BASE_ROWS = "a,1.0\nb,2.0\n";
    private static final long GUARD_MILLIS = 30_000;
    private static final String RESUME = "REFRESH MATERIALIZED VIEW v RESUME SKIP BACKLOG";
    private static final int RESUME_VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String VIEW_SQL_BASE2 = "SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h";
    // a SQL that also produces the column x, the average price of the hour, which the view lacks
    private static final String X_SQL = "SELECT ts, sym, last(price) AS price, avg(price) AS x FROM base SAMPLE BY 1h";

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(PropertyKey.SHARED_WORKER_COUNT + "=1"));
        dbPath.parent().$();
    }

    @Test
    public void testAlterWithBaseWaitingForLatchLeavesWorkerFree() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s")) {
                createView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final String alter = "ALTER MATERIALIZED VIEW v WITH BASE base2 AS (" + VIEW_SQL_BASE2 + ")";
                final String outcome = runWaitingStatement(engine, state, () -> {
                    try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                        statement.execute(alter);
                        return "OK " + queryRows(statement, "SELECT count() FROM base2");
                    }
                });
                Assert.assertEquals("OK 2\n", outcome);
                awaitViewWalApplied(engine);
                Assert.assertTrue(state.isInvalid());
                Assert.assertEquals("base table changed", state.getInvalidationReasonForReport());
                Assert.assertEquals("base2", engine.getDependentViewGraph().getViewDefinition(viewToken(engine)).getBaseTableName());
                assertViewRows(BASE_ROWS);
            }
        });
    }

    @Test
    public void testCancelReachesResumeWaitingOnSingleWorker() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s")) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final long viewSeqTxn = viewSeqTxn(engine);
                final AtomicReference<Statement> waitingStatement = new AtomicReference<>();
                final CountDownLatch isWaiting = new CountDownLatch(1);
                holdLatch(state);
                final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
                final StatementThread waiter = new StatementThread(() -> {
                    try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                        waitingStatement.set(statement);
                        statement.execute(RESUME);
                        return "OK";
                    }
                });
                final Thread canceller = new Thread(() -> {
                    try {
                        waitingStatement.get().cancel();
                    } catch (SQLException ignore) {
                        // the outcome of the waiting statement tells
                    }
                }, "resume-canceller");
                try {
                    Assert.assertTrue("RESUME did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                    canceller.start();
                    final String outcome = waiter.awaitOutcome();
                    Assert.assertNotNull("the cancel did not reach the waiting RESUME", outcome);
                    TestUtils.assertContains(outcome, "cancelled by user");
                    Assert.assertTrue(state.isLocked());
                } finally {
                    state.setOnResumeLatchBusyForTesting(null);
                    releaseLatch(engine, state, isLatchHeld);
                    waiter.join();
                    canceller.join(2 * GUARD_MILLIS);
                }
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
                Assert.assertTrue(state.isInvalid());
                Assert.assertEquals("base table changed", state.getInvalidationReasonForReport());

                // run again, RESUME succeeds
                try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                    statement.execute(RESUME);
                }
                Assert.assertFalse(state.isInvalid());
                assertViewOnBase2();
            }
        });
    }

    @Test
    public void testClientDisconnectStopsAddColumnParkedOnPendingDefinition() throws Exception {
        // A view ADD COLUMN right after ALTER ... AS parks on the single network worker while the definition event
        // waits in the view's WAL: a SELECT 1 on a new connection returns meanwhile. Once its client disconnects, the
        // statement stops, returns its compiler and the view's sequencer metadata, and writes nothing. The same ADD
        // COLUMN succeeds once the test applied the definition event.
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s", PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false")) {
                final CairoEngine engine = serverMain.getEngine();
                createViewWithPendingDefinition(engine);
                final MatViewState state = viewState(engine);
                final long viewSeqTxn = viewSeqTxn(engine);
                final AtomicReference<Connection> waitingConnection = new AtomicReference<>();
                final CountDownLatch isWaiting = new CountDownLatch(1);
                state.setOnDefinitionWaitForTesting(isWaiting::countDown);
                try (SequencerMetadataCounter metadataCounter = new SequencerMetadataCounter(engine, viewToken(engine))) {
                    final StatementThread waiter = new StatementThread(() -> {
                        final Connection connection = pgConnection();
                        waitingConnection.set(connection);
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(ADD_X);
                            return "OK";
                        }
                    });
                    try {
                        Assert.assertTrue("ADD COLUMN did not reach the definition wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                        assertWorkerFree(waiter);
                        Assert.assertEquals(1, metadataCounter.get());
                        waitingConnection.get().close();
                        // the statement stops, and returns its compiler and the view's sequencer metadata
                        TestUtils.assertEventually(() -> {
                            Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
                            Assert.assertEquals(0, metadataCounter.get());
                        }, (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
                    } finally {
                        state.setOnDefinitionWaitForTesting(null);
                        waiter.join();
                    }
                }
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
                TestUtils.drainWalQueue(engine);
                Assert.assertEquals(X_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken(engine)).getMatViewSql());
                assertViewColumns("ts\nsym\nprice\n");

                try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                    statement.execute(ADD_X);
                }
                TestUtils.drainWalQueue(engine);
                assertViewColumns("ts\nsym\nprice\nx\n");
            }
        });
    }

    @Test
    public void testClientDisconnectStopsParkedResume() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s")) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final long viewSeqTxn = viewSeqTxn(engine);
                final AtomicReference<Connection> waitingConnection = new AtomicReference<>();
                final CountDownLatch isWaiting = new CountDownLatch(1);
                holdLatch(state);
                final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
                final StatementThread waiter = new StatementThread(() -> {
                    final Connection connection = pgConnection();
                    waitingConnection.set(connection);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(RESUME);
                        return "OK";
                    }
                });
                try {
                    Assert.assertTrue("RESUME did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                    Assert.assertTrue(engine.getSqlCompilerPool().getBusyCount() > 0);
                    waitingConnection.get().close();
                    // the statement stops and returns its compiler
                    TestUtils.assertEventually(() -> Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount()), (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
                    Assert.assertTrue(state.isLocked());
                } finally {
                    state.setOnResumeLatchBusyForTesting(null);
                    releaseLatch(engine, state, isLatchHeld);
                    waiter.join();
                }
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
                Assert.assertTrue(state.isInvalid());
                Assert.assertEquals("base table changed", state.getInvalidationReasonForReport());

                try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                    statement.execute(RESUME);
                }
                Assert.assertFalse(state.isInvalid());
                assertViewOnBase2();
            }
        });
    }

    @Test
    public void testManyWaitingStatementsLeaveTwoWorkersFree() throws Exception {
        assertManyWaitingStatementsLeaveWorkersFree(2, 2);
    }

    @Test
    public void testManyWaitingStatementsLeaveWorkerFree() throws Exception {
        assertManyWaitingStatementsLeaveWorkersFree(1, 4);
    }

    @Test
    public void testReExecutedPreparedResumeWaitsUnderFence() throws Exception {
        // A named prepared RESUME runs at Parse on its first execution, where it parks, and under pg-wire's demote
        // fence on a re-execution, which holds the role-switch read lock: no fiber may park there, and the statement
        // sleeps while it waits.
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s")) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final Properties properties = new Properties();
                properties.putAll(PG_CONNECTION_PROPERTIES);
                properties.setProperty("prepareThreshold", "1");
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, properties);
                        PreparedStatement resume = connection.prepareStatement(RESUME)
                ) {
                    final AtomicReference<String> firstMode = new AtomicReference<>();
                    final String firstOutcome = runWaitingStatement(engine, state, () -> {
                        resume.execute();
                        return "OK";
                    }, () -> firstMode.set(describeSuspension()));
                    Assert.assertEquals("OK", firstOutcome);
                    Assert.assertEquals("FIBER fiber", firstMode.get());
                    Assert.assertFalse(state.isInvalid());

                    alterWithBase(engine, "base", VIEW_SQL);
                    final AtomicReference<String> secondMode = new AtomicReference<>();
                    final CountDownLatch isWaiting = new CountDownLatch(1);
                    holdLatch(state);
                    final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                    state.setOnResumeLatchBusyForTesting(() -> {
                        secondMode.set(describeSuspension());
                        isWaiting.countDown();
                    });
                    final StatementThread waiter = new StatementThread(() -> {
                        resume.execute();
                        return "OK";
                    });
                    try {
                        Assert.assertTrue("RESUME did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                        Assert.assertEquals("BLOCKING no fiber", secondMode.get());
                        Assert.assertNull(waiter.outcome.get());
                    } finally {
                        state.setOnResumeLatchBusyForTesting(null);
                        releaseLatch(engine, state, isLatchHeld);
                        waiter.join();
                    }
                    Assert.assertEquals("OK", waiter.outcome.get());
                    Assert.assertFalse(state.isInvalid());
                    // the connection keeps working
                    try (Statement statement = connection.createStatement()) {
                        Assert.assertEquals("2\n", queryRows(statement, "SELECT count() FROM base"));
                    }
                }
            }
        });
    }

    @Test
    public void testResumeTimesOutWhileParkedOnBusyLatch() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("2s")) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final long viewSeqTxn = viewSeqTxn(engine);
                holdLatch(state);
                final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                    try {
                        statement.execute(RESUME);
                        Assert.fail("RESUME must time out while the latch is busy");
                    } catch (PSQLException e) {
                        Assert.assertNotNull(e.getServerErrorMessage());
                        TestUtils.assertEquals("materialized view is busy, retry later [view=v]", e.getServerErrorMessage().getMessage());
                        // pg-wire positions count from 1
                        Assert.assertEquals(RESUME_VIEW_NAME_POSITION + 1, e.getServerErrorMessage().getPosition());
                    }
                    Assert.assertTrue(state.isLocked());
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
                    Assert.assertTrue(state.isInvalid());
                    // the connection keeps working
                    Assert.assertEquals("2\n", queryRows(statement, "SELECT count() FROM base2"));
                } finally {
                    releaseLatch(engine, state, isLatchHeld);
                }
            }
        });
    }

    @Test
    public void testResumeWaitingForLatchLeavesWorkerFreeHttp() throws Exception {
        assertResumeLeavesWorkerFree(() -> {
            final String ddlOutcome = httpExec(RESUME);
            return ddlOutcome + ' ' + httpExec("SELECT count() AS n FROM base2");
        }, "{\"ddl\":\"OK\"} {\"query\":\"SELECT count() AS n FROM base2\",\"columns\":[{\"name\":\"n\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[2]],\"count\":1}");
    }

    @Test
    public void testResumeWaitingForLatchLeavesWorkerFreePgExtended() throws Exception {
        assertResumeLeavesWorkerFree(() -> {
            try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                statement.execute(RESUME);
                return "OK " + queryRows(statement, "SELECT count() FROM base2");
            }
        }, "OK 2\n");
    }

    @Test
    public void testResumeWaitingForLatchLeavesWorkerFreePgSimple() throws Exception {
        assertResumeLeavesWorkerFree(() -> {
            final Properties properties = new Properties();
            properties.putAll(PG_CONNECTION_PROPERTIES);
            properties.setProperty("preferQueryMode", "simple");
            try (
                    Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, properties);
                    Statement statement = connection.createStatement()
            ) {
                statement.execute(RESUME);
                return "OK " + queryRows(statement, "SELECT count() FROM base2");
            }
        }, "OK 2\n");
    }

    @Test
    public void testResumeWaitingForLatchLeavesWorkerFreeQwp() throws Exception {
        assertResumeLeavesWorkerFree(() -> {
            try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                client.connect();
                final StringBuilder outcome = new StringBuilder();
                client.execute(RESUME, new OutcomeHandler(outcome));
                client.execute("SELECT count() FROM base2", new OutcomeHandler(outcome));
                return outcome.toString();
            }
        }, "exec done rows=0;end rows=1;");
    }

    @Test
    public void testResumeWaitsOnLegacyPgWorker() throws Exception {
        // A legacy pg-wire pool runs no fiber: RESUME waits on its worker, as every wait there does, and completes
        // once the latch frees up.
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer(
                    "120s",
                    PropertyKey.PG_WORKER_COUNT.getEnvVarName(), "1",
                    PropertyKey.PG_WORKER_FIBER_ENABLED.getEnvVarName(), "false"
            )) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final AtomicReference<String> mode = new AtomicReference<>();
                final CountDownLatch isWaiting = new CountDownLatch(1);
                holdLatch(state);
                final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                state.setOnResumeLatchBusyForTesting(() -> {
                    mode.set(describeSuspension());
                    isWaiting.countDown();
                });
                final StatementThread waiter = new StatementThread(() -> {
                    try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                        statement.execute(RESUME);
                        return "OK " + queryRows(statement, "SELECT count() FROM base2");
                    }
                });
                try {
                    Assert.assertTrue("RESUME did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                    Assert.assertNotNull(mode.get());
                    Assert.assertFalse(mode.get(), mode.get().startsWith("FIBER"));
                    Assert.assertTrue(mode.get(), mode.get().endsWith(" no fiber"));
                    Assert.assertNull(waiter.outcome.get());
                } finally {
                    state.setOnResumeLatchBusyForTesting(null);
                    releaseLatch(engine, state, isLatchHeld);
                    waiter.join();
                }
                Assert.assertEquals("OK 2\n", waiter.outcome.get());
                Assert.assertFalse(state.isInvalid());
                assertViewOnBase2();
            }
        });
    }

    @Test
    public void testServerCloseAbortsAddColumnParkedOnPendingDefinition() throws Exception {
        // A server close aborts a view ADD COLUMN parked on a pending definition event without waiting for it, and its
        // client sees an error. The statement wrote nothing: after a restart with the WAL apply job, the view's WAL
        // applies the definition event, the view lacks the column, and the same ADD COLUMN takes it.
        assertMemoryLeak(() -> {
            final ServerMain serverMain = startServer("120s", PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            final long viewSeqTxn;
            boolean isServerClosed = false;
            StatementThread waiter = null;
            try {
                final CairoEngine engine = serverMain.getEngine();
                createViewWithPendingDefinition(engine);
                viewSeqTxn = viewSeqTxn(engine);
                final CountDownLatch isWaiting = new CountDownLatch(1);
                viewState(engine).setOnDefinitionWaitForTesting(isWaiting::countDown);
                waiter = new StatementThread(() -> {
                    try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                        statement.execute(ADD_X);
                        return "OK";
                    }
                });
                Assert.assertTrue("ADD COLUMN did not reach the definition wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));

                final long closeStartMillis = System.currentTimeMillis();
                serverMain.close();
                isServerClosed = true;
                final long closeMillis = System.currentTimeMillis() - closeStartMillis;
                Assert.assertTrue("server close waited for the parked ADD COLUMN: " + closeMillis + " ms", closeMillis < GUARD_MILLIS);
                final String outcome = waiter.awaitOutcome();
                Assert.assertNotNull("the client of the parked ADD COLUMN did not see the shutdown", outcome);
                Assert.assertTrue(outcome, outcome.startsWith("ERROR "));
            } finally {
                if (!isServerClosed) {
                    serverMain.close();
                }
                if (waiter != null) {
                    waiter.join();
                }
            }

            try (ServerMain restarted = startServer("120s")) {
                final CairoEngine engine = restarted.getEngine();
                awaitViewWalApplied(engine);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
                Assert.assertEquals(X_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken(engine)).getMatViewSql());
                assertViewColumns("ts\nsym\nprice\n");
                try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                    statement.execute(ADD_X);
                }
                awaitViewWalApplied(engine);
                assertViewColumns("ts\nsym\nprice\nx\n");
            }
        });
    }

    @Test
    public void testServerCloseAbortsParkedResume() throws Exception {
        assertMemoryLeak(() -> {
            final ServerMain serverMain = startServer("120s");
            MatViewState state = null;
            boolean isServerClosed = false;
            StatementThread waiter = null;
            try {
                createInvalidView(serverMain);
                state = viewState(serverMain.getEngine());
                final CountDownLatch isWaiting = new CountDownLatch(1);
                holdLatch(state);
                state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
                waiter = new StatementThread(() -> {
                    try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                        statement.execute(RESUME);
                        return "OK";
                    }
                });
                Assert.assertTrue("RESUME did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));

                final long closeStartMillis = System.currentTimeMillis();
                serverMain.close();
                isServerClosed = true;
                final long closeMillis = System.currentTimeMillis() - closeStartMillis;
                Assert.assertTrue("server close waited for the parked RESUME: " + closeMillis + " ms", closeMillis < GUARD_MILLIS);
                final String outcome = waiter.awaitOutcome();
                Assert.assertNotNull("the client of the parked RESUME did not see the shutdown", outcome);
                Assert.assertTrue(outcome, outcome.startsWith("ERROR "));
            } finally {
                if (!isServerClosed) {
                    serverMain.close();
                }
                if (waiter != null) {
                    waiter.join();
                }
                if (state != null) {
                    state.setOnResumeLatchBusyForTesting(null);
                    if (state.isLocked()) {
                        // the closed view state frees its resources once the latch is free
                        state.unlock();
                        state.tryCloseIfClosed();
                    }
                }
            }
        });
    }

    private static void alterWithBase(CairoEngine engine, String baseTableName, String viewSql) throws Exception {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER MATERIALIZED VIEW v WITH BASE " + baseTableName + " AS (" + viewSql + ")");
        }
        awaitViewWalApplied(engine);
        final MatViewState state = viewState(engine);
        Assert.assertTrue(state.isInvalid());
        Assert.assertEquals("base table changed", state.getInvalidationReasonForReport());
    }

    // The view's columns, one name a line.
    private static void assertViewColumns(String expected) throws SQLException {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            Assert.assertEquals(expected, queryRows(statement, "SELECT \"column\" FROM table_columns('v')"));
        }
    }

    // The view's rows as sym,price lines, which the hourly buckets order.
    private static void assertViewOnBase2() throws Exception {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            statement.execute("REFRESH MATERIALIZED VIEW v FULL");
        }
        TestUtils.assertEventually(() -> assertViewRows(BASE2_ROWS), (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
    }

    private static void assertViewRows(String expected) throws SQLException {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            final StringBuilder rows = new StringBuilder();
            try (ResultSet resultSet = statement.executeQuery("SELECT sym, price FROM v ORDER BY ts")) {
                while (resultSet.next()) {
                    rows.append(resultSet.getString(1)).append(',').append(resultSet.getDouble(2)).append('\n');
                }
            }
            Assert.assertEquals(expected, rows.toString());
        }
    }

    // A new pg connection runs SELECT 1 while the statement waits, which needs a free network worker.
    private static void assertWorkerFree(StatementThread waiter) throws SQLException {
        final Properties properties = new Properties();
        properties.putAll(PG_CONNECTION_PROPERTIES);
        final String guardSeconds = Long.toString(TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
        properties.setProperty("connectTimeout", guardSeconds);
        properties.setProperty("loginTimeout", guardSeconds);
        properties.setProperty("socketTimeout", guardSeconds);
        try (
                Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, properties);
                Statement statement = connection.createStatement()
        ) {
            Assert.assertEquals("1\n", queryRows(statement, "SELECT 1"));
        }
        Assert.assertNull("the statement must still wait", waiter.outcome.get());
    }

    private static void awaitViewWalApplied(CairoEngine engine) throws Exception {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken(engine));
        TestUtils.assertEventually(
                () -> Assert.assertTrue(tracker.getWriterTxn() >= tracker.getSeqTxn()),
                (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS)
        );
    }

    // Base rows a@00:10 (1.0) and b@01:10 (2.0), base2 rows x@00:20 (10.0) and y@01:20 (20.0), and a view on base
    // that the server has built.
    private static void createView(ServerMain serverMain) throws Exception {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            statement.execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            statement.execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-01T01:10:00.000000Z')");
            statement.execute("INSERT INTO base2 VALUES ('x', 10.0, '2024-01-01T00:20:00.000000Z'), ('y', 20.0, '2024-01-01T01:20:00.000000Z')");
            statement.execute("CREATE MATERIALIZED VIEW v AS (" + VIEW_SQL + ") PARTITION BY DAY");
        }
        final CairoEngine engine = serverMain.getEngine();
        final TableToken base2Token = engine.verifyTableName("base2");
        final SeqTxnTracker base2Tracker = engine.getTableSequencerAPI().getTxnTracker(base2Token);
        TestUtils.assertEventually(() -> {
            Assert.assertTrue(base2Tracker.getWriterTxn() >= base2Tracker.getSeqTxn());
            assertViewRows(BASE_ROWS);
        }, (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
    }

    // Base rows a@00:10 (1.0) and b@01:10 (2.0), a MANUAL view on base that the server built, so that no refresh writes
    // to the view's WAL, and an ALTER ... AS to X_SQL, whose definition event waits in the view's WAL: the server runs
    // no WAL apply job, and the test applies the WALs.
    private static void createViewWithPendingDefinition(CairoEngine engine) throws Exception {
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            statement.execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-01T01:10:00.000000Z')");
            TestUtils.drainWalQueue(engine);
            statement.execute("CREATE MATERIALIZED VIEW v REFRESH MANUAL AS (" + VIEW_SQL + ") PARTITION BY DAY");
            statement.execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
        }
        TestUtils.assertEventually(() -> {
            TestUtils.drainWalQueue(engine);
            assertViewRows(BASE_ROWS);
        }, (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
        try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER MATERIALIZED VIEW v AS (" + X_SQL + ")");
        }
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken(engine));
        Assert.assertEquals(tracker.getSeqTxn(), viewState(engine).getPendingDefinitionSeqTxn());
        Assert.assertTrue(tracker.getWriterTxn() < tracker.getSeqTxn());
    }

    // The view of createView(), moved to base2: invalid until RESUME SKIP BACKLOG.
    private static void createInvalidView(ServerMain serverMain) throws Exception {
        createView(serverMain);
        alterWithBase(serverMain.getEngine(), "base2", VIEW_SQL_BASE2);
    }

    // The suspension mode of the statement's wait, and whether it runs on a fiber that may park.
    private static String describeSuspension() {
        return SuspensionScope.getMode() + (SqlExecutionSuspension.currentFiber() != null ? " fiber" : " no fiber");
    }

    // Takes the view latch, as a refresh does, once the refresh job has left it.
    private static void holdLatch(MatViewState state) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertTrue(state.tryLock()), (int) TimeUnit.MILLISECONDS.toSeconds(GUARD_MILLIS));
    }

    private static String httpExec(String sql) throws Exception {
        final HttpURLConnection connection = (HttpURLConnection) URI.create(
                "http://127.0.0.1:" + HTTP_PORT + "/exec?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)
        ).toURL().openConnection();
        connection.setConnectTimeout((int) GUARD_MILLIS);
        connection.setReadTimeout((int) (4 * GUARD_MILLIS));
        try (InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
    }

    private static Connection pgConnection() throws SQLException {
        return DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
    }

    private static String queryRows(Statement statement, String sql) throws SQLException {
        final StringBuilder rows = new StringBuilder();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        rows.append(',');
                    }
                    rows.append(resultSet.getString(i));
                }
                rows.append('\n');
            }
        }
        return rows.toString();
    }

    private static void releaseLatch(CairoEngine engine, MatViewState state, AtomicBoolean isLatchHeld) {
        if (isLatchHeld.compareAndSet(true, false)) {
            MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), state.getViewDefinition().getMatViewToken(), state, false);
        }
    }

    // Runs the statement while the test holds the view latch, checks that a SELECT 1 on a new connection returns
    // while the statement waits, frees the latch and returns the statement's outcome.
    private static String runWaitingStatement(CairoEngine engine, MatViewState state, StatementCall call) throws Exception {
        return runWaitingStatement(engine, state, call, null);
    }

    private static String runWaitingStatement(CairoEngine engine, MatViewState state, StatementCall call, Runnable onWait) throws Exception {
        final long viewSeqTxn = viewSeqTxn(engine);
        final CountDownLatch isWaiting = new CountDownLatch(1);
        holdLatch(state);
        final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
        state.setOnResumeLatchBusyForTesting(() -> {
            if (onWait != null) {
                onWait.run();
            }
            isWaiting.countDown();
        });
        final StatementThread waiter = new StatementThread(call);
        try {
            Assert.assertTrue("the statement did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
            assertWorkerFree(waiter);
            Assert.assertEquals(viewSeqTxn, viewSeqTxn(engine));
        } finally {
            state.setOnResumeLatchBusyForTesting(null);
            releaseLatch(engine, state, isLatchHeld);
            waiter.join();
        }
        return waiter.outcome.get();
    }

    private static ServerMain startServer(String queryTimeout, String... envNameValuePairs) {
        final HashMap<String, String> env = new HashMap<>();
        env.put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), queryTimeout);
        for (int i = 0; i < envNameValuePairs.length; i += 2) {
            env.put(envNameValuePairs[i], envNameValuePairs[i + 1]);
        }
        final ServerMain serverMain = ServerMain.create(root, env);
        serverMain.start();
        return serverMain;
    }

    private static long viewSeqTxn(CairoEngine engine) {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken(engine)).getSeqTxn();
    }

    private static MatViewState viewState(CairoEngine engine) {
        return engine.getMatViewStateStore().getViewState(viewToken(engine));
    }

    private static TableToken viewToken(CairoEngine engine) {
        return engine.verifyTableName("v");
    }

    // Several RESUMEs on their own connections wait for the latch that the test holds, one per network worker or more:
    // a SELECT 1 on a new connection still returns. Once the latch frees up, one RESUME resumes the view and the others
    // find it valid.
    private void assertManyWaitingStatementsLeaveWorkersFree(int workerCount, int statementCount) throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s", PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), Integer.toString(workerCount))) {
                Assert.assertEquals(workerCount, serverMain.getWorkerPoolManager().getSharedPoolNetwork().getWorkerCount());
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                holdLatch(state);
                final AtomicBoolean isLatchHeld = new AtomicBoolean(true);
                final ObjList<StatementThread> waiters = new ObjList<>(statementCount);
                try {
                    for (int i = 0; i < statementCount; i++) {
                        final CountDownLatch isWaiting = new CountDownLatch(1);
                        state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
                        waiters.add(new StatementThread(() -> {
                            try (Connection connection = pgConnection(); Statement statement = connection.createStatement()) {
                                statement.execute(RESUME);
                                return "OK " + queryRows(statement, "SELECT count() FROM base2");
                            }
                        }));
                        Assert.assertTrue("RESUME " + i + " did not reach the latch wait", isWaiting.await(GUARD_MILLIS, TimeUnit.MILLISECONDS));
                    }
                    for (int i = 0; i < statementCount; i++) {
                        assertWorkerFree(waiters.getQuick(i));
                    }
                } finally {
                    state.setOnResumeLatchBusyForTesting(null);
                    releaseLatch(engine, state, isLatchHeld);
                    for (int i = 0, n = waiters.size(); i < n; i++) {
                        waiters.getQuick(i).join();
                    }
                }
                for (int i = 0; i < statementCount; i++) {
                    Assert.assertEquals("OK 2\n", waiters.getQuick(i).outcome.get());
                }
                Assert.assertFalse(state.isInvalid());
                assertViewOnBase2();
            }
        });
    }

    private void assertResumeLeavesWorkerFree(StatementCall call, String expectedOutcome) throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = startServer("120s")) {
                createInvalidView(serverMain);
                final CairoEngine engine = serverMain.getEngine();
                final MatViewState state = viewState(engine);
                final AtomicReference<String> mode = new AtomicReference<>();
                final String outcome = runWaitingStatement(engine, state, call, () -> mode.set(describeSuspension()));
                Assert.assertEquals("FIBER fiber", mode.get());
                Assert.assertEquals(expectedOutcome, outcome);
                Assert.assertFalse(state.isInvalid());
                assertViewOnBase2();
            }
        });
    }

    @FunctionalInterface
    private interface StatementCall {
        String call() throws Exception;
    }

    // Records what the QWP client reports for a statement.
    private static final class OutcomeHandler implements QwpColumnBatchHandler {
        private final StringBuilder outcome;

        private OutcomeHandler(StringBuilder outcome) {
            this.outcome = outcome;
        }

        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
            outcome.append("end rows=").append(totalRows).append(';');
        }

        @Override
        public void onError(byte status, String message) {
            outcome.append("error ").append(message).append(';');
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            outcome.append("exec done rows=").append(rowsAffected).append(';');
        }
    }

    // Runs a statement on its own thread and keeps its outcome: the text the call returns, or "ERROR " and the
    // message of what it threw.
    private static final class StatementThread {
        private final AtomicReference<String> outcome = new AtomicReference<>();
        private final Thread thread;

        private StatementThread(StatementCall call) {
            thread = new Thread(() -> {
                try {
                    outcome.set(call.call());
                } catch (Throwable th) {
                    outcome.set("ERROR " + th.getMessage());
                }
            }, "waiting-statement");
            thread.setDaemon(true);
            thread.start();
        }

        private String awaitOutcome() throws InterruptedException {
            thread.join(GUARD_MILLIS);
            return outcome.get();
        }

        private void join() throws InterruptedException {
            thread.join(4 * GUARD_MILLIS);
            Assert.assertFalse("the statement did not return", thread.isAlive());
        }
    }
}
