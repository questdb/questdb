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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage of the per-query timeout of QWP egress, driven by the
 * pinned java-questdb-client. When the server advertises
 * {@code CAP_QUERY_TIMEOUT}, the client sends the query's remaining budget as
 * the {@code timeout_ms} field after {@code query_flags}. The server runs the
 * query under that timeout instead of {@code query.timeout} and ends an
 * over-budget query with {@code STATUS_QUERY_TIMEOUT}, keeping the connection
 * open for the next query.
 * <p>
 * The client also enforces the timeout on its own; it leaves that to a server
 * advertising the capability, and only reports the timeout itself after a grace
 * period (5 seconds by default). So a timeout that arrives with the server's
 * own message ({@code "timeout, query aborted"}) well within that grace proves
 * the server applied the client's timeout.
 */
public class QwpEgressQueryTimeoutTest extends AbstractReusedServerQwpEgressTest {

    @Test
    public void testClientTimeoutBoundsStatementWaitingForBusyWriter() throws Exception {
        // ALTER on a table whose writer someone else holds queues behind it. With a
        // client timeout the server waits for the writer only that long, then ends
        // the statement with STATUS_QUERY_TIMEOUT instead of its own writer limits.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE busy (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                CairoEngine engine = serverMain.getEngine();
                try (
                        QwpQueryClient client = connectClient();
                        TableWriter ignored = engine.getWriter(engine.verifyTableName("busy"), "test")
                ) {
                    RecordingHandler handler = new RecordingHandler();
                    long startNanos = System.nanoTime();
                    client.execute("ALTER TABLE busy ADD COLUMN y INT", null, handler, false, 1_000);
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                    Assert.assertEquals("expected a query timeout, got: " + handler.errorMessage,
                            QwpConstants.STATUS_QUERY_TIMEOUT, handler.errorStatus);
                    TestUtils.assertContains(handler.errorMessage, "may still be applied");
                    Assert.assertTrue("the wait must last about the timeout, took " + elapsedMs + "ms",
                            elapsedMs >= 900 && elapsedMs < 5_000);
                    Assert.assertFalse(client.hasTerminalFailure());
                }
            }
        });
    }

    @Test
    public void testClientTimeoutEndsQueryAndKeepsConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain _ = startServerWithRetry(
                    PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100"
            )) {
                try (QwpQueryClient client = connectClient()) {
                    QwpServerInfo info = client.getServerInfo();
                    Assert.assertNotEquals("the server must advertise the per-query timeout",
                            0, info.getCapabilities() & QwpEgressMsgKind.CAP_QUERY_TIMEOUT);

                    RecordingHandler handler = new RecordingHandler();
                    long startNanos = System.nanoTime();
                    client.execute("sleep(10)", null, handler, false, 1_000);
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                    Assert.assertEquals("expected a query timeout, got: " + handler.errorMessage,
                            QwpConstants.STATUS_QUERY_TIMEOUT, handler.errorStatus);
                    // The server's own message: the client's grace period (5s) had not
                    // run out, so the client did not report the timeout itself.
                    TestUtils.assertContains(handler.errorMessage, "timeout, query aborted");
                    Assert.assertTrue("sleep(10) must end near the 1s client timeout, took " + elapsedMs + "ms",
                            elapsedMs >= 900 && elapsedMs < 4_000);

                    // The authenticated connection stays open and runs the next query.
                    Assert.assertFalse(client.hasTerminalFailure());
                    RecordingHandler next = new RecordingHandler();
                    client.execute("SELECT x FROM long_sequence(3)", next);
                    Assert.assertTrue("the next query must complete: " + next.errorMessage, next.isEnded);
                    Assert.assertEquals(3, next.rowCount);
                    Assert.assertEquals("the connection must not have been replaced", 0, next.failoverResetCount);
                }
            }
        });
    }

    @Test
    public void testClientTimeoutMayExceedServerQueryTimeout() throws Exception {
        // Like the Statement-Timeout header of /exec, the client's timeout replaces
        // query.timeout for its query, even when it is longer.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain _ = startServerWithRetry(
                    PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "1s",
                    PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100"
            )) {
                try (QwpQueryClient client = connectClient()) {
                    RecordingHandler handler = new RecordingHandler();
                    client.execute("sleep(2)", null, handler, false, 30_000);
                    Assert.assertTrue("sleep(2) must outlive query.timeout=1s under a 30s client timeout: "
                            + handler.errorMessage, handler.isEnded);
                    Assert.assertEquals(0, handler.errorStatus);
                }
            }
        });
    }

    @Test
    public void testFacadeReportsServerTimeoutAsTimeout() throws Exception {
        // Through the QuestDB facade the server's STATUS_QUERY_TIMEOUT surfaces as a
        // QueryException whose isTimeout() holds, and the pooled handle runs the
        // next query on the same connection.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain _ = startServerWithRetry(
                    PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100"
            )) {
                try (
                        QuestDB db = QuestDB.connect("ws::addr=127.0.0.1:" + HTTP_PORT
                                + ";sender_pool_min=0;query_pool_min=1;query_pool_max=1;");
                        Query query = db.borrowQuery()
                ) {
                    RecordingHandler handler = new RecordingHandler();
                    query.sql("sleep(10)").handler(handler).timeout(1, TimeUnit.SECONDS);
                    try {
                        query.submit().await();
                        Assert.fail("sleep(10) must time out");
                    } catch (QueryException e) {
                        Assert.assertTrue("expected a timeout, got status=" + e.getStatus() + ": " + e.getMessage(),
                                e.isTimeout());
                        TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                    }

                    RecordingHandler next = new RecordingHandler();
                    query.sql("SELECT x FROM long_sequence(2)").handler(next).submit().await();
                    Assert.assertTrue(next.isEnded);
                    Assert.assertEquals(2, next.rowCount);
                    Assert.assertEquals(0, next.failoverResetCount);
                }
            }
        });
    }

    @Test
    public void testServerQueryTimeoutAppliesAgainToNextQuery() throws Exception {
        // The client's timeout covers its own query only: the next query on the
        // same connection, sent without one, runs under query.timeout again and
        // reports its timeout with the status older clients know.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain _ = startServerWithRetry(
                    PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "1s",
                    PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100"
            )) {
                try (QwpQueryClient client = connectClient()) {
                    RecordingHandler longer = new RecordingHandler();
                    client.execute("sleep(2)", null, longer, false, 30_000);
                    Assert.assertTrue("the first query runs under its own 30s timeout: " + longer.errorMessage,
                            longer.isEnded);

                    RecordingHandler next = new RecordingHandler();
                    client.execute("sleep(2)", null, next, false, 0);
                    Assert.assertEquals("query.timeout=1s must apply again: " + next.errorMessage,
                            QwpConstants.STATUS_LIMIT_EXCEEDED, next.errorStatus);
                    TestUtils.assertContains(next.errorMessage, "timeout, query aborted");
                }
            }
        });
    }

    @Test
    public void testStatementCompletingPastClientTimeoutReportsExecDone() throws Exception {
        // INSERT AS SELECT, UPDATE and CREATE TABLE AS SELECT run to completion inside
        // execute(): while they write, the engine checks only for cancellation. One that
        // outlives the client's timeout has taken effect, so the server must answer
        // EXEC_DONE rather than STATUS_QUERY_TIMEOUT, or a client retry would apply it
        // twice. CROSS JOIN sleep(0.3) makes each statement overrun the 100ms timeout.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                try (QwpQueryClient client = connectClient()) {
                    RecordingHandler handler = assertExecDonePastTimeout(client,
                            "INSERT INTO t SELECT x, x::timestamp FROM long_sequence(3) CROSS JOIN sleep(0.3)");
                    Assert.assertEquals(3, handler.rowsAffected);
                    serverMain.assertSql("SELECT count() FROM t", "count\n3\n");

                    handler = assertExecDonePastTimeout(client, "UPDATE t SET x = x + 10 FROM sleep(0.3)");
                    Assert.assertEquals(3, handler.rowsAffected);
                    serverMain.assertSql("SELECT sum(x) FROM t", "sum\n36\n");

                    assertExecDonePastTimeout(client, "CREATE TABLE c AS (SELECT x FROM t CROSS JOIN sleep(0.3))");
                    serverMain.assertSql("SELECT count() FROM c", "count\n3\n");
                }
            }
        });
    }

    @Test
    public void testStatementTimingOutOnBusyWriterReportsLimitExceeded() throws Exception {
        // Without a client timeout, a statement that times out waiting for the
        // table writer (SqlTimeoutException) reports STATUS_LIMIT_EXCEEDED -- a
        // server-side limit -- rather than a parse error.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startEgressServer()) {
                serverMain.execute("CREATE TABLE busy (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                CairoEngine engine = serverMain.getEngine();
                try (
                        QwpQueryClient client = connectClient();
                        TableWriter ignored = engine.getWriter(engine.verifyTableName("busy"), "test")
                ) {
                    RecordingHandler handler = new RecordingHandler();
                    client.execute("ALTER TABLE busy ADD COLUMN y INT", null, handler, false, 0);
                    Assert.assertEquals("expected a server limit, got: " + handler.errorMessage,
                            QwpConstants.STATUS_LIMIT_EXCEEDED, handler.errorStatus);
                    TestUtils.assertContains(handler.errorMessage, "Timeout expired on waiting for the async command");
                }
            }
        });
    }

    private static RecordingHandler assertExecDonePastTimeout(QwpQueryClient client, String sql) {
        final long timeoutMs = 100;
        RecordingHandler handler = new RecordingHandler();
        long startNanos = System.nanoTime();
        client.execute(sql, null, handler, false, timeoutMs);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        // Without an overrun the test would pass vacuously.
        Assert.assertTrue("the statement must outlive the timeout, took " + elapsedMs + "ms", elapsedMs > timeoutMs);
        Assert.assertNull("a completed statement must not fail: " + handler.errorMessage, handler.errorMessage);
        Assert.assertTrue("expected EXEC_DONE for: " + sql, handler.isExecDone);
        Assert.assertFalse(client.hasTerminalFailure());
        return handler;
    }

    private static QwpQueryClient connectClient() {
        QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";");
        try {
            client.connect();
        } catch (Throwable th) {
            client.close();
            throw th;
        }
        return client;
    }

    private static final class RecordingHandler implements QwpColumnBatchHandler {
        String errorMessage;
        byte errorStatus;
        int failoverResetCount;
        boolean isEnded;
        boolean isExecDone;
        long rowCount;
        long rowsAffected;

        @Override
        public void onBatch(QwpColumnBatch batch) {
            rowCount += batch.getRowCount();
        }

        @Override
        public void onEnd(long totalRows) {
            isEnded = true;
        }

        @Override
        public void onError(byte status, String message) {
            errorStatus = status;
            errorMessage = message;
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            isExecDone = true;
            this.rowsAffected = rowsAffected;
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            failoverResetCount++;
        }
    }
}
