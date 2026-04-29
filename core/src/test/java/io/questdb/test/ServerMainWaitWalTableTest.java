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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * Exercises the {@code wait_wal_table} function over PGWire and HTTP under
 * conditions where it MUST park its worker continuation: the engine's WAL apply
 * job is disabled at boot, so writerTxn cannot advance until
 * {@link TestUtils#drainWalQueue} drives a private apply job from the test thread.
 *
 * <p>Two scenarios per protocol:
 * <ul>
 *     <li>tiny query timeout; the wait never gets to apply, so the breaker trips
 *         on the next wake interval and the client sees an error.</li>
 *     <li>generous query timeout; another thread drives a manual WAL apply, which
 *         advances writerTxn, fires the waiter, the body resumes on a peer worker
 *         and returns true.</li>
 * </ul>
 */
public class ServerMainWaitWalTableTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.SHARED_WORKER_COUNT + "=2",
                PropertyKey.PG_WORKER_COUNT + "=2"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testWaitWalTableSucceedsAfterManualApply() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            // Long enough that the wait shouldn't time out before our manual drain.
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
        }})) {
            serverMain.start();

            // Setup: create the table and insert via a separate session that
            // doesn't suspend (wait_wal_table not called here).
            try (
                    Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = setupConn.createStatement()
            ) {
                stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            }

            // Run wait_wal_table on a separate thread. With WAL apply disabled,
            // writerTxn < seqTxn, so the function parks the worker's cont.
            CountDownLatch waitStarted = new CountDownLatch(1);
            AtomicReference<Boolean> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    waitStarted.countDown();
                    try (ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('foo')")) {
                        Assert.assertTrue(rs.next());
                        result.set(rs.getBoolean(1));
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "wait-wal-table-waiter");
            waiter.setDaemon(true);
            waiter.start();

            Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
            // Let the worker mount and park inside wait_wal_table.
            Os.sleep(100);

            // Drive WAL apply manually. This advances writerTxn, which fires the
            // waiter and schedules the parked cont onto the network pool's
            // resume queue. A network worker remounts the cont and the body
            // returns true to the client.
            TestUtils.drainWalQueue(serverMain.getEngine());

            waiter.join(5_000);
            if (failure.get() != null) {
                throw new AssertionError("wait_wal_table failed", failure.get());
            }
            Assert.assertEquals(Boolean.TRUE, result.get());
        }
    }

    @Test
    public void testWaitWalTableTimesOut() throws Exception {
        // Single-worker network pool. The whole point of suspending in
        // wait_wal_table is that the worker is freed while the query is parked,
        // so even with one worker a concurrent query on a different connection
        // must be served promptly.
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "200ms");
            put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "1");
            put(PropertyKey.PG_WORKER_COUNT.getEnvVarName(), "1");
        }})) {
            serverMain.start();

            try (
                    Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = setupConn.createStatement()
            ) {
                stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            }

            // Issue wait_wal_table on a background thread so the test main can
            // probe a concurrent query while the wait is parked.
            CountDownLatch waitStarted = new CountDownLatch(1);
            AtomicReference<Throwable> waitOutcome = new AtomicReference<>();
            AtomicReference<Long> waitElapsed = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                long t0 = System.currentTimeMillis();
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    waitStarted.countDown();
                    stmt.executeQuery("SELECT wait_wal_table('foo')");
                    waitOutcome.set(new AssertionError("expected query timeout exception"));
                } catch (PSQLException expected) {
                    // good -- breaker tripped on the wake-recheck probe
                } catch (Throwable t) {
                    waitOutcome.set(t);
                } finally {
                    waitElapsed.set(System.currentTimeMillis() - t0);
                }
            }, "wait-wal-table-pg-waiter");
            waiter.setDaemon(true);
            waiter.start();

            Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
            // Let the worker mount and park inside wait_wal_table.
            Os.sleep(100);

            // While the wait is parked, the single worker must still serve a
            // simple query on a different connection -- proving the wait is
            // actually freeing the carrier rather than blocking it.
            long probeStart = System.currentTimeMillis();
            try (
                    Connection probeConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement probeStmt = probeConn.createStatement();
                    ResultSet probeRs = probeStmt.executeQuery("SELECT 1")
            ) {
                Assert.assertTrue(probeRs.next());
                Assert.assertEquals(1, probeRs.getInt(1));
            }
            long probeElapsed = System.currentTimeMillis() - probeStart;
            Assert.assertTrue(
                    "concurrent SELECT 1 took too long: " + probeElapsed + " ms (worker likely blocked on wait_wal_table)",
                    probeElapsed < 300
            );

            waiter.join(2_000);
            if (waitOutcome.get() != null) {
                throw new AssertionError("wait_wal_table thread failed", waitOutcome.get());
            }
            Long elapsed = waitElapsed.get();
            Assert.assertNotNull(elapsed);
            // 200 ms timeout + ~200 ms wake interval == fire by ~400 ms.
            Assert.assertTrue("wait returned too quickly: " + elapsed + " ms", elapsed >= 150);
            Assert.assertTrue("wait returned too slowly: " + elapsed + " ms", elapsed < 1_000);
        }
    }

    @Test
    public void testWaitWalTableTimesOutOverHttp() throws Exception {
        // Single-worker network pool. The HTTP server runs on this pool too;
        // a parked wait_wal_table must free the worker so a concurrent /exec
        // request is served promptly.
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "200ms");
            put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "1");
            put(PropertyKey.PG_WORKER_COUNT.getEnvVarName(), "1");
        }})) {
            serverMain.start();

            // Setup via PG (simpler than scripting CREATE/INSERT over /exec).
            try (
                    Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = setupConn.createStatement()
            ) {
                stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            }

            // Issue wait_wal_table over HTTP on a background thread so we can
            // probe a concurrent query while it is parked.
            CountDownLatch waitStarted = new CountDownLatch(1);
            AtomicReference<String> waitResponse = new AtomicReference<>();
            AtomicReference<Throwable> waitOutcome = new AtomicReference<>();
            AtomicReference<Long> waitElapsed = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                long t0 = System.currentTimeMillis();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/exec").query("query", "SELECT wait_wal_table('foo')");
                    waitStarted.countDown();
                    try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
                        responseHeaders.await();
                        StringSink sink = new StringSink();
                        drainResponse(responseHeaders.getResponse(), sink);
                        waitResponse.set(sink.toString());
                    }
                } catch (Throwable t) {
                    waitOutcome.set(t);
                } finally {
                    waitElapsed.set(System.currentTimeMillis() - t0);
                }
            }, "wait-wal-table-http-waiter");
            waiter.setDaemon(true);
            waiter.start();

            Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
            // Let the worker mount and park inside wait_wal_table.
            Os.sleep(100);

            // While the wait is parked, the single worker must still serve a
            // simple HTTP query -- proving the wait is freeing the carrier.
            long probeStart = System.currentTimeMillis();
            try (HttpClient probeClient = HttpClientFactory.newPlainTextInstance()) {
                HttpClient.Request probeReq = probeClient.newRequest("localhost", HTTP_PORT);
                probeReq.GET().url("/exec").query("query", "SELECT 1");
                try (HttpClient.ResponseHeaders headers = probeReq.send()) {
                    headers.await();
                    StringSink probeSink = new StringSink();
                    drainResponse(headers.getResponse(), probeSink);
                    Assert.assertTrue(
                            "concurrent /exec returned unexpected body: " + probeSink,
                            probeSink.toString().contains("\"dataset\"")
                    );
                }
            }
            long probeElapsed = System.currentTimeMillis() - probeStart;
            Assert.assertTrue(
                    "concurrent SELECT 1 took too long: " + probeElapsed + " ms (worker likely blocked on wait_wal_table)",
                    probeElapsed < 300
            );

            waiter.join(2_000);
            if (waitOutcome.get() != null) {
                throw new AssertionError("wait_wal_table HTTP thread failed", waitOutcome.get());
            }
            String body = waitResponse.get();
            Assert.assertNotNull("no response body received", body);
            Assert.assertTrue(
                    "expected error response, got: " + body,
                    body.contains("\"error\"") || body.contains("timeout")
            );
            Long elapsed = waitElapsed.get();
            Assert.assertNotNull(elapsed);
            Assert.assertTrue("wait returned too quickly: " + elapsed + " ms", elapsed >= 150);
            Assert.assertTrue("wait returned too slowly: " + elapsed + " ms", elapsed < 1_000);
        }
    }

    @Test
    public void testWaitWalTableSucceedsAfterManualApplyOverHttp() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
        }})) {
            serverMain.start();

            // Setup table + insert via PG.
            try (
                    Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = setupConn.createStatement()
            ) {
                stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            }

            // Issue wait_wal_table over HTTP on a separate thread; with WAL apply
            // disabled the function parks the network-pool worker's cont.
            CountDownLatch waitStarted = new CountDownLatch(1);
            AtomicReference<String> responseBody = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/exec").query("query", "SELECT wait_wal_table('foo')");
                    waitStarted.countDown();
                    try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
                        responseHeaders.await();
                        StringSink sink = new StringSink();
                        drainResponse(responseHeaders.getResponse(), sink);
                        responseBody.set(sink.toString());
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "wait-wal-table-http-waiter");
            waiter.setDaemon(true);
            waiter.start();

            Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
            // Let the worker mount and park inside wait_wal_table.
            Os.sleep(100);

            // Drive WAL apply manually -- advances writerTxn, fires the waiter.
            TestUtils.drainWalQueue(serverMain.getEngine());

            waiter.join(5_000);
            if (failure.get() != null) {
                throw new AssertionError("wait_wal_table over HTTP failed", failure.get());
            }
            String body = responseBody.get();
            Assert.assertNotNull("no response body received", body);
            // Successful /exec response embeds the boolean true value in the dataset
            // payload; an error response would have "error" instead.
            Assert.assertTrue("expected success response, got: " + body, body.contains("true"));
            Assert.assertFalse("response should not contain error: " + body, body.contains("\"error\""));
        }
    }

    private static void drainResponse(Response response, StringSink sink) {
        Fragment fragment;
        while ((fragment = response.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
    }
}
