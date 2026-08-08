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
import io.questdb.griffin.QueryRegistry;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.TimerCont;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * Exercises the {@code sleep(D)} SQL function over PGWire and HTTP. The function
 * parks its worker continuation through {@link TimerCont} for the
 * requested duration, freeing the carrier to serve concurrent traffic, and resumes
 * to return the current server timestamp. These tests verify:
 *
 * <ul>
 *     <li>basic timing correctness (slept for at least the requested duration);</li>
 *     <li>the carrier is actually freed during the sleep (a concurrent query on a
 *         single-worker pool returns promptly);</li>
 *     <li>a tight {@code query.timeout} aborts a long-running sleep on the next
 *         wake interval probe;</li>
 *     <li>JDBC {@code Statement.cancel()} aborts the parked sleep promptly;</li>
 *     <li>a forcibly closed PG connection while the sleep is parked does not pin
 *         the worker -- a follow-up query on a fresh connection completes within
 *         a wake-interval window;</li>
 *     <li>HTTP {@code /exec} returns a successful response after the sleep.</li>
 * </ul>
 */
public class ServerMainSleepTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(ServerMainSleepTest.class);
    private static final AtomicLong cancelFuzzSeq = new AtomicLong();

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.SHARED_WORKER_COUNT + "=1",
                PropertyKey.PG_WORKER_COUNT + "=1",
                // Tighter than the 1s production default: tests rely on a wake
                // cycle to observe timeout/cancel/connection-drop and should not
                // wait a full second per cycle.
                PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL + "=100"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testSleepAbortedWhenClientClosesConnection() throws Exception {
        assertMemoryLeak(() -> {
            // Regression guard for a runtime resource-pin bug: a parked sleep() did
            // not observe a clean client disconnect. JDBC conn.close() sends a PG
            // Terminate ('X') byte then FIN; the old recv(MSG_PEEK) probe saw the
            // buffered 'X', reported the socket alive, and the parked sleep kept
            // running and pinned the connection until query.timeout fired.
            //
            // query.timeout is set well above the 100ms wake interval so the two
            // outcomes are far apart: prompt disconnect detection ends the query
            // within a few wake intervals; the masked-disconnect bug ends it only
            // when the timeout trips.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "8s");
                put(PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100");
            }})) {
                serverMain.start();

                final QueryRegistry registry = serverMain.getEngine().getQueryRegistry();
                final String sleepSql = "sleep(3600)";
                final long timerShardsBefore = serverMain.getEngine().getTimerShards().size();
                // Capture the sleep's query id at registration; the text is stable at
                // that instant, so we avoid reading the pooled query sink concurrently
                // during the poll loop.
                final AtomicLong sleepQueryId = new AtomicLong(Long.MIN_VALUE);
                registry.setListener((query, queryId, executionContext) -> {
                    if (Chars.contains(query, sleepSql)) {
                        sleepQueryId.compareAndSet(Long.MIN_VALUE, queryId);
                    }
                });
                try {
                    CountDownLatch sleepStarted = new CountDownLatch(1);
                    AtomicReference<Connection> connRef = new AtomicReference<>();
                    Thread sleeper = new Thread(() -> {
                        try {
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            connRef.set(conn);
                            try (Statement stmt = conn.createStatement()) {
                                sleepStarted.countDown();
                                stmt.executeQuery(sleepSql);
                            }
                        } catch (Throwable ignored) {
                            // executeQuery throws when the socket is torn down -- expected.
                        }
                    }, "sleep-conn-close");
                    sleeper.setDaemon(true);
                    sleeper.start();

                    Assert.assertTrue("sleep thread did not start", sleepStarted.await(5, TimeUnit.SECONDS));
                    // Wait until the server has registered (and parked) the sleep.
                    TestUtils.assertEventually(
                            () -> Assert.assertTrue("sleep did not register", sleepQueryId.get() != Long.MIN_VALUE),
                            10
                    );
                    final long queryId = sleepQueryId.get();
                    TestUtils.assertEventually(
                            () -> Assert.assertTrue("sleep continuation never parked",
                                    serverMain.getEngine().getTimerShards().size() > timerShardsBefore),
                            10
                    );
                    // Pin against a vacuous pass: a breaker false-positive that killed the
                    // healthy query before the disconnect would make awaitQueryEnded return
                    // ~0ms below and satisfy the prompt-abort assertion.
                    Assert.assertNotNull("sleep ended before the client disconnected", registry.getEntry(queryId));

                    connRef.get().close();

                    // Measure how long the server keeps the parked sleep alive after the
                    // client left. The entry drops from the registry when the query ends.
                    // The deadline sits above query.timeout so the wait always terminates,
                    // whether the query ends via disconnect detection (fast) or via
                    // query.timeout (slow).
                    long closeMs = System.currentTimeMillis();
                    long endedAfterMs = awaitQueryEnded(registry, queryId, closeMs);
                    sleeper.join(5_000);
                    Assert.assertFalse("sleeper thread did not terminate", sleeper.isAlive());

                    Assert.assertTrue(
                            "sleep(3600) never ended within 20s of the client closing the connection",
                            endedAfterMs >= 0
                    );
                    Assert.assertTrue(
                            "parked sleep did not abort promptly after the client closed its connection: ended "
                                    + endedAfterMs + " ms later. The server only stopped it when query.timeout fired, "
                                    + "proving it did not detect the client disconnect (masked by the buffered PG Terminate byte).",
                            endedAfterMs < 3_000
                    );
                } finally {
                    registry.setListener(null);
                }
            }
        });
    }

    @Test
    public void testSleepAbortedWhenHttpClientClosesConnection() throws Exception {
        // HTTP counterpart to testSleepAbortedWhenClientClosesConnection. Unlike PG,
        // an HTTP client closes with a bare FIN (no protocol "goodbye" byte), so the
        // breaker's connection probe on the next wake sees the hangup and aborts the
        // parked sleep within a wake interval -- detected by the old peek probe too, so
        // HTTP is NOT masked; this test locks in that prompt detection.
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
                put(PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL.getEnvVarName(), "100");
            }})) {
                serverMain.start();

                final QueryRegistry registry = serverMain.getEngine().getQueryRegistry();
                final String sleepSql = "sleep(3600)";
                final long timerShardsBefore = serverMain.getEngine().getTimerShards().size();
                final AtomicLong sleepQueryId = new AtomicLong(Long.MIN_VALUE);
                registry.setListener((query, queryId, executionContext) -> {
                    if (Chars.contains(query, sleepSql)) {
                        sleepQueryId.compareAndSet(Long.MIN_VALUE, queryId);
                    }
                });
                try {
                    try (Socket sock = new Socket()) {
                        sock.connect(new InetSocketAddress("127.0.0.1", HTTP_PORT), 5_000);
                        // Raw GET so the close below is a plain FIN with nothing buffered
                        // on the server side -- the clean-close shape a well-behaved HTTP
                        // client produces (no PG-style Terminate byte to mask the EOF).
                        final String request = "GET /exec?query=" + URLEncoder.encode(sleepSql, StandardCharsets.UTF_8)
                                + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
                        OutputStream os = sock.getOutputStream();
                        os.write(request.getBytes(StandardCharsets.US_ASCII));
                        os.flush();

                        // Wait until the server has registered (and parked) the sleep.
                        TestUtils.assertEventually(
                                () -> Assert.assertTrue("sleep did not register", sleepQueryId.get() != Long.MIN_VALUE),
                                10
                        );
                        final long queryId = sleepQueryId.get();
                        TestUtils.assertEventually(
                                () -> Assert.assertTrue("sleep continuation never parked",
                                        serverMain.getEngine().getTimerShards().size() > timerShardsBefore),
                                10
                        );
                        Assert.assertNotNull("sleep ended before the client disconnected", registry.getEntry(queryId));

                        // Bare FIN: the server has not written a response yet, so the
                        // client's receive buffer is empty and close() sends a graceful
                        // FIN, not RST.
                        sock.close();

                        long closeMs = System.currentTimeMillis();
                        long endedAfterMs = awaitQueryEnded(registry, queryId, closeMs);

                        Assert.assertTrue(
                                "sleep(3600) never ended within 20s of the HTTP client closing the connection",
                                endedAfterMs >= 0
                        );
                        Assert.assertTrue(
                                "parked sleep did not abort promptly after the HTTP client closed its connection: ended "
                                        + endedAfterMs + " ms later.",
                                endedAfterMs < 3_000
                        );
                    }
                } finally {
                    registry.setListener(null);
                }
            }
        });
    }

    @Test
    public void testSleepCancelledByConnectionDrop() throws Exception {
        assertMemoryLeak(() -> {
            // Two-phase test on a single-worker pool with sleep(60) parked:
            //   1. While sleep is parked but the connection is still alive, a
            //      concurrent SELECT 1 on a fresh connection must complete
            //      promptly -- proves the carrier is freed (TimerCont, not
            //      Os.sleep).
            //   2. After force-closing the sleeping connection from outside the
            //      executing thread, the sleep thread must exit within a
            //      wake-interval window -- proves the server-side breaker detects
            //      the broken FD and aborts the sleep.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "120s");
            }})) {
                serverMain.start();

                CountDownLatch sleepStarted = new CountDownLatch(1);
                AtomicReference<Connection> sleepConnRef = new AtomicReference<>();
                AtomicReference<Throwable> sleepOutcome = new AtomicReference<>();
                Thread sleeper = new Thread(() -> {
                    try {
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        sleepConnRef.set(conn);
                        try (Statement stmt = conn.createStatement()) {
                            sleepStarted.countDown();
                            stmt.executeQuery("sleep(60)");
                        }
                    } catch (Throwable t) {
                        sleepOutcome.set(t);
                    }
                }, "sleep-conn-drop");
                sleeper.setDaemon(true);
                sleeper.start();

                Assert.assertTrue("sleep thread did not start", sleepStarted.await(5, TimeUnit.SECONDS));
                // Give the server enough time to mount the cont and park.
                Thread.sleep(300);

                // Phase 1: while sleep is parked, the single worker must still serve
                // a concurrent query promptly. If sleep were pinning the carrier
                // (e.g. Os.sleep), this would block until query_timeout.
                long probeStart = System.currentTimeMillis();
                try (
                        Connection probeConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement probeStmt = probeConn.createStatement();
                        ResultSet rs = probeStmt.executeQuery("SELECT 1")
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt(1));
                }
                long probeElapsed = System.currentTimeMillis() - probeStart;
                Assert.assertTrue(
                        "concurrent SELECT 1 took too long: " + probeElapsed + " ms (worker likely pinned by sleep)",
                        probeElapsed < 1_000
                );

                // Phase 2: force-close the sleeping connection from outside its
                // executing thread. The PG driver tears the socket down; the
                // server's next wake probe sees the broken FD via the SQL circuit
                // breaker and unwinds the body.
                Connection conn = sleepConnRef.get();
                Assert.assertNotNull(conn);
                try {
                    conn.close();
                } catch (SQLException ignored) {
                    // expected if the client side observes the in-flight query
                }

                // The sleep thread either gets a connection-closed PSQLException
                // (typical) or completes silently if the client tore down before any
                // server response was buffered. Either way we don't want a 60s wait.
                sleeper.join(5_000);
                Assert.assertFalse("sleep thread is still alive after connection drop", sleeper.isAlive());
            }
        });
    }

    @Test
    public void testSleepCancelledByQueryTimeout() throws Exception {
        assertMemoryLeak(() -> {
            // Tight query timeout vs. long sleep: the breaker trips on the next wake
            // interval probe and the sleep returns a timeout error to the client.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "200ms");
            }})) {
                serverMain.start();

                long t0 = System.currentTimeMillis();
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    try {
                        stmt.executeQuery("sleep(60)");
                        Assert.fail("expected PSQLException for query timeout");
                    } catch (PSQLException expected) {
                        // good -- breaker tripped on the wake-interval probe
                    }
                }
                long elapsed = System.currentTimeMillis() - t0;
                // 200 ms timeout + ~100 ms wake interval == abort by ~300 ms.
                Assert.assertTrue("sleep returned too quickly: " + elapsed + " ms", elapsed >= 150);
                Assert.assertTrue("sleep returned too slowly: " + elapsed + " ms", elapsed < 2_000);
            }
        });
    }

    @Test
    public void testSleepCancelledByStatementCancel() throws Exception {
        assertMemoryLeak(() -> {
            // Statement.cancel() sends a PG CancelRequest. The server trips the
            // circuit breaker, the parked cont resumes on the next wake interval,
            // observes the trip on the breaker probe, and throws.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "120s");
            }})) {
                serverMain.start();

                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    CountDownLatch sleepStarted = new CountDownLatch(1);
                    AtomicReference<Throwable> outcome = new AtomicReference<>();
                    Thread sleeper = new Thread(() -> {
                        try {
                            sleepStarted.countDown();
                            stmt.executeQuery("sleep(60)");
                            outcome.set(new AssertionError("expected cancellation"));
                        } catch (PSQLException expected) {
                            // good
                        } catch (Throwable t) {
                            outcome.set(t);
                        }
                    }, "sleep-stmt-cancel");
                    sleeper.setDaemon(true);
                    sleeper.start();

                    Assert.assertTrue("sleep did not start", sleepStarted.await(5, TimeUnit.SECONDS));
                    Thread.sleep(300);

                    long t0 = System.currentTimeMillis();
                    stmt.cancel();
                    sleeper.join(5_000);
                    long elapsed = System.currentTimeMillis() - t0;

                    Assert.assertFalse("sleep thread did not exit after cancel", sleeper.isAlive());
                    if (outcome.get() != null) {
                        throw new AssertionError("sleep cancellation failed", outcome.get());
                    }
                    Assert.assertTrue(
                            "cancel took too long to take effect: " + elapsed + " ms",
                            elapsed < 2_000
                    );
                }
            }
        });
    }

    @Test
    public void testSleepFreesWorkerForConcurrentQueries() throws Exception {
        assertMemoryLeak(() -> {
            // Single-worker pool. Issuing sleep(2) on one connection must not pin the
            // worker; a concurrent SELECT on a different connection must return well
            // inside the sleep window. This is the load-bearing behaviour test for
            // TimerCont vs. Os.sleep.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                CountDownLatch sleepStarted = new CountDownLatch(1);
                AtomicReference<Throwable> sleepOutcome = new AtomicReference<>();
                Thread sleeper = new Thread(() -> {
                    try (
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement stmt = conn.createStatement()
                    ) {
                        sleepStarted.countDown();
                        try (ResultSet rs = stmt.executeQuery("sleep(2)")) {
                            Assert.assertTrue(rs.next());
                            Assert.assertNotNull(rs.getTimestamp(1));
                        }
                    } catch (Throwable t) {
                        sleepOutcome.set(t);
                    }
                }, "sleep-park");
                sleeper.setDaemon(true);
                sleeper.start();

                Assert.assertTrue("sleep did not start", sleepStarted.await(5, TimeUnit.SECONDS));
                Thread.sleep(300);

                // Worker must serve this concurrent query well within the 2s sleep.
                long probeStart = System.currentTimeMillis();
                try (
                        Connection probeConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement probeStmt = probeConn.createStatement();
                        ResultSet rs = probeStmt.executeQuery("SELECT 1")
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt(1));
                }
                long probeElapsed = System.currentTimeMillis() - probeStart;
                Assert.assertTrue(
                        "concurrent SELECT 1 took too long: " + probeElapsed + " ms (worker pinned by sleep)",
                        probeElapsed < 500
                );

                sleeper.join(10_000);
                if (sleepOutcome.get() != null) {
                    throw new AssertionError("sleep thread failed", sleepOutcome.get());
                }
            }
        });
    }

    @Test
    public void testSleepReturnsApproximateTimeOnHttp() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                long t0 = System.currentTimeMillis();
                String body;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/exec").query("query", "sleep(0.4)");
                    try (HttpClient.ResponseHeaders headers = request.send()) {
                        headers.await();
                        StringSink sink = new StringSink();
                        drainResponse(headers.getResponse(), sink);
                        body = sink.toString();
                    }
                }
                long elapsed = System.currentTimeMillis() - t0;

                Assert.assertNotNull(body);
                Assert.assertFalse("response should not contain error: " + body, body.contains("\"error\""));
                // /exec response embeds the timestamp value in the dataset payload.
                Assert.assertTrue("expected dataset in response, got: " + body, body.contains("\"dataset\""));
                Assert.assertTrue(
                        "sleep over HTTP returned too quickly: " + elapsed + " ms",
                        elapsed >= 350
                );
                Assert.assertTrue(
                        "sleep over HTTP returned too slowly: " + elapsed + " ms",
                        elapsed < 5_000
                );
            }
        });
    }

    @Test
    public void testSleepReturnsApproximateTimeOnPg() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                long beforeMillis = System.currentTimeMillis();
                long returnedMillis;
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("sleep(0.5)")
                ) {
                    Assert.assertTrue(rs.next());
                    // getTimestamp via the default Calendar applies the JVM's local
                    // timezone, but the server returns UTC microseconds. Extract the
                    // value via a UTC calendar so the millis comparison is direct.
                    Timestamp returned = rs.getTimestamp(
                            1,
                            java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
                    );
                    Assert.assertNotNull(returned);
                    returnedMillis = returned.getTime();
                }
                long afterMillis = System.currentTimeMillis();
                long elapsed = afterMillis - beforeMillis;

                Assert.assertTrue(
                        "sleep returned too early: " + elapsed + " ms",
                        elapsed >= 450
                );
                Assert.assertTrue(
                        "sleep returned too late: " + elapsed + " ms",
                        elapsed < 5_000
                );
                // Returned timestamp must lie inside the sleep window (with slack
                // for clock granularity and the wire round-trip).
                Assert.assertTrue(
                        "returned timestamp before query start: returned=" + returnedMillis + " before=" + beforeMillis,
                        returnedMillis >= beforeMillis - 1_000
                );
                Assert.assertTrue(
                        "returned timestamp after query end: returned=" + returnedMillis + " after=" + afterMillis,
                        returnedMillis <= afterMillis + 1_000
                );
            }
        });
    }

    @Test(timeout = 180_000)
    public void testFuzzConcurrentSleeps() throws Exception {
        assertMemoryLeak(() -> {
            // Concurrency / load test. Spawns many client threads against a server
            // configured with multiple worker threads and multiple timer shards, each
            // running a randomly-chosen scenario from the per-test cases:
            //   - happy short sleep over PG (~50-300ms)
            //   - zero-second sleep (no shard registration; instant return)
            //   - fractional sub-wake-interval sleep (single timer chunk)
            //   - multi-wake-interval sleep (chunked re-arm path)
            //   - statement.cancel() mid-sleep (breaker trip via PG cancel request)
            //   - connection close mid-sleep (breaker trip via broken FD)
            //   - HTTP /exec sleep
            //   - tight loop of many short sleeps on a single connection
            // The shard count is set to >= 2 so register() distribution is exercised
            // and concurrent timer threads contend on different shards. The worker
            // count is set to >= 2 so two parked sleeps can each be on a different
            // carrier and we exercise concurrent scheduleResume traffic on the
            // origin pools' resume queues. Failure surfaces with the seed so it can
            // be reproduced.
            Rnd rnd = TestUtils.generateRandom(LOG);
            long seed1 = rnd.getSeed1();
            long seed0 = rnd.getSeed0();

            final int workerCount = 2;
            final int timerShardCount = 2;
            // Many more client threads than workers (>>workerCount) so the
            // parallelism check has a wide margin: with carriers pinned by
            // Os.sleep, ratio is bounded by workerCount; with TimerCont, ratio
            // can approach clientThreads. Keep the random coverage in the
            // iteration count, not the thread count: the rarer interleavings
            // come from concurrency, not from raw client threads, and 16 is
            // already 8x the worker count -- a wide enough margin for the
            // parallelism gap. Cranking the thread count higher only
            // oversubscribes small and heavily loaded CI agents (Windows in
            // particular), starving the PG accept/dispatch path badly enough
            // that a cancelled sleep's breaker trip can miss the 20s
            // registration / 30s join windows runStatementCancelFuzz asserts.
            final int totalIterations = 512;
            final int clientThreads = 16;
            final int iterationsPerThread = (totalIterations + clientThreads - 1) / clientThreads;

            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.PG_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.HTTP_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.CAIRO_TIMER_SHARDS.getEnvVarName(), String.valueOf(timerShardCount));
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                // Sanity: server bootstrapped the requested number of timer shards.
                Assert.assertNotNull(serverMain.getEngine().getTimerShards());

                final CyclicBarrier startGate = new CyclicBarrier(clientThreads);
                final CountDownLatch doneLatch = new CountDownLatch(clientThreads);
                final AtomicInteger happyCount = new AtomicInteger();
                final AtomicInteger cancelAttemptCount = new AtomicInteger();
                final AtomicInteger cancelNormalBeforeDeadlineCount = new AtomicInteger();
                final AtomicInteger cancelNormalReturnCount = new AtomicInteger();
                final AtomicInteger cancelObservedCount = new AtomicInteger();
                final AtomicInteger droppedCount = new AtomicInteger();
                // Sum of sleep durations actually completed on the server (only happy
                // scenarios; cancelled/dropped contribute nothing reliable). Compared
                // against the busy-section wall-clock to prove sleeps did NOT execute
                // serially: with proper TimerCont semantics workers are freed during
                // the sleep, so the sum of slept time grows faster than wall time.
                final AtomicLong totalSleptMillis = new AtomicLong();
                final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
                final AtomicLong busyStartMillis = new AtomicLong();
                final ConcurrentHashMap<String, CancelProbe> cancelProbes = new ConcurrentHashMap<>();
                final AtomicLong maxCancelRegistrationLatencyNs = new AtomicLong();
                final AtomicLong maxCancelToExitLatencyNs = new AtomicLong();

                serverMain.getEngine().getQueryRegistry().setListener((query, queryId, executionContext) -> {
                    final CancelProbe probe = cancelProbes.get(query.toString());
                    if (probe != null) {
                        probe.register(queryId);
                    }
                });

                try {
                    for (int i = 0; i < clientThreads; i++) {
                        final long threadSeed1 = rnd.nextLong();
                        final long threadSeed2 = rnd.nextLong();
                        final int threadId = i;
                        Thread t = new Thread(() -> {
                            Rnd tr = new Rnd(threadSeed1, threadSeed2);
                            try {
                                startGate.await();
                                // First thread past the gate stamps the busy section start.
                                busyStartMillis.compareAndSet(0L, System.currentTimeMillis());
                                for (int j = 0; j < iterationsPerThread; j++) {
                                    int scenario = tr.nextInt(8);
                                    try {
                                        switch (scenario) {
                                            case 0:
                                                runHappyPgSleep(tr.nextDouble() * 0.3, happyCount, totalSleptMillis);
                                                break;
                                            case 1:
                                                runHappyPgSleep(0.0, happyCount, totalSleptMillis);
                                                break;
                                            case 2:
                                                // Sub-wake-interval (under 100ms): single timer chunk.
                                                runHappyPgSleep(0.05 + tr.nextDouble() * 0.04, happyCount, totalSleptMillis);
                                                break;
                                            case 3:
                                                // Multi-wake-interval: chunked re-arm path.
                                                runHappyPgSleep(0.25 + tr.nextDouble() * 0.25, happyCount, totalSleptMillis);
                                                break;
                                            case 4:
                                                runStatementCancelFuzz(
                                                        cancelProbes,
                                                        cancelAttemptCount,
                                                        cancelObservedCount,
                                                        cancelNormalReturnCount,
                                                        cancelNormalBeforeDeadlineCount,
                                                        maxCancelRegistrationLatencyNs,
                                                        maxCancelToExitLatencyNs
                                                );
                                                break;
                                            case 5:
                                                runConnectionDropFuzz(tr, droppedCount);
                                                break;
                                            case 6:
                                                runHttpHappySleep(tr.nextDouble() * 0.3, happyCount, totalSleptMillis);
                                                break;
                                            case 7:
                                                runRepeatedShortSleeps(tr, happyCount, totalSleptMillis);
                                                break;
                                        }
                                    } catch (Throwable iterError) {
                                        failures.add(new AssertionError(
                                                "thread=" + threadId + " iter=" + j + " scenario=" + scenario
                                                        + "; " + iterError.getMessage(),
                                                iterError
                                        ));
                                    }
                                }
                            } catch (Throwable outer) {
                                failures.add(outer);
                            } finally {
                                doneLatch.countDown();
                            }
                        }, "sleep-fuzz-" + threadId);
                        // Platform threads (not virtual): isolates the framework under
                        // test from JEP 491 / virtual-thread monitor-handoff
                        // interactions, so any stall surfaces against the worker/timer
                        // hot paths and is not contaminated by carrier-pool semantics
                        // on the client side.
                        t.setDaemon(true);
                        t.start();
                    }

                    // Hard upper bound with plenty of headroom for slow CI scheduling;
                    // the test timeout still bounds the run.
                    Assert.assertTrue(
                            "fuzz did not complete in time, seeds=" + seed0 + "L, " + seed1 + "L, clientThreads="
                                    + clientThreads + ", iterationsPerThread=" + iterationsPerThread,
                            doneLatch.await(150, TimeUnit.SECONDS)
                    );
                    long busyEndMillis = System.currentTimeMillis();
                    long busyWallMillis = busyEndMillis - busyStartMillis.get();

                    if (!failures.isEmpty()) {
                        Throwable head = failures.peek();
                        AssertionError summary = new AssertionError(
                                "fuzz produced " + failures.size() + " failures (seeds=" + seed0 + "L, " + seed1 + "L"
                                        + ", clientThreads=" + clientThreads + ", iterationsPerThread=" + iterationsPerThread + "; first: "
                                        + head.getMessage()
                        );
                        summary.initCause(head);
                        throw summary;
                    }

                    // Final liveness check: the server must still serve a query promptly
                    // after a load of cancellations, drops and concurrent sleeps.
                    long probeStart = System.currentTimeMillis();
                    try (
                            Connection probeConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement probeStmt = probeConn.createStatement();
                            ResultSet rs = probeStmt.executeQuery("SELECT 1")
                    ) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(1, rs.getInt(1));
                    }
                    long probeElapsed = System.currentTimeMillis() - probeStart;
                    Assert.assertTrue(
                            "post-fuzz SELECT 1 took too long: " + probeElapsed + " ms (seeds=" + seed0 + "L, " + seed1 + "L)",
                            probeElapsed < 2_000
                    );

                    long sumSleptMillis = totalSleptMillis.get();
                    LOG.info().$("sleep fuzz completed [happy=").$(happyCount.get())
                            .$(", cancelAttempts=").$(cancelAttemptCount.get())
                            .$(", cancelObserved=").$(cancelObservedCount.get())
                            .$(", cancelNormal=").$(cancelNormalReturnCount.get())
                            .$(", cancelNormalBeforeDeadline=").$(cancelNormalBeforeDeadlineCount.get())
                            .$(", dropped=").$(droppedCount.get())
                            .$(", failures=").$(failures.size())
                            .$(", busyWallMs=").$(busyWallMillis)
                            .$(", sumSleptMs=").$(sumSleptMillis)
                            .$(", parallelism=").$((double) sumSleptMillis / Math.max(1, busyWallMillis))
                            .$(", cancelMaxRegistrationMs=").$(TimeUnit.NANOSECONDS.toMillis(maxCancelRegistrationLatencyNs.get()))
                            .$(", cancelMaxExitMs=").$(TimeUnit.NANOSECONDS.toMillis(maxCancelToExitLatencyNs.get()))
                            .$(", clientThreads=").$(clientThreads)
                            .$(", iterationsPerThread=").$(iterationsPerThread)
                            .$(", seeds=").$(seed0).$("L, ").$(seed1).$("L")
                            .$(']').$();

                    // Did we actually exercise each path? Probabilistic but at >=512
                    // scenario selections with 8 buckets we should hit each at least a few times.
                    Assert.assertTrue("no happy sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", happyCount.get() > 0);
                    Assert.assertTrue("no statement cancel attempts ran (seeds=" + seed0 + "L, " + seed1 + "L)", cancelAttemptCount.get() > 0);
                    Assert.assertTrue("no dropped sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", droppedCount.get() > 0);
                } finally {
                    serverMain.getEngine().getQueryRegistry().setListener(null);
                    cancelProbes.clear();
                }
            }
        });
    }

    private static long awaitQueryEnded(QueryRegistry registry, long queryId, long closeMs) {
        long deadlineMs = closeMs + 20_000;
        while (System.currentTimeMillis() < deadlineMs) {
            if (registry.getEntry(queryId) == null) {
                return System.currentTimeMillis() - closeMs;
            }
            Os.sleep(50);
        }
        return -1;
    }

    private static void drainResponse(Response response, StringSink sink) {
        Fragment fragment;
        while ((fragment = response.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
    }

    private static void runConnectionDropFuzz(Rnd tr, AtomicInteger counter) throws Exception {
        // Fire a sleep on a dedicated connection, then forcibly tear down the
        // socket from the calling thread while the runner is parked in
        // executeQuery. Uses JDBC Connection.abort(Executor) -- conn.close()
        // would deadlock because the PG driver holds the connection's monitor
        // while the runner's executeQuery is waiting for a server response.
        // abort() bypasses that lock and rips the socket down asynchronously,
        // which is exactly the scenario we want to exercise: server-side FD
        // closure mid-query.
        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread runner = new Thread(() -> {
            try (Statement stmt = conn.createStatement()) {
                started.countDown();
                // Short fuse: PG breaker doesn't proactively detect FD closure
                // when the carrier is parked, so the runner observes the abort
                // either via the server's response-write failing or by the
                // sleep returning normally and the response writer crashing.
                // Either way we want the runner unblocked quickly so the fuzz
                // budget isn't dominated by drop scenarios that the server
                // can't actually short-circuit.
                stmt.executeQuery("sleep(0.7)");
                outcome.set(new AssertionError("expected error after connection drop"));
            } catch (PSQLException expected) {
                // good
            } catch (Throwable t) {
                outcome.set(t);
            }
        }, "sleep-fuzz-drop-runner");
        runner.setDaemon(true);
        runner.start();
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        // Random delay before drop so we land in different points of the chunked
        // re-arm cycle (some before first wake, some after).
        Thread.sleep(50 + tr.nextInt(400));
        // PG JDBC's Connection.abort(executor) only schedules close() through the
        // executor; close() is synchronized on the connection's monitor, which the
        // runner holds while parked in executeQuery's blocking socket read. With
        // Runnable::run, the close runs on the calling thread and deadlocks; with
        // any executor, the close() side waits forever until the runner releases
        // the monitor, which only happens when the socket read returns. The reliable
        // unblock signal is socket closure, so we ask the driver to schedule close()
        // on a separate platform thread (it's allowed to block there) and let the
        // outer thread proceed; the socket teardown inside close() will eventually
        // wake the runner regardless.
        try {
            conn.abort(r -> {
                Thread t = new Thread(r, "sleep-fuzz-drop-abort");
                t.setDaemon(true);
                t.start();
            });
        } catch (SQLException ignored) {
        }
        runner.join(10_000);
        Assert.assertFalse("dropped sleep runner did not exit", runner.isAlive());
        counter.incrementAndGet();
    }

    private static void runHappyPgSleep(double seconds, AtomicInteger counter, AtomicLong totalSleptMillis) throws SQLException {
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("sleep(" + formatSeconds(seconds) + ")")
        ) {
            Assert.assertTrue("no row returned for sleep(" + seconds + ")", rs.next());
            Assert.assertNotNull(rs.getTimestamp(
                    1,
                    java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
            ));
        }
        counter.incrementAndGet();
        totalSleptMillis.addAndGet((long) (seconds * 1_000d));
    }

    private static void runHttpHappySleep(double seconds, AtomicInteger counter, AtomicLong totalSleptMillis) {
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
            HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
            request.GET().url("/exec").query("query", "sleep(" + formatSeconds(seconds) + ")");
            try (HttpClient.ResponseHeaders headers = request.send()) {
                headers.await();
                StringSink sink = new StringSink();
                drainResponse(headers.getResponse(), sink);
                String body = sink.toString();
                Assert.assertFalse("HTTP sleep returned error: " + body, body.contains("\"error\""));
                Assert.assertTrue("HTTP sleep missing dataset: " + body, body.contains("\"dataset\""));
            }
        }
        counter.incrementAndGet();
        totalSleptMillis.addAndGet((long) (seconds * 1_000d));
    }

    private static void runRepeatedShortSleeps(Rnd tr, AtomicInteger counter, AtomicLong totalSleptMillis) throws SQLException {
        // Tight loop on one connection; each iteration goes through TimerCont +
        // suspend + resume. Verifies the per-call entry lifecycle is clean.
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement()
        ) {
            int n = 3 + tr.nextInt(4);
            for (int i = 0; i < n; i++) {
                double secs = tr.nextDouble() * 0.1;
                try (ResultSet rs = stmt.executeQuery("sleep(" + formatSeconds(secs) + ")")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertNotNull(rs.getTimestamp(
                            1,
                            java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
                    ));
                }
                counter.incrementAndGet();
                totalSleptMillis.addAndGet((long) (secs * 1_000d));
            }
        }
    }

    private static void runStatementCancelFuzz(
            ConcurrentHashMap<String, CancelProbe> cancelProbes,
            AtomicInteger cancelAttemptCount,
            AtomicInteger cancelObservedCount,
            AtomicInteger cancelNormalReturnCount,
            AtomicInteger cancelNormalBeforeDeadlineCount,
            AtomicLong maxRegistrationLatencyNs,
            AtomicLong maxCancelToExitLatencyNs
    ) throws Exception {
        // Per-call unique sleep argument: QueryRegistry exposes the SQL text
        // verbatim to the test listener, so this lets us pick out exactly our
        // own in-flight call. An integer literal keeps the lexical form stable
        // across locales.
        final long uniqId = cancelFuzzSeq.incrementAndGet();
        final String sleepSql = "sleep(2." + (1_000_000 + uniqId) + ")";
        final CancelProbe probe = new CancelProbe(sleepSql);
        if (cancelProbes.putIfAbsent(sleepSql, probe) != null) {
            throw new AssertionError("duplicate cancel fuzz SQL [sql=" + sleepSql + "]");
        }

        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement()
        ) {
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            Thread runner = new Thread(() -> {
                probe.runnerStartNs = System.nanoTime();
                probe.runnerOutcome.set("running");
                try {
                    started.countDown();
                    try (ResultSet ignored = stmt.executeQuery(sleepSql)) {
                        // Unexpected before cancellation unless the test thread
                        // was descheduled long enough for natural completion.
                    }
                    probe.runnerExitNs = System.nanoTime();
                    probe.runnerOutcome.set("normal");
                } catch (PSQLException expected) {
                    probe.runnerExitNs = System.nanoTime();
                    probe.runnerOutcome.set("cancelled");
                } catch (Throwable t) {
                    probe.runnerExitNs = System.nanoTime();
                    probe.runnerOutcome.set("unexpected: " + t.getClass().getName() + ": " + t.getMessage());
                    outcome.set(t);
                }
            }, "sleep-fuzz-cancel-runner");
            runner.setDaemon(true);
            runner.start();
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

            // The listener observes exact registration without retaining the
            // registry's pooled query sink or polling query_activity(). It does
            // not prove that the circuit breaker flag is already bound or that
            // the PG CancelRequest will be delivered.
            Assert.assertTrue(
                    cancelProbeDiagnostics("sleep was not registered within 20s", probe),
                    probe.registered.await(20, TimeUnit.SECONDS)
            );
            updateMax(maxRegistrationLatencyNs, probe.registeredAtNs.get() - probe.runnerStartNs);

            probe.cancelCallNs = System.nanoTime();
            cancelAttemptCount.incrementAndGet();
            stmt.cancel();
            // The runner must still make forward progress after a cancel
            // attempt. A normal return is recorded as missed-cancel evidence,
            // but this fuzz test keeps master-equivalent strictness and does
            // not fail solely on that outcome.
            runner.join(30_000);
            Assert.assertFalse(cancelProbeDiagnostics("cancelled sleep runner did not exit", probe), runner.isAlive());
            updateMax(maxCancelToExitLatencyNs, probe.runnerExitNs - probe.cancelCallNs);
            if (outcome.get() != null) {
                throw new AssertionError(cancelProbeDiagnostics("statement cancel scenario failed", probe), outcome.get());
            }
            if ("cancelled".equals(probe.runnerOutcome.get())) {
                cancelObservedCount.incrementAndGet();
            } else if ("normal".equals(probe.runnerOutcome.get())) {
                cancelNormalReturnCount.incrementAndGet();
                if (isCancelBeforeNaturalDeadline(probe)) {
                    cancelNormalBeforeDeadlineCount.incrementAndGet();
                }
            } else {
                throw new AssertionError(cancelProbeDiagnostics("statement cancel scenario ended without terminal outcome", probe));
            }
        } finally {
            cancelProbes.remove(sleepSql, probe);
        }
    }

    private static String cancelProbeDiagnostics(String reason, CancelProbe probe) {
        final long registeredAtNs = probe.registeredAtNs.get();
        return reason
                + " [sql=" + probe.sleepSql
                + ", queryId=" + probe.queryId.get()
                + ", registered=" + (registeredAtNs > 0)
                + ", registrationMs=" + nanosToMillisIfKnown(registeredAtNs - probe.runnerStartNs)
                + ", cancelToExitMs=" + nanosToMillisIfKnown(probe.runnerExitNs - probe.cancelCallNs)
                + ", cancelBeforeNaturalDeadline=" + isCancelBeforeNaturalDeadline(probe)
                + ", outcome=" + probe.runnerOutcome.get()
                + ']';
    }

    private static boolean isCancelBeforeNaturalDeadline(CancelProbe probe) {
        final long registeredAtNs = probe.registeredAtNs.get();
        final long cancelCallNs = probe.cancelCallNs;
        if (registeredAtNs <= 0 || cancelCallNs <= 0) {
            return false;
        }
        final long naturalCompletionNs = registeredAtNs + parseSleepDurationNanos(probe.sleepSql);
        final long marginNs = TimeUnit.MILLISECONDS.toNanos(250);
        return cancelCallNs + marginNs < naturalCompletionNs;
    }

    private static long nanosToMillisIfKnown(long nanos) {
        return nanos > 0 ? TimeUnit.NANOSECONDS.toMillis(nanos) : -1;
    }

    private static long parseSleepDurationNanos(String sleepSql) {
        final String seconds = sleepSql.substring("sleep(".length(), sleepSql.length() - 1);
        return (long) (Double.parseDouble(seconds) * 1_000_000_000d);
    }

    private static void updateMax(AtomicLong max, long value) {
        while (true) {
            final long current = max.get();
            if (value <= current || max.compareAndSet(current, value)) {
                return;
            }
        }
    }

    private static String formatSeconds(double seconds) {
        // Stable English-locale formatting independent of JVM default Locale,
        // so commas don't sneak in on machines with German/French locales and
        // turn the decimal into a SQL parse error.
        return String.format(java.util.Locale.ROOT, "%.6f", seconds);
    }

    private static final class CancelProbe {
        private final CountDownLatch registered = new CountDownLatch(1);
        private final AtomicLong queryId = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong registeredAtNs = new AtomicLong();
        private final AtomicReference<String> runnerOutcome = new AtomicReference<>("not-started");
        private final String sleepSql;
        private volatile long cancelCallNs;
        private volatile long runnerExitNs;
        private volatile long runnerStartNs;

        private CancelProbe(String sleepSql) {
            this.sleepSql = sleepSql;
        }

        private void register(long queryId) {
            if (this.queryId.compareAndSet(Long.MIN_VALUE, queryId)) {
                registeredAtNs.set(System.nanoTime());
                registered.countDown();
            }
        }
    }
}
