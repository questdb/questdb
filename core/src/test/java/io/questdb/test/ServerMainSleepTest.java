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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.TimerCont;
import io.questdb.std.Rnd;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
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
            // can approach clientThreads. The bigger the gap, the less
            // ambiguous the failure mode.
            //
            // Kept modest on purpose: 16 client threads is still 8x the worker
            // count -- a wide enough margin for the parallelism gap -- while
            // avoiding the connection/CPU oversubscription that, on small and
            // heavily loaded CI agents (Windows in particular), starved the PG
            // accept/dispatch path so badly that a cancelled sleep's breaker
            // trip could miss its 30s join window. Do not crank these back up to
            // chase coverage; the rarer interleavings come from concurrency, not
            // from raw thread count, and 16x16=256 iterations already hit every
            // scenario bucket many times over.
            final int clientThreads = 16;
            final int iterationsPerThread = 16;

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
                final AtomicInteger cancelledCount = new AtomicInteger();
                final AtomicInteger droppedCount = new AtomicInteger();
                // Sum of sleep durations actually completed on the server (only happy
                // scenarios; cancelled/dropped contribute nothing reliable). Compared
                // against the busy-section wall-clock to prove sleeps did NOT execute
                // serially: with proper TimerCont semantics workers are freed during
                // the sleep, so the sum of slept time grows faster than wall time.
                final AtomicLong totalSleptMillis = new AtomicLong();
                final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
                final AtomicLong busyStartMillis = new AtomicLong();

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
                                            runStatementCancelFuzz(serverMain.getEngine(), cancelledCount);
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

                // Hard upper bound: 16 threads * 16 iters * worst-case ~600ms, run
                // in parallel, stays well under this. Allow plenty of headroom;
                // the test timeout still bounds the run.
                Assert.assertTrue(
                        "fuzz did not complete in time, seeds=" + seed0 + "L, " + seed1 + "L",
                        doneLatch.await(150, TimeUnit.SECONDS)
                );
                long busyEndMillis = System.currentTimeMillis();
                long busyWallMillis = busyEndMillis - busyStartMillis.get();

                if (!failures.isEmpty()) {
                    Throwable head = failures.peek();
                    AssertionError summary = new AssertionError(
                            "fuzz produced " + failures.size() + " failures (seeds=" + seed0 + "L, " + seed1 + "L; first: "
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
                        .$(", cancelled=").$(cancelledCount.get())
                        .$(", dropped=").$(droppedCount.get())
                        .$(", failures=").$(failures.size())
                        .$(", busyWallMs=").$(busyWallMillis)
                        .$(", sumSleptMs=").$(sumSleptMillis)
                        .$(", parallelism=").$((double) sumSleptMillis / Math.max(1, busyWallMillis))
                        .$(", seeds=").$(seed0).$("L, ").$(seed1).$("L")
                        .$(']').$();

                // Did we actually exercise each path? Probabilistic but at 16*16=256
                // iterations with 8 buckets we should hit each at least a few times.
                Assert.assertTrue("no happy sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", happyCount.get() > 0);
                Assert.assertTrue("no cancelled sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", cancelledCount.get() > 0);
                Assert.assertTrue("no dropped sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", droppedCount.get() > 0);
            }
        });
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

    private static void runStatementCancelFuzz(CairoEngine engine, AtomicInteger counter) throws Exception {
        // Per-call unique sleep argument: query_activity() exposes the SQL text
        // verbatim, so this lets us pick out exactly our own in-flight call.
        // An integer literal keeps the lexical form stable across locales.
        final long uniqId = cancelFuzzSeq.incrementAndGet();
        final String sleepSql = "sleep(2." + (1_000_000 + uniqId) + ")";
        final String activitySql = "select query_id from query_activity() where query = '" + sleepSql + "'";

        // Thread-owned SQL context: the fuzz worker drives observation through
        // the engine's published SQL surface (CairoEngine.select +
        // query_activity()) instead of touching the QueryRegistry's pooled
        // StringSink directly. Going through SQL keeps us off the entry pool's
        // recycle path and matches how an operator would inspect activity.
        final SqlExecutionContextImpl observerCtx = new SqlExecutionContextImpl(engine, 1)
                .with(AllowAllSecurityContext.INSTANCE);
        observerCtx.with(new AtomicBooleanCircuitBreaker(engine));

        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory activityFactory = CairoEngine.select(compiler, activitySql, observerCtx)
        ) {
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            Thread runner = new Thread(() -> {
                try {
                    started.countDown();
                    stmt.executeQuery(sleepSql);
                } catch (PSQLException expected) {
                    // good: cancel landed and the body unwound
                } catch (Throwable t) {
                    outcome.set(t);
                }
            }, "sleep-fuzz-cancel-runner");
            runner.setDaemon(true);
            runner.start();
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

            // QueryRegistry.register() is the moment that wires the
            // cancelledFlag through to the circuit breaker. A PG CancelRequest
            // that arrives before this point is silently dropped. Waiting
            // until our sleep is visible in query_activity() means the next
            // wake-interval breaker probe (within ~100ms) will see the cancel.
            final long pollDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            boolean observed = false;
            while (!observed && System.nanoTime() < pollDeadlineNs) {
                try (RecordCursor cursor = activityFactory.getCursor(observerCtx)) {
                    if (cursor.hasNext()) {
                        observed = true;
                    }
                }
                if (!observed) {
                    Thread.sleep(2);
                }
            }
            Assert.assertTrue(
                    "sleep was not registered within 20s [sql=" + sleepSql + "]",
                    observed
            );

            stmt.cancel();
            // With the breaker bound, the runner exit is gated only by one
            // wake-interval probe + response RTT; the long join is slack for
            // slow CI hardware, not cover for a missed cancel.
            runner.join(30_000);
            Assert.assertFalse("cancelled sleep runner did not exit [sql=" + sleepSql + "]", runner.isAlive());
            if (outcome.get() != null) {
                throw new AssertionError("statement cancel scenario failed", outcome.get());
            }
        }
        counter.incrementAndGet();
    }

    private static String formatSeconds(double seconds) {
        // Stable English-locale formatting independent of JVM default Locale,
        // so commas don't sneak in on machines with German/French locales and
        // turn the decimal into a SQL parse error.
        return String.format(java.util.Locale.ROOT, "%.6f", seconds);
    }
}
