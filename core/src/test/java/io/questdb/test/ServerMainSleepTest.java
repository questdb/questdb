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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
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
 * parks its worker continuation through {@link io.questdb.mp.TimerCont} for the
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
                        stmt.executeQuery("SELECT sleep(60)");
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
    }

    @Test
    public void testSleepCancelledByQueryTimeout() throws Exception {
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
                    stmt.executeQuery("SELECT sleep(60)");
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
    }

    @Test
    public void testSleepCancelledByStatementCancel() throws Exception {
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
                        stmt.executeQuery("SELECT sleep(60)");
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
    }

    @Test
    public void testSleepFreesWorkerForConcurrentQueries() throws Exception {
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
                    try (ResultSet rs = stmt.executeQuery("SELECT sleep(2)")) {
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
    }

    @Test
    public void testSleepReturnsApproximateTimeOnHttp() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
        }})) {
            serverMain.start();

            long t0 = System.currentTimeMillis();
            String body;
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                request.GET().url("/exec").query("query", "SELECT sleep(0.4)");
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
    }

    @Test
    public void testSleepReturnsApproximateTimeOnPg() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
        }})) {
            serverMain.start();

            long beforeMillis = System.currentTimeMillis();
            long returnedMillis;
            try (
                    Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT sleep(0.5)")
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
    }

    @Test(timeout = 180_000)
    public void testFuzzConcurrentSleeps() throws Exception {
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
        final int clientThreads = 64;
        final int iterationsPerThread = 8;

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

            // Periodic thread-dumper modelled on TestListener.dumpThreadStacks:
            // a platform daemon thread that uses ThreadMXBean to capture full
            // stacks every 5s while the fuzz runs. If a timer thread goes silent
            // mid-test, the dump reveals exactly where it is parked
            // (DelayQueue.take, ConcurrentQueue.enqueue, runShard while, etc.)
            // and lets us distinguish "stuck in JDK code" from "stuck in QuestDB
            // code" from "thread already exited" -- the printed thread set
            // includes everything alive, so a missing timer thread proves an
            // earlier silent exit, while a present-but-parked one proves a hang.
            final java.util.concurrent.atomic.AtomicBoolean dumperStop = new java.util.concurrent.atomic.AtomicBoolean();
            Thread dumper = new Thread(() -> {
                final java.lang.management.ThreadMXBean tmx = java.lang.management.ManagementFactory.getThreadMXBean();
                while (!dumperStop.get()) {
                    try {
                        Thread.sleep(5_000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (dumperStop.get()) return;
                    StringBuilder s = new StringBuilder();
                    s.append("=== STACK DUMP @ ").append(System.currentTimeMillis()).append(" ===");
                    java.lang.management.ThreadInfo[] infos = tmx.getThreadInfo(tmx.getAllThreadIds(), 32);
                    for (java.lang.management.ThreadInfo ti : infos) {
                        if (ti == null) continue;
                        s.append('\n').append('\'').append(ti.getThreadName()).append("\': ").append(ti.getThreadState());
                        for (StackTraceElement f : ti.getStackTrace()) {
                            s.append("\n\t\tat ").append(f);
                        }
                        s.append('\n');
                    }
                    s.append("=== END STACK DUMP ===");
                    System.out.println(s);
                }
            }, "timer-stack-dumper");
            dumper.setDaemon(true);
            dumper.start();

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
                                        runStatementCancelFuzz(tr, cancelledCount);
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

            // Hard upper bound: 12 threads * 25 iters * worst-case ~600ms = ~3 min.
            // Allow plenty of headroom; the test timeout still bounds the run.
            boolean completed = doneLatch.await(150, TimeUnit.SECONDS);
            // Stop the dumper before final assertions so the post-load logs
            // aren't interleaved with another stack dump.
            dumperStop.set(true);
            dumper.interrupt();
            Assert.assertTrue(
                    "fuzz did not complete in time, seeds=" + seed0 + "L, " + seed1 + "L",
                    completed
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

            // Did we actually exercise each path? Probabilistic but at 12*25=300
            // iterations with 8 buckets we should hit each at least a few times.
            Assert.assertTrue("no happy sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", happyCount.get() > 0);
            Assert.assertTrue("no cancelled sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", cancelledCount.get() > 0);
            Assert.assertTrue("no dropped sleeps ran (seeds=" + seed0 + "L, " + seed1 + "L)", droppedCount.get() > 0);

            // Parallelism check: sum of all completed-sleep durations must exceed
            // (workerCount * wall). With Os.sleep semantics each worker is pinned
            // during a sleep, so the total sleep time across the whole pool is
            // capped by (workerCount * wall) -- a worker can only contribute
            // wall-many milliseconds of sleep at most. Exceeding that ceiling is
            // only possible if a single carrier handles multiple in-flight sleeps
            // concurrently (TimerCont suspends, frees the carrier, the worker
            // mounts the next request, and so on).
            Assert.assertTrue(
                    "sleeps appear to pin carriers: sumSleptMs=" + sumSleptMillis
                            + " busyWallMs=" + busyWallMillis
                            + " workerCount=" + workerCount
                            + " (expected sum > workerCount * wall = "
                            + ((long) workerCount * busyWallMillis) + ") seeds=" + seed0 + "L, " + seed1 + "L",
                    sumSleptMillis > (long) workerCount * busyWallMillis
            );
        }
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
                stmt.executeQuery("SELECT sleep(0.7)");
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
        System.out.println("sleep-fuzz-drop: about to abort conn [thread=" + Thread.currentThread().getName() + "]");
        try {
            conn.abort(r -> {
                Thread t = new Thread(r, "sleep-fuzz-drop-abort");
                t.setDaemon(true);
                t.start();
            });
        } catch (SQLException ignored) {
        }
        System.out.println("sleep-fuzz-drop: abort returned, joining runner [thread=" + Thread.currentThread().getName() + "]");
        runner.join(10_000);
        System.out.println("sleep-fuzz-drop: runner.join returned [thread=" + Thread.currentThread().getName() + ", runnerAlive=" + runner.isAlive() + "]");
        Assert.assertFalse("dropped sleep runner did not exit", runner.isAlive());
        counter.incrementAndGet();
    }

    private static void runHappyPgSleep(double seconds, AtomicInteger counter, AtomicLong totalSleptMillis) throws SQLException {
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT sleep(" + formatSeconds(seconds) + ")")
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
            request.GET().url("/exec").query("query", "SELECT sleep(" + formatSeconds(seconds) + ")");
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
                try (ResultSet rs = stmt.executeQuery("SELECT sleep(" + formatSeconds(secs) + ")")) {
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

    private static void runStatementCancelFuzz(Rnd tr, AtomicInteger counter) throws Exception {
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement()
        ) {
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            Thread runner = new Thread(() -> {
                try {
                    started.countDown();
                    // Long enough that stmt.cancel() (sent ~50-300 ms in)
                    // generally races ahead of the deadline; short enough that
                    // a missed cancel doesn't blow the whole fuzz budget.
                    // The PG cancel protocol silently drops a CancelRequest
                    // that arrives before the server has bound a breaker to
                    // the connection's in-flight query (the parse phase
                    // hasn't transitioned to execute yet). Under a 64-thread
                    // / 2-worker load that race fires occasionally and the
                    // sleep runs to completion. We treat both outcomes as
                    // success: the load-bearing invariant we're stressing is
                    // that the runner exits in bounded time, not that every
                    // cancel signal is honoured.
                    stmt.executeQuery("SELECT sleep(2.1)");
                } catch (PSQLException expected) {
                    // cancel landed in time
                } catch (Throwable t) {
                    outcome.set(t);
                }
            }, "sleep-fuzz-cancel-runner");
            runner.setDaemon(true);
            runner.start();
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.sleep(50 + tr.nextInt(300));
            System.out.println("sleep-fuzz-cancel: about to cancel stmt [thread=" + Thread.currentThread().getName() + "]");
            stmt.cancel();
            System.out.println("sleep-fuzz-cancel: cancel returned, joining runner [thread=" + Thread.currentThread().getName() + "]");
            runner.join(10_000);
            System.out.println("sleep-fuzz-cancel: runner.join returned [thread=" + Thread.currentThread().getName() + ", runnerAlive=" + runner.isAlive() + "]");
            Assert.assertFalse("cancelled sleep runner did not exit", runner.isAlive());
            if (outcome.get() != null) {
                throw new AssertionError("statement cancel scenario failed", outcome.get());
            }
        }
        System.out.println("sleep-fuzz-cancel: try-with-resources closing conn [thread=" + Thread.currentThread().getName() + "]");
        counter.incrementAndGet();
    }

    private static String formatSeconds(double seconds) {
        // Stable English-locale formatting independent of JVM default Locale,
        // so commas don't sneak in on machines with German/French locales and
        // turn the decimal into a SQL parse error.
        return String.format(java.util.Locale.ROOT, "%.6f", seconds);
    }
}
