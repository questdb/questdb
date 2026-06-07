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
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
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
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final Log LOG = LogFactory.getLog(ServerMainWaitWalTableTest.class);

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.SHARED_WORKER_COUNT + "=1",
                PropertyKey.PG_WORKER_COUNT + "=1",
                // Tighter than the 1s production default: tests that rely on a
                // wake-cycle to observe timeout/cancel/dropped should not stretch
                // out to a full second per cycle.
                PropertyKey.GRIFFIN_QUERY_CONTINUATION_WAKE_INTERVAL + "=100"
        ));
        dbPath.parent().$();
    }

    @Test(timeout = 240_000)
    public void testFuzzConcurrentWaitWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // Concurrency / load test for wait_wal_table. Spawns many client threads
            // against a server with multiple workers and timer shards, each running
            // a randomly-chosen scenario:
            //   - happy wait that resolves via the periodic drainWalQueue;
            //   - fast path: wait_wal_table('t', N) with N <= writerTxn (no waiter
            //     registered);
            //   - stmt.cancel mid-wait (PG cancel request trips the breaker);
            //   - connection drop mid-wait (broken FD trips the breaker);
            //   - HTTP /exec happy wait;
            //   - multi-waiter notifyOnDrop terminal: K helpers parked, then DROP;
            //   - multi-waiter setSuspended terminal: K helpers parked, then suspend.
            // Background drainer + inserter threads keep the shared tracker hot, so
            // concurrent fireWaiters vs registerWaiter races and the
            // ContinuationQueue MPMC drain path are exercised throughout. The shard
            // count is set to >= 2 so register() distribution and concurrent timer
            // threads contend on different shards; the worker count is >= 2 so two
            // parked waits can each be on a different carrier and concurrent
            // scheduleResume traffic on the origin pools' resume queues is exercised.
            // The two terminal scenarios are the explicit gap closures: until now,
            // notifyOnDrop / setSuspended were only validated with a single waiter.
            Rnd rnd = TestUtils.generateRandom(LOG);
            long seed1 = rnd.getSeed1();
            long seed0 = rnd.getSeed0();

            final int workerCount = 2;
            final int timerShardCount = 2;
            final int clientThreads = 24;
            final int iterationsPerThread = 4;

            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.PG_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.HTTP_WORKER_COUNT.getEnvVarName(), String.valueOf(workerCount));
                put(PropertyKey.CAIRO_TIMER_SHARDS.getEnvVarName(), String.valueOf(timerShardCount));
                // WAL apply disabled: writerTxn only advances via the drainer thread
                // below, so wait_wal_table actually parks rather than fast-pathing.
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                // Sanity: server bootstrapped the requested number of timer shards.
                Assert.assertNotNull(serverMain.getEngine().getTimerShards());

                // Bootstrap the shared table that the bulk of the scenarios target.
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE shared (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO shared VALUES ('2024-01-01T00:00:00.000000Z', 0)");
                }

                final AtomicBoolean stopBackground = new AtomicBoolean();
                // Drainer: periodically advance writerTxn via a manual apply. Without
                // this the implicit-target wait would only end via the wake-cycle
                // breaker check (timeout / cancel / disconnect).
                Thread drainer = new Thread(() -> {
                    try {
                        while (!stopBackground.get()) {
                            try {
                                TestUtils.drainWalQueue(serverMain.getEngine());
                                Thread.sleep(20);
                            } catch (Throwable ignored) {
                            }
                        }
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "wait-fuzz-drainer");
                drainer.setDaemon(true);
                drainer.start();

                // Inserter: continually advance seqTxn on the shared table. Combined
                // with the drainer this yields a moving target: implicit-form waits
                // capture seqTxn at compile time, then race the drainer's
                // updateWriterTxns -> fireWaiters against their own registerWaiter.
                Thread inserter = new Thread(() -> {
                    int rowId = 1;
                    try (
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement stmt = conn.createStatement()
                    ) {
                        while (!stopBackground.get()) {
                            try {
                                stmt.execute("INSERT INTO shared VALUES ('2024-01-01T00:00:00.000000Z', " + (rowId++) + ")");
                                Thread.sleep(10);
                            } catch (Throwable ignored) {
                            }
                        }
                    } catch (Throwable ignored) {
                    }
                }, "wait-fuzz-inserter");
                inserter.setDaemon(true);
                inserter.start();

                final CyclicBarrier startGate = new CyclicBarrier(clientThreads);
                final CountDownLatch doneLatch = new CountDownLatch(clientThreads);
                final AtomicInteger happyCount = new AtomicInteger();
                final AtomicInteger fastPathCount = new AtomicInteger();
                final AtomicInteger cancelledCount = new AtomicInteger();
                final AtomicInteger droppedCount = new AtomicInteger();
                final AtomicInteger httpCount = new AtomicInteger();
                final AtomicInteger dropTerminalCount = new AtomicInteger();
                final AtomicInteger suspendTerminalCount = new AtomicInteger();
                final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

                for (int i = 0; i < clientThreads; i++) {
                    final long threadSeed1 = rnd.nextLong();
                    final long threadSeed2 = rnd.nextLong();
                    final int threadId = i;
                    Thread t = new Thread(() -> {
                        Rnd tr = new Rnd(threadSeed1, threadSeed2);
                        try {
                            startGate.await();
                            for (int j = 0; j < iterationsPerThread; j++) {
                                int scenario = tr.nextInt(7);
                                try {
                                    switch (scenario) {
                                        case 0:
                                            runHappyWaitFuzz(happyCount);
                                            break;
                                        case 1:
                                            runFastPathSeqTxnFuzz(fastPathCount);
                                            break;
                                        case 2:
                                            runCancelledWaitFuzz(tr, cancelledCount);
                                            break;
                                        case 3:
                                            runDroppedConnectionWaitFuzz(tr, droppedCount);
                                            break;
                                        case 4:
                                            runHttpHappyWaitFuzz(httpCount);
                                            break;
                                        case 5:
                                            runMultiWaiterDropFuzz(serverMain, threadId, j, dropTerminalCount);
                                            break;
                                        case 6:
                                            runMultiWaiterSuspendFuzz(serverMain, threadId, j, suspendTerminalCount);
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
                            Path.clearThreadLocals();
                            doneLatch.countDown();
                        }
                    }, "wait-wal-fuzz-" + threadId);
                    // Platform threads (not virtual): isolates the framework under test
                    // from JEP 491 / virtual-thread monitor-handoff interactions.
                    t.setDaemon(true);
                    t.start();
                }

                Assert.assertTrue(
                        "fuzz did not complete in time, seeds=" + seed0 + "L, " + seed1 + "L",
                        doneLatch.await(180, TimeUnit.SECONDS)
                );
                stopBackground.set(true);
                drainer.join(2_000);
                inserter.join(2_000);

                if (!failures.isEmpty()) {
                    Throwable head = failures.peek();
                    throw new AssertionError(
                            "fuzz produced " + failures.size() + " failures (seeds=" + seed0 + "L, " + seed1 + "L; first: "
                                    + head.getMessage(),
                            head);
                }

                // Final liveness check: server must still serve a query promptly
                // after the load of cancellations, drops, suspends, and concurrent
                // waits.
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

                LOG.info().$("wait_wal_table fuzz completed [happy=").$(happyCount.get())
                        .$(", fastPath=").$(fastPathCount.get())
                        .$(", cancelled=").$(cancelledCount.get())
                        .$(", dropped=").$(droppedCount.get())
                        .$(", http=").$(httpCount.get())
                        .$(", dropTerminal=").$(dropTerminalCount.get())
                        .$(", suspendTerminal=").$(suspendTerminalCount.get())
                        .$(", failures=").$(failures.size())
                        .$(", seeds=").$(seed0).$("L, ").$(seed1).$("L")
                        .$(']').$();

                // Probabilistic coverage: with 24*4=96 iterations across 7 buckets
                // each path is expected to fire several times. The terminal-state
                // scenarios are the load-bearing additions; fail loudly if either
                // never ran -- it would mean the gap closure is silently inactive.
                Assert.assertTrue("no happy waits ran (seeds=" + seed0 + "L, " + seed1 + "L)", happyCount.get() > 0);
                Assert.assertTrue("no cancelled waits ran (seeds=" + seed0 + "L, " + seed1 + "L)", cancelledCount.get() > 0);
                Assert.assertTrue("no dropped-connection waits ran (seeds=" + seed0 + "L, " + seed1 + "L)", droppedCount.get() > 0);
                Assert.assertTrue("no multi-waiter drop terminal ran (seeds=" + seed0 + "L, " + seed1 + "L)", dropTerminalCount.get() > 0);
                Assert.assertTrue("no multi-waiter suspend terminal ran (seeds=" + seed0 + "L, " + seed1 + "L)", suspendTerminalCount.get() > 0);
            }
        });
    }

    @Test
    public void testWaitWalTableErrorsOnDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            // WAL apply disabled, very long query timeout: the only way the wait can
            // end is if the function notices the table has been dropped. With a
            // long timeout we'd otherwise burn CPU until the breaker trips. The
            // wait must instead see the dropped flag, abandon the wait and throw a
            // table-does-not-exist error promptly.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "120s");
            }})) {
                serverMain.start();

                try (
                        Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = setupConn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                }

                CountDownLatch waitStarted = new CountDownLatch(1);
                AtomicReference<Throwable> waitOutcome = new AtomicReference<>();
                AtomicReference<String> errorMessage = new AtomicReference<>();
                AtomicReference<Long> waitElapsed = new AtomicReference<>();
                Thread waiter = new Thread(() -> {
                    long t0 = System.currentTimeMillis();
                    try (
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement stmt = conn.createStatement()
                    ) {
                        waitStarted.countDown();
                        stmt.executeQuery("SELECT wait_wal_table('foo')");
                        waitOutcome.set(new AssertionError("expected table-does-not-exist error"));
                    } catch (PSQLException expected) {
                        errorMessage.set(expected.getMessage());
                    } catch (Throwable t) {
                        waitOutcome.set(t);
                    } finally {
                        waitElapsed.set(System.currentTimeMillis() - t0);
                    }
                }, "wait-wal-table-drop-waiter");
                waiter.setDaemon(true);
                waiter.start();

                Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
                awaitWaiterRegistered(serverMain);

                // Drop the table while the wait is parked. notifyOnDrop sets the
                // dropped flag and fires waiters; the resumed cont must observe
                // dropped on its next loop top and throw, rather than re-registering
                // a waiter (which would fire immediately on the dropped tracker
                // and turn into a CPU-burning busy loop until query_timeout).
                try (
                        Connection dropConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = dropConn.createStatement()
                ) {
                    stmt.execute("DROP TABLE foo");
                }

                waiter.join(5_000);
                if (waitOutcome.get() != null) {
                    throw new AssertionError("wait_wal_table thread failed", waitOutcome.get());
                }
                String msg = errorMessage.get();
                Assert.assertNotNull("expected an error from the parked wait", msg);
                Assert.assertTrue(
                        "expected table-does-not-exist error, got: " + msg,
                        msg.contains("does not exist")
                );
                Long elapsed = waitElapsed.get();
                Assert.assertNotNull(elapsed);
                // Must finish well within the 120s query timeout. The drop should
                // wake the parked cont within a wake-interval (~200ms) at most.
                Assert.assertTrue(
                        "wait took too long after drop: " + elapsed + " ms (expected near-instant resolution)",
                        elapsed < 5_000
                );
            }
        });
    }

    @Test
    public void testWaitWalTableSucceedsAfterManualApply() throws Exception {
        assertMemoryLeak(() -> {
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
                awaitWaiterRegistered(serverMain);

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
        });
    }

    @Test
    public void testWaitWalTableSucceedsAfterManualApplyOverHttp() throws Exception {
        assertMemoryLeak(() -> {
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
                awaitWaiterRegistered(serverMain);

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
        });
    }

    @Test
    public void testWaitWalTableSeqTxnFastPath() throws Exception {
        assertMemoryLeak(() -> {
            // Two-arg overload: wait_wal_table('foo', N) where N <= current writerTxn.
            // The fast path in getBool() must return true immediately without parking
            // a continuation or registering a waiter.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                // WAL apply enabled here: we want writerTxn to actually advance so the
                // request seqTxn is already met by the time wait_wal_table runs.
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    // Make sure the writer has applied at least one txn.
                    try (ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('foo')")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertTrue(rs.getBoolean(1));
                    }
                    long initialWaiters = serverMain.getEngine().getTableSequencerAPI()
                            .getTxnTracker(serverMain.getEngine().verifyTableName("foo"))
                            .getWaiterRegistrationCount();

                    long t0 = System.currentTimeMillis();
                    try (ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('foo', 1)")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertTrue(rs.getBoolean(1));
                    }
                    long elapsed = System.currentTimeMillis() - t0;
                    Assert.assertTrue(
                            "fast path took too long: " + elapsed + " ms (expected immediate return)",
                            elapsed < 1_000
                    );

                    long finalWaiters = serverMain.getEngine().getTableSequencerAPI()
                            .getTxnTracker(serverMain.getEngine().verifyTableName("foo"))
                            .getWaiterRegistrationCount();
                    Assert.assertEquals(
                            "fast path must not register a waiter",
                            initialWaiters,
                            finalWaiters
                    );
                }
            }
        });
    }

    @Test
    public void testWaitWalTableSeqTxnNullArgBehavesLikeNoArg() throws Exception {
        assertMemoryLeak(() -> {
            // wait_wal_table('foo', NULL) must behave like the no-arg form: wait
            // for the seqTxn observed at call time. Regression test for a bug
            // where the NULL sentinel from Function.getLong() short-circuited
            // the writerTxn >= seqTxn check, causing the function to return TRUE
            // immediately without waiting.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                try (
                        Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = setupConn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                }

                CountDownLatch waitStarted = new CountDownLatch(1);
                AtomicReference<Boolean> result = new AtomicReference<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread waiter = new Thread(() -> {
                    try (
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement stmt = conn.createStatement()
                    ) {
                        waitStarted.countDown();
                        try (ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('foo', NULL)")) {
                            Assert.assertTrue(rs.next());
                            result.set(rs.getBoolean(1));
                        }
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                }, "wait-wal-table-null-arg-waiter");
                waiter.setDaemon(true);
                waiter.start();

                Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
                // If NULL incorrectly short-circuited, no waiter would ever register.
                awaitWaiterRegistered(serverMain);

                TestUtils.drainWalQueue(serverMain.getEngine());

                waiter.join(5_000);
                if (failure.get() != null) {
                    throw new AssertionError("wait_wal_table(NULL) failed", failure.get());
                }
                Assert.assertEquals(Boolean.TRUE, result.get());
            }
        });
    }

    @Test
    public void testWaitWalTableSeqTxnSucceedsAfterManualApply() throws Exception {
        assertMemoryLeak(() -> {
            // Two-arg overload: wait_wal_table('foo', 2) parks until writerTxn >= 2.
            // WAL apply is disabled, so the wait must park its continuation; the
            // test thread drives a manual drain that advances writerTxn and fires
            // the parked waiter.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "30s");
            }})) {
                serverMain.start();

                try (
                        Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = setupConn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                }

                CountDownLatch waitStarted = new CountDownLatch(1);
                AtomicReference<Boolean> result = new AtomicReference<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread waiter = new Thread(() -> {
                    try (
                            Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                            Statement stmt = conn.createStatement()
                    ) {
                        waitStarted.countDown();
                        try (ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('foo', 2)")) {
                            Assert.assertTrue(rs.next());
                            result.set(rs.getBoolean(1));
                        }
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                }, "wait-wal-table-seqtxn-waiter");
                waiter.setDaemon(true);
                waiter.start();

                Assert.assertTrue("waiter thread did not start", waitStarted.await(5, TimeUnit.SECONDS));
                awaitWaiterRegistered(serverMain);

                TestUtils.drainWalQueue(serverMain.getEngine());

                waiter.join(5_000);
                if (failure.get() != null) {
                    throw new AssertionError("wait_wal_table(seqTxn) failed", failure.get());
                }
                Assert.assertEquals(Boolean.TRUE, result.get());
            }
        });
    }

    @Test
    public void testWaitWalTableSeqTxnTimesOutWhenSeqTxnUnreachable() throws Exception {
        assertMemoryLeak(() -> {
            // Two-arg overload with a seqTxn that the writer can never reach (no
            // matching insert was made). The function must park, then time out via
            // the circuit breaker on the wake-recheck probe.
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "200ms");
            }})) {
                serverMain.start();

                try (
                        Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = setupConn.createStatement()
                ) {
                    stmt.execute("CREATE TABLE foo (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    stmt.execute("INSERT INTO foo VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                }

                AtomicReference<Throwable> waitOutcome = new AtomicReference<>();
                AtomicReference<Long> waitElapsed = new AtomicReference<>();
                long t0 = System.currentTimeMillis();
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    // 1_000 is far beyond any seqTxn this table will ever see.
                    stmt.executeQuery("SELECT wait_wal_table('foo', 1000)");
                    waitOutcome.set(new AssertionError("expected query timeout exception"));
                } catch (PSQLException expected) {
                    // good -- breaker tripped on the wake-recheck probe
                } catch (Throwable t) {
                    waitOutcome.set(t);
                } finally {
                    waitElapsed.set(System.currentTimeMillis() - t0);
                }

                if (waitOutcome.get() != null) {
                    throw new AssertionError("wait_wal_table(seqTxn) outcome unexpected", waitOutcome.get());
                }
                Long elapsed = waitElapsed.get();
                Assert.assertNotNull(elapsed);
                Assert.assertTrue("wait returned too quickly: " + elapsed + " ms", elapsed >= 150);
                Assert.assertTrue("wait returned too slowly: " + elapsed + " ms", elapsed < 5_000);
            }
        });
    }

    @Test
    public void testWaitWalTableTimesOut() throws Exception {
        assertMemoryLeak(() -> {
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
                awaitWaiterRegistered(serverMain);

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
        });
    }

    @Test
    public void testWaitWalTableTimesOutOverHttp() throws Exception {
        assertMemoryLeak(() -> {
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
                awaitWaiterRegistered(serverMain);

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
        });
    }

    /**
     * Blocks until the {@code wait_wal_table} body has registered a waiter on the named
     * table's tracker. Replaces fixed {@code Os.sleep()} synchronization: on a slow CI
     * box, JDBC connect + parse + suspend may exceed any chosen sleep, leaving the test
     * to drive its trigger (drop, drain, probe) before the waiter is actually parked.
     */
    private static void awaitWaiterRegistered(ServerMain serverMain) throws Exception {
        awaitWaiterRegistered(serverMain, "foo", 1);
    }

    /**
     * Blocks until at least {@code minCount} waiters have registered on the named
     * table's tracker. {@link SeqTxnTracker#getWaiterRegistrationCount()} is
     * monotonic for the tracker's lifetime, so for a freshly created table this
     * upper-bounds the multi-waiter scenarios on parallel registrations.
     */
    private static void awaitWaiterRegistered(ServerMain serverMain, String tableName, long minCount) throws Exception {
        SeqTxnTracker tracker = serverMain.getEngine().getTableSequencerAPI()
                .getTxnTracker(serverMain.getEngine().verifyTableName(tableName));
        TestUtils.assertEventually(
                () -> Assert.assertTrue(
                        "waiters never registered for table " + tableName + " (count="
                                + tracker.getWaiterRegistrationCount() + ", expected >= " + minCount + ")",
                        tracker.getWaiterRegistrationCount() >= minCount
                ),
                10
        );
    }

    private static void drainResponse(Response response, StringSink sink) {
        Fragment fragment;
        while ((fragment = response.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
    }

    private static void runCancelledWaitFuzz(Rnd tr, AtomicInteger counter) throws Exception {
        // stmt.cancel mid-park: the breaker tripper observes the cancel on the
        // next wake-recheck and the parked body throws.
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement()
        ) {
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            Thread runner = new Thread(() -> {
                try {
                    started.countDown();
                    // Target far beyond any plausible writerTxn so only the cancel
                    // can end this. The PG cancel protocol may silently drop a
                    // CancelRequest that arrives before the server has bound a
                    // breaker (parse->execute boundary); under the 24-thread /
                    // 2-worker load that race fires occasionally and the wait
                    // runs to its query timeout. Accept either outcome -- the
                    // load-bearing invariant is bounded exit, not signal delivery.
                    stmt.executeQuery("SELECT wait_wal_table('shared', 1_000_000_000)");
                } catch (PSQLException expected) {
                    // good
                } catch (Throwable t) {
                    outcome.set(t);
                }
            }, "wait-wal-fuzz-cancel-runner");
            runner.setDaemon(true);
            runner.start();
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.sleep(50 + tr.nextInt(300));
            stmt.cancel();
            runner.join(35_000);
            Assert.assertFalse("cancelled wait runner did not exit", runner.isAlive());
            if (outcome.get() != null) {
                throw new AssertionError("statement cancel scenario failed", outcome.get());
            }
        }
        counter.incrementAndGet();
    }

    private static void runDroppedConnectionWaitFuzz(Rnd tr, AtomicInteger counter) throws Exception {
        // Forcible socket teardown via Connection.abort. PG JDBC's close() is
        // synchronized on the connection's monitor, which the runner holds while
        // parked in executeQuery's blocking socket read; abort() schedules close
        // off-thread so the socket teardown can happen while the runner is still
        // parked. The server-side breaker probe trips on the broken FD on the
        // next wake-recheck.
        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread runner = new Thread(() -> {
            try (Statement stmt = conn.createStatement()) {
                started.countDown();
                stmt.executeQuery("SELECT wait_wal_table('shared', 1_000_000_000)");
                outcome.set(new AssertionError("expected error after connection drop"));
            } catch (PSQLException expected) {
                // good
            } catch (Throwable t) {
                outcome.set(t);
            }
        }, "wait-wal-fuzz-drop-runner");
        runner.setDaemon(true);
        runner.start();
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread.sleep(50 + tr.nextInt(300));
        try {
            conn.abort(r -> {
                Thread t = new Thread(r, "wait-wal-fuzz-drop-abort");
                t.setDaemon(true);
                t.start();
            });
        } catch (SQLException ignored) {
        }
        runner.join(35_000);
        Assert.assertFalse("dropped wait runner did not exit", runner.isAlive());
        // The runner must have unwound via the expected PSQLException; surface either an
        // unexpected success (the wait returned instead of erroring on the dropped FD) or
        // any other throwable it captured, both of which would otherwise be swallowed.
        final Throwable unexpected = outcome.get();
        if (unexpected != null) {
            throw new AssertionError("dropped-connection wait produced an unexpected outcome", unexpected);
        }
        counter.incrementAndGet();
    }

    private static void runFastPathSeqTxnFuzz(AtomicInteger counter) throws SQLException {
        // wait_wal_table('shared', 0) targets a writerTxn that has been observed
        // since boot (the bootstrap INSERT advanced past 0 once the drainer ran).
        // The fast path returns immediately and does NOT enter the slow path; this
        // is the read of the tracker that registerWaiter must NOT be called on.
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('shared', 0)")
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
        }
        counter.incrementAndGet();
    }

    private static void runHappyWaitFuzz(AtomicInteger counter) throws SQLException {
        // Implicit-target form: captures seqTxn at compile time. The drainer
        // background thread advances writerTxn in periodic bursts; the wait
        // resolves once the drain catches up. With many concurrent registrants
        // and a parallel drainer, this exercises the registerWaiter / fireWaiters
        // race directly.
        try (
                Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT wait_wal_table('shared')")
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getBoolean(1));
        }
        counter.incrementAndGet();
    }

    private static void runHttpHappyWaitFuzz(AtomicInteger counter) {
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
            HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
            request.GET().url("/exec").query("query", "SELECT wait_wal_table('shared')");
            try (HttpClient.ResponseHeaders headers = request.send()) {
                headers.await();
                StringSink sink = new StringSink();
                drainResponse(headers.getResponse(), sink);
                String body = sink.toString();
                Assert.assertFalse("HTTP wait returned error: " + body, body.contains("\"error\""));
                Assert.assertTrue("HTTP wait missing dataset: " + body, body.contains("\"dataset\""));
            }
        }
        counter.incrementAndGet();
    }

    private static void runMultiWaiterDropFuzz(ServerMain serverMain, int threadId, int iter, AtomicInteger counter) throws Exception {
        // Self-contained scenario: create a fresh table, spawn helperCount
        // threads each issuing wait_wal_table on it with an unreachable target
        // so all helpers park and queue on the tracker, then DROP the table
        // from this thread. notifyOnDrop must wake every parked waiter; the
        // SeqTxnTrackerTest suite only validated this with a single waiter.
        String table = "doomed_drop_" + threadId + "_" + iter;
        try (
                Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement setupStmt = setupConn.createStatement()
        ) {
            setupStmt.execute("CREATE TABLE " + table + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setupStmt.execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
        }

        final int helperCount = 4;
        final CountDownLatch helpersStarted = new CountDownLatch(helperCount);
        final CountDownLatch helpersDone = new CountDownLatch(helperCount);
        final AtomicInteger helperFailures = new AtomicInteger();
        Thread[] helpers = new Thread[helperCount];
        for (int h = 0; h < helperCount; h++) {
            helpers[h] = new Thread(() -> {
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    helpersStarted.countDown();
                    try {
                        stmt.executeQuery("SELECT wait_wal_table('" + table + "', 1_000_000_000)");
                        // Unreachable target + no other terminator means we
                        // must NOT have returned normally.
                        helperFailures.incrementAndGet();
                    } catch (PSQLException expected) {
                        // good -- DROP TABLE wakes the parked wait, which
                        // throws table-does-not-exist on the resumed loop top.
                    }
                } catch (Throwable t) {
                    helperFailures.incrementAndGet();
                } finally {
                    helpersDone.countDown();
                }
            }, "wait-wal-fuzz-multi-drop-" + threadId + "-" + iter + "-" + h);
            helpers[h].setDaemon(true);
            helpers[h].start();
        }
        Assert.assertTrue(
                "multi-waiter drop helpers did not start in time",
                helpersStarted.await(10, TimeUnit.SECONDS)
        );
        // Wait until all helpers are parked on the tracker. The reg counter
        // is monotonic on a fresh tracker, so >= helperCount means each
        // helper has reached its first registerWaiter call.
        awaitWaiterRegistered(serverMain, table, helperCount);

        try (
                Connection dropConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement dropStmt = dropConn.createStatement()
        ) {
            dropStmt.execute("DROP TABLE " + table);
        }

        Assert.assertTrue(
                "multi-waiter drop helpers did not exit after DROP",
                helpersDone.await(15, TimeUnit.SECONDS)
        );
        Assert.assertEquals("multi-waiter drop produced helper failures", 0, helperFailures.get());
        counter.incrementAndGet();
    }

    private static void runMultiWaiterSuspendFuzz(ServerMain serverMain, int threadId, int iter, AtomicInteger counter) throws Exception {
        // Mirrors runMultiWaiterDropFuzz but flips the terminal path to
        // setSuspended: helperCount parked waits, then setSuspended must wake
        // every waiter. The SQL surface has no equivalent of a SUSPEND command,
        // so we drive setSuspended via the engine API (the same path the apply
        // job takes when it fails).
        String table = "doomed_suspend_" + threadId + "_" + iter;
        try (
                Connection setupConn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                Statement setupStmt = setupConn.createStatement()
        ) {
            setupStmt.execute("CREATE TABLE " + table + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setupStmt.execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
        }
        // Settle this table's writerTxn against its seqTxn before suspending.
        // The fuzz's background drainer thread keeps calling drainWalQueue;
        // if we leave a pending INSERT here, that drain advances writerTxn,
        // and updateWriterTxns flips suspendedState back to 1 (un-suspended),
        // racing the waiters back to PENDING before they can throw. With nothing
        // left to apply, subsequent drainer ticks are no-ops for this table and
        // setSuspended sticks.
        TestUtils.drainWalQueue(serverMain.getEngine());

        final int helperCount = 4;
        final CountDownLatch helpersStarted = new CountDownLatch(helperCount);
        final CountDownLatch helpersDone = new CountDownLatch(helperCount);
        final AtomicInteger helperFailures = new AtomicInteger();
        Thread[] helpers = new Thread[helperCount];
        for (int h = 0; h < helperCount; h++) {
            helpers[h] = new Thread(() -> {
                try (
                        Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement stmt = conn.createStatement()
                ) {
                    helpersStarted.countDown();
                    try {
                        stmt.executeQuery("SELECT wait_wal_table('" + table + "', 1_000_000_000)");
                        helperFailures.incrementAndGet();
                    } catch (PSQLException expected) {
                        // good -- table-is-suspended bubbles up to the client
                    }
                } catch (Throwable t) {
                    helperFailures.incrementAndGet();
                } finally {
                    helpersDone.countDown();
                }
            }, "wait-wal-fuzz-multi-suspend-" + threadId + "-" + iter + "-" + h);
            helpers[h].setDaemon(true);
            helpers[h].start();
        }
        Assert.assertTrue(
                "multi-waiter suspend helpers did not start in time",
                helpersStarted.await(10, TimeUnit.SECONDS)
        );
        awaitWaiterRegistered(serverMain, table, helperCount);

        SeqTxnTracker tracker = serverMain.getEngine().getTableSequencerAPI()
                .getTxnTracker(serverMain.getEngine().verifyTableName(table));
        tracker.setSuspended(ErrorTag.NONE, "fuzz-induced");

        Assert.assertTrue(
                "multi-waiter suspend helpers did not exit after setSuspended",
                helpersDone.await(15, TimeUnit.SECONDS)
        );
        Assert.assertEquals("multi-waiter suspend produced helper failures", 0, helperFailures.get());
        counter.incrementAndGet();
    }
}
