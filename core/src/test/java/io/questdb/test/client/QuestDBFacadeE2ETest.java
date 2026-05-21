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

package io.questdb.test.client;

import io.questdb.PropertyKey;
import io.questdb.client.Completion;
import io.questdb.client.QuestDB;
import io.questdb.client.QueryException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end tests for the {@link QuestDB} facade against an embedded server.
 * Verifies that pooled Sender ingest and pooled Query egress both round-trip
 * data correctly and that the pool semantics (flush-on-return, thread-affine,
 * acquire timeout, cancel) hold under real I/O.
 * <p>
 * Every run picks a small random chunk size and forces both HTTP recv and send
 * fragmentation at the server, mirroring {@code QwpEgressFragmentationFuzzTest}.
 * This stresses the WebSocket frame parser, the HTTP request reader, and the
 * egress streamResults park-resume path on top of the facade-level pool
 * semantics being asserted. Without it, the tests would only exercise the
 * happy path of a single TCP segment per frame.
 */
public class QuestDBFacadeE2ETest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(QuestDBFacadeE2ETest.class);
    private int recvChunk;
    private int sendChunk;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        Rnd rnd = TestUtils.generateRandom(LOG);
        // Independent recv / send chunks so the fuzz also exercises asymmetric
        // splits -- the bug class where a parser fights the writer's rhythm
        // only surfaces when the two sides drift apart. Lower bound is 1 to
        // match QwpEgressFragmentationFuzzTest: at chunk=1 every wire byte is
        // its own socket event, hitting park-resume on every single iteration.
        recvChunk = 1 + rnd.nextInt(500);
        sendChunk = 1 + rnd.nextInt(500);
        LOG.info().$("QuestDBFacadeE2ETest fragmentation recvChunk=").$(recvChunk)
                .$(", sendChunk=").$(sendChunk).$();
    }

    /**
     * Boots {@link TestServerMain} with independent send- and recv-side HTTP
     * fragmentation thresholds. Every server-side socket read is capped at
     * {@link #recvChunk} bytes and every send at {@link #sendChunk}, so each
     * WebSocket frame and each HTTP/ILP request body spans many partial I/O
     * events -- and the two sides do not move in lockstep.
     */
    private TestServerMain startFragmented() {
        return startWithEnvVariables(
                PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(recvChunk),
                PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(sendChunk)
        );
    }

    @Test
    public void testBorrowSenderRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE rt(i LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("rt");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.connect(config)) {
                    try (Sender s = db.borrowSender()) {
                        s.table("rt").longColumn("i", 1).at(1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        s.table("rt").longColumn("i", 2).at(2L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        s.table("rt").longColumn("i", 3).at(3L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        // close() flushes -- assertion below proves it ran.
                    }
                    server.awaitTable("rt");

                    AtomicLong sum = new AtomicLong();
                    AtomicInteger rows = new AtomicInteger();
                    Completion c = db.executeSql("SELECT i FROM rt", new CollectingHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum.addAndGet(batch.getLongValue(0, r));
                                rows.incrementAndGet();
                            }
                        }
                    });
                    c.await();
                    Assert.assertEquals(3, rows.get());
                    Assert.assertEquals(6L, sum.get());
                }
            }
        });
    }

    @Test
    public void testCancelInterruptsLongRunningQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE big(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO big SELECT x, x::TIMESTAMP FROM long_sequence(50000)");
                server.awaitTable("big");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(1).build()) {
                    AtomicReference<Byte> errorStatus = new AtomicReference<>();
                    AtomicReference<Throwable> awaitOutcome = new AtomicReference<>();
                    CountDownLatch firstBatch = new CountDownLatch(1);
                    Completion c = db.executeSql("SELECT x FROM big ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            firstBatch.countDown();
                            // Stall longer than the typical batch turnaround so cancel
                            // has time to land while the user thread is parked here.
                            try {
                                Thread.sleep(2_000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte s, String message) {
                            errorStatus.set(s);
                        }
                    });
                    Assert.assertTrue(firstBatch.await(10, TimeUnit.SECONDS));
                    c.cancel();
                    try {
                        c.await();
                    } catch (QueryException qe) {
                        awaitOutcome.set(qe);
                    }
                    // Cancel races completion. Whichever wins, the query must reach
                    // a terminal state without deadlocking and the API must remain
                    // consistent. If cancel won, await threw and onError saw a
                    // non-zero status; if completion won, await returned cleanly.
                    Assert.assertTrue("Completion must terminate", c.isDone());
                    if (awaitOutcome.get() != null) {
                        Assert.assertNotNull("onError must fire when cancel wins", errorStatus.get());
                        Assert.assertNotEquals((byte) 0, (byte) errorStatus.get());
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentQueriesOnPoolOfTwo() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE conc(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO conc SELECT x, x::TIMESTAMP FROM long_sequence(100)");
                server.awaitTable("conc");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(2).senderPoolSize(2).build()) {
                    int threads = 4;
                    AtomicInteger okCount = new AtomicInteger();
                    AtomicReference<Throwable> firstError = new AtomicReference<>();
                    CountDownLatch done = new CountDownLatch(threads);
                    for (int t = 0; t < threads; t++) {
                        new Thread(() -> {
                            try {
                                AtomicInteger seen = new AtomicInteger();
                                Completion c = db.executeSql("SELECT x FROM conc", new CollectingHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                        seen.addAndGet(batch.getRowCount());
                                    }
                                });
                                c.await();
                                if (seen.get() == 100) {
                                    okCount.incrementAndGet();
                                }
                            } catch (Throwable th) {
                                firstError.compareAndSet(null, th);
                            } finally {
                                done.countDown();
                            }
                        }).start();
                    }
                    Assert.assertTrue("threads must finish in 30s", done.await(30, TimeUnit.SECONDS));
                    if (firstError.get() != null) {
                        throw new AssertionError("worker thread threw", firstError.get());
                    }
                    Assert.assertEquals(threads, okCount.get());
                }
            }
        });
    }

    @Test
    public void testQueryExceptionOnBadSql() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain _ = startFragmented()) {
                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.connect(config)) {
                    Completion c = db.executeSql("SELECT * FROM no_such_table", new CollectingHandler());
                    try {
                        c.await();
                        Assert.fail("expected QueryException for missing table");
                    } catch (QueryException qe) {
                        Assert.assertTrue("status byte must be non-zero on server error",
                                qe.getStatus() != 0);
                    }
                }
            }
        });
    }

    @Test
    public void testSenderPoolExhaustionTimesOut() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain _ = startFragmented()) {
                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder()
                        .fromConfig(config)
                        .senderPoolSize(1)
                        .acquireTimeoutMillis(200)
                        .build()) {
                    try (Sender _ = db.borrowSender()) {
                        long start = System.nanoTime();
                        try {
                            db.borrowSender();
                            Assert.fail("expected exhaustion");
                        } catch (LineSenderException ex) {
                            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                            Assert.assertTrue("must wait at least the timeout, elapsed=" + elapsedMs,
                                    elapsedMs >= 180);
                            TestUtils.assertContains(ex.getMessage(), "timed out");
                        }
                    }
                    // After release, a borrow succeeds immediately.
                    Sender second = db.borrowSender();
                    second.close();
                }
            }
        });
    }

    @Test
    public void testThreadAffineSenderRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE ta(i LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("ta");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.connect(config)) {
                    Sender s1 = db.sender();
                    Sender s2 = db.sender();
                    Assert.assertSame("same thread must see the same pinned Sender", s1, s2);
                    s1.table("ta").longColumn("i", 7).at(7L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    s1.flush();
                    db.releaseSender();
                    server.awaitTable("ta");

                    AtomicLong got = new AtomicLong();
                    Completion c = db.executeSql("SELECT i FROM ta", new CollectingHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                got.set(batch.getLongValue(0, r));
                            }
                        }
                    });
                    c.await();
                    Assert.assertEquals(7L, got.get());
                }
            }
        });
    }

    /**
     * Same thread, many sequential queries. Asserts:
     *  - {@link QuestDB#query()} returns the same per-thread instance every call
     *  - state resets between submits (no SQL/handler carryover)
     *  - back-to-back submits with binds reuse the same {@link Completion}
     */
    @Test
    public void testManySequentialQueriesOnSameThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE seq(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO seq SELECT x, x::TIMESTAMP FROM long_sequence(50)");
                server.awaitTable("seq");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(1).build()) {
                    io.questdb.client.Query firstHandle = db.query();
                    Completion firstCompletion = firstHandle
                            .sql("SELECT count() FROM seq")
                            .handler(new CollectingHandler())
                            .submit();
                    firstCompletion.await();

                    // Identity: every subsequent query() on the same thread is the
                    // same handle, and the Completion is the same field on it.
                    final int iterations = 50;
                    for (int i = 0; i < iterations; i++) {
                        io.questdb.client.Query q = db.query();
                        Assert.assertSame("Query handle must be reused per thread", firstHandle, q);

                        AtomicLong observed = new AtomicLong();
                        Completion c = q.sql("SELECT x FROM seq WHERE x = $1")
                                .binds(binds -> binds.setLong(0, 7L))
                                .handler(new QwpColumnBatchHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                        for (int r = 0; r < batch.getRowCount(); r++) {
                                            observed.set(batch.getLongValue(0, r));
                                        }
                                    }

                                    @Override
                                    public void onEnd(long totalRows) {
                                    }

                                    @Override
                                    public void onError(byte status, String message) {
                                        Assert.fail("egress error on iter " + " : " + message);
                                    }
                                })
                                .submit();
                        Assert.assertSame("Completion must be reused per Query", firstCompletion, c);
                        c.await();
                        Assert.assertEquals("iter " + i + ": bound value must reach the server", 7L, observed.get());
                    }

                    // After a binds-using query, a no-binds query on the same handle
                    // must not carry over the previous binds (would fail server-side
                    // with "no placeholder" or a stale value).
                    AtomicLong total = new AtomicLong();
                    db.query()
                            .sql("SELECT count() FROM seq")
                            .handler(new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    total.set(batch.getLongValue(0, 0));
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    Assert.fail("egress error on no-binds tail: " + message);
                                }
                            })
                            .submit()
                            .await();
                    Assert.assertEquals(50L, total.get());
                }
            }
        });
    }

    /**
     * Multi-threaded stress: more threads than pool slots, each running many
     * queries; an ingest thread writes concurrently. Verifies the pools handle
     * contention without deadlock or cross-talk, and that pool acquire blocking
     * doesn't starve any thread.
     */
    @Test
    public void testSustainedMixedConcurrency() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE mix(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO mix SELECT x, x::TIMESTAMP FROM long_sequence(200)");
                server.awaitTable("mix");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder()
                        .fromConfig(config)
                        .queryPoolSize(2)
                        .senderPoolSize(2)
                        .acquireTimeoutMillis(30_000)
                        .build()) {
                    final int queryThreads = 8;
                    final int queriesPerThread = 20;
                    AtomicInteger okCount = new AtomicInteger();
                    AtomicReference<Throwable> firstError = new AtomicReference<>();
                    CountDownLatch done = new CountDownLatch(queryThreads + 1);

                    // Ingest thread: feeds rows in parallel with the query traffic.
                    Thread ingestThread = new Thread(() -> {
                        try {
                            for (int i = 0; i < 100; i++) {
                                try (Sender s = db.borrowSender()) {
                                    s.table("mix")
                                            .longColumn("x", 1_000L + i)
                                            .at((1_000L + i) * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                                }
                            }
                        } catch (Throwable t) {
                            firstError.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                        }
                    });
                    ingestThread.start();

                    for (int t = 0; t < queryThreads; t++) {
                        new Thread(() -> {
                            try {
                                for (int i = 0; i < queriesPerThread; i++) {
                                    AtomicInteger seen = new AtomicInteger();
                                    Completion c = db.query()
                                            .sql("SELECT x FROM mix WHERE x > $1")
                                            .binds(binds -> binds.setLong(0, 0L))
                                            .handler(new QwpColumnBatchHandler() {
                                                @Override
                                                public void onBatch(QwpColumnBatch batch) {
                                                    seen.addAndGet(batch.getRowCount());
                                                }

                                                @Override
                                                public void onEnd(long totalRows) {
                                                }

                                                @Override
                                                public void onError(byte status, String message) {
                                                    firstError.compareAndSet(null,
                                                            new AssertionError("egress error: " + message));
                                                }
                                            })
                                            .submit();
                                    c.await();
                                    if (seen.get() >= 200) {
                                        okCount.incrementAndGet();
                                    }
                                }
                            } catch (Throwable th) {
                                firstError.compareAndSet(null, th);
                            } finally {
                                done.countDown();
                            }
                        }).start();
                    }

                    Assert.assertTrue("threads must finish in 60s", done.await(60, TimeUnit.SECONDS));
                    if (firstError.get() != null) {
                        throw new AssertionError("worker error", firstError.get());
                    }
                    Assert.assertEquals("every query must see at least the 200 pre-loaded rows",
                            queryThreads * queriesPerThread, okCount.get());
                }
            }
        });
    }

    /**
     * One thread submits two queries that execute concurrently on a 2-slot
     * pool. Uses {@link QuestDB#newQuery()} to obtain two independent handles
     * (the thread-local {@code query()} is single-flight).
     * <p>
     * Concurrency proof: each handler stalls in {@code onBatch} for 1s. If
     * the second submit had to wait for the first, total elapsed would be
     * ~2s; if they run in parallel, total elapsed is ~1s. We assert the
     * elapsed is comfortably under 2s.
     */
    @Test
    public void testTwoConcurrentQueriesFromSameThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE cc(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO cc SELECT x, x::TIMESTAMP FROM long_sequence(10)");
                server.awaitTable("cc");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(2).build()) {
                    final long STALL_MS = 1_000;
                    CountDownLatch bothBatchesEntered = new CountDownLatch(2);
                    AtomicReference<Throwable> firstError = new AtomicReference<>();

                    QwpColumnBatchHandler stallingHandler = new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            bothBatchesEntered.countDown();
                            // Wait until the OTHER handler has also entered onBatch.
                            // If only one worker existed, this would deadlock at 30s,
                            // proving the test setup actually requires concurrency.
                            try {
                                if (!bothBatchesEntered.await(30, TimeUnit.SECONDS)) {
                                    firstError.compareAndSet(null,
                                            new AssertionError("only one handler entered onBatch -- not concurrent"));
                                }
                                Thread.sleep(STALL_MS);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            firstError.compareAndSet(null,
                                    new AssertionError("egress error: " + message));
                        }
                    };

                    long start = System.nanoTime();
                    Completion c1 = db.newQuery()
                            .sql("SELECT x FROM cc")
                            .handler(stallingHandler)
                            .submit();
                    Completion c2 = db.newQuery()
                            .sql("SELECT x FROM cc")
                            .handler(stallingHandler)
                            .submit();
                    c1.await();
                    c2.await();
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

                    if (firstError.get() != null) {
                        throw new AssertionError("handler failure", firstError.get());
                    }
                    Assert.assertTrue(
                            "elapsed=" + elapsedMs + "ms; with concurrency it should be ~"
                                    + STALL_MS + "ms, not ~" + (STALL_MS * 2) + "ms",
                            elapsedMs < (STALL_MS * 2) - 200);
                }
            }
        });
    }

    /**
     * Elastic pool: burst load grows the sender pool past its min, idle
     * threshold + housekeeper shrinks it back. Asserts the round-trip writes
     * succeed across the growth and that the post-shrink pool still serves
     * borrows.
     */
    @Test
    public void testElasticPoolGrowsUnderBurstAndShrinksWhenIdle() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE el(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("el");

                String config = "http::addr=127.0.0.1:" + HTTP_PORT + ";protocol_version=2;";
                try (QuestDB db = QuestDB.builder()
                        .fromConfig(config)
                        .senderPoolMin(1)
                        .senderPoolMax(4)
                        .queryPoolMin(1)
                        .queryPoolMax(2)
                        .idleTimeoutMillis(200)
                        .housekeeperIntervalMillis(100)
                        .acquireTimeoutMillis(10_000)
                        .build()) {

                    final int burst = 4;
                    CountDownLatch start = new CountDownLatch(1);
                    CountDownLatch done = new CountDownLatch(burst);
                    AtomicReference<Throwable> firstError = new AtomicReference<>();

                    for (int i = 0; i < burst; i++) {
                        final int rowId = i;
                        new Thread(() -> {
                            try {
                                start.await();
                                try (Sender s = db.borrowSender()) {
                                    s.table("el")
                                            .longColumn("x", rowId)
                                            .at(((long) (rowId + 1)) * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                                }
                            } catch (Throwable th) {
                                firstError.compareAndSet(null, th);
                            } finally {
                                done.countDown();
                            }
                        }).start();
                    }
                    start.countDown();
                    Assert.assertTrue(done.await(15, TimeUnit.SECONDS));
                    if (firstError.get() != null) {
                        throw new AssertionError("burst writer failed", firstError.get());
                    }

                    server.awaitTable("el");
                    AtomicInteger seen = new AtomicInteger();
                    db.executeSql("SELECT x FROM el", new CollectingHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            seen.addAndGet(batch.getRowCount());
                        }
                    }).await();
                    Assert.assertEquals("burst writes must all land", burst, seen.get());

                    // Let the housekeeper reap idle slots back toward min.
                    Thread.sleep(500);

                    // Pool must still serve fresh borrows after shrink.
                    try (Sender s = db.borrowSender()) {
                        s.table("el")
                                .longColumn("x", 9_999)
                                .at(99_999_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    }
                    server.awaitTable("el");
                }
            }
        });
    }

    /** Convenience handler that fails the test on error and ignores end. */
    private static class CollectingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            Assert.fail("unexpected egress error: status=" + status + ", message=" + message);
        }
    }
}
