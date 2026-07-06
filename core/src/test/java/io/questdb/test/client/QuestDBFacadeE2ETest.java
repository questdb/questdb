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
import io.questdb.client.Query;
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
 * data correctly and that the pool semantics (flush-on-return, distinct
 * per-borrow Senders, acquire timeout, cancel) hold under real I/O.
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

    private long firstBatchTimeoutMs(long baseMs) {
        // HttpResponseSink#sendBuffer parks every sendChunk bytes; a first batch
        // can be ~131 KB (MAX_ROWS_PER_BATCH=16384 LONGs) plus framing, so at the
        // worst random sendChunk=1 it needs tens of thousands of park-resume cycles.
        int effectiveChunk = Math.max(1, Math.min(sendChunk, 64));
        return baseMs * 64L / effectiveChunk;
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.connect(config)) {
                    try (Sender s = db.borrowSender()) {
                        s.table("rt").longColumn("i", 1).at(1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        s.table("rt").longColumn("i", 2).at(2L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        s.table("rt").longColumn("i", 3).at(3L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        // close() flushes -- assertion below proves it ran.
                    }
                    // ws ingest is async: close() returns before the server
                    // commits, so wait for the WAL txn to be applied (not just
                    // for the table to exist) before querying it back.
                    server.awaitTxn("rt", 1);

                    AtomicLong sum = new AtomicLong();
                    AtomicInteger rows = new AtomicInteger();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT i FROM rt").handler(new CollectingHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int r = 0; r < batch.getRowCount(); r++) {
                                    sum.addAndGet(batch.getLongValue(0, r));
                                    rows.incrementAndGet();
                                }
                            }
                        }).submit().await();
                    }
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(1).build()) {
                    AtomicReference<Byte> errorStatus = new AtomicReference<>();
                    AtomicReference<Throwable> awaitOutcome = new AtomicReference<>();
                    CountDownLatch firstBatch = new CountDownLatch(1);
                    try (Query q = db.borrowQuery()) {
                        Completion c = q.sql("SELECT x FROM big ORDER BY x").handler(new QwpColumnBatchHandler() {
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
                        }).submit();
                        Assert.assertTrue(firstBatch.await(firstBatchTimeoutMs(10_000), TimeUnit.MILLISECONDS));
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(2).senderPoolSize(2).build()) {
                    int threads = 4;
                    AtomicInteger okCount = new AtomicInteger();
                    AtomicReference<Throwable> firstError = new AtomicReference<>();
                    CountDownLatch done = new CountDownLatch(threads);
                    for (int t = 0; t < threads; t++) {
                        new Thread(() -> {
                            try {
                                AtomicInteger seen = new AtomicInteger();
                                try (Query q = db.borrowQuery()) {
                                    q.sql("SELECT x FROM conc").handler(new CollectingHandler() {
                                        @Override
                                        public void onBatch(QwpColumnBatch batch) {
                                            seen.addAndGet(batch.getRowCount());
                                        }
                                    }).submit().await();
                                }
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

    @Test(timeout = 60_000)
    public void testHandlerCannotCloseOrAwaitOnWorkerThread() throws Exception {
        // A result handler runs on the worker (dispatch) thread, so a reentrant
        // close()/await() from inside it would block forever waiting for a
        // terminal event only that thread can deliver. The reentrancy guard must
        // turn that into an immediate IllegalStateException -- proving both that
        // the handler really runs on the worker thread and that the worker is
        // not deadlocked/leaked (the next borrow still works). The method-level
        // timeout fails the test if the guard regresses into a deadlock.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE rh(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO rh SELECT x, x::TIMESTAMP FROM long_sequence(3)");
                server.awaitTable("rh");

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(1).build()) {
                    AtomicReference<Throwable> closeOutcome = new AtomicReference<>();
                    AtomicReference<Throwable> awaitOutcome = new AtomicReference<>();
                    AtomicInteger rows = new AtomicInteger();

                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT x FROM rh").handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                rows.addAndGet(batch.getRowCount());
                                // q is also the Completion (QueryLease implements both).
                                // Both calls run on the worker thread and must throw.
                                if (closeOutcome.get() == null) {
                                    try {
                                        q.close();
                                        closeOutcome.set(new AssertionError("close() must throw from a handler"));
                                    } catch (Throwable t) {
                                        closeOutcome.set(t);
                                    }
                                    try {
                                        // q is the Completion at runtime (QueryLease implements both).
                                        ((Completion) q).await();
                                        awaitOutcome.set(new AssertionError("await() must throw from a handler"));
                                    } catch (Throwable t) {
                                        awaitOutcome.set(t);
                                    }
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("egress error: " + message);
                            }
                        }).submit().await();
                    }

                    Assert.assertEquals("the handler must have run (onBatch saw the rows)", 3, rows.get());
                    Assert.assertTrue("close() from a handler must throw IllegalStateException, was: "
                                    + closeOutcome.get(),
                            closeOutcome.get() instanceof IllegalStateException);
                    Assert.assertTrue("await() from a handler must throw IllegalStateException, was: "
                                    + awaitOutcome.get(),
                            awaitOutcome.get() instanceof IllegalStateException);
                    Assert.assertTrue("the error must point at cancel() as the safe in-handler stop",
                            ((IllegalStateException) closeOutcome.get()).getMessage().contains("cancel()"));

                    // The worker must not be deadlocked or leaked: re-borrow from
                    // the size-1 pool and run a normal query to completion.
                    AtomicInteger reused = new AtomicInteger();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT x FROM rh").handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                reused.addAndGet(batch.getRowCount());
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("egress error on reuse: " + message);
                            }
                        }).submit().await();
                    }
                    Assert.assertEquals("the worker must still be usable after the in-handler misuse",
                            3, reused.get());
                }
            }
        });
    }

    @Test
    public void testQueryExceptionOnBadSql() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain _ = startFragmented()) {
                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.connect(config)) {
                    try (Query q = db.borrowQuery()) {
                        Completion c = q.sql("SELECT * FROM no_such_table").handler(new CollectingHandler()).submit();
                        try {
                            c.await();
                            Assert.fail("expected QueryException for missing table");
                        } catch (QueryException qe) {
                            Assert.assertTrue("status byte must be non-zero on server error",
                                    qe.getStatus() != 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSenderPoolExhaustionTimesOut() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain _ = startFragmented()) {
                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
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

    /**
     * One borrowed handle, many sequential submits. Asserts:
     * - within a single lease the {@link Completion} is the reused field on the
     * handle (same instance every submit)
     * - bound values reach the server on every submit
     * - re-borrowing from a size-1 pool reuses the pooled worker and its
     * pre-allocated QueryImpl, with builder state reset, so a no-binds query
     * does not carry over the prior binds (the returned lease is a fresh
     * per-borrow handle wrapping that reused state)
     */
    @Test
    public void testManySequentialQueriesOnSameHandle() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE seq(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO seq SELECT x, x::TIMESTAMP FROM long_sequence(50)");
                server.awaitTable("seq");

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.builder().fromConfig(config).queryPoolSize(1).build()) {
                    Completion firstCompletion;
                    try (Query handle = db.borrowQuery()) {
                        firstCompletion = handle
                                .sql("SELECT count() FROM seq")
                                .handler(new CollectingHandler())
                                .submit();
                        firstCompletion.await();

                        // Many sequential submits on the SAME lease: the Completion
                        // is the reused field on the handle, and bound values reach
                        // the server each time.
                        final int iterations = 50;
                        for (int i = 0; i < iterations; i++) {
                            AtomicLong observed = new AtomicLong();
                            Completion c = handle.sql("SELECT x FROM seq WHERE x = $1")
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
                                            Assert.fail("egress error: " + message);
                                        }
                                    })
                                    .submit();
                            Assert.assertSame("Completion must be reused per handle", firstCompletion, c);
                            c.await();
                            Assert.assertEquals("iter " + i + ": bound value must reach the server", 7L, observed.get());
                        }
                    }

                    // Re-borrow from the size-1 pool: the pooled worker and its
                    // reused QueryImpl come back, and resetForBorrow() cleared the
                    // prior SQL/binds/handler, so a no-binds query does not carry
                    // over the earlier $1 binds. The lease is a fresh per-borrow
                    // handle wrapping that reused state; QueryImplResetTest in the
                    // client asserts the same-pooled-object reuse directly.
                    AtomicLong total = new AtomicLong();
                    try (Query handle = db.borrowQuery()) {
                        handle.sql("SELECT count() FROM seq")
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
                    }
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
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
                                    try (Query q = db.borrowQuery()) {
                                        q.sql("SELECT x FROM mix WHERE x > $1")
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
                                                .submit()
                                                .await();
                                    }
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
     * One thread runs two queries concurrently on a 2-slot pool by borrowing
     * two {@link Query} handles (a single handle is single-flight).
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
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
                    try (Query q1 = db.borrowQuery(); Query q2 = db.borrowQuery()) {
                        Completion c1 = q1.sql("SELECT x FROM cc").handler(stallingHandler).submit();
                        Completion c2 = q2.sql("SELECT x FROM cc").handler(stallingHandler).submit();
                        c1.await();
                        c2.await();
                    }
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
     * Mixed ingest + egress over one pooled handle: the main thread borrows
     * two Senders at once -- one per table -- from a 2-slot sender pool and
     * uses them simultaneously (interleaved row-by-row, with a shared mid-flush
     * so they also transmit at once), while a dedicated reader thread streams
     * two DIFFERENT SQLs, each on its own borrowed {@link io.questdb.client.Query}
     * handle, over a 2-slot query pool.
     * <p>
     * Every {@link QuestDB#borrowSender()} call returns a distinct pooled
     * Sender with its own buffer, asserted directly with {@code assertNotSame}.
     * <p>
     * Asserts both pools hand out two leases at once without blocking, the
     * close-on-return flush lands every row, and each query sees the exact
     * rows and sum for its own table (no cross-talk between the two Senders or
     * the two Query handles).
     */
    @Test
    public void testTwoSendersPerTableWhileReaderThreadStreamsTwoQueries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE alpha(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("CREATE TABLE beta(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("alpha");
                server.awaitTable("beta");

                final int rowsAlpha = 100;
                final int rowsBeta = 50;

                // auto_flush_interval=3600000 (1h): the default 100ms interval
                // auto-flush would fire on the next at() after any >=100ms stall
                // (GC/JIT on a loaded CI agent), injecting a third txn per table.
                // That would both break the awaitTxn(name, 2) barrier below (it
                // returns at writerTxn >= 2, before the close-flush txn applies,
                // so the reader would see a partial table) and degrade the
                // choreographed shared mid-flush (a premature auto-flush drains
                // the buffer it is meant to transmit). WS rejects
                // auto_flush_interval=off outright, so a 1h interval -- orders of
                // magnitude beyond the test's runtime -- stands in for it. With
                // that, rows=1000 (> both row counts) and bytes=0, exactly two
                // non-empty flushes per table are guaranteed: the shared
                // mid-flush and the close flush.
                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";auto_flush_interval=3600000;";
                try (QuestDB db = QuestDB.builder()
                        .fromConfig(config)
                        .senderPoolSize(2)
                        .queryPoolSize(2)
                        .acquireTimeoutMillis(30_000)
                        .build()) {

                    // (1) Ingest: hold two pooled Senders at once -- one per
                    // table -- and use them simultaneously. Every borrow hands
                    // out a distinct pooled Sender with its own buffer, so a
                    // 2-slot pool leases both to one thread.
                    final int maxRows = Math.max(rowsAlpha, rowsBeta);
                    try (Sender a = db.borrowSender();
                         Sender b = db.borrowSender()) {
                        // Distinct objects: borrowSender() is not thread-local,
                        // so the two leases are independent Senders.
                        Assert.assertNotSame("borrowSender() must hand out two distinct pooled Senders", a, b);

                        // Interleave the two senders row-by-row so both buffers
                        // fill at the same time, rather than filling alpha
                        // completely before touching beta.
                        for (int i = 0; i < maxRows; i++) {
                            if (i < rowsAlpha) {
                                a.table("alpha").longColumn("x", i)
                                        .at((i + 1L) * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                            }
                            if (i < rowsBeta) {
                                b.table("beta").longColumn("x", 2L * i)
                                        .at((i + 1L) * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                            }
                            // Mid-way -- while both still have rows left to
                            // write -- flush both at once so the two senders also
                            // transmit concurrently, not merely buffer in step.
                            if (i == rowsBeta / 2) {
                                a.flush();
                                b.flush();
                            }
                        }
                        // close() on each flushes whatever remains.
                    }
                    // ws ingest is async. With interval auto-flush disabled in the
                    // config, each table sees exactly two non-empty flushes (the
                    // shared mid-flush + its close flush); wait for both.
                    server.awaitTxn("alpha", 2);
                    server.awaitTxn("beta", 2);

                    // (2) Egress: a dedicated reader thread streams two
                    // different SQLs at once, each on its own borrowed Query handle.
                    final AtomicLong alphaSum = new AtomicLong();
                    final AtomicLong betaSum = new AtomicLong();
                    final AtomicInteger alphaRows = new AtomicInteger();
                    final AtomicInteger betaRows = new AtomicInteger();
                    final AtomicReference<Throwable> firstError = new AtomicReference<>();

                    Thread reader = new Thread(() -> {
                        try (Query qa = db.borrowQuery(); Query qb = db.borrowQuery()) {
                            Completion ca = qa
                                    .sql("SELECT x FROM alpha")
                                    .handler(new QwpColumnBatchHandler() {
                                        @Override
                                        public void onBatch(QwpColumnBatch batch) {
                                            for (int r = 0; r < batch.getRowCount(); r++) {
                                                alphaSum.addAndGet(batch.getLongValue(0, r));
                                                alphaRows.incrementAndGet();
                                            }
                                        }

                                        @Override
                                        public void onEnd(long totalRows) {
                                        }

                                        @Override
                                        public void onError(byte status, String message) {
                                            firstError.compareAndSet(null,
                                                    new AssertionError("alpha egress error: " + message));
                                        }
                                    })
                                    .submit();
                            Completion cb = qb
                                    .sql("SELECT x FROM beta")
                                    .handler(new QwpColumnBatchHandler() {
                                        @Override
                                        public void onBatch(QwpColumnBatch batch) {
                                            for (int r = 0; r < batch.getRowCount(); r++) {
                                                betaSum.addAndGet(batch.getLongValue(0, r));
                                                betaRows.incrementAndGet();
                                            }
                                        }

                                        @Override
                                        public void onEnd(long totalRows) {
                                        }

                                        @Override
                                        public void onError(byte status, String message) {
                                            firstError.compareAndSet(null,
                                                    new AssertionError("beta egress error: " + message));
                                        }
                                    })
                                    .submit();
                            ca.await();
                            cb.await();
                        } catch (Throwable th) {
                            firstError.compareAndSet(null, th);
                        }
                    }, "reader");
                    reader.start();
                    reader.join(TimeUnit.SECONDS.toMillis(30));
                    Assert.assertFalse("reader thread must finish within 30s", reader.isAlive());

                    if (firstError.get() != null) {
                        throw new AssertionError("reader thread failed", firstError.get());
                    }
                    Assert.assertEquals(rowsAlpha, alphaRows.get());
                    Assert.assertEquals(rowsBeta, betaRows.get());
                    // alpha x = 0..rowsAlpha-1; beta x = 2*i for i = 0..rowsBeta-1.
                    Assert.assertEquals((rowsAlpha - 1L) * rowsAlpha / 2, alphaSum.get());
                    Assert.assertEquals((rowsBeta - 1L) * rowsBeta, betaSum.get());
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

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
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

                    // Each of the 4 burst senders flushes once on close -> 4 WAL
                    // txns. Wait for all to apply before counting.
                    server.awaitTxn("el", 4);
                    AtomicInteger seen = new AtomicInteger();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT x FROM el").handler(new CollectingHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                seen.addAndGet(batch.getRowCount());
                            }
                        }).submit().await();
                    }
                    Assert.assertEquals("burst writes must all land", burst, seen.get());

                    // Let the housekeeper reap idle slots back toward min.
                    Thread.sleep(500);

                    // Pool must still serve fresh borrows after shrink.
                    try (Sender s = db.borrowSender()) {
                        s.table("el")
                                .longColumn("x", 9_999)
                                .at(99_999_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    }
                    server.awaitTxn("el", 5);
                }
            }
        });
    }

    /**
     * Single cluster config drives both the ingest and query pools. Exercises
     * the facade's one-config path end-to-end.
     */
    @Test
    public void testSingleConfigConnectRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE two(i LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("two");

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT + ";";
                try (QuestDB db = QuestDB.connect(config)) {
                    try (Sender s = db.borrowSender()) {
                        s.table("two").longColumn("i", 11).at(1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                        s.table("two").longColumn("i", 22).at(2L * 1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    }
                    server.awaitTxn("two", 1);

                    AtomicLong sum = new AtomicLong();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT i FROM two").handler(new CollectingHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int r = 0; r < batch.getRowCount(); r++) {
                                    sum.addAndGet(batch.getLongValue(0, r));
                                }
                            }
                        }).submit().await();
                    }
                    Assert.assertEquals(33L, sum.get());
                }
            }
        });
    }

    /**
     * Pool sizing carried entirely in the connect string (no explicit builder
     * calls). The facade resolves the pool keys and the round-trip still works.
     */
    @Test
    public void testPoolKeysFromConnectStringRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startFragmented()) {
                server.execute("CREATE TABLE pk(i LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("pk");

                String config = "ws::addr=127.0.0.1:" + HTTP_PORT
                        + ";sender_pool_min=1;sender_pool_max=2;query_pool_min=1;query_pool_max=2;acquire_timeout_ms=10000;";
                try (QuestDB db = QuestDB.connect(config)) {
                    try (Sender s = db.borrowSender()) {
                        s.table("pk").longColumn("i", 5).at(1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    }
                    server.awaitTxn("pk", 1);

                    AtomicInteger rows = new AtomicInteger();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT i FROM pk").handler(new CollectingHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                rows.addAndGet(batch.getRowCount());
                            }
                        }).submit().await();
                    }
                    Assert.assertEquals(1, rows.get());
                }
            }
        });
    }

    /**
     * The {@code user}/{@code pass} aliases authenticate both the ingest and the
     * egress WebSocket upgrades: against an auth-enabled server, the correct
     * aliases round-trip and a wrong password is rejected at connect.
     */
    @Test
    public void testUserPassAliasAuthRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain server = startWithEnvVariables(
                    PropertyKey.HTTP_USER.getEnvVarName(), "admin",
                    PropertyKey.HTTP_PASSWORD.getEnvVarName(), "quest"
            )) {
                server.execute("CREATE TABLE au(i LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.awaitTable("au");

                String good = "ws::addr=127.0.0.1:" + HTTP_PORT + ";user=admin;pass=quest;";
                try (QuestDB db = QuestDB.connect(good)) {
                    try (Sender s = db.borrowSender()) {
                        s.table("au").longColumn("i", 99).at(1_000_000L, java.time.temporal.ChronoUnit.MICROS);
                    }
                    server.awaitTxn("au", 1);

                    AtomicLong got = new AtomicLong();
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT i FROM au").handler(new CollectingHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int r = 0; r < batch.getRowCount(); r++) {
                                    got.set(batch.getLongValue(0, r));
                                }
                            }
                        }).submit().await();
                    }
                    Assert.assertEquals(99L, got.get());
                }

                // Wrong password is rejected at the eager pool-prewarm connect.
                String bad = "ws::addr=127.0.0.1:" + HTTP_PORT + ";user=admin;pass=wrong;";
                try (QuestDB _ = QuestDB.connect(bad)) {
                    Assert.fail("expected auth failure with a wrong password");
                } catch (RuntimeException expected) {
                    // connect rejected the bad credentials
                }
            }
        });
    }

    /**
     * Convenience handler that fails the test on error and ignores end.
     */
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
