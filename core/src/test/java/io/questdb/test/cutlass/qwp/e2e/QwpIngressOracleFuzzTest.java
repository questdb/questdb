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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.qwp.load.QwpRow;
import io.questdb.test.cutlass.qwp.load.QwpTable;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * QWP ingress fuzz tests that compare the actual table contents against an
 * in-memory oracle built up front. Every row that the test intends to publish
 * is materialized as a {@link QwpRow} (with all data types — booleans, longs,
 * doubles, strings, symbols, 1D/2D double and long arrays) and added to the
 * {@link QwpTable} oracle. Producer threads then call {@link QwpRow#publishTo}
 * to send the row through QWP. After ingestion completes, every cell of every
 * row in the table is asserted against the oracle's typed values via
 * {@link QwpTable#assertCursor}.
 *
 * <p>Two scenarios:
 * <ul>
 *   <li>{@link #testOracleMultiSenderTortureUnderServerBounces} —
 *       multiple senders publish concurrently while a bouncer thread restarts
 *       the server several times. Each sender owns its own {@code sf_dir}
 *       slot. Sender configs vary per producer (different
 *       {@code auto_flush_rows} and batch sizes). Schema evolves mid-run as
 *       producers occasionally introduce extra columns drawn from a fixed
 *       bank.</li>
 *   <li>{@link #testOracleSenderRestartReplaysAcrossBounces} — each
 *       producer opens-and-closes a fresh sender repeatedly with
 *       {@code close_flush_timeout_millis=0} so unacked frames persist in
 *       {@code sf_dir}. The next sender on the same slot recovers and
 *       replays. A bouncer interleaves server restarts. Final state must
 *       match the oracle exactly.</li>
 * </ul>
 *
 * <p>The oracle is populated up front so a producer that crashes or replays
 * cannot drift the contract: every (ts, id) pair that the producer is
 * supposed to publish carries the same column values, so dedup collapses
 * any wire-level replays cleanly.
 *
 * <p>One {@link Rnd} master seeded by {@link TestUtils#generateRandom}; each
 * producer thread, the bouncer thread, and the lifetime-loop logic each take
 * an independent sub-{@code Rnd} derived once from the master before threads
 * start, so the run is reproducible from one seed.
 */
public class QwpIngressOracleFuzzTest extends AbstractCairoTest {

    private static final int COLUMN_SKIP_FACTOR = 8;     // ~12% of rows skip a base column
    private static final String ID_COLUMN = "id";
    private static final Log LOG = LogFactory.getLog(QwpIngressOracleFuzzTest.class);
    private static final int NEW_COLUMN_FACTOR = 16;     // ~6% of rows inject an extra column
    private static final int NON_ASCII_FACTOR = 4;       // ~25% of string/symbol values get a non-ASCII suffix
    // Bank chosen to span the UTF-8 byte-length spectrum so the wire path
    // exercises 1/2/3/4-byte encoding and (for the emoji) Java surrogate
    // pair handling on the client side.
    private static final String[] NON_ASCII_SUFFIXES = {
            "é",    // U+00E9 (2-byte UTF-8)
            "ñ",    // U+00F1 (2-byte)
            "ж",    // U+0436 (2-byte)
            "Я",    // U+042F (2-byte)
            "日",    // U+65E5 (3-byte)
            "中",    // U+4E2D (3-byte)
            "한",    // U+D55C (3-byte)
            "🎉",   // U+1F389 (4-byte, surrogate pair in Java)
    };
    private static final String TABLE_NAME = "qwp_oracle_fuzz";
    private static final String TS_COLUMN = "ts";

    @Test
    public void testOracleAsyncConnectQueuesBeforeServerStarts() throws Exception {
        // The offline-first contract of initial_connect_retry=async:
        // Sender.fromConfig must return immediately even when no server
        // is listening on the target port. Producer-thread API works
        // straight away — frames pile up in sf_dir while the I/O thread
        // retries connect in the background. Once the server is brought
        // up, the queued frames drain.
        //
        // Test shape: producers open against a port where nothing is
        // listening, push their entire row range, briefly settle, then
        // a starter thread brings the server up. Senders close with a
        // generous flush timeout so close() blocks until the queued
        // frames are ACKed. Final cell-by-cell oracle check confirms no
        // loss across the offline -> online transition.
        assertMemoryLeak(() -> {
            Rnd master = TestUtils.generateRandom(LOG);
            createTargetTable();
            int port = RestartableQwpServer.pickFreePort();

            int producerCount = 2 + master.nextInt(2);              // 2..3
            int rowsPerProducer = 500 + master.nextInt(800);        // 500..1299
            long sfMaxBytes = pickSfMaxBytes(master);
            LOG.info().$("async-connect test sf_max_bytes=").$(sfMaxBytes).$();

            long baseTsMicros = 1_700_000_000_000_000L;
            QwpTable oracle = new QwpTable(TABLE_NAME);
            QwpRow[][] perProducerRows = new QwpRow[producerCount][rowsPerProducer];
            long globalIdx = 0;
            for (int p = 0; p < producerCount; p++) {
                Rnd genRnd = new Rnd(master.nextLong(), master.nextLong());
                for (int r = 0; r < rowsPerProducer; r++) {
                    long id = globalIdx;
                    long ts = baseTsMicros + globalIdx;
                    QwpRow row = generateRow(genRnd, id, ts);
                    perProducerRows[p][r] = row;
                    oracle.addRow(row);
                    globalIdx++;
                }
            }

            String[] sfDirs = new String[producerCount];
            for (int p = 0; p < producerCount; p++) {
                sfDirs[p] = freshSfDir("oracle-async-p" + p);
            }

            // Server stays unbuilt for now. We bind the port only once
            // the producers have queued their rows.
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                AtomicReferenceArray<Throwable> producerErrors = new AtomicReferenceArray<>(producerCount);
                CountDownLatch producersDone = new CountDownLatch(producerCount);
                CountDownLatch allEnqueued = new CountDownLatch(producerCount);

                Thread[] producers = new Thread[producerCount];
                for (int p = 0; p < producerCount; p++) {
                    final int pp = p;
                    final QwpRow[] myRows = perProducerRows[pp];
                    final String mySfDir = sfDirs[pp];
                    producers[p] = new Thread(() -> {
                        try {
                            // close_flush_timeout_millis=120000 so close()
                            // blocks long enough for the I/O thread to
                            // reach the (eventually-up) server, drain, ACK.
                            // Cap per-frame size: chunk-flush every 50
                            // rows. A single un-flushed batch of all
                            // 500-1300 rows would exceed the server's
                            // default WS frame limit and the I/O thread
                            // would raise PROTOCOL_VIOLATION on first
                            // contact.
                            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + mySfDir
                                    + ";initial_connect_retry=async"
                                    + ";reconnect_max_duration_millis=120000"
                                    + ";reconnect_initial_backoff_millis=20"
                                    + ";reconnect_max_backoff_millis=200"
                                    + ";sf_max_bytes=" + sfMaxBytes
                                    + ";close_flush_timeout_millis=120000;";
                            long t0 = System.nanoTime();
                            try (Sender sender = Sender.fromConfig(connect)) {
                                long ctorElapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                                if (ctorElapsedMs > 2_000L) {
                                    throw new AssertionError(
                                            "async Sender.fromConfig must return fast even with no server, "
                                                    + "took " + ctorElapsedMs + "ms");
                                }
                                int chunkSize = 50;
                                for (int i = 0; i < myRows.length; i++) {
                                    myRows[i].publishTo(sender, TABLE_NAME, ID_COLUMN);
                                    if ((i + 1) % chunkSize == 0) {
                                        sender.flush();
                                    }
                                }
                                sender.flush();
                                allEnqueued.countDown();
                            }
                        } catch (Throwable t) {
                            producerErrors.set(pp, t);
                            allEnqueued.countDown();
                        } finally {
                            producersDone.countDown();
                            Path.clearThreadLocals();
                        }
                    }, "qwp-oracle-async-p" + pp);
                }

                AtomicReference<Throwable> starterError = new AtomicReference<>();
                CountDownLatch serverStarted = new CountDownLatch(1);
                Thread starter = new Thread(() -> {
                    try {
                        // Wait for producers to enqueue everything to disk
                        // (or fail). Up to 60s — generous for slow CI.
                        if (!allEnqueued.await(60, TimeUnit.SECONDS)) {
                            throw new AssertionError("producers did not enqueue within 60s");
                        }
                        // Brief settle so the I/O thread has at minimum
                        // hit one ECONNREFUSED retry — that exercises the
                        // ASYNC contract (background connect loop) rather
                        // than letting the first connect attempt happen
                        // post-server-up.
                        Os.sleep(100);
                        server.start();
                        serverStarted.countDown();
                    } catch (Throwable t) {
                        starterError.set(t);
                        serverStarted.countDown();
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "qwp-oracle-async-starter");

                for (Thread t : producers) t.start();
                starter.start();

                if (!producersDone.await(180, TimeUnit.SECONDS)) {
                    throw new AssertionError("producers did not finish within 180s");
                }
                if (!serverStarted.await(60, TimeUnit.SECONDS)) {
                    throw new AssertionError("starter did not finish within 60s");
                }
                if (starterError.get() != null) {
                    throw new AssertionError("starter failed", starterError.get());
                }
                for (int p = 0; p < producerCount; p++) {
                    Throwable t = producerErrors.get(p);
                    if (t != null) {
                        throw new AssertionError("producer " + p + " failed", t);
                    }
                }

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                assertOracle(oracle);
                assertSlotsPurged(sfDirs, slotCapFor(sfMaxBytes));
            }
        });
    }

    @Test
    public void testOracleMultiSenderTortureUnderServerBounces() throws Exception {
        assertMemoryLeak(() -> {
            Rnd master = TestUtils.generateRandom(LOG);
            createTargetTable();
            int port = RestartableQwpServer.pickFreePort();

            int producerCount = 2 + master.nextInt(3);              // 2..4
            int rowsPerProducer = 1_000 + master.nextInt(1_500);    // 1000..2499
            int bounces = 2 + master.nextInt(3);                    // 2..4
            long sfMaxBytes = pickSfMaxBytes(master);
            LOG.info().$("multi-sender test sf_max_bytes=").$(sfMaxBytes).$();

            // Pre-generate oracle: each producer owns a contiguous slice of
            // rows. ids and timestamps are globally unique (interleaved
            // across producers) so the cursor's ts ASC reading order has a
            // single deterministic interpretation.
            long baseTsMicros = 1_700_000_000_000_000L;
            QwpTable oracle = new QwpTable(TABLE_NAME);
            QwpRow[][] perProducerRows = new QwpRow[producerCount][rowsPerProducer];
            int[] batchSizes = new int[producerCount];
            int[] autoFlushRows = new int[producerCount];
            for (int p = 0; p < producerCount; p++) {
                batchSizes[p] = 10 + master.nextInt(80);
                autoFlushRows[p] = 50 + master.nextInt(200);
            }
            Rnd bouncerRnd = new Rnd(master.nextLong(), master.nextLong());
            long globalIdx = 0;
            for (int p = 0; p < producerCount; p++) {
                Rnd genRnd = new Rnd(master.nextLong(), master.nextLong());
                for (int r = 0; r < rowsPerProducer; r++) {
                    long id = globalIdx;
                    long ts = baseTsMicros + globalIdx;
                    QwpRow row = generateRow(genRnd, id, ts);
                    perProducerRows[p][r] = row;
                    oracle.addRow(row);
                    globalIdx++;
                }
            }

            String[] sfDirs = new String[producerCount];
            for (int p = 0; p < producerCount; p++) {
                sfDirs[p] = freshSfDir("oracle-multi-p" + p);
            }

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();

                AtomicReferenceArray<Throwable> producerErrors = new AtomicReferenceArray<>(producerCount);
                CountDownLatch producersDone = new CountDownLatch(producerCount);
                AtomicReference<Throwable> bouncerError = new AtomicReference<>();
                CountDownLatch bouncerDone = new CountDownLatch(1);

                Thread[] producers = new Thread[producerCount];
                for (int p = 0; p < producerCount; p++) {
                    final int pp = p;
                    final QwpRow[] myRows = perProducerRows[pp];
                    final String mySfDir = sfDirs[pp];
                    final int batchSize = batchSizes[pp];
                    final int autoFlush = autoFlushRows[pp];
                    producers[p] = new Thread(() -> {
                        try {
                            // initial_connect_retry=async: Sender.fromConfig
                            // returns immediately and the I/O thread connects
                            // in the background. Without this the producer's
                            // initial WS upgrade can race the bouncer mid-
                            // handshake (server killed between TCP connect
                            // and WS upgrade -> peer-disconnect), which is
                            // realistic but defeats the test's point.
                            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + mySfDir
                                    + ";initial_connect_retry=async"
                                    + ";reconnect_max_duration_millis=120000"
                                    + ";close_flush_timeout_millis=120000"
                                    + ";sf_max_bytes=" + sfMaxBytes
                                    + ";auto_flush_rows=" + autoFlush + ";";
                            try (Sender sender = Sender.fromConfig(connect)) {
                                int written = 0;
                                while (written < myRows.length) {
                                    int end = Math.min(written + batchSize, myRows.length);
                                    for (int i = written; i < end; i++) {
                                        myRows[i].publishTo(sender, TABLE_NAME, ID_COLUMN);
                                    }
                                    sender.flush();
                                    written = end;
                                    Os.sleep(1);
                                }
                            }
                        } catch (Throwable t) {
                            producerErrors.set(pp, t);
                        } finally {
                            producersDone.countDown();
                            Path.clearThreadLocals();
                        }
                    }, "qwp-oracle-producer-" + pp);
                }

                Thread bouncer = new Thread(() -> {
                    try {
                        Os.sleep(150); // let producers warm up
                        for (int i = 0; i < bounces; i++) {
                            LOG.info().$("oracle bounce ").$(i + 1).$('/').$(bounces).$();
                            server.stop();
                            Os.sleep(40 + bouncerRnd.nextInt(60));
                            server.start();
                            Os.sleep(150 + bouncerRnd.nextInt(250));
                        }
                    } catch (Throwable t) {
                        bouncerError.set(t);
                    } finally {
                        bouncerDone.countDown();
                    }
                }, "qwp-oracle-bouncer");

                for (Thread t : producers) t.start();
                bouncer.start();

                if (!bouncerDone.await(120, TimeUnit.SECONDS)) {
                    throw new AssertionError("bouncer timed out");
                }
                if (bouncerError.get() != null) {
                    throw new AssertionError("bouncer failed", bouncerError.get());
                }

                if (!producersDone.await(240, TimeUnit.SECONDS)) {
                    throw new AssertionError("producers timed out");
                }
                for (int p = 0; p < producerCount; p++) {
                    Throwable t = producerErrors.get(p);
                    if (t != null) {
                        throw new AssertionError("producer " + p + " failed", t);
                    }
                }

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                assertOracle(oracle);
                assertSlotsPurged(sfDirs, slotCapFor(sfMaxBytes));
            }
        });
    }

    @Test
    public void testOraclePoisonRowsTriggerErrorHandler() throws Exception {
        // Pin down the per-batch error contract:
        //   1. The async SenderErrorHandler fires for every poisoned chunk.
        //   2. Rows from clean chunks land exactly per the oracle.
        //   3. No row from a poisoned chunk leaks into the table — the
        //      *whole* chunk is dropped, including any well-formed rows
        //      sitting next to the bad one. This documents the per-frame
        //      drop granularity (Sf does not drop per row).
        //
        // No server bouncing here on purpose — failure mode must be
        // unambiguously the per-batch rejection, not a transport blip.
        assertMemoryLeak(() -> {
            Rnd master = TestUtils.generateRandom(LOG);
            createTargetTable();
            int port = RestartableQwpServer.pickFreePort();

            int producerCount = 2 + master.nextInt(2);              // 2..3
            int chunksPerProducer = 30 + master.nextInt(30);        // 30..59
            int chunkSize = 5 + master.nextInt(6);                  // 5..10 rows; small enough to map to one frame
            int poisonChunkInN = 4;                                 // ~25% chunks poisoned
            long sfMaxBytes = pickSfMaxBytes(master);
            LOG.info().$("poison test sf_max_bytes=").$(sfMaxBytes).$();

            long baseTsMicros = 1_700_000_000_000_000L;
            QwpTable oracle = new QwpTable(TABLE_NAME);
            QwpRow[][][] perProducerChunks = new QwpRow[producerCount][chunksPerProducer][chunkSize];
            StringBuilder poisonedIdInList = new StringBuilder();
            int totalPoisonedChunks = 0;

            long globalIdx = 0;
            for (int p = 0; p < producerCount; p++) {
                Rnd genRnd = new Rnd(master.nextLong(), master.nextLong());
                Rnd poisonRnd = new Rnd(master.nextLong(), master.nextLong());
                for (int c = 0; c < chunksPerProducer; c++) {
                    boolean poisoned = poisonRnd.nextInt(poisonChunkInN) == 0;
                    if (poisoned) {
                        totalPoisonedChunks++;
                    }
                    for (int r = 0; r < chunkSize; r++) {
                        long id = globalIdx;
                        long ts = baseTsMicros + globalIdx;
                        QwpRow row = generateRow(genRnd, id, ts);
                        if (poisoned) {
                            // hh=1 forces the unscaled value past 2^192 ~ 6.3e57,
                            // well past DECIMAL(50,6)'s 10^50 cap. Server returns
                            // WRITE_ERROR with DROP_AND_CONTINUE policy.
                            row.setDecimal256("dec256", 1L, 0L, 0L, 0L, 6);
                            if (!poisonedIdInList.isEmpty()) {
                                poisonedIdInList.append(',');
                            }
                            poisonedIdInList.append(id);
                        } else {
                            oracle.addRow(row);
                        }
                        perProducerChunks[p][c][r] = row;
                        globalIdx++;
                    }
                }
            }

            String[] sfDirs = new String[producerCount];
            for (int p = 0; p < producerCount; p++) {
                sfDirs[p] = freshSfDir("oracle-poison-p" + p);
            }

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();

                AtomicReferenceArray<Throwable> producerErrors = new AtomicReferenceArray<>(producerCount);
                CountDownLatch producersDone = new CountDownLatch(producerCount);
                AtomicInteger errorHandlerCalls = new AtomicInteger();

                Thread[] producers = new Thread[producerCount];
                for (int p = 0; p < producerCount; p++) {
                    final int pp = p;
                    final QwpRow[][] myChunks = perProducerChunks[pp];
                    final String mySfDir = sfDirs[pp];
                    producers[p] = new Thread(() -> {
                        try {
                            // Generous error_inbox_capacity so a burst of
                            // poisoned chunks can't drop notifications and
                            // skew the count assertion.
                            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + mySfDir
                                    + ";initial_connect_retry=true"
                                    + ";reconnect_max_duration_millis=120000"
                                    + ";close_flush_timeout_millis=120000"
                                    + ";sf_max_bytes=" + sfMaxBytes
                                    + ";error_inbox_capacity=4096;";
                            SenderErrorHandler handler = (SenderError _) -> errorHandlerCalls.incrementAndGet();
                            try (Sender sender = Sender.builder(connect).errorHandler(handler).build()) {
                                for (int c = 0; c < myChunks.length; c++) {
                                    for (int r = 0; r < myChunks[c].length; r++) {
                                        myChunks[c][r].publishTo(sender, TABLE_NAME, ID_COLUMN);
                                    }
                                    // Explicit flush per chunk -> chunk == frame
                                    // (for these small chunk sizes), making the
                                    // per-batch drop deterministic to model.
                                    sender.flush();
                                }
                            }
                        } catch (Throwable t) {
                            producerErrors.set(pp, t);
                        } finally {
                            producersDone.countDown();
                            Path.clearThreadLocals();
                        }
                    }, "qwp-oracle-poison-p" + pp);
                }

                for (Thread t : producers) t.start();
                if (!producersDone.await(180, TimeUnit.SECONDS)) {
                    throw new AssertionError("producers timed out");
                }
                for (int p = 0; p < producerCount; p++) {
                    Throwable t = producerErrors.get(p);
                    if (t != null) {
                        throw new AssertionError("producer " + p + " failed", t);
                    }
                }

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                // (a) Clean rows: every row in every clean chunk lands
                // exactly once; oracle drives a typed cell-by-cell check.
                assertOracle(oracle);

                // (b) Poisoned rows: no id from any poisoned chunk leaked
                // into the table. This pins the per-batch drop semantic --
                // even good rows in a bad chunk must be absent.
                if (!poisonedIdInList.isEmpty()) {
                    TestUtils.assertSql(engine, sqlExecutionContext,
                            "SELECT count() FROM " + TABLE_NAME
                                    + " WHERE id IN (" + poisonedIdInList + ")",
                            sink,
                            "count\n0\n");
                }

                // (c) Async error notifications: at least one per poisoned
                // chunk reached the handler. Inequality (>=) tolerates the
                // possibility that a chunk gets split across more than one
                // frame; with chunk sizes 5..10 that should be rare but
                // we don't want a flake on the rare event.
                long observed = errorHandlerCalls.get();
                if (observed < totalPoisonedChunks) {
                    throw new AssertionError("error handler fired " + observed
                            + " times, expected at least " + totalPoisonedChunks
                            + " (poisoned chunks)");
                }
                LOG.info().$("poison test: poisoned chunks=").$(totalPoisonedChunks)
                        .$(" handler calls=").$(observed).$();

                assertSlotsPurged(sfDirs, slotCapFor(sfMaxBytes));
            }
        });
    }

    @Test
    public void testOracleSenderRestartReplaysAcrossBounces() throws Exception {
        assertMemoryLeak(() -> {
            Rnd master = TestUtils.generateRandom(LOG);
            createTargetTable();
            int port = RestartableQwpServer.pickFreePort();

            int producerCount = 2 + master.nextInt(2);              // 2..3
            int rowsPerProducer = 600 + master.nextInt(900);        // 600..1499
            int bounces = 1 + master.nextInt(2);                    // 1..2
            long sfMaxBytes = pickSfMaxBytes(master);
            LOG.info().$("restart-replay test sf_max_bytes=").$(sfMaxBytes).$();

            long baseTsMicros = 1_700_000_000_000_000L;
            QwpTable oracle = new QwpTable(TABLE_NAME);
            QwpRow[][] perProducerRows = new QwpRow[producerCount][rowsPerProducer];
            Rnd[] lifetimeRnds = new Rnd[producerCount];
            for (int p = 0; p < producerCount; p++) {
                lifetimeRnds[p] = new Rnd(master.nextLong(), master.nextLong());
            }
            Rnd bouncerRnd = new Rnd(master.nextLong(), master.nextLong());
            long globalIdx = 0;
            for (int p = 0; p < producerCount; p++) {
                Rnd genRnd = new Rnd(master.nextLong(), master.nextLong());
                for (int r = 0; r < rowsPerProducer; r++) {
                    long id = globalIdx;
                    long ts = baseTsMicros + globalIdx;
                    QwpRow row = generateRow(genRnd, id, ts);
                    perProducerRows[p][r] = row;
                    oracle.addRow(row);
                    globalIdx++;
                }
            }

            String[] sfDirs = new String[producerCount];
            for (int p = 0; p < producerCount; p++) {
                sfDirs[p] = freshSfDir("oracle-restart-p" + p);
            }

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();

                AtomicReferenceArray<Throwable> producerErrors = new AtomicReferenceArray<>(producerCount);
                CountDownLatch producersDone = new CountDownLatch(producerCount);
                AtomicReference<Throwable> bouncerError = new AtomicReference<>();
                CountDownLatch bouncerDone = new CountDownLatch(1);

                Thread[] producers = new Thread[producerCount];
                for (int p = 0; p < producerCount; p++) {
                    final int pp = p;
                    final QwpRow[] myRows = perProducerRows[pp];
                    final String mySfDir = sfDirs[pp];
                    final Rnd rnd = lifetimeRnds[pp];
                    producers[p] = new Thread(() -> {
                        try {
                            // Lifetime loop: open a sender, publish a chunk,
                            // close with timeout=0 (leaves unacked frames on
                            // disk for the next sender to recover and
                            // replay), repeat until all rows handed off.
                            // initial_connect_retry=async lets the Sender ctor
                            // return immediately even when a coincident bounce
                            // has the server down; the I/O thread retries in
                            // the background while the producer continues
                            // appending frames to sf_dir.
                            int written = 0;
                            while (written < myRows.length) {
                                int chunk = 30 + rnd.nextInt(200);
                                int end = Math.min(written + chunk, myRows.length);
                                String connect = "ws::addr=localhost:" + port + ";sf_dir=" + mySfDir
                                        + ";initial_connect_retry=async"
                                        + ";reconnect_max_duration_millis=120000"
                                        + ";sf_max_bytes=" + sfMaxBytes
                                        + ";close_flush_timeout_millis=0;";
                                try (Sender sender = Sender.fromConfig(connect)) {
                                    for (int i = written; i < end; i++) {
                                        myRows[i].publishTo(sender, TABLE_NAME, ID_COLUMN);
                                    }
                                    if (rnd.nextBoolean()) {
                                        sender.flush();
                                    }
                                }
                                written = end;
                            }
                            // Final drain pass: open one more sender with
                            // default close_flush_timeout so any residual
                            // unacked frames replay and then ACK before we
                            // assert the oracle.
                            String drainConnect = "ws::addr=localhost:" + port + ";sf_dir=" + mySfDir
                                    + ";initial_connect_retry=async"
                                    + ";reconnect_max_duration_millis=120000"
                                    + ";sf_max_bytes=" + sfMaxBytes + ";";
                            try (Sender sender = Sender.fromConfig(drainConnect)) {
                                sender.flush();
                            }
                        } catch (Throwable t) {
                            producerErrors.set(pp, t);
                        } finally {
                            producersDone.countDown();
                            Path.clearThreadLocals();
                        }
                    }, "qwp-oracle-restart-p" + pp);
                }

                Thread bouncer = new Thread(() -> {
                    try {
                        Os.sleep(200);
                        for (int i = 0; i < bounces; i++) {
                            LOG.info().$("restart-test bounce ").$(i + 1).$('/').$(bounces).$();
                            server.stop();
                            Os.sleep(80 + bouncerRnd.nextInt(120));
                            server.start();
                            Os.sleep(300 + bouncerRnd.nextInt(400));
                        }
                    } catch (Throwable t) {
                        bouncerError.set(t);
                    } finally {
                        bouncerDone.countDown();
                    }
                }, "qwp-oracle-restart-bouncer");

                for (Thread t : producers) t.start();
                bouncer.start();

                if (!bouncerDone.await(120, TimeUnit.SECONDS)) {
                    throw new AssertionError("bouncer timed out");
                }
                if (bouncerError.get() != null) {
                    throw new AssertionError("bouncer failed", bouncerError.get());
                }

                if (!producersDone.await(300, TimeUnit.SECONDS)) {
                    throw new AssertionError("producers timed out");
                }
                for (int p = 0; p < producerCount; p++) {
                    Throwable t = producerErrors.get(p);
                    if (t != null) {
                        throw new AssertionError("producer " + p + " failed", t);
                    }
                }

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                assertOracle(oracle);
                assertSlotsPurged(sfDirs, slotCapFor(sfMaxBytes));
            }
        });
    }

    /**
     * Assert each sender's SF slot directory has been purged of sealed
     * segments. After a clean close (with a non-zero
     * {@code close_flush_timeout_millis}) every published frame is ACKed
     * and the SF cursor deletes the rotated segments. A small residue is
     * normal: the lock file, the active segment header, control files. We
     * use a generous per-slot bound; a regression that stops purging
     * altogether would push slots well past it.
     */
    private static void assertSlotsPurged(String[] sfDirs, long maxBytesPerSlot) {
        for (String dir : sfDirs) {
            // default sender_id slot — see Sender config docs in
            // QwpIngressServerRestartFuzzTest header.
            File slotDir = new File(dir, "default");
            long totalBytes = directorySizeBytes(slotDir);
            if (totalBytes > maxBytesPerSlot) {
                StringBuilder fileList = new StringBuilder();
                File[] files = slotDir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        fileList.append(f.getName()).append('=').append(f.length()).append(' ');
                    }
                }
                throw new AssertionError("SF slot " + dir + " not purged after clean close: "
                        + totalBytes + " bytes (cap " + maxBytesPerSlot + "). Files: " + fileList);
            }
            LOG.info().$("sf_dir purge ok: ").$(dir).$(" bytes=").$(totalBytes).$();
        }
    }

    private static double[] deriveDoubleArr1d(long id, double sign) {
        return new double[]{id * sign, id * 2.0 * sign, id * 3.0 * sign};
    }

    private static double[][] deriveDoubleArr2d(long id, double sign) {
        return new double[][]{
                {id * sign, id * 2.0 * sign},
                {id * 3.0 * sign, id * 4.0 * sign}
        };
    }

    private static double[][][] deriveDoubleArr3d(long id, double sign) {
        // 2x2x3 -> 12 elements, exercising three independent strides
        return new double[][][]{
                {
                        {id * sign, id * 2.0 * sign, id * 3.0 * sign},
                        {id * 4.0 * sign, id * 5.0 * sign, id * 6.0 * sign}
                },
                {
                        {id * 7.0 * sign, id * 8.0 * sign, id * 9.0 * sign},
                        {id * 10.0 * sign, id * 11.0 * sign, id * 12.0 * sign}
                }
        };
    }

    private static long directorySizeBytes(File dir) {
        if (!dir.exists()) {
            return 0;
        }
        long sum = 0;
        File[] children = dir.listFiles();
        if (children == null) {
            return 0;
        }
        for (File f : children) {
            sum += f.isDirectory() ? directorySizeBytes(f) : f.length();
        }
        return sum;
    }

    private static QwpRow generateRow(Rnd rnd, long id, long tsMicros) {
        QwpRow row = new QwpRow(id, tsMicros);
        // BOOLEAN is mandatory: it has no NULL representation, so an absent
        // BOOLEAN cell would be indistinguishable from a stored 'false'.
        row.setBool("b", (id & 1L) == 0L);
        // Sign flips are independent per column to maximize coverage of
        // sign-bit handling in the SF wire encoder. id stays positive
        // because it's the dedup key and sorting alongside ts gives a
        // single deterministic interpretation.
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) row.setLong("l", maybeNegate(rnd, id * 1_000_003L));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) row.setDouble("d", maybeNegate(rnd, id * 1.5));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) row.setString("s", "s_" + id + maybeNonAscii(rnd));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) row.setSymbol("sym", "sym_" + (id & 0xFL) + maybeNonAscii(rnd));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR))
            row.setDoubleArray1d("da", deriveDoubleArr1d(id, rnd.nextBoolean() ? -1.0 : 1.0));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR))
            row.setDoubleArray2d("da2", deriveDoubleArr2d(id, rnd.nextBoolean() ? -1.0 : 1.0));
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR))
            row.setDoubleArray3d("da3", deriveDoubleArr3d(id, rnd.nextBoolean() ? -1.0 : 1.0));
        // DECIMALs: skippable like other base columns to exercise the
        // NULL path through SF replay. NULL is detected on the assertion
        // side via Decimals.DECIMAL64_NULL and Decimal128/256.isNull().
        //
        // Values are sized to push more of each width's bit space while
        // staying inside the declared column precision (max id ~10_000):
        //   DECIMAL(12,3) (max ~10^12, ~40 bits) -> ~10^11 (~37 bits)
        //   DECIMAL(25,4) (max ~10^25, ~84 bits) -> hi up to ~5e5 plus
        //                                           full 64-bit lo
        //   DECIMAL(50,6) (max ~10^50, ~167 bits) -> hh=0, hl up to ~10^10
        //                                            plus full 64-bit lh, ll
        // The server rejects out-of-range values per binary frame with
        // DROP_AND_CONTINUE; the poison test pins that contract.
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) {
            setSignedDecimal64(row, "dec64", id * 10_000_007L + 13L, 3, rnd.nextBoolean());
        }
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) {
            setSignedDecimal128(row, "dec128", id * 40L + 7L, id * 0xDEADBEEFL + 17L, 4, rnd.nextBoolean());
        }
        if (!shouldFuzz(rnd, COLUMN_SKIP_FACTOR)) {
            setSignedDecimal256(row, "dec256",
                    0L,
                    id * 0x123456L + 31L,
                    id * 0xCAFEBABEL + 17L,
                    id * 0xDEADBEEFL - 13L,
                    6,
                    rnd.nextBoolean());
        }
        if (shouldFuzz(rnd, NEW_COLUMN_FACTOR)) {
            injectExtra(rnd, row, id);
        }
        return row;
    }

    private static void injectExtra(Rnd rnd, QwpRow row, long id) {
        // Server auto-creates these columns on first write. For DECIMAL
        // extras the server picks the storage size from the wire-protocol
        // type tag (Decimal64 -> DECIMAL64 prec=18; Decimal128 -> prec=38;
        // Decimal256 -> prec=76) and the scale from the value itself, so
        // we fuzz scale (and, for Decimal256, the hh limb) within each
        // type's auto-precision envelope -- this exercises the full
        // wire-protocol scale field rather than a single hardcoded scale.
        // The oracle records column presence per-row; rows that didn't
        // write the extra expect type-default NULL at assertion time.
        // Numeric extras get the same independent sign-flip treatment as
        // base columns.
        switch (rnd.nextInt(14)) {
            case 0:
                row.setLong("ex_l_0", maybeNegate(rnd, id * 7L));
                break;
            case 1:
                row.setLong("ex_l_1", maybeNegate(rnd, id + 100L));
                break;
            case 2:
                row.setLong("ex_l_2", maybeNegate(rnd, id));
                break;
            case 3:
                row.setDouble("ex_d_0", maybeNegate(rnd, id * 0.25));
                break;
            case 4:
                row.setDouble("ex_d_1", maybeNegate(rnd, id));
                break;
            case 5:
                row.setDouble("ex_d_2", maybeNegate(rnd, id * 13.7));
                break;
            case 6:
                row.setString("ex_s_0", "ex0_" + id + maybeNonAscii(rnd));
                break;
            case 7:
                row.setString("ex_s_1", "ex1_" + id + maybeNonAscii(rnd));
                break;
            case 8:
                row.setSymbol("ex_sym_0", "exsym0_" + (id & 0x7L) + maybeNonAscii(rnd));
                break;
            case 9:
                row.setSymbol("ex_sym_1", "exsym1_" + (id & 0x3L) + maybeNonAscii(rnd));
                break;
            case 10:
                double sign = rnd.nextBoolean() ? -1.0 : 1.0;
                row.setDoubleArray1d("ex_da_0", new double[]{id * sign, (id + 1) * sign, (id + 2) * sign});
                break;
            case 11: {
                // DECIMAL64 auto-precision is 18; scale 0..15 stays inside.
                // The column's scale is locked to the first row's scale, so
                // we encode the scale in the column name -- each randomly
                // chosen scale lands in its own column with a stable scale.
                int scale = rnd.nextInt(16);
                setSignedDecimal64(row, "ex_dec64_s" + scale, id * 7L + 11L, scale, rnd.nextBoolean());
                break;
            }
            case 12: {
                // DECIMAL128 auto-precision is 38 (max unscaled ~10^38, ~2^126).
                // hi up to id * 11 (~37 bits) plus 64-bit lo stays well inside.
                // Scale encoded in column name (see case 11) and spans 0..18.
                int scale = rnd.nextInt(19);
                setSignedDecimal128(row, "ex_dec128_s" + scale, id * 11L + 3L, id * 0xDEADBEEFL + 17L,
                        scale, rnd.nextBoolean());
                break;
            }
            case 13: {
                // DECIMAL256 auto-precision is 76 (max unscaled ~10^76, ~2^252).
                // hh up to ~2^32 keeps the unscaled value well inside that
                // bound across all (hl, lh, ll) limb combinations -- this
                // fuzzes the top 64 bits of the limb stack, which the
                // hardcoded hh=0 used to leave dead. Scale encoded in column
                // name (see case 11) and spans 0..30.
                long hh = id * 0xABCD_EF01L + 7L;
                int scale = rnd.nextInt(31);
                setSignedDecimal256(row, "ex_dec256_s" + scale,
                        hh,
                        id * 0x123456L + 31L,
                        id * 0xCAFEBABEL + 17L,
                        id * 0xDEADBEEFL - 13L,
                        scale,
                        rnd.nextBoolean());
                break;
            }
        }
    }

    private static double maybeNegate(Rnd rnd, double v) {
        return rnd.nextBoolean() ? -v : v;
    }

    private static long maybeNegate(Rnd rnd, long v) {
        return rnd.nextBoolean() ? -v : v;
    }

    private static String maybeNonAscii(Rnd rnd) {
        return shouldFuzz(rnd, NON_ASCII_FACTOR)
                ? NON_ASCII_SUFFIXES[rnd.nextInt(NON_ASCII_SUFFIXES.length)]
                : "";
    }

    /**
     * Pick a per-test {@code sf_max_bytes} value from a fixed pool. Smaller
     * segments force frequent rotation (stresses purge bookkeeping); larger
     * segments resemble the production default (4 MiB). The chosen value is
     * threaded into every Sender config string for the test and into
     * {@link #slotCapFor} so the post-close assertion scales accordingly.
     */
    private static long pickSfMaxBytes(Rnd rnd) {
        long[] pool = {64L * 1024, 256L * 1024, 1024L * 1024, 4L * 1024 * 1024};
        return pool[rnd.nextInt(pool.length)];
    }

    /**
     * Decimal128's two's-complement representation across hi+lo doesn't
     * lend itself to a simple per-word negate, so route through the client
     * class's {@code negate()} method and read back the resulting bits.
     */
    private static void setSignedDecimal128(QwpRow row, String name, long hi, long lo, int scale, boolean negate) {
        if (negate) {
            Decimal128 d = new Decimal128(hi, lo, scale);
            d.negate();
            row.setDecimal128(name, d.getHigh(), d.getLow(), scale);
        } else {
            row.setDecimal128(name, hi, lo, scale);
        }
    }

    /**
     * See {@link #setSignedDecimal128}.
     */
    private static void setSignedDecimal256(QwpRow row, String name, long hh, long hl, long lh, long ll, int scale, boolean negate) {
        if (negate) {
            Decimal256 d = new Decimal256(hh, hl, lh, ll, scale);
            d.negate();
            row.setDecimal256(name, d.getHh(), d.getHl(), d.getLh(), d.getLl(), scale);
        } else {
            row.setDecimal256(name, hh, hl, lh, ll, scale);
        }
    }

    /**
     * See {@link #setSignedDecimal128}.
     */
    private static void setSignedDecimal64(QwpRow row, String name, long unscaledValue, int scale, boolean negate) {
        if (negate) {
            Decimal64 d = Decimal64.fromLong(unscaledValue, scale);
            d.negate();
            row.setDecimal64(name, d.getValue(), scale);
        } else {
            row.setDecimal64(name, unscaledValue, scale);
        }
    }

    private static boolean shouldFuzz(Rnd rnd, int factor) {
        return factor > 0 && rnd.nextInt(factor) == 0;
    }

    /**
     * Per-slot residue bound after a clean close. The active segment file may
     * still exist (sized up to {@code sf_max_bytes}) plus a few small control
     * files (lock, manifest). The 256 KiB slack covers control files even
     * when the segment itself is tiny.
     */
    private static long slotCapFor(long sfMaxBytes) {
        return sfMaxBytes + 256L * 1024;
    }

    private void assertOracle(QwpTable oracle) throws Exception {
        // Use a SQL cursor (page-frame backed) so getArray works; the bare
        // TableReader cursor only implements scalar columns. ORDER BY (ts, id)
        // matches QwpTable.assertCursor's expected snapshot order.
        String sql = "SELECT * FROM " + oracle.getTableName() + " ORDER BY ts, id";
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                oracle.assertCursor(cursor, factory.getMetadata(), ID_COLUMN, TS_COLUMN);
            }
        }
    }

    private void createTargetTable() {
        try {
            execute(
                    "CREATE TABLE " + TABLE_NAME + " ("
                            + "id LONG, "
                            + "b BOOLEAN, "
                            + "l LONG, "
                            + "d DOUBLE, "
                            + "s STRING, "
                            + "sym SYMBOL, "
                            + "da DOUBLE[], "
                            + "da2 DOUBLE[][], "
                            + "da3 DOUBLE[][][], "
                            + "dec64 DECIMAL(12,3), "
                            + "dec128 DECIMAL(25,4), "
                            + "dec256 DECIMAL(50,6), "
                            + "ts TIMESTAMP"
                            + ") TIMESTAMP(ts) PARTITION BY DAY WAL "
                            + "DEDUP UPSERT KEYS(ts, id)"
            );
        } catch (Exception e) {
            throw new AssertionError("failed to create target table", e);
        }
    }

    private String freshSfDir(String tag) throws Exception {
        File dir = temp.newFolder("qwp-sf-" + tag);
        return dir.getAbsolutePath();
    }
}
