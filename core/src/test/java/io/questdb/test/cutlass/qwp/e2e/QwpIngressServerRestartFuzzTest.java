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

import io.questdb.client.Sender;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * QWP ingress fuzz tests that bounce the server while a Store-and-Forward
 * sender is writing. The contract being asserted: every row that the user
 * thread successfully handed off to {@code sender.flush()} (durable on disk
 * inside {@code sf_dir}) must end up in the table after the server comes
 * back, regardless of how many times the server bounces or whether the
 * sender held its connection across the bounce.
 *
 * <p>Server-side dedup is required: when an SF sender reconnects (or is
 * replaced by a fresh sender pointed at the same {@code sf_dir}) it is free
 * to resend any frame whose ACK was lost in the bounce. The target table is
 * created with {@code DEDUP UPSERT KEYS(ts, id)} so that those replays
 * collapse into the original row.
 *
 * <p>The implementation under test (cursor SF on the {@code vi_sf} branch
 * of {@code java-questdb-client}) provides:
 * <ul>
 *   <li>Per-sender slot directory {@code <sf_dir>/<sender_id>/} with an
 *       advisory file lock (default {@code sender_id="default"}).</li>
 *   <li>Startup recovery: a new sender opening an existing slot replays
 *       any unacked sealed segments via {@code SegmentRing.openExisting}.</li>
 *   <li>In-flight reconnect with exponential backoff and a per-outage
 *       budget ({@code reconnect_max_duration_millis}, default 5 min) —
 *       a server bounce mid-write does not break the sender.</li>
 *   <li>{@code close_flush_timeout_millis} (default 5000): close blocks
 *       up to that many ms waiting for ACKs. Spec tests pass {@code 0}
 *       for fast close so the server can be killed mid-flight and leave
 *       genuinely-unacked frames on disk for the next sender to replay.</li>
 * </ul>
 *
 * <p>Tests in this class:
 * <ul>
 *   <li>{@link #testSmokeNoRestart} — wire-path control, no bounce.</li>
 *   <li>{@link #testNewSenderRecoversFromSfDir} — server killed before
 *       sender close; next sender on the same slot recovers and
 *       replays.</li>
 *   <li>{@link #testSameSenderSurvivesServerRestart} — sender stays open
 *       across a server bounce and the I/O loop reconnects.</li>
 *   <li>{@link #testFuzzMultipleRestartsNewSender} — randomized epoch
 *       counts and row counts over the new-sender recovery path.</li>
 *   <li>{@link #testSenderPushesContinuouslyWhileServerBounces} — single
 *       long-lived sender writes rows in a tight loop on one thread while
 *       another thread bounces the server several times; on close every
 *       row that was handed to {@code at(...)} must end up in the table
 *       exactly once.</li>
 * </ul>
 */
public class QwpIngressServerRestartFuzzTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(QwpIngressServerRestartFuzzTest.class);
    private static final String TABLE_NAME = "qwp_restart_fuzz";

    @Test
    public void testSenderPushesContinuouslyWhileServerBounces() throws Exception {
        // The realistic outage scenario: one user thread writes rows
        // continuously through a single SF sender while another thread
        // bounces the server several times. Sender must not throw from
        // user-facing calls, and after close() every row that was handed
        // to at(...) must be present in the table exactly once.
        //
        // Dedup on (ts, id) is the safety net for replays: when the wire
        // disconnects mid-flight, frames the server already wrote-but-did
        // not-ack get replayed by the SF cursor on reconnect. Without
        // dedup the table would over-count those replays.
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = pickFreePort();
            String sfDir = freshSfDir("continuous-bounces");

            Rnd rnd = TestUtils.generateRandom(LOG);
            int bounces = 3 + rnd.nextInt(3);          // 3..5 server bounces
            int batchRows = 25;
            int batchPauseMillis = 2;
            long tsBase = 1_700_000_000_000_000_000L;
            long tsStepNanos = 1_000L;                 // 1us per row, well under DAY

            // Long reconnect budget + long close drain so a 50ms downtime
            // window never makes the sender give up. close_flush_timeout
            // is bounces * downtime + headroom for WAL apply.
            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";reconnect_max_duration_millis=120000"
                    + ";close_flush_timeout_millis=120000;";

            try (RestartableQwpServer server = new RestartableQwpServer(port)) {
                server.start();

                AtomicBoolean stopProducer = new AtomicBoolean();
                AtomicReference<Throwable> producerError = new AtomicReference<>();
                AtomicReference<Throwable> bouncerError = new AtomicReference<>();
                AtomicLong rowsProduced = new AtomicLong();
                CountDownLatch producerDone = new CountDownLatch(1);
                CountDownLatch bouncerDone = new CountDownLatch(1);

                Thread producer = new Thread(() -> {
                    try (Sender sender = Sender.fromConfig(connect)) {
                        long id = 0;
                        while (!stopProducer.get()) {
                            for (int i = 0; i < batchRows; i++) {
                                long currentId = id++;
                                long ts = tsBase + currentId * tsStepNanos;
                                sender.table(TABLE_NAME)
                                        .longColumn("id", currentId)
                                        .doubleColumn("val", currentId * 1.5)
                                        .at(ts, ChronoUnit.NANOS);
                            }
                            // Publish what we just buffered to the SF cursor
                            // promptly so a bounce mid-batch can't lose
                            // rows still sitting in the client autoflush
                            // buffer.
                            sender.flush();
                            rowsProduced.set(id);
                            Os.sleep(batchPauseMillis);
                        }
                        // Final sender.close() (try-with-resources) drains
                        // up to close_flush_timeout_millis waiting for the
                        // ackedFsn to catch up to publishedFsn.
                    } catch (Throwable t) {
                        producerError.set(t);
                    } finally {
                        producerDone.countDown();
                        Path.clearThreadLocals();
                    }
                }, "qwp-producer");

                Thread bouncer = new Thread(() -> {
                    try {
                        // Let the producer get into a steady-state rhythm
                        // before the first bounce, so we exercise the
                        // mid-flight reconnect path rather than first-connect.
                        Os.sleep(100);
                        for (int i = 0; i < bounces; i++) {
                            LOG.info().$("bouncer: bounce ").$(i + 1).$('/').$(bounces).$();
                            server.stop();
                            Os.sleep(30 + rnd.nextInt(50));   // 30..79 ms downtime
                            server.start();
                            Os.sleep(120 + rnd.nextInt(200)); // 120..319 ms uptime
                        }
                    } catch (Throwable t) {
                        bouncerError.set(t);
                    } finally {
                        bouncerDone.countDown();
                    }
                }, "qwp-bouncer");

                producer.start();
                bouncer.start();

                if (!bouncerDone.await(120, TimeUnit.SECONDS)) {
                    stopProducer.set(true);
                    throw new AssertionError("bouncer did not finish within 120s");
                }
                if (bouncerError.get() != null) {
                    stopProducer.set(true);
                    throw new AssertionError("bouncer failed", bouncerError.get());
                }

                // Give the producer one more grace window to emit a few
                // batches against the now-stable server, then signal stop.
                Os.sleep(200);
                stopProducer.set(true);

                if (!producerDone.await(180, TimeUnit.SECONDS)) {
                    throw new AssertionError("producer did not finish within 180s "
                            + "(rowsProduced=" + rowsProduced.get() + ")");
                }
                if (producerError.get() != null) {
                    throw new AssertionError(
                            "producer must not surface failures across server bounces "
                                    + "(rowsProduced=" + rowsProduced.get() + ")",
                            producerError.get());
                }

                long expected = rowsProduced.get();
                if (expected <= 0) {
                    throw new AssertionError("producer wrote zero rows");
                }
                LOG.info().$("producer wrote ").$(expected)
                        .$(" rows across ").$(bounces).$(" server bounces").$();

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                // No loss + no duplicates collapsed into one shape: the
                // distinct id set is exactly [0, expected) and the row
                // count matches. If any row was lost, count would be
                // smaller; if dedup didn't collapse a replay, count would
                // exceed count_distinct(id).
                assertSql(
                        "SELECT count() c, count_distinct(id) d, min(id) lo, max(id) hi"
                                + " FROM " + TABLE_NAME,
                        "c\td\tlo\thi\n"
                                + expected + "\t" + expected + "\t0\t" + (expected - 1) + "\n"
                );
            }
        });
    }

    @Test
    public void testSmokeNoRestart() throws Exception {
        // Wire-path control: N parallel writers, each with its OWN sfDir,
        // pushing rows over QWP+SF without a server bounce. Verifies the
        // happy-path SF send loop in isolation from any restart logic.
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(port)) {
                server.start();
                int writers = 2;
                int rowsPerWriter = 500;
                CountDownLatch done = new CountDownLatch(writers);
                AtomicReference<Throwable> firstError = new AtomicReference<>();
                for (int w = 0; w < writers; w++) {
                    final long writerIdBase = (long) w * rowsPerWriter;
                    final long writerTsBase = 1_700_000_000_000_000_000L
                            + (long) w * rowsPerWriter * 1000L;
                    final String writerSfDir = freshSfDir("smoke-w" + w);
                    new Thread(() -> {
                        try {
                            runOneSfSender(port, writerSfDir, writerIdBase,
                                    rowsPerWriter, writerTsBase);
                        } catch (Throwable t) {
                            firstError.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                            Path.clearThreadLocals();
                        }
                    }, "qwp-writer-" + w).start();
                }
                if (!done.await(60, TimeUnit.SECONDS)) {
                    throw new AssertionError("writers did not finish within 60s");
                }
                if (firstError.get() != null) {
                    throw new AssertionError("at least one writer failed", firstError.get());
                }
                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 30, TimeUnit.SECONDS);
                assertRowCount((long) writers * rowsPerWriter);
            }
        });
    }

    @Test
    public void testNewSenderRecoversFromSfDir() throws Exception {
        // Across epochs the parent sfDir is shared. With the slot-dir model
        // (sf_dir is the parent, sender_id picks the slot), both senders
        // land in <sfDir>/default/ — the second sender opens the same slot
        // after the first releases its lock and recovers any unacked
        // segments the first left behind.
        //
        // To make recovery genuinely exercised we use
        // close_flush_timeout_millis=0 (fast close, no wait for ACKs) and
        // kill the server BEFORE the first sender exits. Otherwise close()
        // would block 5s waiting for ACKs and drain everything to the
        // server, leaving recovery with nothing to do.
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = pickFreePort();
            String sfDir = freshSfDir("new-sender-recovery");

            int rowsPerEpoch = 5_000; // big enough that some won't be drained

            // Epoch 1: sender writes, then server is killed BEFORE sender
            // close so some frames sit unacked on disk.
            RestartableQwpServer server1 = new RestartableQwpServer(port);
            server1.start();
            String connect1 = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";close_flush_timeout_millis=0;";
            Sender sender1 = Sender.fromConfig(connect1);
            try {
                writeRows(sender1, /*idBase*/ 0L, rowsPerEpoch, 1_700_000_000_000_000_000L);
                sender1.flush();
                // Kill server BEFORE the sender exits — sender's I/O loop
                // will see the disconnect; whatever wasn't acked stays on
                // disk in <sfDir>/default/.
                server1.stop();
            } finally {
                sender1.close();
                server1.close();
            }

            // Epoch 2: brand-new server on the same port; the new sender
            // pointed at the same sfDir locks the same slot and replays
            // any unacked frames before continuing with new ones.
            try (RestartableQwpServer server2 = new RestartableQwpServer(port)) {
                server2.start();
                runOneSfSender(port, sfDir, /*idBase*/ rowsPerEpoch, rowsPerEpoch,
                        1_700_000_000_000_000_000L + (long) rowsPerEpoch * 1000L);
                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 30, TimeUnit.SECONDS);
                // Dedup collapses any replays so each unique (ts, id) maps
                // to exactly one row.
                assertRowCount(2L * rowsPerEpoch);
            }
        });
    }

    @Test
    public void testSameSenderSurvivesServerRestart() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = pickFreePort();
            String sfDir = freshSfDir("same-sender-survives");

            try (RestartableQwpServer server = new RestartableQwpServer(port)) {
                server.start();

                int rowsPerPhase = 500;
                String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(connect)) {
                    // Phase 1: write, flush, ensure server has acked some.
                    writeRows(sender, /*idBase*/ 0L, rowsPerPhase, 1_700_000_000_000_000_000L);
                    sender.flush();

                    // Bounce the server. Same port. The sender keeps the same
                    // sfDir and CursorSendEngine; the wire path needs to reconnect.
                    server.stop();
                    server.start();

                    // Phase 2: the same sender must keep working across the bounce.
                    writeRows(sender, /*idBase*/ rowsPerPhase, rowsPerPhase,
                            1_700_000_000_000_000_000L + (long) rowsPerPhase * 1000L);
                    sender.flush();
                }

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 30, TimeUnit.SECONDS);
                assertRowCount(2L * rowsPerPhase);
            }
        });
    }

    @Test
    public void testFuzzMultipleRestartsNewSender() throws Exception {
        // Same recovery scenario as testNewSenderRecoversFromSfDir, but
        // randomized over multiple epochs. Each epoch: open sender (fast
        // close), write rows, kill server before sender exits, restart
        // server, repeat. Final epoch leaves the server up so any
        // last-epoch unacked frames replay through and the row count
        // matches the total ever written.
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = pickFreePort();
            String sfDir = freshSfDir("fuzz-multi-restart");

            Rnd rnd = TestUtils.generateRandom(LOG);
            int epochs = 3 + rnd.nextInt(3);              // 3..5 server bounces
            int rowsPerEpoch = 500 + rnd.nextInt(1500);   // 500..1999 rows per epoch
            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";close_flush_timeout_millis=0;";

            long expected = 0;
            long idBase = 0;
            long tsBase = 1_700_000_000_000_000_000L;
            for (int epoch = 0; epoch < epochs; epoch++) {
                LOG.info().$("fuzz epoch ").$(epoch).$('/').$(epochs)
                        .$(" rows=").$(rowsPerEpoch)
                        .$(" idBase=").$(idBase).$();
                RestartableQwpServer server = new RestartableQwpServer(port);
                server.start();
                Sender sender = Sender.fromConfig(connect);
                try {
                    writeRows(sender, idBase, rowsPerEpoch, tsBase + idBase * 1000L);
                    sender.flush();
                    // Random-length pause before bouncing — sometimes the
                    // server gets to drain everything, sometimes not.
                    Os.sleep(rnd.nextInt(50));
                    // Drop server BEFORE sender exits to maximize unacked-on-disk.
                    server.stop();
                } finally {
                    sender.close();
                    server.close();
                }
                expected += rowsPerEpoch;
                idBase += rowsPerEpoch;
            }

            // Final epoch with no kill: a sender's startup recovery picks
            // up any leftover unacked frames from the previous epoch and
            // replays them; we wait long enough for the I/O loop to drain.
            try (RestartableQwpServer server = new RestartableQwpServer(port)) {
                server.start();
                // Open one more sender against the same slot to trigger
                // recovery and drain. close() with the default 5s flush
                // timeout makes this synchronous-ish.
                String connectFinal = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(connectFinal)) {
                    sender.flush();
                }
                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);
                assertRowCount(expected);
            }
        });
    }

    private void assertRowCount(long expected) {
        assertSql(
                "SELECT count() FROM " + TABLE_NAME,
                "count\n" + expected + "\n"
        );
    }

    private void assertSql(String sql, String expected) {
        try {
            TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, expected);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void createTargetTable() {
        try {
            execute(
                    "CREATE TABLE " + TABLE_NAME + " ("
                            + "id LONG, "
                            + "val DOUBLE, "
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

    /** Pick a free TCP port by binding port 0 and reading what the OS gave us. */
    private static int pickFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    /**
     * Open one SF sender against {@code sfDir}, push {@code count} rows on
     * a deterministic (ts, id) grid, flush, and close. An sfDir is owned by
     * exactly one sender at a time, so callers MUST serialize senders that
     * share a dir (across epochs). Throws on any sender failure.
     */
    private void runOneSfSender(int port, String sfDir, long idBase, int count, long tsBaseNanos) {
        String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
        try (Sender sender = Sender.fromConfig(connect)) {
            writeRows(sender, idBase, count, tsBaseNanos);
            sender.flush();
        }
    }

    /**
     * Append a deterministic row sequence: id in [idBase, idBase+count), ts
     * spaced 1us apart starting at {@code tsBaseNanos}, val derived from id
     * so a future stricter assertion can hash and compare.
     */
    private void writeRows(Sender sender, long idBase, int count, long tsBaseNanos) {
        for (int i = 0; i < count; i++) {
            long id = idBase + i;
            long ts = tsBaseNanos + (long) i * 1000L; // 1us steps; well under DAY partition
            sender.table(TABLE_NAME)
                    .longColumn("id", id)
                    .doubleColumn("val", id * 1.5)
                    .at(ts, ChronoUnit.NANOS);
        }
    }

    /**
     * Wraps an {@link HttpServer} bound to a fixed {@code port}, with a
     * worker pool and the QWP WebSocket processor, so the test can
     * stop/start it across the same port without losing the underlying
     * {@link io.questdb.cairo.CairoEngine} state. Single-threaded worker
     * pool keeps test scheduling deterministic.
     */
    private static final class RestartableQwpServer implements AutoCloseable {
        private final int port;
        private final AtomicBoolean running = new AtomicBoolean();
        private HttpServer server;
        private TestWorkerPool workerPool;

        RestartableQwpServer(int port) {
            this.port = port;
        }

        @Override
        public void close() {
            if (running.get()) {
                stop();
            }
        }

        void start() throws SqlException {
            if (!running.compareAndSet(false, true)) {
                throw new IllegalStateException("already running");
            }
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                    configuration,
                    new DefaultHttpContextConfiguration()
            ) {
                @Override
                public int getBindPort() {
                    return port;
                }
            };

            workerPool = new TestWorkerPool(1);
            server = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE);
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfig.getContextPathQWP();
                }

                @Override
                public QwpWebSocketHttpProcessor newInstance() {
                    return new QwpWebSocketHttpProcessor(engine, httpConfig);
                }
            });
            WorkerPoolUtils.setupWriterJobs(workerPool, engine);
            workerPool.start(LOG);
        }

        void stop() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            try {
                workerPool.halt();
            } catch (Throwable t) {
                LOG.error().$("worker pool halt failed").$(t).$();
            }
            try {
                server.close();
            } catch (Throwable t) {
                LOG.error().$("server close failed").$(t).$();
            }
            server = null;
            workerPool = null;
        }
    }
}
