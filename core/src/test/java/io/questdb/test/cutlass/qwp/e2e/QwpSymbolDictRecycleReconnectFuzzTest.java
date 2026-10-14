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
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exercises dictionary recycling and ordinary reconnects on one store-and-forward sender.
 * Recycling replaces the dictionary and rolls the frame-sequence epoch; reconnecting to a
 * restarted server must replay the current dictionary without changing that epoch.
 *
 * <p>Each round has a randomly timed blind bounce, a wait for a new dictionary epoch, and
 * a targeted bounce of a live connection. Blind bounces run concurrently with the producer
 * and can interrupt recycling or reconnecting. Progress waits leave the server up and reset
 * requests enabled: a slow host gets time to recycle instead of exhausting a restart count.
 * Every round must make progress before its targeted bounce, so post-test recovery cannot
 * satisfy the recycle floor. For targeted bounces the producer drains and pauses with no
 * recycle armed. The server halts its workers while keeping the live socket open, then the
 * producer sends one row. The bounce must interrupt that sent-but-unacknowledged frame,
 * and the producer stays paused until DISCONNECTED so recycling cannot hide the loss.
 *
 * <p>Replay can duplicate rows, so the table deduplicates by timestamp and id. The final
 * queries check the exact id range and every row's symbol, catching dictionary corruption
 * even when row counts are correct. Periodic producer drains also exercise lost/delayed
 * acknowledgements across the bounces.
 */
public class QwpSymbolDictRecycleReconnectFuzzTest extends AbstractCairoTest {

    private static final int BATCH_SIZE = 25;
    private static final Log LOG = LogFactory.getLog(QwpSymbolDictRecycleReconnectFuzzTest.class);
    private static final int MIN_RECYCLES_DURING_BOUNCES = 10;
    // Match RestartableQwpServer's default socket buffers.
    private static final int RECV_BUFFER_SIZE = 131_072;
    // Manual requests keep recycling after the bounded symbol set reaches the anti-thrash floor.
    private static final int RESET_EVERY_N_BATCHES = 3;
    private static final int SEND_BUFFER_SIZE = 131_072;
    private static final int SYMBOL_CARDINALITY = 400;
    private static final int SYMBOL_DICT_RESET_THRESHOLD = 70;
    private static final String TABLE_NAME = "qwp_symbol_dict_recycle_reconnect_fuzz";
    private static final int TARGET_DRAIN_EVERY_N_BATCHES = 20;

    private int recvChunk;
    private Rnd rnd;
    private int sendChunk;

    @Before
    public void setUp() {
        super.setUp();
        rnd = TestUtils.generateRandom(LOG);
        recvChunk = 1 + rnd.nextInt(RECV_BUFFER_SIZE);
        sendChunk = 1 + rnd.nextInt(SEND_BUFFER_SIZE);
        LOG.info().$("QwpSymbolDictRecycleReconnectFuzzTest fragmentation recvChunk=").$(recvChunk)
                .$(", sendChunk=").$(sendChunk).$();
    }

    @Test
    public void testRecycleAndReconnectFuzz() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            int port = RestartableQwpServer.pickFreePort();
            String sfDir = temp.newFolder("qwp-recycle-reconnect-fuzz").getAbsolutePath();
            // 20..30 bounces, with at least ten distinct recycle-progress windows.
            int rounds = MIN_RECYCLES_DURING_BOUNCES + rnd.nextInt(6);
            long tsBase = 1_700_000_000_000_000_000L;
            long tsStepNanos = 1_000L;
            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                    + ";reconnect_max_duration_millis=120000"
                    + ";close_flush_timeout_millis=120000;";

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port, recvChunk, sendChunk)) {
                server.start();
                AtomicBoolean isProducerStopped = new AtomicBoolean();
                AtomicReference<ProducerPause> producerPause = new AtomicReference<>();
                AtomicReference<Throwable> producerError = new AtomicReference<>();
                AtomicReference<QwpWebSocketSender> senderRef = new AtomicReference<>();
                AtomicLong rowsProduced = new AtomicLong();
                AtomicInteger unplannedDisconnects = new AtomicInteger();
                CountDownLatch firstBatchAcked = new CountDownLatch(1);

                Thread producer = new Thread(() -> {
                    try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(connect)) {
                        senderRef.set(sender);
                        // Planned recycle teardown does not emit DISCONNECTED; only the
                        // I/O loop observing a lost, previously established connection does.
                        sender.setConnectionListener(event -> {
                            if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                                unplannedDisconnects.incrementAndGet();
                            }
                        });
                        long id = 0;
                        int batchesSinceDrain = 0;
                        int batchesSinceReset = 0;
                        while (!isProducerStopped.get()) {
                            for (int i = 0; i < BATCH_SIZE; i++) {
                                writeRow(sender, id++, tsBase, tsStepNanos);
                            }
                            batchesSinceDrain++;
                            batchesSinceReset++;
                            if (batchesSinceDrain >= TARGET_DRAIN_EVERY_N_BATCHES) {
                                Assert.assertTrue("periodic drain must succeed within 30s even across a bounce",
                                        sender.drain(30_000));
                                batchesSinceDrain = 0;
                            } else {
                                sender.flush();
                            }
                            if (batchesSinceReset >= RESET_EVERY_N_BATCHES) {
                                sender.resetSymbolDictionary();
                                batchesSinceReset = 0;
                            }
                            rowsProduced.set(id);
                            if (firstBatchAcked.getCount() > 0) {
                                // Trim the first registrations before bouncing, so reconnect
                                // must use catch-up rather than replaying the initial batch.
                                Assert.assertTrue("first batch must drain before the first bounce",
                                        sender.drain(30_000));
                                firstBatchAcked.countDown();
                            }
                            ProducerPause pause = producerPause.get();
                            if (pause != null) {
                                Assert.assertTrue("producer must drain before a targeted bounce",
                                        sender.drain(30_000));
                                // An armed recycle needs another normal batch before pausing:
                                // the targeted row's table() must not reconnect with workers halted.
                                if (!sender.isResetArmed()) {
                                    pause.ready.countDown();
                                    pause.publish.await();
                                    writeRow(sender, id++, tsBase, tsStepNanos);
                                    sender.flush();
                                    // at() may auto-flush, so flushAndGetSequence() could return -1.
                                    pause.publishedFsn = sender.getFsnEpochBaseForTesting()
                                            + sender.getCursorEngineForTesting().publishedFsn();
                                    rowsProduced.set(id);
                                    pause.published.countDown();
                                    pause.resume.await();
                                }
                            }
                            Os.sleep(2);
                        }
                        // close() must drain every published row or surface a producer error.
                    } catch (Throwable t) {
                        producerError.set(t);
                    } finally {
                        firstBatchAcked.countDown();
                        Path.clearThreadLocals();
                    }
                }, "qwp-recycle-reconnect-fuzz-producer");

                int restarts = 0;
                int liveDrops = 0;
                int targetedLiveDrops = 0;
                long epochAtStart = 0;
                long epochAtBounceEnd = 0;
                Throwable scheduleError = null;
                producer.start();
                try {
                    Assert.assertTrue("producer's first batch never drained",
                            firstBatchAcked.await(60, TimeUnit.SECONDS));
                    assertProducerHealthy(producerError, rowsProduced);
                    QwpWebSocketSender sender = senderRef.get();
                    epochAtStart = sender.getSymbolDictEpoch();
                    for (int round = 0; round < rounds; round++) {
                        // Blind bounce: preserve stops during a recycle or reconnect.
                        Os.sleep(40 + rnd.nextInt(160));
                        liveDrops += bounce(server);
                        restarts++;

                        // Require fresh progress after this bounce, while the producer still
                        // writes and requests resets. A live socket alone is insufficient:
                        // recycling also needs the published backlog to be acknowledged.
                        long epochBeforeWait = sender.getSymbolDictEpoch();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (sender.getSymbolDictEpoch() == epochBeforeWait) {
                            assertProducerHealthy(producerError, rowsProduced);
                            if (System.nanoTime() >= deadline) {
                                throw new AssertionError("recycle did not progress within 30s after blind bounce"
                                        + " [round=" + round + ", epoch=" + sender.getSymbolDictEpoch()
                                        + ", resetArmed=" + sender.isResetArmed()
                                        + ", starvationTimeouts=" + sender.getSymbolDictResetStarvationTimeouts()
                                        + ", ackedFsn=" + sender.getAckedFsn()
                                        + ", rowsProduced=" + rowsProduced.get() + ']');
                            }
                            Os.sleep(1);
                        }

                        // Hold server processing until the targeted row is sent, so even a
                        // slow bouncer must kill a live connection with unacknowledged data.
                        Os.sleep(40 + rnd.nextInt(160));
                        ProducerPause pause = new ProducerPause();
                        producerPause.set(pause);
                        try {
                            Assert.assertTrue("producer did not pause within 30s in round " + round,
                                    pause.ready.await(30, TimeUnit.SECONDS));
                            assertProducerHealthy(producerError, rowsProduced);
                            int disconnectsBeforeBounce = unplannedDisconnects.get();
                            int killed;
                            try {
                                server.haltWorkers();
                                long framesBefore = sender.getTotalFramesSent();
                                pause.publish.countDown();
                                Assert.assertTrue("producer did not publish within 30s in round " + round,
                                        pause.published.await(30, TimeUnit.SECONDS));
                                assertProducerHealthy(producerError, rowsProduced);
                                TestUtils.assertEventually(() -> Assert.assertTrue(
                                        "targeted row must reach the live socket",
                                        sender.getTotalFramesSent() > framesBefore
                                ), 30);
                                Assert.assertTrue("targeted bounce must have unacknowledged published data",
                                        sender.getAckedFsn() < pause.publishedFsn);
                            } finally {
                                // A failed gate must also restore service before producer close().
                                killed = bounce(server);
                            }
                            liveDrops += killed;
                            targetedLiveDrops += killed;
                            restarts++;
                            Assert.assertTrue("targeted bounce must kill a live connection in round " + round,
                                    killed > 0);
                            TestUtils.assertEventually(() -> Assert.assertTrue(
                                    "targeted bounce must surface as DISCONNECTED",
                                    unplannedDisconnects.get() > disconnectsBeforeBounce
                            ), 30);
                        } finally {
                            producerPause.set(null);
                            pause.publish.countDown();
                            pause.resume.countDown();
                        }
                    }
                    epochAtBounceEnd = sender.getSymbolDictEpoch();
                    Os.sleep(200);
                } catch (Throwable t) {
                    scheduleError = t;
                } finally {
                    isProducerStopped.set(true);
                    // Keep the server alive until the sender has drained and closed, including
                    // on schedule failure. The test thread owns all server restarts.
                    producer.join(180_000);
                }
                Assert.assertFalse("producer did not finish within 180s [rowsProduced=" + rowsProduced.get() + ']',
                        producer.isAlive());
                assertProducerHealthy(producerError, rowsProduced);
                if (scheduleError != null) {
                    throw new AssertionError("bounce schedule failed after " + restarts + " restarts", scheduleError);
                }

                long expected = rowsProduced.get();
                int unplanned = unplannedDisconnects.get();
                LOG.info().$("fuzz run complete: rowsProduced=").$(expected)
                        .$(", serverRestarts=").$(restarts)
                        .$(", targetedBounces=").$(rounds)
                        .$(", targetedLiveDrops=").$(targetedLiveDrops)
                        .$(", liveDrops=").$(liveDrops)
                        .$(", unplannedDisconnects=").$(unplanned)
                        .$(", recyclesDuringBounces=").$(epochAtBounceEnd - epochAtStart).$();

                Assert.assertTrue("producer wrote zero rows", expected > 0);
                Assert.assertEquals("every round must complete both bounces", 2 * rounds, restarts);
                Assert.assertTrue("expected at least " + MIN_RECYCLES_DURING_BOUNCES
                                + " recycles during the bounce schedule [startEpoch=" + epochAtStart
                                + ", endEpoch=" + epochAtBounceEnd + ']',
                        epochAtBounceEnd - epochAtStart >= MIN_RECYCLES_DURING_BOUNCES);
                Assert.assertTrue("expected at least " + rounds + " DISCONNECTED events"
                                + " [unplannedDisconnects=" + unplanned + ']',
                        unplanned >= rounds);

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);
                assertQuery(
                        "SELECT count() c, count_distinct(id) d, min(id) lo, max(id) hi"
                                + " FROM " + TABLE_NAME)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(
                                "c\td\tlo\thi\n"
                                        + expected + "\t" + expected + "\t0\t" + (expected - 1) + "\n"
                        );
                assertSymbolsIntact(expected);
            }
        });
    }

    private static void assertProducerHealthy(AtomicReference<Throwable> producerError, AtomicLong rowsProduced) {
        if (producerError.get() != null) {
            throw new AssertionError("producer failed across recycle/restart interleaving [rowsProduced="
                    + rowsProduced.get() + ']', producerError.get());
        }
    }

    /**
     * Per-row oracle from {@link QwpSymbolDictRecycleE2ETest}: every row's
     * symbol must be exactly what its id implies. A dictionary shifted (or
     * NULLed) by even one entry across a recycle or a restart's catch-up
     * reads back wrong here even though the row-count oracle stays correct.
     */
    private void assertSymbolsIntact(long expected) throws Exception {
        assertQuery("SELECT count() FROM " + TABLE_NAME
                + " WHERE sym IS NULL OR sym <> concat('s', (id % " + SYMBOL_CARDINALITY + ")::string)")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n0\n");

        // Positive control, same rationale as QwpSymbolDictRecycleE2ETest:
        // pairs with the zero-mismatch query above so a degenerate predicate
        // can't pass both checks vacuously.
        assertQuery("SELECT count() FROM " + TABLE_NAME
                + " WHERE sym = concat('s', (id % " + SYMBOL_CARDINALITY + ")::string)")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expected + "\n");
    }

    private int bounce(RestartableQwpServer server) throws Exception {
        int killed = server.stop();
        try {
            Os.sleep(15 + rnd.nextInt(60));
        } finally {
            server.start();
        }
        return killed;
    }

    private void createTargetTable() {
        try {
            execute(
                    "CREATE TABLE " + TABLE_NAME + " ("
                            + "id LONG, "
                            + "sym SYMBOL, "
                            + "ts TIMESTAMP"
                            + ") TIMESTAMP(ts) PARTITION BY DAY WAL "
                            + "DEDUP UPSERT KEYS(ts, id)"
            );
        } catch (Exception e) {
            throw new AssertionError("failed to create target table", e);
        }
    }

    private void writeRow(QwpWebSocketSender sender, long id, long tsBaseNanos, long tsStepNanos) {
        sender.table(TABLE_NAME)
                .symbol("sym", "s" + (id % SYMBOL_CARDINALITY))
                .longColumn("id", id)
                .at(tsBaseNanos + id * tsStepNanos, ChronoUnit.NANOS);
    }

    private static class ProducerPause {
        private final CountDownLatch publish = new CountDownLatch(1);
        private final CountDownLatch published = new CountDownLatch(1);
        private final CountDownLatch ready = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);
        private long publishedFsn;
    }
}
