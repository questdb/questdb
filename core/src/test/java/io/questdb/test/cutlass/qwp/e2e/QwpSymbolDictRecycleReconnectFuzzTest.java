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
 * Soak test that interleaves the two reconnect paths a QWP {@link QwpWebSocketSender}
 * can take -- and proves neither one's state reset leaks onto the other:
 * <ul>
 *   <li><b>Recycle reconnect</b> (client-driven, see {@link QwpSymbolDictRecycleE2ETest}):
 *       once the producer-visible symbol dictionary reaches
 *       {@code symbol_dict_reset_threshold} distinct symbols, or the producer
 *       asks via {@code resetSymbolDictionary()} (this test does, every
 *       {@link #RESET_EVERY_N_BATCHES} batches -- see that constant), the
 *       sender tears down its engine, rolls its FSN epoch base, rebuilds a
 *       fresh dictionary and reconnects -- entirely on its own clock, at its
 *       own barrier.</li>
 *   <li><b>Ordinary reconnect</b> (server-driven, see
 *       {@link QwpIngressServerRestartFuzzTest}): an unplanned server bounce
 *       drops the wire; the sender's I/O loop reconnects with a delta
 *       catch-up that re-registers whatever the CURRENT epoch's dictionary
 *       holds, preserving the epoch baseline (no recycle).</li>
 * </ul>
 * A single long-lived sender undergoes both kinds of reconnect repeatedly, in
 * an order this test does not control: one background thread bounces the
 * server on a seeded random schedule while the producer thread keeps writing,
 * so a restart can land mid-recycle and a recycle can land mid-catch-up.
 * Every even-indexed bounce is targeted: the bouncer pauses the producer's
 * reset requests and waits for a connection that has stayed live for 20 ms
 * before stopping, so the stop lands on an armed client; odd-indexed bounces
 * stay blind so restarts still land mid-recycle and mid-outage.
 * The schedule runs to a random 15-30 restart target and then continues,
 * up to five times the target, until at least ten recycles have completed
 * during the bouncing, so the recycle-density floor is a condition the
 * schedule satisfies on any host rather than a bet on how fast this one
 * reconnects (hosted Windows completes about one recycle per three
 * bounces; a mac several per bounce).
 * That overlap is the risk surface -- a recycle's engine teardown/epoch roll must
 * never observe (or be observed by) an in-flight ordinary reconnect, and vice
 * versa.
 * <p>
 * Duplicates are expected and tolerated exactly like
 * {@link QwpIngressServerRestartFuzzTest}: a restart mid-flight can make the
 * sender replay frames the server already committed but had not yet acked.
 * The target table carries the same {@code DEDUP UPSERT KEYS(ts, id)} safety
 * net, so replays collapse and the final row count is exact, not just a
 * lower bound.
 * <p>
 * There is no dedicated "ack delay" injection seam anywhere in this harness
 * (checked both {@link RestartableQwpServer} and the QWP client builder) --
 * a server restart landing while a batch is in flight already produces
 * delayed and (from the sender's perspective) lost acks, which is exactly
 * what {@link #testRecycleAndReconnectFuzz} exercises via the periodic
 * blocking {@code drain()} calls below.
 */
public class QwpSymbolDictRecycleReconnectFuzzTest extends AbstractCairoTest {

    private static final int BATCH_SIZE = 25;
    private static final Log LOG = LogFactory.getLog(QwpSymbolDictRecycleReconnectFuzzTest.class);
    // Recycle-density floor: the bouncer keeps bouncing past its restart target until this
    // many recycles have completed during the bouncing, so the epoch assertion below is a
    // schedule condition the run satisfies on any host, not a bet on how fast this host
    // reconnects; only a run that reaches the ceiling first fails it.
    private static final int MIN_RECYCLES_DURING_BOUNCES = 10;
    // Server defaults from DefaultIODispatcherConfiguration, mirroring
    // QwpIngressServerRestartFuzzTest -- RestartableQwpServer does not
    // override these, so the actual buffers are this size.
    private static final int RECV_BUFFER_SIZE = 131_072;
    // The client's anti-thrash re-arm floor (2x the dictionary size at the
    // last swap) lets this bounded live set recycle organically only a
    // handful of times; the producer requests the rest on this cadence so
    // recycles stay dense across the whole bounce schedule. The swap itself
    // still runs only at the client's own barrier (drained ring, no row in
    // progress), so a request that lands mid-outage waits like an organic arm.
    private static final int RESET_EVERY_N_BATCHES = 3;
    // Ceiling on the bounce schedule as a multiple of its random restart target. Hosted
    // Windows pays ~500 ms per reconnect into an outage (a refused loopback SYN is retried
    // after 500 ms instead of returning ECONNREFUSED, and the client's foreground connect is
    // untimed), so it completes about one recycle per three bounces; five times the 15-bounce
    // low end leaves the floor a comfortable margin where three times it does not.
    private static final int RESTART_CEILING_FACTOR = 5;
    private static final int SEND_BUFFER_SIZE = 131_072;
    // Comfortably above the reset threshold -- as in QwpSymbolDictRecycleE2ETest,
    // this keeps most in-epoch growth on genuinely novel symbols. Total rows
    // over a multi-second continuous run vastly exceed SYMBOL_CARDINALITY, so
    // plenty of later rows still land as true back-references (the property
    // QwpIngressServerRestartFuzzTest's TAG_CARDINALITY bound exists to test).
    private static final int SYMBOL_CARDINALITY = 400;
    private static final int SYMBOL_DICT_RESET_THRESHOLD = 70;
    private static final String TABLE_NAME = "qwp_symbol_dict_recycle_reconnect_fuzz";
    // Every this many batches, block on drain() instead of a fire-and-forget
    // flush(). A drain that straddles a bounce is exactly the delayed/lost-ack
    // scenario this suite has no other seam for (see class javadoc).
    private static final int TARGET_DRAIN_EVERY_N_BATCHES = 20;

    private int recvChunk;
    private Rnd rnd;
    private int sendChunk;

    @Before
    public void setUp() {
        super.setUp();
        rnd = TestUtils.generateRandom(LOG);
        // Independent recv / send fragmentation chunks (asymmetric, min=1),
        // same rationale as QwpIngressServerRestartFuzzTest: chunk=1 makes
        // every wire byte its own socket event, exposing park-resume bugs in
        // both the WS parser and the recycle/catch-up frame builders.
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

            // 15..30 server bounces, randomly paced -- large enough to
            // interleave densely with the tens of recycles the reset cadence
            // produces, small enough to keep the run under a handful of
            // seconds on a fast host. Past the target the bouncer keeps going
            // only until MIN_RECYCLES_DURING_BOUNCES recycles have completed
            // during the bouncing, up to the ceiling.
            int restartTarget = 15 + rnd.nextInt(16);
            int restartCeiling = restartTarget * RESTART_CEILING_FACTOR;
            // One full stability-wait bound per bounce on top of the schedule itself, so a
            // stalled client fails the per-bounce wait assertion (10 s, naming the bounce)
            // before this outer ceiling can fire.
            long bouncerBudgetSeconds = 120 + 10L * restartCeiling;
            long tsBase = 1_700_000_000_000_000_000L;
            long tsStepNanos = 1_000L; // 1us per row, well under DAY partition

            // Any reconnect_* knob set explicitly promotes the INITIAL connect to
            // SYNC (Sender.build()), so the first connect against the freshly
            // started server is bounded here. A recycle's step-7 reconnect never
            // runs on the producer thread: every post-initial ensureConnected()
            // entry defers the socket connect to the I/O thread with unbounded
            // retry, so no budget funds a bounce that lands on a recycle.
            String connect = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                    + ";reconnect_max_duration_millis=120000"
                    + ";close_flush_timeout_millis=120000;";

            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port, recvChunk, sendChunk)) {
                server.start();

                AtomicBoolean stopProducer = new AtomicBoolean();
                AtomicReference<Throwable> producerError = new AtomicReference<>();
                AtomicReference<Throwable> bouncerError = new AtomicReference<>();
                AtomicLong rowsProduced = new AtomicLong();
                AtomicLong symbolDictEpochHolder = new AtomicLong();
                AtomicInteger restartsDone = new AtomicInteger();
                AtomicInteger unplannedDisconnects = new AtomicInteger();
                AtomicInteger liveDrops = new AtomicInteger();
                AtomicInteger targetedBounces = new AtomicInteger();
                AtomicInteger targetedLiveDrops = new AtomicInteger();
                // Set by the bouncer while it lines up a targeted stop, read by the producer
                // before each reset request. With manual requests paused, only a recycle armed
                // before the pause -- manual or organic (threshold-driven) -- can still tear the
                // connection down, and the stability window the bouncer waits for is what
                // absorbs it.
                AtomicBoolean holdResets = new AtomicBoolean();
                AtomicReference<QwpWebSocketSender> senderRef = new AtomicReference<>();
                CountDownLatch producerDone = new CountDownLatch(1);
                CountDownLatch bouncerDone = new CountDownLatch(1);
                CountDownLatch firstBatchAcked = new CountDownLatch(1);

                Thread producer = new Thread(() -> {
                    try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(connect)) {
                        senderRef.set(sender);
                        // DISCONNECTED fires only when an already-established
                        // connection is observed dropped mid-stream by the I/O
                        // loop's OWN reused reconnect factory (the
                        // ctx.previousIdx >= 0 check in QwpWebSocketSender.connectWalk).
                        // A recycle's step-7 reconnect always builds a brand-new
                        // factory (ReconnectSupplier.previousIdx starts at -1), and
                        // CursorWebSocketSendLoop.close()'s running=false guard
                        // means a planned teardown never re-enters connectLoop at
                        // all -- so this listener gives a clean, isolated count of
                        // unplanned (bouncer-triggered) reconnects that can never be
                        // conflated with a recycle's own step-7 connect.
                        sender.setConnectionListener(event -> {
                            if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                                unplannedDisconnects.incrementAndGet();
                            }
                        });
                        long id = 0;
                        int batchesSinceDrain = 0;
                        int batchesSinceReset = 0;
                        while (!stopProducer.get()) {
                            for (int i = 0; i < BATCH_SIZE; i++) {
                                long currentId = id++;
                                writeRow(sender, currentId, tsBase, tsStepNanos);
                            }
                            batchesSinceDrain++;
                            batchesSinceReset++;
                            if (batchesSinceDrain >= TARGET_DRAIN_EVERY_N_BATCHES) {
                                // Blocking wait for acks -- may straddle a bounce
                                // mid-wait. See class javadoc. 30s covers a full
                                // outage plus reconnect plus a full SF replay under
                                // chunk=1 fragmentation on a single-threaded server
                                // worker pool -- the tightest deadline in this test.
                                Assert.assertTrue("periodic drain must succeed within 30s even across a bounce",
                                        sender.drain(30_000));
                                batchesSinceDrain = 0;
                            } else {
                                sender.flush();
                            }
                            if (batchesSinceReset >= RESET_EVERY_N_BATCHES && !holdResets.get()) {
                                sender.resetSymbolDictionary();
                                batchesSinceReset = 0;
                            }
                            rowsProduced.set(id);
                            if (firstBatchAcked.getCount() > 0) {
                                // Deterministic barrier, same rationale as
                                // QwpIngressServerRestartFuzzTest: the first
                                // batch's registrations must be acked (and
                                // trimmed) before the first bounce, or the
                                // bounce's catch-up is never load-bearing --
                                // an unacked first batch just replays with
                                // its own delta intact.
                                Assert.assertTrue("first batch must drain before the first bounce",
                                        sender.drain(30_000));
                                firstBatchAcked.countDown();
                            }
                            Os.sleep(2);
                        }
                        // Captured right after the batch loop, before the implicit
                        // close() below runs. recycleForDictReset() is reachable
                        // only from table(...), so
                        // close() itself can never advance these counters further --
                        // this sample is already final. Durability is close()'s job,
                        // not this sample's: drainOnClose() throws on timeout, which
                        // the catch (Throwable) below turns into a real
                        // producerError, so unacked rows can never silently shrink
                        // the table below rowsProduced.
                        //
                        // Deliberately NOT sampling getTotalReconnectsSucceeded()
                        // here: it is sender-lifetime and carried across recycles,
                        // but a single end-of-run sample says nothing this test can
                        // check -- every recycle's own reconnect and every injected
                        // outage both count, and the outage schedule is seeded noise.
                        symbolDictEpochHolder.set(sender.getSymbolDictEpoch());
                    } catch (Throwable t) {
                        producerError.set(t);
                    } finally {
                        // Release the barrier even if the producer died before its
                        // first batch drained, so a pre-barrier producer death
                        // doesn't leave the bouncer blocked on it for a full 60s --
                        // idempotent, a no-op if already counted down during normal
                        // operation.
                        firstBatchAcked.countDown();
                        producerDone.countDown();
                        Path.clearThreadLocals();
                    }
                }, "qwp-recycle-reconnect-fuzz-producer");

                Thread bouncer = new Thread(() -> {
                    try {
                        // Same rationale as QwpIngressServerRestartFuzzTest: wait
                        // for the first batch to be acked and trimmed so every
                        // bounce below exercises real catch-up, not a race with
                        // the initial connect.
                        Assert.assertTrue("producer's first batch never drained",
                                firstBatchAcked.await(60, TimeUnit.SECONDS));
                        for (int i = 0; i < restartCeiling; i++) {
                            if (i >= restartTarget) {
                                // Schedule condition: past the target, bounce only while the
                                // recycle floor is unmet (a dead producer leaves senderRef set
                                // and the targeted wait below fails loudly within 10 s).
                                QwpWebSocketSender s = senderRef.get();
                                if (s != null && s.getSymbolDictEpoch() >= MIN_RECYCLES_DURING_BOUNCES) {
                                    break;
                                }
                            }
                            Os.sleep(40 + rnd.nextInt(160)); // 40..199ms uptime
                            final boolean targeted = (i & 1) == 0;
                            if (targeted) {
                                // Targeted bounce: pause the producer's reset requests, then
                                // wait for one upgraded connection to stay live for 20 ms (a
                                // recycle armed before the pause fires at its next barrier
                                // within a batch or two), so this stop lands on a connection
                                // whose I/O loop is armed and that no planned recycle can tear
                                // down in the meantime. Odd bounces stay blind and keep the
                                // "restart during the client's outage or reconnect walk"
                                // interleaving this suite exists for.
                                holdResets.set(true);
                                Assert.assertTrue("client did not hold a live connection for 20ms within 10s before bounce " + i,
                                        server.awaitStableLiveConnection(10_000, 20));
                                targetedBounces.incrementAndGet();
                            }
                            final int killed = server.stop();
                            liveDrops.addAndGet(killed);
                            if (targeted) {
                                targetedLiveDrops.addAndGet(killed);
                            }
                            Os.sleep(15 + rnd.nextInt(60));  // 15..74ms downtime
                            // Lift the pause only once the server is back: a reset requested
                            // while it is down can only wait like an organic arm, and lifting
                            // earlier would let a planned teardown absorb a drop the I/O loop
                            // has not observed yet.
                            holdResets.set(false);
                            server.start();
                            restartsDone.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        bouncerError.set(t);
                    } finally {
                        // A failed wait leaves the pause set; lift it so the producer's
                        // remaining batches still exercise resets before the main thread
                        // stops it.
                        holdResets.set(false);
                        bouncerDone.countDown();
                    }
                }, "qwp-recycle-reconnect-fuzz-bouncer");

                producer.start();
                bouncer.start();

                if (!bouncerDone.await(bouncerBudgetSeconds, TimeUnit.SECONDS)) {
                    stopProducer.set(true);
                    throw new AssertionError("bouncer did not finish within " + bouncerBudgetSeconds + "s, restartsDone=" + restartsDone.get());
                }
                if (bouncerError.get() != null) {
                    stopProducer.set(true);
                    // A producer death before the first-batch barrier also
                    // surfaces as a bouncer failure (the barrier now always
                    // releases via the producer's own finally, so the bouncer's
                    // own assertTrue on it should pass -- but if the bouncer
                    // failed for some other, unrelated reason while a producer
                    // failure is ALSO in flight, wait briefly for the producer to
                    // settle and prefer its real exception: it is the more
                    // informative root cause than the bouncer's derived failure.
                    producerDone.await(5, TimeUnit.SECONDS);
                    if (producerError.get() != null) {
                        throw new AssertionError("producer failed (observed via a concurrent "
                                + "bouncer failure); rowsProduced=" + rowsProduced.get(), producerError.get());
                    }
                    throw new AssertionError("bouncer failed after restartsDone=" + restartsDone.get(), bouncerError.get());
                }

                // Sample the sender's cumulative reset counter the moment the
                // bouncer's restart schedule finishes, so the floor below
                // proves recycling happened DURING the bouncing window itself,
                // not in the post-bounce grace window that follows -- an
                // end-of-run-only sample could in principle be satisfied by a
                // pathological serialization where all recycling is starved
                // while the server bounces and then bursts afterward (arithmetically
                // reachable at ~2.5ms/batch and a recycle per ~70 rows).
                // The floor on this sample is asserted only after the producer's
                // own error check below -- a producer that died mid-run must
                // surface its real exception, not a low-recycle-count headline.
                QwpWebSocketSender senderAtBounceEnd = senderRef.get();
                long epochAtBounceEnd = senderAtBounceEnd != null
                        ? senderAtBounceEnd.getSymbolDictEpoch() : 0L;

                // Grace window against a now-stable server before stopping the
                // producer, same as QwpIngressServerRestartFuzzTest.
                Os.sleep(200);
                stopProducer.set(true);

                if (!producerDone.await(180, TimeUnit.SECONDS)) {
                    throw new AssertionError("producer did not finish within 180s (rowsProduced="
                            + rowsProduced.get() + ")");
                }
                if (producerError.get() != null) {
                    throw new AssertionError("producer must not surface failures across recycle/restart "
                            + "interleaving (rowsProduced=" + rowsProduced.get() + ")", producerError.get());
                }

                Assert.assertTrue("expected the reset threshold to be crossed at least "
                                + MIN_RECYCLES_DURING_BOUNCES + " times DURING the server restarts, but "
                                + "symbolDictEpoch=" + epochAtBounceEnd + " when the bounce schedule finished "
                                + "after " + restartsDone.get() + " restarts (target " + restartTarget
                                + ", ceiling " + restartCeiling + ")",
                        epochAtBounceEnd >= MIN_RECYCLES_DURING_BOUNCES);

                long expected = rowsProduced.get();
                long symbolDictEpoch = symbolDictEpochHolder.get();
                int restarts = restartsDone.get();
                int unplanned = unplannedDisconnects.get();
                int liveDropCount = liveDrops.get();
                int targeted = targetedBounces.get();
                int targetedLiveDropCount = targetedLiveDrops.get();
                if (expected <= 0) {
                    throw new AssertionError("producer wrote zero rows");
                }
                LOG.info().$("fuzz run complete: rowsProduced=").$(expected)
                        .$(", serverRestarts=").$(restarts)
                        .$(", restartTarget=").$(restartTarget)
                        .$(", restartCeiling=").$(restartCeiling)
                        .$(", targetedBounces=").$(targeted)
                        .$(", targetedLiveDrops=").$(targetedLiveDropCount)
                        .$(", liveDrops=").$(liveDropCount)
                        .$(", unplannedDisconnects=").$(unplanned)
                        .$(", symbolDictEpoch=").$(symbolDictEpoch).$();

                Assert.assertTrue("bouncer must have completed at least its randomized restart target: "
                                + "restartTarget=" + restartTarget + ", restarts=" + restarts,
                        restarts >= restartTarget);
                Assert.assertTrue("bouncer must not have exceeded its ceiling: restartCeiling="
                                + restartCeiling + ", restarts=" + restarts,
                        restarts <= restartCeiling);
                // Every targeted bounce stopped the server while the client held a connection
                // that had already carried a frame, with reset requests paused. The only way
                // such a stop is not counted is a recycle armed before the pause firing inside
                // the few milliseconds between the stability check and the worker halt; half
                // is ample margin for that. Kills on blind bounces are not counted here, so a
                // broken targeting path cannot pass on luck.
                Assert.assertTrue("targeted bounces must land on live connections: targetedBounces=" + targeted
                                + ", targetedLiveDrops=" + targetedLiveDropCount
                                + ", required=" + ((targeted + 1) / 2),
                        targetedLiveDropCount >= (targeted + 1) / 2);
                // Direct, isolated evidence that the ordinary (unplanned) reconnect path ran:
                // DISCONNECTED fires only when the client's I/O loop observes a live connection
                // drop and re-enters its own reconnect factory (see the setConnectionListener
                // comment above for why it can't be conflated with a recycle's step-7
                // reconnect). A drop the loop has not observed before a recycle barrier closes
                // the loop is absorbed by the planned teardown, so the count trails liveDrops
                // by at most the width of that race, which the paused resets make rare on
                // targeted bounces; half is ample margin. On hosted Windows CI the old
                // restarts/12 floor failed with 0 of 22 because the client was live for only
                // 7% of server uptime, so only one blind stop ever hit a connection at all.
                Assert.assertTrue("at least half of the " + liveDropCount + " server stops that hit a live "
                                + "connection must surface as an unplanned DISCONNECTED event, but unplannedDisconnects="
                                + unplanned,
                        unplanned >= liveDropCount / 2);

                drainWalQueue();
                engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

                // Dedup safety net: no loss (count == expected) and no
                // under-collapsed replay (count == count_distinct(id)), same
                // oracle shape as QwpIngressServerRestartFuzzTest.
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
}
