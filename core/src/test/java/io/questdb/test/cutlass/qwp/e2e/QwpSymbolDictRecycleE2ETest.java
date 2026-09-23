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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end coverage for the client-side symbol-dictionary recycle feature
 * (a real {@link QwpWebSocketSender} against a real QWP ingress server): once
 * a sender's producer-visible symbol dictionary reaches
 * {@code symbol_dict_reset_threshold} distinct symbols, the sender tears down
 * its engine, rolls its FSN epoch base, rebuilds a fresh (empty) dictionary
 * and reconnects -- all transparently to the caller, with externally-visible
 * FSNs epoch-translated so they stay monotonic across the swap.
 * <p>
 * This test streams several hundred rows with a bounded, deterministic
 * {@code sym = "s" + (id % SYMBOL_CARDINALITY)} mapping across a threshold
 * low enough to cross it once organically; the client's anti-thrash
 * re-arm floor then keeps this bounded live set from re-arming on its
 * own, so a manual {@code resetSymbolDictionary()} call
 * (which bypasses the floor by design) drives a second recycle. It then
 * proves:
 * <ul>
 *   <li>every row's symbol is exactly what its id implies (a per-row oracle,
 *       not just a row count -- misattribution across the epoch boundary is
 *       silent while counts stay correct);</li>
 *   <li>the server actually accepted more than one ingress connection;</li>
 *   <li>the FSNs handed out by {@code flushAndGetSequence()} strictly
 *       increase across both recycles, and a {@code SenderProgressHandler}
 *       is still delivering acks in the final epoch;</li>
 *   <li>a FSN captured in the epoch before the last recycle is still
 *       awaitable (and acked) after that recycle has happened.</li>
 * </ul>
 * Runs the same scenario in both delta-SF (disk-backed {@code sf_dir}) and
 * memory mode, mirroring the client's own
 * {@code SymbolDictRecycleTest}/{@code SymbolDictRecycleMemoryModeTest} split.
 */
public class QwpSymbolDictRecycleE2ETest extends AbstractQwpWebSocketTest {

    private static final int BATCH_SIZE = 30;
    private static final Log LOG = LogFactory.getLog(QwpSymbolDictRecycleE2ETest.class);
    // Rows streamed through the bounded organic loop, well past
    // SYMBOL_CARDINALITY, so the threshold is crossed once. The client's
    // anti-thrash re-arm floor (resetFloorSymbols = 2x dictSizeAtSwap) then
    // makes a second organic recycle impossible for this bounded live set --
    // one recycle, then settle, is the intended behavior under the floor.
    private static final int ORGANIC_ROWS = 900;
    // K in "sym = 's' + (id % K)". Comfortably above the reset threshold so
    // every epoch's dictionary keeps growing on genuinely novel symbols
    // rather than immediately recycling repeats.
    private static final int SYMBOL_CARDINALITY = 150;
    private static final int SYMBOL_DICT_RESET_THRESHOLD = 64;
    private static final String TABLE_NAME = "qwp_symbol_dict_recycle_e2e";
    // One extra batch, written after a manual Sender.resetSymbolDictionary()
    // call (which bypasses the floor by design), to drive the second recycle
    // this test needs in order to prove FSN/epoch continuity across more
    // than one recycle boundary.
    private static final int TOTAL_ROWS = ORGANIC_ROWS + BATCH_SIZE;

    @Test
    public void testRecycleAcrossThresholdInDeltaSfMode() throws Exception {
        runRecycleScenario(true);
    }

    @Test
    public void testRecycleAcrossThresholdInMemoryMode() throws Exception {
        runRecycleScenario(false);
    }

    private static void writeRow(QwpWebSocketSender sender, long id, long tsBase, long tsStepNanos) {
        sender.table(TABLE_NAME)
                .symbol("sym", "s" + (id % SYMBOL_CARDINALITY))
                .longColumn("id", id)
                .at(tsBase + id * tsStepNanos, ChronoUnit.NANOS);
    }

    /**
     * Like {@link AbstractQwpWebSocketTest#runInContext(QwpTestContext)} but
     * wires the QWP URL to a {@link ConnectionCountingIngressProcessor}
     * instead of a bare {@link QwpIngressHttpProcessor}, so the caller gets a
     * genuine server-side "ingress connections accepted" count. Counting
     * happens on {@code onHeadersReady} -- the WebSocket handshake request,
     * fired exactly once per accepted TCP connection and never again once the
     * protocol switches to WS binary framing -- rather than on pooled
     * {@code HttpConnectionContext} construction, which the framework reuses
     * across many connections and would undercount.
     */
    private void runInContextCountingConnections(QwpTestContext r, AtomicInteger ingressConnections) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                configuration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public int getForceRecvFragmentationChunkSize() {
                        return recvChunk;
                    }

                    @Override
                    public int getForceSendFragmentationChunkSize() {
                        return sendChunk;
                    }
                }
        ) {
            @Override
            public int getBindPort() {
                return 0;
            }

            @Override
            public int getRecvBufferSize() {
                return 65_536;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    TestWorkerPool workerPool = new TestWorkerPool(1);
                    HttpServer server = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                server.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfig.getContextPathQWP();
                    }

                    @Override
                    public ConnectionCountingIngressProcessor newInstance() {
                        return new ConnectionCountingIngressProcessor(engine, httpConfig, ingressConnections);
                    }
                });
                WorkerPoolUtils.setupWriterJobs(workerPool, engine);
                workerPool.start(LOG);
                try {
                    r.run(server.getPort());
                } catch (Throwable err) {
                    LOG.error().$("Stopping QWP worker pool because of an error").$(err).$();
                    throw err;
                } finally {
                    workerPool.halt();
                    Path.clearThreadLocals();
                }
            }
        });
    }

    private void runRecycleScenario(boolean sfMode) throws Exception {
        AtomicInteger ingressConnections = new AtomicInteger();
        runInContextCountingConnections((port) -> {
            execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "id LONG, "
                    + "sym SYMBOL, "
                    + "ts TIMESTAMP"
                    + ") TIMESTAMP(ts) PARTITION BY DAY WAL "
                    + "DEDUP UPSERT KEYS(ts, id)");

            // Pin auto-flush off: the exact-count assertions below need arming to
            // happen only at this test's explicit flushAndGetSequence() calls. The
            // default WS auto_flush_interval (100ms) could otherwise fire an
            // intra-batch flush that arms the recycle early (a smaller
            // dictSizeAtSwap at the first swap leaves a lower re-arm floor),
            // letting the bounded SYMBOL_CARDINALITY set legally recycle a
            // second time organically.
            // WebSocket rejects auto_flush_interval=off outright, so pin it to a
            // value well beyond this test's runtime instead of disabling it.
            String cfg;
            if (sfMode) {
                String sfDir = temp.newFolder("qwp-symbol-dict-recycle-sf").getAbsolutePath();
                cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                        + ";auto_flush_rows=off;auto_flush_interval=60000"
                        + ";close_flush_timeout_millis=120000;";
            } else {
                cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                        + ";auto_flush_rows=off;auto_flush_interval=60000"
                        + ";close_flush_timeout_millis=120000;";
            }

            List<Long> ackedFsns = Collections.synchronizedList(new ArrayList<>());
            List<Long> flushedFsns = new ArrayList<>();
            long finalEpochBase;
            long lastOrganicFsn = -1;
            long preRecycleFsn;
            long symbolDictEpoch;
            long tsBase = 1_700_000_000_000_000_000L;
            long tsStepNanos = 1_000L;

            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg)) {
                sender.setProgressHandler(ackedFsns::add);

                long id = 0;
                for (int i = 0; i < BATCH_SIZE; i++) {
                    writeRow(sender, id, tsBase, tsStepNanos);
                    id++;
                }
                preRecycleFsn = sender.flushAndGetSequence();
                Assert.assertTrue("flushAndGetSequence() must return a real FSN for the first batch",
                        preRecycleFsn >= 0);
                flushedFsns.add(preRecycleFsn);
                Assert.assertTrue("first batch must be acked before the threshold is ever crossed",
                        sender.awaitAckedFsn(preRecycleFsn, 10_000));
                Assert.assertFalse("threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                                + " must not be crossed by the first " + BATCH_SIZE + " (all-novel) symbols",
                        sender.isResetArmed());

                while (id < ORGANIC_ROWS) {
                    for (int i = 0; i < BATCH_SIZE && id < ORGANIC_ROWS; i++) {
                        writeRow(sender, id, tsBase, tsStepNanos);
                        id++;
                    }
                    long batchFsn = sender.flushAndGetSequence();
                    if (batchFsn >= 0) {
                        flushedFsns.add(batchFsn);
                        Assert.assertTrue("batch ending at id=" + id + " must be acked within 10s",
                                sender.awaitAckedFsn(batchFsn, 10_000));
                        lastOrganicFsn = batchFsn;
                    }
                }

                Assert.assertTrue("setup: the organic loop must have handed out a non-zero FSN, got "
                        + lastOrganicFsn, lastOrganicFsn > 0);

                Assert.assertEquals("a bounded live set of " + SYMBOL_CARDINALITY
                                + " symbols must recycle exactly once and then settle "
                                + "under the client's anti-thrash re-arm floor",
                        1, sender.getSymbolDictEpoch());

                // Sender.resetSymbolDictionary() bypasses the re-arm floor by
                // design -- mirrors the client's own
                // SymbolDictRecycleHealingTest#testMetricsAfterTwoRecycles -- so
                // drive the second recycle, and the multi-epoch continuity this
                // test exists to prove, through it.
                sender.resetSymbolDictionary();
                Assert.assertTrue("manual reset request must bypass the re-arm floor and arm immediately",
                        sender.isResetArmed());
                for (int i = 0; i < BATCH_SIZE; i++) {
                    writeRow(sender, id, tsBase, tsStepNanos);
                    id++;
                }
                long finalBatchFsn = sender.flushAndGetSequence();
                flushedFsns.add(finalBatchFsn);
                Assert.assertTrue("final batch ending at id=" + id + " must be acked within 10s",
                        sender.awaitAckedFsn(finalBatchFsn, 10_000));

                symbolDictEpoch = sender.getSymbolDictEpoch();
                // Captured here (post-loop, pre-close) so the FSN-continuity
                // anchor below reflects the LAST epoch this run ever reached,
                // not an intermediate one.
                finalEpochBase = sender.getFsnEpochBaseForTesting();
                Assert.assertEquals("the organic recycle (floor-limited to one) plus the "
                                + "manual resetSymbolDictionary() recycle must together total "
                                + "exactly 2, got symbolDictEpoch=" + symbolDictEpoch,
                        2, symbolDictEpoch);

                Assert.assertTrue("fsnEpochBase must have rolled past the last organic FSN: base="
                                + finalEpochBase + " lastOrganicFsn=" + lastOrganicFsn,
                        finalEpochBase > lastOrganicFsn);
                // Anchored on the LAST organic batch, not the first: with the first
                // (FSN 0) every epoch base satisfies the check above, and with an
                // epoch-0 anchor a lost SECOND roll (base 3 instead of 30) still passes.
                // A lost roll is caught by that base assertion, so this await pins the
                // accessor's epoch translation rather than the roll: the last organic
                // FSN sits in the epoch before the final one, a correct base turns it
                // into a negative internal target, and awaitAckedFsn must answer true
                // at once via the prior-epoch short-circuit. An accessor that forgot
                // the base would compare the raw FSN against the final epoch's ack
                // watermark -- which acked a single frame -- and time the await out.
                Assert.assertTrue("post-recycle awaitAckedFsn(lastOrganicFsn) must return true via the "
                                + "prior-epoch short-circuit: base=" + finalEpochBase
                                + " lastOrganicFsn=" + lastOrganicFsn,
                        sender.awaitAckedFsn(lastOrganicFsn, 5_000));
            }

            drainWalQueue();
            engine.awaitTable(TABLE_NAME, 60, TimeUnit.SECONDS);

            assertQuery("SELECT count() FROM " + TABLE_NAME)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + TOTAL_ROWS + "\n");

            // Per-row oracle: every row's symbol must be exactly what its id
            // implies. A dictionary shifted (or nulled) by even one entry
            // across a recycle boundary reads back the wrong value here even
            // though the row count above stays correct.
            assertQuery("SELECT count() FROM " + TABLE_NAME
                    + " WHERE sym IS NULL OR sym <> concat('s', (id % " + SYMBOL_CARDINALITY + ")::string)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");

            // Positive control: pairs with the zero-mismatch query above into a
            // partition check. If the mismatch predicate ever degraded to
            // always-false (a type-resolution change, a future concat overload),
            // both the zero-mismatch query and this one would still pass on
            // their own -- but only a correct predicate can make both hold at
            // once, since together they must account for every one of the
            // TOTAL_ROWS rows exactly once.
            assertQuery("SELECT count() FROM " + TABLE_NAME
                    + " WHERE sym = concat('s', (id % " + SYMBOL_CARDINALITY + ")::string)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + TOTAL_ROWS + "\n");

            // Every healthy recycle produces exactly one new ingress connection:
            // symbolDictEpoch is pinned at 2 above, so a skipped or doubled
            // reconnect fails here.
            Assert.assertEquals("each recycle must produce exactly one new ingress connection",
                    symbolDictEpoch + 1, ingressConnections.get());

            // FSN continuity from the producer's side: every value that
            // flushAndGetSequence() handed out is external (epoch base plus
            // internal FSN), so the sequence must strictly increase across both
            // recycle boundaries. A base that failed to roll, or rolled short,
            // shows up as a repeat or a step back here.
            for (int i = 1, n = flushedFsns.size(); i < n; i++) {
                Assert.assertTrue("flushAndGetSequence() must strictly increase across every recycle "
                                + "boundary, got " + flushedFsns.get(i - 1) + " -> " + flushedFsns.get(i)
                                + " at flush " + i,
                        flushedFsns.get(i) > flushedFsns.get(i - 1));
            }

            // Progress-handler continuity: the dispatcher lives for the whole
            // sender and only ever delivers increasing values, so the list
            // cannot show a step back; what it can show is a dispatcher that
            // stopped firing at a recycle boundary (a lost re-attachment to the
            // rebuilt cursor loop at step 7). Anchoring on finalEpochBase, the
            // base as of the LAST recycle, catches that: a delivery at or above
            // it is necessarily an ack from the final epoch. A size bound would
            // not do -- SenderProgressDispatcher is a single-slot coalescing
            // mailbox, so the list length says nothing about how many acks landed.
            List<Long> snapshot = new ArrayList<>(ackedFsns);
            Assert.assertFalse("progress handler must have fired at least once", snapshot.isEmpty());
            Assert.assertTrue("progress handler must have kept firing through the final epoch, "
                            + "last observed FSN=" + snapshot.get(snapshot.size() - 1)
                            + ", finalEpochBase=" + finalEpochBase,
                    snapshot.get(snapshot.size() - 1) >= finalEpochBase);
        }, ingressConnections);
    }

    /**
     * A {@link QwpIngressHttpProcessor} that wraps its returned processor in
     * a {@link HandshakeCountingProcessor}. {@link #getProcessor} always
     * returns the same singleton per the base class's own contract ("Per-
     * connection state lives in LocalValue, so the instance is safe to
     * share"), so the wrapper is built once and cached.
     */
    private static final class ConnectionCountingIngressProcessor extends QwpIngressHttpProcessor {
        private final AtomicInteger ingressConnections;
        private HttpRequestProcessor wrapped;

        ConnectionCountingIngressProcessor(
                CairoEngine engine,
                HttpFullFatServerConfiguration httpConfiguration,
                AtomicInteger ingressConnections
        ) {
            super(engine, httpConfiguration);
            this.ingressConnections = ingressConnections;
        }

        @Override
        public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
            if (wrapped == null) {
                wrapped = new HandshakeCountingProcessor(super.getProcessor(requestHeader), ingressConnections);
            }
            return wrapped;
        }
    }

    /**
     * Delegates every {@link HttpRequestProcessor} callback to the real QWP
     * upgrade processor unchanged, and additionally counts each accepted
     * handshake. {@code onHeadersReady} fires exactly once per WebSocket
     * upgrade request -- once per ingress TCP connection -- and never again
     * once the protocol switches to WS binary framing, so it is a genuine
     * "connections accepted" signal, unlike counting pooled
     * {@code HttpConnectionContext} construction (the framework reuses those
     * across many connections via a {@code WeakMutableObjectPool}).
     */
    private static final class HandshakeCountingProcessor implements HttpRequestProcessor {
        private final AtomicInteger ingressConnections;
        private final HttpRequestProcessor real;

        HandshakeCountingProcessor(HttpRequestProcessor real, AtomicInteger ingressConnections) {
            this.real = real;
            this.ingressConnections = ingressConnections;
        }

        @Override
        public void failRequest(HttpConnectionContext context, HttpException exception)
                throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            real.failRequest(context, exception);
        }

        @Override
        public String getName() {
            return real.getName();
        }

        @Override
        public byte getRequiredAuthType() {
            return real.getRequiredAuthType();
        }

        @Override
        public short getSupportedRequestTypes() {
            return real.getSupportedRequestTypes();
        }

        @Override
        public boolean ignoreConnectionLimitCheck() {
            return real.ignoreConnectionLimitCheck();
        }

        @Override
        public void onConnectionClosed(HttpConnectionContext context) {
            real.onConnectionClosed(context);
        }

        @Override
        public void onHeadersReady(HttpConnectionContext context) throws PeerDisconnectedException {
            ingressConnections.incrementAndGet();
            real.onHeadersReady(context);
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            real.onRequestComplete(context);
        }

        @Override
        public void onRequestRetry(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            real.onRequestRetry(context);
        }

        @Override
        public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
            real.parkRequest(context, pausedQuery);
        }

        @Override
        public boolean processServiceAccountCookie(HttpConnectionContext context, SecurityContext securityContext)
                throws PeerIsSlowToReadException, PeerDisconnectedException {
            return real.processServiceAccountCookie(context, securityContext);
        }

        @Override
        public boolean requiresAuthentication() {
            return real.requiresAuthentication();
        }

        @Override
        public boolean reservedOneAdminConnection() {
            return real.reservedOneAdminConnection();
        }

        @Override
        public void resumeRecv(HttpConnectionContext context)
                throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
            real.resumeRecv(context);
        }

        @Override
        public void resumeSend(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            real.resumeSend(context);
        }
    }
}
