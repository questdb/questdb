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
 * low enough to cross it several times, then proves:
 * <ul>
 *   <li>every row's symbol is exactly what its id implies (a per-row oracle,
 *       not just a row count -- misattribution across the epoch boundary is
 *       silent while counts stay correct);</li>
 *   <li>the server actually accepted more than one ingress connection;</li>
 *   <li>a {@code SenderProgressHandler} observes a strictly increasing
 *       external FSN stream straight through every recycle boundary;</li>
 *   <li>a FSN captured well before the first recycle is still awaitable
 *       (and acked) after the recycle has happened.</li>
 * </ul>
 * Runs the same scenario in both delta-SF (disk-backed {@code sf_dir}) and
 * memory mode, mirroring the client's own
 * {@code SymbolDictRecycleTest}/{@code SymbolDictRecycleMemoryModeTest} split.
 */
public class QwpSymbolDictRecycleE2ETest extends AbstractQwpWebSocketTest {

    private static final int BATCH_SIZE = 30;
    private static final Log LOG = LogFactory.getLog(QwpSymbolDictRecycleE2ETest.class);
    // K in "sym = 's' + (id % K)". Comfortably above the reset threshold so
    // every epoch's dictionary keeps growing on genuinely novel symbols
    // rather than immediately recycling repeats.
    private static final int SYMBOL_CARDINALITY = 150;
    private static final int SYMBOL_DICT_RESET_THRESHOLD = 64;
    private static final String TABLE_NAME = "qwp_symbol_dict_recycle_e2e";
    // Several hundred rows, well past SYMBOL_CARDINALITY, so the threshold is
    // crossed (and the sender recycles) many times over the run.
    private static final int TOTAL_ROWS = 900;

    @Test
    public void testRecycleAcrossThresholdInDeltaSfMode() throws Exception {
        runRecycleScenario(true);
    }

    @Test
    public void testRecycleAcrossThresholdInMemoryMode() throws Exception {
        runRecycleScenario(false);
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

            String cfg;
            if (sfMode) {
                String sfDir = temp.newFolder("qwp-symbol-dict-recycle-sf").getAbsolutePath();
                cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                        + ";close_flush_timeout_millis=120000;";
            } else {
                cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                        + ";close_flush_timeout_millis=120000;";
            }

            List<Long> ackedFsns = Collections.synchronizedList(new ArrayList<>());
            long finalEpochBase;
            long preRecycleFsn;
            long resetsPerformed;
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
                Assert.assertTrue("first batch must be acked before the threshold is ever crossed",
                        sender.awaitAckedFsn(preRecycleFsn, 10_000));
                Assert.assertFalse("threshold=" + SYMBOL_DICT_RESET_THRESHOLD
                                + " must not be crossed by the first " + BATCH_SIZE + " (all-novel) symbols",
                        sender.isResetArmed());

                while (id < TOTAL_ROWS) {
                    for (int i = 0; i < BATCH_SIZE && id < TOTAL_ROWS; i++) {
                        writeRow(sender, id, tsBase, tsStepNanos);
                        id++;
                    }
                    long batchFsn = sender.flushAndGetSequence();
                    if (batchFsn >= 0) {
                        Assert.assertTrue("batch ending at id=" + id + " must be acked within 10s",
                                sender.awaitAckedFsn(batchFsn, 10_000));
                    }
                }

                resetsPerformed = sender.getSymbolDictResetsPerformed();
                // Captured here (post-loop, pre-close) so the FSN-continuity
                // anchor below reflects the LAST epoch this run ever reached,
                // not an intermediate one.
                finalEpochBase = sender.getFsnEpochBaseForTest();
                Assert.assertTrue("expected the threshold to be crossed several times over "
                                + TOTAL_ROWS + " rows, but symbolDictResetsPerformed=" + resetsPerformed,
                        resetsPerformed >= 2);
                Assert.assertTrue("epoch must have advanced at least twice past the initial "
                                + "connection's epoch 0 (a single recycle would only reach epoch 1)",
                        sender.getSymbolDictEpoch() >= 2);

                Assert.assertTrue("post-recycle awaitAckedFsn(preRecycleFsn) must return true, "
                                + "proving fsnEpochBase rolled forward past the pre-recycle "
                                + "high-water mark -- awaitAckedFsn short-circuits true for any "
                                + "target from a prior epoch without consulting ack state, so this "
                                + "proves the epoch translation, not that the FSN is acked (ack "
                                + "state itself is covered by the row-count oracle below)",
                        sender.awaitAckedFsn(preRecycleFsn, 5_000));
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
            // once, since together they must account for every one of the 900
            // rows exactly once.
            assertQuery("SELECT count() FROM " + TABLE_NAME
                            + " WHERE sym = concat('s', (id % " + SYMBOL_CARDINALITY + ")::string)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + TOTAL_ROWS + "\n");

            // Relational, not absolute: the exact recycle count depends on batch
            // timing that this test does not pin (a time-based auto-flush could
            // shift a batch boundary under load), but every healthy recycle
            // produces exactly one new ingress connection regardless of how many
            // recycles happened, so this is flake-free while still catching a
            // regression that halves the recycle rate or reconnects twice per
            // recycle -- neither of which a bare ">= 2" could see.
            Assert.assertEquals("each recycle must produce exactly one new ingress connection",
                    resetsPerformed + 1, ingressConnections.get());

            // FSN continuity: the progress handler must have observed a
            // strictly increasing external FSN stream straight through every
            // recycle boundary in the run above.
            List<Long> snapshot = new ArrayList<>(ackedFsns);
            Assert.assertFalse("progress handler must have fired at least once", snapshot.isEmpty());
            // Not vacuous, and anchored to survive every boundary, not just the
            // first: a regression that drops the progress dispatcher's
            // re-attachment to the rebuilt cursor loop after ANY recycle
            // (QwpWebSocketSender's step-7 reconnect) stops every callback from
            // that boundary onward. Comparing against preRecycleFsn alone would
            // NOT catch this -- epoch 0 spans three batches, so a dispatcher
            // dying at the very first boundary still leaves batch-2/3 acks in
            // snapshot that are already > preRecycleFsn. Anchoring on
            // finalEpochBase (the epoch base as of the LAST recycle, captured
            // above before close()) closes that gap: any delivery at or above
            // finalEpochBase is necessarily an ack from the final epoch, so
            // this only passes if callbacks survived every recycle boundary
            // the run crossed (resetsPerformed of them, asserted >= 2 above).
            // A size-based lower bound on snapshot would NOT work as a
            // substitute or supplement here -- SenderProgressDispatcher is a
            // single-slot coalescing watermark mailbox, so the list length
            // says nothing about how many acks actually landed.
            Assert.assertTrue("progress handler must have kept firing through the final epoch, "
                            + "last observed FSN=" + snapshot.get(snapshot.size() - 1)
                            + ", finalEpochBase=" + finalEpochBase,
                    snapshot.get(snapshot.size() - 1) >= finalEpochBase);
            for (int i = 1, n = snapshot.size(); i < n; i++) {
                Assert.assertTrue("external FSN must strictly increase across every recycle "
                                + "boundary, got " + snapshot.get(i - 1) + " -> " + snapshot.get(i)
                                + " at index " + i,
                        snapshot.get(i) > snapshot.get(i - 1));
            }
        }, ingressConnections);
    }

    private static void writeRow(QwpWebSocketSender sender, long id, long tsBase, long tsStepNanos) {
        sender.table(TABLE_NAME)
                .symbol("sym", "s" + (id % SYMBOL_CARDINALITY))
                .longColumn("id", id)
                .at(tsBase + id * tsStepNanos, ChronoUnit.NANOS);
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
