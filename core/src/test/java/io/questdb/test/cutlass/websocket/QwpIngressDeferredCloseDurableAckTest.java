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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocket;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.LogCapture;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression tests for the blocked-close resume path dropping the final
 * durable ack ({@code QwpIngressUpgradeProcessor}).
 * <p>
 * The happy path honours the deferral invariant: {@code sendFatalClose} runs
 * {@code flushPendingAck} (cumulative ACK, then durable ACK) before writing
 * the CLOSE frame, so a durable-ack client's replay watermark is current when
 * the connection drops. But when the send side is (or becomes) blocked, the
 * CLOSE is parked via {@code onFatalCloseBlocked} into
 * {@code SEND_STATE_RESUME_ACK_THEN_CLOSE}, and the resume branch calls
 * {@code sendDeferredFatalClose}, which writes ONLY the CLOSE frame — the
 * durable ack is never sent (unlike the plain {@code RESUME_ACK} branch,
 * which drains the parked ACK and then flushes durable progress).
 * <p>
 * Cumulative OK acks cannot substitute: a store-and-forward client in
 * durable-ack mode advances its replay/trim watermark ONLY on
 * {@code STATUS_DURABLE_ACK} frames ({@code CursorWebSocketSendLoop}). A
 * stale watermark at disconnect means the client replays batches the server
 * (or, after a demote, the promoted replica via replication) already has —
 * duplicates on tables without DEDUP UPSERT KEYS.
 * <p>
 * Invariant asserted (fix-agnostic): when the registry's durable-upload
 * watermark covers the connection's committed work at fatal-close time, a
 * {@code STATUS_DURABLE_ACK} frame covering that work must be sent before the
 * CLOSE frame — regardless of whether the sends around the close blocked.
 * The tests stay green whether the fix re-runs the ack/durable-ack flush in
 * the {@code *_THEN_CLOSE} resume branches, re-checks durable progress inside
 * {@code sendDeferredFatalClose}, or re-arms the deferral.
 * <p>
 * Harness lineage: engine/registry/frame doubles from
 * {@code QwpIngressAckLeapfrogTest}; blocked-send + resumeSend drive pattern
 * from {@code QwpIngressUpgradeProcessorResumeRecvTest}.
 */
public class QwpIngressDeferredCloseDurableAckTest extends AbstractCairoTest {
    private static final Log SENTINEL_LOG = LogFactory.getLog(QwpIngressDeferredCloseDurableAckTest.class);
    private static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    private static final int RECV_BUFFER_SIZE = 1024;
    private static final int SEND_BUFFER_SIZE = 1024;

    /**
     * The finding's headline scenario, including the "already parked" variant:
     * a slow client's cumulative ACK blocks BEFORE the demote, so the
     * connection sits in {@code RESUME_ACK} when the role-change close
     * deferral exits with full durable coverage. {@code sendFatalClose}'s
     * {@code flushPendingAck} no-ops (both halves require READY),
     * {@code onFatalCloseBlocked} collapses to {@code RESUME_ACK_THEN_CLOSE},
     * and the resume path emits the CLOSE with no durable ack — precisely the
     * send-backpressure-at-demote-time case the deferral exists to protect.
     */
    @Test
    public void testRoleChangeCloseMustFlushFinalDurableAckWhenAckParked() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            // The demotable engine below shares the root with the static test
            // engine, which holds the table-registry lock. Pre-create the WAL
            // table through the lock holder so the QWP path only needs to
            // acquire a WAL writer, not register a new table.
            execute("create table taba (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("taba", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("taba", 200L, 2_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, ping, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; the end-of-recv
                    // cumulative ACK hits a full client receive buffer and
                    // parks -- connection enters RESUME_ACK.
                    rawSocket.throwSlowToReadOnCall = 1;
                    nf.release(frame0.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (parked cumulative ACK)");
                    } catch (PeerIsSlowToReadException expected) {
                        // ACK bytes queued in the framework buffer; the
                        // dispatcher would park the connection for write.
                    }
                    Assert.assertFalse(
                            "test setup: cumulative ACK must be parked (RESUME_ACK)",
                            state.isSendReady()
                    );

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage
                    // (registry watermark still lags at -1).
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes -- every committed
                    // seqTxn is now durably uploaded. A durable ack flushed
                    // now would leave the client's replay window empty.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase D: the client's durable-ack keepalive PING is the
                    // recv-driven poll that observes upload completion. The
                    // deferral exits into sendFatalClose, which finds the
                    // send side parked and defers the CLOSE.
                    nf.release(ping.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (deferred CLOSE behind parked ACK)");
                    } catch (PeerIsSlowToReadException expected) {
                    }
                    Assert.assertEquals(
                            "test setup: CLOSE must be deferred behind the parked ACK (SEND_STATE_RESUME_ACK_THEN_CLOSE)",
                            7, state.getSendState()
                    );

                    // Phase E: the client drains its receive buffer; the
                    // dispatcher fires resumeSend. The parked ACK flushes,
                    // then the deferred CLOSE goes out -- and the connection
                    // enters the close-echo wait (RFC 6455 close handshake)
                    // instead of tearing down: the final durable ack it just
                    // delivered is only provably consumed once the client's
                    // CLOSE echo arrives.
                    processor.resumeSend(context);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the deferred CLOSE",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase F: the client's CLOSE echo completes the handshake;
                    // only then does the framework tear the connection down.
                    completeCloseEcho(processor, context, nf, closeEcho.length);

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The narrow variant from the finding: the cumulative ACK blocks INSIDE
     * {@code sendFatalClose}'s own {@code flushPendingAck}, before
     * {@code trySendDurableAck} is ever reached. Requires a committed frame
     * and the close trigger in the same recv chunk (the chunk-end ACK flush
     * has not run yet), which needs no demote machinery: a protocol-violating
     * TEXT frame after a committed BINARY frame does it. Durable coverage is
     * complete the whole time, yet the resume path closes without ever
     * sending the durable ack.
     */
    @Test
    public void testFatalCloseMustFlushDurableAckWhenAckBlocksDuringClose() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(Long.MAX_VALUE);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabb (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabb", 100L, 1_000_000L));
                byte[] textFrame = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'h', 'i'});
                byte[] wire = concat(frame0, textFrame);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Both frames land in one recv chunk: frame0 commits
                    // (pending cumulative ACK, chunk-end flush not yet run),
                    // then the TEXT frame routes to sendFatalClose, whose
                    // flushPendingAck attempts the cumulative ACK first --
                    // and blocks. trySendDurableAck is never reached; state
                    // collapses to RESUME_ACK_THEN_CLOSE.
                    rawSocket.throwSlowToReadOnCall = 1;
                    nf.release(wire.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (deferred CLOSE behind blocked ACK)");
                    } catch (PeerIsSlowToReadException expected) {
                    }
                    Assert.assertEquals(
                            "test setup: CLOSE must be deferred behind the blocked ACK (SEND_STATE_RESUME_ACK_THEN_CLOSE)",
                            7, state.getSendState()
                    );

                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException after deferred CLOSE flush");
                    } catch (ServerDisconnectException expected) {
                    }

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1003 /* UNSUPPORTED_DATA */);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Client-initiated CLOSE with the pre-response ACK send itself parked:
     * {@code [BINARY, CLOSE]} in one recv chunk, so {@code handleClose}'s
     * {@code flushPendingAck} attempts the cumulative ACK for the committed
     * frame and blocks. The old code swallowed the backpressure, the CLOSE
     * dispatch threw {@code ServerDisconnectException}, and
     * {@code onConnectionClosed}'s single best-effort flush gave up under the
     * same backpressure -- the client's LAST cumulative ack was lost and its
     * committed-but-unacknowledged work replayed after reconnect (duplicates
     * on tables without DEDUP UPSERT KEYS).
     * <p>
     * Fixed behaviour: the backpressure propagates (the framework parks the
     * connection for write) with an ack-then-close-response continuation
     * armed; {@code resumeSend} finishes the ACK, emits the close response
     * echoing the client's code, then disconnects.
     */
    @Test
    public void testClientCloseAckBackpressurePreservesFinalAck() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabcc (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabcc", 100L, 1_000_000L));
                // client-initiated CLOSE, NORMAL_CLOSURE (1000) big-endian
                byte[] clientClose = createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8});
                byte[] wire = concat(frame0, clientClose);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Both frames in one recv chunk: frame0 commits (pending
                    // cumulative ACK, chunk-end flush not yet run), then
                    // handleClose's flushPendingAck attempts the ACK -- and
                    // blocks. The backpressure must PROPAGATE so the framework
                    // parks for write; ServerDisconnectException here means
                    // the dispatch closed the connection with the client's
                    // last cumulative ack still parked.
                    rawSocket.throwSlowToReadOnCall = 1;
                    nf.release(wire.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (final ACK parked behind client CLOSE)");
                    } catch (PeerIsSlowToReadException expected) {
                        // parked for write; the ack-then-close-response
                        // continuation must now be armed
                    } catch (ServerDisconnectException e) {
                        Assert.fail("FINAL ACK LOST: the CLOSE dispatch disconnected while the client's "
                                + "last cumulative ACK was still parked under write backpressure; "
                                + "committed-but-unacknowledged work will replay after reconnect");
                    }
                    Assert.assertEquals(
                            "client CLOSE under ack backpressure must park the "
                                    + "ack-then-close-response continuation "
                                    + "(SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE)",
                            12, state.getSendState()
                    );

                    // The client drains its receive buffer; the dispatcher
                    // fires resumeSend. The parked ACK flushes, the close
                    // response goes out, and only then does the connection
                    // tear down.
                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException after ack + close response");
                    } catch (ServerDisconnectException expected) {
                    }

                    int closeIdx = indexOfCloseFrame(rawSocket.sentFrames);
                    Assert.assertTrue("close response must be sent", closeIdx >= 0);
                    Assert.assertEquals(
                            "close response must echo the client's close code",
                            1000 /* NORMAL_CLOSURE */, closeCode(rawSocket.sentFrames.getQuick(closeIdx))
                    );
                    int ackIdx = indexOfCumulativeAckFrame(rawSocket.sentFrames);
                    Assert.assertTrue(
                            "FINAL ACK LOST: no cumulative ACK precedes the close response "
                                    + "(ackFrameIndex=" + ackIdx + ", closeFrameIndex=" + closeIdx
                                    + ", framesSent=" + rawSocket.sentFrames.size() + "); the client "
                                    + "never learns its committed data was persisted and replays it "
                                    + "after reconnect",
                            ackIdx >= 0 && ackIdx < closeIdx
                    );
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The deferral can also exit while a PONG (or error response) is parked:
     * {@code onFatalCloseBlocked} collapses those states to
     * {@code RESUME_CLOSE}, whose resume branch assumes the parked bytes ARE
     * the CLOSE frame. They are pong bytes — so the deferred CLOSE is never
     * written (the client sees a bare FIN with no close code) and the final
     * durable ack is dropped with it. Same stale-watermark consequence as the
     * other two tests, plus a missing protocol-level close signal.
     */
    @Test
    public void testRoleChangeCloseMustSendCloseAndDurableAckWhenPongParked() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabc (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabc", 100L, 1_000_000L));
                byte[] ping1 = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1});
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabc", 200L, 2_000_000L));
                byte[] ping2 = createMaskedFrame(WebSocketOpcode.PING, new byte[]{2});
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, ping1, frame1, ping2, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; the cumulative ACK
                    // (send #1) goes out cleanly. No durable progress yet:
                    // uploads lag, so no durable ack is emitted either.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: keepalive PING; the PONG (send #2) parks
                    // mid-write under send backpressure.
                    rawSocket.throwSlowToReadOnCall = 2;
                    nf.release(ping1.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (parked PONG)");
                    } catch (PeerIsSlowToReadException expected) {
                    }
                    Assert.assertFalse("test setup: PONG must be parked", state.isSendReady());

                    // Phase C: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase D: the demote drain completes.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase E: the next keepalive PING observes coverage; the
                    // deferral exits into sendFatalClose behind the parked PONG.
                    nf.release(ping2.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (deferred CLOSE behind parked PONG)");
                    } catch (PeerIsSlowToReadException expected) {
                    }

                    // Phase F: the client drains its receive buffer; the
                    // dispatcher fires resumeSend to finish the close. The
                    // CLOSE goes out and the connection enters the close-echo
                    // wait; the client's echo completes the handshake.
                    processor.resumeSend(context);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the deferred CLOSE",
                            state.isAwaitingCloseEcho()
                    );
                    completeCloseEcho(processor, context, nf, closeEcho.length);

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Grace-expiry diagnostics, PING re-entry: {@code handlePing} completes a
     * deferred role-change close through an inline copy of the completion
     * predicate and skips the "role-change close upload grace expired"
     * LOG.error that the equivalent exit in
     * {@code roleChangeCloseWithUploadGrace} emits. PING is the designated
     * recv-driven re-entry poll for a quiesced client (data frames are refused
     * by the deferral gate), so the one close the operator must see -- the
     * grace budget exhausting while committed work is still not durably
     * uploaded, exposing the client to replay duplicates -- happens silently.
     * <p>
     * Invariant (fix-agnostic): a grace-expired close with un-acked durable
     * work logs the grace-expired diagnostic no matter which re-entry point
     * observes the expiry.
     */
    @Test
    public void testGraceExpiredPingCloseMustLogAbandonedDurableWork() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L); // uploads lag for the whole test
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabd (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabd", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabd", 200L, 2_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] wire = concat(frame0, frame1, ping);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; the chunk-end
                    // cumulative ACK drains cleanly (send side stays READY).
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage
                    // (registry watermark lags at -1).
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: uploads STALL past the grace budget; committed
                    // work is still not durably uploaded.
                    nowMicros[0] += QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS;
                    Assert.assertTrue(
                            "test setup: grace budget must be exhausted",
                            state.isRoleChangeCloseGraceExpired()
                    );
                    Assert.assertFalse(
                            "test setup: durable work must NOT be fully uploaded",
                            state.isDurableWorkFullyUploaded(demotableEngine.getDurableAckRegistry())
                    );

                    // Phase D: the keepalive PING -- the designated deferral
                    // re-entry poll -- observes the expiry; the close proceeds
                    // abandoning un-acked durable work.
                    capture.start();
                    try {
                        nf.release(ping.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (grace-expired close)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: grace-expired PING close done");
                    } finally {
                        capture.stop();
                    }

                    // Behavioural lock (green before and after the fix): the
                    // close is still the reconnect-eligible NORMAL_CLOSURE.
                    int closeIdx = indexOfCloseFrame(rawSocket.sentFrames);
                    Assert.assertTrue("CLOSE frame must be sent", closeIdx >= 0);
                    Assert.assertEquals(
                            "CLOSE frame must carry the reconnect-eligible close code",
                            1000 /* NORMAL_CLOSURE */, closeCode(rawSocket.sentFrames.getQuick(closeIdx))
                    );

                    // RED until fixed: handlePing's inline completion
                    // predicate closes without emitting the diagnostic that
                    // roleChangeCloseWithUploadGrace emits on the same exit.
                    capture.assertLogged("role-change close upload grace expired");
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Eligibility TOCTOU on the grace-expired path. {@code sendFatalClose}
     * flushes the final durable ack (T1) against a registry that still lags,
     * sends the CLOSE, then {@code beginCloseEchoWaitIfEligible} decides the
     * echo wait (T2). The demote drain advances the registry concurrently, so
     * an upload can land in the (T1, T2] window. Pre-fix, T2 re-read the live
     * registry: the late advance made it arm the echo wait with the pending
     * durable maps still non-empty (T1 could not prune them), which both held
     * a 5s wait open on the path the design tears down immediately AND left a
     * STATUS_DURABLE_ACK frame primed to slip behind our CLOSE via the next
     * recv-driven flush (RFC 6455: nothing may follow the CLOSE).
     * <p>
     * The fix decides eligibility from local pending state
     * ({@code hasPendingDurableWork}), never a fresh registry read, so the
     * concurrent advance cannot change the decision: pending maps non-empty at
     * T2 means immediate teardown.
     * <p>
     * The race is reproduced deterministically by flipping the registry
     * watermark to fully-covered exactly when the recording socket observes
     * the CLOSE send -- which is emitted between T1 and T2. Pre-fix this arms
     * the wait (no {@code ServerDisconnectException}, {@code isAwaitingCloseEcho}
     * true); post-fix the connection tears down and the CLOSE stays final.
     */
    @Test
    public void testGraceExpiredEligibilityIgnoresRacingRegistryAdvance() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            // Lags through T1; the recording socket flips it to MAX when it
            // sees the CLOSE send, i.e. inside the (T1, T2] window.
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabp (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabp", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabp", 200L, 2_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] wire = concat(frame0, frame1, ping);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                // Arm the deterministic (T1, T2] registry advance.
                rawSocket.flipWatermarkToMaxOnCloseSend = durableWatermark;
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );
                    Assert.assertTrue(
                            "test setup: durable work must be pending (uploads lag)",
                            state.hasPendingDurableWork()
                    );

                    // Phase C: uploads stall past the grace budget; committed
                    // work is still not durably uploaded (watermark lags).
                    nowMicros[0] += QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS;
                    Assert.assertTrue(
                            "test setup: grace budget must be exhausted",
                            state.isRoleChangeCloseGraceExpired()
                    );

                    // Phase D: the keepalive PING drives the grace-expired
                    // close. sendFatalClose flushes at T1 (watermark still
                    // lags -> nothing to ack, maps stay non-empty), sends the
                    // CLOSE (the socket flips the watermark to MAX here), then
                    // reaches T2. The fix must decide eligibility from the
                    // still-non-empty maps, NOT the now-advanced registry, and
                    // tear down immediately.
                    capture.start();
                    try {
                        nf.release(ping.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException; a racing registry advance must NOT arm the echo wait");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: grace-expired eligibility race done");
                    } finally {
                        capture.stop();
                    }

                    Assert.assertFalse(
                            "the racing registry advance must NOT arm the close-echo wait",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "test integrity: the registry advance must have actually landed in the (T1, T2] window",
                            Long.MAX_VALUE, durableWatermark.get()
                    );
                    // The core invariant: no STATUS_DURABLE_ACK frame slipped
                    // behind the CLOSE.
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                    int closeIdx = indexOfCloseFrame(rawSocket.sentFrames);
                    Assert.assertTrue("CLOSE frame must be sent", closeIdx >= 0);
                    Assert.assertEquals(
                            "CLOSE frame must carry the reconnect-eligible close code",
                            1000 /* NORMAL_CLOSURE */, closeCode(rawSocket.sentFrames.getQuick(closeIdx))
                    );
                    // The alarm fires at T1 (watermark still lagging), before
                    // the CLOSE-triggered flip -- the operator still sees the
                    // one close they must see.
                    capture.assertLogged("role-change close upload grace expired");
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Grace-expiry diagnostics, false alarm: {@code roleChangeCloseWithUploadGrace}
     * raises "closing with un-acked durable work, client replay may duplicate"
     * purely on grace expiry, without consulting
     * {@code isDurableWorkFullyUploaded}. A slow-but-clean close -- uploads
     * catching up AFTER the deadline but BEFORE the next re-entry -- leaves an
     * empty replay window (the final durable ack precedes the CLOSE, locked
     * below), yet still fires the duplicate-risk alarm.
     * <p>
     * Invariant (fix-agnostic): a grace-expired close whose durable work is
     * fully uploaded must NOT claim un-acked durable work.
     */
    @Test
    public void testGraceExpiredCleanCloseMustNotRaiseFalseDuplicateAlarm() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabe (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabe", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabe", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabe", 300L, 3_000_000L));
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, frame2, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the slow-but-clean close. Uploads catch up,
                    // but only after the grace deadline has passed.
                    durableWatermark.set(Long.MAX_VALUE);
                    nowMicros[0] += QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS;
                    Assert.assertTrue(
                            "test setup: grace budget must be exhausted",
                            state.isRoleChangeCloseGraceExpired()
                    );
                    Assert.assertTrue(
                            "test setup: durable work must be fully uploaded",
                            state.isDurableWorkFullyUploaded(demotableEngine.getDurableAckRegistry())
                    );

                    // Phase D: a data frame re-enters through the deferral
                    // gate into roleChangeCloseWithUploadGrace; the close
                    // proceeds with an empty replay window.
                    capture.start();
                    try {
                        // The clean close enters the close-echo wait (coverage
                        // is complete, so the final durable ack is worth
                        // confirming); the client's echo completes it.
                        drive(processor, context, nf, frame2.length);
                        Assert.assertTrue(
                                "connection must await the client's close echo after the clean close",
                                state.isAwaitingCloseEcho()
                        );
                        completeCloseEcho(processor, context, nf, closeEcho.length);
                        drainLogQueue(capture, "sentinel: grace-expired clean close done");
                    } finally {
                        capture.stop();
                    }

                    // Behavioural lock (green before and after the fix): the
                    // close is clean -- final durable ack precedes the
                    // reconnect-eligible CLOSE, replay window empty.
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);

                    // RED until fixed: the grace-expired branch claims
                    // un-acked durable work without checking upload coverage.
                    capture.assertNotLogged("closing with un-acked durable work");
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Completion via the DATA-FRAME re-entry on coverage alone, strictly
     * WITHIN the grace budget. {@code isRoleChangeCloseCompletable} is a
     * disjunction -- durable coverage reached OR grace expired -- and every
     * other deferral-exit test in this class either completes via the PING
     * re-entry or crosses the grace deadline first (so the expiry disjunct
     * is also true when the close fires). This test isolates the remaining
     * exit: the injected clock NEVER moves off zero, so the ONLY way the
     * close can complete is the coverage disjunct, observed by the
     * gate-refused data frame in {@code handleBinaryMessage}'s deferral
     * branch.
     * <p>
     * Pins, fix-shape-agnostic:
     * <ul>
     *   <li>the deferral exits promptly on coverage -- a regression that
     *       quietly rewires completion to grace expiry alone (a full grace
     *       stall for every well-behaved client on every demote) goes red
     *       here and nowhere else;</li>
     *   <li>the final durable ack precedes the reconnect-eligible CLOSE on
     *       this exit too (the exactly-once handshake);</li>
     *   <li>the data frame that triggers the completion is refused, not
     *       committed -- INVARIANT B's engine-untouched deferral window;</li>
     *   <li>a within-grace close must not raise the grace-expired operator
     *       alarm.</li>
     * </ul>
     */
    @Test
    public void testDataFrameReEntryCompletesCloseWithinGraceOnCoverageAlone() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabf (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabf", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabf", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabf", 300L, 3_000_000L));
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, frame2, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage
                    // (registry watermark lags at -1).
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes WELL within the
                    // grace budget -- the clock never moves off zero, so the
                    // expiry disjunct stays false for the whole test.
                    durableWatermark.set(Long.MAX_VALUE);
                    Assert.assertFalse(
                            "test setup: grace budget must NOT be exhausted -- this test isolates the coverage disjunct",
                            state.isRoleChangeCloseGraceExpired()
                    );
                    Assert.assertTrue(
                            "test setup: durable work must be fully uploaded",
                            state.isDurableWorkFullyUploaded(demotableEngine.getDurableAckRegistry())
                    );

                    // Phase D: the writer is NOT quiesced -- the next data
                    // frame hits the deferral gate, which must refuse it AND
                    // observe coverage, completing the close on the spot.
                    capture.start();
                    try {
                        // The coverage-complete close enters the close-echo
                        // wait; the client's echo completes the handshake.
                        drive(processor, context, nf, frame2.length);
                        Assert.assertTrue(
                                "connection must await the client's close echo after the coverage-complete close",
                                state.isAwaitingCloseEcho()
                        );
                        completeCloseEcho(processor, context, nf, closeEcho.length);
                        drainLogQueue(capture, "sentinel: within-grace data-frame close done");
                    } finally {
                        capture.stop();
                    }

                    // The exactly-once handshake holds on this exit: the
                    // final durable ack precedes the reconnect-eligible CLOSE.
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);

                    // A within-grace close is clean by construction: the
                    // grace-expired operator alarm must not fire.
                    capture.assertNotLogged("role-change close upload grace expired");

                    // INVARIANT B: the data frame that completed the close
                    // was refused -- only seq=0's row may exist. A second row
                    // here means the deferral gate let a frame commit while
                    // its sequence was simultaneously marked unresolved for
                    // the ack clamp -- the double-accounting the gate exists
                    // to prevent. (The gate is engine-role-agnostic by then,
                    // so flip the read-only flag back for the WAL apply.)
                    readOnly.set(false);
                    drainWalQueue(demotableEngine);
                    try (TableReader reader = demotableEngine.getReader("tabf")) {
                        Assert.assertEquals(
                                "data frame refused during the deferral must not commit",
                                1, reader.size()
                        );
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Echo-eligibility gap: the exactly-once guard must depend on WHAT the
     * close delivers, not on WHETHER the close was ever deferred. Here the
     * uploader catches up in the gap between the last committed batch and
     * the demote's first gate-rejected frame, so
     * {@code roleChangeCloseWithUploadGrace} finds coverage already complete
     * and never arms the deferral. The final durable ack still goes out only
     * now -- durable acks are recv-driven, so no send opportunity existed
     * between the registry advance and the rejected frame -- and
     * {@code sendFatalClose} emits [durable ack][CLOSE] exactly like the
     * deferred path. Skipping the echo wait
     * ({@code beginCloseEchoWaitIfEligible} requires
     * {@code isRoleChangeCloseDeferred}) tears the fd down against a client
     * that is still streaming: unread in-flight frames make the close
     * abortive (RST) and destroy the client's unread [durable ack][CLOSE]
     * tail -- full-corpus replay, duplicates on tables without DEDUP UPSERT
     * KEYS. Same delivery contract, same wait: upload completion a moment
     * before the first rejection must not be weaker than a moment after.
     */
    @Test
    public void testCloseEchoWaitMustArmWhenCoverageCompletesBeforeFirstRejectedFrame() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabq (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabq", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabq", 200L, 2_000_000L));
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; the cumulative ACK
                    // drains, but the durable ack stays pending -- the
                    // registry watermark still lags at -1.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());
                    Assert.assertTrue(
                            "test setup: seq=0's durable ack must still be pending",
                            state.hasPendingDurableWork()
                    );

                    // Phase B: the uploader catches up BEFORE the demote's
                    // first rejected frame. Durable acks are recv-driven, so
                    // the client has still not seen the durable ack when the
                    // next frame arrives.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase C: in-place demote. seq=1 is gate-rejected;
                    // coverage is already complete, so the close completes
                    // immediately: sendFatalClose flushes the FIRST durable
                    // ack, then the CLOSE. Delivery of that ack is only
                    // provable via the close-echo handshake -- the connection
                    // must enter the echo wait exactly as the deferred path
                    // does, not tear down.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo: the durable ack sent just before"
                                    + " the CLOSE is unconfirmed, whether or not the close was ever deferred",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase D: the client's CLOSE echo completes the handshake.
                    completeCloseEcho(processor, context, nf, closeEcho.length);

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Pins the close-echo wait window itself: after the coverage-complete
     * CLOSE goes out, the connection stays up but inert -- in-flight client
     * data frames are read and DISCARDED (no engine work, or an in-place
     * re-promote inside the window would commit them and advance the
     * cumulative ack past the refused frame that armed the deferral), and
     * nothing is sent after our CLOSE (no pong for a stray PING, no close
     * response for the echo -- RFC 6455 allows no frames after CLOSE).
     * Reading-and-dropping is the point: an unread inbound frame at fd-close
     * time makes the close abortive (RST) and destroys the client's unread
     * [durable ack][CLOSE] tail -- the exact loss the echo wait exists to
     * prevent (SqlFailoverQwpDeferredCloseExactlyOnceTest failure mode:
     * full-corpus replay, count &gt; appended on a dedup-free table).
     */
    @Test
    public void testCloseEchoWaitDiscardsInboundAndCompletesOnEcho() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabg (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabg", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabg", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabg", 300L, 3_000_000L));
                byte[] frame3 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabg", 400L, 4_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[]{7});
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, frame2, frame3, ping, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase D: the data-frame re-entry completes the CLOSE and
                    // enters the close-echo wait.
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the coverage-complete close",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase E: the client has not processed our CLOSE yet and
                    // keeps pumping -- a data frame and a keepalive PING land.
                    // Both must be read and discarded: the wait survives, no
                    // engine work happens, and NOTHING goes out after CLOSE.
                    drive(processor, context, nf, frame3.length + ping.length);
                    Assert.assertTrue(
                            "echo wait must survive in-flight client frames",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "no frame may be sent after our CLOSE (RFC 6455)",
                            framesAtClose, rawSocket.sentFrames.size()
                    );

                    // Phase F: the echo completes the handshake.
                    completeCloseEcho(processor, context, nf, closeEcho.length);

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);

                    // Only seq=0's row may exist: the refused frame that armed
                    // the deferral, the re-entry frame that completed the
                    // close, and the echo-window frame were all refused or
                    // discarded. (The gate is engine-role-agnostic by then, so
                    // flip the read-only flag back for the WAL apply.)
                    readOnly.set(false);
                    drainWalQueue(demotableEngine);
                    try (TableReader reader = demotableEngine.getReader("tabg")) {
                        Assert.assertEquals(
                                "frames refused during the deferral or discarded during the echo wait must not commit",
                                1, reader.size()
                        );
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The echo wait's availability escape: a peer that never echoes (wedged
     * client, half-open connection) must not hold the demoted node's
     * connection forever. The bounded budget
     * ({@link QwpIngressProcessorState#CLOSE_ECHO_WAIT_GRACE_MICROS}) is
     * polled on inbound re-entry -- here the keepalive PING -- and on expiry
     * the close proceeds without delivery confirmation, logging the
     * duplicate-risk diagnostic (the same availability-over-duplicate-guard
     * trade the upload grace makes).
     */
    @Test
    public void testCloseEchoWaitExpiryTearsDownWithoutEcho() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabh (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabh", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabh", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabh", 300L, 3_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] wire = concat(frame0, frame1, frame2, ping);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes; the data-frame
                    // re-entry completes the CLOSE and enters the echo wait.
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the coverage-complete close",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase D: the echo never arrives; the budget expires. The
                    // next inbound event -- the keepalive PING -- polls the
                    // deadline and the close proceeds without confirmation.
                    nowMicros[0] += QwpIngressProcessorState.CLOSE_ECHO_WAIT_GRACE_MICROS;
                    Assert.assertTrue(
                            "test setup: echo grace budget must be exhausted",
                            state.isCloseEchoWaitExpired()
                    );
                    capture.start();
                    try {
                        nf.release(ping.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (echo wait expired)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: echo-wait expiry close done");
                    } finally {
                        capture.stop();
                    }

                    // The operator-visible diagnostic: closing without
                    // delivery confirmation exposes the client to replay
                    // duplicates.
                    capture.assertLogged("close echo wait expired");
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The echo-wait discard gate must cover protocol-violating opcodes too.
     * TEXT and fragmented BINARY/CONTINUATION frames route to
     * {@code sendFatalClose}, whose contract changed with the echo wait: it
     * RETURNS (rather than throwing) once the wait is armed. Ungated, a
     * fragmenting intermediary's frame landing inside the echo window (the
     * reject path's own diagnostic documents proxies/LBs splitting frames as
     * a real occurrence) would emit a SECOND CLOSE frame after the
     * role-change CLOSE -- RFC 6455 allows none -- skip the expiry poll, and
     * silently re-enter the wait: one extra CLOSE per inbound frame. Pins:
     * violating frames are discarded exactly like data frames (no second
     * CLOSE, the wait survives), the client's echo still completes the
     * handshake, and the engine stays untouched.
     */
    @Test
    public void testCloseEchoWaitDiscardsProtocolViolatingFramesWithoutSecondClose() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabi (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabi", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabi", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabi", 300L, 3_000_000L));
                // What a fragmenting proxy makes of one data frame -- a FIN=0
                // BINARY leader and its CONTINUATION tail -- plus a stray TEXT
                // frame. Outside the echo window all three are fatal protocol
                // violations; inside it they must be discarded.
                byte[] fragLeader = createMaskedFragmentFrame(WebSocketOpcode.BINARY, new byte[]{1, 2, 3});
                byte[] fragTail = createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{4, 5, 6});
                byte[] textFrame = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'h', 'i'});
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, frame2, fragLeader, fragTail, textFrame, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase D: the data-frame re-entry completes the CLOSE and
                    // enters the close-echo wait.
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the coverage-complete close",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase E: the protocol violations land inside the echo
                    // window. Each one must be read and DISCARDED: no reject
                    // CLOSE may follow our CLOSE, and the wait must survive
                    // (a second CLOSE plus a silent re-arm is exactly the
                    // ungated-sendFatalClose failure shape).
                    drive(processor, context, nf, fragLeader.length + fragTail.length + textFrame.length);
                    Assert.assertTrue(
                            "echo wait must survive protocol-violating frames in the echo window",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "no frame may be sent after our CLOSE (RFC 6455): a rejected TEXT/fragmented"
                                    + " frame must not emit a second CLOSE",
                            framesAtClose, rawSocket.sentFrames.size()
                    );

                    // Phase F: the echo completes the handshake.
                    completeCloseEcho(processor, context, nf, closeEcho.length);

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);

                    // Only seq=0's row may exist: everything after the CLOSE
                    // was refused or discarded without touching the engine.
                    readOnly.set(false);
                    drainWalQueue(demotableEngine);
                    try (TableReader reader = demotableEngine.getReader("tabi")) {
                        Assert.assertEquals(
                                "frames refused during the deferral or discarded during the echo wait must not commit",
                                1, reader.size()
                        );
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The expiry poll must fire on the protocol-violating re-entry path too:
     * when a wedged peer's only inbound events are frames that would
     * otherwise route to {@code sendFatalClose}, they must observe the
     * exhausted echo budget and tear the connection down with the
     * duplicate-risk diagnostic -- exactly like the keepalive PING poll --
     * not emit a second CLOSE and keep the wait alive forever.
     */
    @Test
    public void testCloseEchoWaitExpiryPolledOnProtocolViolatingFrame() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabj (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabj", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabj", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabj", 300L, 3_000_000L));
                byte[] textFrame = createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'h', 'i'});
                byte[] wire = concat(frame0, frame1, frame2, textFrame);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes; the data-frame
                    // re-entry completes the CLOSE and enters the echo wait.
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the coverage-complete close",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase D: no echo; the budget expires. The next inbound
                    // event is a protocol-violating TEXT frame -- it must
                    // poll the deadline and the close proceeds without
                    // confirmation.
                    nowMicros[0] += QwpIngressProcessorState.CLOSE_ECHO_WAIT_GRACE_MICROS;
                    Assert.assertTrue(
                            "test setup: echo grace budget must be exhausted",
                            state.isCloseEchoWaitExpired()
                    );
                    capture.start();
                    try {
                        nf.release(textFrame.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (echo wait expired)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: echo-wait expiry via violating frame done");
                    } finally {
                        capture.stop();
                    }

                    capture.assertLogged("close echo wait expired");
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    // Even on the expiry path the rejected frame must not
                    // have emitted a reject CLOSE of its own.
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The defense-in-depth full-buffer branch in resumeRecv: when the recv
     * buffer is completely full mid-frame during the echo wait, the trailing
     * frame can never complete and frame sync is lost. In production
     * geometry the header-parse too-big check fires first (the configured
     * recv buffer size and the context's buffer are the same), so this
     * branch is reached only when the two sizes diverge -- this test keeps
     * the default (larger) configured size so the header check stays quiet
     * and the buffer genuinely fills. Pins: the branch enters the sync-lost
     * discard mode instead of returning normally without consuming socket
     * bytes -- the pre-fix behavior that left the jammed frame's tail unread
     * in the kernel buffer and spun the dispatcher's edge-triggered oneshot
     * re-arm until the grace expired.
     */
    @Test
    public void testCloseEchoWaitFullBufferJamEntersDiscardMode() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabo (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabo", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabo", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabo", 300L, 3_000_000L));
                // Declared payload (2000 bytes) exceeds the context's recv
                // buffer (1024) but not the configured recv buffer size (the
                // 128K default), so the header-parse too-big check stays
                // quiet and the frame jams the buffer at full capacity.
                byte[] oversizedHeader = {
                        (byte) 0x82,          // FIN | BINARY
                        (byte) (0x80 | 126),  // MASK | 16-bit extended length
                        0x07, (byte) 0xD0,    // payload length 2000, big-endian
                        0x12, 0x34, 0x56, 0x78 // mask key
                };
                byte[] oversizedBody = new byte[RECV_BUFFER_SIZE - oversizedHeader.length];
                byte[] wire = concat(frame0, frame1, frame2, oversizedHeader, oversizedBody);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes; the data-frame
                    // re-entry completes the CLOSE and enters the echo wait.
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "test setup: connection must await the client's close echo",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase D: header and body fill the recv buffer to
                    // capacity; the frame can never complete.
                    drive(processor, context, nf, oversizedHeader.length);
                    drive(processor, context, nf, oversizedBody.length);
                    Assert.assertEquals(
                            "test setup: the jammed frame must fill the recv buffer",
                            RECV_BUFFER_SIZE, state.getRecvBufferLen()
                    );
                    Assert.assertFalse(
                            "test setup: the full buffer, not the header check, must detect the jam in this geometry",
                            state.hasLostCloseEchoSync()
                    );

                    // Phase E: the next readable event hits the full-buffer
                    // branch, which must enter the sync-lost discard mode:
                    // drop the unparseable bytes and re-arm for read.
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("resumeRecv must throw PeerIsSlowToWriteException after entering discard mode");
                    } catch (PeerIsSlowToWriteException expected) {
                    }
                    Assert.assertTrue(
                            "the full-buffer jam must flip the connection into the sync-lost discard mode",
                            state.hasLostCloseEchoSync()
                    );
                    Assert.assertEquals(
                            "sync loss must drop the unparseable buffered bytes",
                            0, state.getRecvBufferLen()
                    );
                    Assert.assertTrue(
                            "echo wait must survive the full-buffer jam",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "the full-buffer branch must not emit a CLOSE during the echo wait",
                            framesAtClose, rawSocket.sentFrames.size()
                    );
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The oversized-frame paths during the echo wait: a frame whose declared
     * payload exceeds the recv buffer can never be parsed, so the CLOSE echo
     * behind it is unreachable -- frame sync is lost for good. Two pre-fix
     * failure modes are pinned here. First, ungated, the too-big branches
     * routed to sendFatalClose and each inbound event emitted another
     * MESSAGE_TOO_BIG CLOSE after our role-change CLOSE. Second, gated but
     * without consuming socket bytes, resumeRecv returned normally while the
     * jammed frame's tail sat unread in the kernel buffer: the dispatcher's
     * edge-triggered oneshot re-arm re-fired on the stale readiness
     * immediately, spinning dispatcher and worker at full speed until the
     * grace expired. Pins: the sync-lost paths send nothing, drop the
     * unparseable buffered bytes, read-and-discard every subsequent socket
     * byte (empty kernel buffer = no stale-readiness re-fire), re-arm via
     * PeerIsSlowToWriteException, and stay bounded by
     * CLOSE_ECHO_WAIT_GRACE_MICROS -- the first re-entry past the deadline
     * tears down with the duplicate-risk diagnostic.
     */
    @Test
    public void testCloseEchoWaitOversizedFrameBoundedByEchoGrace() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            // Production geometry: the processor's configured recv buffer
            // size (the header-parse too-big threshold) must equal the
            // context's actual recv buffer, as it does in a real server.
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return RECV_BUFFER_SIZE;
                }
            };

            execute("create table tabk (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabk", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabk", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabk", 300L, 3_000_000L));
                // A frame whose declared payload (2000 bytes) exceeds the
                // recv buffer (1024): the header alone trips the too-big
                // branch at parse time and flips the connection into the
                // sync-lost discard mode; the body then streams into the
                // discard gate.
                byte[] oversizedHeader = {
                        (byte) 0x82,          // FIN | BINARY
                        (byte) (0x80 | 126),  // MASK | 16-bit extended length
                        0x07, (byte) 0xD0,    // payload length 2000, big-endian
                        0x12, 0x34, 0x56, 0x78 // mask key
                };
                byte[] oversizedBody = new byte[RECV_BUFFER_SIZE - oversizedHeader.length];
                byte[] wire = concat(frame0, frame1, frame2, oversizedHeader, oversizedBody);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes; the data-frame
                    // re-entry completes the CLOSE and enters the echo wait.
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "connection must await the client's close echo after the coverage-complete close",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase D: the oversized header lands inside the echo
                    // window -- the too-big branch fires at header parse and
                    // declares frame sync lost. No CLOSE may go out; the
                    // wait must survive; the unparseable buffered bytes must
                    // be dropped, not retained for a later misparse.
                    drive(processor, context, nf, oversizedHeader.length);
                    Assert.assertTrue(
                            "echo wait must survive the too-big frame header",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertTrue(
                            "the too-big header must flip the connection into the sync-lost discard mode",
                            state.hasLostCloseEchoSync()
                    );
                    Assert.assertEquals(
                            "sync loss must drop the unparseable buffered bytes",
                            0, state.getRecvBufferLen()
                    );
                    Assert.assertEquals(
                            "the too-big branch must not emit a CLOSE during the echo wait",
                            framesAtClose, rawSocket.sentFrames.size()
                    );

                    // Phase E: the body streams in behind the jammed header.
                    // The sync-lost gate must read-and-discard every byte --
                    // an empty kernel buffer is what stops the edge-triggered
                    // oneshot re-arm from re-firing on stale readiness and
                    // spinning until the grace expires -- and re-arm for read
                    // via PeerIsSlowToWriteException. Still nothing may go
                    // out.
                    nf.release(oversizedBody.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("resumeRecv must throw PeerIsSlowToWriteException after discarding the jammed bytes");
                    } catch (PeerIsSlowToWriteException expected) {
                    }
                    Assert.assertEquals(
                            "the sync-lost gate must consume every pending socket byte",
                            0, nf.pendingBytes()
                    );
                    Assert.assertEquals(
                            "discarded bytes must not accumulate in the recv buffer",
                            0, state.getRecvBufferLen()
                    );
                    Assert.assertTrue(
                            "echo wait must survive the discard re-entries",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "the discard gate must not emit a CLOSE during the echo wait",
                            framesAtClose, rawSocket.sentFrames.size()
                    );

                    // Phase F: the cycle must be bounded by the echo grace,
                    // not by peer death or the idle reaper: the first
                    // re-entry past the deadline tears the connection down
                    // with the duplicate-risk diagnostic.
                    nowMicros[0] += QwpIngressProcessorState.CLOSE_ECHO_WAIT_GRACE_MICROS;
                    capture.start();
                    try {
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (echo wait expired)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: echo-wait expiry via oversized frame done");
                    } finally {
                        capture.stop();
                    }

                    capture.assertLogged("close echo wait expired");
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The sync-lost drain must be bounded per dispatch. Pre-fix,
     * {@code discardInboundBytes} looped until recv() returned zero with the
     * echo deadline polled only BEFORE the loop: a peer that keeps the
     * kernel receive buffer non-empty (flooding garbage behind the oversized
     * frame that killed frame sync) keeps recv() positive indefinitely,
     * pinning the HTTP worker, starving every other connection dispatched to
     * it, and never re-polling the deadline. Pins: one dispatch consumes at
     * most CLOSE_ECHO_DISCARD_READ_BUDGET reads then yields via
     * PeerIsSlowToWriteException with the wait still armed (the leftover
     * readiness re-fires the dispatcher, so the drain continues next
     * dispatch with a fresh budget); and a dispatch entered past the grace
     * deadline tears down promptly even against a still-flooding peer, its
     * pre-close best-effort drain bounded by GRACEFUL_CLOSE_DRAIN_READ_BUDGET
     * rather than delegated to HttpConnectionContext.drainRecvBuffer's
     * unbounded {@code while (recv() > 0)} loop.
     */
    @Test
    public void testCloseEchoWaitSyncLostDrainYieldsWithinReadBudgetAgainstFloodingPeer() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return RECV_BUFFER_SIZE;
                }
            };

            execute("create table tabr (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabr", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabr", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabr", 300L, 3_000_000L));
                // Declared payload (2000) exceeds the recv buffer (1024): the
                // header alone kills frame sync inside the echo wait.
                byte[] oversizedHeader = {
                        (byte) 0x82,          // FIN | BINARY
                        (byte) (0x80 | 126),  // MASK | 16-bit extended length
                        0x07, (byte) 0xD0,    // payload length 2000, big-endian
                        0x12, 0x34, 0x56, 0x78 // mask key
                };
                byte[] wire = concat(frame0, frame1, frame2, oversizedHeader);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phases A-C: commit, demote, coverage complete; the
                    // role-change CLOSE goes out and the echo wait arms.
                    drive(processor, context, nf, frame0.length);
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "test setup: connection must await the client's close echo",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase D: the oversized header kills frame sync inside
                    // the wait; the connection is now in discard mode.
                    drive(processor, context, nf, oversizedHeader.length);
                    Assert.assertTrue(
                            "test setup: the too-big header must flip the connection into the sync-lost discard mode",
                            state.hasLostCloseEchoSync()
                    );

                    // Phase E: the peer floods -- every recv returns a full
                    // buffer of garbage. One dispatch must consume exactly
                    // the read budget, then yield the worker.
                    nf.startFlood(64, null, 0);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("resumeRecv must yield via PeerIsSlowToWriteException when the discard read budget is exhausted");
                    } catch (PeerIsSlowToWriteException expected) {
                    }
                    Assert.assertEquals(
                            "one dispatch must drain exactly the per-dispatch read budget from a flooding peer, then yield the worker",
                            QwpIngressUpgradeProcessor.CLOSE_ECHO_DISCARD_READ_BUDGET, nf.floodReadsObserved()
                    );
                    Assert.assertTrue(
                            "echo wait must survive the budget-bounded dispatch",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase F: the dispatcher re-fires on the leftover
                    // readiness; the next dispatch continues with a FRESH
                    // budget -- progress without monopolization.
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("resumeRecv must yield via PeerIsSlowToWriteException when the discard read budget is exhausted");
                    } catch (PeerIsSlowToWriteException expected) {
                    }
                    Assert.assertEquals(
                            "the drain must continue on the next dispatch with a fresh budget",
                            2 * QwpIngressUpgradeProcessor.CLOSE_ECHO_DISCARD_READ_BUDGET, nf.floodReadsObserved()
                    );
                    Assert.assertEquals(
                            "nothing may be sent while discarding flood bytes",
                            framesAtClose, rawSocket.sentFrames.size()
                    );

                    // Phase G: grace expires. The teardown must be prompt
                    // even though the peer keeps flooding: the pre-close
                    // best-effort drain is bounded, not "until recv() == 0".
                    nowMicros[0] += QwpIngressProcessorState.CLOSE_ECHO_WAIT_GRACE_MICROS;
                    int floodReadsBeforeExpiry = nf.floodReadsObserved();
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected ServerDisconnectException (echo wait expired)");
                    } catch (ServerDisconnectException expected) {
                    }
                    Assert.assertTrue(
                            "the expiry teardown's best-effort drain must be bounded against a flooding peer",
                            nf.floodReadsObserved() - floodReadsBeforeExpiry
                                    <= QwpIngressUpgradeProcessor.GRACEFUL_CLOSE_DRAIN_READ_BUDGET
                    );
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The echo deadline must be observable WHILE the drain runs, not only on
     * dispatch entry. The flood advances the test clock per read, modeling
     * wall-clock time passing while the worker drains a peer that keeps the
     * kernel buffer non-empty: the deadline is crossed mid-drain and the
     * SAME dispatch must tear the connection down with the duplicate-risk
     * diagnostic. Pre-fix the loop never re-checked the deadline: the
     * dispatch consumed the entire flood (bounded by the facade here so the
     * suite cannot hang; unbounded in production) and returned with the
     * connection still alive arbitrarily far past its grace.
     */
    @Test
    public void testCloseEchoWaitSyncLostDrainPollsDeadlineAgainstFloodingPeer() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final long[] nowMicros = {0L};
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return RECV_BUFFER_SIZE;
                }
            };

            execute("create table tabs (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabs", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabs", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabs", 300L, 3_000_000L));
                byte[] oversizedHeader = {
                        (byte) 0x82,          // FIN | BINARY
                        (byte) (0x80 | 126),  // MASK | 16-bit extended length
                        0x07, (byte) 0xD0,    // payload length 2000, big-endian
                        0x12, 0x34, 0x56, 0x78 // mask key
                };
                byte[] wire = concat(frame0, frame1, frame2, oversizedHeader);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupClockedState(httpConfig, context, demotableEngine, nowMicros);

                    // Phases A-C: commit, demote, coverage complete; the
                    // role-change CLOSE goes out and the echo wait arms at
                    // clock 0 (deadline = CLOSE_ECHO_WAIT_GRACE_MICROS).
                    drive(processor, context, nf, frame0.length);
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "test setup: connection must await the client's close echo",
                            state.isAwaitingCloseEcho()
                    );

                    // Phase D: the oversized header kills frame sync inside
                    // the wait; the connection is now in discard mode.
                    drive(processor, context, nf, oversizedHeader.length);
                    Assert.assertTrue(
                            "test setup: the too-big header must flip the connection into the sync-lost discard mode",
                            state.hasLostCloseEchoSync()
                    );

                    // Phase E: the peer floods and the clock advances by a
                    // generous grace-quarter per read: the fourth read
                    // crosses the deadline MID-DRAIN, well inside the read
                    // budget -- only an in-loop poll can observe it.
                    nf.startFlood(64, nowMicros, QwpIngressProcessorState.CLOSE_ECHO_WAIT_GRACE_MICROS / 4 + 1);
                    capture.start();
                    try {
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("deadline crossed mid-drain must tear the connection down in the same dispatch");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: flooding-peer deadline poll done");
                    } finally {
                        capture.stop();
                    }

                    capture.assertLogged("close echo wait expired");
                    Assert.assertTrue(
                            "teardown must complete within the deadline-crossing reads plus the bounded pre-close drain",
                            nf.floodReadsObserved()
                                    <= 4 + QwpIngressUpgradeProcessor.GRACEFUL_CLOSE_DRAIN_READ_BUDGET
                    );
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Peer FIN during the sync-lost phase of the echo wait: the
     * read-and-discard loop observes recv < 0 and tears the connection down.
     * During the echo wait the FIN is delivery confirmation -- the client
     * consumed the [durable ack][CLOSE] tail and closed its end -- so this
     * teardown is the success path, not an error: the wait must not linger
     * for the rest of the grace budget once the peer is gone.
     */
    @Test
    public void testCloseEchoWaitSyncLostPeerFinEndsWait() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            // Production geometry: configured recv buffer size == context
            // recv buffer, so the header-parse too-big branch declares the
            // sync loss (see testCloseEchoWaitOversizedFrameBoundedByEchoGrace).
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return RECV_BUFFER_SIZE;
                }
            };

            execute("create table tabn (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabn", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabn", 200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabn", 300L, 3_000_000L));
                // Declared payload (2000 bytes) exceeds the recv buffer
                // (1024): the header alone trips the too-big branch and
                // flips the connection into sync-lost discard mode.
                byte[] oversizedHeader = {
                        (byte) 0x82,          // FIN | BINARY
                        (byte) (0x80 | 126),  // MASK | 16-bit extended length
                        0x07, (byte) 0xD0,    // payload length 2000, big-endian
                        0x12, 0x34, 0x56, 0x78 // mask key
                };
                byte[] wire = concat(frame0, frame1, frame2, oversizedHeader);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains.
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes; the data-frame
                    // re-entry completes the CLOSE and enters the echo wait.
                    durableWatermark.set(Long.MAX_VALUE);
                    drive(processor, context, nf, frame2.length);
                    Assert.assertTrue(
                            "test setup: connection must await the client's close echo",
                            state.isAwaitingCloseEcho()
                    );
                    int framesAtClose = rawSocket.sentFrames.size();

                    // Phase D: the oversized header lands; frame sync is lost.
                    drive(processor, context, nf, oversizedHeader.length);
                    Assert.assertTrue(
                            "test setup: the too-big header must flip the connection into the sync-lost discard mode",
                            state.hasLostCloseEchoSync()
                    );

                    // Phase E: the peer closes its end. The discard loop must
                    // treat the FIN as the end of the close handshake and
                    // tear down -- not park for the rest of the grace budget.
                    nf.closePeer();
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected ServerDisconnectException (peer FIN ends the sync-lost echo wait)");
                    } catch (ServerDisconnectException expected) {
                    }
                    Assert.assertEquals(
                            "no frame may follow the role-change CLOSE",
                            framesAtClose, rawSocket.sentFrames.size()
                    );
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The role-change CLOSE parks mid-send while the client's CLOSE is
     * already sitting unprocessed in the recv buffer (crossing close). The
     * resume branch must return the send machine to READY when it arms the
     * echo wait, so the fall-through drainBufferedFrames dispatches the
     * buffered echo and the handshake completes immediately. The pre-fix
     * branch left sendState parked in RESUME_CLOSE for the whole wait:
     * drainBufferedFrames no-oped, the buffered echo was never dispatched,
     * and -- with a conformant post-close client sending nothing more (RFC
     * 6455 s5.5.1 has the client wait for the server to close TCP) -- the
     * recv-driven expiry poll never ran either, so the connection outlived
     * the echo budget until the transport idle reaper collected it.
     */
    @Test
    public void testParkedRoleChangeCloseResumeMustDispatchBufferedCrossingEcho() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabl (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabl", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabl", 200L, 2_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] closeEcho = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, ping, closeEcho);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains
                    // (send call 1).
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase D: the keepalive PING and the client's crossing
                    // CLOSE land in the same recv chunk. The PING re-entry
                    // flushes the final durable ack (send call 2) and exits
                    // the deferral into sendFatalClose, whose CLOSE (send
                    // call 3) parks mid-send -- the crossing CLOSE behind it
                    // is compacted into the recv buffer, unprocessed.
                    rawSocket.throwSlowToReadOnCall = 3;
                    nf.release(ping.length + closeEcho.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (parked role-change CLOSE)");
                    } catch (PeerIsSlowToReadException expected) {
                    }
                    Assert.assertFalse(
                            "test setup: echo wait must not be armed while the CLOSE tail is parked",
                            state.isAwaitingCloseEcho()
                    );
                    Assert.assertEquals(
                            "test setup: the client's crossing CLOSE must be buffered, unprocessed",
                            closeEcho.length, state.getRecvBufferLen()
                    );

                    // Phase E: the dispatcher fires resumeSend; the CLOSE
                    // tail flushes and the echo wait is armed. The resume
                    // branch must return the send machine to READY so the
                    // fall-through drainBufferedFrames dispatches the
                    // buffered echo -- completing the handshake NOW, via
                    // ServerDisconnectException, with no further inbound
                    // bytes required from the (conformant, silent) client.
                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException: the buffered crossing CLOSE completes"
                                + " the handshake at resume time; leaving it undispatched strands the"
                                + " connection until the idle reaper (the expiry poll is recv-driven and a"
                                + " post-close client sends nothing more)");
                    } catch (ServerDisconnectException expected) {
                    }

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The OTHER frame kind that can park into a resume-close state: the close
     * RESPONSE to a client-initiated CLOSE, blocked mid-send while a
     * role-change close deferral happens to be armed. The client's CLOSE is
     * already consumed, so the RFC 6455 handshake is complete the moment the
     * response tail flushes: the resume path must tear the connection down
     * immediately (s5.5.1: the server closes TCP first) and must NOT arm the
     * close-echo wait -- no echo can ever arrive. The pre-fix code parked
     * both frame kinds in one state (RESUME_CLOSE), and the resume branch's
     * eligibility flags (durable-ack mode, deferral armed, uploads covered)
     * could not tell them apart: it armed a wait for a handshake that was
     * already complete, stranding the connection until the idle reaper.
     */
    @Test
    public void testParkedCloseResponseResumeMustDisconnectWithoutEchoWait() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            execute("create table tabm (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = newEngineWithRegistry(readOnly, durableWatermark)) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabm", 100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tabm", 200L, 2_000_000L));
                // the client's VOLUNTARY close (Sender.close() during
                // failover), not an echo -- the server has sent no CLOSE yet
                byte[] clientClose = closeEchoFrame();
                byte[] wire = concat(frame0, frame1, clientClose);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                BlockingRecordingRawSocket rawSocket = new BlockingRecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = setupState(httpConfig, context, demotableEngine);

                    // Phase A: PRIMARY. seq=0 commits; cumulative ACK drains
                    // (send call 1).
                    drive(processor, context, nf, frame0.length);
                    Assert.assertTrue("test setup: cumulative ACK must have drained", state.isSendReady());

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: the demote drain completes -- every
                    // eligibility flag the resume branch consults is now
                    // true, which is exactly what made the conflated state
                    // dangerous.
                    durableWatermark.set(Long.MAX_VALUE);

                    // Phase D: the client closes voluntarily. handleClose
                    // flushes the final durable ack (send call 2), then the
                    // close response (send call 3) parks mid-send.
                    rawSocket.throwSlowToReadOnCall = 3;
                    nf.release(clientClose.length);
                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected PeerIsSlowToReadException (parked close response)");
                    } catch (PeerIsSlowToReadException expected) {
                    }
                    Assert.assertEquals(
                            "test setup: the parked frame must be recorded as a close RESPONSE"
                                    + " (SEND_STATE_RESUME_CLOSE_RESPONSE), distinct from a parked fatal CLOSE",
                            11, state.getSendState()
                    );

                    // Phase E: the dispatcher fires resumeSend; the response
                    // tail flushes and the handshake is complete. The resume
                    // path must disconnect NOW and must not arm the echo
                    // wait: the client's CLOSE was consumed in Phase D, so no
                    // echo can ever arrive, and the recv-driven expiry poll
                    // would never run against a conformant post-close client.
                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException: the close response tail completes the"
                                + " client-initiated close handshake; arming an echo wait here waits for a"
                                + " frame that can never arrive");
                    } catch (ServerDisconnectException expected) {
                    }
                    Assert.assertFalse(
                            "no close-echo wait may be armed for a client-initiated close",
                            state.isAwaitingCloseEcho()
                    );

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
                    assertCloseIsFinalFrame(rawSocket.sentFrames);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * The close-echo handshake's outbound half: after our CLOSE frame nothing
     * else may go out -- not a pong for a stray PING, not a close response
     * for the client's echo, not a late ack (RFC 6455: no frames after
     * CLOSE). The CLOSE must be the last recorded outbound frame.
     */
    private static void assertCloseIsFinalFrame(ObjList<byte[]> frames) {
        int closeIdx = indexOfCloseFrame(frames);
        Assert.assertTrue("CLOSE frame must be sent", closeIdx >= 0);
        Assert.assertEquals(
                "no frame may follow the CLOSE frame (RFC 6455); the close-echo wait must not answer"
                        + " pings, respond to the client's echo, or flush further acks",
                frames.size() - 1, closeIdx
        );
    }

    /**
     * The invariant all tests assert: durable coverage was confirmed at
     * close time, so a {@code STATUS_DURABLE_ACK} frame must precede the
     * CLOSE frame in the outbound frame log.
     */
    private static void assertFinalDurableAckPrecedesClose(ObjList<byte[]> frames, int expectedCloseCode) {
        int closeIdx = indexOfCloseFrame(frames);
        Assert.assertTrue("CLOSE frame must be sent on resume", closeIdx >= 0);
        Assert.assertEquals(
                "CLOSE frame must carry the deferred close code",
                expectedCloseCode, closeCode(frames.getQuick(closeIdx))
        );

        int durableIdx = indexOfDurableAckFrame(frames);
        Assert.assertTrue(
                "STALE REPLAY WATERMARK: the registry's durable-upload watermark covered every"
                        + " committed seqTxn at close time, but no STATUS_DURABLE_ACK frame precedes the"
                        + " CLOSE (durableAckFrameIndex=" + durableIdx + ", closeFrameIndex=" + closeIdx
                        + ", framesSent=" + frames.size() + "); a durable-ack store-and-forward client"
                        + " advances its replay/trim watermark only on STATUS_DURABLE_ACK frames, so on"
                        + " reconnect it replays batches the server already owns -- duplicates on tables"
                        + " without DEDUP UPSERT KEYS",
                durableIdx >= 0 && durableIdx < closeIdx
        );
    }

    private static int closeCode(byte[] closeFrame) {
        // small unmasked server frame: 2-byte header, close code big-endian
        return ((closeFrame[2] & 0xFF) << 8) | (closeFrame[3] & 0xFF);
    }

    /**
     * A client CLOSE echo: NORMAL_CLOSURE (1000), big-endian payload, masked
     * like every client frame. What {@code WebSocketClient} sends on receipt
     * of the server's CLOSE (RFC 6455 s5.5.1), before dispatching the close
     * to its handler.
     */
    private static byte[] closeEchoFrame() {
        return createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8});
    }

    /**
     * Feeds the client's CLOSE echo and asserts the handshake completes: the
     * dispatch path surfaces {@code ServerDisconnectException}, which is what
     * makes the framework tear the connection down -- now provably after the
     * client consumed the final durable ack that preceded our CLOSE.
     */
    private static void completeCloseEcho(
            QwpIngressUpgradeProcessor processor,
            HttpConnectionContext context,
            PhasedNetworkFacade nf,
            int echoLength
    ) {
        nf.release(echoLength);
        try {
            processor.resumeRecv(context);
            Assert.fail("Expected ServerDisconnectException on the client's close echo");
        } catch (ServerDisconnectException expected) {
        } catch (Exception e) {
            throw new AssertionError("unexpected exception on close echo", e);
        }
    }

    private static byte[] concat(byte[]... arrays) {
        int len = 0;
        for (byte[] a : arrays) {
            len += a.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, out, pos, a.length);
            pos += a.length;
        }
        return out;
    }

    /**
     * Like {@link #createMaskedFrame}, but with FIN clear: the leading
     * fragment of a fragmented client message, as produced by a WebSocket
     * intermediary that splits frames.
     */
    private static byte[] createMaskedFragmentFrame(int opcode, byte[] payload) {
        byte[] frame = createMaskedFrame(opcode, payload);
        frame[0] &= 0x7F; // clear FIN
        return frame;
    }

    private static byte[] createMaskedFrame(int opcode, byte[] payload) {
        // all test frames are tiny; single-byte payload length is sufficient
        assert payload.length <= 125;
        byte[] frame = new byte[2 + 4 + payload.length];
        int offset = 0;
        frame[offset++] = (byte) (0x80 | (opcode & 0x0F));
        frame[offset++] = (byte) (0x80 | payload.length);
        System.arraycopy(DEFAULT_MASK_KEY, 0, frame, offset, 4);
        offset += 4;
        for (int i = 0; i < payload.length; i++) {
            frame[offset + i] = (byte) (payload[i] ^ DEFAULT_MASK_KEY[i % 4]);
        }
        return frame;
    }

    /**
     * QuestDB logging is asynchronous: {@code LOG.error()} enqueues and a
     * single writer job drains. Both grace-expiry diagnostics under test are
     * ERROR level, so logging an ERROR-level sentinel AFTER the action and
     * waiting for it guarantees the writer has drained every earlier record
     * of the same level -- making assertLogged/assertNotLogged race-free
     * without a blind timeout.
     */
    private static void drainLogQueue(LogCapture capture, String sentinel) {
        SENTINEL_LOG.error().$(sentinel).$();
        capture.waitFor(sentinel);
    }

    private static void drive(
            QwpIngressUpgradeProcessor processor,
            HttpConnectionContext context,
            PhasedNetworkFacade nf,
            int bytes
    ) throws Exception {
        nf.release(bytes);
        try {
            processor.resumeRecv(context);
        } catch (PeerIsSlowToWriteException e) {
            // all released bytes consumed; the dispatcher would re-arm for read
        }
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpIngressProcessorState> getLV() throws Exception {
        Field lvField = QwpIngressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpIngressProcessorState>) lvField.get(null);
    }

    /**
     * Index of the first CLOSE frame in the outbound log, or -1.
     */
    private static int indexOfCloseFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (f.length >= 4 && (f[0] & 0x0F) == WebSocketOpcode.CLOSE) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Index of the first server-to-client BINARY frame whose payload status
     * byte is {@code STATUS_OK} (a cumulative ACK), or -1. Server frames are
     * unmasked and small in these tests, so the payload starts at offset 2.
     */
    private static int indexOfCumulativeAckFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (f.length >= 3
                    && (f[0] & 0x0F) == WebSocketOpcode.BINARY
                    && (f[1] & 0x80) == 0
                    && (f[1] & 0x7F) < 126
                    && f[2] == QwpConstants.STATUS_OK) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Index of the first server-to-client BINARY frame whose payload status
     * byte is {@code STATUS_DURABLE_ACK}, or -1. Server frames are unmasked
     * and small in these tests, so the payload starts at offset 2.
     */
    private static int indexOfDurableAckFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (f.length >= 3
                    && (f[0] & 0x0F) == WebSocketOpcode.BINARY
                    && (f[1] & 0x80) == 0
                    && (f[1] & 0x7F) < 126
                    && f[2] == QwpConstants.STATUS_DURABLE_ACK) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Engine whose read-only mode and durable-upload watermark are test-owned
     * knobs. The registry treats the watermark as global: any dir name
     * reports the same durably-uploaded seqTxn, mirroring "uploads lag"
     * ({@code -1}) vs "uploads caught up" ({@code Long.MAX_VALUE}).
     */
    private static CairoEngine newEngineWithRegistry(AtomicBoolean readOnly, AtomicLong durableWatermark) {
        return new CairoEngine(new DefaultTestCairoConfiguration(root)) {
            private final DurableAckRegistry testRegistry = new DurableAckRegistry() {
                @Override
                public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                    return durableWatermark.get();
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            };

            @Override
            public @NotNull DurableAckRegistry getDurableAckRegistry() {
                return testRegistry;
            }

            @Override
            public boolean isReadOnlyMode() {
                return readOnly.get();
            }
        };
    }

    /**
     * QWP v1 message: one row into {@code tableName}, schema
     * [{@code v} LONG, {@code ""} TIMESTAMP (designated)].
     */
    private static byte[] oneRowMessage(String tableName, long value, long tsMicros) {
        int nameLen = tableName.length();
        byte[] payload = new byte[26 + nameLen];
        int i = 0;
        // table header
        payload[i++] = (byte) nameLen; // table name length (varint)
        for (int c = 0; c < nameLen; c++) {
            payload[i++] = (byte) tableName.charAt(c);
        }
        payload[i++] = 1; // rowCount (varint)
        payload[i++] = 2; // columnCount (varint)
        // schema
        payload[i++] = 1; // column name length (varint)
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_LONG;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v
        payload[i++] = 0; // no null bitmap
        i = writeLeLong(payload, i, value);
        // column data: designated timestamp
        payload[i++] = 0; // no null bitmap
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = 1; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payload.length;
        message[9] = 0;
        message[10] = 0;
        message[11] = 0;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * Same as {@link #setupState}, but the state's deferral clock is the
     * test-owned {@code nowMicros[0]}, so the grace deadline
     * ({@link QwpIngressProcessorState#ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS})
     * can be crossed deterministically. Clock-override pattern from
     * {@code QwpIngressProcessorStateTest#testRoleChangeCloseDeferralLifecycle}.
     */
    private static QwpIngressProcessorState setupClockedState(
            HttpFullFatServerConfiguration httpConfig,
            TestableContext context,
            CairoEngine engine,
            long[] nowMicros
    ) throws Exception {
        LineHttpProcessorConfiguration lineConfig =
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return () -> nowMicros[0];
                    }
                };
        QwpIngressProcessorState state = new QwpIngressProcessorState(
                RECV_BUFFER_SIZE,
                httpConfig.getSendBufferSize(),
                engine,
                lineConfig
        );
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        // durable-ack opt-in, as negotiated via X-QWP-Request-Durable-Ack
        state.setDurableAckEnabled(true);
        getLV().set(context, state);
        return state;
    }

    private static QwpIngressProcessorState setupState(
            HttpFullFatServerConfiguration httpConfig,
            TestableContext context,
            CairoEngine engine
    ) throws Exception {
        QwpIngressProcessorState state = new QwpIngressProcessorState(
                RECV_BUFFER_SIZE,
                httpConfig.getSendBufferSize(),
                engine,
                httpConfig.getLineHttpProcessorConfiguration()
        );
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        // durable-ack opt-in, as negotiated via X-QWP-Request-Durable-Ack
        state.setDurableAckEnabled(true);
        getLV().set(context, state);
        return state;
    }

    private static int writeLeLong(byte[] buf, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    /**
     * Captures every frame the server sends, in order, and can simulate a
     * slow client by throwing {@link PeerIsSlowToReadException} on the Nth
     * send. The blocked frame is still recorded BEFORE the throw: in the real
     * framework, {@code PeerIsSlowToReadException} from
     * {@code HttpRawSocket.send} means the bytes are queued in the framework
     * buffer and WILL be delivered by {@code resumeResponseSend} — the frame
     * is delayed, not dropped. (The assertions only ever test for the ABSENCE
     * of a durable-ack frame, so this fidelity choice cannot mask the bug.)
     */
    private static class BlockingRecordingRawSocket implements HttpRawSocket {
        final ObjList<byte[]> sentFrames = new ObjList<>();
        private final long bufferAddress;
        private final int bufferSize;
        // When set, the observed send of a server CLOSE frame flips this
        // watermark to Long.MAX_VALUE. The CLOSE leaves sendFatalClose AFTER
        // the final durable-ack flush (T1) and BEFORE the eligibility check
        // (T2), so this reproduces the (T1, T2] registry-advance race
        // deterministically -- no fragile call-counting.
        AtomicLong flipWatermarkToMaxOnCloseSend;
        int sendCallCount;
        int throwSlowToReadOnCall = -1;

        BlockingRecordingRawSocket(long bufferAddress, int bufferSize) {
            this.bufferAddress = bufferAddress;
            this.bufferSize = bufferSize;
        }

        @Override
        public long getBufferAddress() {
            return bufferAddress;
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public void send(int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
            byte[] copy = new byte[size];
            for (int i = 0; i < size; i++) {
                copy[i] = Unsafe.getByte(bufferAddress + i);
            }
            sentFrames.add(copy);
            if (flipWatermarkToMaxOnCloseSend != null
                    && size >= 1
                    && (copy[0] & 0x0F) == WebSocketOpcode.CLOSE) {
                flipWatermarkToMaxOnCloseSend.set(Long.MAX_VALUE);
            }
            if (++sendCallCount == throwSlowToReadOnCall) {
                throw PeerIsSlowToReadException.INSTANCE;
            }
        }
    }

    /**
     * Network facade that releases the client's wire bytes in explicit phases
     * so the engine's read-only flag and the registry watermark can be
     * flipped between frames — the demote/backpressure race distilled to its
     * deterministic core.
     */
    private static class PhasedNetworkFacade extends NetworkFacadeImpl {
        private final byte[] data;
        private long advanceClockPerFloodRead;
        private long[] floodClock;
        private int floodReadsObserved;
        private int floodReadsRemaining;
        private int limit;
        private boolean peerClosed;
        private int pos;

        PhasedNetworkFacade(byte[] data) {
            this.data = data;
        }

        @Override
        public void close(long fd, Log log) {
            // no-op for test
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (pos >= limit) {
                if (floodReadsRemaining > 0) {
                    floodReadsRemaining--;
                    floodReadsObserved++;
                    if (floodClock != null) {
                        floodClock[0] += advanceClockPerFloodRead;
                    }
                    // mid-frame garbage: the sync-lost discard gate never
                    // parses these bytes, it only has to keep consuming them
                    for (int i = 0; i < bufferLen; i++) {
                        Unsafe.putByte(buffer + i, (byte) 0x55);
                    }
                    return bufferLen;
                }
                return peerClosed ? -1 : 0; // peer FIN / would block
            }
            int n = Math.min(bufferLen, limit - pos);
            for (int i = 0; i < n; i++) {
                Unsafe.putByte(buffer + i, data[pos++]);
            }
            return n;
        }

        void closePeer() {
            peerClosed = true;
        }

        int floodReadsObserved() {
            return floodReadsObserved;
        }

        int pendingBytes() {
            return limit - pos;
        }

        void release(int bytes) {
            limit = Math.min(data.length, limit + bytes);
        }

        /**
         * Flips the facade into "flooding peer" mode once the scripted wire
         * bytes are exhausted: every recvRaw returns a FULL buffer of
         * unparseable garbage, modeling a peer that keeps the kernel receive
         * buffer non-empty faster than the server can drain it -- the
         * continuously readable socket of the review finding. Bounded by
         * {@code maxReads} so an unbounded server-side drain FAILS the
         * test's read-count assertions instead of hanging the suite. Each
         * flood read may advance the test clock by
         * {@code advancePerRead}, modeling wall-clock time passing while
         * the worker is stuck draining.
         */
        void startFlood(int maxReads, long[] clock, long advancePerRead) {
            floodReadsRemaining = maxReads;
            floodClock = clock;
            advanceClockPerFloodRead = advancePerRead;
        }
    }

    /**
     * Same shape as the contexts in
     * {@code QwpIngressUpgradeProcessorResumeRecvTest}: overrides the I/O
     * access points with the test doubles. {@code resumeResponseSend} is a
     * no-op because the blocked frame's bytes were already captured by
     * {@link BlockingRecordingRawSocket} at the original {@code send} call.
     */
    private static class TestableContext extends HttpConnectionContext {
        private final BlockingRecordingRawSocket rawSocket;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;

        TestableContext(
                HttpServerConfiguration config,
                PhasedNetworkFacade nf,
                BlockingRecordingRawSocket rawSocket,
                long recvBuffer,
                int recvBufferSize
        ) {
            super(config, (_, log) -> new PlainSocket(nf, log));
            this.rawSocket = rawSocket;
            this.testRecvBuffer = recvBuffer;
            this.testRecvBufferSize = recvBufferSize;
        }

        @Override
        public HttpRawSocket getRawResponseSocket() {
            return rawSocket;
        }

        @Override
        public long getRecvBuffer() {
            return testRecvBuffer;
        }

        @Override
        public int getRecvBufferSize() {
            return testRecvBufferSize;
        }

        @Override
        public void resumeResponseSend() {
            // parked bytes already recorded by BlockingRecordingRawSocket
        }
    }
}
