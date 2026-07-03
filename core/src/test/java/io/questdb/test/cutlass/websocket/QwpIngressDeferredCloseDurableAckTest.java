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
                byte[] wire = concat(frame0, frame1, ping);

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
                    // then the deferred CLOSE goes out and the framework
                    // tears the connection down.
                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException after deferred CLOSE flush");
                    } catch (ServerDisconnectException expected) {
                    }

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
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
                byte[] wire = concat(frame0, ping1, frame1, ping2);

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
                    // dispatcher fires resumeSend to finish the close.
                    try {
                        processor.resumeSend(context);
                        Assert.fail("Expected ServerDisconnectException after deferred CLOSE flush");
                    } catch (ServerDisconnectException expected) {
                    }

                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);
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
                byte[] wire = concat(frame0, frame1, frame2);

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
                        nf.release(frame2.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (grace-expired clean close)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: grace-expired clean close done");
                    } finally {
                        capture.stop();
                    }

                    // Behavioural lock (green before and after the fix): the
                    // close is clean -- final durable ack precedes the
                    // reconnect-eligible CLOSE, replay window empty.
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);

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
                byte[] wire = concat(frame0, frame1, frame2);

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
                        nf.release(frame2.length);
                        try {
                            processor.resumeRecv(context);
                            Assert.fail("Expected ServerDisconnectException (coverage-complete close via data-frame re-entry)");
                        } catch (ServerDisconnectException expected) {
                        }
                        drainLogQueue(capture, "sentinel: within-grace data-frame close done");
                    } finally {
                        capture.stop();
                    }

                    // The exactly-once handshake holds on this exit: the
                    // final durable ack precedes the reconnect-eligible CLOSE.
                    assertFinalDurableAckPrecedesClose(rawSocket.sentFrames, 1000 /* NORMAL_CLOSURE */);

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
        private int limit;
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
                return 0; // would block
            }
            int n = Math.min(bufferLen, limit - pos);
            for (int i = 0; i < n; i++) {
                Unsafe.putByte(buffer + i, data[pos++]);
            }
            return n;
        }

        void release(int bytes) {
            limit = Math.min(data.length, limit + bytes);
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
