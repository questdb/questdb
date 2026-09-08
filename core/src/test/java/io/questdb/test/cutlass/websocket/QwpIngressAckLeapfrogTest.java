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
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocket;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression test for the cumulative-ACK leapfrog over a silently gate-rejected
 * frame during a deferred role-change close (in-place PRIMARY-to-REPLICA demote
 * that re-promotes within the durable-upload grace window).
 * <p>
 * Failure chain that was possible before the deferral gate in
 * {@code handleBinaryMessage} ({@link QwpIngressUpgradeProcessor}):
 * <ol>
 *   <li>Durable-ack connection commits frame seq=0 (durable upload lags, so
 *       the connection has un-covered durable work).</li>
 *   <li>Demote: frame seq=1 is rejected by the {@code engine.isReadOnlyMode()}
 *       gate. {@code roleChangeCloseWithUploadGrace} arms the bounded deferral
 *       and returns — seq=1 gets NO error response, yet its sequence number is
 *       consumed. The {@code finally}-block {@code state.clear()} re-arms the
 *       connection ({@code currentStatus=OK}, {@code roleChangeClosePending=false}).</li>
 *   <li>Re-promote within the grace window: frame seq=2 passes the live gate,
 *       commits, and {@code setHighestProcessedSequence(2)} is recorded.</li>
 *   <li>The client's durable-ack keepalive PING flushes a cumulative OK-ACK
 *       carrying seq=2.</li>
 * </ol>
 * The OK-ACK contract is cumulative — "the server has confirmed every FSN up
 * to and including this value" ({@code SegmentRing.acknowledge} in the Java
 * client) — so an ACK of seq=2 makes a store-and-forward client trim the
 * segment slot holding frame seq=1, whose rows the server refused, never
 * wrote, and never reported. Silent data loss.
 * <p>
 * Invariant asserted (fix-agnostic): a cumulative OK-ACK must not cover a
 * sequence that was neither committed nor answered with an error response.
 * The test stays green whether the fix NACKs the rejected frame, caps the
 * cumulative ACK below it, or refuses data frames while a role-change close
 * deferral is armed. The shipped fix does the latter (primary gate) plus the
 * ack-watermark clamp in {@code QwpIngressProcessorState} (last-resort
 * containment, unit-covered in {@code QwpIngressProcessorStateTest}).
 */
public class QwpIngressAckLeapfrogTest extends AbstractCairoTest {
    private static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    private static final int RECV_BUFFER_SIZE = 1024;
    private static final int SEND_BUFFER_SIZE = 1024;

    @Test
    public void testCumulativeAckMustNotLeapfrogGateRejectedFrame() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            // The demotable engine below shares the root with the static test
            // engine, which holds the table-registry lock. Pre-create the WAL
            // table through the lock holder so the QWP path only needs to
            // acquire a WAL writer, not register a new table.
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private final DurableAckRegistry laggingRegistry = new DurableAckRegistry() {
                    @Override
                    public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                        // uploads never catch up: the deferral stays armed for
                        // the whole grace window
                        return -1L;
                    }

                    @Override
                    public boolean isEnabled() {
                        return true;
                    }
                };

                @Override
                public @NotNull DurableAckRegistry getDurableAckRegistry() {
                    return laggingRegistry;
                }

                @Override
                public boolean isReadOnlyMode() {
                    return readOnly.get();
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] wire = concat(frame0, frame1, frame2, ping);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            demotableEngine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    // durable-ack opt-in, as negotiated via X-QWP-Request-Durable-Ack
                    state.setDurableAckEnabled(true);
                    getLV().set(context, state);

                    // Phase A: PRIMARY. seq=0 commits; durable upload lags behind.
                    drive(processor, context, nf, frame0.length);

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage;
                    // no error response goes out for seq=1.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: re-promote within the grace window. seq=2 passes
                    // the live read-only gate and commits.
                    readOnly.set(false);
                    drive(processor, context, nf, frame2.length);

                    // Phase D: durable-ack keepalive PING — documented flush
                    // point for pending cumulative ACKs.
                    drive(processor, context, nf, ping.length);

                    // The rejected frame's row must not have been written
                    // (the refusal itself is legitimate; acking it is the bug).
                    drainWalQueue(demotableEngine);
                    long ingestedRows;
                    try (TableReader reader = demotableEngine.getReader("tab")) {
                        ingestedRows = reader.size();
                    }
                    Assert.assertTrue("gate-rejected frame must not be committed", ingestedRows <= 2);

                    // INVARIANT: a cumulative OK-ACK confirms every sequence up
                    // to and including its value, so it must never cover a
                    // sequence that was neither committed nor refused with an
                    // error response.
                    long maxOkAck = maxCumulativeOkAck(rawSocket.sentFrames);
                    boolean seq1Refused = hasErrorResponseForSeq(rawSocket.sentFrames, 1);
                    if (!seq1Refused) {
                        Assert.assertTrue(
                                "SILENT DATA LOSS: cumulative OK-ACK confirms sequences 0.." + maxOkAck
                                        + ", but seq=1 was gate-rejected during the demote, never committed ("
                                        + ingestedRows + " rows in table for " + (maxOkAck + 1)
                                        + " acked frames) and never refused with an error response;"
                                        + " a durable-ack store-and-forward client trims its replay slot"
                                        + " for seq=1 on this ACK and the rows are lost",
                                maxOkAck < 1
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
     * A per-message error must break the ordered pipeline: a valid frame the
     * client already pipelined behind the failing one must be refused, never
     * committed, and the cumulative OK-ACK must not leapfrog the gap.
     */
    @Test
    public void testErrorRefusesPipelinedTailOnSameConnection() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // seq=0 valid, seq=1 schema mismatch (VARCHAR "x" into LONG v),
            // seq=2 valid but pipelined behind the gap.
            ObjList<byte[]> sent = ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.PING, new byte[0])
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("only seq=0 must commit; the pipelined tail after the error is refused", 1, reader.size());
            }
            Assert.assertTrue("seq=1 must be refused with an error response", hasErrorResponseForSeq(sent, 1));
            long maxOkAck = maxCumulativeOkAck(sent);
            Assert.assertTrue("cumulative OK-ACK must not leapfrog the errored seq=1 (was " + maxOkAck + ")", maxOkAck < 1);
        });
    }

    /**
     * A frame refused because a prior error broke the pipeline is neither
     * committed nor acked, so a reconnecting client replays it from its acked
     * watermark and it lands exactly once -- no loss, no duplicate.
     */
    @Test
    public void testReconnectReplayLandsRefusedTailExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Connection 1: A commits, B (schema mismatch) errors, C is refused.
            ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
            );
            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("first connection commits only A", 1, reader.size());
            }

            // Reconnect: the client replays its unacked tail from ackedFsn+1 --
            // the corrected B and the still-valid C.
            ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
            );
            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                // A + B + C, C exactly once (never committed on connection 1).
                Assert.assertEquals("replayed tail must land exactly once, no duplicate", 3, reader.size());
            }
        });
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
     * A cumulative OK ack must not cover a rowless deferred frame, even once a
     * LATER deferred frame's own rows have become durable.
     * <p>
     * This drives the real {@code handleBinaryMessage} and reads the acks off
     * the wire, so it covers the processor's wiring -- the state-level test can
     * only call the decision by hand. Deleting the processor's
     * {@code withholdDeferredFrame} call leaves every state-level test green.
     */
    @Test
    public void testCumulativeAckMustNotCoverRowlessDeferredFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Cap 1 so the row-bearing deferred frame's rows are force-committed
            // during the append, making that frame ack-coverable on its own
            // merits. Without that the clamp would hold for the ordinary reason
            // and the test would say nothing about the rowless one.
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    final LineHttpProcessorConfiguration delegate = super.getLineHttpProcessorConfiguration();
                    return new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getQwpMaxUncommittedRows() {
                            return 1;
                        }
                    };
                }
            };

            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine engine2 = new CairoEngine(new DefaultTestCairoConfiguration(root))) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine2, httpConfig);

                byte[] chunk = createMaskedFrame(WebSocketOpcode.BINARY, rowlessDeferredMessage());
                byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(100L, 1_000_000L)));
                byte[] commit = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
                byte[] wire = concat(chunk, data, commit);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            engine2,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    getLV().set(context, state);

                    // seq=0: rowless deferred -- withheld, and its sequence must
                    // stay uncovered until the group commits.
                    drive(processor, context, nf, chunk.length);
                    Assert.assertEquals("a rowless deferred frame must not be acked",
                            -1, maxCumulativeOkAck(rawSocket.sentFrames));

                    // seq=1: deferred, but the cap force-commits its row, so it
                    // is ack-coverable on its own. Covering it would also cover
                    // seq=0, which the client needs retained.
                    drive(processor, context, nf, data.length);
                    Assert.assertEquals(
                            "a cumulative ack must not reach the rowless frame at seq=0, even though"
                                    + " seq=1's own rows are durable -- the client trims on the cumulative"
                                    + " ack and its data frames reference what seq=0 registered",
                            -1,
                            maxCumulativeOkAck(rawSocket.sentFrames)
                    );

                    // seq=2: the group-closing commit covers everything.
                    drive(processor, context, nf, commit.length);
                    Assert.assertEquals("the group-closing commit must cover the whole group",
                            2, maxCumulativeOkAck(rawSocket.sentFrames));
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDurableAckPollAdvancesAckOnCleanConnection() throws Exception {
        // Counterpart to the deferred-group case below. With nothing deferred,
        // the poll consumes its own message sequence and the cumulative OK ack
        // must name it. Without that advance a client's acked watermark falls
        // one behind the sequence it issued for every keepalive poll it sends,
        // and its store-and-forward records never retire.
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage());
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(first, poll, second);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                getLV().set(context, state);

                drive(processor, context, nf, first.length);
                Assert.assertEquals("the first data frame must be acknowledged", 0,
                        maxCumulativeOkAck(rawSocket.sentFrames));

                drive(processor, context, nf, poll.length);
                Assert.assertEquals(
                        "a poll on a clean connection must advance the cumulative ack over its own sequence",
                        1,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );

                drive(processor, context, nf, second.length);
                Assert.assertEquals("the trailing data frame must be acknowledged", 2,
                        maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("the poll must neither add nor drop rows", 2, reader.size());
            }
        });
    }

    /**
     * The poll exists so a browser that cannot send WebSocket PINGs can still
     * pull durable progress. The registry is armed only after the data frame's
     * own flush has gone out, so the poll is the sole flush point able to carry
     * the new watermark.
     */
    @Test
    public void testDurableAckPollFlushesDurableProgress() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine durableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private final DurableAckRegistry uploadingRegistry = new DurableAckRegistry() {
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
                    return uploadingRegistry;
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(durableEngine, httpConfig);

                byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
                byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage());
                byte[] wire = concat(data, poll);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            durableEngine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    state.setDurableAckEnabled(true);
                    getLV().set(context, state);

                    // Uploads still lag: the commit is acknowledged, but no
                    // durable watermark exists to report.
                    drive(processor, context, nf, data.length);
                    Assert.assertEquals("the data frame must be acknowledged", 0,
                            maxCumulativeOkAck(rawSocket.sentFrames));
                    Assert.assertFalse(
                            "nothing is durably uploaded yet, so no durable ACK may be sent",
                            hasDurableAckFrame(rawSocket.sentFrames)
                    );

                    // The upload completes between the two frames.
                    durableWatermark.set(1L);
                    drive(processor, context, nf, poll.length);
                    Assert.assertTrue(
                            "a durable ACK poll must flush the durable watermark that landed since the last frame",
                            hasDurableAckFrame(rawSocket.sentFrames)
                    );
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }

                drainWalQueue(durableEngine);
                try (TableReader reader = durableEngine.getReader("tab")) {
                    Assert.assertEquals("the poll must neither add nor drop rows", 1, reader.size());
                }
            }
        });
    }

    @Test
    public void testDurableAckPollMustNotCommitDeferredGroup() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] deferred = createMaskedFrame(
                    WebSocketOpcode.BINARY,
                    deferred(oneRowMessage(100L, 1_000_000L))
            );
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage());
            byte[] commit = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(deferred, poll, commit);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                // The observable ack is identical whether the poll arm's
                // hasUncommittedDeferredRows() guard withholds the advance or
                // the last-resort clamp inside setHighestProcessedSequence
                // refuses it: both leave the watermark untouched, and the clamp
                // only differs by a LOG.critical() line. Count the calls so the
                // guard itself is what this test holds -- the clamp is
                // documented as containment for a regression of exactly this
                // path, so a green test that leans on it proves nothing.
                AtomicLong watermarkAdvanceAttempts = new AtomicLong();
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                ) {
                    @Override
                    public void setHighestProcessedSequence(long highestProcessedSequence) {
                        watermarkAdvanceAttempts.incrementAndGet();
                        super.setHighestProcessedSequence(highestProcessedSequence);
                    }
                };
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                getLV().set(context, state);

                drive(processor, context, nf, deferred.length);
                Assert.assertEquals("deferred rows must remain unacknowledged", -1, maxCumulativeOkAck(rawSocket.sentFrames));

                long attemptsBeforePoll = watermarkAdvanceAttempts.get();
                drive(processor, context, nf, poll.length);
                Assert.assertEquals(
                        "a durable ACK poll must not even ask the watermark to advance while a deferred group is open",
                        attemptsBeforePoll,
                        watermarkAdvanceAttempts.get()
                );
                Assert.assertEquals(
                        "a durable ACK poll must not commit or acknowledge an in-progress deferred group",
                        -1,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );

                drive(processor, context, nf, commit.length);
                Assert.assertEquals("the real commit frame must cover the deferred group and poll", 2,
                        maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("both real data frames must commit exactly once", 2, reader.size());
            }
        });
    }

    @Test
    public void testDurableAckPollRequiresNegotiation() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            ObjList<byte[]> sent = ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage())
            );

            Assert.assertTrue(
                    "an unnegotiated durable ACK poll must receive STATUS_PARSE_ERROR",
                    hasResponseForSeqAndStatus(sent, 0, QwpConstants.STATUS_PARSE_ERROR)
            );
        });
    }

    /**
     * The reject arm must also clamp the pipelined tail. A refused poll is
     * consumed without an ack of its own, so {@code markSequenceUnresolved}
     * has to run before the flush: without it a frame pipelined behind the
     * refused poll commits and the cumulative OK ack jumps over the refused
     * sequence. A store-and-forward sender that treats STATUS_PARSE_ERROR as
     * terminal tears down before reading that ack, replays from its old
     * watermark, and duplicates the tail frame's rows.
     */
    @Test
    public void testDurableAckPollRejectClampsPipelinedTail() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage());
            byte[] tail = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(data, poll, tail);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                // Durable ack deliberately left off: this is the reject arm.
                getLV().set(context, state);

                // All three frames in one pass, so the tail is already buffered
                // when the poll at seq=1 is refused.
                drive(processor, context, nf, wire.length);

                Assert.assertTrue(
                        "the refused poll must receive STATUS_PARSE_ERROR",
                        hasResponseForSeqAndStatus(rawSocket.sentFrames, 1, QwpConstants.STATUS_PARSE_ERROR)
                );
                Assert.assertEquals(
                        "the cumulative ack must stop at the frame before the refused poll",
                        0,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(
                        "the frame pipelined behind the refused poll must not commit",
                        1,
                        reader.size()
                );
            }
        });
    }

    /**
     * The reject arm must flush the pending cumulative ack before the error,
     * exactly like the error arm at the tail of {@code handleBinaryMessage}.
     * Both frames go out either way, but a store-and-forward sender that treats
     * the error as terminal tears the connection down on reading it; anything
     * still behind the error on the wire is never read, so those frames stay in
     * its replay queue and reconnect duplicates their rows.
     */
    @Test
    public void testDurableAckPollRejectFlushesPendingAckFirst() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, durableAckPollMessage());
            byte[] wire = concat(data, poll);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                // Durable ack deliberately left off: this is the reject arm.
                getLV().set(context, state);

                // Both frames in one pass, so the data frame's ack is still
                // pending (ACK_BATCH_SIZE is 8) when the poll is refused.
                drive(processor, context, nf, wire.length);

                int ackIndex = indexOfBinaryFrame(rawSocket.sentFrames, QwpConstants.STATUS_OK, 0);
                int errorIndex = indexOfBinaryFrame(rawSocket.sentFrames, QwpConstants.STATUS_PARSE_ERROR, 1);
                Assert.assertTrue("the committed data frame must be acknowledged", ackIndex >= 0);
                Assert.assertTrue("the refused poll must receive STATUS_PARSE_ERROR", errorIndex >= 0);
                Assert.assertTrue(
                        "the cumulative ack must precede the error on the wire, got ack at "
                                + ackIndex + " and error at " + errorIndex,
                        ackIndex < errorIndex
                );
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("the refused poll must neither add nor drop rows", 1, reader.size());
            }
        });
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
     * True if any server-to-client BINARY frame is a STATUS_DURABLE_ACK frame.
     * Durable acks carry per-table watermarks rather than a sequence, so there
     * is nothing to match on beyond the status byte.
     */
    private static boolean hasDurableAckFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == QwpConstants.STATUS_DURABLE_ACK) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if any server-to-client BINARY frame is an error response
     * (status != OK) carrying the given sequence. Error frames share the
     * ACK's [status][seq LE] payload prefix.
     */
    private static boolean hasErrorResponseForSeq(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f)) {
                continue;
            }
            if (f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == seq) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasResponseForSeqAndStatus(ObjList<byte[]> frames, long seq, byte status) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == status && readLeLong(f, 3) == seq) {
                return true;
            }
        }
        return false;
    }

    /**
     * Position of the first server-to-client BINARY frame carrying the given
     * status and sequence, or -1 when none was sent. Order matters where an ack
     * and an error leave in the same pass.
     */
    private static int indexOfBinaryFrame(ObjList<byte[]> frames, byte status, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == status && readLeLong(f, 3) == seq) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isBinaryFrame(byte[] frame) {
        // server frames are unmasked; all frames in this test have single-byte
        // payload lengths, so the payload starts at offset 2
        return frame.length >= 11
                && (frame[0] & 0x0F) == WebSocketOpcode.BINARY
                && (frame[1] & 0x80) == 0
                && (frame[1] & 0x7F) < 126;
    }

    /**
     * Highest sequence carried by any cumulative OK-ACK frame sent to the
     * client, or -1 when none was sent.
     */
    private static long maxCumulativeOkAck(ObjList<byte[]> frames) {
        long max = -1;
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f)) {
                continue;
            }
            if (f[2] == QwpConstants.STATUS_OK) {
                max = Math.max(max, readLeLong(f, 3));
            }
        }
        return max;
    }

    /**
     * QWP v1 message: table "tab", one row, schema
     * [{@code v} LONG, {@code ""} TIMESTAMP (designated)].
     */
    /// A frame carrying no table blocks at all, with FLAG_DEFER_COMMIT. The
    /// client emits exactly this shape for its symbol-dictionary chunks and its
    /// reconnect catch-up, and the data frames after them reference the ids
    /// those frames register.
    private static byte[] rowlessDeferredMessage() {
        byte[] payload = new byte[2];
        payload[0] = 0; // delta_start (varint)
        payload[1] = 0; // new_symbols count (varint)

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = QwpConstants.FLAG_DEFER_COMMIT;
        message[6] = 0; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payload.length;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    private static byte[] deferred(byte[] message) {
        byte[] copy = message.clone();
        copy[5] |= QwpConstants.FLAG_DEFER_COMMIT;
        return copy;
    }

    private static byte[] durableAckPollMessage() {
        byte[] message = new byte[QwpConstants.HEADER_SIZE];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = QwpConstants.FLAG_DURABLE_ACK_POLL;
        return message;
    }

    private static byte[] oneRowMessage(long value, long tsMicros) {
        byte[] payload = new byte[29];
        int i = 0;
        // table header
        payload[i++] = 3; // table name length (varint)
        payload[i++] = 't';
        payload[i++] = 'a';
        payload[i++] = 'b';
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
     * Like {@link #oneRowMessage} but declares {@code v} as VARCHAR carrying a
     * non-numeric string, which the LONG column rejects as SCHEMA_MISMATCH.
     */
    private static byte[] oneRowVarcharMessage(String value, long tsMicros) {
        byte[] utf8 = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] payload = new byte[11 + 1 + 8 + utf8.length + 1 + 8];
        int i = 0;
        payload[i++] = 3;
        payload[i++] = 't';
        payload[i++] = 'a';
        payload[i++] = 'b';
        payload[i++] = 1; // rowCount
        payload[i++] = 2; // columnCount
        payload[i++] = 1; // column name length
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_VARCHAR;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v -- [no-null-bitmap flag][offset array][utf8 bytes]
        payload[i++] = 0;
        i = writeLeInt(payload, i, 0);
        i = writeLeInt(payload, i, utf8.length);
        System.arraycopy(utf8, 0, payload, i, utf8.length);
        i += utf8.length;
        // column data: designated timestamp
        payload[i++] = 0;
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0;
        message[6] = 1;
        message[7] = 0;
        message[8] = (byte) payload.length;
        message[9] = 0;
        message[10] = 0;
        message[11] = 0;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    private static long readLeLong(byte[] buf, int offset) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (buf[offset + i] & 0xFFL);
        }
        return v;
    }

    private static int writeLeInt(byte[] buf, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    private static int writeLeLong(byte[] buf, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    private ObjList<byte[]> ingestOnFreshConnection(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            getLV().set(context, state);
            for (byte[] frame : frames) {
                drive(processor, context, nf, frame.length);
            }
        } finally {
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return rawSocket.sentFrames;
    }

    /**
     * Network facade that releases the client's wire bytes in explicit phases
     * so the engine's read-only flag can be flipped between frames — the
     * demote/re-promote race distilled to its deterministic core.
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
     * Captures every frame the server sends, in order.
     */
    private static class RecordingRawSocket implements HttpRawSocket {
        final ObjList<byte[]> sentFrames = new ObjList<>();
        private final long bufferAddress;
        private final int bufferSize;

        RecordingRawSocket(long bufferAddress, int bufferSize) {
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
        public void send(int size) {
            byte[] copy = new byte[size];
            for (int i = 0; i < size; i++) {
                copy[i] = Unsafe.getByte(bufferAddress + i);
            }
            sentFrames.add(copy);
        }
    }

    /**
     * Same shape as the contexts in
     * {@code QwpIngressUpgradeProcessorResumeRecvTest}: overrides the I/O
     * access points with the test doubles.
     */
    private static class TestableContext extends HttpConnectionContext {
        private final RecordingRawSocket rawSocket;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;

        TestableContext(
                HttpServerConfiguration config,
                PhasedNetworkFacade nf,
                RecordingRawSocket rawSocket,
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
    }
}
