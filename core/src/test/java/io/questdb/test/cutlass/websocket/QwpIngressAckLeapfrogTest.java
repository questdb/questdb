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
