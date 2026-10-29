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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.http.HttpConnectionContext;
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
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Drives a {@link QwpIngressUpgradeProcessor} over in-memory sockets: builds client
 * frames and inspects the frames the server sends back.
 */
final class QwpIngressWireHarness {

    static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    static final int RECV_BUFFER_SIZE = 1024;
    static final int SEND_BUFFER_SIZE = 1024;

    static byte[] clientEncodedFrame(int rowsPerBlock, String... tables) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setDeferCommit(false);
            encoder.beginMessage(tables.length, new GlobalSymbolDictionary(), -1, -1);
            for (String table : tables) {
                try (QwpTableBuffer buffer = new QwpTableBuffer(table)) {
                    var value = buffer.getOrCreateColumn("v", QwpConstants.TYPE_LONG, false);
                    var timestamp = buffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    for (int i = 0; i < rowsPerBlock; i++) {
                        value.addLong(i + 1);
                        timestamp.addLong((i + 1L) * 1_000_000);
                        buffer.nextRow();
                    }
                    encoder.addTable(buffer);
                }
            }
            int length = encoder.finishMessage();
            byte[] payload = new byte[length];
            long address = encoder.getBuffer().getBufferPtr();
            for (int i = 0; i < length; i++) {
                payload[i] = Unsafe.getByte(address + i);
            }
            return createMaskedFrame(WebSocketOpcode.BINARY, payload);
        }
    }

    static byte[] concat(byte[]... arrays) {
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

    static byte[] createMaskedFrame(int opcode, byte[] payload) {
        return createMaskedFrame(opcode, payload, true);
    }

    static byte[] createMaskedFrame(int opcode, byte[] payload, boolean isFin) {
        // all test frames are tiny; single-byte payload length is sufficient
        assert payload.length <= 125;
        byte[] frame = new byte[2 + 4 + payload.length];
        int offset = 0;
        frame[offset++] = (byte) ((isFin ? 0x80 : 0) | (opcode & 0x0F));
        frame[offset++] = (byte) (0x80 | payload.length);
        System.arraycopy(DEFAULT_MASK_KEY, 0, frame, offset, 4);
        offset += 4;
        for (int i = 0; i < payload.length; i++) {
            frame[offset + i] = (byte) (payload[i] ^ DEFAULT_MASK_KEY[i % 4]);
        }
        return frame;
    }

    static byte[] deferred(byte[] message) {
        byte[] copy = message.clone();
        copy[5] |= QwpConstants.FLAG_DEFER_COMMIT;
        return copy;
    }

    static void drive(
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
    static LocalValue<QwpIngressProcessorState> getLV() throws Exception {
        Field lvField = QwpIngressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpIngressProcessorState>) lvField.get(null);
    }

    /**
     * True if any server-to-client BINARY frame is an error response
     * (status != OK) carrying the given sequence. Error frames share the
     * ACK's [status][seq LE] payload prefix.
     */
    static boolean hasErrorResponseForSeq(ObjList<byte[]> frames, long seq) {
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

    /**
     * Position of the first server-to-client BINARY frame carrying the given
     * status and sequence, or -1 when none was sent. Order matters where an ack
     * and an error leave in the same pass.
     */
    static int indexOfBinaryFrame(ObjList<byte[]> frames, byte status, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == status && readLeLong(f, 3) == seq) {
                return i;
            }
        }
        return -1;
    }

    static boolean isBinaryFrame(byte[] frame) {
        // server frames are unmasked; all frames in these tests have single-byte
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
    static long maxCumulativeOkAck(ObjList<byte[]> frames) {
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
    static byte[] oneRowMessage(long value, long tsMicros) {
        return oneRowMessage("tab", value, tsMicros);
    }

    static byte[] oneRowMessage(String table, long value, long tsMicros) {
        return qwpMessage(tableBlock(table, value, tsMicros));
    }

    /**
     * Like {@link #oneRowMessage(long, long)} but declares {@code v} as VARCHAR carrying
     * a non-numeric string, which the LONG column rejects as SCHEMA_MISMATCH.
     */
    static byte[] oneRowVarcharMessage(String value, long tsMicros) {
        return oneRowVarcharMessage("tab", value, tsMicros);
    }

    static byte[] oneRowVarcharMessage(String table, String value, long tsMicros) {
        return qwpMessage(varcharTableBlock(table, value, tsMicros));
    }

    /**
     * QWP v1 message without flags, carrying the given table blocks.
     */
    static byte[] qwpMessage(byte[]... blocks) {
        int payloadLength = 0;
        for (byte[] block : blocks) {
            payloadLength += block.length;
        }
        assert payloadLength < 256;
        byte[] message = new byte[QwpConstants.HEADER_SIZE + payloadLength];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = (byte) blocks.length; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payloadLength;
        int pos = QwpConstants.HEADER_SIZE;
        for (byte[] block : blocks) {
            System.arraycopy(block, 0, message, pos, block.length);
            pos += block.length;
        }
        return message;
    }

    static long readLeLong(byte[] buf, int offset) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (buf[offset + i] & 0xFFL);
        }
        return v;
    }

    /// A frame carrying no table blocks at all, with FLAG_DEFER_COMMIT. The
    /// client emits exactly this shape for its symbol-dictionary chunks and its
    /// reconnect catch-up, and the data frames after them reference the ids
    /// those frames register.
    static byte[] rowlessDeferredMessage() {
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

    /**
     * Table block with schema [{@code v} LONG, {@code ""} TIMESTAMP (designated)];
     * {@code pairs} holds (v, ts) for each row.
     */
    static byte[] tableBlock(String table, long... pairs) {
        final byte[] name = table.getBytes(StandardCharsets.UTF_8);
        final int rows = pairs.length / 2;
        assert name.length < 128 && rows < 128;
        byte[] block = new byte[1 + name.length + 8 + 8 * rows + 1 + 8 * rows];
        int i = 0;
        // table header
        block[i++] = (byte) name.length; // table name length (varint)
        System.arraycopy(name, 0, block, i, name.length);
        i += name.length;
        block[i++] = (byte) rows; // rowCount (varint)
        block[i++] = 2; // columnCount (varint)
        // schema
        block[i++] = 1; // column name length (varint)
        block[i++] = 'v';
        block[i++] = QwpConstants.TYPE_LONG;
        block[i++] = 0; // empty name = designated timestamp
        block[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v
        block[i++] = 0; // no null bitmap
        for (int r = 0; r < rows; r++) {
            i = writeLeLong(block, i, pairs[2 * r]);
        }
        // column data: designated timestamp
        block[i++] = 0; // no null bitmap
        for (int r = 0; r < rows; r++) {
            i = writeLeLong(block, i, pairs[2 * r + 1]);
        }
        assert i == block.length;
        return block;
    }

    /**
     * One-row table block like {@link #tableBlock} but with {@code v} declared as VARCHAR.
     */
    static byte[] varcharTableBlock(String table, String value, long tsMicros) {
        final byte[] name = table.getBytes(StandardCharsets.UTF_8);
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        assert name.length < 128;
        byte[] block = new byte[1 + name.length + 8 + 8 + utf8.length + 1 + 8];
        int i = 0;
        block[i++] = (byte) name.length;
        System.arraycopy(name, 0, block, i, name.length);
        i += name.length;
        block[i++] = 1; // rowCount
        block[i++] = 2; // columnCount
        block[i++] = 1; // column name length
        block[i++] = 'v';
        block[i++] = QwpConstants.TYPE_VARCHAR;
        block[i++] = 0; // empty name = designated timestamp
        block[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v -- [no-null-bitmap flag][offset array][utf8 bytes]
        block[i++] = 0;
        i = writeLeInt(block, i, 0);
        i = writeLeInt(block, i, utf8.length);
        System.arraycopy(utf8, 0, block, i, utf8.length);
        i += utf8.length;
        // column data: designated timestamp
        block[i++] = 0;
        i = writeLeLong(block, i, tsMicros);
        assert i == block.length;
        return block;
    }

    static int writeLeInt(byte[] buf, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    static int writeLeLong(byte[] buf, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    /**
     * Network facade that releases the client's wire bytes in explicit phases
     * so the engine's read-only flag can be flipped between frames — the
     * demote/re-promote race distilled to its deterministic core.
     */
    static class PhasedNetworkFacade extends NetworkFacadeImpl {
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
    static class RecordingRawSocket implements HttpRawSocket {
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
    static class TestableContext extends HttpConnectionContext {
        private final HttpRawSocket rawSocket;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;

        TestableContext(
                HttpServerConfiguration config,
                PhasedNetworkFacade nf,
                HttpRawSocket rawSocket,
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
