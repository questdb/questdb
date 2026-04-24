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

package io.questdb.cutlass.qwp.websocket;

import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Zero-allocation WebSocket frame writer.
 * Writes WebSocket frames according to RFC 6455.
 *
 * <p>All methods are static utilities that write directly to memory buffers.
 *
 * <p>Thread safety: This class is thread-safe as it contains no mutable state.
 */
public final class WebSocketFrameWriter {
    private static final byte[] EMPTY_BYTES = new byte[0];
    // Frame header bits
    private static final int FIN_BIT = 0x80;
    private static final int MASK_BIT = 0x80;
    // RFC 6455 Section 5.5: control frame payload <= 125 bytes;
    // close payload starts with a 2-byte status code, leaving 123 for the reason.
    private static final int MAX_CLOSE_REASON_BYTES = 123;

    private WebSocketFrameWriter() {
        // Static utility class
    }

    /**
     * Calculates the header size for a given payload length and masking.
     *
     * @param payloadLength the payload length
     * @param masked        true if the payload will be masked
     * @return the header size in bytes
     */
    public static int headerSize(long payloadLength, boolean masked) {
        int size;
        if (payloadLength <= 125) {
            size = 2;
        } else if (payloadLength <= 65535) {
            size = 4;
        } else {
            size = 10;
        }
        return masked ? size + 4 : size;
    }

    /**
     * Writes a binary frame header only (for when payload is written separately).
     *
     * @param buf        the buffer to write to
     * @param payloadLen length of payload that will follow
     * @return the header size in bytes
     */
    public static int writeBinaryFrameHeader(long buf, int payloadLen) {
        return writeHeader(buf, true, WebSocketOpcode.BINARY, payloadLen, false);
    }

    /**
     * Writes a complete Close frame to the buffer if it fits.
     *
     * @param buf        the buffer to write to
     * @param bufferSize the buffer capacity in bytes
     * @param code       the close status code
     * @param reason     the close reason (may be null)
     * @return the total number of bytes written, or -1 if the buffer is too small
     */
    public static int writeCloseFrame(long buf, int bufferSize, int code, String reason) {
        byte[] reasonBytes = encodeReason(reason);
        int payloadLen = 2 + reasonBytes.length;

        int frameSize = headerSize(payloadLen, false) + payloadLen;
        if (frameSize > bufferSize) {
            return -1;
        }

        int headerLen = writeHeader(buf, true, WebSocketOpcode.CLOSE, payloadLen, false);
        int payloadOffset = writeClosePayload(buf + headerLen, code, reasonBytes);

        return headerLen + payloadOffset;
    }

    /**
     * Writes a WebSocket frame header to the buffer.
     *
     * @param buf           the buffer to write to
     * @param fin           true if this is the final frame
     * @param opcode        the frame opcode
     * @param payloadLength the payload length
     * @param masked        true if the payload should be masked
     * @return the number of bytes written (header size)
     */
    public static int writeHeader(long buf, boolean fin, int opcode, long payloadLength, boolean masked) {
        int offset = 0;

        // First byte: FIN + opcode
        int byte0 = (fin ? FIN_BIT : 0) | (opcode & 0x0F);
        Unsafe.getUnsafe().putByte(buf + offset++, (byte) byte0);

        // Second byte: MASK + payload length
        int maskBit = masked ? MASK_BIT : 0;

        if (payloadLength <= 125) {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | payloadLength));
        } else if (payloadLength <= 65535) {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | 126));
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) ((payloadLength >> 8) & 0xFF));
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (payloadLength & 0xFF));
        } else {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | 127));
            Unsafe.getUnsafe().putLong(buf + offset, Long.reverseBytes(payloadLength));
            offset += 8;
        }

        return offset;
    }

    /**
     * Writes a complete Ping frame to the buffer.
     *
     * @param buf        the buffer to write to
     * @param payload    the ping payload
     * @param payloadOff offset into payload array
     * @param payloadLen length of payload to write
     * @return the total number of bytes written
     */
    public static int writePingFrame(long buf, byte[] payload, int payloadOff, int payloadLen) {
        int headerLen = writeHeader(buf, true, WebSocketOpcode.PING, payloadLen, false);

        // Copy payload
        for (int i = 0; i < payloadLen; i++) {
            Unsafe.getUnsafe().putByte(buf + headerLen + i, payload[payloadOff + i]);
        }

        return headerLen + payloadLen;
    }

    /**
     * Writes a Pong frame with payload from a memory address.
     *
     * @param buf        the buffer to write to
     * @param payloadPtr pointer to the ping payload to echo
     * @param payloadLen length of payload
     * @return the total number of bytes written
     */
    public static int writePongFrame(long buf, long payloadPtr, int payloadLen) {
        int headerLen = writeHeader(buf, true, WebSocketOpcode.PONG, payloadLen, false);

        // Copy payload from memory
        Unsafe.getUnsafe().copyMemory(payloadPtr, buf + headerLen, payloadLen);

        return headerLen + payloadLen;
    }

    private static byte[] encodeReason(String reason) {
        if (reason == null || reason.isEmpty()) {
            return EMPTY_BYTES;
        }
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_CLOSE_REASON_BYTES) {
            return bytes;
        }
        // Truncate without splitting a multi-byte UTF-8 sequence.
        int len = MAX_CLOSE_REASON_BYTES;
        while (len > 0 && (bytes[len] & 0xC0) == 0x80) {
            len--;
        }
        byte[] truncated = new byte[len];
        System.arraycopy(bytes, 0, truncated, 0, len);
        return truncated;
    }

    private static int writeClosePayload(long buf, int code, byte[] reasonBytes) {
        Unsafe.getUnsafe().putShort(buf, Short.reverseBytes((short) code));
        if (reasonBytes.length > 0) {
            Unsafe.getUnsafe().copyMemory(reasonBytes, Unsafe.BYTE_OFFSET, null, buf + 2, reasonBytes.length);
        }
        return 2 + reasonBytes.length;
    }
}
