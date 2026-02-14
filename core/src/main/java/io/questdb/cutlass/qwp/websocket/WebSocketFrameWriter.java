/*******************************************************************************
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
    // Frame header bits
    private static final int FIN_BIT = 0x80;
    private static final int MASK_BIT = 0x80;

    private WebSocketFrameWriter() {
        // Static utility class
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
     * Writes a WebSocket frame header with optional mask key.
     *
     * @param buf           the buffer to write to
     * @param fin           true if this is the final frame
     * @param opcode        the frame opcode
     * @param payloadLength the payload length
     * @param maskKey       the mask key (only used if masked is true)
     * @return the number of bytes written (header size including mask key)
     */
    public static int writeHeader(long buf, boolean fin, int opcode, long payloadLength, int maskKey) {
        int offset = writeHeader(buf, fin, opcode, payloadLength, true);
        Unsafe.getUnsafe().putInt(buf + offset, maskKey);
        return offset + 4;
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
     * Writes the payload for a Close frame.
     *
     * @param buf    the buffer to write to (after the header)
     * @param code   the close status code
     * @param reason the close reason (may be null)
     * @return the number of bytes written
     */
    public static int writeClosePayload(long buf, int code, String reason) {
        // Write status code in network byte order (big-endian)
        Unsafe.getUnsafe().putShort(buf, Short.reverseBytes((short) code));
        int offset = 2;

        // Write reason if provided
        if (reason != null && !reason.isEmpty()) {
            byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
            for (byte reasonByte : reasonBytes) {
                Unsafe.getUnsafe().putByte(buf + offset++, reasonByte);
            }
        }

        return offset;
    }

    /**
     * Writes a complete Close frame to the buffer.
     *
     * @param buf    the buffer to write to
     * @param code   the close status code
     * @param reason the close reason (may be null)
     * @return the total number of bytes written (header + payload)
     */
    public static int writeCloseFrame(long buf, int code, String reason) {
        int payloadLen = 2; // status code
        if (reason != null && !reason.isEmpty()) {
            payloadLen += reason.getBytes(StandardCharsets.UTF_8).length;
        }

        int headerLen = writeHeader(buf, true, WebSocketOpcode.CLOSE, payloadLen, false);
        int payloadOffset = writeClosePayload(buf + headerLen, code, reason);

        return headerLen + payloadOffset;
    }

    /**
     * Writes a complete Ping frame to the buffer.
     *
     * @param buf         the buffer to write to
     * @param payload     the ping payload
     * @param payloadOff  offset into payload array
     * @param payloadLen  length of payload to write
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
     * Writes a complete Pong frame to the buffer.
     *
     * @param buf         the buffer to write to
     * @param payload     the pong payload (should match the received ping)
     * @param payloadOff  offset into payload array
     * @param payloadLen  length of payload to write
     * @return the total number of bytes written
     */
    public static int writePongFrame(long buf, byte[] payload, int payloadOff, int payloadLen) {
        int headerLen = writeHeader(buf, true, WebSocketOpcode.PONG, payloadLen, false);

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

    /**
     * Writes a binary frame with payload from a memory address.
     *
     * @param buf        the buffer to write to
     * @param payloadPtr pointer to the payload data
     * @param payloadLen length of payload
     * @return the total number of bytes written
     */
    public static int writeBinaryFrame(long buf, long payloadPtr, int payloadLen) {
        int headerLen = writeHeader(buf, true, WebSocketOpcode.BINARY, payloadLen, false);

        // Copy payload from memory
        Unsafe.getUnsafe().copyMemory(payloadPtr, buf + headerLen, payloadLen);

        return headerLen + payloadLen;
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
     * Masks payload data in place using XOR with the given mask key.
     *
     * @param buf     the payload buffer
     * @param len     the payload length
     * @param maskKey the 4-byte mask key
     */
    public static void maskPayload(long buf, long len, int maskKey) {
        // Process 8 bytes at a time when possible
        long i = 0;
        long longMask = ((long) maskKey << 32) | (maskKey & 0xFFFFFFFFL);

        // Process 8-byte chunks
        while (i + 8 <= len) {
            long value = Unsafe.getUnsafe().getLong(buf + i);
            Unsafe.getUnsafe().putLong(buf + i, value ^ longMask);
            i += 8;
        }

        // Process 4-byte chunk if remaining
        if (i + 4 <= len) {
            int value = Unsafe.getUnsafe().getInt(buf + i);
            Unsafe.getUnsafe().putInt(buf + i, value ^ maskKey);
            i += 4;
        }

        // Process remaining bytes (0-3 bytes) - extract mask byte inline to avoid allocation
        while (i < len) {
            byte b = Unsafe.getUnsafe().getByte(buf + i);
            int maskByte = (maskKey >> (((int) i & 3) << 3)) & 0xFF;
            Unsafe.getUnsafe().putByte(buf + i, (byte) (b ^ maskByte));
            i++;
        }
    }
}
