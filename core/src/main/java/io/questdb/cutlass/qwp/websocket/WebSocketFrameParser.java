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

import java.nio.ByteOrder;

/**
 * Zero-allocation WebSocket frame parser.
 * Parses WebSocket frames according to RFC 6455.
 *
 * <p>The parser operates on raw memory buffers and maintains minimal state.
 * It can parse frames incrementally when data arrives in chunks.
 *
 * <p>Thread safety: This class is NOT thread-safe. Each connection should
 * have its own parser instance.
 */
public class WebSocketFrameParser {
    /**
     * Frame completely parsed.
     */
    public static final int STATE_COMPLETE = 3;
    /**
     * Error state - frame is invalid.
     */
    public static final int STATE_ERROR = 4;
    /**
     * Initial state, waiting for frame header.
     */
    public static final int STATE_HEADER = 0;
    /**
     * Need more data to complete parsing.
     */
    public static final int STATE_NEED_MORE = 1;
    /**
     * Header parsed, need payload data.
     */
    public static final int STATE_NEED_PAYLOAD = 2;
    // Frame header bits
    private static final int FIN_BIT = 0x80;
    private static final boolean IS_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    private static final int LENGTH_MASK = 0x7F;
    private static final int MASK_BIT = 0x80;
    // Control frame max payload size (RFC 6455)
    private static final int MAX_CONTROL_FRAME_PAYLOAD = 125;
    private static final int OPCODE_MASK = 0x0F;
    private static final int RSV_BITS = 0x70;
    private int errorCode;
    // Parsed frame data
    private boolean fin;
    private int headerSize;
    private boolean isStrictMode = false;  // If true, reject non-minimal length encodings
    private int maskKey;
    private boolean masked;
    private int opcode;
    private long payloadLength;
    // Parser state
    private int state = STATE_HEADER;

    public int getErrorCode() {
        return errorCode;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public int getMaskKey() {
        return maskKey;
    }

    public int getOpcode() {
        return opcode;
    }

    public long getPayloadLength() {
        return payloadLength;
    }

    public int getState() {
        return state;
    }

    public boolean isFin() {
        return fin;
    }

    public boolean isMasked() {
        return masked;
    }

    /**
     * Parses a WebSocket frame from the given buffer.
     *
     * @param buf   the start of the buffer
     * @param limit the end of the buffer (exclusive)
     * @return the number of bytes consumed, or 0 if more data is needed
     */
    public int parse(long buf, long limit) {
        long available = limit - buf;

        if (available < 2) {
            state = STATE_NEED_MORE;
            return 0;
        }

        // Parse first two bytes
        int byte0 = Unsafe.getUnsafe().getByte(buf) & 0xFF;
        int byte1 = Unsafe.getUnsafe().getByte(buf + 1) & 0xFF;

        // Check reserved bits (must be 0 unless extension negotiated)
        if ((byte0 & RSV_BITS) != 0) {
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        fin = (byte0 & FIN_BIT) != 0;
        opcode = byte0 & OPCODE_MASK;

        // Validate opcode
        if (!WebSocketOpcode.isValid(opcode)) {
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        // Control frames must not be fragmented
        if (WebSocketOpcode.isControlFrame(opcode) && !fin) {
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        masked = (byte1 & MASK_BIT) != 0;
        int lengthField = byte1 & LENGTH_MASK;

        if (!masked) {
            // Client frames MUST be masked
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        // Calculate header size and payload length
        int offset = 2;

        if (lengthField <= 125) {
            payloadLength = lengthField;
        } else if (lengthField == 126) {
            // 16-bit extended length
            if (available < 4) {
                state = STATE_NEED_MORE;
                return 0;
            }
            int high = Unsafe.getUnsafe().getByte(buf + 2) & 0xFF;
            int low = Unsafe.getUnsafe().getByte(buf + 3) & 0xFF;
            payloadLength = (high << 8) | low;

            // Strict mode: reject non-minimal encodings
            if (isStrictMode && payloadLength < 126) {
                state = STATE_ERROR;
                errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
                return 0;
            }

            offset = 4;
        } else {
            // 64-bit extended length
            if (available < 10) {
                state = STATE_NEED_MORE;
                return 0;
            }
            payloadLength = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + 2));

            // Strict mode: reject non-minimal encodings
            if (isStrictMode && payloadLength <= 65535) {
                state = STATE_ERROR;
                errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
                return 0;
            }

            // MSB must be 0 (no negative lengths)
            if (payloadLength < 0) {
                state = STATE_ERROR;
                errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
                return 0;
            }

            offset = 10;
        }

        // Control frames must not have payload > 125 bytes
        if (WebSocketOpcode.isControlFrame(opcode) && payloadLength > MAX_CONTROL_FRAME_PAYLOAD) {
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        // Close frame with 1 byte payload is invalid (must be 0 or >= 2)
        if (opcode == WebSocketOpcode.CLOSE && payloadLength == 1) {
            state = STATE_ERROR;
            errorCode = WebSocketCloseCode.PROTOCOL_ERROR;
            return 0;
        }

        // Parse mask key if present
        if (masked) {
            if (available < offset + 4) {
                state = STATE_NEED_MORE;
                return 0;
            }
            // Read mask key in network byte order (big-endian) per RFC 6455
            maskKey = ((Unsafe.getUnsafe().getByte(buf + offset) & 0xFF) << 24)
                    | ((Unsafe.getUnsafe().getByte(buf + offset + 1) & 0xFF) << 16)
                    | ((Unsafe.getUnsafe().getByte(buf + offset + 2) & 0xFF) << 8)
                    | (Unsafe.getUnsafe().getByte(buf + offset + 3) & 0xFF);
            offset += 4;
        } else {
            maskKey = 0;
        }

        headerSize = offset;

        // Check if we have the complete payload.
        // The totalFrameSize < 0 check is there for the edge case of long overflow.
        long totalFrameSize = headerSize + payloadLength;
        if (totalFrameSize < 0 || available < totalFrameSize) {
            state = STATE_NEED_PAYLOAD;
            return headerSize;
        }

        state = STATE_COMPLETE;
        return (int) totalFrameSize;
    }

    /**
     * Resets the parser state for parsing a new frame.
     */
    public void reset() {
        state = STATE_HEADER;
        fin = false;
        opcode = 0;
        masked = false;
        maskKey = 0;
        payloadLength = 0;
        headerSize = 0;
        errorCode = 0;
    }

    /**
     * Sets the mask key for unmasking. Used in testing.
     */
    public void setMaskKey(int maskKey) {
        this.maskKey = maskKey;
        this.masked = true;
    }

    public void setStrictMode(boolean isStrictMode) {
        this.isStrictMode = isStrictMode;
    }

    /**
     * Unmasks the payload data in place.
     *
     * @param buf the start of the payload data
     * @param len the length of the payload
     */
    public void unmaskPayload(long buf, long len) {
        if (!masked || maskKey == 0) {
            return;
        }

        // maskKey is in big-endian convention: MSB = wire byte 0 = mask byte for position 0.
        // For bulk XOR via getInt/getLong (native byte order), convert to native order
        // so that memory position 0 XORs with mask byte 0, position 1 with mask byte 1, etc.
        int nativeMask = IS_BIG_ENDIAN ? maskKey : Integer.reverseBytes(maskKey);
        long longMask = ((long) nativeMask << 32) | (nativeMask & 0xFFFFFFFFL);

        long i = 0;

        // Process 8-byte chunks
        while (i + 8 <= len) {
            long value = Unsafe.getUnsafe().getLong(buf + i);
            Unsafe.getUnsafe().putLong(buf + i, value ^ longMask);
            i += 8;
        }

        // Process 4-byte chunk if remaining
        if (i + 4 <= len) {
            int value = Unsafe.getUnsafe().getInt(buf + i);
            Unsafe.getUnsafe().putInt(buf + i, value ^ nativeMask);
            i += 4;
        }

        // Process remaining bytes - extract mask byte in big-endian order
        while (i < len) {
            byte b = Unsafe.getUnsafe().getByte(buf + i);
            int maskByte = (maskKey >>> ((3 - ((int) i & 3)) << 3)) & 0xFF;
            Unsafe.getUnsafe().putByte(buf + i, (byte) (b ^ maskByte));
            i++;
        }
    }
}
