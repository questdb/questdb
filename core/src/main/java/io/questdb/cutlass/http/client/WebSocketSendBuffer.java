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

package io.questdb.cutlass.http.client;

import io.questdb.cutlass.http.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.http.websocket.WebSocketOpcode;
import io.questdb.cutlass.line.websocket.IlpBufferWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Zero-GC WebSocket send buffer that implements {@link ArrayBufferAppender} for direct
 * payload writing. Manages native memory with safe growth and handles WebSocket frame
 * building (reserve header -> write payload -> patch header -> mask).
 * <p>
 * Usage pattern:
 * <pre>
 * buffer.beginBinaryFrame();
 * // Write payload using ArrayBufferAppender methods
 * buffer.putLong(value);
 * buffer.putBlockOfBytes(ptr, len);
 * // Finish frame and get send info
 * FrameInfo frame = buffer.endBinaryFrame();
 * // Send frame using socket
 * socket.send(buffer.getBufferPtr() + frame.offset, frame.length);
 * buffer.reset();
 * </pre>
 * <p>
 * Thread safety: This class is NOT thread-safe. Each connection should have its own buffer.
 */
public class WebSocketSendBuffer implements IlpBufferWriter, QuietCloseable {

    // Maximum header size: 2 (base) + 8 (64-bit length) + 4 (mask key)
    private static final int MAX_HEADER_SIZE = 14;

    private static final int DEFAULT_INITIAL_CAPACITY = 65536;
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8; // Leave room for alignment

    private long bufPtr;
    private int bufCapacity;
    private int writePos;           // Current write position (offset from bufPtr)
    private int frameStartOffset;   // Where current frame's reserved header starts
    private int payloadStartOffset; // Where payload begins (frameStart + MAX_HEADER_SIZE)

    private final Rnd rnd;
    private final int maxBufferSize;

    /**
     * Creates a new WebSocket send buffer with default initial capacity.
     */
    public WebSocketSendBuffer() {
        this(DEFAULT_INITIAL_CAPACITY, MAX_BUFFER_SIZE);
    }

    /**
     * Creates a new WebSocket send buffer with specified initial capacity.
     *
     * @param initialCapacity initial buffer size in bytes
     */
    public WebSocketSendBuffer(int initialCapacity) {
        this(initialCapacity, MAX_BUFFER_SIZE);
    }

    /**
     * Creates a new WebSocket send buffer with specified initial and max capacity.
     *
     * @param initialCapacity initial buffer size in bytes
     * @param maxBufferSize   maximum buffer size in bytes
     */
    public WebSocketSendBuffer(int initialCapacity, int maxBufferSize) {
        this.bufCapacity = Math.max(initialCapacity, MAX_HEADER_SIZE * 2);
        this.maxBufferSize = maxBufferSize;
        this.bufPtr = Unsafe.malloc(bufCapacity, MemoryTag.NATIVE_DEFAULT);
        this.writePos = 0;
        this.frameStartOffset = 0;
        this.payloadStartOffset = 0;
        this.rnd = new Rnd(System.nanoTime(), System.currentTimeMillis());
    }

    @Override
    public void close() {
        if (bufPtr != 0) {
            Unsafe.free(bufPtr, bufCapacity, MemoryTag.NATIVE_DEFAULT);
            bufPtr = 0;
            bufCapacity = 0;
        }
    }

    // === Buffer Management ===

    /**
     * Ensures the buffer has capacity for the specified number of additional bytes.
     * May reallocate the buffer if necessary.
     *
     * @param additionalBytes number of additional bytes needed
     */
    @Override
    public void ensureCapacity(int additionalBytes) {
        long requiredCapacity = (long) writePos + additionalBytes;
        if (requiredCapacity > bufCapacity) {
            grow(requiredCapacity);
        }
    }

    private void grow(long requiredCapacity) {
        if (requiredCapacity > maxBufferSize) {
            throw new HttpClientException("WebSocket buffer size exceeded maximum [required=")
                    .put(requiredCapacity)
                    .put(", max=")
                    .put(maxBufferSize)
                    .put(']');
        }
        int newCapacity = (int) Math.min(
                Numbers.ceilPow2((int) requiredCapacity),
                maxBufferSize
        );
        bufPtr = Unsafe.realloc(bufPtr, bufCapacity, newCapacity, MemoryTag.NATIVE_DEFAULT);
        bufCapacity = newCapacity;
    }

    // === ArrayBufferAppender Implementation ===

    @Override
    public void putByte(byte b) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufPtr + writePos, b);
        writePos++;
    }

    @Override
    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufPtr + writePos, value);
        writePos += 4;
    }

    @Override
    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufPtr + writePos, value);
        writePos += 8;
    }

    @Override
    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(bufPtr + writePos, value);
        writePos += 8;
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        if (len <= 0) {
            return;
        }
        ensureCapacity((int) len);
        Vect.memcpy(bufPtr + writePos, from, len);
        writePos += (int) len;
    }

    // === Additional write methods (not in ArrayBufferAppender but useful) ===

    /**
     * Writes a short value in little-endian format.
     */
    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufPtr + writePos, value);
        writePos += 2;
    }

    /**
     * Writes a float value.
     */
    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(bufPtr + writePos, value);
        writePos += 4;
    }

    /**
     * Writes a long value in big-endian format.
     */
    public void putLongBE(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufPtr + writePos, Long.reverseBytes(value));
        writePos += 8;
    }

    /**
     * Writes raw bytes from a byte array.
     */
    public void putBytes(byte[] bytes, int offset, int length) {
        if (length <= 0) {
            return;
        }
        ensureCapacity(length);
        for (int i = 0; i < length; i++) {
            Unsafe.getUnsafe().putByte(bufPtr + writePos + i, bytes[offset + i]);
        }
        writePos += length;
    }

    /**
     * Writes an ASCII string.
     */
    public void putAscii(CharSequence cs) {
        if (cs == null) {
            return;
        }
        int len = cs.length();
        ensureCapacity(len);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(bufPtr + writePos + i, (byte) cs.charAt(i));
        }
        writePos += len;
    }

    // === IlpBufferWriter Implementation ===

    /**
     * Writes an unsigned variable-length integer (LEB128 encoding).
     */
    @Override
    public void putVarint(long value) {
        while (value > 0x7F) {
            putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        putByte((byte) value);
    }

    /**
     * Writes a length-prefixed UTF-8 string.
     */
    @Override
    public void putString(String value) {
        if (value == null || value.isEmpty()) {
            putVarint(0);
            return;
        }
        int utf8Len = IlpBufferWriter.utf8Length(value);
        putVarint(utf8Len);
        putUtf8(value);
    }

    /**
     * Writes UTF-8 encoded bytes directly without length prefix.
     */
    @Override
    public void putUtf8(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                putByte((byte) c);
            } else if (c < 0x800) {
                putByte((byte) (0xC0 | (c >> 6)));
                putByte((byte) (0x80 | (c & 0x3F)));
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n) {
                char c2 = value.charAt(++i);
                int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                putByte((byte) (0xF0 | (codePoint >> 18)));
                putByte((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                putByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                putByte((byte) (0x80 | (codePoint & 0x3F)));
            } else {
                putByte((byte) (0xE0 | (c >> 12)));
                putByte((byte) (0x80 | ((c >> 6) & 0x3F)));
                putByte((byte) (0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Patches an int value at the specified offset.
     */
    @Override
    public void patchInt(int offset, int value) {
        Unsafe.getUnsafe().putInt(bufPtr + offset, value);
    }

    /**
     * Skips the specified number of bytes, advancing the position.
     */
    @Override
    public void skip(int bytes) {
        ensureCapacity(bytes);
        writePos += bytes;
    }

    /**
     * Gets the current write position (number of bytes written).
     */
    @Override
    public int getPosition() {
        return writePos;
    }

    // === Frame Building ===

    /**
     * Begins a new binary WebSocket frame. Reserves space for the maximum header size.
     * After calling this method, use ArrayBufferAppender methods to write the payload.
     */
    public void beginBinaryFrame() {
        beginFrame(WebSocketOpcode.BINARY);
    }

    /**
     * Begins a new text WebSocket frame. Reserves space for the maximum header size.
     */
    public void beginTextFrame() {
        beginFrame(WebSocketOpcode.TEXT);
    }

    /**
     * Begins a new WebSocket frame with the specified opcode.
     *
     * @param opcode the frame opcode
     */
    public void beginFrame(int opcode) {
        frameStartOffset = writePos;
        // Reserve maximum header space
        ensureCapacity(MAX_HEADER_SIZE);
        writePos += MAX_HEADER_SIZE;
        payloadStartOffset = writePos;
    }

    /**
     * Finishes the current binary frame, writing the header and applying masking.
     * Returns information about where to find the complete frame in the buffer.
     * <p>
     * IMPORTANT: Only call this after all payload writes are complete. The buffer
     * pointer is stable after this call (no more reallocations for this frame).
     *
     * @return frame info containing offset and length for sending
     */
    public FrameInfo endBinaryFrame() {
        return endFrame(WebSocketOpcode.BINARY);
    }

    /**
     * Finishes the current text frame, writing the header and applying masking.
     */
    public FrameInfo endTextFrame() {
        return endFrame(WebSocketOpcode.TEXT);
    }

    /**
     * Finishes the current frame with the specified opcode.
     *
     * @param opcode the frame opcode
     * @return frame info containing offset and length for sending
     */
    public FrameInfo endFrame(int opcode) {
        int payloadLen = writePos - payloadStartOffset;

        // Calculate actual header size (with mask key for client frames)
        int actualHeaderSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        int unusedSpace = MAX_HEADER_SIZE - actualHeaderSize;
        int actualFrameStart = frameStartOffset + unusedSpace;

        // Generate mask key
        int maskKey = rnd.nextInt();

        // Write header at actual position (after unused space)
        WebSocketFrameWriter.writeHeader(bufPtr + actualFrameStart, true, opcode, payloadLen, maskKey);

        // Apply mask to payload
        if (payloadLen > 0) {
            WebSocketFrameWriter.maskPayload(bufPtr + payloadStartOffset, payloadLen, maskKey);
        }

        return new FrameInfo(actualFrameStart, actualHeaderSize + payloadLen);
    }

    /**
     * Writes a complete ping frame (control frame, no masking needed for server).
     * Note: Client frames MUST be masked per RFC 6455. This writes a masked ping.
     *
     * @return frame info for sending
     */
    public FrameInfo writePingFrame() {
        return writePingFrame(0, 0);
    }

    /**
     * Writes a complete ping frame with payload.
     *
     * @param payloadPtr pointer to ping payload
     * @param payloadLen length of payload (max 125 bytes for control frames)
     * @return frame info for sending
     */
    public FrameInfo writePingFrame(long payloadPtr, int payloadLen) {
        if (payloadLen > 125) {
            throw new HttpClientException("Ping payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.PING, payloadLen, maskKey);
        writePos += written;

        if (payloadLen > 0) {
            Vect.memcpy(bufPtr + writePos, payloadPtr, payloadLen);
            WebSocketFrameWriter.maskPayload(bufPtr + writePos, payloadLen, maskKey);
            writePos += payloadLen;
        }

        return new FrameInfo(frameStart, headerSize + payloadLen);
    }

    /**
     * Writes a complete pong frame.
     *
     * @param payloadPtr pointer to pong payload (should match received ping)
     * @param payloadLen length of payload
     * @return frame info for sending
     */
    public FrameInfo writePongFrame(long payloadPtr, int payloadLen) {
        if (payloadLen > 125) {
            throw new HttpClientException("Pong payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.PONG, payloadLen, maskKey);
        writePos += written;

        if (payloadLen > 0) {
            Vect.memcpy(bufPtr + writePos, payloadPtr, payloadLen);
            WebSocketFrameWriter.maskPayload(bufPtr + writePos, payloadLen, maskKey);
            writePos += payloadLen;
        }

        return new FrameInfo(frameStart, headerSize + payloadLen);
    }

    /**
     * Writes a complete close frame.
     *
     * @param code   close status code (e.g., 1000 for normal closure)
     * @param reason optional reason string (may be null)
     * @return frame info for sending
     */
    public FrameInfo writeCloseFrame(int code, String reason) {
        int payloadLen = 2; // status code
        byte[] reasonBytes = null;
        if (reason != null && !reason.isEmpty()) {
            reasonBytes = reason.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            payloadLen += reasonBytes.length;
        }

        if (payloadLen > 125) {
            throw new HttpClientException("Close payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.CLOSE, payloadLen, maskKey);
        writePos += written;

        // Write status code (big-endian)
        long payloadStart = bufPtr + writePos;
        Unsafe.getUnsafe().putByte(payloadStart, (byte) ((code >> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(payloadStart + 1, (byte) (code & 0xFF));
        writePos += 2;

        // Write reason if present
        if (reasonBytes != null) {
            for (byte reasonByte : reasonBytes) {
                Unsafe.getUnsafe().putByte(bufPtr + writePos++, reasonByte);
            }
        }

        // Mask the payload (including status code and reason)
        WebSocketFrameWriter.maskPayload(payloadStart, payloadLen, maskKey);

        return new FrameInfo(frameStart, headerSize + payloadLen);
    }

    // === Buffer State ===

    /**
     * Gets the buffer pointer. Only use this for reading after frame is complete.
     */
    public long getBufferPtr() {
        return bufPtr;
    }

    /**
     * Gets the current buffer capacity.
     */
    public int getCapacity() {
        return bufCapacity;
    }

    /**
     * Gets the current write position (total bytes written since last reset).
     */
    public int getWritePos() {
        return writePos;
    }

    /**
     * Gets the payload length of the current frame being built.
     */
    public int getCurrentPayloadLength() {
        return writePos - payloadStartOffset;
    }

    /**
     * Resets the buffer for reuse. Does not deallocate memory.
     */
    public void reset() {
        writePos = 0;
        frameStartOffset = 0;
        payloadStartOffset = 0;
    }

    /**
     * Information about a completed WebSocket frame's location in the buffer.
     */
    public static final class FrameInfo {
        /**
         * Offset from buffer start where the frame begins.
         */
        public final int offset;

        /**
         * Total length of the frame (header + payload).
         */
        public final int length;

        public FrameInfo(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }
}
