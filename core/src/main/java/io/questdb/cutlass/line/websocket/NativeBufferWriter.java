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

package io.questdb.cutlass.line.websocket;

import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * A simple native memory buffer writer for encoding ILP v4 messages.
 * <p>
 * This class provides write methods similar to HttpClient.Request but writes
 * to a native memory buffer that can be sent over WebSocket.
 * <p>
 * All multi-byte values are written in little-endian format unless otherwise specified.
 */
public class NativeBufferWriter implements IlpBufferWriter, QuietCloseable {

    private static final int DEFAULT_CAPACITY = 8192;

    private long bufferPtr;
    private int capacity;
    private int position;

    public NativeBufferWriter() {
        this(DEFAULT_CAPACITY);
    }

    public NativeBufferWriter(int initialCapacity) {
        this.capacity = initialCapacity;
        this.bufferPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        this.position = 0;
    }

    /**
     * Returns the buffer pointer.
     */
    @Override
    public long getBufferPtr() {
        return bufferPtr;
    }

    /**
     * Returns the current write position (number of bytes written).
     */
    @Override
    public int getPosition() {
        return position;
    }

    /**
     * Resets the buffer for reuse.
     */
    @Override
    public void reset() {
        position = 0;
    }

    /**
     * Writes a single byte.
     */
    @Override
    public void putByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufferPtr + position, value);
        position++;
    }

    /**
     * Writes a short (2 bytes, little-endian).
     */
    @Override
    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufferPtr + position, value);
        position += 2;
    }

    /**
     * Writes an int (4 bytes, little-endian).
     */
    @Override
    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufferPtr + position, value);
        position += 4;
    }

    /**
     * Writes a long (8 bytes, little-endian).
     */
    @Override
    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufferPtr + position, value);
        position += 8;
    }

    /**
     * Writes a long in big-endian order.
     */
    @Override
    public void putLongBE(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufferPtr + position, Long.reverseBytes(value));
        position += 8;
    }

    /**
     * Writes a float (4 bytes, little-endian).
     */
    @Override
    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(bufferPtr + position, value);
        position += 4;
    }

    /**
     * Writes a double (8 bytes, little-endian).
     */
    @Override
    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(bufferPtr + position, value);
        position += 8;
    }

    /**
     * Writes a block of bytes from native memory.
     */
    @Override
    public void putBlockOfBytes(long from, long len) {
        ensureCapacity((int) len);
        Unsafe.getUnsafe().copyMemory(from, bufferPtr + position, len);
        position += (int) len;
    }

    /**
     * Writes a varint (unsigned LEB128).
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

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        putVarint(bytes.length);
        for (byte b : bytes) {
            putByte(b);
        }
    }

    /**
     * Writes UTF-8 bytes directly without length prefix.
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
     * Returns the UTF-8 encoded length of a string.
     */
    public static int utf8Length(String s) {
        if (s == null) return 0;
        int len = 0;
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c < 0x800) {
                len += 2;
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n) {
                i++;
                len += 4;
            } else {
                len += 3;
            }
        }
        return len;
    }

    /**
     * Patches an int value at the specified offset.
     * Used for updating length fields after writing content.
     */
    @Override
    public void patchInt(int offset, int value) {
        Unsafe.getUnsafe().putInt(bufferPtr + offset, value);
    }

    /**
     * Returns the current buffer capacity.
     */
    @Override
    public int getCapacity() {
        return capacity;
    }

    /**
     * Skips the specified number of bytes, advancing the position.
     * Used when data has been written directly to the buffer via getBufferPtr().
     *
     * @param bytes number of bytes to skip
     */
    @Override
    public void skip(int bytes) {
        position += bytes;
    }

    /**
     * Ensures the buffer has at least the specified additional capacity.
     *
     * @param needed additional bytes needed beyond current position
     */
    @Override
    public void ensureCapacity(int needed) {
        if (position + needed > capacity) {
            int newCapacity = Math.max(capacity * 2, position + needed);
            bufferPtr = Unsafe.realloc(bufferPtr, capacity, newCapacity, MemoryTag.NATIVE_DEFAULT);
            capacity = newCapacity;
        }
    }

    @Override
    public void close() {
        if (bufferPtr != 0) {
            Unsafe.free(bufferPtr, capacity, MemoryTag.NATIVE_DEFAULT);
            bufferPtr = 0;
        }
    }
}
