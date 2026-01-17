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

package io.questdb.cutlass.ilpv4.client;

import io.questdb.cutlass.line.array.ArrayBufferAppender;

/**
 * Buffer writer interface for ILP v4 message encoding.
 * <p>
 * This interface extends {@link ArrayBufferAppender} with additional methods
 * required for encoding ILP v4 messages, including varint encoding, string
 * handling, and buffer manipulation.
 * <p>
 * Implementations include:
 * <ul>
 *   <li>{@link NativeBufferWriter} - standalone native memory buffer</li>
 *   <li>{@link io.questdb.cutlass.http.client.WebSocketSendBuffer} - WebSocket frame buffer</li>
 * </ul>
 * <p>
 * All multi-byte values are written in little-endian format unless the method
 * name explicitly indicates big-endian (e.g., {@link #putLongBE}).
 */
public interface IlpBufferWriter extends ArrayBufferAppender {

    // === Primitive writes (little-endian) ===

    /**
     * Writes a short (2 bytes, little-endian).
     */
    void putShort(short value);

    /**
     * Writes a float (4 bytes, little-endian).
     */
    void putFloat(float value);

    // === Big-endian writes ===

    /**
     * Writes a long in big-endian byte order.
     */
    void putLongBE(long value);

    // === Variable-length encoding ===

    /**
     * Writes an unsigned variable-length integer (LEB128 encoding).
     * <p>
     * Each byte contains 7 bits of data with the high bit indicating
     * whether more bytes follow.
     */
    void putVarint(long value);

    // === String encoding ===

    /**
     * Writes a length-prefixed UTF-8 string.
     * <p>
     * Format: varint length + UTF-8 bytes
     *
     * @param value the string to write (may be null or empty)
     */
    void putString(String value);

    /**
     * Writes UTF-8 encoded bytes directly without length prefix.
     *
     * @param value the string to encode (may be null or empty)
     */
    void putUtf8(String value);

    // === Buffer manipulation ===

    /**
     * Patches an int value at the specified offset in the buffer.
     * <p>
     * Used for updating length fields after writing content.
     *
     * @param offset the byte offset from buffer start
     * @param value  the int value to write
     */
    void patchInt(int offset, int value);

    /**
     * Skips the specified number of bytes, advancing the position.
     * <p>
     * Used when data has been written directly to the buffer via
     * {@link #getBufferPtr()}.
     *
     * @param bytes number of bytes to skip
     */
    void skip(int bytes);

    /**
     * Ensures the buffer has capacity for at least the specified
     * additional bytes beyond the current position.
     *
     * @param additionalBytes number of additional bytes needed
     */
    void ensureCapacity(int additionalBytes);

    /**
     * Resets the buffer for reuse, setting the position to 0.
     * <p>
     * Does not deallocate memory.
     */
    void reset();

    // === Buffer state ===

    /**
     * Returns the current write position (number of bytes written).
     */
    int getPosition();

    /**
     * Returns the current buffer capacity in bytes.
     */
    int getCapacity();

    /**
     * Returns the native memory pointer to the buffer start.
     * <p>
     * The returned pointer is valid until the next buffer growth operation.
     * Use with care and only for reading completed data.
     */
    long getBufferPtr();

    // === Utility ===

    /**
     * Returns the UTF-8 encoded length of a string.
     *
     * @param s the string (may be null)
     * @return the number of bytes needed to encode the string as UTF-8
     */
    static int utf8Length(String s) {
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
}
