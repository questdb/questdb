/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.binary;

import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.Long256;
import io.questdb.std.str.Utf8Sequence;

/**
 * Binary data sink for writing primitive values to native memory buffers.
 * <p>
 * This interface abstracts a fixed-size native memory buffer that can be
 * written to using Unsafe operations. When the buffer is full, write operations
 * throw {@link NoSpaceLeftInResponseBufferException}.
 * <p>
 * The sink guarantees that any single primitive value up to 256 bits (32 bytes)
 * can always be written if there is available space, but variable-length data
 * may require multiple writes across buffer flushes.
 * <p>
 * Thread-safety: Implementations are NOT thread-safe.
 */
public interface BinaryDataSink {

    /**
     * Returns the number of bytes available in the buffer.
     * Guaranteed to be at least 32 bytes (for Long256) when non-zero.
     *
     * @return bytes available for writing
     */
    int available();

    /**
     * Saves the current position for potential rollback.
     * Used when attempting to write data that might not fit.
     *
     * @return saved position (bookmark)
     */
    int bookmark();

    /**
     * Resets the sink to point to a new buffer.
     *
     * @param address native memory address of the buffer
     * @param size    buffer size in bytes
     */
    void of(long address, int size);

    /**
     * Returns the current write position in the buffer.
     *
     * @return number of bytes written to current buffer
     */
    int position();

    /**
     * Writes a single byte to the buffer.
     *
     * @param value byte to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putByte(byte value);

    /**
     * Writes bytes from a byte array without length prefix.
     * This method may write partial data if the buffer fills up.
     *
     * @param data   source byte array
     * @param offset offset in the array to start from
     * @param length number of bytes to write
     * @return number of bytes actually written
     * @throws NoSpaceLeftInResponseBufferException if buffer is full before any bytes written
     */
    int putByteArray(byte[] data, int offset, int length);

    /**
     * Writes raw bytes without length prefix.
     * This method may write partial data if the buffer fills up.
     *
     * @param address native memory address of source data
     * @param length  number of bytes to write
     * @return number of bytes actually written
     * @throws NoSpaceLeftInResponseBufferException if buffer is full before any bytes written
     */
    int putBytes(long address, long length);

    /**
     * Writes a char (2 bytes, little-endian) to the buffer.
     *
     * @param value char to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putChar(char value);

    /**
     * Writes a double (8 bytes, IEEE 754, little-endian) to the buffer.
     *
     * @param value double to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putDouble(double value);

    /**
     * Writes a float (4 bytes, IEEE 754, little-endian) to the buffer.
     *
     * @param value float to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putFloat(float value);

    /**
     * Writes an int (4 bytes, little-endian) to the buffer.
     *
     * @param value int to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putInt(int value);

    /**
     * Writes a long (8 bytes, little-endian) to the buffer.
     *
     * @param value long to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putLong(long value);

    /**
     * Writes a Long128/UUID (16 bytes, little-endian) to the buffer.
     * Writes low 64 bits first, then high 64 bits.
     *
     * @param lo low 64 bits
     * @param hi high 64 bits
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putLong128(long lo, long hi);

    /**
     * Writes a Long256 (32 bytes, little-endian) to the buffer.
     *
     * @param value Long256 to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putLong256(Long256 value);

    /**
     * Writes a short (2 bytes, little-endian) to the buffer.
     *
     * @param value short to write
     * @throws NoSpaceLeftInResponseBufferException if buffer is full
     */
    void putShort(short value);

    /**
     * Writes UTF-8 bytes from a sequence without length prefix.
     * This method may write partial data if the buffer fills up.
     *
     * @param value  UTF-8 sequence to write
     * @param offset byte offset to start from
     * @param length number of bytes to write
     * @return number of bytes actually written
     * @throws NoSpaceLeftInResponseBufferException if buffer is full before any bytes written
     */
    int putUtf8(Utf8Sequence value, int offset, int length);

    /**
     * Restores the sink to a previously bookmarked position.
     * Used to rollback partial writes that didn't complete.
     *
     * @param position the bookmarked position to restore
     */
    void rollback(int position);
}
