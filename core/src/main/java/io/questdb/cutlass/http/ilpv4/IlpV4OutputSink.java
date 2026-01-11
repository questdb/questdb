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

package io.questdb.cutlass.http.ilpv4;

/**
 * Abstract output sink for ILP v4 encoding.
 * <p>
 * This interface allows the encoder to write to different output targets:
 * - Native memory buffer (for TCP sender)
 * - HTTP client request buffer (for HTTP sender)
 */
public interface IlpV4OutputSink {

    /**
     * Writes a single byte.
     */
    void putByte(byte value);

    /**
     * Writes a short (little-endian).
     */
    void putShort(short value);

    /**
     * Writes an int (little-endian).
     */
    void putInt(int value);

    /**
     * Writes a long (little-endian).
     */
    void putLong(long value);

    /**
     * Writes a float (little-endian).
     */
    void putFloat(float value);

    /**
     * Writes a double (little-endian).
     */
    void putDouble(double value);

    /**
     * Writes a long in big-endian order.
     */
    void putLongBE(long value);

    /**
     * Returns the current write position (bytes written since content start).
     */
    int position();

    /**
     * Sets the write position.
     * <p>
     * This is used for seeking back to patch header fields.
     */
    void position(int pos);

    /**
     * Returns the native address at the given offset from content start.
     * <p>
     * This is needed for BitWriter (Gorilla encoding) which writes directly to memory.
     */
    long addressAt(int offset);

    /**
     * Returns the current capacity.
     */
    int capacity();

    /**
     * Ensures the sink has at least the specified additional capacity.
     *
     * @param required number of additional bytes needed
     */
    void ensureCapacity(int required);

    /**
     * Writes bytes from native memory to the sink.
     *
     * @param srcAddress source native memory address
     * @param length     number of bytes to write
     */
    void putBytes(long srcAddress, int length);
}
