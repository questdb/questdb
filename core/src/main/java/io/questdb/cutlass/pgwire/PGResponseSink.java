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

package io.questdb.cutlass.pgwire;

import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.BinarySequence;
import io.questdb.std.str.Utf8Sink;

public interface PGResponseSink extends Utf8Sink {

    void bookmark();

    void bump(int size);

    void checkCapacity(long size);

    long getMaxBlobSize();

    long getSendBufferPtr();

    long getSendBufferSize();

    long getWrittenBytes();

    void put(BinarySequence sequence);

    /**
     * Writes a pre-formatted int value directly to the buffer without byte order conversion.
     * <p>
     * IMPORTANT: When using this method with PGWire protocol, the input value must already
     * be in network byte order (big-endian). Since QuestDB operates on little-endian architecture,
     * this means the bytes must be reversed before calling this method.
     *
     * @param xValue the int value in big-endian byte order
     * @see #putNetworkInt(int) for automatic byte order conversion
     */
    void putDirectInt(int xValue);

    /**
     * Writes a pre-formatted show value directly to the buffer without byte order conversion.
     * <p>
     * IMPORTANT: When using this method with PGWire protocol, the input value must already
     * be in network byte order (big-endian). Since QuestDB operates on little-endian architecture,
     * this means the bytes must be reversed before calling this method.
     *
     * @param xValue the int value in big-endian byte order
     * @see #putNetworkInt(int) for automatic byte order conversion
     */
    void putDirectShort(short xValue);

    void putIntDirect(int value);

    void putIntUnsafe(long offset, int value);

    void putLen(long start);

    void putLenEx(long start);

    void putNetworkDouble(double value);

    void putNetworkFloat(float value);

    /**
     * Writes an int value to the buffer in PGWire network byte order (big-endian).
     * <p>
     * This method handles the necessary byte order conversion internally. Since QuestDB
     * operates on a little-endian architecture, this method will reverse the bytes before
     * writing to ensure PGWire compatibility.
     *
     * @param value the int value to write
     * @see #putDirectInt(int) for writing pre-reversed bytes
     */
    void putNetworkInt(int value);

    /**
     * Writes an int value to the buffer in PGWire network byte order (big-endian).
     * <p>
     * This method handles the necessary byte order conversion internally. Since QuestDB
     * operates on a little-endian architecture, this method will reverse the bytes before
     * writing to ensure PGWire compatibility.
     *
     * @param address to write the value to
     * @param value   the int value to write
     * @see #getSendBufferPtr() to retrieve a writable address
     */
    void putNetworkInt(long address, int value);

    void putNetworkLong(long value);

    /**
     * Writes a short value to the buffer in PGWire network byte order (big-endian).
     * <p>
     * This method handles the necessary byte order conversion internally. Since QuestDB
     * operates on little-endian architecture, this method will reverse the bytes before
     * writing to ensure PGWire compatibility.
     *
     * @param value the int value to write
     * @see #putDirectInt(int) for writing pre-reversed bytes
     */
    void putNetworkShort(short value);

    /**
     * Writes a short value to the buffer in PGWire network byte order (big-endian) to a specific position.
     * <p>
     * This method handles the necessary byte order conversion internally. Since QuestDB
     * operates on little-endian architecture, this method will reverse the bytes before
     * writing to ensure PGWire compatibility.
     *
     * @param address to write the value to
     * @param value   the int value to write
     * @see #getSendBufferPtr() to retrieve a writable address
     */
    void putNetworkShort(long address, short value);

    void putZ(CharSequence value);

    void reset();

    void resetToBookmark();

    void resetToBookmark(long address);

    int sendBufferAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException;

    void setNullValue();

    long skipInt();
}
