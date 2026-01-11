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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Native memory buffer implementation of IlpV4OutputSink.
 * <p>
 * This is used by the TCP sender where we need to accumulate the entire
 * message before sending over the socket.
 */
public class IlpV4NativeBufferSink implements IlpV4OutputSink, Closeable {

    private long bufferAddress;
    private int bufferCapacity;
    private int position;

    public IlpV4NativeBufferSink() {
        this(64 * 1024); // 64KB initial buffer
    }

    public IlpV4NativeBufferSink(int initialCapacity) {
        this.bufferCapacity = initialCapacity;
        this.bufferAddress = Unsafe.malloc(bufferCapacity, MemoryTag.NATIVE_DEFAULT);
        this.position = 0;
    }

    @Override
    public void putByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufferAddress + position, value);
        position++;
    }

    @Override
    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufferAddress + position, value);
        position += 2;
    }

    @Override
    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufferAddress + position, value);
        position += 4;
    }

    @Override
    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufferAddress + position, value);
        position += 8;
    }

    @Override
    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(bufferAddress + position, value);
        position += 4;
    }

    @Override
    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(bufferAddress + position, value);
        position += 8;
    }

    @Override
    public void putLongBE(long value) {
        ensureCapacity(8);
        for (int i = 7; i >= 0; i--) {
            Unsafe.getUnsafe().putByte(bufferAddress + position + (7 - i), (byte) (value >> (i * 8)));
        }
        position += 8;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(int pos) {
        this.position = pos;
    }

    @Override
    public long addressAt(int offset) {
        return bufferAddress + offset;
    }

    @Override
    public int capacity() {
        return bufferCapacity;
    }

    @Override
    public void ensureCapacity(int required) {
        if (position + required > bufferCapacity) {
            int newCapacity = Math.max(bufferCapacity * 2, position + required);
            long newAddress = Unsafe.malloc(newCapacity, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().copyMemory(bufferAddress, newAddress, position);
            Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
            bufferAddress = newAddress;
            bufferCapacity = newCapacity;
        }
    }

    @Override
    public void putBytes(long srcAddress, int length) {
        ensureCapacity(length);
        Unsafe.getUnsafe().copyMemory(srcAddress, bufferAddress + position, length);
        position += length;
    }

    /**
     * Resets the buffer for reuse.
     */
    public void reset() {
        position = 0;
    }

    /**
     * Returns the buffer start address.
     */
    public long getBufferAddress() {
        return bufferAddress;
    }

    /**
     * Copies the buffer contents to a byte array.
     */
    public byte[] toByteArray() {
        byte[] result = new byte[position];
        for (int i = 0; i < position; i++) {
            result[i] = Unsafe.getUnsafe().getByte(bufferAddress + i);
        }
        return result;
    }

    @Override
    public void close() {
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
            bufferAddress = 0;
        }
    }
}
