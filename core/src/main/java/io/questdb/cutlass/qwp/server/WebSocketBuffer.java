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

package io.questdb.cutlass.qwp.server;

import io.questdb.cutlass.qwp.protocol.*;

import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * A resizable native memory buffer for WebSocket I/O operations.
 *
 * <p>The buffer manages read and write positions, supports automatic growth,
 * and provides methods for compaction.
 *
 * <p>Thread safety: This class is NOT thread-safe.
 */
public class WebSocketBuffer implements Mutable, QuietCloseable {
    private static final int MIN_CAPACITY = 256;

    private long address;
    private int capacity;
    private int readPos;
    private int writePos;

    public WebSocketBuffer(int initialCapacity) {
        this.capacity = Math.max(initialCapacity, MIN_CAPACITY);
        this.address = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        this.readPos = 0;
        this.writePos = 0;
    }

    /**
     * Returns the start address for reading.
     */
    public long readAddress() {
        return address + readPos;
    }

    /**
     * Returns the start address for writing.
     */
    public long writeAddress() {
        return address + writePos;
    }

    /**
     * Returns the number of readable bytes.
     */
    public long readableBytes() {
        return writePos - readPos;
    }

    /**
     * Returns the number of writable bytes without resizing.
     */
    public long writableBytes() {
        return capacity - writePos;
    }

    /**
     * Returns the buffer capacity.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Writes data from a byte array.
     */
    public void write(byte[] data) {
        write(data, 0, data.length);
    }

    /**
     * Writes data from a byte array.
     */
    public void write(byte[] data, int offset, int length) {
        ensureCapacity(length);
        for (int i = 0; i < length; i++) {
            Unsafe.getUnsafe().putByte(address + writePos + i, data[offset + i]);
        }
        writePos += length;
    }

    /**
     * Writes data from a native memory address.
     */
    public void write(long srcAddress, int length) {
        ensureCapacity(length);
        Unsafe.getUnsafe().copyMemory(srcAddress, address + writePos, length);
        writePos += length;
    }

    /**
     * Ensures the buffer has enough capacity to write the given number of bytes.
     */
    public void ensureCapacity(int additional) {
        if (writePos + additional <= capacity) {
            return;
        }

        // Compact first if possible
        if (readPos > 0) {
            compact();
            if (writePos + additional <= capacity) {
                return;
            }
        }

        // Grow buffer
        int newCapacity = Math.max(capacity * 2, writePos + additional);
        long newAddress = Unsafe.malloc(newCapacity, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().copyMemory(address, newAddress, writePos);
        Unsafe.free(address, capacity, MemoryTag.NATIVE_DEFAULT);
        address = newAddress;
        capacity = newCapacity;
    }

    /**
     * Advances the write position.
     */
    public void advanceWrite(int bytes) {
        writePos += bytes;
    }

    /**
     * Skips (consumes) bytes from the read position.
     */
    public void skip(int bytes) {
        readPos += bytes;
        if (readPos > writePos) {
            readPos = writePos;
        }
    }

    /**
     * Compacts the buffer by moving unread data to the beginning.
     */
    public void compact() {
        if (readPos == 0) {
            return;
        }
        int remaining = writePos - readPos;
        if (remaining > 0) {
            Unsafe.getUnsafe().copyMemory(address + readPos, address, remaining);
        }
        readPos = 0;
        writePos = remaining;
    }

    /**
     * Returns the buffer contents as a byte array.
     * Note: This allocates a new array, use only for testing.
     */
    public byte[] toByteArray() {
        int length = writePos - readPos;
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = Unsafe.getUnsafe().getByte(address + readPos + i);
        }
        return result;
    }

    @Override
    public void clear() {
        readPos = 0;
        writePos = 0;
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, capacity, MemoryTag.NATIVE_DEFAULT);
            address = 0;
            capacity = 0;
        }
        readPos = 0;
        writePos = 0;
    }
}
