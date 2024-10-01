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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;

/**
 * Simplified off-heap variant of {@link BitSet}.
 */
public class DirectBitSet implements QuietCloseable, Reopenable, Mutable {
    public static final int BITS_PER_WORD = 64; // each word is 64-bit
    private final int initialNBits;
    private long ptr;
    private long size;

    public DirectBitSet() {
        this(16 * BITS_PER_WORD);
    }

    public DirectBitSet(int nBits) {
        this.initialNBits = Math.max(16 * BITS_PER_WORD, nBits);
        reopen();
    }

    public int capacity() {
        return (int) (size * 8); // the capacity is in bits
    }

    @Override
    public void clear() {
        Vect.memset(ptr, size, 0);
    }

    @Override
    public void close() {
        ptr = Unsafe.free(ptr, size, MemoryTag.NATIVE_BIT_SET);
        size = 0;
    }

    public boolean get(int bitIndex) {
        long offset = wordOffset(bitIndex);
        if (offset < size) {
            long word = Unsafe.getUnsafe().getLong(ptr + offset);
            return (word & 1L << bitIndex) != 0L;
        }
        return false;
    }

    /**
     * Sets the given bit to 1 and returns its old value.
     */
    public boolean getAndSet(int bitIndex) {
        long offset = wordOffset(bitIndex);
        checkCapacity(offset + 8);
        long oldWord = Unsafe.getUnsafe().getLong(ptr + offset);
        Unsafe.getUnsafe().putLong(ptr + offset, oldWord | 1L << bitIndex);
        return (oldWord & 1L << bitIndex) != 0L;
    }

    @Override
    public void reopen() {
        if (ptr == 0) {
            this.size = wordOffset(initialNBits - 1) + 8;
            this.ptr = Unsafe.malloc(size, MemoryTag.NATIVE_BIT_SET);
            clear();
        }
    }

    public void resetCapacity() {
        if (ptr == 0) {
            reopen();
        } else {
            long oldSize = size;
            this.size = wordOffset(initialNBits - 1) + 8;
            this.ptr = Unsafe.realloc(ptr, oldSize, size, MemoryTag.NATIVE_BIT_SET);
            clear();
        }
    }

    /**
     * Sets the given bit to 1.
     */
    public void set(int bitIndex) {
        long offset = wordOffset(bitIndex);
        checkCapacity(offset + 8);
        long oldWord = Unsafe.getUnsafe().getLong(ptr + offset);
        Unsafe.getUnsafe().putLong(ptr + offset, oldWord | 1L << bitIndex);
    }

    private static long wordOffset(int bitIndex) {
        // offsets are 8-byte aligned
        return (bitIndex >> 6) << 3;
    }

    private void checkCapacity(long requiredSize) {
        if (requiredSize > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("bitset capacity overflow");
        }
        if (size < requiredSize) {
            long newSize = Math.max(2 * size, requiredSize);
            ptr = Unsafe.realloc(ptr, size, newSize, MemoryTag.NATIVE_BIT_SET);
            Vect.memset(ptr + size, newSize - size, 0);
            size = newSize;
        }
    }
}
