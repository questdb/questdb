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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.Unsafe;
import io.questdb.std.str.AsciiCharSequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;

/**
 * Specialized flyweight utf8 sink used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in bytes) |  size (in bytes)  | is ascii | byte array |
 * +---------------------+-------------------+----------+------------+
 * |       4 bytes       |      4 bytes      | 1 byte   |  payload   |
 * +---------------------+-------------------+----------+------------+
 * </pre>
 */
public final class GroupByUtf8Sink implements Utf8Sink, Utf8Sequence {
    private static final long HEADER_SIZE = 2 * Integer.BYTES + Byte.BYTES;
    private static final long IS_ASCII_OFFSET = 2 * Integer.BYTES;
    // Must be at least 8 bytes, so that zeroPaddedSixPrefix can safely read long.
    private static final int MIN_CAPACITY = 8;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private GroupByAllocator allocator;
    private AsciiCharSequence asciiCharSequence;
    private long ptr;

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        if (asciiCharSequence == null) {
            asciiCharSequence = new AsciiCharSequence();
        }
        asciiCharSequence.of(this);
        return asciiCharSequence;
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(ptr + HEADER_SIZE + index);
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, true);
            // zero first 8 bytes for zeroPaddedSixPrefix
            Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, 0);
        }
    }

    @Override
    public int intAt(int offset) {
        return Unsafe.getUnsafe().getInt(ptr + HEADER_SIZE + offset);
    }

    @Override
    public boolean isAscii() {
        return ptr == 0 || Unsafe.getUnsafe().getBoolean(null, ptr + IS_ASCII_OFFSET);
    }

    @Override
    public long longAt(int offset) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + offset);
    }

    public GroupByUtf8Sink of(long ptr) {
        this.ptr = ptr;
        return this;
    }

    public long ptr() {
        return ptr;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence seq) {
        if (seq == null) {
            return this;
        }
        int thatSize = seq.size();
        checkCapacity(thatSize);
        int thisSize = size();
        long lo = ptr + HEADER_SIZE + thisSize;
        seq.writeTo(lo, 0, thatSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, thisSize + thatSize);
        Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, seq.isAscii() && isAscii());
        return this;
    }

    @Override
    public Utf8Sink put(byte b) {
        checkCapacity(1);
        Unsafe.getUnsafe().putByte(ptr + HEADER_SIZE + size(), b);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, size() + 1);
        Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, false);
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    @Override
    public void writeTo(long addr, int lo, int hi) {
        if (ptr != 0) {
            Utf8Sequence.super.writeTo(addr, lo, hi);
        }
    }

    @Override
    public long zeroPaddedSixPrefix() {
        // MIN_CAPACITY is 8, so it's safe to read 8 bytes even for short varchars.
        return ptr != 0 ? Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE) & VARCHAR_INLINED_PREFIX_MASK : 0;
    }

    private void checkCapacity(int bytes) {
        int capacity = capacity();
        int len = size();
        int requiredCapacity = len + bytes;
        if (requiredCapacity > 0 && requiredCapacity <= capacity) {
            return;
        }
        int newCapacity = Math.max(capacity, MIN_CAPACITY);
        while (newCapacity < requiredCapacity) {
            newCapacity *= 2;
        }
        long newSize = newCapacity + HEADER_SIZE;
        if (ptr == 0) {
            ptr = allocator.malloc(newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, true);
            // zero first 8 bytes for zeroPaddedSixPrefix
            Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, 0);
        } else {
            ptr = allocator.realloc(ptr, capacity + HEADER_SIZE, newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }
    }
}
