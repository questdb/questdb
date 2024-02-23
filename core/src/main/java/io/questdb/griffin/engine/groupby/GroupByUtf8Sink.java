/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized flyweight utf8 sink used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in bytes) | size (in bytes) | byte array |
 * +---------------------+-------------------+------------+
 * |       4 bytes       |      4 bytes      |     -      |
 * +---------------------+-------------------+------------+
 * </pre>
 */
public final class GroupByUtf8Sink implements Utf8Sink, Utf8Sequence {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private static final int MIN_CAPACITY = 16;
    private GroupByAllocator allocator;

    private long ptr;

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public byte byteAt(int index) {
        assert ptr != 0;
        return Unsafe.getUnsafe().getByte(ptr + HEADER_SIZE + index);
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
        return this;
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
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
        } else {
            ptr = allocator.realloc(ptr, capacity + HEADER_SIZE, newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }
    }

    @Override
    public Utf8Sink put(byte b) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Utf8Sink putUtf8(long lo, long hi) {
        throw new UnsupportedOperationException("not implemented");
    }


    @Override
    public int size() {
        if (ptr == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET);
    }

    @Override
    public void writeTo(long addr, int lo, int hi) {
        Utf8Sequence.super.writeTo(addr, lo, hi);
    }

    public void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
        }
    }

    public GroupByUtf8Sink of(long ptr) {
        this.ptr = ptr;
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }
}
