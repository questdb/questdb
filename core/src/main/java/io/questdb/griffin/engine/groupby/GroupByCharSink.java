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

import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized flyweight char sink used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocatorImpl} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in chars) | length (in chars) | char array |
 * +---------------------+-------------------+------------+
 * |       4 bytes       |      4 bytes      |     -      |
 * +---------------------+-------------------+------------+
 * </pre>
 */
public class GroupByCharSink implements Utf16Sink, CharSequence, Mutable {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long LEN_OFFSET = Integer.BYTES;
    private static final int MIN_CAPACITY = 16;
    private GroupByAllocator allocator;
    private long ptr;
    private boolean direct;

    /**
     * Returns capacity in chars.
     */
    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    @Override
    public char charAt(int index) {
        if (direct) {
            long directPtr = Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE);
            assert directPtr != 0;
            assert index < length();
            return Unsafe.getUnsafe().getChar(directPtr + 2L * index);
        } else {
            return Unsafe.getUnsafe().getChar(ptr + HEADER_SIZE + 2L * index);
        }
    }

    @Override
    public void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, 0);
            direct = false;
        }
    }

    @Override
    public int length() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + LEN_OFFSET) : 0;
    }

    public GroupByCharSink of(long ptr) {
        // clear the top bit
        this.ptr = ptr & 0x7FFFFFFFFFFFFFFFL;
        this.direct = (ptr & 0x8000000000000000L) != 0;
        return this;
    }

    public long ptr() {
        return ptr | (direct ? 0x8000000000000000L : 0);
    }

    @Override
    public GroupByCharSink put(@Nullable Utf8Sequence us) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GroupByCharSink put(@Nullable CharSequence cs) {
        if (cs instanceof DirectCharSequence && length() == 0) {
            direct = true;
            DirectCharSequence dcs = (DirectCharSequence) cs;
            checkCapacity(4); // pointer is 8 bytes = 4 chars
            Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, dcs.ptr());
            Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, dcs.length());
        } else if (cs != null) {
            copyFromDirect();
            int thatLen = cs.length();
            checkCapacity(thatLen);
            int thisLen = length();
            long lo = ptr + HEADER_SIZE + 2L * thisLen;
            for (int i = 0; i < thatLen; i++) {
                Unsafe.getUnsafe().putChar(lo + 2L * i, cs.charAt(i));
            }
            Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, thisLen + thatLen);
        }
        return this;
    }

    private void copyFromDirect() {
        if (!direct) {
            return;
        }
        direct = false;
        int len = length();
        if (len == 0) {
            return;
        }
        long directPtr = Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE);
        assert directPtr != 0;
        checkCapacity(0); // check we have enough space to transfer only the current length, no extra chars needed
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(ptr + HEADER_SIZE + 2L * i, Unsafe.getUnsafe().getChar(directPtr + 2L * i));
        }
    }

    @Override
    public GroupByCharSink put(char c) {
        copyFromDirect();
        checkCapacity(1);
        int len = length();
        long lo = ptr + HEADER_SIZE + 2L * len;
        Unsafe.getUnsafe().putChar(lo, c);
        Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, len + 1);
        return this;
    }

    @Override
    public GroupByCharSink putAscii(char c) {
        return put(c);
    }

    @Override
    public GroupByCharSink putAscii(@Nullable CharSequence cs) {
        return put(cs);
    }

    @Override
    public GroupByCharSink putUtf8(long lo, long hi) {
        throw new UnsupportedOperationException();
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public @NotNull CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public String toString() {
        return AbstractCharSequence.getString(this);
    }

    private void checkCapacity(int nChars) {
        int capacity = capacity();
        int len = length();
        int requiredCapacity = len + nChars;
        if (capacity > 0 && requiredCapacity <= capacity) {
            return;
        }
        int newCapacity = Math.max(capacity, MIN_CAPACITY);
        while (newCapacity < requiredCapacity) {
            newCapacity *= 2;
        }
        long newSize = ((long) newCapacity << 1) + HEADER_SIZE;
        if (ptr == 0) {
            ptr = allocator.malloc(newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
            Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, 0);
        } else {
            ptr = allocator.realloc(ptr, ((long) capacity << 1) + HEADER_SIZE, newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }

        assert ptr != 0 || nChars == 0;
        // the highest bit is not set
        assert (ptr & 0x8000000000000000L) == 0;
    }
}
