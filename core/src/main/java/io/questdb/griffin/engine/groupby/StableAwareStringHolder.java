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
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized flyweight used in some {@link io.questdb.griffin.engine.functions.GroupByFunction}s. Unlike sinks
 * this holder cannot append to the underlying buffer. When you set a value then any previous value is cleared.
 * <p>
 * It can hold either a pointer to a char sequence or a char sequence itself. When a sequence to be held is
 * direct, then the Holder stores only a pointer to the char sequence and its length. Otherwise, it stores the char
 * sequence itself.
 * <br>
 * The information about whether a stored sequence is direct or not is not stored in the header of the Holder. Instead, the
 * top bit of the pointer returned by {@link #colouredPtr()} is used to store this information. This is done to save space in the
 * header and to avoid the need to store this information separately. Thus, the value returned by {@link #colouredPtr()} cannot
 * be used as a pointer directly and should be treated as an opaque value.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in chars) | length (in chars) | char array or pointer/
 * +---------------------+-------------------+----------------------+
 * |       4 bytes       |      4 bytes      |           -          |
 * +---------------------+-------------------+----------------------+
 * </pre>
 */
public class StableAwareStringHolder implements CharSequence {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long LEN_OFFSET = Integer.BYTES;
    private static final int MIN_CAPACITY = 4; // 4 chars = 8 bytes = enough to store a pointer
    private GroupByAllocator allocator;
    private boolean direct;
    private long ptr;

    @Override
    public char charAt(int index) {
        if (direct) {
            // we could cache the direct pointer, but then we would need to invalidate it when the pointer changes
            // and we assume of() is called more frequently than charAt()
            long directPtr = Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE);
            assert directPtr != 0;
            return Unsafe.getUnsafe().getChar(directPtr + 2L * index);
        } else {
            return Unsafe.getUnsafe().getChar(ptr + HEADER_SIZE + 2L * index);
        }
    }

    public void clearAndSet(@Nullable CharSequence cs) {
        clear();
        if (cs == null) {
            return;
        }
        if (cs instanceof DirectString) {
            DirectString ds = (DirectString) cs;
            if (ds.isStable()) {
                direct = true;
                checkCapacity(4); // pointer is 8 bytes = 4 chars
                Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, ds.ptr());
                Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, cs.length());
                return;
            }
        }

        int thatLen = cs.length();
        checkCapacity(thatLen);
        long lo = ptr + HEADER_SIZE;
        for (int i = 0; i < thatLen; i++) {
            Unsafe.getUnsafe().putChar(lo + 2L * i, cs.charAt(i));
        }
        Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, thatLen);
    }

    public long colouredPtr() {
        return ptr | (direct ? 0x8000000000000000L : 0);
    }

    @Override
    public int length() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + LEN_OFFSET) : 0;
    }

    public StableAwareStringHolder of(long colouredPtr) {
        // clear the top bit
        this.ptr = colouredPtr & 0x7FFFFFFFFFFFFFFFL;
        // extract the top bit
        this.direct = (colouredPtr & 0x8000000000000000L) != 0;
        return this;
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

    private int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    private void checkCapacity(int nChars) {
        int capacity = capacity();
        if (capacity > 0 && nChars <= capacity) {
            return;
        }
        int newCapacity = Math.max(capacity, MIN_CAPACITY);
        while (newCapacity < nChars) {
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

        assert ptr != 0;
        // the highest bit is not set
        assert (ptr & 0x8000000000000000L) == 0;
    }

    private void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + LEN_OFFSET, 0);
            direct = false;
        }
    }
}
