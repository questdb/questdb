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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized flyweight used in some {@link io.questdb.griffin.engine.functions.GroupByFunction}s. Unlike sinks
 * this holder cannot append to the underlying buffer. When you set a value then any previous value is cleared.
 * <p>
 * It can hold either a pointer to a UTF-8 sequence or a UTF-8 sequence itself. When a sequence to be held is
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
 * | capacity (in bytes) |  size (in bytes)  | is ascii | byte array or pointer |
 * +---------------------+-------------------+----------+-----------------------+
 * |       4 bytes       |      4 bytes      | 1 byte   |           -           |
 * +---------------------+-------------------+----------+-----------------------+
 * </pre>
 */
public class StableAwareUtf8StringHolder implements Utf8Sequence {
    private static final long HEADER_SIZE = 2 * Integer.BYTES + Byte.BYTES;
    private static final long IS_ASCII_OFFSET = 2 * Integer.BYTES;
    private static final int MIN_CAPACITY = 8; // 8 bytes = enough to store a pointer
    private static final long SIZE_OFFSET = Integer.BYTES;
    private GroupByAllocator allocator;
    private AsciiCharSequence asciiCharSequence;
    private boolean direct;
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
        if (direct) {
            // we could cache the direct pointer, but then we would need to invalidate it when the pointer changes
            // and we assume of() is called more frequently than charAt()
            long directPtr = Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE);
            assert directPtr != 0;
            return Unsafe.getUnsafe().getByte(directPtr + index);
        } else {
            return Unsafe.getUnsafe().getByte(ptr + HEADER_SIZE + index);
        }
    }

    public void clearAndSet(@Nullable Utf8Sequence us) {
        clear();
        if (us == null) {
            return;
        }
        if (us.isStable()) {
            direct = true;
            checkCapacity(8); // pointer is 8 bytes
            Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, us.ptr());
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, us.size());
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, us.isAscii());
        } else {
            int thatSize = us.size();
            checkCapacity(thatSize);
            long lo = ptr + HEADER_SIZE;
            us.writeTo(lo, 0, thatSize);
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, thatSize);
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, us.isAscii());
        }
    }

    public long colouredPtr() {
        return ptr | (direct ? 0x8000000000000000L : 0);
    }

    @Override
    public boolean isAscii() {
        return ptr == 0 || Unsafe.getUnsafe().getBoolean(null, ptr + IS_ASCII_OFFSET);
    }

    public StableAwareUtf8StringHolder of(long colouredPtr) {
        // clear the top bit
        this.ptr = colouredPtr & 0x7FFFFFFFFFFFFFFFL;
        // extract the top bit
        this.direct = (colouredPtr & 0x8000000000000000L) != 0;
        return this;
    }

    @Override
    public long ptr() {
        return ptr;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    private int capacity() {
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
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, true);
        } else {
            ptr = allocator.realloc(ptr, capacity + HEADER_SIZE, newSize);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }

        assert ptr != 0;
        // the highest bit is not set
        assert (ptr & 0x8000000000000000L) == 0;
    }

    private void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putBoolean(null, ptr + IS_ASCII_OFFSET, true);
            direct = false;
        }
    }
}
