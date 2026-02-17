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

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight hash map used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in longs) | size (in longs) | size limit (in longs) | padding | 16 byte bucket array |
 * +---------------------+-----------------+-----------------------+---------+----------------------+
 * |       4 bytes       |     4 bytes     |       4 bytes         | 4 bytes |           -          |
 * +---------------------+-----------------+-----------------------+---------+----------------------+
 *
 * Stores pointers to CharSequences in the `key` field, adding an indirection.
 * </pre>
 */
public class GroupByCharSequenceLongHashMap {
    private static final long HEADER_SIZE = 4 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 2;
    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final GroupByCharSink keyIndexSink;
    private final double loadFactor;
    private final long noEntryValue;
    private final long noKeyValue;
    private final GroupByCharSink spareSink;
    private GroupByAllocator allocator;
    private long mask;
    private long ptr;

    public GroupByCharSequenceLongHashMap(int initialCapacity, double loadFactor, long noKeyValue, long noEntryValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.loadFactor = loadFactor;
        this.noKeyValue = noKeyValue;
        this.noEntryValue = noEntryValue;
        this.keyIndexSink = new GroupByCharSink();
        this.spareSink = new GroupByCharSink();
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public long get(CharSequence key) {
        return getAt(keyIndex(key));
    }

    public void inc(CharSequence key) {
        inc(key, 1);
    }

    public void inc(CharSequence key, long delta) {
        long index = keyIndex(key);
        if (index < 0) {
            incRaw(-index - 1, delta);
        } else {
            putAt(index, key, delta);
        }
    }

    public long keyAt(int index) {
        return index < 0 ? keyAtRaw(-index - 1) : keyAtRaw(index);
    }

    public long mergeAdd(GroupByCharSequenceLongHashMap srcMap) {
        assert srcMap.ptr != this.ptr;
        for (int i = 0; i < srcMap.capacity(); i++) {
            final long kPtr = srcMap.keyAt(i);
            if (kPtr != noKeyValue) {
                spareSink.of(kPtr);
                final long value = srcMap.valueAt(i);
                inc(spareSink, value);
            }
        }
        return ptr;
    }

    public GroupByCharSequenceLongHashMap of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + 16L * initialCapacity);
            zero(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (initialCapacity * loadFactor));
            mask = initialCapacity - 1;
        } else {
            this.ptr = ptr;
            mask = capacity() - 1;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void put(CharSequence key, long value) {
        putAt(keyIndex(key), key, value);
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
        this.keyIndexSink.setAllocator(allocator);
        this.spareSink.setAllocator(allocator);
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    public int sizeLimit() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
    }

    public long valueAt(long index) {
        return index < 0 ? valueAtRaw(-index - 1) : valueAtRaw(index);
    }

    private void incRaw(long index, long delta) {
        setValueAtRaw(index, valueAtRaw(index) + delta);
    }

    private long keyAtRaw(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 16L * index);
    }

    private void putAt(long index, CharSequence key, long value) {
        if (index < 0) {
            setValueAtRaw(-index - 1, value);
        } else {
            keyIndexSink.of(0).put(key); // allocating
            setKeyAtRaw(index, keyIndexSink.ptr());
            setValueAtRaw(index, value);
            int size = size();
            int sizeLimit = sizeLimit();
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
            if (size >= sizeLimit) {
                rehash(capacity() << 1, sizeLimit << 1);
            }
        }
    }

    private void rehash(int newCapacity, int newSizeLimit) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("char sequence long long hash map capacity overflow");
        }

        final int oldSize = size();
        final int oldCapacity = capacity();

        long oldPtr = ptr;
        ptr = allocator.malloc(16L * newCapacity + HEADER_SIZE);
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
        mask = newCapacity - 1;

        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + 16L * oldCapacity; p < lim; p += 16L) {
            final long kPtr = Unsafe.getUnsafe().getLong(p);
            if (kPtr != noKeyValue) {
                spareSink.of(kPtr);
                final long index = keyIndex(spareSink);
                setKeyAtRaw(index, kPtr);
                final long value = Unsafe.getUnsafe().getLong(p + 8L);
                setValueAtRaw(index, value);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + 16L * oldCapacity);
    }

    private void setKeyAtRaw(long index, long kPtr) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index, kPtr);
    }

    private void setValueAtRaw(long index, long value) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index + 8L, value);
    }

    private long valueAtRaw(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 16L * index + 8L);
    }

    private void zero(long ptr, int cap) {
        if (noKeyValue == 0) {
            // Vectorized fast path for zero default value.
            Vect.memset(ptr + HEADER_SIZE, 16L * cap, 0);
        } else {
            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 16L * cap; p < lim; p += 16L) {
                Unsafe.getUnsafe().putLong(p, noKeyValue);
            }
        }
    }

    long getAt(long index) {
        if (index < 0) {
            return valueAtRaw(-index - 1);
        } else {
            return noEntryValue;
        }
    }

    long keyIndex(CharSequence key) {
        long hashCode = Chars.hashCode(key);
        long index = hashCode & mask;
        long kPtr = keyAtRaw(index);
        if (kPtr == noKeyValue) {
            return index;
        }
        keyIndexSink.of(kPtr);
        if (Chars.equals(key, keyIndexSink)) {
            return -index - 1;
        }
        return probe(key, index);
    }

    long probe(CharSequence key, long index) {
        final long index0 = index;
        do {
            index = (index + 1) & mask;
            long kPtr = keyAtRaw(index);
            if (kPtr == noKeyValue) {
                return index;
            }
            keyIndexSink.of(kPtr);
            if (Chars.equals(key, keyIndexSink)) {
                return -index - 1;
            }
        } while (index != index0);

        throw CairoException.critical(0).put("corrupt char sequence long long hash map");
    }
}