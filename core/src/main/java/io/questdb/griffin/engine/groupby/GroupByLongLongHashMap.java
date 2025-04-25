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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized flyweight hash set used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in longs) | size (in longs) | size limit (in longs) | padding | long array |
 * +---------------------+-----------------+-----------------------+---------+------------+
 * |       4 bytes       |     4 bytes     |       4 bytes         | 4 bytes |     -      |
 * +---------------------+-----------------+-----------------------+---------+------------+
 * </pre>
 */
public class GroupByLongLongHashMap {
    private static final long HEADER_SIZE = 4 * Integer.BYTES;
    private static final int MIN_INITIAL_CAPACITY = 2;
    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private final double loadFactor;
    private final long noKeyValue;
    private GroupByAllocator allocator;
    private long mask;
    private long ptr;

    public GroupByLongLongHashMap(int initialCapacity, double loadFactor, long noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
        this.loadFactor = loadFactor;
        this.noKeyValue = noKeyValue;
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public long get(long key) {
        return valueAt(keyIndex(key));
    }

    public long keyAt(long index) {
        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 16L * index);
    }

    public long keyIndex(long key) {
        long hashCode = Hash.hashLong64(key);
        long index = hashCode & mask;
        long k = keyAt(index);
        if (k == noKeyValue) {
            return index;
        }
        if (key == k) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public GroupByLongLongHashMap of(long ptr) {
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

    public void put(long key, long value) {
        long index = keyIndex(key);
        putAt(index, key, value);
    }

    public void putAt(long index, long key, long value) {
        if (index > 0) {
            setKeyAt(index, key);
            setValueAt(index, value);
            int size = size();
            int sizeLimit = sizeLimit();
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
            if (size >= sizeLimit) {
                rehash(capacity() << 1, sizeLimit << 1);
            }
        } else {
            if (index != 0) {
                index = -index - 1;
            }
            setValueAt(index, value);
        }
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    public int sizeLimit() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
    }

    private long probe(long key, long index) {
        final long index0 = index;
        do {
            index = (index + 1) & mask;
            long k = keyAt(index);
            if (k == noKeyValue) {
                return index;
            }
            if (key == k) {
                return -index - 1;
            }
        } while (index != index0);

        throw CairoException.critical(0).put("corrupt long hash set");
    }

    private void rehash(int newCapacity, int newSizeLimit) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("long hash set capacity overflow");
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
            final long key = Unsafe.getUnsafe().getLong(p);
            if (key != noKeyValue) {
                final long index = keyIndex(key);
                setKeyAt(index, key);
                final long value = Unsafe.getUnsafe().getLong(p + 8L);
                setValueAt(index, value);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + 16L * oldCapacity);
    }

    private void setKeyAt(long index, long key) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index, key);
    }

    private void setValueAt(long index, long value) {
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index + 8L, value);
    }

    private long valueAt(long index) {
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
}


/// *******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2024 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb.griffin.engine.groupby;
//
//import io.questdb.cairo.CairoException;
//import io.questdb.std.Hash;
//import io.questdb.std.Numbers;
//import io.questdb.std.Unsafe;
//import io.questdb.std.Vect;
//
/// **
// * Specialized flyweight hash set used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
// * <p>
// * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
// * <p>
// * Buffer layout is the following:
// * <pre>
// * | capacity (in buckets) | size (in buckets) | size limit (in buckets) | padding |  array of 16 byte buckets   |
// * +-----------------------+-------------------+-------------------------+---------+-----------------------------+
// * |       4 bytes         |     4 bytes       |       4 bytes           | 4 bytes |  (8 byte key, 8 byte value) |
// * +-----------------------+-------------------+-------------------------+---------+-----------------------------+
// * </pre>
// */
//public class GroupByLongLongHashMap {
//    private static final long HEADER_SIZE = 4 * Integer.BYTES;
//    private static final int MIN_INITIAL_CAPACITY = 2;
//    private static final long SIZE_LIMIT_OFFSET = 2 * Integer.BYTES;
//    private static final long SIZE_OFFSET = Integer.BYTES;
//    private final int initialCapacity;
//    private final double loadFactor;
//    private final long noKeyValue;
//    private GroupByAllocator allocator;
//    private long mask;
//    private long ptr;
//
//    public GroupByLongLongHashMap(int initialCapacity, double loadFactor, long noKeyValue) {
//        if (loadFactor <= 0d || loadFactor >= 1d) {
//            throw new IllegalArgumentException("0 < loadFactor < 1");
//        }
//        this.initialCapacity = Numbers.ceilPow2((int) (Math.max(initialCapacity, MIN_INITIAL_CAPACITY) / loadFactor));
//        this.loadFactor = loadFactor;
//        this.noKeyValue = noKeyValue;
//    }
//
//    public int capacity() {
//        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
//    }
//
//    public long keyAt(long index) {
//        assert index < capacity();
//        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 16L * index);
//    }
//
//    public long keyIndex(long key) {
//        final long hashCode = Hash.hashLong64(key);
//        final long index = hashCode & mask;
//        final long k = keyAt(index);
//        if (k == noKeyValue) {
//            return index;
//        }
//        if (key == k) {
//            return -index - 1;
//        }
//        return probe(key, index);
//    }
//
//    public GroupByLongLongHashMap of(long ptr) {
//        if (ptr == 0) {
//            this.ptr = allocator.malloc(HEADER_SIZE + 16L * initialCapacity);
//            zero(this.ptr, initialCapacity);
//            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
//            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
//            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (initialCapacity * loadFactor));
//            mask = initialCapacity - 1;
//        } else {
//            this.ptr = ptr;
//            mask = capacity() - 1;
//        }
//        return this;
//    }
//
//    public long ptr() {
//        return ptr;
//    }
//
//    public void put(long key, long value) {
//        long index = keyIndex(key);
//        if (index < 0) {
//            setValueAt(index, value);
//        } else {
//            putAt(index, key, value);
//        }
//    }
//
//    public void putAt(long index, long key, long value) {
//        setKeyAt(index, key);
//        setValueAt(index, value);
//        int size = size();
//        int sizeLimit = sizeLimit();
//        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
//        if (size >= sizeLimit) {
//            rehash(capacity() << 1, sizeLimit << 1);
//        }
//    }
//
//    public void resetPtr() {
//        ptr = 0;
//    }
//
//    public void setAllocator(GroupByAllocator allocator) {
//        this.allocator = allocator;
//    }
//
//    public int size() {
//        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
//    }
//
//    public int sizeLimit() {
//        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
//    }
//
//    public long valueAt(long index) {
//        return Unsafe.getUnsafe().getLong(ptr + HEADER_SIZE + 16L * index + 8L);
//    }
//
//    private long probe(long key, long index) {
//        final long index0 = index;
//        do {
//            index = (index + 1) & mask;
//            long k = keyAt(index);
//            if (k == noKeyValue) {
//                return index;
//            }
//            if (key == k) {
//                return -index - 1;
//            }
//        } while (index != index0);
//
//        throw CairoException.critical(0).put("corrupt long long hash map");
//    }
//
//    private void rehash(int newCapacity, int newSizeLimit) {
//        if (newCapacity < 0) {
//            throw CairoException.nonCritical().put("long long hash map capacity overflow");
//        }
//
//        final int oldSize = size();
//        final int oldCapacity = capacity();
//
//        long oldPtr = ptr;
//        ptr = allocator.malloc(HEADER_SIZE + 16L * newCapacity);
//        zero(ptr, newCapacity);
//        Unsafe.getUnsafe().putInt(ptr, newCapacity);
//        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
//        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
//        mask = newCapacity - 1;
//
//        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + 16L * oldCapacity; p < lim; p += 16L) {
//            long key = Unsafe.getUnsafe().getLong(p);
//            if (key != noKeyValue) {
//                long index = keyIndex(key);
//                setKeyAt(index, key);
//                long value = Unsafe.getUnsafe().getLong(p + 8L);
//                setValueAt(index, value);
//            }
//        }
//
//        allocator.free(oldPtr, HEADER_SIZE + 16L * oldCapacity);
//    }
//
//    private void setKeyAt(long index, long key) {
//        assert index < capacity();
//        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index, key);
//    }
//
//    private void setValueAt(long index, long value) {
//        assert index < capacity();
//        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + 16L * index + 8L, value);
//    }
//
//    private void zero(long ptr, int cap) {
//        if (noKeyValue == 0) {
//            // Vectorized fast path for zero default value.
//            Vect.memset(ptr + HEADER_SIZE, 16L * cap, 0);
//        } else {
//            for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + 16L * cap; p < lim; p += 16L) {
//                Unsafe.getUnsafe().putLong(p, noKeyValue);
//            }
//        }
//    }
//}
