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
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Specialized flyweight hash set for UTF-8 sequences used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffers.
 * Uses segregated buffers: one for the hash table structure and one for the string data.
 * This allows the data area to grow independently without rehashing.
 * <p>
 * Main buffer layout:
 * <pre>
 * | capacity | size | size_limit | data_ptr | data_size | data_capacity | slot_array |
 * +----------+------+------------+----------+-----------+---------------+------------+
 * | 4 bytes  | 4B   | 4B         | 8B       | 4B        | 4B            | variable   |
 * +----------+------+------------+----------+-----------+---------------+------------+
 * </pre>
 * <p>
 * Each slot in the slot array is 12 bytes:
 * <pre>
 * | hash_code | data_offset | data_length |
 * +-----------+-------------+-------------+
 * | 4 bytes   | 4 bytes     | 4 bytes     |
 * +-----------+-------------+-------------+
 * </pre>
 * <p>
 * Data buffer contains UTF-8 strings stored contiguously.
 */
public class GroupByVarcharHashSet {
    private static final long CAPACITY_OFFSET = 0;
    private static final long DATA_CAPACITY_OFFSET = 24;  // data buffer capacity
    private static final long DATA_PTR_OFFSET = 12;  // pointer to data buffer
    private static final long DATA_SIZE_OFFSET = 20;  // current data usage
    private static final int EMPTY_SLOT = -1;
    private static final long HEADER_SIZE = 28;
    private static final int INITIAL_DATA_AREA_SIZE = 256;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long SIZE_LIMIT_OFFSET = 8;
    private static final long SIZE_OFFSET = 4;
    private static final int SLOT_DATA_OFFSET = 4;
    private static final int SLOT_HASH_OFFSET = 0;
    private static final int SLOT_LENGTH_OFFSET = 8;
    private static final long SLOT_SIZE = 12;
    private final int initialCapacity;
    private final double loadFactor;
    private GroupByAllocator allocator;
    private long dataCapacity;  // Current data buffer capacity
    private long dataPtr;       // Separate data buffer  
    private DirectUtf8String directUtf8String;
    private long mask;
    private long ptr;           // Main buffer with header + slots

    public GroupByVarcharHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.initialCapacity = Numbers.ceilPow2(Math.max(initialCapacity, MIN_INITIAL_CAPACITY));
        this.loadFactor = loadFactor;
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key key to be added.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(Utf8Sequence key) {
        long index = keyIndex(key);
        if (index < 0) {
            return false;
        }
        addAt(index, key);
        return true;
    }

    public void addAt(long index, Utf8Sequence key) {
        int keyLen = key.size();
        int dataSize = getDataSize();

        if (dataSize + keyLen > dataCapacity) {
            growDataBuffer(keyLen);
        }

        for (int i = 0; i < keyLen; i++) {
            Unsafe.getUnsafe().putByte(dataPtr + dataSize + i, key.byteAt(i));
        }

        long slotAddr = getSlotAddress(index);
        Unsafe.getUnsafe().putInt(slotAddr + SLOT_HASH_OFFSET, hash(key));
        Unsafe.getUnsafe().putInt(slotAddr + SLOT_DATA_OFFSET, dataSize);  // offset within data buffer
        Unsafe.getUnsafe().putInt(slotAddr + SLOT_LENGTH_OFFSET, keyLen);

        int size = size();
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
        Unsafe.getUnsafe().putInt(ptr + DATA_SIZE_OFFSET, dataSize + keyLen);

        if (size >= sizeLimit()) {
            rehash();
        }
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET) : 0;
    }

    /**
     * Finds the index of a key in the hash set.
     *
     * @param key the key to search for
     * @return non-negative index if key is not in the set (can be used with addAt),
     * or negative value (-index - 1) if key exists
     */
    public long keyIndex(Utf8Sequence key) {
        int hash = hash(key);
        long idealIndex = hash & mask;
        long slotAddr = getSlotAddress(idealIndex);

        int storedHash = Unsafe.getUnsafe().getInt(slotAddr + SLOT_HASH_OFFSET);
        int offset = Unsafe.getUnsafe().getInt(slotAddr + SLOT_DATA_OFFSET);

        if (offset == EMPTY_SLOT) {
            return idealIndex;
        }

        if (storedHash == hash) {
            int length = Unsafe.getUnsafe().getInt(slotAddr + SLOT_LENGTH_OFFSET);
            if (dataEqualsUtf8(key, dataPtr + offset, length)) {
                return -idealIndex - 1;
            }
        }

        return probe(key, hash, idealIndex);
    }

    public void merge(GroupByVarcharHashSet srcSet) {
        if (srcSet.ptr == 0 || srcSet.size() == 0) {
            return;
        }

        if (directUtf8String == null) {
            if (srcSet.directUtf8String != null) {
                // steal the directUtf8String from srcSet
                directUtf8String = srcSet.directUtf8String;
                srcSet.directUtf8String = null;
            } else {
                directUtf8String = new DirectUtf8String();
            }
        }

        int srcCapacity = srcSet.capacity();
        long srcSlotBase = srcSet.ptr + HEADER_SIZE;

        for (int i = 0; i < srcCapacity; i++) {
            long slotAddr = srcSlotBase + (i * SLOT_SIZE);
            int offset = Unsafe.getUnsafe().getInt(slotAddr + SLOT_DATA_OFFSET);

            if (offset != EMPTY_SLOT) {
                int hash = Unsafe.getUnsafe().getInt(slotAddr + SLOT_HASH_OFFSET);
                int length = Unsafe.getUnsafe().getInt(slotAddr + SLOT_LENGTH_OFFSET);
                long dataAddr = srcSet.dataPtr + offset;

                directUtf8String.of(dataAddr, dataAddr + length);
                long index = keyIndexWithHash(directUtf8String, hash);
                if (index >= 0) {
                    addAt(index, directUtf8String);
                }
            }
        }
    }

    public GroupByVarcharHashSet of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + (SLOT_SIZE * initialCapacity));

            this.dataCapacity = INITIAL_DATA_AREA_SIZE;
            this.dataPtr = allocator.malloc(dataCapacity);

            Unsafe.getUnsafe().putInt(this.ptr + CAPACITY_OFFSET, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (initialCapacity * loadFactor));
            Unsafe.getUnsafe().putLong(this.ptr + DATA_PTR_OFFSET, dataPtr);
            Unsafe.getUnsafe().putInt(this.ptr + DATA_SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + DATA_CAPACITY_OFFSET, (int) dataCapacity);

            clearSlots(this.ptr, initialCapacity);
            mask = initialCapacity - 1;
        } else {
            this.ptr = ptr;
            this.dataPtr = Unsafe.getUnsafe().getLong(ptr + DATA_PTR_OFFSET);
            this.dataCapacity = Unsafe.getUnsafe().getInt(ptr + DATA_CAPACITY_OFFSET);
            mask = capacity() - 1;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void resetPtr() {
        ptr = 0;
        dataPtr = 0;
        dataCapacity = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    private static int hash(Utf8Sequence key) {
        return Hash.spread(
                Utf8s.hashCode(key)
        ) & 0x7FFFFFFF; // non-negative hash
    }

    private static int hash2(int hash) {
        // must return an odd number to visit all slots in a power-of-2 sized table
        return (hash ^ (hash >>> 16)) | 1;
    }

    private void clearSlots(long ptr, int capacity) {
        long slotBase = ptr + HEADER_SIZE;
        for (int i = 0; i < capacity; i++) {
            long slotAddr = slotBase + (i * SLOT_SIZE);
            // todo: single put Long
            Unsafe.getUnsafe().putInt(slotAddr + SLOT_DATA_OFFSET, EMPTY_SLOT);
            Unsafe.getUnsafe().putInt(slotAddr + SLOT_HASH_OFFSET, EMPTY_SLOT);
        }
    }

    private boolean dataEqualsUtf8(Utf8Sequence key, long comparePtr, int length) {
        if (key.size() != length) {
            return false;
        }
        long keyPtr = key.ptr();
        if (keyPtr != -1) {
            return Vect.memeq(keyPtr, comparePtr, length);
        }

        for (int i = 0; i < length; i++) {
            if (key.byteAt(i) != Unsafe.getUnsafe().getByte(comparePtr + i)) {
                return false;
            }
        }
        return true;
    }

    private int getDataSize() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + DATA_SIZE_OFFSET) : 0;
    }

    private long getSlotAddress(long index) {
        return ptr + HEADER_SIZE + (index * SLOT_SIZE);
    }

    private void growDataBuffer(int additionalBytes) {
        int dataSize = getDataSize();
        long newCapacity = dataCapacity;
        long requiredCapacity = dataSize + additionalBytes;

        if (newCapacity == 0) {
            newCapacity = Math.max(INITIAL_DATA_AREA_SIZE, requiredCapacity);
        } else {
            while (newCapacity < requiredCapacity) {
                if (newCapacity > Integer.MAX_VALUE / 2) {
                    throw CairoException.nonCritical().put("varchar hash set data capacity overflow [")
                            .put("required=").put(requiredCapacity)
                            .put(", current=").put(dataCapacity).put(']');
                }
                newCapacity = newCapacity * 2;
            }
        }

        dataPtr = allocator.realloc(dataPtr, dataCapacity, newCapacity);
        dataCapacity = newCapacity;

        Unsafe.getUnsafe().putLong(ptr + DATA_PTR_OFFSET, dataPtr);
        Unsafe.getUnsafe().putInt(ptr + DATA_CAPACITY_OFFSET, (int) dataCapacity);
    }

    private long keyIndexWithHash(Utf8Sequence key, int hash) {
        long index = hash & mask;
        long slotAddr = getSlotAddress(index);

        int storedHash = Unsafe.getUnsafe().getInt(slotAddr + SLOT_HASH_OFFSET);
        int offset = Unsafe.getUnsafe().getInt(slotAddr + SLOT_DATA_OFFSET);

        if (offset == EMPTY_SLOT) {
            return index;
        }

        if (storedHash == hash) {
            int length = Unsafe.getUnsafe().getInt(slotAddr + SLOT_LENGTH_OFFSET);
            if (dataEqualsUtf8(key, dataPtr + offset, length)) {
                return -index - 1;
            }
        }

        return probe(key, hash, index);
    }

    private long probe(Utf8Sequence key, int hash, long initialIndex) {
        int step = hash2(hash);
        long index = initialIndex;
        for (; ; ) {
            index = (index + step) & mask;
            long slotAddr = getSlotAddress(index);
            int storedHash = Unsafe.getUnsafe().getInt(slotAddr + SLOT_HASH_OFFSET);
            if (storedHash == EMPTY_SLOT) {
                return index;
            }
            if (storedHash == hash) {
                int length = Unsafe.getUnsafe().getInt(slotAddr + SLOT_LENGTH_OFFSET);
                int offset = Unsafe.getUnsafe().getInt(slotAddr + SLOT_DATA_OFFSET);
                if (dataEqualsUtf8(key, dataPtr + offset, length)) {
                    return -index - 1;
                }
            }
        }
    }

    private void rehash() {
        final int oldCapacity = capacity();
        final int newCapacity = oldCapacity << 1;
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("varchar hash set capacity overflow");
        }

        final int oldSize = size();
        final long oldPtr = ptr;

        int dataSize = getDataSize();

        ptr = allocator.malloc(HEADER_SIZE + (SLOT_SIZE * newCapacity));

        Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, oldSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, (int) (newCapacity * loadFactor));
        Unsafe.getUnsafe().putLong(ptr + DATA_PTR_OFFSET, dataPtr);  // Same data buffer!
        Unsafe.getUnsafe().putInt(ptr + DATA_SIZE_OFFSET, dataSize);
        Unsafe.getUnsafe().putInt(ptr + DATA_CAPACITY_OFFSET, (int) dataCapacity);

        clearSlots(ptr, newCapacity);
        mask = newCapacity - 1;

        long oldSlotBase = oldPtr + HEADER_SIZE;
        for (int i = 0; i < oldCapacity; i++) {
            long oldSlotAddr = oldSlotBase + (i * SLOT_SIZE);
            int offset = Unsafe.getUnsafe().getInt(oldSlotAddr + SLOT_DATA_OFFSET);

            if (offset != EMPTY_SLOT) {
                int hash = Unsafe.getUnsafe().getInt(oldSlotAddr + SLOT_HASH_OFFSET);
                int hash2 = hash2(hash);
                int length = Unsafe.getUnsafe().getInt(oldSlotAddr + SLOT_LENGTH_OFFSET);

                long index = hash & mask;
                long slotAddr = getSlotAddress(index);

                while (Unsafe.getUnsafe().getInt(slotAddr + SLOT_DATA_OFFSET) != EMPTY_SLOT) {
                    index = (index + hash2) & mask;
                    slotAddr = getSlotAddress(index);
                }

                Unsafe.getUnsafe().putInt(slotAddr + SLOT_HASH_OFFSET, hash);
                Unsafe.getUnsafe().putInt(slotAddr + SLOT_DATA_OFFSET, offset);
                Unsafe.getUnsafe().putInt(slotAddr + SLOT_LENGTH_OFFSET, length);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + (SLOT_SIZE * oldCapacity));
    }

    private int sizeLimit() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
    }
}