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

package io.questdb.griffin.engine.groupby.hyperloglog;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * This represents a sparse version of HyperLogLog++. This implementation diverges from the proposed method in the
 * paper by utilizing a specialized hash set instead of a sorted list and temporary buffer. Consequently, it trades
 * higher memory consumption for faster inserts.
 * <p>
 * The memory layout is as follows:
 * <pre>
 * |  type  | cached cardinality | capacity | size limit |   size  | padding |      sparse set      |
 * +--------+--------------------+----------+------------+---------+---------+----------------------+
 * | 1 byte |       8 bytes      | 4 bytes  |   4 bytes  | 4 bytes | 3 bytes | (capacity * 4) bytes |
 * +-----------------------------+----------+------------+---------+---------+----------------------+
 * </pre>
 * <p>
 * The first two fields (type and cached cardinality) are used by {@link HyperLogLog}.
 */
public class HyperLogLogSparseRepresentation {
    private static final long CAPACITY_OFFSET = Byte.BYTES + Long.BYTES;
    private static final long HEADER_SIZE = Byte.BYTES + Long.BYTES + 3 * Integer.BYTES + 3;
    private static final int INITIAL_CAPACITY = 16;
    private static final double LOAD_FACTOR = 0.7;
    private static final int NO_ENTRY_VALUE = 0;
    private static final long SIZE_LIMIT_OFFSET = Byte.BYTES + Long.BYTES + 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Byte.BYTES + Long.BYTES + Integer.BYTES;
    private static final int SPARSE_PRECISION = 25;
    private static final int SPARSE_REGISTER_COUNT = 1 << SPARSE_PRECISION;
    private final int densePrecision;
    private final long encodeMask;
    private final long leadingZerosMask;
    private final int sparseSetMaxSize;
    private GroupByAllocator allocator;
    private int moduloMask;
    private long ptr;

    public HyperLogLogSparseRepresentation(int densePrecision) {
        this.sparseSetMaxSize = calculateSparseSetMaxSize(densePrecision);
        this.densePrecision = densePrecision;
        this.leadingZerosMask = 1L << (densePrecision - 1);
        this.encodeMask = (0x8000000000000000L >> (SPARSE_PRECISION - densePrecision - 1)) >>> (densePrecision);
    }

    public static int calculateSparseSetMaxSize(int densePrecision) {
        long denseSizeInBytes = HyperLogLogDenseRepresentation.calculateSizeInBytes(densePrecision);
        // We divide the number of bytes by 4 because each element of the sparse set has the size of 4 bytes.
        int maxSparseSetCapacity = (int) ((denseSizeInBytes - HEADER_SIZE) >> 2);
        return Math.max(maxSparseSetCapacity, 0);
    }

    public void add(long hash) {
        // This is a modified version of the encoding proposed in the paper. Potentially, it consumes more memory but
        // reduces the number of branches during decoding.
        //
        // There are two differences compared to the original version:
        // - The least significant bit is set to 0 when the number of leading zeros is expressed as an integer.
        // - The 7 least significant bits are set to 1 when the number of leading zeros can be calculated based
        // on the register index.
        int registerIdx = computeRegisterIndex(hash);
        int value = 0x7F;
        if ((hash & encodeMask) == 0) {
            value = computeNumberOfLeadingZeros(hash) << 1;
        }
        int entry = (registerIdx << 7) | value;
        add(registerIdx, entry);
    }

    public long computeCardinality() {
        return linearCounting(SPARSE_REGISTER_COUNT, (SPARSE_REGISTER_COUNT - size()));
    }

    public void convertToDense(HyperLogLogDenseRepresentation dst) {
        dst.setAllocator(allocator);
        dst.of(0);
        for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + ((long) capacity() << 2); p < lim; p += 4L) {
            int entry = Unsafe.getUnsafe().getInt(p);
            if (entry != NO_ENTRY_VALUE) {
                int idx = decodeDenseIndex(entry);
                byte leadingZeros = (byte) decodeNumberOfLeadingZeros(entry);
                dst.add(idx, leadingZeros);
            }
        }

        allocator.free(ptr, HEADER_SIZE + ((long) capacity() << 2));
    }

    public boolean isFull() {
        return size() >= sparseSetMaxSize;
    }

    public HyperLogLogSparseRepresentation of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + (INITIAL_CAPACITY << 2));
            zero(this.ptr, INITIAL_CAPACITY);
            Unsafe.getUnsafe().putInt(this.ptr + CAPACITY_OFFSET, INITIAL_CAPACITY);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_LIMIT_OFFSET, (int) (INITIAL_CAPACITY * LOAD_FACTOR));
            moduloMask = INITIAL_CAPACITY - 1;
        } else {
            this.ptr = ptr;
            moduloMask = capacity() - 1;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    private static int computeRegisterIndex(long hash) {
        return (int) (hash >>> (Long.SIZE - SPARSE_PRECISION));
    }

    private static int decodeSparseIndex(int entry) {
        return entry >>> 7;
    }

    private void add(int registerIdx, int entry) {
        int index = Integer.hashCode(registerIdx) & moduloMask;
        if (!tryAddAt(index, registerIdx, entry)) {
            index = probe(registerIdx, index);
            tryAddAt(index, registerIdx, entry);
        }
    }

    private void addAt(int index, int entry) {
        setAt(index, entry);
        int size = size();
        int sizeLimit = sizeLimit();
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, ++size);
        if (size >= sizeLimit) {
            rehash(capacity() << 1, sizeLimit << 1);
        }
    }

    private int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET) : 0;
    }

    private int computeNumberOfLeadingZeros(long hash) {
        return Long.numberOfLeadingZeros((hash << densePrecision) | leadingZerosMask) + 1;
    }

    private int decodeDenseIndex(int entry) {
        int sparseIndex = decodeSparseIndex(entry);
        return sparseIndex >>> (SPARSE_PRECISION - densePrecision);
    }

    private int decodeNumberOfLeadingZeros(int entry) {
        if ((entry & 1) == 0) {
            return (entry >>> 1) & 0x3F;
        } else {
            return Integer.numberOfLeadingZeros(entry << densePrecision) + 1;
        }
    }

    private int entryAt(int index) {
        return Unsafe.getUnsafe().getInt(ptr + HEADER_SIZE + ((long) index << 2));
    }

    private long linearCounting(int total, int empty) {
        return Math.round(total * Math.log(total / (double) empty));
    }

    private int probe(int registerIdx, int index) {
        do {
            index = (index + 1) & moduloMask;
            int entry = entryAt(index);
            if (entry == NO_ENTRY_VALUE) {
                return index;
            }
            int currentRegisterIdx = decodeSparseIndex(entry);
            if (registerIdx == currentRegisterIdx) {
                return index;
            }
        } while (true);
    }

    private void rehash(int newCapacity, int newSizeLimit) {
        if (newCapacity < 0) {
            throw CairoException.nonCritical().put("sparse set capacity overflow");
        }

        final int oldCapacity = capacity();
        final byte type = Unsafe.getUnsafe().getByte(ptr);

        long oldPtr = ptr;
        ptr = allocator.malloc(HEADER_SIZE + ((long) newCapacity << 2));
        zero(ptr, newCapacity);
        Unsafe.getUnsafe().putByte(ptr, type);
        Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, newCapacity);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
        Unsafe.getUnsafe().putInt(ptr + SIZE_LIMIT_OFFSET, newSizeLimit);
        moduloMask = newCapacity - 1;

        for (long p = oldPtr + HEADER_SIZE, lim = oldPtr + HEADER_SIZE + ((long) oldCapacity << 2); p < lim; p += 4L) {
            int entry = Unsafe.getUnsafe().getInt(p);
            if (entry != NO_ENTRY_VALUE) {
                int registerIdx = decodeSparseIndex(entry);
                add(registerIdx, entry);
            }
        }

        allocator.free(oldPtr, HEADER_SIZE + ((long) oldCapacity << 2));
    }

    private void setAt(int index, int entry) {
        Unsafe.getUnsafe().putInt(ptr + HEADER_SIZE + ((long) index << 2), entry);
    }

    private int sizeLimit() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_LIMIT_OFFSET) : 0;
    }

    private boolean tryAddAt(int index, int registerIdx, int entry) {
        int currentEntry = entryAt(index);
        if (currentEntry == NO_ENTRY_VALUE) {
            addAt(index, entry);
            return true;
        } else {
            int currentRegisterIdx = decodeSparseIndex(currentEntry);
            if (currentRegisterIdx == registerIdx) {
                if (currentEntry > entry) {
                    setAt(index, entry);
                }
                return true;
            }
        }
        return false;
    }

    private void zero(long ptr, int cap) {
        Vect.memset(ptr + HEADER_SIZE, ((long) cap << 2), 0);
    }

    void copyTo(HyperLogLogSparseRepresentation dst) {
        for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + ((long) capacity() << 2); p < lim; p += 4L) {
            int entry = Unsafe.getUnsafe().getInt(p);
            if (entry != NO_ENTRY_VALUE) {
                int registerIdx = decodeSparseIndex(entry);
                dst.add(registerIdx, entry);
            }
        }
    }

    void copyTo(HyperLogLogDenseRepresentation dst) {
        for (long p = ptr + HEADER_SIZE, lim = ptr + HEADER_SIZE + ((long) capacity() << 2); p < lim; p += 4L) {
            int entry = Unsafe.getUnsafe().getInt(p);
            if (entry != NO_ENTRY_VALUE) {
                int idx = decodeDenseIndex(entry);
                byte leadingZeros = (byte) decodeNumberOfLeadingZeros(entry);
                dst.add(idx, leadingZeros);
            }
        }
    }

    int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }
}
