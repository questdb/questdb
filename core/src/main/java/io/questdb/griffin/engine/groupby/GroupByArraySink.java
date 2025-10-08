/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import static io.questdb.cairo.TableUtils.NULL_LEN;

/**
 * Specialized flyweight array sink used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | totalSize |   dim0  |  dim1  | ... |     dimN-1     | array data |
 * +-----------+---------+--------+-----+----------------+------------+
 * |  4 bytes  |                 N * 4 bytes             |     -      |
 * +-----------+-----------------------------------------+------------+
 * </pre>
 */
public class GroupByArraySink implements Mutable {
    private static final long INT_SIZE = Integer.BYTES;
    private final BorrowedArray borrowedArray = new BorrowedArray();
    private final int type;
    private GroupByAllocator allocator;
    private long ptr;
    private long allocatedSize;

    public GroupByArraySink(int type) {
        this.type = type;
    }

    @Override
    public void clear() {
        ptr = 0;
        allocatedSize = 0;
    }

    public ArrayView getArray() {
        if (ptr == 0) {
            return null;
        }

        int totalSize = Unsafe.getUnsafe().getInt(ptr);
        if (totalSize <= 0) {
            return null;
        }

        int nDims = ColumnType.decodeArrayDimensionality(type);
        int dimLen = nDims * Integer.BYTES;
        long dimPtr = ptr + INT_SIZE;
        borrowedArray.of(type, dimPtr, dimPtr + dimLen, totalSize - dimLen);

        return borrowedArray;
    }

    public GroupByArraySink of(long ptr) {
        this.ptr = ptr;

        if (ptr != 0) {
            int totalSize = Unsafe.getUnsafe().getInt(ptr);
            this.allocatedSize = INT_SIZE + totalSize;
        } else {
            this.allocatedSize = 0;
        }

        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void put(ArrayView array) {
        if (array == null || array.isNull()) {
            putNull();
            return;
        }

        long requiredSize = calculateRequiredSize(array);
        ensureCapacity(requiredSize);

        long offset = putHeader(requiredSize);
        offset = putShape(array, offset);
        putData(array, offset);
    }

    private long calculateRequiredSize(ArrayView array) {
        int nDims = array.getDimCount();
        int elemSize = ColumnType.sizeOf(array.getElemType());
        return INT_SIZE + nDims * INT_SIZE + (long) array.getFlatViewLength() * elemSize;
    }

    private long putHeader(long requiredSize) {
        int totalSize = (int) (requiredSize - INT_SIZE);
        Unsafe.getUnsafe().putInt(ptr, totalSize);
        return ptr + Integer.BYTES;
    }

    private void putNull() {
        ensureCapacity(INT_SIZE);
        Unsafe.getUnsafe().putInt(ptr, NULL_LEN);
    }

    private long putShape(ArrayView array, long offset) {
        int nDims = array.getDimCount();
        for (int i = 0; i < nDims; i++) {
            long dimPos = offset + i * INT_SIZE;
            Unsafe.getUnsafe().putInt(dimPos, array.getDimLen(i));
        }
        return offset + nDims * INT_SIZE;
    }

    private void putData(ArrayView array, long offset) {
        int elemSize = ColumnType.sizeOf(array.getElemType());
        long srcOffset = array.getFlatViewOffset();
        long length = array.getFlatViewLength();

        long srcPtr = array.borrowedFlatView().ptr() + srcOffset * elemSize;
        long dataSize = length * elemSize;
        Vect.memcpy(offset, srcPtr, dataSize);
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    private void ensureCapacity(long requiredSize) {
        if (ptr == 0) {
            ptr = allocator.malloc(requiredSize);
            allocatedSize = requiredSize;
        } else if (requiredSize > allocatedSize) {
            ptr = allocator.realloc(ptr, allocatedSize, requiredSize);
            allocatedSize = requiredSize;
        }
    }
}
