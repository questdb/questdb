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

package io.questdb.cairo.arr;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable array that owns its backing native memory.
 */
public final class DirectArray extends MutableArray implements Mutable {
    private static final long DOUBLE_BYTES = 8;
    private static final long LONG_BYTES = 8;
    private static final int MEM_TAG = MemoryTag.NATIVE_ND_ARRAY;
    private final CairoConfiguration configuration;
    private final FlatViewMemory flatViewMemory = new FlatViewMemory();
    private long ptr = 0;
    private long size;

    public DirectArray(CairoConfiguration configuration) {
        this.flatView = new BorrowedFlatArrayView();
        this.configuration = configuration;
    }

    public DirectArray() {
        this.flatView = new BorrowedFlatArrayView();
        this.configuration = null;
    }

    public void applyShape() {
        applyShape(-1);
    }

    public void applyShape(int errorPosition) {
        short elemType = getElemType();
        int maxArrayElementCount = configuration != null ? configuration.maxArrayElementCount() :
                Integer.MAX_VALUE >> ColumnType.pow2SizeOf(elemType);
        resetToDefaultStrides(maxArrayElementCount, errorPosition);
        int byteSize = flatViewLength << ColumnType.pow2SizeOf(elemType);
        ensureSize(byteSize);
        borrowedFlatView().of(ptr, elemType, flatViewLength);
    }

    @Override
    public void clear() {
        strides.clear();
        flatViewLength = 0;
        borrowedFlatView().reset();
        for (int n = shape.size(), i = 0; i < n; i++) {
            shape.set(i, 0);
        }
    }

    @Override
    public void close() {
        type = ColumnType.UNDEFINED;
        ptr = Unsafe.free(ptr, size, MEM_TAG);
        flatViewLength = 0;
        size = 0;
        shape.clear();
        strides.clear();
        assert ptr == 0;
    }

    /**
     * Copies the type and shape from the source array, and then calls
     * {@link #startMemoryA()}. The returned {@code MemoryA} is ready to be populated
     * by as many elements as there are in the source array, of the same type.
     *
     * @param sourceArray the source array
     * @return memory ready to get populated
     */
    public MemoryA copyShapeAndStartMemoryA(ArrayView sourceArray) {
        setType(sourceArray.getType());
        copyShapeFrom(sourceArray);
        applyShape();
        return startMemoryA();
    }

    public IntList getShape() {
        return shape;
    }

    public void ofNull() {
        flatViewLength = 0;
        type = ColumnType.NULL;
        borrowedFlatView().reset();
        shape.clear();
        strides.clear();
    }

    public long ptr() {
        return ptr;
    }

    /**
     * Puts a double to a randomly accessed flat index of the array. If you're populating
     * the whole array, prefer obtaining {@code MemoryA} via {@link #startMemoryA()},
     * and then use {@link MemoryA#putDouble(double)} to append all values in row-major order.
     */
    public void putDouble(int flatIndex, double value) {
        assert getElemType() == ColumnType.DOUBLE : "putting DOUBLE to a non-DOUBLE array";
        assert flatIndex >= 0 : "negative flatIndex";
        assert ptr > 0 : "ptr <= 0";
        long offset = flatIndex * DOUBLE_BYTES;
        assert size >= offset + DOUBLE_BYTES : "size < offset + DOUBLE_BYTES";
        Unsafe.getUnsafe().putDouble(ptr + offset, value);
    }

    /**
     * Puts long value to a randomly accessed flat index of the array. If you're populating
     * the whole array, prefer obtaining {@code MemoryA} via {@link #startMemoryA()},
     * and then use {@link MemoryA#putLong(long)} to append all values in row-major order.
     */
    public void putLong(int flatIndex, long value) {
        assert getElemType() == ColumnType.LONG : "putting LONG to a non-LONG array";
        assert flatIndex >= 0 && flatIndex < flatViewLength : "flatIndex out of bounds";
        long offset = flatIndex * LONG_BYTES;
        assert size >= offset + LONG_BYTES : "size < offset + LONG_BYTES";
        Unsafe.getUnsafe().putLong(ptr + offset, value);
    }

    public MemoryA startMemoryA() {
        flatViewMemory.appendOffset = 0;
        return flatViewMemory;
    }

    private void ensureSize(long requiredSize) {
        if (requiredSize <= size) {
            return;
        }
        if (ptr == 0) {
            ptr = Unsafe.malloc(requiredSize, MEM_TAG);
            Vect.memset(ptr, requiredSize, 0);
            size = requiredSize;
            return;
        }
        long newSize = size;
        while (newSize < requiredSize) {
            newSize = newSize * 3 / 2;
            if (newSize < 0) {
                throw CairoException.nonCritical().put("array capacity overflow");
            }
        }
        ptr = Unsafe.realloc(ptr, size, newSize, MEM_TAG);
        long sizeToInitialize = newSize - size;
        // arrays are grow-only, so we should never shrink
        assert sizeToInitialize > 0 : "shrinking array";
        Vect.memset(ptr + size, sizeToInitialize, 0);
        size = newSize;
    }

    private class FlatViewMemory implements MemoryA {
        long appendOffset;

        @Override
        public void close() {
        }

        @Override
        public long getAppendOffset() {
            return appendOffset;
        }

        @Override
        public long getExtendSegmentSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void jumpTo(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putArray(ArrayView array) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putBin(long from, long len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBlockOfBytes(long from, long len) {
            assert ptr != 0 : "ptr == 0";
            assert len >= 0 : "len < 0";
            assert appendOffset <= size - len : "appending beyond limit";
            Vect.memcpy(ptr + appendOffset, from, len);
            appendOffset += len;
        }

        @Override
        public void putBool(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putChar(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal128(long hi, long lo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal256(long hh, long hl, long lh, long ll) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(double value) {
            assert ptr != 0 : "ptr == 0";
            assert appendOffset <= size - Double.BYTES : "appending beyond limit";
            Unsafe.getUnsafe().putDouble(ptr + appendOffset, value);
            appendOffset += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            assert ptr != 0 : "ptr == 0";
            assert appendOffset <= size - Long.BYTES : "appending beyond limit";
            Unsafe.getUnsafe().putLong(ptr + appendOffset, value);
            appendOffset += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(Long256 value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(@Nullable CharSequence hexString) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(@NotNull CharSequence hexString, int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256Utf8(@Nullable Utf8Sequence hexString) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putNullBin() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putNullStr() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putShort(short value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putStr(CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putStr(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putStr(CharSequence value, int pos, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putStrUtf8(DirectUtf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long putVarchar(@NotNull Utf8Sequence value, int lo, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip(long bytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void zeroMem(int length) {
            throw new UnsupportedOperationException();
        }
    }
}
