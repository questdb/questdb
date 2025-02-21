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
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.Arrays;

/**
 * Mutable array that owns its backing native memory.
 */
public class DirectArray implements ArrayView, ArraySink, Mutable, QuietCloseable {
    private static final long DOUBLE_BYTES = 8;
    private static final int[] EMPTY_INTS = new int[0];
    private static final long LONG_BYTES = 8;
    private static final int MEM_TAG = MemoryTag.NATIVE_ND_ARRAY;
    private final CairoConfiguration configuration;
    private final BorrowedFlatArrayView flatView = new BorrowedFlatArrayView();
    private long capacity;
    private int flatViewLength = 0;
    private int maxArrayElementCount;
    private long ptr = 0;
    private int[] shape;
    private int[] strides;
    private int type = ColumnType.UNDEFINED;

    public DirectArray(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.maxArrayElementCount = configuration.maxArrayElementCount();
    }

    public DirectArray(int maxArrayElementCount) {
        this.configuration = null;
        this.maxArrayElementCount = maxArrayElementCount;
    }

    @Override
    public void appendToMem(MemoryA mem) {
        flatView.appendToMem(mem);
    }

    @Override
    public void applyShape(int errorPosition) {
        assert strides.length == shape.length;

        int maxArrayElementCount = configuration != null ? configuration.maxArrayElementCount() : this.maxArrayElementCount;
        int flatElemCount = 1;
        for (int i = 0, n = shape.length; i < n; i++) {
            flatElemCount *= shape[i];
            if (flatElemCount > maxArrayElementCount) {
                throw CairoException.nonCritical().position(errorPosition)
                        .put("resulting array is too large [elementCount=").put(flatElemCount)
                        .put(", dimensionsLeft=").put(n - i - 1)
                        .put(", max=").put(maxArrayElementCount)
                        .put(']');
            }
            assert flatElemCount > 0;
        }
        int byteSize = flatElemCount << ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(type));
        ensureCapacity(byteSize);
        this.flatViewLength = flatElemCount;
        flatView.of(ptr, byteSize);

        int stride = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            strides[i] = stride;
            stride *= shape[i];
        }
    }

    @Override
    public void clear() {
        flatViewLength = 0;
        flatView.reset();
        if (shape != null) {
            Arrays.fill(shape, 0);
            Arrays.fill(strides, 0);
        }
    }

    @Override
    public void close() {
        type = ColumnType.UNDEFINED;
        ptr = Unsafe.free(ptr, capacity, MEM_TAG);
        flatViewLength = 0;
        capacity = 0;
        shape = null;
        strides = null;
        assert ptr == 0;
    }

    @Override
    public FlatArrayView flatView() {
        return flatView;
    }

    @Override
    public int getDimCount() {
        return shape.length;
    }

    @Override
    public int getDimLen(int dimension) {
        return shape[dimension];
    }

    @Override
    public int getFlatViewLength() {
        return flatViewLength;
    }

    public int[] getShape() {
        return shape;
    }

    @Override
    public int getStride(int dimension) {
        return strides[dimension];
    }

    @Override
    public int getType() {
        return type;
    }

    public void ofNull() {
        flatViewLength = 0;
        type = ColumnType.UNDEFINED;
        flatView.reset();
        shape = EMPTY_INTS;
        strides = EMPTY_INTS;
    }

    @Override
    public void putByte(int flatIndex, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putDouble(int flatIndex, double value) {
        assert ColumnType.decodeArrayElementType(type) == ColumnType.DOUBLE : "putting DOUBLE to a non-DOUBLE array";
        assert flatIndex >= 0 : "negative flatIndex";
        long offset = flatIndex * DOUBLE_BYTES;
        ensureCapacity(offset + DOUBLE_BYTES);
        Unsafe.getUnsafe().putDouble(ptr + offset, value);
    }

    @Override
    public void putFloat(int flatIndex, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt(int flatIndex, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong(int flatIndex, long value) {
        assert ColumnType.decodeArrayElementType(type) == ColumnType.LONG : "putting LONG to a non-LONG array";
        assert flatIndex >= 0 : "negative flatIndex";
        long offset = flatIndex * LONG_BYTES;
        ensureCapacity(offset + LONG_BYTES);
        Unsafe.getUnsafe().putLong(ptr + offset, value);
    }

    @Override
    public void putShort(int flatIndex, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDimLen(int dimension, int length) {
        shape[dimension] = length;
    }

    @Override
    public void setType(int encodedType) {
        assert ColumnType.isArray(encodedType);

        int nDims = ColumnType.decodeArrayDimensionality(encodedType);
        this.type = encodedType;
        if (shape == null || shape.length != nDims) {
            shape = new int[nDims];
            strides = new int[nDims];
        } else {
            Arrays.fill(shape, 0);
            Arrays.fill(strides, 0);
        }
    }

    private void ensureCapacity(long requiredCapacity) {
        if (ptr == 0) {
            ptr = Unsafe.malloc(requiredCapacity, MEM_TAG);
            capacity = requiredCapacity;
        } else if (capacity < requiredCapacity) {
            long newCapacity = capacity;
            while (newCapacity < requiredCapacity) {
                newCapacity = newCapacity * 3 / 2;
                if (newCapacity < 0) {
                    throw CairoException.nonCritical().put("array capacity overflow");
                }
            }
            ptr = Unsafe.realloc(ptr, capacity, newCapacity, MEM_TAG);
            capacity = newCapacity;
        }
    }
}
