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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;

import java.util.Arrays;

public class DirectArrayView implements ArrayView, ArraySink, Mutable, QuietCloseable {
    private static final int MEM_TAG = MemoryTag.NATIVE_ND_ARRAY;
    private int capacity;
    private long mem = 0;
    private int offset = 0;
    private int[] shape;
    private int size = 0;
    private int[] strides;
    private int type = ColumnType.UNDEFINED;

    public static void main(String[] args) {
        Rnd rnd = new Rnd();
        try (DirectArrayView arrayView = new DirectArrayView()) {
            for (int i = 0; i < 1000; i++) {
                arrayView.clear();
                rnd.nextDoubleArray(2, arrayView, 0, 8);
            }
        }
    }

    @Override
    public int getFlatElemCount() {
        return size >> ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(type));
    }

    @Override
    public void appendWithDefaultStrides(MemoryA mem) {
        mem.putBlockOfBytes(this.mem, size);
    }

    @Override
    public void clear() {
        this.size = 0;
        this.type = ColumnType.UNDEFINED;
        this.offset = 0;
        if (shape != null) {
            Arrays.fill(this.shape, 0);
        }
        if (strides != null) {
            Arrays.fill(this.strides, 0);
        }
    }

    @Override
    public void close() {
        this.type = ColumnType.UNDEFINED;
        this.mem = Unsafe.free(mem, capacity, MEM_TAG);
        this.size = 0;
        this.capacity = 0;
        assert this.mem == 0;
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
    public double getDoubleAtFlatIndex(int flatIndex) {
        return Unsafe.getUnsafe().getDouble(mem + offset + flatIndex * 8L);
    }

    @Override
    public long getLongAtFlatIndex(int flatIndex) {
        return Unsafe.getUnsafe().getLong(mem + offset + flatIndex * 8L);
    }

    @Override
    public int getStride(int dimension) {
        return strides[dimension];
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void prepareFlatArray() {
        int size = 1;
        int shl = ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(type));
        for (int i = 0, n = shape.length; i < n; i++) {
            size *= (shape[i] << shl);
            assert size > 0;
        }

        if (mem == 0) {
            this.mem = Unsafe.malloc(size, MEM_TAG);
            this.size = size;
            this.capacity = size;
        } else if (size <= capacity) {
            this.size = size;
        } else {
            this.mem = Unsafe.realloc(this.mem, this.capacity, size, MEM_TAG);
            this.size = size;
            this.capacity = size;
        }

        this.strides = new int[shape.length];
        int stride = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            strides[i] = stride;
            stride *= shape[i];
        }
    }

    @Override
    public void putDouble(int flatIndex, double value) {
        assert mem != 0;
        assert offset + flatIndex * 8L < size;
        assert flatIndex > -1;
        Unsafe.getUnsafe().putDouble(mem + offset + flatIndex * 8L, value);
    }

    @Override
    public void setDimLen(int dimension, int length) {
        shape[dimension] = length;
    }

    @Override
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public void setType(int type) {
        this.type = type;
        shape = new int[ColumnType.decodeArrayDimensionality(type)];
    }
}
