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
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * Mutable array that owns its backing native memory.
 */
public final class DirectArray extends MutableArray implements Mutable {
    private static final long DOUBLE_BYTES = 8;
    private static final long LONG_BYTES = 8;
    private static final int MEM_TAG = MemoryTag.NATIVE_ND_ARRAY;
    private final CairoConfiguration configuration;
    private long capacity;
    private long ptr = 0;

    public DirectArray(CairoConfiguration configuration) {
        this.flatView = new BorrowedFlatArrayView();
        this.configuration = configuration;
    }

    public DirectArray() {
        this.flatView = new BorrowedFlatArrayView();
        this.configuration = null;
    }

    public void applyShape(int errorPosition) {
        int maxArrayElementCount = configuration != null ? configuration.maxArrayElementCount() :
                Integer.MAX_VALUE >> ColumnType.pow2SizeOf(ColumnType.decodeArrayElementType(this.type));
        resetToDefaultStrides(maxArrayElementCount, errorPosition);
        short elemType = ColumnType.decodeArrayElementType(type);
        int byteSize = flatViewLength << ColumnType.pow2SizeOf(elemType);
        ensureCapacity(byteSize);
        borrowedFlatView().of(ptr, elemType, flatViewLength);
    }

    @Override
    public void clear() {
        strides.clear();
        flatViewLength = 0;
        borrowedFlatView().reset();
        for (int n = getDimCount(), i = 0; i < n; i++) {
            shape.set(i, 0);
        }
    }

    @Override
    public void close() {
        type = ColumnType.UNDEFINED;
        ptr = Unsafe.free(ptr, capacity, MEM_TAG);
        flatViewLength = 0;
        capacity = 0;
        shape.clear();
        strides.clear();
        assert ptr == 0;
    }

    public void ofNull() {
        flatViewLength = 0;
        type = ColumnType.NULL;
        borrowedFlatView().reset();
        shape.clear();
        strides.clear();
    }

    public void putDouble(int flatIndex, double value) {
        assert ColumnType.decodeArrayElementType(type) == ColumnType.DOUBLE : "putting DOUBLE to a non-DOUBLE array";
        assert flatIndex >= 0 : "negative flatIndex";
        long offset = flatIndex * DOUBLE_BYTES;
        ensureCapacity(offset + DOUBLE_BYTES);
        Unsafe.getUnsafe().putDouble(ptr + offset, value);
    }

    public void putLong(int flatIndex, long value) {
        assert ColumnType.decodeArrayElementType(type) == ColumnType.LONG : "putting LONG to a non-LONG array";
        assert flatIndex >= 0 : "negative flatIndex";
        long offset = flatIndex * LONG_BYTES;
        ensureCapacity(offset + LONG_BYTES);
        Unsafe.getUnsafe().putLong(ptr + offset, value);
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
