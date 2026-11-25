/*+*****************************************************************************
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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

// A heap-owned copy: bind snapshots can outlive their source and have no close lifecycle.
final class ArraySnapshot extends ArrayView implements FlatArrayView {
    private final long[] numbers;
    private final ObjList<Utf8String> strings;

    ArraySnapshot(ArrayView source) {
        type = source.getType();
        final int nDims = source.getDimCount();
        int[] dimensions = new int[nDims];
        int[] rowMajorStrides = new int[nDims];
        for (int i = 0; i < nDims; i++) {
            dimensions[i] = source.getDimLen(i);
        }
        if (!source.isEmpty()) {
            flatViewLength = computeRowMajorStrides(dimensions, rowMajorStrides);
        }
        for (int i = 0; i < nDims; i++) {
            shape.add(dimensions[i]);
            strides.add(rowMajorStrides[i]);
        }
        flatView = this;
        if (source.getElemType() == ColumnType.VARCHAR) {
            numbers = null;
            strings = new ObjList<>(flatViewLength);
        } else {
            strings = null;
            numbers = new long[flatViewLength];
        }
        if (!isEmpty()) {
            copyValues(source, 0, 0, 0);
        }
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        assert numbers != null;
        for (int i = offset, n = offset + length; i < n; i++) {
            mem.putLong(numbers[i]);
        }
    }

    @Override
    public double getDoubleAtAbsIndex(int elemIndex) {
        return Double.longBitsToDouble(numbers[elemIndex]);
    }

    @Override
    public long getLongAtAbsIndex(int elemIndex) {
        return numbers[elemIndex];
    }

    @Override
    public Utf8Sequence getVarcharAt(int index) {
        return strings.getQuick(index);
    }

    @Override
    public int length() {
        return flatViewLength;
    }

    private int copyValues(ArrayView source, int dim, int sourceIndex, int targetIndex) {
        final int stride = source.getStride(dim);
        for (int i = 0, n = source.getDimLen(dim); i < n; i++, sourceIndex += stride) {
            if (dim + 1 < source.getDimCount()) {
                targetIndex = copyValues(source, dim + 1, sourceIndex, targetIndex);
            } else {
                if (strings != null) {
                    Utf8Sequence value = source.getVarchar(sourceIndex);
                    strings.add(value == null ? null : Utf8String.newInstance(value));
                } else {
                    numbers[targetIndex] = source.getElemType() == ColumnType.DOUBLE
                            ? Double.doubleToRawLongBits(source.getDouble(sourceIndex))
                            : source.getLong(sourceIndex);
                }
                targetIndex++;
            }
        }
        return targetIndex;
    }
}
