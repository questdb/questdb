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

public class HeapDoubleArray implements ArrayView {
    private final int nDims;
    private final int[] shape;
    private final int[] strides;
    private double[] values;

    public HeapDoubleArray(int nDims) {
        this.nDims = nDims;
        this.shape = new int[nDims];
        this.strides = new int[nDims];
    }

    @Override
    public void appendWithDefaultStrides(MemoryA mem) {
        for (double value : values) {
            mem.putDouble(value);
        }
    }

    public void applyShape() {
        int stride = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            int dimLen = shape[i];
            if (dimLen == 0) {
                throw new IllegalStateException("Zero dimLen at " + i);
            }
            strides[i] = stride;
            stride *= dimLen;
        }
        if (values == null || values.length < stride) {
            values = new double[stride];
        }
    }

    @Override
    public int getDimCount() {
        return nDims;
    }

    @Override
    public int getDimLen(int dim) {
        return shape[dim];
    }

    @Override
    public double getDoubleAtFlatIndex(int flatIndex) {
        return values[flatIndex];
    }

    @Override
    public int getFlatElemCount() {
        int count = 1;
        for (int dimLen : shape) {
            count *= dimLen;
        }
        return count;
    }

    @Override
    public long getLongAtFlatIndex(int flatIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStride(int dimension) {
        return strides[dimension];
    }

    @Override
    public int getType() {
        return ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
    }

    public void setDimLen(int dim, int len) {
        shape[dim] = len;
    }

    public void setDoubleAtFlatIndex(int flatIndex, double value) {
        values[flatIndex] = value;
    }
}
