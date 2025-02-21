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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.IntList;

/**
 * A view over a native-memory array. Does not own the backing native memory.
 * The array contents can't be mutated through this view, but the view itself can be.
 * You can change what slice of the underlying flat array it represents, as well as
 * transpose it.
 */
public class BorrowedArrayView implements ArrayView {
    private final IntList shape = new IntList(0);
    private final IntList strides = new IntList(0);
    private FlatArrayView flatView;
    private int flatViewLength;
    private int flatViewOffset;
    // Encoded array type, contains element type class, type precision, and dimensionality
    private int type = ColumnType.UNDEFINED;

    @Override
    public void appendToMem(MemoryA mem) {
        appendToMemRecursive(0, 0, mem);
    }

    public void asSubArrayAt(int index, int argPos) {
        if (index >= getDimLen(0)) {
            throw CairoException.nonCritical().position(argPos)
                    .put("array index out of range [index=").put(index)
                    .put(", length=").put(getDimLen(0)).put(']');
        }
        flatViewOffset += index * strides.get(0);
        shape.removeIndex(0);
        strides.removeIndex(0);
    }

    @Override
    public FlatArrayView flatView() {
        return flatView;
    }

    @Override
    public int getDimCount() {
        return shape.size();
    }

    @Override
    public int getDimLen(int dim) {
        return shape.get(dim);
    }

    @Override
    public int getFlatViewLength() {
        return flatViewLength;
    }

    @Override
    public int getFlatViewOffset() {
        return flatViewOffset;
    }

    @Override
    public int getStride(int dimension) {
        return strides.get(dimension);
    }

    @Override
    public int getType() {
        return type;
    }

    /**
     * The array is a typeless zero-dimensional array.
     * <p>
     * This maps to the <code>NULL</code> value in an array column.
     */
    public boolean isNull() {
        return type == ColumnType.NULL;
    }

    public void of(ArrayView other) {
        this.type = other.getType();
        this.flatView = other.flatView();
        this.flatViewOffset = other.getFlatViewOffset();
        this.flatViewLength = other.getFlatViewLength();
        shape.clear();
        strides.clear();
        int nDims = other.getDimCount();
        for (int i = 0; i < nDims; i++) {
            shape.add(other.getDimLen(i));
            strides.add(other.getStride(i));
        }
    }

    /**
     * Set to a null array.
     */
    public void ofNull() {
        reset();
        type = ColumnType.NULL;
    }

    /**
     * Reset to an invalid array.
     */
    public void reset() {
        this.type = ColumnType.UNDEFINED;
        this.shape.clear();
        this.strides.clear();
        this.flatViewOffset = 0;
        this.flatViewLength = 0;
    }

    public void sliceOneDim(int dim, int left, int right, int argPos) {
        if (dim < 0 || dim >= getDimCount()) {
            throw CairoException.nonCritical().position(argPos)
                    .put("array dimension doesn't exist [dim=").put(dim)
                    .put(", nDims=").put(getDimCount()).put(']');
        }
        int dimLen = getDimLen(dim);
        if (left >= right) {
            throw CairoException.nonCritical()
                    .position(argPos)
                    .put("lower bound is not less than upper bound [dim=").put(dim)
                    .put(", lowerBound=").put(left)
                    .put(", upperBound=").put(right)
                    .put(']');
        }
        if (left < 0 || left >= dimLen || right > dimLen) {
            throw CairoException.nonCritical()
                    .position(argPos)
                    .put("array slice bounds out of range [dim=").put(dim)
                    .put(", dimLen=").put(dimLen)
                    .put(", lowerBound=").put(left)
                    .put(", upperBound=").put(right)
                    .put(']');
        }
        flatViewOffset += left * getStride(dim);
        shape.set(dim, right - left);
    }

    public void transpose() {
        strides.reverse();
        shape.reverse();
    }

    private void appendToMemRecursive(int dim, int flatIndex, MemoryA mem) {
        final int count = getDimLen(dim);
        final int stride = getStride(dim);
        final boolean atDeepestDim = dim == getDimCount() - 1;
        if (atDeepestDim) {
            for (int i = 0; i < count; i++) {
                mem.putDouble(flatView.getDouble(flatViewOffset + flatIndex));
                flatIndex += stride;
            }
        } else {
            for (int i = 0; i < count; i++) {
                appendToMemRecursive(dim + 1, flatIndex, mem);
                flatIndex += stride;
            }
        }
    }
}
