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
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;

public abstract class ArrayView implements QuietCloseable {
    protected final IntList shape = new IntList();
    protected final IntList strides = new IntList();
    protected FlatArrayView flatView;
    protected int flatViewLength;
    protected int flatViewOffset;
    protected boolean isVanilla;
    protected int type = ColumnType.UNDEFINED;

    public final void appendToMem(MemoryA mem) {
        if (isVanilla) {
            if (flatView instanceof BorrowedFlatArrayView) {
                // Ensure a dedicated, inlineable call site
                ((BorrowedFlatArrayView) flatView).appendToMemFlat(mem);
            } else {
                flatView.appendToMemFlat(mem);
            }
        } else {
            appendToMemRecursive(0, 0, mem);
        }
    }

    @Override
    public void close() {
    }

    /**
     * Returns a flat view over the elements of the N-dimensional array. It contains
     * the values stored in row-major order. For example, for a 4x3x2 array:
     * <pre>
     * {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * </pre>
     * The flat array would contain a flat vector of elements with the numbers
     * <code>[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]</code>.
     */
    public final FlatArrayView flatView() {
        return flatView;
    }

    /**
     * Returns the number of dimensions in this array (i.e., its dimensionality).
     */
    public final int getDimCount() {
        return shape.size();
    }

    /**
     * Returns the number of elements in the given dimension (sub-arrays or leaf values)
     */
    public final int getDimLen(int dimension) {
        assert dimension >= 0 && dimension < shape.size();
        return shape.getQuick(dimension);
    }

    /**
     * Returns the total number of data points (leaf values) in this array.
     */
    public final int getFlatViewLength() {
        return flatViewLength;
    }

    /**
     * Returns the index of the underlying flat array at which the first element
     * of this array view is located.
     */
    public final int getFlatViewOffset() {
        return flatViewOffset;
    }

    public final int getStride(int dimension) {
        assert dimension >= 0 && dimension < strides.size();
        return strides.getQuick(dimension);
    }

    /**
     * Returns the encoded array type, as specified in {@link ColumnType#encodeArrayType(short, int)}.
     */
    public final int getType() {
        return type;
    }

    private void appendToMemRecursive(int dim, int flatIndex, MemoryA mem) {
        assert ColumnType.isDouble(ColumnType.decodeArrayElementType(this.type)) : "implemented only for double";

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

    protected final BorrowedFlatArrayView borrowedFlatView() {
        return (BorrowedFlatArrayView) flatView;
    }

    protected void resetToDefaultStrides() {
        strides.clear();
        int nDims = shape.size();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }
        int stride = 1;
        for (int dimIndex = nDims - 1; dimIndex >= 0; dimIndex--) {
            strides.set(dimIndex, stride);
            stride *= shape.get(dimIndex);
        }
    }
}
