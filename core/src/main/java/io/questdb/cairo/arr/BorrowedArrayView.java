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
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

/**
 * A view over a native-memory array. Does not own the backing native memory.
 * The array contents can't be mutated through this view, but the view itself can be.
 * You can change what slice of the underlying flat array it represents, as well as
 * transpose it.
 */
public class BorrowedArrayView implements ArrayView, AutoCloseable {
    private int flatElemCount;
    private FlatArrayView flatView;
    private int flatViewOffset;
    private DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG1);
    private DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG1);
    // Encoded array type, contains element type class, type precision, and dimensionality
    private int type = ColumnType.UNDEFINED;

    @Override
    public void close() {
        this.shape = Misc.free(shape);
        this.strides = Misc.free(strides);
    }

    @Override
    public FlatArrayView flatView() {
        return flatView;
    }

    @Override
    public int getDimCount() {
        return (int) shape.size();
    }

    @Override
    public int getDimLen(int dim) {
        return shape.get(dim);
    }

    @Override
    public int getFlatElemCount() {
        return flatElemCount;
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

    public void of(ArrayView view) {
        this.type = view.getType();
        this.flatView = view.flatView();
        this.flatViewOffset = view.getFlatViewOffset();
        this.flatElemCount = view.getFlatElemCount();
        shape.clear();
        strides.clear();
        int nDims = view.getDimCount();
        for (int i = 0; i < nDims; i++) {
            shape.add(view.getDimLen(i));
            strides.add(view.getStride(i));
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
        this.flatElemCount = 0;
    }

    public void transpose() {
        strides.reverse();
        shape.reverse();
    }

    private static void validateValuesSize(int type, int valuesOffset, int valuesLength, int valuesSize) {
        assert ColumnType.isArray(type) : "type class is not Array";
        final int totExpectedElementCapacity = valuesOffset + valuesLength;
        final int expectedByteSize = totExpectedElementCapacity * ColumnType.sizeOf(ColumnType.decodeArrayElementType(type));
        if (valuesSize != expectedByteSize) {
            throw new AssertionError(String.format("invalid valuesSize, expected %,d actual %,d", expectedByteSize, valuesSize));
        }
    }
}
