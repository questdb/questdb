/*******************************************************************************
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

public class MutableArray extends ArrayView {

    /**
     * Copies the shape from the provided source array.
     * <p>
     * <strong>IMPORTANT:</strong> if you are calling this as the first step in
     * populating the array with new data, you must call {@code applyShape()} before
     * adding the data. A method of that name is defined on subclasses such as {@link
     * DirectArray#applyShape() DirectArray} and {@link FunctionArray#applyShape
     * FunctionArray}.
     * <p>
     * If you're calling this while not changing the data, it is most likely an error
     * and may result in a segmentation fault when accessing the data. If your goal is
     * to create a view of this array with a different shape, use {@link DerivedArrayView}.
     */
    public final void copyShapeFrom(ArrayView source) {
        int nDims = getDimCount();
        if (source.getDimCount() != nDims) {
            throw CairoException.nonCritical()
                    .put("source array doesn't have the same dimensionality [nDimsThis=").put(nDims)
                    .put(", nDimsSource=").put(source.getDimCount())
                    .put(']');
        }
        for (int i = 0; i < nDims; i++) {
            shape.set(i, source.shape.getQuick(i));
        }
    }

    /**
     * Sets the length of one dimension.
     * <p>
     * <strong>IMPORTANT:</strong> if you are calling this as the first step in populating
     * the array with new data, you must call {@code applyShape()} before adding the data.
     * A method of that name is defined on subclasses such as {@link DirectArray#applyShape()
     * DirectArray} and {@link FunctionArray#applyShape FunctionArray}.
     * <p>
     * If you're calling this while not changing the data, it is most likely an error
     * and may result in a segmentation fault when accessing the data. If your goal is
     * to create a view of this array with a different shape, use {@link DerivedArrayView}.
     */
    public final void setDimLen(int dimension, int length) {
        if (length < 0) {
            throw CairoException.nonCritical()
                    .put("dimension length must not be negative [dim=").put(dimension)
                    .put(", dimLen=").put(length)
                    .put(']');
        }
        if (length > DIM_MAX_LEN) {
            throw CairoException.nonCritical()
                    .put("dimension length out of range [dim=").put(dimension)
                    .put(", dimLen=").put(length)
                    .put(", maxLen=").put(DIM_MAX_LEN);

        }
        shape.set(dimension, length);
    }

    /**
     * Sets the encoded type of the array. This includes the element type and the number
     * of dimensions. Encode the type using {@link ColumnType#encodeArrayType}.
     * <p>
     * <strong>IMPORTANT:</strong> if you are calling this as the first step in populating
     * the array with new data, you must call {@code applyShape()} before adding the data.
     * A method of that name is defined on subclasses such as {@link DirectArray#applyShape()
     * DirectArray} and {@link FunctionArray#applyShape FunctionArray}.
     * <p>
     * If you're calling this while not changing the data, it is most likely an error
     * and may result in a segmentation fault when accessing the data. If your goal is
     * to create a view of this array with a different shape, use {@link DerivedArrayView}.
     */
    public final void setType(int encodedType) {
        assert ColumnType.isArray(encodedType);
        this.type = encodedType;
        shape.clear();
        strides.clear();
        flatViewLength = 0;
        final int dims = ColumnType.decodeArrayDimensionality(encodedType);
        shape.checkCapacity(dims);
        strides.checkCapacity(dims);
        for (int i = 0; i < dims; i++) {
            shape.add(0);
        }
    }

    private int maxPossibleElemCount() {
        short elemType = ColumnType.decodeArrayElementType(this.type);
        return Integer.MAX_VALUE >> (elemType != ColumnType.UNDEFINED ? ColumnType.pow2SizeOf(elemType) : 0);
    }

    protected final void resetToDefaultStrides() {
        resetToDefaultStrides(Integer.MAX_VALUE >> 3, -1);
    }

    protected final void resetToDefaultStrides(int maxArrayElemCount, int errorPos) {
        assert maxArrayElemCount <= maxPossibleElemCount() : "maxArrayElemCount > " + maxPossibleElemCount();

        final int nDims = shape.size();
        strides.clear();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }

        // An empty array can have various shapes, such as (100_000_000, 100_000_000, 0).
        // Avoid initializing the strides in this case, because that may erroneously fail with "array is too large".
        if (isEmpty()) {
            this.flatViewLength = 0;
            return;
        }

        int stride = 1;
        for (int i = nDims - 1; i >= 0; i--) {
            int dimLen = shape.get(i);
            strides.set(i, stride);
            try {
                stride = Math.multiplyExact(stride, dimLen);
                if (stride > maxArrayElemCount) {
                    throw new ArithmeticException();
                }
            } catch (ArithmeticException e) {
                throw CairoException.nonCritical().position(errorPos)
                        .put("array element count exceeds max [max=")
                        .put(maxArrayElemCount)
                        .put(", shape=")
                        .put(shape)
                        .put(']');
            }
        }
        this.flatViewLength = stride;
    }
}
