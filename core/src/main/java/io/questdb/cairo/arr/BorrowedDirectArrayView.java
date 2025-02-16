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
import io.questdb.std.DirectIntSlice;

/**
 * An immutable view over a native-memory array. Does not own the backing native memory.
 */
public class BorrowedDirectArrayView implements ArrayView {
    private final DirectIntSlice shape = new DirectIntSlice();
    private final DirectIntSlice strides = new DirectIntSlice();
    private final DirectArraySlice values = new DirectArraySlice();
    private volatile short crc;
    // Encoded array type, contains element type class, type precision, and dimensionality
    private int type = ColumnType.UNDEFINED;
    private int valuesOffset = 0;

    @Override
    public void appendWithDefaultStrides(MemoryA mem) {
        mem.putBlockOfBytes(values.ptr(), values.size());
    }

    public boolean getBoolean(DirectIntSlice coordinates) {
        return values.getBoolean(flatIndex(coordinates));
    }

    public byte getByte(DirectIntSlice coordinates) {
        return values.getByte(flatIndex(coordinates));
    }

    @Override
    public int getDimCount() {
        return shape.getDimCount();
    }

    @Override
    public int getDimLen(int dim) {
        return shape.getDimLen(dim);
    }

    public double getDouble(DirectIntSlice coordinates) {
        return values.getDouble(flatIndex(coordinates));
    }

    @Override
    public double getDoubleAtFlatIndex(int flatIndex) {
        return values.getDouble(flatIndex);
    }

    @Override
    public int getFlatElemCount() {
        return values.size();
    }

    public float getFloat(DirectIntSlice coordinates) {
        return values.getFloat(flatIndex(coordinates));
    }

    public int getInt(DirectIntSlice coordinates) {
        return values.getInt(flatIndex(coordinates));
    }

    public long getLong(DirectIntSlice coordinates) {
        return values.getLong(flatIndex(coordinates));
    }

    @Override
    public long getLongAtFlatIndex(int flatIndex) {
        return values.getLong(flatIndex);
    }

    public short getShort(DirectIntSlice coordinates) {
        return values.getShort(flatIndex(coordinates));
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
     * Get the array's shapes
     */
    public DirectIntSlice getShapes() {
        return shape;
    }

    /**
     * Buffer holding the flattened array values.
     * <p>
     * Data is stored in row-major order.
     * <p>
     * For example, for the 4x3x2 nd array:
     * <pre>
     * {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * </pre>
     * The buffer would contain a flat vector of elements with the numbers
     * <code>[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]</code>.
     */
    public DirectArraySlice getValues() {
        return values;
    }

    /**
     * Number of values to skip before applying the strides logic to access
     * the dense array.
     * <p>
     * This is exposed (rather than being a part of the Values object)
     * because of densely packed datatypes, such as boolean bit arrays,
     * where this might mean slicing across the byte boundary.
     */
    public int getValuesOffset() {
        return valuesOffset;
    }

    public boolean hasDefaultStrides() {
        return ArrayMetaUtils.isDefaultStrides(shape, strides);
    }

    /**
     * The array is a typeless zero-dimensional array.
     * <p>
     * This maps to the <code>NULL</code> value in an array column.
     */
    public boolean isNull() {
        return type == ColumnType.NULL;
    }

    /**
     * Set to a non-null array.
     *
     * @param shapeLength   number of elements
     * @param stridesLength number of elements
     * @param valuesSize    number of bytes
     */
    public void of(
            int type,
            long shapePtr,
            int shapeLength,
            long stridesPtr,
            int stridesLength,
            long valuesPtr,
            int valuesSize,
            int valuesOffset
    ) {
        boolean complete = false;
        try {
            if (!ColumnType.isArray(type)) {
                throw new AssertionError("type class is not ARRAY: " + type);
            }
            if (shapeLength != stridesLength) {
                throw new AssertionError("shapeLength != stridesLength");
            }
            if (ColumnType.decodeArrayDimensionality(type) != shapeLength) {
                throw new AssertionError("shapeLength != nDims decoded from type");
            }
            this.type = type;
            shape.of(shapePtr, shapeLength);
            ArrayMetaUtils.validateShape(shape);
            final int valuesLength = ArrayMetaUtils.flatLength(shape);
            validateValuesSize(type, valuesOffset, valuesLength, valuesSize);
            strides.of(stridesPtr, stridesLength);
            values.of(valuesPtr, valuesSize);
            this.valuesOffset = valuesOffset;
            complete = true;
        } finally {
            if (!complete) {
                reset();
            }
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
        this.shape.reset();
        this.strides.reset();
        this.values.reset();
        this.valuesOffset = 0;
    }

    private static void validateValuesSize(int type, int valuesOffset, int valuesLength, int valuesSize) {
        assert ColumnType.isArray(type) : "type class is not Array";
        final int totExpectedElementCapacity = valuesOffset + valuesLength;
        final int expectedByteSize = totExpectedElementCapacity * ColumnType.sizeOf(ColumnType.decodeArrayElementType(type));
        if (valuesSize != expectedByteSize) {
            throw new AssertionError(String.format("invalid valuesSize, expected %,d actual %,d", expectedByteSize, valuesSize));
        }
    }

    /**
     * Convert the coordinates into an element index into the values array.
     */
    private int flatIndex(DirectIntSlice coordinates) {
        assert coordinates.length() == strides.length();
        int flatIndex = 0;
        for (int dimsIndex = 0, nDims = strides.length(); dimsIndex < nDims; dimsIndex++) {
            final int dimCoordinate = coordinates.get(dimsIndex);
            final int dimStride = strides.get(dimsIndex);
            flatIndex += (dimCoordinate * dimStride);
        }
        assert flatIndex < ArrayMetaUtils.flatLength(shape);
        return valuesOffset + flatIndex;
    }
}
