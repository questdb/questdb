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

package io.questdb.std.ndarr;

import io.questdb.cairo.ColumnType;
import io.questdb.std.CRC16XModem;
import io.questdb.std.DirectIntSlice;

/**
 * A view over an immutable N-dimensional Array.
 * This is a flyweight object.
 */
public class NdArrayView {
    private volatile short crc;

    public enum ValidatonStatus {
        OK,

        /** Not an ARRAY(...) ColumnType. */
        BAD_TYPE,

        /** There are a different number of dimensions and strides. */
        UNALIGNED_SHAPE_AND_STRIDES,

        /** The shape contains 0 or negative dimensions. */
        BAD_SHAPE,

        /** The values vector is of an unexpected byte length. */
        BAD_VALUES_SIZE,
    }

    private final DirectIntSlice shape = new DirectIntSlice();
    private final DirectIntSlice strides = new DirectIntSlice();
    private final NdArrayValuesSlice values = new NdArrayValuesSlice();
    int valuesLength = 0;
    int valuesOffset = 0;
    private int type = ColumnType.UNDEFINED;

    public boolean getBoolean(DirectIntSlice coordinates) {
        return values.getBoolean(flatIndex(coordinates));
    }

    public byte getByte(DirectIntSlice coordinates) {
        return values.getByte(flatIndex(coordinates));
    }

    public double getDouble(DirectIntSlice coordinates) {
        return values.getDouble(flatIndex(coordinates));
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

    public short getShort(DirectIntSlice coordinates) {
        return values.getShort(flatIndex(coordinates));
    }

    /**
     * Get the array's type
     */
    public int getType() {
        return type;
    }

    /**
     * Buffer holding the flattened array values.
     * <p>Data is stored in row-major format.</p>
     * <p>For example, for the 4x3x2 nd array:
     * <pre>
     * {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * </pre></p>
     * <p>The buffer would contain a flat vector of elements
     * with the numbers <code>[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]</code>.</p>
     */
    public NdArrayValuesSlice getValues() {
        return values;
    }

    /**
     * Number of values readable, after skipping {@link NdArrayView#getValuesOffset}.
     */
    public int getValuesLength() {
        return valuesLength;
    }

    /**
     * Number of values to skip reading before
     * applying the strides logic to access the dense array.
     * <p>This is exposed (rather than being a part of the Values object)
     * because of densely packed datatypes, such as boolean bit arrays,
     * where this might mean slicing across the byte boundary.</p>
     */
    public int getValuesOffset() {
        return valuesOffset;
    }

    /**
     * The array is a typeless zero-dimensional array.
     * <p>This maps to the <code>NULL</code> value in an array column.</p>
     */
    public boolean isNull() {
        return type == ColumnType.NULL;
    }

    /**
     * Set to a non-null array.
     *
     * @param stridesLength number of elements
     * @param shapeLength   number of elements
     * @param valuesSize    number of bytes
     */
    public ValidatonStatus of(
            int type,
            long shapePtr,
            int shapeLength,
            long stridesPtr,
            int stridesLength,
            long valuesPtr,
            int valuesSize,
            int valuesOffset) {
        boolean complete = false;
        try {
            if (!ColumnType.isNdArray(type)) {
                return ValidatonStatus.BAD_TYPE;
            }
            if (shapeLength != stridesLength) {
                return ValidatonStatus.UNALIGNED_SHAPE_AND_STRIDES;
            }
            this.type = type;
            this.shape.of(shapePtr, shapeLength);
            if (!NdArrayMeta.validShape(this.shape)) {
                return ValidatonStatus.BAD_SHAPE;
            }
            valuesLength = calcValuesLength();
            if (!validValuesSize(type, valuesOffset, valuesLength, valuesSize)) {
                return ValidatonStatus.BAD_VALUES_SIZE;
            }
            this.strides.of(stridesPtr, stridesLength);
            this.values.of(valuesPtr, valuesSize);
            this.valuesOffset = valuesOffset;
            complete = true;
        }
        finally {
            if (!complete) {
                reset();
            }
        }
        return ValidatonStatus.OK;
    }

    private static boolean validValuesSize(int type, int valuesOffset, int valuesLength, int valuesSize) {
        final int totExpectedElementCapacity = valuesOffset + valuesLength;
        final long typeBitWidth = 1L << ColumnType.getNdArrayElementTypePrecision(type);
        final long requiredBits = totExpectedElementCapacity * typeBitWidth;
        final long expectedByteSize = (requiredBits + 7L) / 8L;
        return expectedByteSize == (long) valuesSize;
    }

    /** Infer the number of addressable elements in the flat buffer from the shape. */
    private int calcValuesLength() {
        int length = 1;
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            length *= dim;
        }
        return length;
    }

    /**
     * Set to a null array.
     */
    public NdArrayView ofNull() {
        reset();
        type = ColumnType.NULL;
        return this;
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
        this.valuesLength = 0;
        this.crc = 0;
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
        assert flatIndex < valuesLength;
        return valuesOffset + flatIndex;
    }

    /**
     * Get the dimensions (<i>aka shape</i>) of the array.
     * <p>Examples shapes:
     * <ul>
     *     <li>A 1-D vector of 100 elements: <code>[100]</code>.</li>
     *     <li>A 2-D matrix of 50 rows and 2 columns: <code>[50, 2]</code>.</li>
     * </ul></p>
     */
    public DirectIntSlice getShape() {
        return shape;
    }

    /**
     * Get the array's strides, in element space.
     * <p>The returned strides expresses the number of elements to skip
     * to read the next element in each dimension.</p>
     * <p><strong>IMPORTANT:</strong>
     * <ul>
     *     <li>A stride can be <code>0</code>, in case of broadcasting, or
     *         <code>&lt; 0</code> in case of reversing of data.</li>
     *     <li>Most libraries support strides expressed in the byte space.
     *         Since we also support packed arrays (e.g. bool bit arrays),
     *         the strides here are expressed in the element count space
     *         instead.</li>
     * </ul></p>
     */
    public DirectIntSlice getStrides() {
        return strides;
    }

    public boolean hasDefaultStrides() {
        return NdArrayMeta.isDefaultStrides(shape, strides);
    }

    public short getCrc() {
        if (crc == 0) {
            short checksum = 0;
            // Add the dimension information first.
            checksum = CRC16XModem.calc(checksum, shape.length());
            for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
                checksum = CRC16XModem.calc((short) 0, valuesLength);
            }

            // Add the values next.
            if ((ColumnType.getNdArrayElementTypePrecision(type) < 3) && (valuesOffset > 0)) {
                // We don't currently support walking data that has a byte-unaligned start.
                // In other words, a scenario where the first value is not at the start of a byte boundary.
                // We simplify this even further by not supporting `valuesOffset` at all yet.
                throw new UnsupportedOperationException("nyi");
            }
            if (!hasDefaultStrides()) {
                throw new UnsupportedOperationException("nyi");
            }
            crc = CRC16XModem.calc(checksum, values.ptr(), values.size());
        }
        return crc;
    }
}
