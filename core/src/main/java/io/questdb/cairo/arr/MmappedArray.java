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
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import static io.questdb.cairo.arr.ArrayMetaUtils.validateShapeAndGetFlatElemCount;
import static io.questdb.cairo.arr.ArrayTypeDriver.bytesToSkipForAlignment;

public class MmappedArray implements ArrayView, QuietCloseable {
    private final DirectIntList cachedDefaultStrides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG1);
    private final BorrowedFlatArrayView flatView = new BorrowedFlatArrayView();
    private final DirectIntSlice shapeView = new DirectIntSlice();
    // Encoded array type, contains element type and dimensionality
    private int type = ColumnType.UNDEFINED;

    @Override
    public void appendToMem(MemoryA mem) {
        flatView.appendToMem(mem);
    }

    @Override
    public void close() {
        Misc.free(cachedDefaultStrides);
    }

    @Override
    public FlatArrayView flatView() {
        return flatView;
    }

    @Override
    public int getDimCount() {
        return shapeView.length();
    }

    @Override
    public int getDimLen(int dimension) {
        return shapeView.get(dimension);
    }

    @Override
    public int getFlatViewLength() {
        // flatView is element type-agnostic, so it doesn't know its length in elements.
        // Therefore, compute the length from the array shape.
        return getDimLen(0) * getStride(0);
    }

    @Override
    public int getStride(int dimension) {
        return cachedDefaultStrides.get(dimension);
    }

    @Override
    public int getType() {
        return type;
    }

    public MmappedArray of(
            int columnType,
            long auxAddr,
            long auxLim,
            long dataAddr,
            long dataLim,
            long row
    ) {
        assert ColumnType.isArray(columnType) : "type class is not Array";
        this.type = columnType;
        final int elementSize = ColumnType.sizeOf(ColumnType.decodeArrayElementType(columnType));
        final int nDims = ColumnType.decodeArrayDimensionality(columnType);
        assert nDims > 0 && nDims <= ColumnType.ARRAY_NDIMS_LIMIT : "nDims out of range";

        final long rowOffset = ArrayTypeDriver.getAuxVectorOffsetStatic(row);
        assert auxAddr + ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES <= auxLim;
        final long crcAndOffset = Unsafe.getUnsafe().getLong(auxAddr + rowOffset);
        final long sizeBytes = Unsafe.getUnsafe().getInt(auxAddr + rowOffset + Long.BYTES);
        if (sizeBytes == 0) {
            ofNull();
            return this;
        }

        assert dataAddr + sizeBytes <= dataLim;
        // This is a non-null array, we need to access the data vec.
        final long offset = crcAndOffset & ArrayTypeDriver.OFFSET_MAX;

        // Decode the shape and set the default strides.
        final long dataEntryPtr = dataAddr + offset;
        assert (dataEntryPtr + nDims * Integer.BYTES) <= dataLim;
        ArrayMetaUtils.determineDefaultStrides(dataEntryPtr, nDims, cachedDefaultStrides);
        assert cachedDefaultStrides.size() == nDims;

        // Obtain the values ptr / len from the data.
        final long unalignedValuesOffset = offset + ((long) (nDims) * Integer.BYTES);
        final long bytesToSkipForAlignment = bytesToSkipForAlignment(unalignedValuesOffset, elementSize);
        final long valuesPtr = dataAddr + unalignedValuesOffset + bytesToSkipForAlignment;
        shapeView.of(dataEntryPtr, nDims);
        final int flatLength = ArrayMetaUtils.validateShapeAndGetFlatElemCount(shapeView);
        final int valuesSize = flatLength * ColumnType.sizeOf(elementSize);
        assert valuesPtr + valuesSize <= dataLim;
        final int flatElemCout = validateShapeAndGetFlatElemCount(shapeView);
        validateValuesSize(columnType, flatElemCout, valuesSize);
        flatView.of(valuesPtr, valuesSize);
        return this;
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
        this.shapeView.reset();
        this.cachedDefaultStrides.clear();
        this.flatView.reset();
    }

    private static void validateValuesSize(int type, int valuesLength, int valuesSize) {
        assert ColumnType.isArray(type) : "type class is not Array";
        final int expectedByteSize = valuesLength * ColumnType.sizeOf(ColumnType.decodeArrayElementType(type));
        if (valuesSize != expectedByteSize) {
            throw new AssertionError(String.format("invalid valuesSize, expected %,d actual %,d", expectedByteSize, valuesSize));
        }
    }
}
