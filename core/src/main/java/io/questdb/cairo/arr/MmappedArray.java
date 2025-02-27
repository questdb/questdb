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
import io.questdb.std.DirectIntSlice;
import io.questdb.std.Unsafe;

import static io.questdb.cairo.arr.ArrayTypeDriver.bytesToSkipForAlignment;

public class MmappedArray extends ArrayView {
    /**
     * Maximum size of any given dimension.
     * <p>Why:
     * <ul>
     *   <li>Our buffers are at most Integer.MAX_VALUE long</li>
     *   <li>Our largest datatypes are 8 bytes</li>
     * </ul>
     * Assuming a 1-D array, <code>Integer.MAX_VALUE / Long.BYTES</code> gives us a maximum
     * of 2 ** 28 - 1, i.e. all bits 0 to (inc) 27 set.
     * <p><strong>NOTE</strong>: This value can also be used as a mask to extract the dim
     * from the lower bits of an int, packed with extra data in the remaining bits.</p>
     */
    public static final int DIM_MAX_SIZE = (1 << 28) - 1;
    // Helper object used during init
    private final DirectIntSlice mmappedShape = new DirectIntSlice();

    public MmappedArray() {
        this.flatView = new BorrowedFlatArrayView();
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
        this.flatViewOffset = 0;
        shape.clear();
        strides.clear();
        short elemType = ColumnType.decodeArrayElementType(columnType);
        final int elemSize = ColumnType.sizeOf(elemType);
        final int nDims = ColumnType.decodeArrayDimensionality(columnType);
        assert nDims > 0 && nDims <= ColumnType.ARRAY_NDIMS_LIMIT;

        final long rowOffset = ArrayTypeDriver.getAuxVectorOffsetStatic(row);
        assert auxAddr + ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES <= auxLim;
        final long crcAndOffset = Unsafe.getUnsafe().getLong(auxAddr + rowOffset);
        final long sizeBytes = Unsafe.getUnsafe().getInt(auxAddr + rowOffset + Long.BYTES);
        if (sizeBytes == 0) {
            ofNull();
            return this;
        }

        assert dataAddr + sizeBytes <= dataLim : "dataAddr + sizeBytes > dataLim";
        final long offset = crcAndOffset & ArrayTypeDriver.OFFSET_MAX;
        final long dataEntryPtr = dataAddr + offset;

        validateAndInitShape(dataEntryPtr, nDims);
        assert (dataEntryPtr + nDims * Integer.BYTES) <= dataLim : "dataEntryPtr + shapeSize > dataLim";
        resetToDefaultStrides();

        // Obtain the values ptr / len from the data.
        final long unalignedValuesOffset = offset + ((long) nDims * Integer.BYTES);
        final long bytesToSkipForAlignment = bytesToSkipForAlignment(unalignedValuesOffset, elemSize);
        final long valuesPtr = dataAddr + unalignedValuesOffset + bytesToSkipForAlignment;
        assert valuesPtr + (long) flatViewLength * elemSize <= dataLim;
        borrowedFlatView().of(valuesPtr, elemType, flatViewLength);
        return this;
    }

    /**
     * Sets to a null array.
     */
    public void ofNull() {
        reset();
        type = ColumnType.NULL;
    }

    /**
     * Resets to an invalid array.
     */
    public void reset() {
        this.type = ColumnType.UNDEFINED;
        borrowedFlatView().reset();
        shape.clear();
        strides.clear();
    }

    private void validateAndInitShape(long shapePtr, int nDims) {
        mmappedShape.of(shapePtr, nDims);
        try {
            if (mmappedShape.length() > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw new AssertionError("shape length exceeds max dimensionality: " + mmappedShape.length());
            }
            int flatLength = 1;
            for (int i = 0; i < nDims; ++i) {
                final int dimLength = mmappedShape.get(i);
                if (dimLength <= 0 || dimLength >= DIM_MAX_SIZE) {
                    throw new AssertionError(String.format("shape dimension %,d out of bounds: %,d", i, dimLength));
                }
                flatLength = Math.multiplyExact(flatLength, dimLength);
            }
            this.flatViewLength = flatLength;
            shape.clear();
            for (int i = 0; i < nDims; i++) {
                shape.add(mmappedShape.get(i));
            }
        } finally {
            mmappedShape.reset();
        }
    }
}
