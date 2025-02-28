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

public class MmappedArray extends MutableArray {
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
        setType(columnType);
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

        mmappedShape.of(dataEntryPtr, nDims);
        try {
            for (int i = 0; i < nDims; i++) {
                setDimLen(i, mmappedShape.get(i));
            }
        } finally {
            mmappedShape.reset();
        }
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
}
