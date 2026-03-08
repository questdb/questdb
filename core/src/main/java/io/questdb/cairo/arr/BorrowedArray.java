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

import io.questdb.cairo.ColumnType;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

import static io.questdb.cairo.arr.ArrayTypeDriver.bytesToSkipForAlignment;

public class BorrowedArray extends MutableArray implements Mutable {
    // Helper object used during init
    private final DirectIntSlice borrowedShape = new DirectIntSlice();

    public BorrowedArray() {
        this.flatView = new BorrowedFlatArrayView();
    }

    /**
     * Resets to an invalid array.
     */
    @Override
    public void clear() {
        this.type = ColumnType.UNDEFINED;
        borrowedFlatView().reset();
        shape.clear();
        strides.clear();
    }

    public BorrowedArray of(int columnType, long auxAddr, long auxLim, long dataAddr, long dataLim, long rowNum) {
        assert ColumnType.isArray(columnType) : "type class is not Array";
        setType(columnType);
        short elemType = ColumnType.decodeArrayElementType(columnType);
        final int elemSize = ColumnType.sizeOf(elemType);
        final int dims = ColumnType.decodeArrayDimensionality(columnType);
        final long rowOffset = ArrayTypeDriver.getAuxVectorOffsetStatic(rowNum);
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

        loadShape(dataEntryPtr, dims);
        assert (dataEntryPtr + (long) dims * Integer.BYTES) <= dataLim : "dataEntryPtr + shapeSize > dataLim";
        resetToDefaultStrides();

        // Get the values ptr / len from the data.
        final long unalignedValuesOffset = offset + ((long) dims * Integer.BYTES);
        final long bytesToSkipForAlignment = bytesToSkipForAlignment(unalignedValuesOffset, elemSize);
        final long valuesPtr = dataAddr + unalignedValuesOffset + bytesToSkipForAlignment;
        assert valuesPtr + (long) flatViewLength * elemSize <= dataLim;
        borrowedFlatView().of(valuesPtr, elemType, flatViewLength);
        return this;
    }

    public BorrowedArray of(int columnType, long shapeAddr, long valuePtr, int valueSize) {
        assert shapeAddr != 0 : "shapeAddr == 0";
        assert ColumnType.isArray(columnType) : "columnType is not Array";
        short elemType = ColumnType.decodeArrayElementType(columnType);
        setType(columnType);
        loadShape(shapeAddr, ColumnType.decodeArrayDimensionality(columnType));
        resetToDefaultStrides();
        assert ColumnType.sizeOf(elemType) * flatViewLength == valueSize;
        if (!isEmpty()) {
            borrowedFlatView().of(valuePtr, elemType, flatViewLength);
        }
        return this;
    }

    public BorrowedArray of(BorrowedArray other) {
        this.type = other.type;
        this.shape.clear();
        this.strides.clear();
        this.shape.addAll(other.shape);
        this.strides.addAll(other.strides);
        this.flatViewLength = other.flatViewLength;
        this.flatViewOffset = other.flatViewOffset;
        this.borrowedFlatView().of(other.borrowedFlatView());
        return this;
    }

    /**
     * Sets to a null array.
     */
    public void ofNull() {
        clear();
        type = ColumnType.NULL;
    }


    private void loadShape(long shapeAddr, int nDims) {
        borrowedShape.of(shapeAddr, nDims);
        try {
            for (int i = 0; i < nDims; i++) {
                setDimLen(i, borrowedShape.get(i));
            }
        } finally {
            borrowedShape.reset();
        }
    }
}
