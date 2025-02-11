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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.arr.ArrayTypeDriver.bytesToSkipForAlignment;

public class ArrayMmapBuffer implements QuietCloseable {
    private final DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY_DBG3);
    private final BorrowedDirectArrayView view = new BorrowedDirectArrayView();
    private @Nullable BorrowedDirectArrayView viewRes = null;  // set to `view` when has value, or `null` otherwise.

    @Override
    public void close() {
        Misc.free(strides);
    }

    public BorrowedDirectArrayView getView() {
        return viewRes;
    }

    public ArrayMmapBuffer of(
            int columnType,
            long auxAddr,
            long auxLim,
            long dataAddr,
            long dataLim,
            long row
    ) {
        final long rowOffset = ArrayTypeDriver.getAuxVectorOffsetStatic(row);
        assert auxAddr + ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES <= auxLim;
        final long crcAndOffset = Unsafe.getUnsafe().getLong(auxAddr + rowOffset);
        final long size = Unsafe.getUnsafe().getInt(auxAddr + rowOffset + Long.BYTES);
        if (size == 0) {
            // A null array
            viewRes = null;
            return this;
        }
        assert dataAddr + size <= dataLim;

        // A non-null array, we need to access the data vec.
        final long offset = crcAndOffset & ArrayTypeDriver.OFFSET_MAX;

        // Decode the shape and set the default strides.
        final long dataEntryPtr = dataAddr + offset;
        assert (dataEntryPtr + Byte.BYTES) <= dataLim;
        final int shapeLength = ColumnType.decodeArrayDimensionality(columnType);
        assert (dataEntryPtr + shapeLength * Byte.BYTES) <= dataLim;
        ArrayMeta.determineDefaultStrides(dataEntryPtr, shapeLength, strides);

        // Obtain the values ptr / len from the data.
        final int elementSize = ColumnType.sizeOf(ColumnType.decodeArrayElementType(columnType));
        final long unalignedValuesOffset = offset + ((long) (shapeLength) * Integer.BYTES);
        final long bytesToSkipForAlignment = bytesToSkipForAlignment(unalignedValuesOffset, elementSize);
        final long valuesPtr = dataAddr + unalignedValuesOffset + bytesToSkipForAlignment;
        final int flatLength = ArrayMeta.flatLength(dataEntryPtr, shapeLength);
        assert ColumnType.isArray(columnType) : "type class is not Array";
        final int valuesSize = flatLength * ColumnType.sizeOf(elementSize);
        assert valuesPtr + valuesSize <= dataLim;

        view.of(
                columnType,
                dataEntryPtr,
                shapeLength,
                strides.getAddress(),
                (int) strides.size(),
                valuesPtr,
                valuesSize,
                0 // No values to skip in the values vec.
        );
        viewRes = view;
        return this;
    }
}
