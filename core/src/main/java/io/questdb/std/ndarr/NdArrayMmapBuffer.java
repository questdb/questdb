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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NdArrayTypeDriver;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

public class NdArrayMmapBuffer implements QuietCloseable {
    private final DirectIntList strides = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    private final NdArrayView view = new NdArrayView();
    private @Nullable NdArrayView viewRes = null;  // set to `view` when has value, or `null` otherwise.

    public NdArrayMmapBuffer of(
            int columnType,
            long auxAddr,
            long auxLim,
            long dataAddr,
            long dataLim,
            long row
    ) {
        final long rowOffset = NdArrayTypeDriver.INSTANCE.getAuxVectorOffset(row);
        assert auxAddr + (3 * Integer.BYTES) <= auxLim;
        final long crcAndOffset = NdArrayTypeDriver.getIntAlignedLong(auxAddr + rowOffset);
        final long size = Unsafe.getUnsafe().getInt(auxAddr + rowOffset + Long.BYTES);
        if (size == 0) {
            // A null array
            viewRes = null;
            return this;
        }
        assert dataAddr + size <= dataLim;

        // A non-null array, we need to access the data vec.
        final long offset = crcAndOffset & NdArrayTypeDriver.OFFSET_MAX;
        final short crc = (short) (crcAndOffset >> NdArrayTypeDriver.CRC16_SHIFT);

        // Decode the shape and set the default strides.
        final long dataEntryPtr = dataAddr + offset;
        assert (dataEntryPtr + Byte.BYTES) <= dataLim;
        final int shapeLength = Unsafe.getUnsafe().getInt(dataEntryPtr);  // i.e., the number of dimensions.
        final long shapePtr = dataEntryPtr + Integer.BYTES;
        assert (dataEntryPtr + ((1 + shapeLength) * Byte.BYTES)) <= dataLim;
        NdArrayMeta.setDefaultStrides(shapePtr, shapeLength, strides);

        // Obtain the values ptr / len from the data.
        final int bitWidth = 1 << ColumnType.getNdArrayElementTypePrecision(columnType);
        final int requiredByteAlignment = Math.max(1, bitWidth / 8);
        final long unalignedValuesOffset = offset + ((long) (1 + shapeLength) * Integer.BYTES);
        final long toSkip = NdArrayTypeDriver.skipsToAlign(unalignedValuesOffset, requiredByteAlignment);
        final long valuesPtr = dataAddr + unalignedValuesOffset + toSkip;
        final int flatLength = NdArrayMeta.flatLength(shapePtr, shapeLength);
        final int valuesSize = NdArrayMeta.calcRequiredValuesByteSize(columnType, flatLength);
        assert valuesPtr + valuesSize <= dataLim;

        NdArrayView.ValidatonStatus status = view.of(
                columnType,
                shapePtr,
                shapeLength,
                strides.getAddress(),
                (int) strides.size(),
                valuesPtr,
                valuesSize,
                0, // No values to skip in the values vec.
                crc);
        if (status != NdArrayView.ValidatonStatus.OK) {
            // TODO(amunra): Improve exception here.
            throw CairoException.nonCritical().put("Invalid array encoding: " + status);
        }
        viewRes = view;
        return this;
    }

    public NdArrayView getView() {
        return viewRes;
    }

    @Override
    public void close() {
        Misc.free(strides);
    }
}
