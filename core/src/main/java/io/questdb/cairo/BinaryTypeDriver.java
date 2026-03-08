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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class BinaryTypeDriver extends StringTypeDriver {
    public static final BinaryTypeDriver INSTANCE = new BinaryTypeDriver();

    @Override
    public void appendNull(MemoryA auxMem, MemoryA dataMem) {
        auxMem.putLong(dataMem.putNullBin());
    }

    public long getDataVectorMinEntrySize() {
        return Long.BYTES;
    }

    @Override
    public boolean isSparseDataVector(long auxMemAddr, long dataMemAddr, long rowCount) {
        for (int row = 0; row < rowCount; row++) {
            long offset = Unsafe.getUnsafe().getLong(auxMemAddr + (long) row * Long.BYTES);
            long iLen = Unsafe.getUnsafe().getLong(auxMemAddr + (long) (row + 1) * Long.BYTES) - offset;
            long dLen = Unsafe.getUnsafe().getLong(dataMemAddr + offset);
            int lenLen = 8;
            long dStorageLen = dLen > 0 ? dLen + lenLen : lenLen;
            if (iLen != dStorageLen) {
                // Swiss cheese hole in var col file
                return true;
            }
        }
        return false;
    }

    @Override
    public void o3ColumnMerge(
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long srcAuxAddr1,
            long srcDataAddr1,
            long srcAuxAddr2,
            long srcDataAddr2,
            long dstAuxAddr,
            long dstDataAddr,
            long dstDataOffset
    ) {
        Vect.oooMergeCopyBinColumn(
                timestampMergeIndexAddr,
                timestampMergeIndexCount,
                srcAuxAddr1,
                srcDataAddr1,
                srcAuxAddr2,
                srcDataAddr2,
                dstAuxAddr,
                dstDataAddr,
                dstDataOffset
        );
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        Vect.memset(dataMemAddr, rowCount * Long.BYTES, -1);
    }

    @Override
    public void setFullAuxVectorNull(long auxMemAddr, long rowCount) {
        Vect.setBinaryColumnNullRefs(auxMemAddr, 0, rowCount + 1);
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        Vect.setBinaryColumnNullRefs(auxMemAddr, initialOffset, columnTop);
    }
}
