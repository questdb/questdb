/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.*;

import java.io.Closeable;

public class DedupColumnCommitAddresses implements Closeable {
    private static final long COL_DATA_ARRAY_INDEX = 0L;
    private static final long COL_DATA_SIZE_ARRAY_INDEX = COL_DATA_ARRAY_INDEX + 1L;
    private static final long COL_DATA_TOP_ARRAY_INDEX = COL_DATA_SIZE_ARRAY_INDEX + 1L;
    private static final long COL_FD_ARRAY_INDEX = COL_DATA_TOP_ARRAY_INDEX + 1L;
    private static final long COL_OOO_ARRAY_INDEX = COL_FD_ARRAY_INDEX + 1L;
    private static final int LONG_PER_COL = (int) COL_OOO_ARRAY_INDEX + 1;
    private PagedDirectLongList addresses;
    private int columnCount;

    public long allocateBlock() {
        if (columnCount == 0) {
            return -1;
        }
        return addresses.allocateBlock();
    }

    public void clear(long dedupColSinkAddr) {
        Vect.memset(dedupColSinkAddr, 0, columnCount * Long.BYTES * LONG_PER_COL);
    }

    @Override
    public void close() {
        addresses = Misc.free(addresses);
    }

    public long getColDataAddresses(long dedupBlockAddress) {
        return getArrayPtr(dedupBlockAddress, COL_DATA_ARRAY_INDEX);
    }

    public long getColDataTops(long dedupBlockAddress) {
        return getArrayPtr(dedupBlockAddress, COL_DATA_TOP_ARRAY_INDEX);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getColumnMapAddress(long dedupColSinkAddr, int dedupKeyIndex) {
        return getArrayElement(dedupColSinkAddr, COL_DATA_ARRAY_INDEX, dedupKeyIndex);
    }

    public long getColumnMapSize(long dedupColSinkAddr, int dedupKeyIndex) {
        return getArrayElement(dedupColSinkAddr, COL_DATA_SIZE_ARRAY_INDEX, dedupKeyIndex);
    }

    public long getOooAddresses(long dedupBlockAddress) {
        return getArrayPtr(dedupBlockAddress, COL_OOO_ARRAY_INDEX);
    }

    public int getOpenFd(long dedupColSinkAddr, int dedupKeyIndex) {
        return (int) getArrayElement(dedupColSinkAddr, COL_FD_ARRAY_INDEX, dedupKeyIndex);
    }

    public void setColumnMapAddress(long dedupColSinkAddr, int dedupKeyIndex, long mappedAddress) {
        setArrayElement(dedupColSinkAddr, COL_DATA_ARRAY_INDEX, dedupKeyIndex, mappedAddress);
    }

    public void setColumnMapSize(long dedupColSinkAddr, int dedupKeyIndex, long size) {
        setArrayElement(dedupColSinkAddr, COL_DATA_SIZE_ARRAY_INDEX, dedupKeyIndex, size);
    }

    public void setColumnTop(long dedupColSinkAddr, int dedupKeyIndex, long columnTop) {
        setArrayElement(dedupColSinkAddr, COL_DATA_TOP_ARRAY_INDEX, dedupKeyIndex, columnTop);
    }

    public void setDedupColumnCount(int dedupColumnCount) {
        if (dedupColumnCount > 0) {
            if (addresses == null) {
                addresses = new PagedDirectLongList(MemoryTag.NATIVE_O3);
            } else {
                addresses.clear();
            }
            addresses.setBlockSize(dedupColumnCount * LONG_PER_COL);
            this.columnCount = dedupColumnCount;
        }
    }

    public void setOooColumnMapAddress(long dedupColSinkAddr, int dedupKeyIndex, long pageAddress) {
        setArrayElement(dedupColSinkAddr, COL_OOO_ARRAY_INDEX, dedupKeyIndex, pageAddress);
    }

    public void setOpenFd(long dedupColSinkAddr, int dedupKeyIndex, int fd) {
        setArrayElement(dedupColSinkAddr, COL_FD_ARRAY_INDEX, dedupKeyIndex, fd);
    }

    private long getArrayElement(long dedupColSinkAddr, long arrayIndex, long dedupKeyIndex) {
        return Unsafe.getUnsafe().getLong(dedupColSinkAddr + Long.BYTES * arrayIndex * columnCount + dedupKeyIndex * Long.BYTES);
    }

    private long getArrayPtr(long dedupBlockAddress, long arrayIndex) {
        return dedupBlockAddress + arrayIndex * Long.BYTES * columnCount;
    }

    private void setArrayElement(long dedupColSinkAddr, long arrayIndex, int dedupKeyIndex, long value) {
        Unsafe.getUnsafe().putLong(dedupColSinkAddr + Long.BYTES * arrayIndex * columnCount + (long) dedupKeyIndex * Long.BYTES, value);
    }
}
