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

import io.questdb.cairo.vm.api.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

public class StringTypeDriver implements ColumnTypeDriver {
    public static final StringTypeDriver INSTANCE = new StringTypeDriver();

    @Override
    public void configureAuxMemMA(FilesFacade ff, MemoryMA auxMem, LPSZ fileName, long dataAppendPageSize, int memoryTag, long opts, int madviseOpts) {
        auxMem.of(
                ff,
                fileName,
                dataAppendPageSize,
                -1,
                MemoryTag.MMAP_TABLE_WRITER,
                opts,
                madviseOpts
        );
        auxMem.putLong(0L);
    }

    @Override
    public void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, int fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts) {
        auxMem.ofOffset(
                ff,
                fd,
                fileName,
                rowLo << ColumnType.LEGACY_VAR_SIZE_AUX_SHL,
                (rowHi + 1) << ColumnType.LEGACY_VAR_SIZE_AUX_SHL,
                memoryTag,
                opts
        );
    }

    @Override
    public void configureDataMemOM(
            FilesFacade ff,
            MemoryR auxMem,
            MemoryOM dataMem,
            int dataFd,
            LPSZ fileName,
            long rowLo,
            long rowHi,
            int memoryTag,
            long opts
    ) {
        dataMem.ofOffset(
                ff,
                dataFd,
                fileName,
                auxMem.getLong(rowLo << ColumnType.LEGACY_VAR_SIZE_AUX_SHL),
                auxMem.getLong(rowHi << ColumnType.LEGACY_VAR_SIZE_AUX_SHL),
                memoryTag,
                opts
        );
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return (storageRowCount + 1) << ColumnType.LEGACY_VAR_SIZE_AUX_SHL;
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        return O3Utils.findVarOffset(auxMemAddr, rowHi + 1) - O3Utils.findVarOffset(auxMemAddr, rowLo);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        // For STRING storage aux vector (mem) contains N+1 offsets. Where N is the
        // row count. Offset indexes are 0 based, so reading Nth element of the vector gives
        // the size of the data vector.
        auxMem.jumpTo(rowCount << ColumnType.LEGACY_VAR_SIZE_AUX_SHL);
        // it is safe to read offset from the raw memory pointer because paged
        // memories (which MemoryMA is) have power-of-2 page size.

        final long dataMemOffset = rowCount > 0 ? Unsafe.getUnsafe().getLong(auxMem.getAppendAddress()) : 0;

        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo((rowCount + 1) << ColumnType.LEGACY_VAR_SIZE_AUX_SHL);
        return dataMemOffset;
    }

    @Override
    public void o3sort(
            long timestampIndex,
            long timestampIndexSize,
            MemoryCR srcDataMem,
            MemoryCR srcAuxMem,
            MemoryCARW dstDataMem,
            MemoryCARW dstAuxMem
    ) {
        // ensure we have enough memory allocated
        final long srcDataAddr = srcDataMem.addressOf(0);
        final long srcAuxAddr = srcAuxMem.addressOf(0);
        // exclude the trailing offset from shuffling
        final long tgtDataAddr = dstDataMem.resize(srcDataMem.size());
        final long tgtAuxAddr = dstAuxMem.resize(timestampIndexSize * Long.BYTES);

        assert srcDataAddr != 0;
        assert srcAuxAddr != 0;
        assert tgtDataAddr != 0;
        assert tgtAuxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
        final long offset = Vect.sortVarColumn(
                timestampIndex,
                timestampIndexSize,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(timestampIndexSize * Long.BYTES);
        dstAuxMem.putLong(offset);
    }
}
