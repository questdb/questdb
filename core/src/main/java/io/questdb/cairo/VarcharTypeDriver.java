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
import io.questdb.std.str.LPSZ;

import static io.questdb.cairo.ColumnType.VARCHAR_AUX_SHL;
import static io.questdb.std.str.Utf8s.varcharGetDataVectorSize;

public class VarcharTypeDriver implements ColumnTypeDriver {
    public static final VarcharTypeDriver INSTANCE = new VarcharTypeDriver();

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
    }

    @Override
    public void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, int fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, long opts) {
        auxMem.ofOffset(
                ff,
                fd,
                fileName,
                rowLo << VARCHAR_AUX_SHL,
                rowHi << VARCHAR_AUX_SHL,
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
                varcharGetDataVectorSize(auxMem, rowLo << VARCHAR_AUX_SHL),
                varcharGetDataVectorSize(auxMem, (rowHi - 1) << VARCHAR_AUX_SHL),
                memoryTag,
                opts
        );
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return storageRowCount << VARCHAR_AUX_SHL;
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        // For STRING storage aux vector (mem) contains N+1 offsets. Where N is the
        // row count. Offset indexes are 0 based, so reading Nth element of the vector gives
        // the size of the data vector.
        auxMem.jumpTo((rowCount - 1) << VARCHAR_AUX_SHL);
        // it is safe to read offset from the raw memory pointer because paged
        // memories (which MemoryMA is) have power-of-2 page size.

        if (rowCount > 0) {
            final long dataMemOffset = varcharGetDataVectorSize(auxMem.getAppendAddress());
            auxMem.jumpTo(rowCount << VARCHAR_AUX_SHL);
            return dataMemOffset;
        }
        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo(0);
        return 0;
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        final long loSize = varcharGetDataVectorSize(auxMemAddr + (rowLo << VARCHAR_AUX_SHL));
        final long hiSize = varcharGetDataVectorSize(auxMemAddr + ((rowHi) << VARCHAR_AUX_SHL));
        return hiSize - loSize;
    }

    @Override
    public void o3sort(long timestampIndex, long timestampIndexSize, MemoryCR srcDataMem, MemoryCR srcAuxMem, MemoryCARW dstDataMem, MemoryCARW dstAuxMem) {
        throw new UnsupportedOperationException();
    }
}
