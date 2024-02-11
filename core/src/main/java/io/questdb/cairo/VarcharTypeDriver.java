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
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.ColumnType.VARCHAR_AUX_SHL;

public class VarcharTypeDriver implements ColumnTypeDriver {
    public static final VarcharTypeDriver INSTANCE = new VarcharTypeDriver();

    public static long varcharGetDataOffset(MemoryR auxMem, long offset) {
        long dataOffset = auxMem.getShort(offset + 10);
        dataOffset <<= 32;
        dataOffset |= auxMem.getInt(offset + 12);
        return dataOffset;
    }

    public static long varcharGetDataVectorSize(MemoryR auxMem, long offset) {
        int raw = auxMem.getInt(offset);
        final long dataOffset = varcharGetDataOffset(auxMem, offset);
        int flags = raw & 0x0f; // 4 bit flags

        if ((flags & 4) == 4 || (flags & 1) == 1) {
            // null flag is set or fully inlined value
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> 4) & 0xffffff;
        return dataOffset + size - Utf8s.UTF8_STORAGE_SPLIT_BYTE;
    }

    public static long varcharGetDataVectorSize(long auxEntry) {
        int raw = Unsafe.getUnsafe().getInt(auxEntry);
        long dataOffset = Unsafe.getUnsafe().getShort(auxEntry + 10);
        dataOffset <<= 32;
        dataOffset |= Unsafe.getUnsafe().getInt(auxEntry + 12);
        int flags = raw & 0x0f; // 4 bit flags

        if ((flags & 4) == 4 || (flags & 1) == 1) {
            // null flag is set or fully inlined value
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> 4) & 0xffffff;
        return dataOffset + size - Utf8s.UTF8_STORAGE_SPLIT_BYTE;
    }

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
    public long getAuxVectorOffset(long row) {
        return row << VARCHAR_AUX_SHL;
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return storageRowCount << VARCHAR_AUX_SHL;
    }

    public long getDataVectorOffset(long auxMemAddr, long row) {
        return varcharGetDataVectorSize(auxMemAddr + (row << VARCHAR_AUX_SHL));
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        return getDataVectorOffset(auxMemAddr, rowHi) - getDataVectorOffset(auxMemAddr, rowLo);
    }

    @Override
    public void o3ColumnCopy(FilesFacade ff, long srcAuxAddr, long srcDataAddr, long srcLo, long srcHi, long dstAuxAddr, int dstAuxFd, long dstAuxFileOffset, long dstDataAddr, int dstDataFd, long dstDataOffset, long dstDataAdjust, long dstDataSize, boolean mixedIOFlag) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void o3MoveLag(long rowCount, long columnDataRowOffset, long existingLagRows, MemoryCR srcAuxMem, MemoryCR srcDataMem, MemoryARW dstAuxMem, MemoryARW dstDataMem) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void o3PartitionAppend(AtomicInteger columnCounter, int columnType, long srcOooFixAddr, long srcOooVarAddr, long srcOooLo, long srcOooHi, long srcOooMax, long timestampMin, long partitionTimestamp, long srcDataTop, long srcDataMax, int indexBlockCapacity, int srcTimestampFd, long srcTimestampAddr, long srcTimestampSize, int activeFixFd, int activeVarFd, MemoryMA dstFixMem, MemoryMA dstVarMem, long dstRowCount, long srcDataNewPartitionSize, long srcDataOldPartitionSize, long o3SplitPartitionSize, TableWriter tableWriter, long partitionUpdateSinkAddr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void o3PartitionMerge(Path pathToNewPartition, int pplen, CharSequence columnName, AtomicInteger columnCounter, AtomicInteger partCounter, int columnType, long timestampMergeIndexAddr, long timestampMergeIndexSize, long srcOooFixAddr, long srcOooVarAddr, long srcOooLo, long srcOooHi, long srcOooMax, long oooPartitionMin, long oooPartitionHi, long srcDataTop, long srcDataMax, int prefixType, long prefixLo, long prefixHi, int mergeType, long mergeOOOLo, long mergeOOOHi, long mergeDataLo, long mergeDataHi, long mergeLen, int suffixType, long suffixLo, long suffixHi, int indexBlockCapacity, int srcTimestampFd, long srcTimestampAddr, long srcTimestampSize, int srcDataFixFd, int srcDataVarFd, long srcDataNewPartitionSize, long srcDataOldPartitionSize, long o3SplitPartitionSize, TableWriter tableWriter, long colTopSinkAddr, long columnNameTxn, long partitionUpdateSinkAddr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void o3sort(long timestampMergeIndexAddr, long timestampMergeIndexSize, MemoryCR srcDataMem, MemoryCR srcAuxMem, MemoryCARW dstDataMem, MemoryCARW dstAuxMem) {
        throw new UnsupportedOperationException();
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
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem, boolean doubleAllocate) {
        throw new UnsupportedOperationException();
    }
}
