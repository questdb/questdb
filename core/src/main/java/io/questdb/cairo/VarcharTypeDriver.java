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
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;

import static io.questdb.cairo.ColumnType.VARCHAR_AUX_SHL;

public class VarcharTypeDriver implements ColumnTypeDriver {
    public static final VarcharTypeDriver INSTANCE = new VarcharTypeDriver();

    public static long varcharGetDataOffset(MemoryR auxMem, long offset) {
        return auxMem.getLong(offset + 8L) >>> 16;
    }

    public static long varcharGetDataOffset(long auxEntry) {
        return Unsafe.getUnsafe().getLong(auxEntry + 8L) >>> 16;
    }

    public static long varcharGetDataVectorSize(long auxEntry) {
        final int raw = Unsafe.getUnsafe().getInt(auxEntry);
        final int flags = raw & 0x0f; // 4 bit flags
        final long dataOffset = varcharGetDataOffset(auxEntry);

        if ((flags & 4) == 4 || (flags & 1) == 1) {
            // null flag is set or fully inlined value
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> 4) & 0xffffff;
        return dataOffset + size - Utf8s.UTF8_STORAGE_SPLIT_BYTE;
    }

    public static long varcharGetDataVectorSize(MemoryR auxMem, long offset) {
        final int raw = auxMem.getInt(offset);
        final int flags = raw & 0x0f; // 4 bit flags
        final long dataOffset = varcharGetDataOffset(auxMem, offset);

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
    public void configureAuxMemO3RSS(MemoryARW auxMem) {
        // no-op for varchar
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
        long lo;
        if (rowLo > 0) {
            lo = varcharGetDataVectorSize(auxMem, (rowLo - 1) << VARCHAR_AUX_SHL);
        } else {
            lo = 0;
        }
        long hi = varcharGetDataVectorSize(auxMem, (rowHi - 1) << VARCHAR_AUX_SHL);
        dataMem.ofOffset(
                ff,
                dataFd,
                fileName,
                lo,
                hi,
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

    @Override
    public long getDataVectorMinEntrySize() {
        return 0;
    }

    public long getDataVectorOffset(long auxMemAddr, long row) {
        return varcharGetDataOffset(auxMemAddr + (row << VARCHAR_AUX_SHL));
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        if (rowLo > 0) {
            return getDataVectorSizeAt(auxMemAddr, rowHi) - getDataVectorSizeAt(auxMemAddr, rowLo -1);
        }
        return getDataVectorSizeAt(auxMemAddr, rowHi);
    }

    @Override
    public long getDataVectorSizeAt(long auxMemAddr, long row) {
        return varcharGetDataVectorSize(auxMemAddr + (row << VARCHAR_AUX_SHL));
    }

    @Override
    public long getDataVectorSizeAtFromFd(FilesFacade ff, int auxFd, long row) {
        long auxFileOffset = row << VARCHAR_AUX_SHL;
        if (row < 0) {
            return 0;
        }
        final int raw = readInt(ff, auxFd, auxFileOffset);
        final int flags = raw & 0x0f; // 4 bit flags

        final int offsetLo = readInt(ff, auxFd, auxFileOffset + 8L);
        final int offsetHi = readInt(ff, auxFd, auxFileOffset + 12L);
        final long dataOffset = Numbers.encodeLowHighInts(offsetLo, offsetHi) >>> 16;

        if ((flags & 4) == 4 || (flags & 1) == 1) {
            // null flag is set or fully inlined value
            return dataOffset;
        }
        // size of the string at this offset
        final int size = (raw >> 4) & 0xffffff;
        return dataOffset + size - Utf8s.UTF8_STORAGE_SPLIT_BYTE;
    }

    @Override
    public long getMinAuxVectorSize() {
        return 0;
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
        Vect.oooMergeCopyVarcharColumn(
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
    public void o3copyAuxVector(FilesFacade ff, long srcAddr, long srcLo, long srcHi, long dstAddr, long dstFileOffset, int dstFd, boolean mixedIOFlag) {
        O3CopyJob.copyFixedSizeCol(ff, srcAddr, srcLo, srcHi, dstAddr, dstFileOffset, dstFd, VARCHAR_AUX_SHL, mixedIOFlag);
    }

    @Override
    public void o3sort(
            long sortedTimestampsAddr,
            long sortedTimestampsRowCount,
            MemoryCR srcDataMem,
            MemoryCR srcAuxMem,
            MemoryCARW dstDataMem,
            MemoryCARW dstAuxMem
    ) {
        // ensure we have enough memory allocated
        final long srcDataAddr = srcDataMem.addressOf(0);
        final long srcAuxAddr = srcAuxMem.addressOf(0);
        // exclude the trailing offset from shuffling
        final long tgtAuxAddr = dstAuxMem.resize(getAuxVectorSize(sortedTimestampsRowCount));
        final long tgtDataAddr = dstDataMem.resize(getDataVectorSizeAt(srcAuxAddr, sortedTimestampsRowCount - 1));

        assert srcAuxAddr != 0;
        assert tgtAuxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
        final long offset = Vect.sortVarcharColumn(
                sortedTimestampsAddr,
                sortedTimestampsRowCount,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(sortedTimestampsRowCount * 16);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        if (rowCount > 0) {
            auxMem.jumpTo((rowCount - 1) << VARCHAR_AUX_SHL);
            final long dataMemOffset = varcharGetDataVectorSize(auxMem.getAppendAddress());
            auxMem.jumpTo(rowCount << VARCHAR_AUX_SHL);
            return dataMemOffset;
        }
        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo(0);
        return 0;
    }

    @Override
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem) {
        if (pos > 0) {
            long auxVectorSize = getAuxVectorSize(pos);

            // first we need to calculate already used space. both data and aux vectors.
            long auxVectorOffset = getAuxVectorOffset(pos - 1); // the last entry we are NOT overwriting
            auxMem.jumpTo(auxVectorOffset);
            long auxEntryPtr = auxMem.getAppendAddress();
            long dataVectorSize = varcharGetDataVectorSize(auxEntryPtr);
            long totalDataSizeBytes = dataVectorSize + auxVectorSize;

            auxVectorOffset = getAuxVectorOffset(pos); // the entry we are about to overwrite with the next append
            auxMem.jumpTo(auxVectorOffset);
            dataMem.jumpTo(dataVectorSize);
            return totalDataSizeBytes;
        }

        dataMem.jumpTo(0);
        auxMem.jumpTo(0);
        // Assume var length columns use 28 bytes per value to estimate the record size
        // if there are no rows in the partition yet.
        // The record size used to estimate the partition size
        // to split partition in O3 commit when necessary
        return TableUtils.ESTIMATED_VAR_COL_SIZE;
    }

    @Override
    public void setColumnRefs(long address, long initialOffset, long count) {
        Vect.setVarcharColumnNullRefs(address, initialOffset, count);
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        // this is a no-op, NULLs do not occupy space in the data vector
    }

    @Override
    public void shiftCopyAuxVector(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr) {
        O3Utils.shiftCopyVarcharColumnAux(
                shift,
                srcAddr,
                srcLo,
                srcHi,
                dstAddr
        );
    }

    private static int readInt(FilesFacade ff, int fd, long offset) {
        long res = ff.readIntAsUnsignedLong(fd, offset);
        if (res < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid data read from varchar aux file [fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", fileSize=").put(ff.length(fd))
                    .put(", result=").put(res)
                    .put(']');
        }
        return Numbers.decodeLowInt(res);
    }
}
