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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

import static io.questdb.cairo.ColumnType.LEGACY_VAR_SIZE_AUX_SHL;

public class StringTypeDriver implements ColumnTypeDriver {
    public static final StringTypeDriver INSTANCE = new StringTypeDriver();
    private static final Log LOG = LogFactory.getLog(StringTypeDriver.class);

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
                rowLo << LEGACY_VAR_SIZE_AUX_SHL,
                (rowHi + 1) << LEGACY_VAR_SIZE_AUX_SHL,
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
                auxMem.getLong(rowLo << LEGACY_VAR_SIZE_AUX_SHL),
                auxMem.getLong(rowHi << LEGACY_VAR_SIZE_AUX_SHL),
                memoryTag,
                opts
        );
    }

    @Override
    public long getAuxVectorOffset(long row) {
        return row << LEGACY_VAR_SIZE_AUX_SHL;
    }

    @Override
    public long getAuxVectorSize(long storageRowCount) {
        return (storageRowCount + 1) << LEGACY_VAR_SIZE_AUX_SHL;
    }

    @Override
    public long getDataVectorMinEntrySize() {
        return Integer.BYTES;
    }

    @Override
    public long getDataVectorOffset(long auxMemAddr, long row) {
        return findVarOffset(auxMemAddr, row);
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        return getDataVectorOffset(auxMemAddr, rowHi + 1) - getDataVectorOffset(auxMemAddr, rowLo);
    }

    @Override
    public long getDataVectorSizeAt(long auxMemAddr, long row) {
        return getDataVectorOffset(auxMemAddr, row + 1);
    }

    @Override
    public long getMinAuxVectorSize() {
        return Long.BYTES;
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
        Vect.oooMergeCopyStrColumn(
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
    public void o3copyAuxVector(
            FilesFacade ff,
            long src,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstFixFileOffset,
            int dstFd,
            boolean mixedIOFlag
    ) {
        // srcHi is inclusive, and we also copy 1 extra entry due to N+1 aux vector structure
        final long len = (srcHi + 1 - srcLo + 1) << LEGACY_VAR_SIZE_AUX_SHL;
        final long fromAddress = src + (srcLo << LEGACY_VAR_SIZE_AUX_SHL);
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstFd), fromAddress, len, dstFixFileOffset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy fixed column prefix [fd=")
                        .put(dstFd).put(", len=").put(len).put(", offset=").put(fromAddress).put(']');
            }
        } else {
            Vect.memcpy(dstFixAddr, fromAddress, len);
        }
    }

    @Override
    public void o3setColumnRefs(long address, long initialOffset, long count) {
        Vect.setVarColumnRefs32Bit(address, initialOffset, count);
    }

    @Override
    public void o3shiftCopyAuxVector(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        O3Utils.shiftCopyFixedSizeColumnData(
                shift,
                src,
                srcLo,
                srcHi,
                dstAddr
        );
    }

    @Override
    public void o3sort(
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
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
        final long tgtAuxAddr = dstAuxMem.resize(timestampMergeIndexSize * Long.BYTES);

        assert srcDataAddr != 0;
        assert srcAuxAddr != 0;
        assert tgtDataAddr != 0;
        assert tgtAuxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
        final long offset = Vect.sortVarColumn(
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(timestampMergeIndexSize * Long.BYTES);
        dstAuxMem.putLong(offset);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, long rowCount) {
        // For STRING storage aux vector (mem) contains N+1 offsets. Where N is the
        // row count. Offset indexes are 0 based, so reading Nth element of the vector gives
        // the size of the data vector.
        auxMem.jumpTo(rowCount << LEGACY_VAR_SIZE_AUX_SHL);
        // it is safe to read offset from the raw memory pointer because paged
        // memories (which MemoryMA is) have power-of-2 page size.

        final long dataMemOffset = rowCount > 0 ? Unsafe.getUnsafe().getLong(auxMem.getAppendAddress()) : 0;

        // Jump to the end of file to correctly trim the file
        auxMem.jumpTo((rowCount + 1) << LEGACY_VAR_SIZE_AUX_SHL);
        return dataMemOffset;
    }

    @Override
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem, boolean doubleAllocate) {
        if (pos > 0) {
            if (doubleAllocate) {
                auxMem.allocate(pos * Long.BYTES + Long.BYTES);
            }
            // Jump to the number of records written to read length of var column correctly
            auxMem.jumpTo(pos * Long.BYTES);
            long m1pos = Unsafe.getUnsafe().getLong(auxMem.getAppendAddress());
            // Jump to the end of file to correctly trim the file
            auxMem.jumpTo((pos + 1) * Long.BYTES);
            long dataSizeBytes = m1pos + (pos + 1) * Long.BYTES;
            if (doubleAllocate) {
                dataMem.allocate(m1pos);
            }
            dataMem.jumpTo(m1pos);
            return dataSizeBytes;
        }

        dataMem.jumpTo(0);
        auxMem.jumpTo(0);
        auxMem.putLong(0);
        // Assume var length columns use 28 bytes per value to estimate the record size
        // if there are no rows in the partition yet.
        // The record size used to estimate the partition size
        // to split partition in O3 commit when necessary
        return TableUtils.ESTIMATED_VAR_COL_SIZE;
    }

    static long findVarOffset(long srcFixAddr, long srcLo) {
        long result = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
        assert (srcLo == 0 && result == 0) || result > 0;
        return result;
    }
}
