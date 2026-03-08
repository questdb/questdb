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
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

import static io.questdb.cairo.ColumnType.LEGACY_VAR_SIZE_AUX_SHL;

public class StringTypeDriver implements ColumnTypeDriver {
    public static final StringTypeDriver INSTANCE = new StringTypeDriver();

    public static void appendValue(MemoryA auxMem, MemoryA dataMem, CharSequence value) {
        auxMem.putLong(dataMem.putStr(value));
    }

    @Override
    public void appendNull(MemoryA auxMem, MemoryA dataMem) {
        auxMem.putLong(dataMem.putNullStr());
    }

    @Override
    public long auxRowsToBytes(long rowCount) {
        return rowCount << LEGACY_VAR_SIZE_AUX_SHL;
    }

    @Override
    public void configureAuxMemMA(MemoryMA auxMem) {
        auxMem.putLong(0);
    }

    @Override
    public void configureAuxMemMA(FilesFacade ff, MemoryMA auxMem, LPSZ fileName, long dataAppendPageSize, int memoryTag, int opts, int madviseOpts) {
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
    public void configureAuxMemO3RSS(MemoryARW auxMem) {
        // string starts with 8-byte offset
        auxMem.putLong(0);
    }

    @Override
    public void configureAuxMemOM(FilesFacade ff, MemoryOM auxMem, long fd, LPSZ fileName, long rowLo, long rowHi, int memoryTag, int opts) {
        auxMem.ofOffset(
                ff,
                fd,
                false,
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
            long dataFd,
            LPSZ fileName,
            long rowLo,
            long rowHi,
            int memoryTag,
            int opts
    ) {
        dataMem.ofOffset(
                ff,
                dataFd,
                false,
                fileName,
                auxMem.getLong(rowLo << LEGACY_VAR_SIZE_AUX_SHL),
                auxMem.getLong(rowHi << LEGACY_VAR_SIZE_AUX_SHL),
                memoryTag,
                opts
        );
    }

    @Override
    public long dedupMergeVarColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr) {
        return Vect.dedupMergeStrBinColumnSize(mergeIndexAddr, mergeIndexCount, srcDataFixAddr, srcOooFixAddr);
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
        // It's tempting to assert the result.
        //        assert (row == 0 && result == 0) || result > 0;
        // However, we can't do that, because the partition attach/detach mechanism has to be able to gracefully
        // recover from attempts to attach damaged partition data. Throwing AssertError makes it impossible,
        // unless we want to catch AssertError in the partition attach code.
        return Unsafe.getUnsafe().getLong(auxMemAddr + (row << LEGACY_VAR_SIZE_AUX_SHL));
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
    public long getDataVectorSizeAtFromFd(FilesFacade ff, long auxFd, long row) {
        long auxFileOffset = getAuxVectorOffset(row + 1);
        long dataOffset = row > -1 ? ff.readNonNegativeLong(auxFd, auxFileOffset) : 0;

        if (dataOffset < 0 || dataOffset > 1L << 40 || (row > -1 && dataOffset == 0)) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid variable file length offset read from offset file [auxFd=").put(auxFd)
                    .put(", offset=").put(auxFileOffset)
                    .put(", fileSize=").put(ff.length(auxFd))
                    .put(", result=").put(dataOffset)
                    .put(']');
        }
        return dataOffset;
    }

    @Override
    public long getMinAuxVectorSize() {
        return Long.BYTES;
    }

    @Override
    public boolean isSparseDataVector(long auxMemAddr, long dataMemAddr, long rowCount) {
        for (int row = 0; row < rowCount; row++) {
            long offset = Unsafe.getUnsafe().getLong(auxMemAddr + (long) row * Long.BYTES);
            long iLen = Unsafe.getUnsafe().getLong(auxMemAddr + (long) (row + 1) * Long.BYTES) - offset;
            long dLen = Unsafe.getUnsafe().getInt(dataMemAddr + offset);
            int lenLen = 4;
            long dataLen = dLen * 2;
            long dStorageLen = dLen > 0 ? dataLen + lenLen : lenLen;
            if (iLen != dStorageLen) {
                // Swiss cheese hole in var col file
                return true;
            }
        }
        return false;

    }

    @Override
    public long mergeShuffleColumnFromManyAddresses(
            long indexFormat,
            long primaryAddressList,
            long secondaryAddressList,
            long outPrimaryAddress,
            long outSecondaryAddress,
            long mergeIndex,
            long destVarOffset,
            long destDataSize
    ) {
        return Vect.mergeShuffleStringColumnFromManyAddresses(
                indexFormat,
                (int) getDataVectorMinEntrySize(),
                primaryAddressList,
                secondaryAddressList,
                outPrimaryAddress,
                outSecondaryAddress,
                mergeIndex,
                destVarOffset,
                destDataSize
        );
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
            long srcAddr,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstFileOffset,
            long dstFd,
            boolean mixedIOFlag
    ) {
        // srcHi is inclusive, and we also copy 1 extra entry due to N+1 aux vector structure
        final long len = (srcHi + 1 - srcLo + 1) << LEGACY_VAR_SIZE_AUX_SHL;
        O3Utils.copyFixedSizeCol(
                ff,
                srcAddr,
                srcLo,
                dstAddr,
                dstFileOffset,
                dstFd,
                mixedIOFlag,
                len,
                LEGACY_VAR_SIZE_AUX_SHL
        );
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

        assert srcDataAddr != 0;
        assert srcAuxAddr != 0;
        assert tgtDataAddr != 0;
        assert tgtAuxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
        final long offset = Vect.sortStringColumn(
                sortedTimestampsAddr,
                sortedTimestampsRowCount,
                srcDataAddr,
                srcAuxAddr,
                tgtDataAddr,
                tgtAuxAddr
        );
        dstDataMem.jumpTo(offset);
        dstAuxMem.jumpTo(sortedTimestampsRowCount << LEGACY_VAR_SIZE_AUX_SHL);
        dstAuxMem.putLong(offset);
    }

    @Override
    public long setAppendAuxMemAppendPosition(MemoryMA auxMem, MemoryMA dataMem, int columnType, long rowCount) {
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
    public long setAppendPosition(long pos, MemoryMA auxMem, MemoryMA dataMem) {
        if (pos > 0) {
            // Jump to the number of records written to read length of var column correctly
            auxMem.jumpTo(pos << LEGACY_VAR_SIZE_AUX_SHL);
            long m1pos = Unsafe.getUnsafe().getLong(auxMem.getAppendAddress());
            // Jump to the end of file to correctly trim the file
            auxMem.jumpTo((pos + 1) << LEGACY_VAR_SIZE_AUX_SHL);
            long dataSizeBytes = m1pos + ((pos + 1) << LEGACY_VAR_SIZE_AUX_SHL);
            dataMem.jumpTo(m1pos);
            return dataSizeBytes;
        }

        dataMem.jumpTo(0);
        auxMem.jumpTo(0);
        auxMem.putLong(0);
        return Long.BYTES;
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        Vect.memset(dataMemAddr, rowCount * Integer.BYTES, -1);
    }

    @Override
    public void setFullAuxVectorNull(long auxMemAddr, long rowCount) {
        Vect.setStringColumnNullRefs(auxMemAddr, 0, rowCount + 1);
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        Vect.setStringColumnNullRefs(auxMemAddr, initialOffset, columnTop);
    }

    @Override
    public void shiftCopyAuxVector(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstAddrSize
    ) {
        // +2 because
        // 1. srcHi is inclusive
        // 2. we copy 1 extra entry due to N+1 string aux vector structure
        assert (srcHi - srcLo + 2) * 8 <= dstAddrSize;
        Vect.shiftCopyFixedSizeColumnData(shift, src, srcLo, srcHi + 1, dstAddr);
    }
}
