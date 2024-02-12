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
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.ColumnType.LEGACY_VAR_SIZE_AUX_SHL;
import static io.questdb.cairo.O3CopyJob.copyFixedSizeCol;
import static io.questdb.cairo.O3OpenColumnJob.*;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

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
    public long getDataVectorOffset(long auxMemAddr, long row) {
        return findVarOffset(auxMemAddr, row);
    }

    @Override
    public long getDataVectorSize(long auxMemAddr, long rowLo, long rowHi) {
        return getDataVectorOffset(auxMemAddr, rowHi + 1) - getDataVectorOffset(auxMemAddr, rowLo);
    }

    @Override
    public void o3ColumnCopy(
            FilesFacade ff,
            long srcAuxAddr,
            long srcDataAddr,
            long srcLo,
            long srcHi,
            long dstAuxAddr,
            int dstAuxFd,
            long dstAuxFileOffset,
            long dstDataAddr,
            int dstDataFd,
            long dstDataOffset,
            long dstDataAdjust,
            long dstDataSize,
            boolean mixedIOFlag
    ) {
        // we can find out the edge of string column in one of two ways
        // 1. if srcOooHi is at the limit of the page - we need to copy the whole page of strings
        // 2  if there are more items behind srcOooHi we can get offset of srcOooHi+1

        final long lo = findVarOffset(srcAuxAddr, srcLo);
        assert lo >= 0;
        final long hi = findVarOffset(srcAuxAddr, srcHi + 1);
        assert hi >= lo;
        // copy this before it changes
        final long len = hi - lo;
        assert len <= Math.abs(dstDataSize) - dstDataOffset;
        final long offset = dstDataOffset + dstDataAdjust;
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstDataFd), srcDataAddr + lo, len, offset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy var data column prefix [fd=").put(dstDataFd).put(", offset=").put(offset).put(", len=").put(len).put(']');
            }
        } else {
            Vect.memcpy(dstDataAddr + dstDataOffset, srcDataAddr + lo, len);
        }
        if (lo == offset) {
            copyFixedSizeCol(
                    ff,
                    srcAuxAddr,
                    srcLo,
                    srcHi + 1,
                    dstAuxAddr,
                    dstAuxFileOffset,
                    dstAuxFd,
                    3,
                    mixedIOFlag
            );
        } else {
            o3shiftCopyAuxVector(lo - offset, srcAuxAddr, srcLo, srcHi + 1, dstAuxAddr);
        }
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
    public void o3MoveLag(long rowCount, long columnRowCount, long lagRowCount, MemoryCR srcAuxMem, MemoryCR srcDataMem, MemoryARW dstAuxMem, MemoryARW dstDataMem) {
        long committedIndexOffset = columnRowCount << 3;
        final long sourceOffset = srcAuxMem.getLong(committedIndexOffset);
        final long size = srcAuxMem.getLong((columnRowCount + rowCount) << 3) - sourceOffset;
        final long destOffset = lagRowCount == 0 ? 0L : dstAuxMem.getLong(lagRowCount << 3);

        // adjust append position of the index column to
        // maintain n+1 number of entries
        dstAuxMem.jumpTo((lagRowCount + rowCount + 1) << 3);

        // move count + 1 rows, to make sure index column remains n+1
        // the data is copied back to start of the buffer, no need to set size first
        o3shiftCopyAuxVector(
                sourceOffset - destOffset,
                srcAuxMem.addressOf(committedIndexOffset),
                0,
                rowCount, // No need to do +1 here, hi is inclusive
                dstAuxMem.addressOf(lagRowCount << 3)
        );
        dstDataMem.jumpTo(destOffset + size);
        assert srcDataMem.size() >= size;
        Vect.memmove(dstDataMem.addressOf(destOffset), srcDataMem.addressOf(sourceOffset), size);
    }

    @Override
    public void o3PartitionAppend(
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int activeFixFd,
            int activeVarFd,
            MemoryMA dstFixMem,
            MemoryMA dstVarMem,
            long dstRowCount,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            long partitionUpdateSinkAddr
    ) {
        long dstFixAddr = 0;
        long dstFixOffset;
        long dstFixFileOffset;
        long dstVarAddr = 0;
        long dstVarOffset;
        long dstVarAdjust;
        long dstVarSize = 0;
        long dstFixSize = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

            long l = columnTypeDriver.getDataVectorSize(srcOooFixAddr, srcOooLo, srcOooHi);
            dstFixSize = (dstRowCount + 1) * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize || dstVarMem.getAppendAddressSize() < l) {
                assert dstFixMem == null || dstFixMem.getAppendOffset() - Long.BYTES == (srcDataMax - srcDataTop) * Long.BYTES;

                dstFixOffset = (srcDataMax - srcDataTop) * Long.BYTES;
                dstFixFileOffset = dstFixOffset;
                dstFixAddr = mapRW(ff, Math.abs(activeFixFd), dstFixSize, MemoryTag.MMAP_O3);

                if (dstFixOffset > 0) {
                    dstVarOffset = Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset);
                } else {
                    dstVarOffset = 0;
                }

                dstVarSize = l + dstVarOffset;
                dstVarAddr = mapRW(ff, Math.abs(activeVarFd), dstVarSize, MemoryTag.MMAP_O3);
                dstVarAdjust = 0;
            } else {
                assert dstFixMem.getAppendOffset() >= Long.BYTES;
                assert dstFixMem.getAppendOffset() - Long.BYTES == (srcDataMax - srcDataTop) * Long.BYTES;

                dstFixAddr = dstFixMem.getAppendAddress() - Long.BYTES;
                dstVarAddr = dstVarMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixFileOffset = dstFixMem.getAppendOffset() - Long.BYTES;
                dstFixSize = -dstFixSize;
                dstVarOffset = 0;
                dstVarSize = -l;
                dstVarAdjust = dstVarMem.getAppendOffset();
            }
        } catch (Throwable e) {
            LOG.error().$("append var error [table=").utf8(tableWriter.getTableToken().getTableName())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.unmapAndClose(ff, activeFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff, activeVarFd, dstVarAddr, dstVarSize);
            freeTimestampIndex(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff
            );
            throw e;
        }

        o3PublishCopyTask(
                columnCounter,
                null,
                columnType,
                O3_BLOCK_O3,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcOooLo,
                srcOooHi,
                srcDataTop,
                srcDataMax,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                partitionTimestamp, // <-- pass thru
                activeFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixFileOffset,
                dstFixSize,
                activeVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarAdjust,
                dstVarSize,
                0,
                0,
                0,
                0,
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                null,
                partitionUpdateSinkAddr
        );
    }

    @Override
    public void o3PartitionMerge(
            Path pathToNewPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeOOOLo,
            long mergeOOOHi,
            long mergeDataLo,
            long mergeDataHi,
            long mergeLen,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int srcDataFixFd,
            int srcDataVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            long colTopSinkAddr,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        int partCount = 0;
        int dstVarFd = 0;
        long dstVarAddr = 0;
        long srcDataFixOffset;
        long srcDataFixAddr = 0;
        long dstVarSize = 0;
        long srcDataTopOffset;
        long dstFixSize = 0;
        long dstFixAppendOffset1;
        long srcDataFixSize = 0;
        long srcDataVarSize = 0;
        long dstVarAppendOffset2;
        long dstFixAppendOffset2;
        int dstFixFd = 0;
        long dstFixAddr = 0;
        long srcDataVarAddr = 0;
        long srcDataVarOffset = 0;
        long dstVarAppendOffset1 = 0;
        final int srcFixFd = Math.abs(srcDataFixFd);
        final int srcVarFd = Math.abs(srcDataVarFd);
        final FilesFacade ff = tableWriter.getFilesFacade();
        final boolean mixedIOFlag = tableWriter.allowMixedIO();
        final long initialSrcDataTop = srcDataTop;

        try {
            pathToNewPartition.trimTo(pplen);
            if (srcDataTop > 0) {
                // Size of data actually in the index (fixed) file.
                final long srcDataActualBytes = (srcDataMax - srcDataTop) * Long.BYTES;
                // Size of data in the index (fixed) file if it didn't have column top.
                final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // Extend the existing column down, we will be discarding it anyway.
                    // Materialize nulls at the end of the column and add non-null data to merge.
                    // Do all of this beyond existing written data, using column as a buffer.
                    // It is also fine in case when last partition contains WAL LAG, since
                    // At the beginning of the transaction LAG is copied into memory buffers (o3 mem columns).

                    srcDataFixSize = srcDataActualBytes + Long.BYTES + srcDataMaxBytes + Long.BYTES;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    if (srcDataActualBytes > 0) {
                        srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataActualBytes);
                    }

                    // at bottom of source var column set length of strings to null (-1) for as many strings
                    // as srcDataTop value.
                    srcDataVarOffset = srcDataVarSize;
                    long reservedBytesForColTopNulls;
                    // We need to reserve null values for every column top value
                    // in the variable len file. Each null value takes 4 bytes for string
                    reservedBytesForColTopNulls = srcDataTop * (ColumnType.isString(columnType) ? Integer.BYTES : Long.BYTES);
                    srcDataVarSize += reservedBytesForColTopNulls + srcDataVarSize;
                    srcDataVarAddr = mapRW(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);

                    // Set var column values to null first srcDataTop times
                    // Next line should be:
                    // Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);
                    // But we can replace it with memset setting each byte to -1
                    // because binary repr of int -1 is 4 bytes of -1
                    // memset is faster than any SIMD implementation we can come with
                    Vect.memset(srcDataVarAddr + srcDataVarOffset, (int) reservedBytesForColTopNulls, -1);

                    // Copy var column data
                    Vect.memcpy(srcDataVarAddr + srcDataVarOffset + reservedBytesForColTopNulls, srcDataVarAddr, srcDataVarOffset);

                    // we need to shift copy the original column so that new block points at strings "below" the
                    // nulls we created above
                    long hiInclusive = srcDataMax - srcDataTop; // STOP. DON'T ADD +1 HERE. srcHi is inclusive, no need to do +1
                    assert srcDataFixSize >= srcDataMaxBytes + (hiInclusive + 1) * 8; // make sure enough len mapped
                    o3shiftCopyAuxVector(
                            -reservedBytesForColTopNulls,
                            srcDataFixAddr,
                            0,
                            hiInclusive,
                            srcDataFixAddr + srcDataMaxBytes + Long.BYTES
                    );

                    // now set the "empty" bit of fixed size column with references to those
                    // null strings we just added
                    // Call to setVarColumnRefs32Bit must be after shiftCopyFixedSizeColumnData
                    // because data first have to be shifted before overwritten
                    if (ColumnType.isString(columnType)) {
                        Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes + Long.BYTES, 0, srcDataTop);
                    } else {
                        Vect.setVarColumnRefs64Bit(srcDataFixAddr + srcDataActualBytes + Long.BYTES, 0, srcDataTop);
                    }

                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes + Long.BYTES;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    if (prefixType != O3_BLOCK_NONE) {
                        // Set the column top if it's not split partition case.
                        Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataTop);
                        // For split partition, old partition column top will remain the same.
                        // And the new partition will not have any column top since srcDataTop <= prefixHi.
                    }
                    srcDataFixSize = srcDataActualBytes + Long.BYTES;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    srcDataFixOffset = 0;

                    srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES);
                    srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);
                }
            } else {
                // var index column is n+1
                srcDataFixSize = (srcDataMax + 1) * Long.BYTES;
                srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                srcDataFixOffset = 0;

                srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES);
                srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = srcDataTop * Long.BYTES;

            iFile(pathToNewPartition.trimTo(pplen), columnName, columnNameTxn);
            dstFixFd = openRW(ff, pathToNewPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop + 1) * Long.BYTES;
            if (prefixType == O3_BLOCK_NONE) {
                // split partition
                dstFixSize -= (prefixHi - srcDataTop + 1) * Long.BYTES;
            }
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            if (!mixedIOFlag) {
                ff.madvise(dstFixAddr, dstFixSize, Files.POSIX_MADV_RANDOM);
            }

            dFile(pathToNewPartition.trimTo(pplen), columnName, columnNameTxn);
            dstVarFd = openRW(ff, pathToNewPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            dstVarSize = srcDataVarSize - srcDataVarOffset
                    + getDataVectorSize(srcOooFixAddr, srcOooLo, srcOooHi);

            if (prefixType == O3_BLOCK_NONE && prefixHi > initialSrcDataTop) {
                // split partition
                assert prefixLo == 0;
                dstVarSize -= getDataVectorSize(srcDataFixAddr, prefixLo, prefixHi - initialSrcDataTop);
            }

            dstVarAddr = mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3);
            if (!mixedIOFlag) {
                ff.madvise(dstVarAddr, dstVarSize, Files.POSIX_MADV_RANDOM);
            }

            if (prefixType == O3_BLOCK_DATA) {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - srcDataTop) * Long.BYTES;
                prefixHi -= srcDataTop;
            } else if (prefixType == O3_BLOCK_NONE) {
                // split partition
                dstFixAppendOffset1 = 0;
            } else {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;
            }

            if (suffixType == O3_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            // configure offsets
            switch (prefixType) {
                case O3_BLOCK_O3:
                    dstVarAppendOffset1 = getDataVectorSize(srcOooFixAddr, prefixLo, prefixHi);
                    partCount++;
                    break;
                case O3_BLOCK_DATA:
                    dstVarAppendOffset1 = getDataVectorSize(
                            srcDataFixAddr + srcDataFixOffset,
                            prefixLo,
                            prefixHi
                    );
                    partCount++;
                    break;
                default:
                    break;
            }

            // offset 2
            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                if (mergeLen == mergeDataHi - mergeDataLo + 1 + mergeOOOHi - mergeOOOLo + 1) {
                    // No deduplication, all rows from O3 and column data will be written.
                    // In this case var col length is calculated as o3 var col len + data var col len
                    long oooLen = getDataVectorSize(srcOooFixAddr, mergeOOOLo, mergeOOOHi);
                    long dataLen = getDataVectorSize(srcDataFixAddr + srcDataFixOffset, mergeDataLo - srcDataTop, mergeDataHi - srcDataTop);
                    dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
                } else {
                    // Deduplication happens, some rows are eliminated.
                    // Dedup eliminates some rows, there is no way to know the append offset of var file beforehand.
                    // Dedup usually reduces the size of the var column, but in some cases it can increase it.
                    // For example if var col in src has 3 rows of data
                    // '1'
                    // '1'
                    // '1'
                    // and all rows match single row in o3 with value
                    // 'long long value'
                    // the result will be 3 rows with new new value
                    // 'long long value'
                    // 'long long value'
                    // 'long long value'
                    // Which is longer than oooLen + dataLen
                    // To deal with unpredicatability of the dedup var col size run the dedup merged size calculation
                    dstVarAppendOffset2 = dstVarAppendOffset1 + Vect.dedupMergeVarColumnLen(
                            timestampMergeIndexAddr,
                            timestampMergeIndexSize / TIMESTAMP_MERGE_ENTRY_BYTES,
                            srcDataFixAddr + srcDataFixOffset - srcDataTop * 8,
                            srcOooFixAddr
                    );
                }
            } else {
                dstFixAppendOffset2 = dstFixAppendOffset1;
                dstVarAppendOffset2 = dstVarAppendOffset1;
            }

            if (mergeType != O3_BLOCK_NONE) {
                partCount++;
            }

            if (suffixType != O3_BLOCK_NONE) {
                partCount++;
            }
        } catch (Throwable e) {
            LOG.error().$("merge var error [table=").utf8(tableWriter.getTableToken().getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    srcDataVarFd,
                    srcDataVarAddr,
                    srcDataVarSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstFixFd,
                    dstFixAddr,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarSize,
                    0,
                    0,
                    tableWriter
            );
            throw e;
        }

        partCounter.set(partCount);

        o3PublishCopyTasks(
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixOffset,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarOffset,
                srcDataVarSize,
                srcDataTopOffset,
                srcDataMax,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooPartitionMin,
                oooPartitionHi,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                suffixType,
                suffixLo,
                suffixHi,
                dstFixFd,
                dstFixAddr,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarSize,
                dstFixAppendOffset1,
                dstFixAppendOffset2,
                dstVarAppendOffset1,
                dstVarAppendOffset2,
                0,
                0,
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                null,
                srcDataTopOffset >> 2,
                partitionUpdateSinkAddr
        );
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
