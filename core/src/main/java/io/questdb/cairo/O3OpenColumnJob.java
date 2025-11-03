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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3OpenColumnJob extends AbstractQueueConsumerJob<O3OpenColumnTask> {
    public static final int OPEN_LAST_PARTITION_FOR_APPEND = 2;
    public static final int OPEN_LAST_PARTITION_FOR_MERGE = 4;
    public static final int OPEN_MID_PARTITION_FOR_APPEND = 1;
    public static final int OPEN_MID_PARTITION_FOR_MERGE = 3;
    public static final int OPEN_NEW_PARTITION_FOR_APPEND = 5;
    private final static Log LOG = LogFactory.getLog(O3OpenColumnJob.class);

    public O3OpenColumnJob(MessageBus messageBus) {
        super(messageBus.getO3OpenColumnQueue(), messageBus.getO3OpenColumnSubSeq());
    }

    public static void appendLastPartition(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
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
            MemoryMA dstFixMem,
            MemoryMA dstVarMem,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        if (ColumnType.isVarSize(columnType)) {
            o3VarSizePartitionAppend(
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    indexBlockCapacity,
                    0,
                    0,
                    0,
                    -dstFixMem.getFd(),
                    -dstVarMem.getFd(),
                    dstFixMem,
                    dstVarMem,
                    dstLen,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    partitionUpdateSinkAddr
            );
        } else if (ColumnType.isDesignatedTimestamp(columnType)) {
            appendTimestampColumn(
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataMax,
                    indexBlockCapacity,
                    -dstFixMem.getFd(),
                    0,
                    0,
                    dstFixMem,
                    dstLen,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    partitionUpdateSinkAddr
            );
        } else {
            appendFixColumn(
                    pathToPartition,
                    plen,
                    columnName,
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    indexBlockCapacity,
                    0,
                    0,
                    0,
                    -dstFixMem.getFd(),
                    dstFixMem,
                    dstLen,
                    tableWriter,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    indexWriter,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        }
    }

    public static void freeTimestampIndex(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            FilesFacade ff,
            boolean isOom
    ) {
        tableWriter.o3BumpErrorCount(isOom);
        if (columnCounter.decrementAndGet() == 0) {
            O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
            O3Utils.close(ff, srcTimestampFd);
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
            if (timestampMergeIndexAddr != 0) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            }
        }
    }

    public static boolean isOpenColumnModeForAppend(int openColumnMode) {
        switch (openColumnMode) {
            case OPEN_MID_PARTITION_FOR_APPEND:
            case OPEN_LAST_PARTITION_FOR_APPEND:
            case OPEN_NEW_PARTITION_FOR_APPEND:
                return true;
            default:
                return false;
        }
    }

    public static void mergeVarColumn(
            Path pathToNewPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long o3AuxAddr,
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
            long mergeRowCount,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataFixFd,
            long srcDataVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            long colTopSinkAddr,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        int partCount = 0;
        long dstDataFd = 0;
        long dstVarAddr = 0;
        long srcDataFixOffset;
        long srcAuxAddr = 0;
        long newAuxSize = 0;
        long dstDataSize = 0;
        long srcDataTopOffset;
        long dstAuxSize = 0;
        long dstAuxAppendOffset1;
        long srcDataSize = 0;
        long dstDataAppendOffset2;
        long dstAuxAppendOffset2;
        long dstAuxFd = 0;
        long dstAuxAddr = 0;
        long srcDataAddr = 0;
        long srcDataOffset = 0;
        long dstDataAppendOffset1 = 0;
        final long srcFixFd = Math.abs(srcDataFixFd);
        final long srcVarFd = Math.abs(srcDataVarFd);
        final FilesFacade ff = tableWriter.getFilesFacade();
        final boolean mixedIOFlag = tableWriter.allowMixedIO();
        try {
            pathToNewPartition.trimTo(pplen);
            final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
            if (srcDataTop > 0 && tableWriter.isCommitReplaceMode()) {
                long dataMax = 0;
                if (prefixType == O3_BLOCK_DATA && prefixHi >= prefixLo) {
                    dataMax = prefixHi + 1;
                }
                if (suffixType == O3_BLOCK_DATA && suffixHi >= suffixLo) {
                    dataMax = suffixHi + 1;
                }
                srcDataMax = Math.min(srcDataMax, dataMax);
                srcDataTop = Math.min(srcDataTop, dataMax);
            }

            if (srcDataTop > 0) {
                // srcDataMax is the row count in the existing column data
                final long auxRowCount = srcDataMax - srcDataTop;
                // Size of data actually in the aux (fixed) file,
                // THIS IS N+1 size, it used to be N offset, e.g. this used to be pointing at
                // the last row of the aux vector (column)
                final long oldAuxSize = columnTypeDriver.getAuxVectorSize(auxRowCount);

                // Size of data in the index (fixed) file if it didn't have column top.
                // this size DOES NOT INCLUDE N+1, so it is N here
                final long wouldBeAuxSize = columnTypeDriver.getAuxVectorSize(srcDataMax);

                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // Extend the existing column down, we will be discarding it anyway.
                    // Materialize nulls at the end of the column and add non-null data to merge.
                    // Do all of this beyond existing written data, using column as a buffer.
                    // It is also fine in case when last partition contains WAL LAG, since
                    // At the beginning of the transaction LAG is copied into memory buffers (o3 mem columns).
                    newAuxSize =
                            columnTypeDriver.getAuxVectorSize(auxRowCount) +
                                    columnTypeDriver.getAuxVectorSize(srcDataMax);

                    srcAuxAddr = mapRW(ff, srcFixFd, newAuxSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcAuxAddr, newAuxSize, Files.POSIX_MADV_SEQUENTIAL);
                    if (auxRowCount > 0) {
                        srcDataSize = columnTypeDriver.getDataVectorSizeAt(srcAuxAddr, auxRowCount - 1);
                    }

                    // at bottom of source var column set length of strings to null (-1) for as many strings
                    // as srcDataTop value.
                    srcDataOffset = srcDataSize;
                    // We need to reserve null values for every column top value
                    // in the variable len file. Each null value takes 4 bytes for string
                    final long reservedBytesForColTopNulls = srcDataTop * columnTypeDriver.getDataVectorMinEntrySize();
                    srcDataSize += reservedBytesForColTopNulls + srcDataSize;
                    srcDataAddr = srcDataSize > 0 ? mapRW(ff, srcVarFd, srcDataSize, MemoryTag.MMAP_O3) : srcDataAddr;
                    ff.madvise(srcDataAddr, srcDataSize, Files.POSIX_MADV_SEQUENTIAL);

                    // Set var column values to null first srcDataTop times
                    // Next line should be:
                    // Vect.setMemoryInt(srcDataAddr + srcDataOffset, -1, srcDataTop);
                    // But we can replace it with memset setting each byte to -1
                    // because binary repr of int -1 is 4 bytes of -1
                    // memset is faster than any SIMD implementation we can come with
                    columnTypeDriver.setDataVectorEntriesToNull(srcDataAddr + srcDataOffset, srcDataTop);

                    // Copy var column data
                    Vect.memcpy(srcDataAddr + srcDataOffset + reservedBytesForColTopNulls, srcDataAddr, srcDataOffset);

                    // we need to shift copy the original column so that new block points at strings "below" the
                    // nulls we created above
                    long dstAddr = srcAuxAddr + wouldBeAuxSize;
                    long dstAddrSize = newAuxSize - wouldBeAuxSize;
                    columnTypeDriver.shiftCopyAuxVector(
                            -reservedBytesForColTopNulls,
                            srcAuxAddr,
                            0,
                            auxRowCount - 1, // inclusive
                            dstAddr,
                            dstAddrSize
                    );

                    // now set the "empty" bit of fixed size column with references to those
                    // null strings we just added
                    // Call to o3setColumnRefs must be after o3shiftCopyAuxVector
                    // because data first have to be shifted before overwritten
                    columnTypeDriver.setPartAuxVectorNull(srcAuxAddr + oldAuxSize, 0, srcDataTop);
                    srcDataTop = 0;
                    srcDataFixOffset = oldAuxSize;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    if (prefixType != O3_BLOCK_NONE) {
                        // Set the column top if it's not split partition case.
                        Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataTop);
                        // For split partition, old partition column top will remain the same.
                        // And the new partition will not have any column top since srcDataTop <= prefixHi.
                    }

                    srcDataFixOffset = 0;
                    if (auxRowCount > 0) {
                        newAuxSize = columnTypeDriver.getAuxVectorSize(auxRowCount);
                        srcAuxAddr = mapRW(ff, srcFixFd, newAuxSize, MemoryTag.MMAP_O3);
                        ff.madvise(srcAuxAddr, newAuxSize, Files.POSIX_MADV_SEQUENTIAL);

                        srcDataSize = columnTypeDriver.getDataVectorSizeAt(srcAuxAddr, auxRowCount - 1);
                        srcDataAddr = srcDataSize > 0 ? mapRO(ff, srcVarFd, srcDataSize, MemoryTag.MMAP_O3) : 0;
                        ff.madvise(srcDataAddr, srcDataSize, Files.POSIX_MADV_SEQUENTIAL);
                    }
                }
            } else {
                srcDataFixOffset = 0;
                if (srcDataMax > 0) {
                    newAuxSize = columnTypeDriver.getAuxVectorSize(srcDataMax);
                    srcAuxAddr = mapRW(ff, srcFixFd, newAuxSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcAuxAddr, newAuxSize, Files.POSIX_MADV_SEQUENTIAL);
                    srcDataSize = columnTypeDriver.getDataVectorSizeAt(srcAuxAddr, srcDataMax - 1);
                    srcDataAddr = srcDataSize > 0 ? mapRO(ff, srcVarFd, srcDataSize, MemoryTag.MMAP_O3) : 0;
                }
                ff.madvise(srcDataAddr, srcDataSize, Files.POSIX_MADV_SEQUENTIAL);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = columnTypeDriver.getAuxVectorOffset(srcDataTop);

            dstAuxFd = openRW(ff, iFile(pathToNewPartition.trimTo(pplen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());

            // Use target partition size to determine the size of fixed file, it's already compensated
            // for partitions splits and duplicates found by dedup
            long newRowCount = o3SplitPartitionSize > 0 ? o3SplitPartitionSize : srcDataNewPartitionSize - srcDataTop;
            dstAuxSize = columnTypeDriver.getAuxVectorSize(newRowCount);

            dstAuxAddr = mapRW(ff, dstAuxFd, dstAuxSize, MemoryTag.MMAP_O3);
            if (!mixedIOFlag) {
                ff.madvise(dstAuxAddr, dstAuxSize, Files.POSIX_MADV_RANDOM);
            }

            if (prefixType == O3_BLOCK_DATA) {
                dstAuxAppendOffset1 = columnTypeDriver.auxRowsToBytes(prefixHi - prefixLo + 1 - srcDataTop);
                prefixHi -= srcDataTop;
            } else if (prefixType == O3_BLOCK_NONE) {
                // split partition
                dstAuxAppendOffset1 = 0;
            } else {
                dstAuxAppendOffset1 = columnTypeDriver.auxRowsToBytes(prefixHi - prefixLo + 1);
            }

            if (suffixType == O3_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            // configure offsets
            switch (prefixType) {
                case O3_BLOCK_O3:
                    dstDataAppendOffset1 = columnTypeDriver.getDataVectorSize(o3AuxAddr, prefixLo, prefixHi);
                    partCount++;
                    break;
                case O3_BLOCK_DATA:
                    dstDataAppendOffset1 = columnTypeDriver.getDataVectorSize(
                            srcAuxAddr + srcDataFixOffset,
                            prefixLo,
                            prefixHi
                    );
                    partCount++;
                    break;
                default:
            }

            // offset 2
            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                final long mergeDataSize;
                if (mergeRowCount == mergeDataHi - mergeDataLo + 1 + mergeOOOHi - mergeOOOLo + 1) {
                    // No deduplication, all rows from O3 and column data will be written.
                    // In this case var col length is calculated as o3 var col len + data var col len
                    final long o3size = columnTypeDriver.getDataVectorSize(o3AuxAddr, mergeOOOLo, mergeOOOHi);
                    final long dataSize = columnTypeDriver.getDataVectorSize(
                            srcAuxAddr + srcDataFixOffset,
                            mergeDataLo - srcDataTop,
                            mergeDataHi - srcDataTop
                    );

                    mergeDataSize = o3size + dataSize;
                } else if (tableWriter.isCommitReplaceMode() && mergeType == O3_BLOCK_O3) {
                    mergeDataSize = columnTypeDriver.getDataVectorSize(o3AuxAddr, mergeOOOLo, mergeOOOHi);
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
                    // the result will be 3 rows with new value
                    // 'long long value'
                    // 'long long value'
                    // 'long long value'
                    // Which is longer than oooLen + dataLen
                    // To deal with unpredictability of the dedup var col size run the dedup merged size calculation
                    mergeDataSize = timestampMergeIndexAddr > 0 ? columnTypeDriver.dedupMergeVarColumnSize(
                            timestampMergeIndexAddr,
                            mergeRowCount,
                            srcAuxAddr + srcDataFixOffset - columnTypeDriver.getAuxVectorOffset(srcDataTop),
                            o3AuxAddr
                    ) : 0;
                }

                dstAuxAppendOffset2 = dstAuxAppendOffset1 + columnTypeDriver.getAuxVectorOffset(mergeRowCount);
                dstDataAppendOffset2 = dstDataAppendOffset1 + mergeDataSize;
            } else {
                dstAuxAppendOffset2 = dstAuxAppendOffset1;
                dstDataAppendOffset2 = dstDataAppendOffset1;
            }

            long suffixSize = suffixType == O3_BLOCK_DATA ?
                    columnTypeDriver.getDataVectorSize(
                            // No need to compensate for srcDataTop here,
                            // suffixLo, suffixHi are already adjusted for srcDataTop
                            srcAuxAddr + srcDataFixOffset,
                            suffixLo,
                            suffixHi
                    )
                    : 0;
            long suffixSize2 = suffixType == O3_BLOCK_O3
                    ? columnTypeDriver.getDataVectorSize(o3AuxAddr, suffixLo, suffixHi)
                    : 0;
            dstDataSize = dstDataAppendOffset2 + suffixSize + suffixSize2;

            dstDataFd = openRW(ff, dFile(pathToNewPartition.trimTo(pplen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            if (dstDataSize > 0) {
                dstVarAddr = mapRW(ff, dstDataFd, dstDataSize, MemoryTag.MMAP_O3);
                if (!mixedIOFlag) {
                    ff.madvise(dstVarAddr, dstDataSize, Files.POSIX_MADV_RANDOM);
                }
            }

            if (mergeType != O3_BLOCK_NONE) {
                partCount++;
            }

            if (suffixType != O3_BLOCK_NONE) {
                partCount++;
            }
        } catch (Throwable e) {
            LOG.error().$("merge var error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
            O3CopyJob.unmapAndCloseAllPartsComplete(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcDataFixFd,
                    srcAuxAddr,
                    newAuxSize,
                    srcDataVarFd,
                    srcDataAddr,
                    srcDataSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstAuxFd,
                    dstAuxAddr,
                    dstAuxSize,
                    dstDataFd,
                    dstVarAddr,
                    dstDataSize,
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
                srcAuxAddr,
                srcDataFixOffset,
                newAuxSize,
                srcDataVarFd,
                srcDataAddr,
                srcDataOffset,
                srcDataSize,
                srcDataTopOffset,
                srcDataMax,
                o3AuxAddr,
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
                dstAuxFd,
                dstAuxAddr,
                dstAuxSize,
                dstDataFd,
                dstVarAddr,
                dstDataSize,
                dstAuxAppendOffset1,
                dstAuxAppendOffset2,
                dstDataAppendOffset1,
                dstDataAppendOffset2,
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

    public static void o3PublishCopyTask(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcDataTop,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixFileOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr
    ) {
        long cursor = tableWriter.getO3CopyPubSeq().next();
        if (cursor > -1) {
            publishCopyTaskHarmonized(
                    columnCounter,
                    partCounter,
                    columnType,
                    blockType,
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
                    srcDataLo,
                    srcDataHi,
                    srcDataTop,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    timestampMin,
                    partitionTimestamp,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixFileOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr
            );
        } else {
            publishCopyTaskContended(
                    cursor,
                    columnCounter,
                    partCounter,
                    columnType,
                    blockType,
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
                    srcDataTop,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    timestampMin,
                    partitionTimestamp,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixFileOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr
            );
        }
    }

    public static void o3PublishCopyTasks(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataVarSize,
            long srcDataTopOffset,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp, // <-- pass thru
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long dstAuxAppendOffset1,
            long dstAuxAppendOffset2,
            long dstDataAppendOffset1,
            long dstDataAppendOffset2,
            long dstKFd,
            long dstVFd,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long dstIndexAdjust,
            long partitionUpdateSinkAddr
    ) {
        final boolean partitionMutates = true;

        int partsPublished = 0;
        int partsToPublish = partCounter.get();

        try {
            switch (prefixType) {
                case O3_BLOCK_O3:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            prefixType,
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
                            0,
                            0,
                            srcDataTopOffset,
                            srcDataMax,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            prefixLo,
                            prefixHi,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            0,
                            0,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            0,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                case O3_BLOCK_DATA:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            prefixType,
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
                            prefixLo,
                            prefixHi,
                            srcDataTopOffset,
                            srcDataMax,
                            0,
                            0,
                            0,
                            0,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            0,
                            0,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            0,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                default:
                    break;
            }

            switch (mergeType) {
                case O3_BLOCK_O3:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            mergeType,
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
                            0,
                            0,
                            srcDataTopOffset,
                            srcDataMax,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            mergeOOOLo,
                            mergeOOOHi,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            dstAuxAppendOffset1,
                            dstAuxAppendOffset1,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            dstDataAppendOffset1,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                case O3_BLOCK_DATA:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            mergeType,
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
                            mergeDataLo,
                            mergeDataHi,
                            srcDataTopOffset,
                            srcDataMax,
                            0,
                            0,
                            0,
                            0,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            dstAuxAppendOffset1,
                            dstAuxAppendOffset1,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            dstDataAppendOffset1,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                case O3_BLOCK_MERGE:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            mergeType,
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
                            mergeDataLo,
                            mergeDataHi,
                            srcDataTopOffset,
                            srcDataMax,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            mergeOOOLo,
                            mergeOOOHi,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            dstAuxAppendOffset1,
                            dstAuxAppendOffset1,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            dstDataAppendOffset1,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                default:
                    break;
            }

            switch (suffixType) {
                case O3_BLOCK_O3:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            suffixType,
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
                            0,
                            0,
                            srcDataTopOffset,
                            srcDataMax,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            suffixLo,
                            suffixHi,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            dstAuxAppendOffset2,
                            dstAuxAppendOffset2,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            dstDataAppendOffset2,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                case O3_BLOCK_DATA:
                    partsPublished++;
                    o3PublishCopyTask(
                            columnCounter,
                            partCounter,
                            columnType,
                            suffixType,
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
                            suffixLo,
                            suffixHi,
                            srcDataTopOffset,
                            srcDataMax,
                            0,
                            0,
                            0,
                            0,
                            srcOooMax,
                            srcOooLo,
                            srcOooHi,
                            timestampMin,
                            partitionTimestamp,
                            dstFixFd,
                            dstFixAddr,
                            dstAuxAppendOffset2,
                            dstAuxAppendOffset2,
                            dstFixSize,
                            dstVarFd,
                            dstVarAddr,
                            dstDataAppendOffset2,
                            0,
                            dstVarSize,
                            dstKFd,
                            dstVFd,
                            0,
                            dstIndexAdjust,
                            indexBlockCapacity,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            partitionMutates,
                            srcDataNewPartitionSize,
                            srcDataOldPartitionSize,
                            o3SplitPartitionSize,
                            tableWriter,
                            indexWriter,
                            partitionUpdateSinkAddr
                    );
                    break;
                default:
                    break;
            }
        } finally {
            if (partsPublished != partsToPublish) {
                // An exception happened, we need to adjust partCounter
                if (partCounter.addAndGet(partsPublished - partsToPublish) == 0) {
                    O3CopyJob.unmapAndCloseAllPartsComplete(
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
                            dstKFd,
                            dstVFd,
                            tableWriter
                    );
                }
            }
        }
    }

    public static void o3VarSizePartitionAppend(
            AtomicInteger columnCounter,
            int columnType,
            long srcOooAuxAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long activeFixFd,
            long activeVarFd,
            MemoryMA dstAuxMem,
            MemoryMA dstDataMem,
            long dstRowCount,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            long partitionUpdateSinkAddr
    ) {
        long dstAuxAddr = 0;
        long dstAuxOffset;
        long dstAuxFileOffset;
        long dstDataAddr = 0;
        long dstDataOffset;
        long dstDataAdjust;
        long dstDataSize = 0;
        long dstAuxSize = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

            long o3DataSize = columnTypeDriver.getDataVectorSize(srcOooAuxAddr, srcOooLo, srcOooHi);
            dstAuxSize = columnTypeDriver.getAuxVectorSize(dstRowCount);

            if (dstAuxMem == null || dstAuxMem.getAppendAddressSize() < dstAuxSize || dstDataMem.getAppendAddressSize() < o3DataSize) {
                assert dstAuxMem == null || dstAuxMem.getAppendOffset() - columnTypeDriver.getMinAuxVectorSize() == columnTypeDriver.getAuxVectorOffset(srcDataMax - srcDataTop);

                dstAuxOffset = columnTypeDriver.getAuxVectorOffset(srcDataMax - srcDataTop);
                dstAuxFileOffset = dstAuxOffset;
                dstAuxAddr = mapRW(ff, Math.abs(activeFixFd), dstAuxSize, MemoryTag.MMAP_O3);

                if (dstAuxOffset > 0) {
                    dstDataOffset = columnTypeDriver.getDataVectorSizeAt(dstAuxAddr, srcDataMax - 1 - srcDataTop);
                } else {
                    dstDataOffset = 0;
                }

                dstDataSize = o3DataSize + dstDataOffset;
                dstDataAddr = dstDataSize > 0 ? mapRW(ff, Math.abs(activeVarFd), dstDataSize, MemoryTag.MMAP_O3) : 0;
                dstDataAdjust = 0;
            } else {
                assert dstAuxMem.getAppendOffset() >= columnTypeDriver.getMinAuxVectorSize();
                assert dstAuxMem.getAppendOffset() - columnTypeDriver.getMinAuxVectorSize() == columnTypeDriver.getAuxVectorOffset(srcDataMax - srcDataTop);

                dstAuxFileOffset = columnTypeDriver.getAuxVectorOffset(srcDataMax - srcDataTop);
                // this formula is a derivative of:
                // dstAuxMem.getAppendOffset() - columnTypeDriver.getMinAuxVectorSize() == columnTypeDriver.getAuxVectorOffset(srcDataMax - srcDataTop);
                dstAuxAddr = dstAuxMem.getAppendAddress() - (dstAuxMem.getAppendOffset() - dstAuxFileOffset);
                dstDataAddr = dstDataMem.getAppendAddress();
                dstAuxOffset = 0;
                dstAuxSize = -dstAuxSize;
                dstDataOffset = 0;
                dstDataSize = -o3DataSize;
                dstDataAdjust = dstDataMem.getAppendOffset();
            }
        } catch (Throwable e) {
            LOG.error().$("append var error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.unmapAndClose(ff, activeFixFd, dstAuxAddr, dstAuxSize);
            O3Utils.unmapAndClose(ff, activeVarFd, dstDataAddr, dstDataSize);
            freeTimestampIndex(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff,
                    CairoException.isCairoOomError(e)
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
                srcOooAuxAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                partitionTimestamp, // <-- pass thru
                activeFixFd,
                dstAuxAddr,
                dstAuxOffset,
                dstAuxFileOffset,
                dstAuxSize,
                activeVarFd,
                dstDataAddr,
                dstDataOffset,
                dstDataAdjust,
                dstDataSize,
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

    public static void openColumn(
            int openColumnMode,
            Path pathToTable,
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
            long timestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeOOOLo,
            long mergeOOOHi,
            long mergeDataLo,
            long mergeDataHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        final long mergeRowCount;
        if (mergeType == O3_BLOCK_MERGE) {
            mergeRowCount = timestampMergeIndexSize / TIMESTAMP_MERGE_ENTRY_BYTES;
        } else if (mergeType == O3_BLOCK_O3) {
            // This can happen in replace commit mode
            mergeRowCount = mergeOOOHi - mergeOOOLo + 1;
        } else if (mergeType == O3_BLOCK_NONE) {
            // This can happen in replace commit mode
            mergeRowCount = 0;
        } else {
            mergeRowCount = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        }
        final Path pathToOldPartition = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForNativePartition(
                pathToOldPartition,
                tableWriter.getMetadata().getTimestampType(),
                tableWriter.getPartitionBy(),
                oldPartitionTimestamp,
                srcNameTxn
        );
        int plen = pathToOldPartition.size();

        final Path pathToNewPartition = Path.getThreadLocal2(pathToTable);
        boolean partitionAppend = isOpenColumnModeForAppend(openColumnMode);
        TableUtils.setPathForNativePartition(
                pathToNewPartition,
                tableWriter.getMetadata().getTimestampType(),
                tableWriter.getPartitionBy(),
                partitionTimestamp,
                partitionAppend ? srcNameTxn : txn
        );
        int pplen = pathToNewPartition.size();
        final long colTopSinkAddr = columnTopAddress(partitionUpdateSinkAddr, columnIndex);

        // append jobs do not set value of part counter, we do it here for those
        switch (openColumnMode) {
            case OPEN_LAST_PARTITION_FOR_APPEND:
            case OPEN_MID_PARTITION_FOR_APPEND:
                appendMidPartition(
                        pathToNewPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        partitionTimestamp,
                        oldPartitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        tableWriter,
                        // Even though we are not using mapped memory for the last partition from the table writer
                        // we must use its indexers to produce consistent index results. The condition here is
                        // "creative" - we want to avoid bitmap index lookup for each and every column. We will use
                        // presence of "indexWriter" as the guide. For columns that do not need indexing, this
                        // writer would be null.

                        // Consistent use of tableWriter's indexer is required because last partition could be updated
                        // from two places, this is one of them. The other also uses tableWriter's index and is more
                        // straightforward. Failure to make them consistent manifests when this site updates index and
                        // extends its value memory, it may do so without extending key memory. Then key memory has
                        // the reference to a value block, which would be outside the mapped area for tableWriters' indexer.
                        openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND && indexWriter != null ? tableWriter.getBitmapIndexWriter(columnIndex) : indexWriter,
                        colTopSinkAddr,
                        columnIndex,
                        columnNameTxn,
                        partitionUpdateSinkAddr
                );
                break;
            case OPEN_MID_PARTITION_FOR_MERGE:
                mergeMidPartition(
                        pathToOldPartition,
                        plen,
                        pathToNewPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeRowCount,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        oldPartitionTimestamp,
                        columnIndex,
                        columnNameTxn,
                        partitionUpdateSinkAddr
                );
                break;
            case OPEN_LAST_PARTITION_FOR_MERGE:
                mergeLastPartition(
                        pathToNewPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeRowCount,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        indexBlockCapacity,
                        activeFixFd,
                        activeVarFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        columnNameTxn,
                        partitionUpdateSinkAddr
                );
                break;
            case OPEN_NEW_PARTITION_FOR_APPEND:
                // mark the fact that the column is touched in the partition to the column version file
                // It's fine to overwrite this value if needed inside the job branches.
                Unsafe.getUnsafe().putLong(colTopSinkAddr, 0L);
                appendNewPartition(
                        pathToNewPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        partitionTimestamp,
                        srcDataMax,
                        indexBlockCapacity,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        tableWriter,
                        indexWriter,
                        columnNameTxn,
                        partitionUpdateSinkAddr
                );
                break;
            default:
                assert false;
                break;
        }
    }

    public static void openColumn(O3OpenColumnTask task, long cursor, Sequence subSeq) {
        final int openColumnMode = task.getOpenColumnMode();
        final Path pathToTable = task.getPathToTable();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long timestampMin = task.getTimestampMin();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long oldPartitionTimestamp = task.getOldPartitionTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcNameTxn = task.getSrcNameTxn();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final int indexBlockCapacity = task.getIndexBlockCapacity();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long mergeOOOLo = task.getMergeOOOLo();
        final long mergeOOOHi = task.getMergeOOOHi();
        final long mergeDataLo = task.getMergeDataLo();
        final long mergeDataHi = task.getMergeDataHi();
        final long txn = task.getTxn();
        final int prefixType = task.getPrefixType();
        final long prefixLo = task.getPrefixLo();
        final long prefixHi = task.getPrefixHi();
        final int suffixType = task.getSuffixType();
        final long suffixLo = task.getSuffixLo();
        final long suffixHi = task.getSuffixHi();
        final int mergeType = task.getMergeType();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final long timestampMergeIndexSize = task.getTimestampMergeIndexSize();
        final long activeFixFd = task.getActiveFixFd();
        final long activeVarFd = task.getActiveVarFd();
        final long srcDataTop = task.getSrcDataTop();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();
        final int columnIndex = task.getColumnIndex();
        final long columnNameTxn = task.getColumnNameTxn();
        final long srcDataNewPartitionSize = task.getSrcDataNewPartitionSize();
        final long srcDataOldPartitionSize = task.getSrcDataOldPartitionSize();
        final long o3SplitPartitionSize = task.getO3SplitPartitionSize();

        subSeq.done(cursor);

        openColumn(
                openColumnMode,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                timestampMin,
                partitionTimestamp,
                oldPartitionTimestamp,
                srcDataTop,
                srcDataMax,
                srcNameTxn,
                txn,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeOOOLo,
                mergeOOOHi,
                mergeDataLo,
                mergeDataHi,
                suffixType,
                suffixLo,
                suffixHi,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                indexBlockCapacity,
                activeFixFd,
                activeVarFd,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr,
                columnIndex,
                columnNameTxn
        );
    }

    private static void appendFixColumn(
            Path pathToNewPartition,
            int pNewLen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp, // <- pass thru
            long srcDataTop,
            long srcDataMax,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long dstFixFd,
            MemoryMA dstFixMem,
            long dstLen,
            TableWriter tableWriter,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            BitmapIndexWriter indexWriter,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        long dstKFd = 0;
        long dstVFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixFileOffset;
        long dstIndexOffset;
        long dstIndexAdjust;
        long dstFixSize;
        final int shl = ColumnType.pow2SizeOf(columnType);
        final FilesFacade ff = tableWriter.getFilesFacade();

        dstFixAddr = 0;
        dstFixSize = dstLen << shl;
        try {
            dstFixOffset = (srcDataMax - srcDataTop) << shl;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                // Area we want to write is not mapped
                dstFixAddr = mapRW(ff, Math.abs(dstFixFd), dstFixSize, MemoryTag.MMAP_O3);
            } else {
                // Area we want to write is mapped.
                // Set dstFixAddr to Append Address with adjustment that dstFixOffset offset points to offset 0.
                dstFixAddr = dstFixMem.getAppendAddress() - dstFixOffset;
                // Set size negative meaning it will not be freed
                dstFixSize = -dstFixSize;
            }
            dstIndexOffset = dstFixOffset;
            dstIndexAdjust = srcDataTop;
            dstFixFileOffset = dstFixOffset;

            if (indexBlockCapacity > -1 && !indexWriter.isOpen()) {
                dstKFd = openRW(ff, BitmapIndexUtils.keyFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstVFd = openRW(ff, BitmapIndexUtils.valueFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            }
        } catch (Throwable e) {
            LOG.error().$("append fix error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            if (dstFixSize > 0) {
                O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            } else {
                O3Utils.unmapAndClose(ff, dstFixFd, 0, 0);
            }
            O3Utils.close(ff, dstKFd);
            O3Utils.close(ff, dstVFd);
            freeTimestampIndex(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff,
                    CairoException.isCairoOomError(e)
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
                srcDataTop << shl,
                srcDataMax,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixFileOffset,
                dstFixSize,
                0,
                0,
                0,
                0,
                0,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                dstIndexAdjust,
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr
        );
    }

    private static void appendMidPartition(
            Path pathToNewPartition,
            int pNewLen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            int columnIndex,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        long dstFixFd = 0;
        long dstVarFd = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        if (srcDataTop == -1) {
            try {
                // When partition is split, it's column top remains same.
                // On writing to the split partition second time the column top can be bigger than the partition.
                // Trim column top to partition top.
                srcDataTop = Math.min(srcDataMax, tableWriter.getColumnTop(oldPartitionTimestamp, columnIndex, srcDataMax));
                if (srcDataTop == srcDataMax) {
                    Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataMax);
                }
            } catch (Throwable e) {
                LOG.error().$("append mid partition error 1 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                freeTimestampIndex(
                        columnCounter,
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }
        }

        final long dstRowCount = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        if (ColumnType.isVarSize(columnType)) {
            try {
                // index files are opened as normal
                dstFixFd = openRW(ff, iFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                // open data file now
                dstVarFd = openRW(ff, dFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            } catch (Throwable e) {
                LOG.error().$("append mid partition error 2 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.close(ff, dstFixFd);
                O3Utils.close(ff, dstVarFd);
                freeTimestampIndex(
                        columnCounter,
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }
            o3VarSizePartitionAppend(
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstFixFd,
                    dstVarFd,
                    null,
                    null,
                    dstRowCount,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    partitionUpdateSinkAddr
            );
        } else if (ColumnType.isDesignatedTimestamp(columnType)) {
            appendTimestampColumn(
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataMax,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    null,
                    dstRowCount,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    partitionUpdateSinkAddr
            );
        } else {
            try {
                dstFixFd = openRW(ff, dFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            } catch (Throwable e) {
                LOG.error().$("append mid partition error 3 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.close(ff, dstFixFd);
                O3Utils.close(ff, dstVarFd);
                freeTimestampIndex(
                        columnCounter,
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }
            appendFixColumn(
                    pathToNewPartition,
                    pNewLen,
                    columnName,
                    columnCounter,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    timestampMin,
                    partitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstFixFd,
                    null,
                    dstRowCount,
                    tableWriter,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    indexWriter,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        }
    }

    private static void appendNewPartition(
            Path pathToNewPartition,
            int pNewLen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long srcDataMax,
            int indexBlockCapacity,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long dstFixSize = 0;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            if (ColumnType.isVarSize(columnType)) {
                dstFixFd = openRW(ff, iFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());

                ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

                dstFixSize = columnTypeDriver.getAuxVectorSize(srcOooHi - srcOooLo + 1);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);

                dstVarFd = openRW(ff, dFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstVarSize = columnTypeDriver.getDataVectorSize(srcOooFixAddr, srcOooLo, srcOooHi);
                dstVarAddr = dstVarSize > 0 ? mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3) : 0;
            } else {
                dstFixFd = openRW(ff, dFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
                if (indexBlockCapacity > -1) {
                    dstKFd = openRW(ff, BitmapIndexUtils.keyFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    dstVFd = openRW(ff, BitmapIndexUtils.valueFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                }
            }
        } catch (Throwable e) {
            LOG.error().$("append new partition error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
            final FilesFacade ff1 = tableWriter.getFilesFacade();
            O3Utils.unmapAndClose(ff1, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff1, dstVarFd, dstVarAddr, dstVarSize);
            O3Utils.close(ff1, dstKFd);
            O3Utils.close(ff1, dstVFd);
            if (columnCounter.decrementAndGet() == 0) {
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
            }
            throw e;
        }

        o3PublishCopyTask(
                columnCounter,
                null,
                columnType,
                O3_BLOCK_O3,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
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
                0,
                srcDataMax,
                // this is new partition
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                0,
                0,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                0,
                0,
                dstVarSize,
                dstKFd,
                dstVFd,
                0,
                0,
                indexBlockCapacity,
                0,
                0,
                0,
                false, // partition does not mutate above the append line
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr
        );
    }

    private static void appendTimestampColumn(
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long srcDataMax,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            MemoryMA dstFixMem,
            long dstLen,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            long partitionUpdateSinkAddr
    ) {
        long dstFixFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixFileOffset;
        long dstFixSize;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            dstFixSize = dstLen * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFileOffset = dstFixOffset;
                dstFixFd = -Math.abs(srcTimestampFd);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixFileOffset = dstFixMem.getAppendOffset();
                dstFixSize = -dstFixSize;
            }
        } catch (Throwable e) {
            LOG.error().$("append ts error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.close(ff, dstFixFd);
            freeTimestampIndex(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff,
                    CairoException.isCairoOomError(e)
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
                0, // designated timestamp column cannot be added after table is created
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
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixFileOffset,
                dstFixSize,
                0,
                0,
                0,
                0,
                0,
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

    private static long columnTopAddress(long partitionUpdateSinkAddr, int columnIndex) {
        return partitionUpdateSinkAddr + PARTITION_SINK_COL_TOP_OFFSET + (long) columnIndex * Long.BYTES;
    }

    private static void mergeFixColumn(
            Path pathToNewPartition,
            int pNewLen,
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
            long srcDataMax,
            long srcDataTop,
            long srcDataFixFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeOOOLo,
            long mergeOOOHi,
            long mergeDataLo,
            long mergeDataHi,
            long mergeRowCount,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        int partCount = 0;
        long dstFixAppendOffset1;
        long srcDataFixSize = 0;
        long srcDataFixOffset;
        long srcDataFixAddr = 0;
        long dstFixAppendOffset2;
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long srcDataTopOffset;
        long dstIndexAdjust;
        long dstFixSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        final long srcFixFd = Math.abs(srcDataFixFd);
        final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
        final FilesFacade ff = tableWriter.getFilesFacade();
        final boolean mixedIOFlag = tableWriter.allowMixedIO();

        try {
            if (srcDataTop > 0 && tableWriter.isCommitReplaceMode()) {
                long dataMax = 0;
                if (prefixType == O3_BLOCK_DATA && prefixHi >= prefixLo) {
                    dataMax = prefixHi + 1;
                }
                if (suffixType == O3_BLOCK_DATA && suffixHi >= suffixLo) {
                    dataMax = suffixHi + 1;
                }
                srcDataMax = Math.min(srcDataMax, dataMax);
                srcDataTop = Math.min(srcDataTop, dataMax);
            }

            if (srcDataTop > 0) {
                // Size of data actually in the file.
                final long srcDataActualBytes = (srcDataMax - srcDataTop) << shl;
                // Size of data in the file if it didn't have column top.
                final long srcDataMaxBytes = srcDataMax << shl;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // Extend the existing column down, we will be discarding it anyway.
                    // Materialize nulls at the end of the column and add non-null data to merge.
                    // Do all of this beyond existing written data, using column as a buffer.
                    // It is also fine in case when last partition contains WAL LAG, since
                    // At the beginning of the transaction LAG is copied into memory buffers (o3 mem columns).
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    TableUtils.setNull(columnType, srcDataFixAddr + srcDataActualBytes, srcDataTop);
                    Vect.memcpy(srcDataFixAddr + srcDataMaxBytes, srcDataFixAddr, srcDataActualBytes);
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    if (prefixType != O3_BLOCK_NONE) {
                        // Set column top if it's not split partition.
                        Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataTop);
                        // If it's split partition, do nothing. Old partition will have the old column top
                        // New partition will have 0 column top, since srcDataTop <= prefixHi.
                    }
                    srcDataFixSize = srcDataActualBytes;
                    if (srcDataFixSize != 0) {
                        srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                        ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    }
                    srcDataFixOffset = 0;
                }
            } else {
                srcDataFixSize = srcDataMax << shl;
                if (srcDataFixSize != 0) {
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                }
                srcDataFixOffset = 0;
            }

            srcDataTopOffset = srcDataTop << shl;
            dstIndexAdjust = srcDataTopOffset >> 2;

            dstFixFd = openRW(ff, dFile(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            // Use target partition size to determine the size of fixed file, it's already compensated
            // for partitions splits and duplicates found by dedup
            long rowCount = o3SplitPartitionSize > 0 ? o3SplitPartitionSize : srcDataNewPartitionSize - srcDataTop;
            dstFixSize = rowCount << shl;
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            if (!mixedIOFlag) {
                ff.madvise(dstFixAddr, dstFixSize, Files.POSIX_MADV_RANDOM);
            }

            // when prefix is "data" we need to reduce it by "srcDataTop"
            if (prefixType == O3_BLOCK_DATA) {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - srcDataTop) << shl;
                prefixHi -= srcDataTop;
            } else if (prefixType == O3_BLOCK_NONE) {
                // Split partition.
                dstFixAppendOffset1 = 0;
                prefixHi -= srcDataTop;
                dstIndexAdjust = 0;
            } else {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
            }

            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeRowCount << shl);
            } else {
                dstFixAppendOffset2 = dstFixAppendOffset1;
            }

            if (suffixType == O3_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            if (indexBlockCapacity > -1) {
                dstKFd = openRW(ff, BitmapIndexUtils.keyFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstVFd = openRW(ff, BitmapIndexUtils.valueFileName(pathToNewPartition.trimTo(pNewLen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            }

            if (prefixType != O3_BLOCK_NONE) {
                partCount++;
            }

            if (mergeType != O3_BLOCK_NONE) {
                partCount++;
            }

            if (suffixType != O3_BLOCK_NONE) {
                partCount++;
            }
        } catch (Throwable e) {
            LOG.error().$("merge fix error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.close(ff, dstKFd);
            O3Utils.close(ff, dstVFd);
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
            if (columnCounter.decrementAndGet() == 0) {
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
            }
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
                0,
                0,
                0,
                0,
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
                0,
                0,
                0,
                dstFixAppendOffset1,
                dstFixAppendOffset2,
                0,
                0,
                dstKFd,
                dstVFd,
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                dstIndexAdjust,
                partitionUpdateSinkAddr
        );
    }

    private static void mergeLastPartition(
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
            long mergeRowCount,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {

        if (ColumnType.isVarSize(columnType)) {
            // index files are opened as normal
            mergeVarColumn(
                    pathToNewPartition,
                    pplen,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooPartitionMin,
                    oooPartitionHi,
                    srcDataTop,
                    srcDataMax,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    mergeRowCount,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    -activeFixFd,
                    -activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    colTopSinkAddr,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        } else {
            mergeFixColumn(
                    pathToNewPartition,
                    pplen,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooPartitionMin,
                    oooPartitionHi,
                    srcDataMax,
                    srcDataTop,
                    -activeFixFd,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    mergeRowCount,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    colTopSinkAddr,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        }
    }

    private static void mergeMidPartition(
            Path pathToOldPartition,
            int plen,
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
            long mergeRowCount,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long oldPartitionTimestamp,
            int columnIndex,
            long columnNameTxn,
            long partitionUpdateSinkAddr
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        // not set, we need to check file existence and read
        if (srcDataTop == -1) {
            try {
                srcDataTop = Math.min(tableWriter.getColumnTop(oldPartitionTimestamp, columnIndex, srcDataMax), srcDataMax);
            } catch (Throwable e) {
                LOG.error().$("merge mid partition error 1 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                freeTimestampIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }
        }

        long srcDataFixFd = 0;
        long srcDataVarFd = 0;
        if (ColumnType.isVarSize(columnType)) {
            try {
                srcDataFixFd = openRW(ff, iFile(pathToOldPartition.trimTo(plen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                srcDataVarFd = openRW(ff, dFile(pathToOldPartition.trimTo(plen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            } catch (Throwable e) {
                LOG.error().$("merge mid partition error 2 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.close(ff, srcDataFixFd);
                O3Utils.close(ff, srcDataVarFd);
                freeTimestampIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }

            mergeVarColumn(
                    pathToNewPartition,
                    pplen,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooPartitionMin,
                    oooPartitionHi,
                    srcDataTop,
                    srcDataMax,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    mergeRowCount,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    srcDataFixFd,
                    srcDataVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    colTopSinkAddr,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        } else {
            try {
                if (columnType < 0 && srcTimestampFd > 0) {
                    // ensure timestamp srcDataFixFd is always negative, we will close it externally
                    srcDataFixFd = -srcTimestampFd;
                } else {
                    srcDataFixFd = openRW(ff, dFile(pathToOldPartition.trimTo(plen), columnName, columnNameTxn), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                }
            } catch (Throwable e) {
                LOG.error().$("merge mid partition error 3 [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                freeTimestampIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        CairoException.isCairoOomError(e)
                );
                throw e;
            }
            mergeFixColumn(
                    pathToNewPartition,
                    pplen,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooPartitionMin,
                    oooPartitionHi,
                    srcDataMax,
                    srcDataTop,
                    srcDataFixFd,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    mergeRowCount,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    colTopSinkAddr,
                    columnNameTxn,
                    partitionUpdateSinkAddr
            );
        }
    }

    private static void publishCopyTaskContended(
            long cursor,
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataVarSize,
            long srcDataTop,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixFileOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr
    ) {
        while (cursor == -2) {
            cursor = tableWriter.getO3CopyPubSeq().next();
        }

        if (cursor == -1) {
            O3CopyJob.copy(
                    columnCounter,
                    partCounter,
                    columnType,
                    blockType,
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
                    srcDataLo,
                    srcDataHi,
                    srcDataTop,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    timestampMin,
                    partitionTimestamp,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixFileOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr
            );
        } else {
            publishCopyTaskHarmonized(
                    columnCounter,
                    partCounter,
                    columnType,
                    blockType,
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
                    srcDataLo,
                    srcDataHi,
                    srcDataTop,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    timestampMin,
                    partitionTimestamp,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixFileOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr
            );
        }
    }

    private static void publishCopyTaskHarmonized(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcDataTop,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixFileOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            long cursor,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr
    ) {
        final O3CopyTask task = tableWriter.getO3CopyQueue().get(cursor);
        task.of(
                columnCounter,
                partCounter,
                columnType,
                blockType,
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
                srcDataLo,
                srcDataHi,
                srcDataTop,
                srcDataMax,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooPartitionLo,
                srcOooPartitionHi,
                timestampMin,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixFileOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarAdjust,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                dstIndexAdjust,
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr
        );
        tableWriter.getO3CopyPubSeq().done(cursor);
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        openColumn(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
