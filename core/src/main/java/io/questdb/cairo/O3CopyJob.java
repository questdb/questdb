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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.tasks.O3CopyTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableWriter.*;

public class O3CopyJob extends AbstractQueueConsumerJob<O3CopyTask> {
    private static final Log LOG = LogFactory.getLog(O3CopyJob.class);

    public O3CopyJob(MessageBus messageBus) {
        super(messageBus.getO3CopyQueue(), messageBus.getO3CopySubSeq());
    }

    public static void copy(
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
            long partitionTimestamp, // <-- this is used to determine if partition is last or not as well as partition dir
            long dstFixFd,
            long dstAuxAddr,
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
        final boolean mixedIOFlag = tableWriter.allowMixedIO();
        LOG.debug().$("o3 copy [blockType=").$(blockType)
                .$(", columnType=").$(columnType)
                .$(", dstFixFd=").$(dstFixFd)
                .$(", dstFixSize=").$(dstFixSize)
                .$(", dstFixOffset=").$(dstFixOffset)
                .$(", dstVarFd=").$(dstVarFd)
                .$(", dstVarSize=").$(dstVarSize)
                .$(", dstVarOffset=").$(dstVarOffset)
                .$(", srcDataLo=").$(srcDataLo)
                .$(", srcDataHi=").$(srcDataHi)
                .$(", srcDataMax=").$(srcDataMax)
                .$(", srcOooLo=").$(srcOooLo)
                .$(", srcOooHi=").$(srcOooHi)
                .$(", srcOooMax=").$(srcOooMax)
                .$(", srcOooPartitionLo=").$(srcOooPartitionLo)
                .$(", srcOooPartitionHi=").$(srcOooPartitionHi)
                .$(", mixedIOFlag=").$(mixedIOFlag)
                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                .I$();

        try {
            switch (blockType) {
                case O3_BLOCK_MERGE:
                    mergeCopy(
                            columnType,
                            timestampMergeIndexAddr,
                            timestampMergeIndexSize / TIMESTAMP_MERGE_ENTRY_BYTES,
                            // this is a hack, when we have column top we can have only of the two:
                            // srcDataFixOffset, when we had to shift data to back-fill nulls or
                            // srcDataTopOffset - if we kept the column top
                            // when one value is present the other will be 0
                            srcDataFixAddr + srcDataFixOffset - srcDataTop,
                            srcDataVarAddr + srcDataVarOffset,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            dstAuxAddr + dstFixOffset,
                            dstVarAddr,
                            dstVarOffset
                    );
                    break;
                case O3_BLOCK_O3:
                    copyO3(
                            tableWriter.getFilesFacade(),
                            columnType,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            srcOooLo,
                            srcOooHi,
                            dstFixFd,
                            dstAuxAddr + dstFixOffset,
                            Math.abs(dstFixSize) - dstFixOffset,
                            dstFixFileOffset,
                            dstVarAddr,
                            dstVarFd,
                            dstVarOffset,
                            dstVarAdjust,
                            dstVarSize,
                            mixedIOFlag
                    );
                    break;
                case O3_BLOCK_DATA:
                    if (srcDataLo <= srcDataHi) {
                        copyData(
                                tableWriter.getFilesFacade(),
                                columnType,
                                srcDataFixAddr + srcDataFixOffset,
                                srcDataVarAddr + srcDataVarOffset,
                                srcDataLo,
                                srcDataHi,
                                dstAuxAddr + dstFixOffset,
                                Math.abs(dstFixSize) - dstFixOffset,
                                dstFixFd,
                                dstFixFileOffset,
                                dstVarAddr,
                                dstVarFd,
                                dstVarOffset,
                                dstVarAdjust,
                                dstVarSize,
                                mixedIOFlag
                        );
                    }
                    break;
                default:
                    break;
            }
        } catch (Throwable th) {
            // Notify table writer of error before clocking down done counters
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(th));

            // We cannot close / unmap fds here. Other copy jobs may still be running and using them.
            // exception handling code of the stack will check if all the parts are finished before closing the memory / fds.
            if (partCounter == null || partCounter.decrementAndGet() == 0) {
                unmapAndCloseAllPartsComplete(
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
                        dstAuxAddr,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        tableWriter
                );
            }

            throw th;
        }

        copyTail(
                columnCounter,
                partCounter,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                timestampMin,
                partitionTimestamp,
                dstFixFd,
                dstAuxAddr,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
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

    public static void copy(O3CopyTask task, long cursor, Sequence subSeq) {
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final int columnType = task.getColumnType();
        final int blockType = task.getBlockType();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final long timestampMergeIndexSize = task.getTimestampMergeIndexSize();
        final long srcDataFixFd = task.getSrcDataFixFd();
        final long srcDataFixAddr = task.getSrcDataFixAddr();
        final long srcDataFixOffset = task.getSrcDataFixOffset();
        final long srcDataFixSize = task.getSrcDataFixSize();
        final long srcDataVarFd = task.getSrcDataVarFd();
        final long srcDataVarAddr = task.getSrcDataVarAddr();
        final long srcDataVarOffset = task.getSrcDataVarOffset();
        final long srcDataVarSize = task.getSrcDataVarSize();
        final long srcDataTop = task.getSrcDataTop();
        final long srcDataLo = task.getSrcDataLo();
        final long srcDataMax = task.getSrcDataMax();
        final long srcDataHi = task.getSrcDataHi();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long srcOooPartitionLo = task.getSrcOooPartitionLo();
        final long srcOooPartitionHi = task.getSrcOooPartitionHi();
        final long timestampMin = task.getTimestampMin();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long dstFixFd = task.getDstFixFd();
        final long dstAuxAddr = task.getDstFixAddr();
        final long dstFixOffset = task.getDstFixOffset();
        final long dstFixFileOffset = task.getDstFixFileOffset();
        final long dstFixSize = task.getDstFixSize();
        final long dstVarFd = task.getDstVarFd();
        final long dstVarAddr = task.getDstVarAddr();
        final long dstVarOffset = task.getDstVarOffset();
        final long dstVarAdjust = task.getDstVarAdjust();
        final long dstVarSize = task.getDstVarSize();
        final long dstKFd = task.getDstKFd();
        final long dskVFd = task.getDstVFd();
        final long dstIndexOffset = task.getDstIndexOffset();
        final long dstIndexAdjust = task.getDstIndexAdjust();
        final int indexBlockCapacity = task.getIndexBlockCapacity();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final boolean partitionMutates = task.isPartitionMutates();
        final long srcDataNewPartitionSize = task.getSrcDataNewPartitionSize();
        final long srcDataOldPartitionSize = task.getSrcDataOldPartitionSize();
        final long o3SplitPartitionSize = task.getO3SplitPartitionSize();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();

        subSeq.done(cursor);

        try {
            copy(
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
                    dstAuxAddr,
                    dstFixOffset,
                    dstFixFileOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dskVFd,
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
        } catch (Throwable th) {
            LOG.error().$("o3 copy failed [table=").$(tableWriter.getTableToken())
                    .$(", partition=").$ts(ColumnType.getTimestampDriver(tableWriter.getTimestampType()), partitionTimestamp)
                    .$(", columnType=").$(columnType)
                    .$(", exception=").$(th)
                    .I$();
            throw th;
        }
    }

    public static void copyFixedSizeCol(
            FilesFacade ff,
            long src,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstFixFileOffset,
            long dstFd,
            int shl,
            boolean mixedIOFlag
    ) {
        final long len = (srcHi - srcLo + 1) << shl;
        O3Utils.copyFixedSizeCol(ff, src, srcLo, dstFixAddr, dstFixFileOffset, dstFd, mixedIOFlag, len, shl);
    }

    public static void mergeCopy(
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        if (ColumnType.isVarSize(columnType)) {
            ColumnType.getDriver(columnType).o3ColumnMerge(
                    timestampMergeIndexAddr,
                    timestampMergeIndexCount,
                    srcDataFixAddr,
                    srcDataVarAddr,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    dstFixAddr,
                    dstVarAddr,
                    dstVarOffset
            );
        } else if (ColumnType.isDesignatedTimestamp(columnType)) {
            Vect.oooCopyIndex(timestampMergeIndexAddr, timestampMergeIndexCount, dstFixAddr);
        } else {
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                case ColumnType.DECIMAL8:
                    Vect.mergeShuffle8Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                case ColumnType.SHORT:
                case ColumnType.CHAR:
                case ColumnType.GEOSHORT:
                case ColumnType.DECIMAL16:
                    Vect.mergeShuffle16Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.FLOAT:
                case ColumnType.SYMBOL:
                case ColumnType.GEOINT:
                case ColumnType.DECIMAL32:
                    Vect.mergeShuffle32Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.GEOLONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DECIMAL64:
                    Vect.mergeShuffle64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                case ColumnType.UUID:
                case ColumnType.LONG128:
                case ColumnType.DECIMAL128:
                    Vect.mergeShuffle128Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                case ColumnType.LONG256:
                case ColumnType.DECIMAL256:
                    Vect.mergeShuffle256Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, timestampMergeIndexCount);
                    break;
                default:
                    break;
            }
        }
    }

    public static void o3ColumnCopy(
            FilesFacade ff,
            int columnType,
            long srcAuxAddr,
            long srcDataAddr,
            long srcLo,
            long srcHi,
            long dstAuxAddr,
            long dstAuxAddrSize,
            long dstAuxFd,
            long dstAuxFileOffset,
            long dstDataAddr,
            long dstDataFd,
            long dstDataOffset,
            long dstDataAdjust,
            long dstDataSize,
            boolean mixedIOFlag
    ) {
        assert srcLo <= srcHi : String.format("srcLo %,d > srcHi %,d", srcLo, srcHi);

        // we can find out the edge of string column in one of two ways
        // 1. if srcOooHi is at the limit of the page - we need to copy the whole page of strings
        // 2  if there are more items behind srcOooHi we can get offset of srcOooHi+1

        ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

        final long srcDataOffset = columnTypeDriver.getDataVectorOffset(srcAuxAddr, srcLo);
        assert srcDataOffset >= 0;
        // copy this before it changes
        final long len = columnTypeDriver.getDataVectorSize(srcAuxAddr, srcLo, srcHi);
        assert len <= Math.abs(dstDataSize) - dstDataOffset :
                String.format("columnType %d srcLo %,d srcHi %,d srcDataOffset %,d dstDataOffset %,d len %,d dstDataSize %,d dst_diff %,d",
                        columnType, srcLo, srcHi, srcDataOffset, dstDataOffset, len, dstDataSize, dstDataOffset + len - Math.abs(dstDataSize));
        final long offset = dstDataOffset + dstDataAdjust;
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstDataFd), srcDataAddr + srcDataOffset, len, offset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy var data column prefix [fd=").put(dstDataFd).put(", offset=").put(offset).put(", len=").put(len).put(']');
            }
        } else {
            Vect.memcpy(dstDataAddr + dstDataOffset, srcDataAddr + srcDataOffset, len);
        }
        if (srcDataOffset == offset) {
            columnTypeDriver.o3copyAuxVector(
                    ff,
                    srcAuxAddr,
                    srcLo,
                    srcHi,
                    dstAuxAddr,
                    dstAuxFileOffset,
                    dstAuxFd,
                    mixedIOFlag
            );
        } else {
            columnTypeDriver.shiftCopyAuxVector(srcDataOffset - offset, srcAuxAddr, srcLo, srcHi, dstAuxAddr, dstAuxAddrSize);
        }
    }

    private static void copyData(
            FilesFacade ff,
            int columnType,
            long srcAuxAddr,
            long srcVarAddr,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstFixAddrSize,
            long dstFixFd,
            long dstFixFileOffset,
            long dstVarAddr,
            long dstVarFd,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            boolean mixedIOFlag
    ) {
        if (ColumnType.isVarSize(columnType)) {
            o3ColumnCopy(
                    ff,
                    columnType,
                    srcAuxAddr,
                    srcVarAddr,
                    srcLo,
                    srcHi,
                    dstFixAddr,
                    dstFixAddrSize,
                    dstFixFd,
                    dstFixFileOffset,
                    dstVarAddr,
                    dstVarFd,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    mixedIOFlag
            );
        } else {
            copyFixedSizeCol(
                    ff,
                    srcAuxAddr,
                    srcLo,
                    srcHi,
                    dstFixAddr,
                    dstFixFileOffset,
                    dstFixFd,
                    ColumnType.pow2SizeOf(Math.abs(columnType)),
                    mixedIOFlag
            );
        }
    }

    private static void copyTail(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long timestampMin,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
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
        if (partCounter == null || partCounter.decrementAndGet() == 0) {
            final FilesFacade ff = tableWriter.getFilesFacade();
            if (indexBlockCapacity > -1) {
                updateIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        dstFixFd,
                        dstFixAddr,
                        // Do not use Math.abs(dstFixSize) here, pass negative values indicating that
                        // the dstFix memory is not mapped and should not be released in case of exception
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        // Do not use Math.abs(dstVarSize) here, pass negative values indicating that
                        // the dstVar memory is not mapped and should not be released in case of exception
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        dstIndexOffset,
                        dstIndexAdjust,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        indexBlockCapacity
                );
            }

            final int commitMode = tableWriter.getConfiguration().getCommitMode();
            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        dstFixFd,
                        dstFixAddr,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff,
                        commitMode
                );
            }

            // unmap memory
            O3Utils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            O3Utils.unmapAndClose(ff, srcDataVarFd, srcDataVarAddr, srcDataVarSize);
            O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff, dstVarFd, dstVarAddr, dstVarSize);

            final int columnsRemaining = columnCounter.decrementAndGet();
            LOG.debug()
                    .$("organic [columnsRemaining=").$(columnsRemaining)
                    .I$();

            if (columnsRemaining == 0) {
                updatePartition(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        timestampMin,
                        partitionTimestamp,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        partitionUpdateSinkAddr,
                        tableWriter
                );
            }
        }
    }

    private static void syncColumns(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            FilesFacade ff,
            int commitMode
    ) {
        try {
            boolean async = commitMode == CommitMode.ASYNC;
            if (dstFixAddr != 0 && dstFixSize > 0) {
                ff.msync(dstFixAddr, dstFixSize, async);
                // sync FD in case we wrote data not via mmap
                if (dstFixFd != -1 && dstFixFd != 0) {
                    ff.fsync(Math.abs(dstFixFd));
                }
            }
            if (dstVarAddr != 0 && dstVarSize > 0) {
                ff.msync(dstVarAddr, dstVarSize, async);
                if (dstVarFd != -1 && dstVarFd != 0) {
                    ff.fsync(Math.abs(dstVarFd));
                }
            }
        } catch (Throwable e) {
            LOG.error()
                    .$("sync error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount(false);
            unmapAndCloseAllPartsComplete(
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
    }

    private static void updateIndex(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            int indexBlockCapacity
    ) {
        // dstKFd & dstVFd are closed by the indexer
        try {
            long row = dstIndexOffset / Integer.BYTES;
            boolean closed = !indexWriter.isOpen();
            if (closed) {
                indexWriter.of(tableWriter.getConfiguration(), dstKFd, dstVFd, row == 0, indexBlockCapacity);
            }
            try {
                updateIndex(dstFixAddr, Math.abs(dstFixSize), indexWriter, dstIndexOffset / Integer.BYTES, dstIndexAdjust);
                indexWriter.commit();
            } finally {
                if (closed) {
                    Misc.free(indexWriter);
                }
            }
        } catch (Throwable e) {
            LOG.error()
                    .$("index error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount(false);
            unmapAndCloseAllPartsComplete(
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
    }

    private static void updateIndex(long dstFixAddr, long dstFixSize, BitmapIndexWriter w, long row, long rowAdjust) {
        w.rollbackConditionally(row + rowAdjust);
        final long count = dstFixSize / Integer.BYTES;
        for (; row < count; row++) {
            w.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(dstFixAddr + row * Integer.BYTES)), row + rowAdjust);
        }
        w.setMaxValue(row + rowAdjust);
    }

    // lowest timestamp of partition where data is headed

    private static void updatePartition(
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long timestampMin,
            long partitionTimestamp,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            long partitionUpdateSinkAddr,
            TableWriter tableWriter
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        try {
            try {
                O3Utils.close(ff, srcTimestampFd);
            } finally {
                o3NotifyPartitionUpdate(
                        tableWriter,
                        partitionUpdateSinkAddr,
                        timestampMin,
                        partitionTimestamp,
                        srcDataNewPartitionSize,
                        srcDataOldPartitionSize,
                        o3SplitPartitionSize,
                        partitionMutates
                );
            }
        } finally {
            if (timestampMergeIndexAddr != 0) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            }
            tableWriter.o3CountDownDoneLatch();
        }
    }

    static void closeColumnIdle(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter
    ) {
        final int columnsRemaining = columnCounter.decrementAndGet();
        LOG.debug()
                .$("idle [table=").$(tableWriter.getTableToken())
                .$(", columnsRemaining=").$(columnsRemaining)
                .I$();
        if (columnsRemaining == 0) {

            closeColumnIdleQuick(
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    static void closeColumnIdleQuick(
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter
    ) {
        try {
            final FilesFacade ff = tableWriter.getFilesFacade();
            O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
            O3Utils.close(ff, srcTimestampFd);
            if (timestampMergeIndexAddr != 0) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            }
        } finally {
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
        }
    }

    static void copyO3(
            FilesFacade ff,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixAddrSize,
            long dstFixFileOffset,
            long dstVarAddr,
            long dstVarFd,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            boolean mixedIOFlag
    ) {
        if (ColumnType.isVarSize(columnType)) {
            o3ColumnCopy(
                    ff,
                    columnType,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    dstFixAddr,
                    dstFixAddrSize,
                    dstFixFd,
                    dstFixFileOffset,
                    dstVarAddr,
                    dstVarFd,
                    dstVarOffset,
                    dstVarAdjust,
                    dstVarSize,
                    mixedIOFlag
            );
        } else if (ColumnType.isDesignatedTimestamp(columnType)) {
            O3Utils.copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr);
        } else {
            copyFixedSizeCol(
                    ff,
                    srcOooFixAddr,
                    srcOooLo,
                    srcOooHi,
                    dstFixAddr,
                    dstFixFileOffset,
                    dstFixFd,
                    ColumnType.pow2SizeOf(columnType),
                    mixedIOFlag
            );
        }
    }

    static void o3NotifyPartitionUpdate(
            TableWriter tableWriter,
            final long partitionUpdateSinkAddr,
            long timestampMin,
            long partitionTimestamp,
            final long srcDataNewPartitionSize,
            final long srcDataOldPartitionSize,
            final long o3SplitPartitionSize,
            boolean partitionMutates
    ) {
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + Long.BYTES, timestampMin);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, srcDataNewPartitionSize);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, srcDataOldPartitionSize);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, partitionMutates ? 1 : 0);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, o3SplitPartitionSize);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, -1); // parquet partition file size

        TimestampDriver driver = ColumnType.getTimestampDriver(tableWriter.getTimestampType());
        LOG.debug()
                .$("sending partition update [partitionTimestamp=").$ts(driver, partitionTimestamp)
                .$(", partitionTimestamp=").$ts(driver, timestampMin)
                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                .$();

        tableWriter.o3ClockDownPartitionUpdateCount();
    }

    static void unmapAndClose(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long dstKFd,
            long dstVFd,
            TableWriter tableWriter
    ) {
        if (partCounter == null || partCounter.decrementAndGet() == 0) {
            // unmap memory
            unmapAndCloseAllPartsComplete(
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

    // This method should only be called after check that partCounter is 0
    static void unmapAndCloseAllPartsComplete(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            TableWriter tableWriter
    ) {
        try {
            final FilesFacade ff = tableWriter.getFilesFacade();
            O3Utils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            O3Utils.unmapAndClose(ff, srcDataVarFd, srcDataVarAddr, srcDataVarSize);
            O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff, dstVarFd, dstVarAddr, dstVarSize);
            O3Utils.close(ff, dstKFd);
            O3Utils.close(ff, dstVFd);
        } finally {
            closeColumnIdle(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        try {
            copy(queue.get(cursor), cursor, subSeq);
        } catch (CairoException th) {
            // Already logged, continue the job execution
        }
        return true;
    }
}
