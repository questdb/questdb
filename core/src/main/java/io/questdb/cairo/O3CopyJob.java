/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3PartitionUpdateTask;
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
            long timestampMax,
            long partitionTimestamp, // <-- this is used to determine if partition is last or not as well as partition dir
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarOffsetEnd,
            long dstVarAdjust,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
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
                .I$();

        switch (blockType) {
            case O3_BLOCK_MERGE:
                mergeCopy(
                        columnType,
                        timestampMergeIndexAddr,
                        // this is a hack, when we have column top we can have only of the two:
                        // srcDataFixOffset, when we had to shift data to backfill nulls or
                        // srcDataTopOffset - if we kept the column top
                        // when one value is present the other will be 0
                        srcDataFixAddr + srcDataFixOffset - srcDataTop,
                        srcDataVarAddr + srcDataVarOffset,
                        srcDataLo,
                        srcDataHi,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr + dstFixOffset,
                        dstVarAddr,
                        dstVarOffset,
                        dstVarOffsetEnd
                );
                break;
            case O3_BLOCK_O3:
                copyO3(
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr + dstFixOffset,
                        dstVarAddr,
                        dstVarOffset,
                        dstVarAdjust
                );
                break;
            case O3_BLOCK_DATA:
                copyData(
                        columnType,
                        srcDataFixAddr + srcDataFixOffset,
                        srcDataVarAddr + srcDataVarOffset,
                        srcDataLo,
                        srcDataHi,
                        dstFixAddr + dstFixOffset,
                        dstVarAddr,
                        dstVarOffset,
                        dstVarAdjust
                );
                break;
            default:
                break;
        }
        copyTail(
                columnCounter,
                partCounter,
                timestampMergeIndexAddr,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcDataMax,
                srcOooMax,
                srcOooPartitionLo,
                srcOooPartitionHi,
                timestampMin,
                timestampMax,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                dstIndexAdjust,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                tableWriter,
                indexWriter
        );
    }

    public static void copy(O3CopyTask task, long cursor, Sequence subSeq) {
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final int columnType = task.getColumnType();
        final int blockType = task.getBlockType();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
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
        final long timestampMax = task.getTimestampMax();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long dstFixFd = task.getDstFixFd();
        final long dstFixAddr = task.getDstFixAddr();
        final long dstFixOffset = task.getDstFixOffset();
        final long dstFixSize = task.getDstFixSize();
        final long dstVarFd = task.getDstVarFd();
        final long dstVarAddr = task.getDstVarAddr();
        final long dstVarOffset = task.getDstVarOffset();
        final long dstVarOffsetEnd = task.getDstVarOffsetEnd();
        final long dstVarAdjust = task.getDstVarAdjust();
        final long dstVarSize = task.getDstVarSize();
        final long dstKFd = task.getDstKFd();
        final long dskVFd = task.getDstVFd();
        final long dstIndexOffset = task.getDstIndexOffset();
        final long dstIndexAdjust = task.getDstIndexAdjust();
        final boolean isIndexed = task.isIndexed();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final boolean partitionMutates = task.isPartitionMutates();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();

        subSeq.done(cursor);

        copy(
                columnCounter,
                partCounter,
                columnType,
                blockType,
                timestampMergeIndexAddr,
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
                timestampMax,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarOffsetEnd,
                dstVarAdjust,
                dstVarSize,
                dstKFd,
                dskVFd,
                dstIndexOffset,
                dstIndexAdjust,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                tableWriter,
                indexWriter
        );
    }

    private static void copyTail(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            long timestampMergeIndexAddr,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcDataMax,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
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
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        if (partCounter == null || partCounter.decrementAndGet() == 0) {
            final FilesFacade ff = tableWriter.getFilesFacade();
            if (isIndexed) {
                updateIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        dstFixFd,
                        dstFixAddr,
                        Math.abs(dstFixSize),
                        dstVarFd,
                        dstVarAddr,
                        Math.abs(dstVarSize),
                        dstKFd,
                        dstVFd,
                        dstIndexOffset,
                        dstIndexAdjust,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter
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
                        srcDataMax,
                        srcOooMax,
                        srcOooPartitionLo,
                        srcOooPartitionHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter
                );
            }
        }
    }

    private static void updatePartition(
            long timestampMergeIndexAddr,
            long srcDataMax,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        try {
            try {
                O3Utils.close(ff, srcTimestampFd);
            } finally {
                notifyWriter(
                        srcOooPartitionLo,
                        srcOooPartitionHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcOooMax,
                        srcDataMax,
                        partitionMutates,
                        tableWriter
                );
            }
        } finally {
            if (timestampMergeIndexAddr != 0) {
                Vect.freeMergedIndex(timestampMergeIndexAddr);
            }
            tableWriter.o3CountDownDoneLatch();
        }
    }

    private static void updateIndex(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
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
            BitmapIndexWriter indexWriter
    ) {
        // dstKFd & dstVFd are closed by the indexer
        try {
            long row = dstIndexOffset / Integer.BYTES;
            boolean closed = !indexWriter.isOpen();
            if (closed) {
                indexWriter.of(tableWriter.getConfiguration(), dstKFd, dstVFd, row == 0);
            }
            try {
                updateIndex(dstFixAddr, dstFixSize, indexWriter, dstIndexOffset / Integer.BYTES, dstIndexAdjust);
            } finally {
                if (closed) {
                    Misc.free(indexWriter);
                }
            }
        } catch (Throwable e) {
            LOG.error()
                    .$("index error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
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

    static void copyIdle(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            long timestampMergeIndexAddr,
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
            copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
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

    static void copyIdleQuick(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
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
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    static void closeColumnIdle(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter
    ) {
        final int columnsRemaining = columnCounter.decrementAndGet();
        LOG.debug()
                .$("idle [table=").$(tableWriter.getTableName())
                .$(", columnsRemaining=").$(columnsRemaining)
                .I$();
        if (columnsRemaining == 0) {
            closeColumnIdleQuick(
                    timestampMergeIndexAddr,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    static void closeColumnIdleQuick(
            long timestampMergeIndexAddr,
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
                Vect.freeMergedIndex(timestampMergeIndexAddr);
            }
        } finally {
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
        }
    }

    // lowest timestamp of partition where data is headed

    static void notifyWriter(
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcOooMax,
            long srcDataMax,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        final long cursor = tableWriter.getO3PartitionUpdatePubSeq().next();
        if (cursor > -1) {
            publishUpdPartitionSizeTaskHarmonized(
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    partitionTimestamp,
                    srcDataMax,
                    partitionMutates,
                    tableWriter
            );
        } else {
            publishUpdPartitionSizeTaskContended(
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    timestampMin,
                    timestampMax,
                    partitionTimestamp,
                    srcOooMax,
                    srcDataMax,
                    partitionMutates,
                    tableWriter
            );
        }
    }

    private static void publishUpdPartitionSizeTaskContended(
            long cursor,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcOooMax,
            long srcDataMax,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        while (cursor == -2) {
            cursor = tableWriter.getO3PartitionUpdatePubSeq().next();
        }

        if (cursor > -1) {
            publishUpdPartitionSizeTaskHarmonized(
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    partitionTimestamp,
                    srcDataMax,
                    partitionMutates,
                    tableWriter
            );
        } else {
            tableWriter.o3PartitionUpdateSynchronized(
                    timestampMin,
                    timestampMax,
                    partitionTimestamp,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    partitionMutates,
                    srcOooMax,
                    srcDataMax
            );
        }
    }

    private static void publishUpdPartitionSizeTaskHarmonized(
            long cursor,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long partitionTimestamp,
            long srcDataMax,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        final O3PartitionUpdateTask task = tableWriter.getO3PartitionUpdateQueue().get(cursor);
        task.of(
                partitionTimestamp,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcDataMax,
                partitionMutates
        );
        tableWriter.getO3PartitionUpdatePubSeq().done(cursor);
    }

    private static void copyData(
            int columnType,
            long srcFixAddr,
            long srcVarAddr,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust
    ) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                copyVarSizeCol(
                        srcFixAddr,
                        srcVarAddr,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset,
                        dstVarAdjust
                );
                break;
            default:
                copyFixedSizeCol(
                        srcFixAddr,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        ColumnType.pow2SizeOf(Math.abs(columnType))
                );
                break;
        }
    }

    private static void copyFixedSizeCol(long src, long srcLo, long srcHi, long dst, final int shl) {
        Vect.memcpy(src + (srcLo << shl), dst, (srcHi - srcLo + 1) << shl);
    }

    private static void copyO3(
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust
    ) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                // we can find out the edge of string column in one of two ways
                // 1. if srcOooHi is at the limit of the page - we need to copy the whole page of strings
                // 2  if there are more items behind srcOooHi we can get offset of srcOooHi+1
                copyVarSizeCol(
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset,
                        dstVarAdjust
                );
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
                copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 1);
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
            case ColumnType.GEOINT:
                copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 2);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
            case ColumnType.GEOLONG:
                copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 3);
                break;
            case ColumnType.TIMESTAMP:
                final boolean designated = ColumnType.isDesignatedTimestamp(columnType);
                if (designated) {
                    O3Utils.copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr);
                } else {
                    copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 3);
                }
                break;
            case ColumnType.LONG256:
                copyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 5);
                break;
            default:
                // we have exhausted all supported types in "case" clauses
                break;
        }
    }

    private static void copyVarSizeCol(
            long srcFixAddr,
            long srcVarAddr,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarAdjust

    ) {
        final long lo = O3Utils.findVarOffset(srcFixAddr, srcLo);
        final long hi = O3Utils.findVarOffset(srcFixAddr, srcHi + 1);
        // copy this before it changes
        final long dest = dstVarAddr + dstVarOffset;
        final long len = hi - lo;
        Vect.memcpy(srcVarAddr + lo, dest, len);
        final long offset = dstVarOffset + dstVarAdjust;
        if (lo == offset) {
            copyFixedSizeCol(srcFixAddr, srcLo, srcHi + 1, dstFixAddr, 3);
        } else {
            O3Utils.shiftCopyFixedSizeColumnData(lo - offset, srcFixAddr, srcLo, srcHi + 1, dstFixAddr);
        }
    }

    private static void mergeCopy(
            int columnType,
            long timestampMergeIndexAddr,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcDataLo,
            long srcDataHi,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarOffsetEnd
    ) {
        final long rowCount = srcOooHi - srcOooLo + 1 + srcDataHi - srcDataLo + 1;
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                Vect.mergeShuffle8Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
            case ColumnType.GEOSHORT:
                Vect.mergeShuffle16Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            case ColumnType.STRING:
                Vect.oooMergeCopyStrColumn(
                        timestampMergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                // multiple threads could be writing to this location as var index segments overlap,
                // but they will be writing the same value
                Unsafe.getUnsafe().putLong(dstFixAddr + rowCount * 8, dstVarOffsetEnd);
                break;
            case ColumnType.BINARY:
                Vect.oooMergeCopyBinColumn(
                        timestampMergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                Unsafe.getUnsafe().putLong(dstFixAddr + rowCount * 8, dstVarOffsetEnd);
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
            case ColumnType.GEOINT:
                Vect.mergeShuffle32Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.GEOLONG:
                Vect.mergeShuffle64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            case ColumnType.TIMESTAMP:
                final boolean designated = ColumnType.isDesignatedTimestamp(columnType);
                if (designated) {
                    Vect.oooCopyIndex(timestampMergeIndexAddr, rowCount, dstFixAddr);
                } else {
                    Vect.mergeShuffle64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                }
                break;
            case ColumnType.LONG256:
                Vect.mergeShuffle256Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            default:
                break;
        }
    }

    private static void updateIndex(long dstFixAddr, long dstFixSize, BitmapIndexWriter w, long row, long rowAdjust) {
        w.rollbackConditionally(row + rowAdjust);
        final long count = dstFixSize / Integer.BYTES - rowAdjust;
        for (; row < count; row++) {
            w.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(dstFixAddr + row * Integer.BYTES)), row + rowAdjust);
        }
        w.setMaxValue(count - 1);
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        copy(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
