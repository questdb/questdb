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
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.mp.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionUpdateTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.O3Utils.get8ByteBuf;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3OpenColumnJob extends AbstractQueueConsumerJob<O3OpenColumnTask> {
    public static final int OPEN_MID_PARTITION_FOR_APPEND = 1;
    public static final int OPEN_LAST_PARTITION_FOR_APPEND = 2;
    public static final int OPEN_MID_PARTITION_FOR_MERGE = 3;
    public static final int OPEN_LAST_PARTITION_FOR_MERGE = 4;
    public static final int OPEN_NEW_PARTITION_FOR_APPEND = 5;
    private final CairoConfiguration configuration;
    private final RingQueue<O3CopyTask> outboundQueue;
    private final Sequence outboundPubSeq;
    private final RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue;
    private final MPSequence updPartitionSizePubSeq;

    public O3OpenColumnJob(MessageBus messageBus) {
        super(messageBus.getO3OpenColumnQueue(), messageBus.getO3OpenColumnSubSeq());
        this.configuration = messageBus.getConfiguration();
        this.outboundQueue = messageBus.getO3CopyQueue();
        this.outboundPubSeq = messageBus.getO3CopyPubSeq();
        this.updPartitionSizeTaskQueue = messageBus.getO3PartitionUpdateQueue();
        this.updPartitionSizePubSeq = messageBus.getO3PartitionUpdatePubSeq();
    }

    public static void appendLastPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            boolean isIndexed,
            AppendOnlyVirtualMemory dstFixMem,
            AppendOnlyVirtualMemory dstVarMem,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                appendVarColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        isIndexed,
                        0,
                        0,
                        0,
                        tableWriter,
                        doneLatch,
                        -dstFixMem.getFd(),
                        -dstVarMem.getFd(),
                        dstFixMem,
                        dstVarMem,
                        dstLen
                );
                break;
            case -ColumnType.TIMESTAMP:
                appendTimestampColumn(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataMax,
                        isIndexed,
                        -dstFixMem.getFd(),
                        0,
                        0,
                        tableWriter,
                        doneLatch,
                        dstFixMem,
                        dstLen
                );
                break;
            default:
                appendFixColumn(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        isIndexed,
                        0,
                        0,
                        0,
                        tableWriter,
                        indexWriter,
                        doneLatch,
                        -dstFixMem.getFd(),
                        dstFixMem,
                        dstLen
                );
                break;
        }
    }

    public static void openColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            O3OpenColumnTask task,
            long cursor,
            Sequence subSeq
    ) {
        final int openColumnMode = task.getOpenColumnMode();
        final CharSequence pathToTable = task.getPathToTable();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
        final FilesFacade ff = task.getFf();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long timestampMin = task.getTimestampMin();
        final long timestampMax = task.getTimestampMax();
        final long oooTimestampLo = task.getOooTimestampLo();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcDataTxn = task.getSrcDataTxn();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final boolean isIndexed = task.isIndexed();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooFixSize = task.getSrcOooFixSize();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooVarSize = task.getSrcOooVarSize();
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
        final long activeFixFd = task.getActiveFixFd();
        final long activeVarFd = task.getActiveVarFd();
        final long srcDataTop = task.getSrcDataTop();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();
        final SOUnboundedCountDownLatch doneLatch = task.getDoneLatch();

        subSeq.done(cursor);

        openColumn(
                workerId,
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                openColumnMode,
                ff,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                timestampMin,
                timestampMax,
                oooTimestampLo,
                partitionTimestamp,
                srcDataTop,
                srcDataMax,
                srcDataTxn,
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
                isIndexed,
                activeFixFd,
                activeVarFd,
                tableWriter,
                indexWriter,
                doneLatch
        );
    }

    public static void openColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            int openColumnMode,
            FilesFacade ff,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long oooTimestampLo,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcDataTxn,
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
            boolean isIndexed,
            long activeFixFd,
            long activeVarFd,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), oooTimestampLo, false);
        final int pplen = path.length();
        TableUtils.txnPartitionConditionally(path, srcDataTxn);
        final int plen = path.length();
        // append jobs do not set value of part counter, we do it here for those
        switch (openColumnMode) {
            case OPEN_MID_PARTITION_FOR_APPEND:
                appendMidPartition(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case OPEN_MID_PARTITION_FOR_MERGE:
                mergeMidPartition(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case OPEN_LAST_PARTITION_FOR_MERGE:
                mergeLastPartition(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        activeFixFd,
                        activeVarFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case OPEN_NEW_PARTITION_FOR_APPEND:
                appendNewPartition(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataMax,
                        isIndexed,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }
    }

    private static long getVarColumnSize(FilesFacade ff, int columnType, long dataFd, long lastValueOffset, int workerId) {
        final long offset;
        final long tmp = get8ByteBuf(workerId);
        if (columnType == ColumnType.STRING) {
            if (ff.read(dataFd, tmp, Integer.BYTES, lastValueOffset) != Integer.BYTES) {
                throw CairoException.instance(ff.errno()).put("could not read string length [fd=").put(dataFd).put(']');
            }
            final int len = Unsafe.getUnsafe().getInt(tmp);
            if (len < 1) {
                offset = lastValueOffset + Integer.BYTES;
            } else {
                offset = lastValueOffset + Integer.BYTES + len * 2L; // character bytes
            }
        } else {
            // BINARY
            if (ff.read(dataFd, tmp, Long.BYTES, lastValueOffset) != Long.BYTES) {
                throw CairoException.instance(ff.errno()).put("could not read blob length [fd=").put(dataFd).put(']');
            }
            final long len = Unsafe.getUnsafe().getLong(tmp);
            if (len < 1) {
                offset = lastValueOffset + Long.BYTES;
            } else {
                offset = lastValueOffset + Long.BYTES + len;
            }
        }
        return offset;
    }

    private static void appendMidPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long dstFixFd = 0;
        long dstVarFd = 0;

        if (srcDataTop == -1) {
            try {
                srcDataTop = getSrcDataTop(workerId, ff, path, plen, columnName, srcDataMax);
                if (srcDataTop == srcDataMax) {
                    writeColumnTop(ff, path.trimTo(plen), columnName, srcDataMax, workerId);
                }
            } catch (Throwable e) {
                tableWriter.o3BumpErrorCount();
                O3CopyJob.copyIdleQuick(
                        columnCounter,
                        ff,
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
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        0,
                        0,
                        tableWriter,
                        doneLatch
                );
                throw e;
            }
        }

        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                try {
                    // index files are opened as normal
                    iFile(path.trimTo(plen), columnName);
                    dstFixFd = O3Utils.openRW(ff, path);
                    // open data file now
                    dFile(path.trimTo(plen), columnName);
                    dstVarFd = O3Utils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
                            ff,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            dstFixFd,
                            0,
                            0,
                            dstVarFd,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            0,
                            0,
                            tableWriter,
                            doneLatch
                    );
                    throw e;
                }
                appendVarColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        dstFixFd,
                        dstVarFd,
                        null,
                        null,
                        dstLen
                );
                break;
            case -ColumnType.TIMESTAMP:
                appendTimestampColumn(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataMax,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        null,
                        dstLen
                );
                break;
            default:
                try {
                    dFile(path.trimTo(plen), columnName);
                    dstFixFd = O3Utils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
                            ff,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            dstFixFd,
                            0,
                            0,
                            dstVarFd,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            0,
                            0,
                            tableWriter,
                            doneLatch
                    );
                    throw e;
                }
                appendFixColumn(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataTop,
                        srcDataMax,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch,
                        dstFixFd,
                        null,
                        dstLen
                );
                break;
        }
    }

    private static void appendFixColumn(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp, // <- pass thru
            long srcDataTop,
            long srcDataMax,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch,
            long dstFixFd,
            AppendOnlyVirtualMemory dstFixMem,
            long dstLen
    ) {
        long dstKFd = 0;
        long dstVFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstIndexOffset;
        long dstIndexAdjust;
        long dstFixSize;
        final int shl = ColumnType.pow2SizeOf(columnType);

        try {
            dstFixSize = dstLen << shl;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = (srcDataMax - srcDataTop) << shl;
                dstFixAddr = O3Utils.mapRW(ff, Math.abs(dstFixFd), dstFixSize);
                dstIndexOffset = dstFixOffset;
                dstIndexAdjust = 0;
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixSize = -dstFixSize;
                dstIndexOffset = 0;
                dstIndexAdjust = srcDataMax - srcDataTop;
            }
            if (isIndexed && !indexWriter.isOpen()) {
                BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                dstKFd = O3Utils.openRW(ff, path);
                BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                dstVFd = O3Utils.openRW(ff, path);
            }
        } catch (Throwable e) {
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    dstFixFd,
                    0,
                    0,
                    0,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstKFd,
                    dstVFd,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }

        publishCopyTask(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                null,
                ff,
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
                srcOooLo,
                srcOooHi,
                srcDataTop,
                srcDataMax,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                timestampMax,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                indexWriter,
                doneLatch
        );
    }

    private static void appendTimestampColumn(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataMax,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            AppendOnlyVirtualMemory dstFixMem,
            long dstLen
    ) {
        long dstFixFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixSize;
        try {
            dstFixSize = dstLen * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFd = -Math.abs(srcTimestampFd);
                dstFixAddr = O3Utils.mapRW(ff, -dstFixFd, dstFixSize);
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixSize = -dstFixSize;
            }
        } catch (Throwable e) {
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    dstFixFd,
                    0,
                    0,
                    0,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    0,
                    0,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }

        publishCopyTask(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                null,
                ff,
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
                srcOooLo,
                srcOooHi,
                0, // designated timestamp column cannot be added after table is created
                srcDataMax,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                timestampMax,
                partitionTimestamp, // <-- pass thru
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                null,
                doneLatch
        );
    }

    private static void appendVarColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            long activeFixFd,
            long activeVarFd,
            AppendOnlyVirtualMemory dstFixMem,
            AppendOnlyVirtualMemory dstVarMem,
            long dstLen
    ) {
        long dstFixAddr = 0;
        long dstFixOffset;
        long dstVarAddr = 0;
        long dstVarOffset;
        long dstVarAdjust;
        long dstVarSize = 0;
        long dstFixSize = 0;
        try {
            long l = O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
            dstFixSize = dstLen * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize || dstVarMem.getAppendAddressSize() < l) {
                dstFixOffset = (srcDataMax - srcDataTop) * Long.BYTES;
                dstFixAddr = O3Utils.mapRW(ff, Math.abs(activeFixFd), dstFixSize);

                if (dstFixOffset > 0) {
                    dstVarOffset = getVarColumnSize(
                            ff,
                            columnType,
                            Math.abs(activeVarFd),
                            Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES),
                            workerId
                    );
                } else {
                    dstVarOffset = 0;
                }

                dstVarSize = l + dstVarOffset;
                dstVarAddr = O3Utils.mapRW(ff, Math.abs(activeVarFd), dstVarSize);
                dstVarAdjust = 0;
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstVarAddr = dstVarMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixSize = -dstFixSize;
                dstVarOffset = 0;
                dstVarSize = -l;
                dstVarAdjust = dstVarMem.getAppendOffset();
            }
        } catch (Throwable e) {
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    activeFixFd,
                    dstFixAddr,
                    dstFixSize,
                    activeVarFd,
                    dstVarAddr,
                    dstVarSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    0,
                    0,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }
        publishCopyTask(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                null,
                ff,
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
                srcOooLo,
                srcOooHi,
                srcDataTop,
                srcDataMax,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                timestampMax,
                partitionTimestamp, // <-- pass thru
                activeFixFd,
                dstFixAddr,
                dstFixOffset,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                null,
                doneLatch
        );
    }

    private static void publishCopyTask(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            FilesFacade ff,
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
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
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
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long cursor = outboundPubSeq.next();
        if (cursor > -1) {
            publishCopyTaskHarmonized(
                    outboundQueue,
                    outboundPubSeq,
                    columnCounter,
                    partCounter,
                    ff,
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
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
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
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    isIndexed,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
                    indexWriter,
                    doneLatch
            );
        } else {
            publishCopyTaskContended(
                    configuration,
                    outboundQueue,
                    outboundPubSeq,
                    cursor, updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    columnCounter,
                    partCounter,
                    ff,
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
                    srcDataTop,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
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
                    dstVarAdjust,
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
                    indexWriter,
                    doneLatch
            );
        }
    }

    private static void publishCopyTaskContended(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            long cursor,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            FilesFacade ff,
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
            long srcDataTop,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
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
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        while (cursor == -2) {
            cursor = outboundPubSeq.next();
        }

        if (cursor == -1) {
            O3CopyJob.copy(
                    configuration,
                    updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    columnCounter,
                    partCounter,
                    ff,
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
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
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
                    dstVarAdjust,
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
                    indexWriter,
                    doneLatch
            );
        } else {
            publishCopyTaskHarmonized(
                    outboundQueue,
                    outboundPubSeq,
                    columnCounter,
                    partCounter,
                    ff,
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
                    srcDataLo, srcDataHi, srcDataTop,
                    srcDataMax,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
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
                    dstVarAdjust,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    isIndexed,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
                    indexWriter,
                    doneLatch
            );
        }
    }

    private static void publishCopyTaskHarmonized(
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            FilesFacade ff,
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
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
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
            boolean isIndexed,
            long cursor,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        O3CopyTask task = outboundQueue.get(cursor);
        task.of(
                columnCounter,
                partCounter,
                ff,
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
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
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
                dstVarAdjust,
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
                indexWriter,
                doneLatch
        );
        outboundPubSeq.done(cursor);
    }

    private static void mergeLastPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            long txn,
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
            boolean isIndexed,
            long activeFixFd,
            long activeVarFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long srcDataFixFd;
        long srcDataVarFd;
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                srcDataFixFd = -activeFixFd;
                srcDataVarFd = -activeVarFd;
                mergeVarColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataTop,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        srcDataFixFd,
                        srcDataVarFd
                );
                break;
            default:
                srcDataFixFd = -activeFixFd;
                mergeFixColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        srcDataTop,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch,
                        srcDataFixFd
                );
                break;
        }
    }

    private static void mergeMidPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            long txn,
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
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {

        // not set, we need to check file existence and read
        if (srcDataTop == -1) {
            try {
                srcDataTop = getSrcDataTop(workerId, ff, path, plen, columnName, srcDataMax);
            } catch (Throwable e) {
                tableWriter.o3BumpErrorCount();
                O3CopyJob.copyIdleQuick(
                        columnCounter,
                        ff,
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
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        0,
                        0,
                        tableWriter,
                        doneLatch
                );
                throw e;
            }
        }

        long srcDataFixFd = 0;
        long srcDataVarFd = 0;
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                try {
                    iFile(path.trimTo(plen), columnName);
                    srcDataFixFd = O3Utils.openRW(ff, path);
                    dFile(path.trimTo(plen), columnName);
                    srcDataVarFd = O3Utils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
                            ff,
                            0,
                            srcDataFixFd,
                            0,
                            0,
                            srcDataVarFd,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            0,
                            0,
                            tableWriter,
                            doneLatch
                    );
                    throw e;
                }

                mergeVarColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataTop,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        srcDataFixFd,
                        srcDataVarFd
                );
                break;
            default:
                try {
                    if (columnType < 0 && srcTimestampFd > 0) {
                        // ensure timestamp srcDataFixFd is always negative, we will close it externally
                        srcDataFixFd = -srcTimestampFd;
                    } else {
                        dFile(path.trimTo(plen), columnName);
                        srcDataFixFd = O3Utils.openRW(ff, path);
                    }
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
                            ff,
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
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            0,
                            0,
                            tableWriter,
                            doneLatch
                    );
                    throw e;
                }
                mergeFixColumn(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        txn,
                        prefixType,
                        prefixLo,
                        prefixHi,
                        mergeType,
                        mergeOOOLo,
                        mergeOOOHi,
                        mergeDataLo,
                        mergeDataHi,
                        mergeLen,
                        suffixType,
                        suffixLo,
                        suffixHi,
                        isIndexed,
                        srcDataTop,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        doneLatch,
                        srcDataFixFd
                );
                break;
        }
    }

    private static long getSrcDataTop(
            int workerId,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            long srcDataMax
    ) {
        boolean dFileExists = ff.exists(dFile(path.trimTo(plen), columnName));
        topFile(path.trimTo(plen), columnName);
        if (dFileExists && ff.exists(path)) {
            long buf = get8ByteBuf(workerId);
            long topFd = O3Utils.openRW(ff, path);
            try {
                if (ff.read(topFd, buf, Long.BYTES, 0) == Long.BYTES) {
                    return Unsafe.getUnsafe().getLong(buf);
                }
                throw CairoException.instance(ff.errno()).put("could not read [file=").put(path).put(']');
            } finally {
                ff.close(topFd);
            }
        }
        if (dFileExists) {
            return 0;
        }
        return srcDataMax;
    }

    private static void mergeFixColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataMax,
            long txn,
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
            boolean isIndexed,
            long srcDataTop,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch,
            long srcDataFixFd
    ) {
        int partCount = 0;
        long dstFixAppendOffset1;
        long srcDataFixSize = 0;
        long srcDataFixOffset;
        long srcDataFixAddr = 0;
        final int pDirNameLen;
        long dstFixAppendOffset2;
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long srcDataTopOffset;
        long dstFixSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        final long srcFixFd = Math.abs(srcDataFixFd);
        final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));

        try {
            txnPartition(path.trimTo(pplen), txn);
            pDirNameLen = path.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) << shl;
                final long srcDataMaxBytes = srcDataMax << shl;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);
                    setNull(columnType, srcDataFixAddr + srcDataActualBytes, srcDataTop);
                    Vect.memcpy(srcDataFixAddr, srcDataFixAddr + srcDataMaxBytes, srcDataActualBytes);
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, srcDataTop, workerId);
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;
                }
            } else {
                srcDataFixSize = srcDataMax << shl;
                srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);
                srcDataFixOffset = 0;
            }

            srcDataTopOffset = srcDataTop << shl;

            path.trimTo(pDirNameLen).concat(columnName).put(FILE_SUFFIX_D).$();
            dstFixFd = O3Utils.openRW(ff, path);
            dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - srcDataTop) << shl;
            dstFixAddr = O3Utils.mapRW(ff, dstFixFd, dstFixSize);

            // when prefix is "data" we need to reduce it by "srcDataTop"
            if (prefixType == O3_BLOCK_DATA) {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - srcDataTop) << shl;
                prefixHi -= srcDataTop;
            } else {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
            }

            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
            } else {
                dstFixAppendOffset2 = dstFixAppendOffset1;
            }

            if (suffixType == O3_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            if (isIndexed) {
                BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                dstKFd = O3Utils.openRW(ff, path);
                BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                dstVFd = O3Utils.openRW(ff, path);
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
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    timestampMergeIndexAddr,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    0,
                    0,
                    0,
                    dstFixFd,
                    dstFixAddr,
                    dstFixSize,
                    0,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstKFd,
                    dstVFd,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }

        partCounter.set(partCount);
        publishMultiCopyTasks(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                partCounter,
                ff,
                columnType,
                timestampMergeIndexAddr,
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
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooPartitionMin,
                oooPartitionMax,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                tableWriter,
                indexWriter,
                doneLatch
        );
    }

    private static void mergeVarColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            long txn,
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
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            long srcDataFixFd,
            long srcDataVarFd
    ) {
        int partCount = 0;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long srcDataFixOffset;
        long srcDataFixAddr = 0;
        final int pDirNameLen;
        long dstVarSize = 0;
        long srcDataTopOffset;
        long dstFixSize = 0;
        long dstFixAppendOffset1;
        long srcDataFixSize = 0;
        long srcDataVarSize = 0;
        long dstVarAppendOffset2;
        long dstFixAppendOffset2;
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long srcDataVarAddr = 0;
        long srcDataVarOffset = 0;
        long dstVarAppendOffset1 = 0;
        final long srcFixFd = Math.abs(srcDataFixFd);
        final long srcVarFd = Math.abs(srcDataVarFd);

        try {
            txnPartition(path.trimTo(pplen), txn);
            pDirNameLen = path.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) * Long.BYTES;
                final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);

                    if (srcDataActualBytes > 0) {
                        srcDataVarSize = getVarColumnSize(
                                ff,
                                columnType,
                                srcVarFd,
                                Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataActualBytes - Long.BYTES),
                                workerId
                        );
                    }

                    // at bottom of source var column set length of strings to null (-1) for as many strings
                    // as srcDataTop value.
                    if (columnType == ColumnType.STRING) {
                        srcDataVarOffset = srcDataVarSize;
                        srcDataVarSize += srcDataTop * Integer.BYTES + srcDataVarSize;
                        srcDataVarAddr = O3Utils.mapRW(ff, srcVarFd, srcDataVarSize);
                        Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        O3Utils.shiftCopyFixedSizeColumnData(-srcDataTop * Integer.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop, srcDataFixAddr + srcDataMaxBytes);
                        Vect.memcpy(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Integer.BYTES, srcDataVarOffset);
                    } else {
                        srcDataVarOffset = srcDataVarSize;
                        srcDataVarSize += srcDataTop * Long.BYTES + srcDataVarSize;
                        srcDataVarAddr = O3Utils.mapRW(ff, srcVarFd, srcDataVarSize);

                        Vect.setMemoryLong(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs64Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        O3Utils.shiftCopyFixedSizeColumnData(-srcDataTop * Long.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop, srcDataFixAddr + srcDataMaxBytes);
                        Vect.memcpy(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Long.BYTES, srcDataVarOffset);
                    }
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, srcDataTop, workerId);
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;

                    srcDataVarSize = getVarColumnSize(
                            ff,
                            columnType,
                            srcVarFd,
                            Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - srcDataFixOffset - Long.BYTES),
                            workerId
                    );
                    srcDataVarAddr = O3Utils.mapRO(ff, srcVarFd, srcDataVarSize);
                }
            } else {
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = O3Utils.mapRW(ff, srcFixFd, srcDataFixSize);
                srcDataFixOffset = 0;

                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES),
                        workerId
                );
                srcDataVarAddr = O3Utils.mapRO(ff, srcVarFd, srcDataVarSize);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = srcDataTop * Long.BYTES;

            path.trimTo(pDirNameLen).concat(columnName);
            int pColNameLen = path.length();
            path.put(FILE_SUFFIX_I).$();
            dstFixFd = O3Utils.openRW(ff, path);
            dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop) * Long.BYTES;
            dstFixAddr = O3Utils.mapRW(ff, dstFixFd, dstFixSize);

            path.trimTo(pColNameLen);
            path.put(FILE_SUFFIX_D).$();
            dstVarFd = O3Utils.openRW(ff, path);
            dstVarSize = srcDataVarSize - srcDataVarOffset + O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
            dstVarAddr = O3Utils.mapRW(ff, dstVarFd, dstVarSize);

            if (prefixType == O3_BLOCK_DATA) {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - srcDataTop) * Long.BYTES;
                prefixHi -= srcDataTop;
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
                    dstVarAppendOffset1 = O3Utils.getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    partCount++;
                    break;
                case O3_BLOCK_DATA:
                    dstVarAppendOffset1 = O3Utils.getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr + srcDataFixOffset, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                    partCount++;
                    break;
                default:
                    break;
            }

            // offset 2
            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                long oooLen = O3Utils.getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                long dataLen = O3Utils.getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr + srcDataFixOffset - srcDataTop * 8, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
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
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    timestampMergeIndexAddr,
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
                    0,
                    0,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }

        partCounter.set(partCount);
        publishMultiCopyTasks(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                partCounter,
                ff,
                columnType,
                timestampMergeIndexAddr,
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
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooPartitionMin,
                oooPartitionMax,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                tableWriter,
                null,
                doneLatch
        );
    }

    private static void setNull(int columnType, long addr, long count) {
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Vect.memset(addr, count, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                Vect.setMemoryShort(addr, (short) 0, count);
                break;
            case ColumnType.INT:
                Vect.setMemoryInt(addr, Numbers.INT_NaN, count);
                break;
            case ColumnType.FLOAT:
                Vect.setMemoryFloat(addr, Float.NaN, count);
                break;
            case ColumnType.SYMBOL:
                Vect.setMemoryInt(addr, -1, count);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case -ColumnType.TIMESTAMP:
            case ColumnType.TIMESTAMP:
                Vect.setMemoryLong(addr, Numbers.LONG_NaN, count);
                break;
            case ColumnType.DOUBLE:
                Vect.setMemoryDouble(addr, Double.NaN, count);
                break;
            case ColumnType.LONG256:
                // Long256 is null when all 4 longs are NaNs
                Vect.setMemoryLong(addr, Numbers.LONG_NaN, count * 4);
                break;
            default:
                break;
        }
    }

    private static void writeColumnTop(FilesFacade ff, Path path, CharSequence columnName, long columnTop, int workerId) {
        TableUtils.writeColumnTop(
                ff,
                path,
                columnName,
                columnTop,
                get8ByteBuf(workerId)
        );
    }

    private static void appendNewPartition(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataMax,
            boolean isIndexed,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long dstFixSize = 0;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;

        try {
            switch (columnType) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    setPath(path.trimTo(plen), columnName, FILE_SUFFIX_I);
                    dstFixFd = O3Utils.openRW(ff, path);
                    dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES;
                    dstFixAddr = O3Utils.mapRW(ff, dstFixFd, dstFixSize);

                    setPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                    dstVarFd = O3Utils.openRW(ff, path);
                    dstVarSize = O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    dstVarAddr = O3Utils.mapRW(ff, dstVarFd, dstVarSize);
                    break;
                default:
                    setPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                    dstFixFd = O3Utils.openRW(ff, path);
                    dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                    dstFixAddr = O3Utils.mapRW(ff, dstFixFd, dstFixSize);
                    if (isIndexed) {
                        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                        dstKFd = O3Utils.openRW(ff, path);
                        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                        dstVFd = O3Utils.openRW(ff, path);
                    }
                    break;
            }
        } catch (Throwable e) {
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    ff,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    dstFixFd,
                    dstFixAddr,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarSize,
                    0,
                    0,
                    0,
                    dstKFd,
                    dstVFd,
                    tableWriter,
                    doneLatch
            );
            throw e;
        }

        publishCopyTask(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                columnCounter,
                null,
                ff,
                columnType,
                O3_BLOCK_O3,
                timestampMergeIndexAddr,
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
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                timestampMin,
                timestampMax,
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
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
                isIndexed,
                0,
                0,
                0,
                false, // partition does not mutate above the append line
                tableWriter,
                indexWriter,
                doneLatch
        );
    }

    private static void setPath(Path path, CharSequence columnName, CharSequence suffix) {
        path.concat(columnName).put(suffix).$();
    }

    private static void publishMultiCopyTasks(
            CairoConfiguration configuration,
            RingQueue<O3CopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<O3PartitionUpdateTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            int columnType,
            long timestampMergeIndexAddr,
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
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
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
            long dstFixAppendOffset1,
            long dstFixAppendOffset2,
            long dstVarAppendOffset1,
            long dstVarAppendOffset2,
            long dstKFd,
            long dstVFd,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        final boolean partitionMutates = true;
        switch (prefixType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        prefixType,
                        0,
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
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        prefixLo,
                        prefixHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
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
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        prefixType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixOffset,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarOffset,
                        srcDataVarSize,
                        prefixLo, prefixHi, srcDataTopOffset,
                        srcDataMax,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
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
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }

        switch (mergeType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        mergeType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixOffset,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarOffset,
                        srcDataVarSize,
                        0, 0, srcDataTopOffset,
                        srcDataMax,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        mergeType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixOffset,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarOffset,
                        srcDataVarSize,
                        mergeDataLo, mergeDataHi, srcDataTopOffset,
                        srcDataMax,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case O3_BLOCK_MERGE:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        mergeType,
                        timestampMergeIndexAddr,
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
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }

        switch (suffixType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        suffixType,
                        0,
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
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        suffixLo,
                        suffixHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        columnType,
                        suffixType,
                        0,
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
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        indexWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        // increment worker index to leave room for anonymous worker to steal work
        openColumn(workerId + 1, queue.get(cursor), cursor, subSeq);
        return true;
    }

    private void openColumn(int workerId, O3OpenColumnTask task, long cursor, Sequence subSeq) {
        openColumn(
                workerId,
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                task,
                cursor,
                subSeq
        );
    }
}
