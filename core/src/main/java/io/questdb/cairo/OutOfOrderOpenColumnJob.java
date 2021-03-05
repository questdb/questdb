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
import io.questdb.mp.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderCopyTask;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import io.questdb.tasks.OutOfOrderUpdPartitionSizeTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.OutOfOrderUtils.get8ByteBuf;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderOpenColumnJob extends AbstractQueueConsumerJob<OutOfOrderOpenColumnTask> {
    public static final int OPEN_MID_PARTITION_FOR_APPEND = 1;
    public static final int OPEN_LAST_PARTITION_FOR_APPEND = 2;
    public static final int OPEN_MID_PARTITION_FOR_MERGE = 3;
    public static final int OPEN_LAST_PARTITION_FOR_MERGE = 4;
    public static final int OPEN_NEW_PARTITION_FOR_APPEND = 5;
    private final CairoConfiguration configuration;
    private final RingQueue<OutOfOrderCopyTask> outboundQueue;
    private final Sequence outboundPubSeq;
    private final RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue;
    private final MPSequence updPartitionSizePubSeq;

    public OutOfOrderOpenColumnJob(MessageBus messageBus) {
        super(messageBus.getOutOfOrderOpenColumnQueue(), messageBus.getOutOfOrderOpenColumnSubSequence());
        this.configuration = messageBus.getConfiguration();
        this.outboundQueue = messageBus.getOutOfOrderCopyQueue();
        this.outboundPubSeq = messageBus.getOutOfOrderCopyPubSequence();
        this.updPartitionSizeTaskQueue = messageBus.getOutOfOrderUpdPartitionSizeQueue();
        this.updPartitionSizePubSeq = messageBus.getOutOfOrderUpdPartitionSizePubSequence();
    }

    public static void openColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            int openColumnMode,
            FilesFacade ff,
            CharSequence pathToTable,
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
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampLo,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
            SOUnboundedCountDownLatch doneLatch
    ) {
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), oooTimestampLo);
        final int plen = path.length();
        // append jobs do not set value of part counter, we do it here for those
        // todo: cache
        final AtomicInteger partCounter = new AtomicInteger(1);
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
                        pathToTable,
                        columnName,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch
                );
                break;
            case OPEN_LAST_PARTITION_FOR_APPEND:
                appendLastPartition(
                        workerId,
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        ff,
                        path,
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        activeFixFd,
                        activeVarFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
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
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop, srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        tableWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }
    }

    public static void openColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            OutOfOrderOpenColumnTask task,
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
        final long oooTimestampMin = task.getOooTimestampMin();
        final long oooTimestampMax = task.getOooTimestampMax();
        final long oooTimestampLo = task.getOooTimestampLo();
        final long oooTimestampHi = task.getOooTimestampHi();
        final long srcDataMax = task.getSrcDataMax();
        final long tableFloorOfMaxTimestamp = task.getTableFloorOfMaxTimestamp();
        final long dataTimestampHi = task.getDataTimestampHi();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final AtomicInteger columnCounter = task.getColumnCounter();
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
        final long activeTop = task.getActiveTop();
        final TableWriter tableWriter = task.getTableWriter();
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
                columnType,
                timestampMergeIndexAddr,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampLo,
                oooTimestampHi,
                activeTop, srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
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
                doneLatch
        );
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
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
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
                tableWriter.bumpOooErrorCount();
                OutOfOrderCopyJob.copyIdleQuick(
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
                    dstFixFd = OutOfOrderUtils.openRW(ff, path);
                    // open data file now
                    dFile(path.trimTo(plen), columnName);
                    dstVarFd = OutOfOrderUtils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.bumpOooErrorCount();
                    OutOfOrderCopyJob.copyIdleQuick(
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
                        pathToTable,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        dstFixFd,
                        dstVarFd,
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
                        ff, pathToTable,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        dstLen
                );
                break;
            default:
                try {
                    dFile(path.trimTo(plen), columnName);
                    dstFixFd = OutOfOrderUtils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.bumpOooErrorCount();
                    OutOfOrderCopyJob.copyIdleQuick(
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
                        pathToTable,
                        columnName,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        dstFixFd,
                        dstLen
                );
                break;
        }
    }

    private static void appendFixColumn(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            long dstFixFd,
            long dstLen
    ) {
        long dstKFd = 0;
        long dstVFd = 0;
        long dstFixOffset;
        long dstIndexOffset;
        long dstFixSize = 0;
        long dstFixAddr = 0;
        final int shl = ColumnType.pow2SizeOf(columnType);

        try {
            dstFixSize = dstLen << shl;
            dstFixOffset = (srcDataMax - srcDataTop) << shl;
            dstFixAddr = OutOfOrderUtils.mapRW(ff, Math.abs(dstFixFd), dstFixSize);
            dstIndexOffset = dstFixOffset;
            if (isIndexed) {
                BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                dstKFd = OutOfOrderUtils.openRW(ff, path);
                BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                dstVFd = OutOfOrderUtils.openRW(ff, path);
            }
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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
                partCounter,
                ff,
                pathToTable,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcDataTop,
                srcOooLo,
                srcOooHi,
                srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                0,
                0,
                0,
                0,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                doneLatch
        );
    }

    private static void appendTimestampColumn(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            CharSequence pathToTable,
            AtomicInteger partCounter,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            long dstLen
    ) {
        long dstFixFd = 0;
        long dstFixOffset;
        long dstFixSize = 0;
        long dstFixAddr = 0;
        try {
            dstFixSize = dstLen * Long.BYTES;
            dstFixOffset = srcDataMax * Long.BYTES;
            dstFixFd = -srcTimestampFd;
            dstFixAddr = OutOfOrderUtils.mapRW(ff, Math.abs(dstFixFd), dstFixSize);
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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
                partCounter,
                ff,
                pathToTable,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcDataTop,
                srcOooLo,
                srcOooHi,
                srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
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
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                doneLatch
        );
    }

    private static void appendVarColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            CharSequence pathToTable,
            AtomicInteger partCounter,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch,
            long dstFixFd,
            long dstVarFd,
            long dstLen
    ) {
        long dstVarAddr = 0;
        long dstVarOffset;
        long dstFixOffset;
        long dstVarSize = 0;
        long dstFixSize = 0;
        long dstFixAddr = 0;
        try {
            dstFixSize = dstLen * Long.BYTES;
            dstFixAddr = OutOfOrderUtils.mapRW(ff, Math.abs(dstFixFd), dstFixSize);
            dstFixOffset = (srcDataMax - srcDataTop) * Long.BYTES;
            if (dstFixOffset > 0) {
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        Math.abs(dstVarFd),
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES),
                        workerId
                );
            } else {
                dstVarOffset = 0;
            }

            dstVarSize = OutOfOrderUtils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize) + dstVarOffset;
            dstVarAddr = OutOfOrderUtils.mapRW(ff, Math.abs(dstVarFd), dstVarSize);

        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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
                partCounter,
                ff,
                pathToTable,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcDataTop,
                srcOooLo,
                srcOooHi,
                srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarSize,
                0,
                0,
                0,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                doneLatch
        );
    }

    private static void publishCopyTask(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            CharSequence pathToTable,
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
            long srcDataTopOffset,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
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
                    pathToTable,
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
                    srcDataTopOffset,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    isIndexed,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
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
                    pathToTable,
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
                    srcDataTopOffset,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    isIndexed,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
                    doneLatch
            );
        }
    }

    private static void publishCopyTaskContended(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            long cursor,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            CharSequence pathToTable,
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
            long srcDataTopOffset,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        while (cursor == -2) {
            cursor = outboundPubSeq.next();
        }

        if (cursor == -1) {
            OutOfOrderCopyJob.copy(
                    configuration,
                    updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    columnCounter,
                    partCounter,
                    ff,
                    pathToTable,
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
                    srcDataTopOffset,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    isIndexed,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
                    doneLatch
            );
        } else {
            publishCopyTaskHarmonized(
                    outboundQueue,
                    outboundPubSeq,
                    columnCounter,
                    partCounter,
                    ff,
                    pathToTable,
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
                    srcDataTopOffset,
                    srcDataLo,
                    srcDataHi,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    dstFixFd,
                    dstFixAddr,
                    dstFixOffset,
                    dstFixSize,
                    dstVarFd,
                    dstVarAddr,
                    dstVarOffset,
                    dstVarSize,
                    dstKFd,
                    dstVFd,
                    dstIndexOffset,
                    isIndexed,
                    cursor,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    partitionMutates,
                    tableWriter,
                    doneLatch
            );
        }
    }

    private static void publishCopyTaskHarmonized(
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            CharSequence pathToTable,
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
            long srcDataTopOffset,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            boolean isIndexed,
            long cursor,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        OutOfOrderCopyTask task = outboundQueue.get(cursor);
        task.of(
                columnCounter,
                partCounter,
                ff,
                pathToTable,
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
                srcDataTopOffset,
                srcDataLo,
                srcDataHi,
                srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooPartitionLo,
                srcOooPartitionHi,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                tableWriter,
                doneLatch
        );
        outboundPubSeq.done(cursor);
    }

    private static void appendLastPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long activeFixFd,
            long activeVarFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
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
                        pathToTable,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        -activeFixFd,
                        -activeVarFd,
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
                        pathToTable,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        -srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
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
                        pathToTable,
                        columnName,
                        partCounter,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcDataTop,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch,
                        -activeFixFd,
                        dstLen
                );
                break;
        }
    }

    private static void mergeLastPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
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
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
                        doneLatch,
                        srcDataFixFd
                );
                break;
        }
    }

    private static void mergeMidPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
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
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataTop,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
            SOUnboundedCountDownLatch doneLatch
    ) {

        // not set, we need to check file existence and read
        if (srcDataTop == -1) {
            try {
                srcDataTop = getSrcDataTop(workerId, ff, path, plen, columnName, srcDataMax);
            } catch (Throwable e) {
                tableWriter.bumpOooErrorCount();
                OutOfOrderCopyJob.copyIdleQuick(
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
                    srcDataFixFd = OutOfOrderUtils.openRW(ff, path);
                    dFile(path.trimTo(plen), columnName);
                    srcDataVarFd = OutOfOrderUtils.openRW(ff, path);
                } catch (Throwable e) {
                    tableWriter.bumpOooErrorCount();
                    OutOfOrderCopyJob.copyIdleQuick(
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
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
                        srcDataFixFd = OutOfOrderUtils.openRW(ff, path);
                    }
                } catch (Throwable e) {
                    tableWriter.bumpOooErrorCount();
                    OutOfOrderCopyJob.copyIdleQuick(
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
                        plen,
                        pathToTable,
                        columnName,
                        partCounter,
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
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
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
            long topFd = OutOfOrderUtils.openRW(ff, path);
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
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
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
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
            newPartitionName(path.trimTo(plen), txn);
            pDirNameLen = path.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) << shl;
                final long srcDataMaxBytes = srcDataMax << shl;
                if (srcDataTop > prefixHi || prefixType == OO_BLOCK_OO) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);
                    setNull(columnType, srcDataFixAddr + srcDataActualBytes, srcDataTop);
                    Unsafe.getUnsafe().copyMemory(srcDataFixAddr, srcDataFixAddr + srcDataMaxBytes, srcDataActualBytes);
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, srcDataTop, workerId);
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;
                }
            } else {
                srcDataFixSize = srcDataMax << shl;
                srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);
                srcDataFixOffset = 0;
            }

            srcDataTopOffset = srcDataTop << shl;

            path.trimTo(pDirNameLen).concat(columnName).put(FILE_SUFFIX_D).$();
            dstFixFd = OutOfOrderUtils.openRW(ff, path);
            dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - srcDataTop) << shl;
            dstFixAddr = OutOfOrderUtils.mapRW(ff, dstFixFd, dstFixSize);

            // when prefix is "data" we need to reduce it by "srcDataTop"
            if (prefixType == OO_BLOCK_DATA) {
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

            if (suffixType == OO_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            if (isIndexed) {
                BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                dstKFd = OutOfOrderUtils.openRW(ff, path);
                BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                dstVFd = OutOfOrderUtils.openRW(ff, path);
            }

            if (prefixType != OO_BLOCK_NONE) {
                partCount++;
            }

            if (mergeType != OO_BLOCK_NONE) {
                partCount++;
            }

            if (suffixType != OO_BLOCK_NONE) {
                partCount++;
            }
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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

        publishMultiCopyTasks(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                partCount,
                columnCounter,
                partCounter,
                ff,
                pathToTable,
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
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
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
                doneLatch
        );
    }

    private static void mergeVarColumn(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
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
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
            newPartitionName(path.trimTo(plen), txn);
            pDirNameLen = path.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) * Long.BYTES;
                final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                if (srcDataTop > prefixHi || prefixType == OO_BLOCK_OO) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);

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
                        srcDataVarAddr = OutOfOrderUtils.mapRW(ff, srcVarFd, srcDataVarSize);
                        Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        OutOfOrderUtils.shiftCopyFixedSizeColumnData(-srcDataTop * Integer.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop, srcDataFixAddr + srcDataMaxBytes);
                        Unsafe.getUnsafe().copyMemory(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Integer.BYTES, srcDataVarOffset);
                    } else {
                        srcDataVarOffset = srcDataVarSize;
                        srcDataVarSize += srcDataTop * Long.BYTES + srcDataVarSize;
                        srcDataVarAddr = OutOfOrderUtils.mapRW(ff, srcVarFd, srcDataVarSize);

                        Vect.setMemoryLong(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs64Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        OutOfOrderUtils.shiftCopyFixedSizeColumnData(-srcDataTop * Long.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop, srcDataFixAddr + srcDataMaxBytes);
                        Unsafe.getUnsafe().copyMemory(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Long.BYTES, srcDataVarOffset);
                    }
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, srcDataTop, workerId);
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;

                    srcDataVarSize = getVarColumnSize(
                            ff,
                            columnType,
                            srcVarFd,
                            Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - srcDataFixOffset - Long.BYTES),
                            workerId
                    );
                    srcDataVarAddr = OutOfOrderUtils.mapRO(ff, srcVarFd, srcDataVarSize);
                }
            } else {
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = OutOfOrderUtils.mapRW(ff, srcFixFd, srcDataFixSize);
                srcDataFixOffset = 0;

                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES),
                        workerId
                );
                srcDataVarAddr = OutOfOrderUtils.mapRO(ff, srcVarFd, srcDataVarSize);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = srcDataTop * Long.BYTES;

            path.trimTo(pDirNameLen).concat(columnName);
            int pColNameLen = path.length();
            path.put(FILE_SUFFIX_I).$();
            dstFixFd = OutOfOrderUtils.openRW(ff, path);
            dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop) * Long.BYTES;
            dstFixAddr = OutOfOrderUtils.mapRW(ff, dstFixFd, dstFixSize);

            path.trimTo(pColNameLen);
            path.put(FILE_SUFFIX_D).$();
            dstVarFd = OutOfOrderUtils.openRW(ff, path);
            dstVarSize = srcDataVarSize - srcDataVarOffset + OutOfOrderUtils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
            dstVarAddr = OutOfOrderUtils.mapRW(ff, dstVarFd, dstVarSize);

            if (prefixType == OO_BLOCK_DATA) {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - srcDataTop) * Long.BYTES;
                prefixHi -= srcDataTop;
            } else {
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;
            }

            if (suffixType == OO_BLOCK_DATA && srcDataTop > 0) {
                suffixHi -= srcDataTop;
                suffixLo -= srcDataTop;
            }

            // configure offsets
            switch (prefixType) {
                case OO_BLOCK_OO:
                    dstVarAppendOffset1 = OutOfOrderUtils.getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    partCount++;
                    break;
                case OO_BLOCK_DATA:
                    dstVarAppendOffset1 = OutOfOrderUtils.getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr + srcDataFixOffset, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                    partCount++;
                    break;
                default:
                    break;
            }

            // offset 2
            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                long oooLen = OutOfOrderUtils.getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                long dataLen = OutOfOrderUtils.getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr + srcDataFixOffset - srcDataTop * 8, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
            } else {
                dstFixAppendOffset2 = dstFixAppendOffset1;
                dstVarAppendOffset2 = dstVarAppendOffset1;
            }

            if (mergeType != OO_BLOCK_NONE) {
                partCount++;
            }

            if (suffixType != OO_BLOCK_NONE) {
                partCount++;
            }
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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

        publishMultiCopyTasks(
                configuration,
                outboundQueue,
                outboundPubSeq,
                updPartitionSizeTaskQueue,
                updPartitionSizePubSeq,
                partCount,
                columnCounter,
                partCounter,
                ff,
                pathToTable,
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
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
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
                doneLatch
        );
    }

    private static void setNull(int columnType, long addr, long count) {
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Unsafe.getUnsafe().setMemory(addr, count, (byte) 0);
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
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger partCounter,
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
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            TableWriter tableWriter,
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
                    oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_I);
                    dstFixFd = OutOfOrderUtils.openRW(ff, path);
                    dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES;
                    dstFixAddr = OutOfOrderUtils.mapRW(ff, dstFixFd, dstFixSize);

                    oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                    dstVarFd = OutOfOrderUtils.openRW(ff, path);
                    dstVarSize = OutOfOrderUtils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    dstVarAddr = OutOfOrderUtils.mapRW(ff, dstVarFd, dstVarSize);
                    break;
                default:
                    oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                    dstFixFd = OutOfOrderUtils.openRW(ff, path);
                    dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                    dstFixAddr = OutOfOrderUtils.mapRW(ff, dstFixFd, dstFixSize);
                    if (isIndexed) {
                        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                        dstKFd = OutOfOrderUtils.openRW(ff, path);
                        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                        dstVFd = OutOfOrderUtils.openRW(ff, path);
                    }
                    break;
            }
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            OutOfOrderCopyJob.copyIdleQuick(
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
                partCounter,
                ff,
                pathToTable,
                columnType,
                OO_BLOCK_OO,
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
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                srcOooLo,
                srcOooHi,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                dstFixFd,
                dstFixAddr,
                0,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                0,
                dstVarSize,
                dstKFd,
                dstVFd,
                0,
                isIndexed,
                0,
                0,
                0,
                false, // partition does not mutate above the append line
                tableWriter,
                doneLatch
        );
    }

    private static void oooSetPath(Path path, CharSequence columnName, CharSequence suffix) {
        path.concat(columnName).put(suffix).$();
    }

    private static void publishMultiCopyTasks(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            int partCount,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            CharSequence pathToTable,
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
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
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
            SOUnboundedCountDownLatch doneLatch
    ) {
        final boolean partitionMutates = true;
        partCounter.set(partCount);
        switch (prefixType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        0,
                        0,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        prefixLo,
                        prefixHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        0,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        prefixLo,
                        prefixHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        0,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }

        switch (mergeType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        0,
                        0,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        mergeDataLo,
                        mergeDataHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            case OO_BLOCK_MERGE:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        mergeDataLo,
                        mergeDataHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            default:
                break;
        }

        switch (suffixType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        0,
                        0,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        suffixLo,
                        suffixHi,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
                        doneLatch
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        configuration,
                        outboundQueue,
                        outboundPubSeq,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        columnCounter,
                        partCounter,
                        ff,
                        pathToTable,
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
                        srcDataTopOffset,
                        suffixLo,
                        suffixHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        srcOooMax,
                        srcOooLo,
                        srcOooHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter,
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

    private void openColumn(int workerId, OutOfOrderOpenColumnTask task, long cursor, Sequence subSeq) {
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
