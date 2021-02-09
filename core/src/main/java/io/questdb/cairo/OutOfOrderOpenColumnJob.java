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
import io.questdb.mp.*;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderCopyTask;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import io.questdb.tasks.OutOfOrderUpdPartitionSizeTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderOpenColumnJob extends AbstractQueueConsumerJob<OutOfOrderOpenColumnTask> {
    public static final int OPEN_MID_PARTITION_FOR_APPEND = 1;
    public static final int OPEN_LAST_PARTITION_FOR_APPEND = 2;
    public static final int OPEN_MID_PARTITION_FOR_MERGE = 3;
    public static final int OPEN_LAST_PARTITION_FOR_MERGE = 4;
    public static final int OPEN_NEW_PARTITION_FOR_APPEND = 5;
    private static final Log LOG = LogFactory.getLog(OutOfOrderOpenColumnJob.class);
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
            long activeTop,
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
                oooOpenMidPartitionForAppend(
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
                oooOpenLastPartitionForAppend(
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
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        isIndexed,
                        activeFixFd,
                        activeVarFd,
                        activeTop,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch
                );
                break;
            case OPEN_MID_PARTITION_FOR_MERGE:
                oooOpenMidPartitionForMerge(
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
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        isIndexed,
                        tableWriter,
                        doneLatch
                );
                break;
            case OPEN_LAST_PARTITION_FOR_MERGE:
                oooOpenLastPartitionForMerge(
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
                        activeTop,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        doneLatch
                );
                break;
            case OPEN_NEW_PARTITION_FOR_APPEND:
                oooOpenNewPartitionForAppend(
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
                suffixType,
                suffixLo,
                suffixHi,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                isIndexed,
                activeFixFd,
                activeVarFd,
                activeTop,
                tableWriter,
                doneLatch
        );
    }

    private static long mapReadWriteOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        long addr = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (addr > -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap column [file=").put(path).put(", fd=").put(fd).put(", size=").put(size).put(']');
    }

    private static long openReadWriteOrFail(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd > -1) {
            LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private static void truncateToSizeOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        if (ff.isRestrictedFileSystem()) {
            return;
        }
        if (!ff.truncate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("could not resize [file=").put(path).put(", size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    private static long getVarColumnSize(FilesFacade ff, int columnType, long dataFd, long lastValueOffset) {
        final long addr;
        final long offset;
        if (columnType == ColumnType.STRING) {
            addr = ff.mmap(dataFd, lastValueOffset + Integer.BYTES, 0, Files.MAP_RO);
            final int len = Unsafe.getUnsafe().getInt(addr + lastValueOffset);
            ff.munmap(addr, lastValueOffset + Integer.BYTES);
            if (len < 1) {
                offset = lastValueOffset + Integer.BYTES;
            } else {
                offset = lastValueOffset + Integer.BYTES + len * 2L; // character bytes
            }
        } else {
            // BINARY
            addr = ff.mmap(dataFd, lastValueOffset + Long.BYTES, 0, Files.MAP_RO);
            final long len = Unsafe.getUnsafe().getLong(addr + lastValueOffset);
            ff.munmap(addr, lastValueOffset + Long.BYTES);
            if (len < 1) {
                offset = lastValueOffset + Long.BYTES;
            } else {
                offset = lastValueOffset + Long.BYTES + len;
            }
        }
        return offset;
    }

    private static void oooOpenMidPartitionForAppend(
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
        long dstKFd = 0;
        long dstVFd = 0;
        long dstIndexOffset = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstFixOffset;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstVarOffset = 0;
        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                iFile(path.trimTo(plen), columnName);
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = dstLen * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixOffset = srcDataMax * Long.BYTES;

                // open data file now
                dFile(path.trimTo(plen), columnName);
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES)
                );
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize) + dstVarOffset;
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);
                break;
            case -ColumnType.TIMESTAMP:
                dstFixSize = dstLen * Long.BYTES;
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFd = -srcTimestampFd;
                truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);
                break;
            default:
                final int shl = ColumnType.pow2SizeOf(columnType);
                dstFixSize = dstLen << shl;
                dstFixOffset = srcDataMax << shl;
                dFile(path.trimTo(plen), columnName);
                dstFixFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, dstFixFd, dstFixSize);

                dstIndexOffset = dstFixOffset;
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }
                break;
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
            long srcDataTop,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
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
                    srcDataTop,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    srcDataVarFd,
                    srcDataVarAddr,
                    srcDataVarSize,
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
                    srcDataTop,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    srcDataVarFd,
                    srcDataVarAddr,
                    srcDataVarSize,
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
            long srcDataTop,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
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
                    srcDataTop,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    srcDataVarFd,
                    srcDataVarAddr,
                    srcDataVarSize,
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
                    srcDataTop,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    srcDataVarFd,
                    srcDataVarAddr,
                    srcDataVarSize,
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
            long srcDataTop,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
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
                srcDataTop,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
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

    private static void oooOpenLastPartitionForAppend(
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
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            boolean isIndexed,
            long activeFixFd,
            long activeVarFd,
            long activeTop,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long dstFixFd;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixSize;
        long dstVarFd = 0;
        long dstVarSize = 0;
        long dstVarAddr = 0;
        long dstVarOffset = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        long dstIndexOffset = 0;
        final long dstLen = srcOooHi - srcOooLo + 1;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFd = -activeFixFd;
                dstFixSize = dstLen * Long.BYTES + dstFixOffset;
                truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);

                dstVarFd = -activeVarFd;
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        -dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES)
                );
                dstVarSize = dstVarOffset + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, null, -dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, null, -dstVarFd, dstVarSize);
                break;
            default:
                int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                long oooSize = dstLen << shl;
                dstFixOffset = srcDataMax << shl;
                dstFixFd = -activeFixFd;
                dstFixSize = oooSize + dstFixOffset;
                truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                    dstIndexOffset = dstFixOffset;
                }
                break;
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
                srcOooLo, // the entire OOO block gets appended
                srcOooHi, // its size is the same as partition size
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
                false, // this is append, rest of the partition does not mutate
                tableWriter,
                doneLatch
        );
    }

    private static void oooOpenLastPartitionForMerge(
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
            long activeFixFd,
            long activeVarFd,
            long activeTop,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long srcDataFixFd;
        long srcDataFixAddr;
        long srcDataFixSize;
        long srcDataVarFd = 0;
        long srcDataVarAddr = 0;
        long srcDataVarSize = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstFixAppendOffset1;
        long dstFixAppendOffset2;
        long dstVarAppendOffset1 = 0;
        long dstVarAppendOffset2 = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        int partCount = 0;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                iFile(path.trimTo(plen), columnName);
                srcDataFixFd = -activeFixFd;
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = mapRO(ff, -srcDataFixFd, srcDataFixSize);

                // open data file now
                dFile(path.trimTo(plen), columnName);
                srcDataVarFd = -activeVarFd;
                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        -srcDataVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES)
                );
                srcDataVarAddr = mapRO(ff, -srcDataVarFd, srcDataVarSize);

                appendTxnToPath(path.trimTo(plen), txn);
                path.concat(columnName);
                final int pColNameLen = path.length();

                path.put(FILE_SUFFIX_I).$();
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                path.trimTo(pColNameLen);
                path.put(FILE_SUFFIX_D).$();
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarSize = srcDataVarSize + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);

                // configure offsets
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                        partCount++;
                        break;
                    case OO_BLOCK_DATA:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                        partCount++;
                        break;
                    default:
                        break;
                }

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    long oooLen = getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                    dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                    dstVarAppendOffset2 = dstVarAppendOffset1;
                }

                break;
            default:
                srcDataFixFd = -activeFixFd;
                if (activeTop > 0) {
                    System.out.println("ok");
                }
                final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                dFile(path.trimTo(plen), columnName);
                srcDataFixSize = (srcDataMax-activeTop) << shl;
                srcDataFixAddr = mapRO(ff, -srcDataFixFd, srcDataFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - activeTop) << shl;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                // when prefix is "data" we need to reduce it by "activeTop"
                if (prefixType == OO_BLOCK_DATA) {
                    dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - activeTop) << shl;
                    prefixHi -= activeTop;
                } else {
                    dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                }

                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                }

                if (suffixType == OO_BLOCK_DATA && activeTop > 0) {
                    suffixHi -= activeTop;
                    suffixLo -= activeTop;
                }

                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }

                if (prefixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                break;
        }

        if (mergeType != OO_BLOCK_NONE) {
            partCount++;
        }

        if (suffixType != OO_BLOCK_NONE) {
            partCount++;
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
                activeTop,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
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

    private static void oooOpenMidPartitionForMerge(
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
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean isIndexed,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        long srcDataFixFd;
        long srcDataFixAddr;
        long srcDataFixSize;
        long srcDataVarFd = 0;
        long srcDataVarAddr = 0;
        long srcDataVarSize = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        int partCount = 0;
        long dstFixAppendOffset1;
        long dstFixAppendOffset2;
        long dstVarAppendOffset1 = 0;
        long dstVarAppendOffset2 = 0;
        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                iFile(path.trimTo(plen), columnName);
                srcDataFixFd = openReadWriteOrFail(ff, path);
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = mapRO(ff, srcDataFixFd, srcDataFixSize);

                dFile(path.trimTo(plen), columnName);
                srcDataVarFd = openReadWriteOrFail(ff, path);
                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcDataVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES)
                );
                srcDataVarAddr = mapRO(ff, srcDataVarFd, srcDataVarSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPath(path, columnName, FILE_SUFFIX_I);

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = dstLen * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPath(path, columnName, FILE_SUFFIX_D);
                dstVarSize = srcDataVarSize + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                dstVarFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);

                // configure offsets
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                        partCount++;
                        break;
                    case OO_BLOCK_DATA:
                        partCount++;
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                        break;
                    default:
                        dstVarAppendOffset1 = 0;
                        break;
                }

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                dstVarAppendOffset2 = dstVarAppendOffset1;
                dstFixAppendOffset2 = dstFixAppendOffset1;
                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    final long oooLen = getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    final long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                    dstVarAppendOffset2 += oooLen + dataLen;
                    dstFixAppendOffset2 += mergeLen * Long.BYTES;
                    partCount++;
                }

                if (suffixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                break;
            default:
                if (columnType < 0 && srcTimestampFd > 0) {
                    // ensure timestamp srcDataFixFd is always negative, we will close it externally
                    srcDataFixFd = -srcTimestampFd;
                } else {
                    dFile(path.trimTo(plen), columnName);
                    srcDataFixFd = openReadWriteOrFail(ff, path);
                }

                final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                srcDataFixSize = srcDataMax << shl;
                dFile(path.trimTo(plen), columnName);
                srcDataFixAddr = mapRO(ff, Math.abs(srcDataFixFd), srcDataFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = dstLen << shl;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                }

                // we have "src" index
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
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

                break;
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
                0,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
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

    private static void oooOpenNewPartitionForAppend(
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
        final long dstFixFd;
        final long dstFixAddr;
        final long dstFixSize;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_I);
                dstFixFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstFixFd, (srcOooHi - srcOooLo + 1) * Long.BYTES);
                dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES;
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);
                break;
            default:
                oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }
                break;
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
            long srcDataTop,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                        srcDataTop,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
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
        openColumn(queue.get(cursor), cursor, subSeq);
        return true;
    }

    private void openColumn(OutOfOrderOpenColumnTask task, long cursor, Sequence subSeq) {
        openColumn(
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
