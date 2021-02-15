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
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderCopyTask;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import io.questdb.tasks.OutOfOrderUpdPartitionSizeTask;

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

    private static long mapRW(FilesFacade ff, long fd, long size) {
        long addr = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (addr > -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap column [fd=").put(fd).put(", size=").put(size).put(']');
    }

    private static long openRW(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd > -1) {
            LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private static void allocateDiskSpace(FilesFacade ff, long fd, long size) {
        if (ff.isRestrictedFileSystem()) {
            return;
        }
        if (!ff.allocate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("could allocate file [size=").put(size).put(", fd=").put(fd).put(']');
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
                dstFixFd = openRW(ff, path);
                dstFixSize = dstLen * Long.BYTES;
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);
                dstFixOffset = srcDataMax * Long.BYTES;

                // open data file now
                dFile(path.trimTo(plen), columnName);
                dstVarFd = openRW(ff, path);
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES)
                );
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize) + dstVarOffset;
                allocateDiskSpace(ff, dstVarFd, dstVarSize);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize);
                break;
            case -ColumnType.TIMESTAMP:
                dstFixSize = dstLen * Long.BYTES;
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFd = -srcTimestampFd;
                allocateDiskSpace(ff, -dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize);
                break;
            default:
                final int shl = ColumnType.pow2SizeOf(columnType);
                dstFixSize = dstLen << shl;
                dstFixOffset = srcDataMax << shl;
                dFile(path.trimTo(plen), columnName);
                dstFixFd = openRW(ff, path);
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

                dstIndexOffset = dstFixOffset;
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openRW(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openRW(ff, path);
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
                allocateDiskSpace(ff, -dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize);

                dstVarFd = -activeVarFd;
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        -dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES)
                );
                dstVarSize = dstVarOffset + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                allocateDiskSpace(ff, -dstVarFd, dstVarSize);
                dstVarAddr = mapRW(ff, -dstVarFd, dstVarSize);
                break;
            default:
                int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                long oooSize = dstLen << shl;
                dstFixOffset = srcDataMax << shl;
                dstFixFd = -activeFixFd;
                dstFixSize = oooSize + dstFixOffset;
                allocateDiskSpace(ff, -dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize);
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openRW(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openRW(ff, path);
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
        long srcDataFixOffset;
        long srcDataFixSize;
        long srcDataVarFd = 0;
        long srcDataVarAddr = 0;
        long srcDataVarOffset = 0;
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
        final int pDirNameLen;
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                srcDataFixFd = -activeFixFd;
                srcDataVarFd = -activeVarFd;

                appendTxnToPath(path.trimTo(plen), txn);
                pDirNameLen = path.length();

                if (activeTop > 0) {
                    final long srcDataActualBytes = (srcDataMax - activeTop) * Long.BYTES;
                    final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                    if (activeTop > prefixHi || prefixType == OO_BLOCK_OO) {
                        // extend the existing column down, we will be discarding it anyway
                        srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                        srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);

                        srcDataVarSize = getVarColumnSize(
                                ff,
                                columnType,
                                -srcDataVarFd,
                                Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataActualBytes - Long.BYTES)
                        );

                        // at bottom of source var column set length of strings to null (-1) for as many strings
                        // as activeTop value.
                        if (columnType == ColumnType.STRING) {
                            srcDataVarOffset = srcDataVarSize;
                            srcDataVarSize += activeTop * Integer.BYTES + srcDataVarSize;
                            srcDataVarAddr = mapRW(ff, -srcDataVarFd, srcDataVarSize);
                            Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, activeTop);

                            // now set the "empty" bit of fixed size column with references to those
                            // null strings we just added
                            Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes, 0, activeTop);
                            // we need to shift copy the original column so that new block points at strings "below" the
                            // nulls we created above
                            TableUtils.shiftCopyFixedSizeColumnData(-activeTop * Integer.BYTES, srcDataFixAddr, 0, srcDataMax - activeTop, srcDataFixAddr + srcDataMaxBytes);
                            Unsafe.getUnsafe().copyMemory(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + activeTop * Integer.BYTES, srcDataVarOffset);
                        }
                        activeTop = 0;
                        srcDataFixOffset = -srcDataActualBytes;
                    } else {
                        // when we are shuffling "empty" space we can just reduce column top instead
                        // of moving data
                        writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, activeTop);
                        srcDataFixSize = srcDataActualBytes;
                        srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);
                        srcDataFixOffset = 0;

                        srcDataVarSize = getVarColumnSize(
                                ff,
                                columnType,
                                -srcDataVarFd,
                                Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - srcDataFixOffset - Long.BYTES)
                        );
                        srcDataVarAddr = mapRO(ff, -srcDataVarFd, srcDataVarSize);
                    }
                } else {
                    srcDataFixSize = srcDataMax * Long.BYTES;
                    srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;

                    srcDataVarSize = getVarColumnSize(
                            ff,
                            columnType,
                            -srcDataVarFd,
                            Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES)
                    );
                    srcDataVarAddr = mapRO(ff, -srcDataVarFd, srcDataVarSize);
                }


                path.trimTo(pDirNameLen).concat(columnName);
                int pColNameLen = path.length();
                path.put(FILE_SUFFIX_I).$();
                dstFixFd = openRW(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - activeTop) * Long.BYTES;
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

                path.trimTo(pColNameLen);
                path.put(FILE_SUFFIX_D).$();
                dstVarFd = openRW(ff, path);
                dstVarSize = srcDataVarSize - srcDataVarOffset + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                allocateDiskSpace(ff, dstVarFd, dstVarSize);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize);

                if (prefixType == OO_BLOCK_DATA) {
                    dstFixAppendOffset1 = (prefixHi - prefixLo + 1 - activeTop) * Long.BYTES;
                    prefixHi -= activeTop;
                } else {
                    dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;
                }

                if (suffixType == OO_BLOCK_DATA && activeTop > 0) {
                    suffixHi -= activeTop;
                    suffixLo -= activeTop;
                }

                // configure offsets
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                        partCount++;
                        break;
                    case OO_BLOCK_DATA:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr - srcDataFixOffset, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                        partCount++;
                        break;
                    default:
                        break;
                }

                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    long oooLen = getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr - srcDataFixOffset, srcDataFixSize, srcDataVarSize - srcDataVarOffset);
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                    dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                    dstVarAppendOffset2 = dstVarAppendOffset1;
                }


                break;
            default:
                srcDataFixFd = -activeFixFd;
                final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                appendTxnToPath(path.trimTo(plen), txn);
                pDirNameLen = path.length();

                if (activeTop > 0) {
                    final long srcDataActualBytes = (srcDataMax - activeTop) << shl;
                    final long srcDataMaxBytes = srcDataMax << shl;
                    if (activeTop > prefixHi || prefixType == OO_BLOCK_OO) {
                        // extend the existing column down, we will be discarding it anyway
                        srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                        srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);
                        setNull(columnType, srcDataFixAddr + srcDataActualBytes, activeTop);
                        Unsafe.getUnsafe().copyMemory(srcDataFixAddr, srcDataFixAddr + srcDataMaxBytes, srcDataActualBytes);
                        activeTop = 0;
                        srcDataFixOffset = -srcDataActualBytes;
                    } else {
                        // when we are shuffling "empty" space we can just reduce column top instead
                        // of moving data
                        writeColumnTop(ff, path.trimTo(pDirNameLen), columnName, activeTop);
                        srcDataFixSize = srcDataActualBytes;
                        srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);
                        srcDataFixOffset = 0;
                    }
                } else {
                    srcDataFixSize = srcDataMax << shl;
                    srcDataFixAddr = mapRW(ff, -srcDataFixFd, srcDataFixSize);
                    srcDataFixOffset = 0;
                }

                path.trimTo(pDirNameLen).concat(columnName).put(FILE_SUFFIX_D).$();
                dstFixFd = openRW(ff, path);
                dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - activeTop) << shl;
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

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
                    dstKFd = openRW(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openRW(ff, path);
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
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixOffset,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarOffset,
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

    private static void setNull(int columnType, long addr, long count) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                // todo: implement
                break;
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

    private static void writeColumnTop(FilesFacade ff, Path path, CharSequence columnName, long columnTop) {
        topFile(path, columnName);
        final long fd = openRW(ff, path);
        allocateDiskSpace(ff, fd, Long.BYTES);
        long addr = mapRW(ff, fd, Long.BYTES);
        //noinspection SuspiciousNameCombination
        Unsafe.getUnsafe().putLong(addr, columnTop);
        ff.munmap(addr, Long.BYTES);
        ff.close(fd);
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
                srcDataFixFd = openRW(ff, path);
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = mapRO(ff, srcDataFixFd, srcDataFixSize);

                dFile(path.trimTo(plen), columnName);
                srcDataVarFd = openRW(ff, path);
                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcDataVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES)
                );
                srcDataVarAddr = mapRO(ff, srcDataVarFd, srcDataVarSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPath(path, columnName, FILE_SUFFIX_I);

                dstFixFd = openRW(ff, path);
                dstFixSize = dstLen * Long.BYTES;
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPath(path, columnName, FILE_SUFFIX_D);
                dstVarSize = srcDataVarSize + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                dstVarFd = openRW(ff, path);
                allocateDiskSpace(ff, dstVarFd, dstVarSize);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize);

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
                    srcDataFixFd = openRW(ff, path);
                }

                final int shl = ColumnType.pow2SizeOf(Math.abs(columnType));
                srcDataFixSize = srcDataMax << shl;
                dFile(path.trimTo(plen), columnName);
                srcDataFixAddr = mapRO(ff, Math.abs(srcDataFixFd), srcDataFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();

                dstFixFd = openRW(ff, path);
                dstFixSize = dstLen << shl;
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                }

                // we have "src" index
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    dstKFd = openRW(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openRW(ff, path);
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
                srcDataFixFd,
                srcDataFixAddr,
                0, //todo:
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                0, // todo:
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
                dstFixFd = openRW(ff, path);
                allocateDiskSpace(ff, dstFixFd, (srcOooHi - srcOooLo + 1) * Long.BYTES);
                dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES;
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);

                oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstVarFd = openRW(ff, path);
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                allocateDiskSpace(ff, dstVarFd, dstVarSize);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize);
                break;
            default:
                oooSetPath(path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstFixFd = openRW(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                allocateDiskSpace(ff, dstFixFd, dstFixSize);
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize);
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openRW(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openRW(ff, path);
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
