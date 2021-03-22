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
import io.questdb.tasks.OutOfOrderUpdPartitionSizeTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderCopyJob extends AbstractQueueConsumerJob<OutOfOrderCopyTask> {
    private static final Log LOG = LogFactory.getLog(OutOfOrderCopyJob.class);
    private final CairoConfiguration configuration;
    private final RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue;
    private final MPSequence updPartitionSizePubSeq;

    public OutOfOrderCopyJob(MessageBus messageBus) {
        super(messageBus.getOutOfOrderCopyQueue(), messageBus.getOutOfOrderCopySubSequence());
        this.configuration = messageBus.getConfiguration();
        this.updPartitionSizeQueue = messageBus.getOutOfOrderUpdPartitionSizeQueue();
        this.updPartitionSizePubSeq = messageBus.getOutOfOrderUpdPartitionSizePubSequence();
    }

    public static void copy(
            CairoConfiguration configuration,
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
            long srcDataTop,
            long srcDataMax,
            long srcDataTxn,
            long txn,
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
            long partitionTimestamp, // <-- this is used to determine if partition is last or not as well as partition dir
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
        switch (blockType) {
            case OO_BLOCK_MERGE:
                if (srcDataFixOffset == 0 && srcDataTop == 0) {
                    oooMergeCopy(
                            columnType,
                            timestampMergeIndexAddr,
                            srcDataFixAddr,
                            srcDataVarAddr,
                            srcDataLo,
                            srcDataHi,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            srcOooLo,
                            srcOooHi,
                            dstFixAddr + dstFixOffset,
                            dstVarAddr,
                            dstVarOffset
                    );
                } else {
                    oooMergeCopyWithOffset(
                            columnType,
                            timestampMergeIndexAddr,
                            srcDataFixAddr,
                            // this is a hack, when we have column top we can have only of of the two:
                            // srcDataFixOffset, when we had to shift data to back fill nulls or
                            // srcDataTopOffset - if we kept the column top
                            // when one value is present the other will be 0
                            srcDataFixOffset - srcDataTop,
                            srcDataVarAddr,
                            srcDataVarOffset,
                            srcDataLo,
                            srcDataHi,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            srcOooLo,
                            srcOooHi,
                            dstFixAddr + dstFixOffset,
                            dstVarAddr,
                            dstVarOffset
                    );
                }
                break;
            case OO_BLOCK_OO:
                oooCopyOOO(
                        columnType,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr + dstFixOffset,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case OO_BLOCK_DATA:
                oooCopyData(
                        columnType,
                        srcDataFixAddr + srcDataFixOffset,
                        srcDataFixSize - srcDataFixOffset,
                        srcDataVarAddr + srcDataVarOffset,
                        srcDataVarSize - srcDataVarOffset,
                        srcDataLo,
                        srcDataHi,
                        dstFixAddr + dstFixOffset,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            default:
                break;
        }
        // decrement part counter and if we are the last task - perform final steps
        if (partCounter.decrementAndGet() == 0) {
            // todo: pool indexer
            if (isIndexed) {
                // dstKFd & dstVFd are closed by the indexer
                try {
                    updateIndex(configuration, dstFixAddr, dstFixSize, dstKFd, dstVFd, dstIndexOffset);
                } catch (Throwable e) {
                    tableWriter.bumpOooErrorCount();
                    copyIdleQuick(
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
                            dstKFd,
                            dstVFd,
                            tableWriter,
                            doneLatch
                    );
                    throw e;
                }
            }

            // unmap memory
            OutOfOrderUtils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            OutOfOrderUtils.unmapAndClose(ff, srcDataVarFd, srcDataVarAddr, srcDataVarSize);
            OutOfOrderUtils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            OutOfOrderUtils.unmapAndClose(ff, dstVarFd, dstVarAddr, dstVarSize);

            final int columnsRemaining = columnCounter.decrementAndGet();
            LOG.debug().$("organic [columnsRemaining=").$(columnsRemaining).$(']').$();
            if (columnsRemaining == 0) {
                OutOfOrderUtils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                try {
                    touchPartition(
                            ff,
                            updPartitionSizeTaskQueue,
                            updPartitionSizePubSeq,
                            pathToTable,
                            srcOooPartitionLo,
                            srcOooPartitionHi,
                            timestampMin,
                            timestampMax,
                            partitionTimestamp,
                            srcOooMax,
                            srcDataMax,
                            srcDataTxn,
                            txn,
                            srcTimestampFd,
                            partitionMutates,
                            tableWriter
                    );
                } finally {
                    if (timestampMergeIndexAddr != 0) {
                        Vect.freeMergedIndex(timestampMergeIndexAddr);
                    }
                    doneLatch.countDown();
                }
            }
        }
    }

    public static void copy(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionSizePubSeq,
            OutOfOrderCopyTask task,
            long cursor,
            Sequence subSeq
    ) {
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final FilesFacade ff = task.getFf();
        final CharSequence pathToTable = task.getPathToTable();
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
        final long srcDataTxn = task.getSrcDataTxn();
        final long txn = task.getTxn();
        final long srcDataHi = task.getSrcDataHi();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooFixSize = task.getSrcOooFixSize();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooVarSize = task.getSrcOooVarSize();
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
        final long dstVarSize = task.getDstVarSize();
        final long dstKFd = task.getDstKFd();
        final long dskVFd = task.getDstVFd();
        final long dstIndexOffset = task.getDstIndexOffset();
        final boolean isIndexed = task.isIndexed();
        final long srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final boolean partitionMutates = task.isPartitionMutates();
        final TableWriter tableWriter = task.getTableWriter();
        final SOUnboundedCountDownLatch doneLatch = task.getDoneLatch();

        subSeq.done(cursor);

        copy(
                configuration,
                updPartitionSizeQueue,
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
                srcDataTop,
                srcDataMax,
                srcDataTxn,
                txn,
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
                dstVarSize,
                dstKFd,
                dskVFd,
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

    static void copyIdle(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
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
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        if (partCounter.decrementAndGet() == 0) {
            // unmap memory
            copyIdleQuick(
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
                    dstKFd,
                    dstVFd,
                    tableWriter,
                    doneLatch
            );
        }
    }

    static void copyIdleQuick(
            AtomicInteger columnCounter,
            FilesFacade ff,
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
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        OutOfOrderUtils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
        OutOfOrderUtils.unmapAndClose(ff, srcDataVarFd, srcDataVarAddr, srcDataVarSize);
        OutOfOrderUtils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
        OutOfOrderUtils.unmapAndClose(ff, dstVarFd, dstVarAddr, dstVarSize);
        OutOfOrderUtils.close(ff, dstKFd);
        OutOfOrderUtils.close(ff, dstVFd);

        closeColumnIdle(
                columnCounter,
                ff,
                timestampMergeIndexAddr,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                tableWriter,
                doneLatch
        );
    }

    static void closeColumnIdle(
            AtomicInteger columnCounter,
            FilesFacade ff,
            long timestampMergeIndexAddr,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        final int columnsRemaining = columnCounter.decrementAndGet();
        LOG.debug().$("idle [columnsRemaining=").$(columnsRemaining).$(']').$();
        if (columnsRemaining == 0) {
            closeColumnIdleQuick(
                    ff,
                    timestampMergeIndexAddr,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    doneLatch
            );
        }
    }

    static void closeColumnIdleQuick(
            FilesFacade ff,
            long timestampMergeIndexAddr,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        OutOfOrderUtils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        OutOfOrderUtils.close(ff, srcTimestampFd);
        if (timestampMergeIndexAddr != 0) {
            Vect.freeMergedIndex(timestampMergeIndexAddr);
        }
        tableWriter.bumpPartitionUpdateCount();
        doneLatch.countDown();
    }

    private static void touchPartition(
            FilesFacade ff,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq,
            CharSequence pathToTable,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp, // lowest timestamp of partition where data is headed
            long srcOooMax,
            long srcDataMax,
            long srcDataTxn,
            long txn,
            long srcTimestampFd,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        try {
            if (partitionMutates) {
                if (srcTimestampFd < 0) {
                    // srcTimestampFd negative indicates that we are reusing existing file descriptor
                    // as opposed to opening file by name. This also indicated that "this" partition
                    // is, or used to be, active for the writer. So we have to close existing files so thatT
                    // rename on Windows does not fall flat.
                    tableWriter.closeActivePartition();
                } else {
                    // this timestamp column was opened by file name
                    // so we can close it as not needed (and to enable table rename on Windows)
                    ff.close(srcTimestampFd);
                }

                if (false && tableWriter.getOooErrorCount() == 0
                        && tableWriter.acquireWriterLock(partitionTimestamp, srcDataTxn)
                ) {
                    LOG.info()
                            .$("lock successful [table=`").utf8(pathToTable)
                            .$("`, ts=").$ts(partitionTimestamp)
                            .$(", txn=").$(srcDataTxn).$(']').$();

                    try {
                        swapPartition(
                                ff,
                                pathToTable,
                                partitionTimestamp,
                                srcDataTxn,
                                txn,
                                tableWriter
                        );
                        partitionMutates = false;
                    } finally {
                        tableWriter.releaseWriterLock(partitionTimestamp, srcDataTxn);
                    }
                } else {
                    LOG.info()
                            .$("partition busy [table=`").utf8(pathToTable)
                            .$("`, ts=").$ts(partitionTimestamp)
                            .$(", txn=").$(srcDataTxn).$(']').$();
                }
            } else if (srcTimestampFd > 0) {
                ff.close(srcTimestampFd);
            }
        } catch (Throwable e) {
            tableWriter.bumpOooErrorCount();
            throw e;
        } finally {
            notifyWriter(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
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

    static void notifyWriter(
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq,
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
        final long cursor = updPartitionPubSeq.next();
        if (cursor > -1) {
            publishUpdPartitionSizeTaskHarmonized(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    partitionTimestamp,
                    srcDataMax,
                    partitionMutates
            );
        } else {
            publishUpdPartitionSizeTaskContended(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
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
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq,
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
            cursor = updPartitionPubSeq.next();
        }

        if (cursor > -1) {
            publishUpdPartitionSizeTaskHarmonized(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    partitionTimestamp,
                    srcDataMax,
                    partitionMutates
            );
        } else {
            tableWriter.oooUpdatePartitionSizeSynchronized(
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
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq, long cursor,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long partitionTimestamp,
            long srcDataMax,
            boolean partitionMutates
    ) {
        final OutOfOrderUpdPartitionSizeTask task = updPartitionSizeQueue.get(cursor);
        task.of(
                partitionTimestamp,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcDataMax,
                partitionMutates
        );
        updPartitionPubSeq.done(cursor);
    }

    private static void swapPartition(
            FilesFacade ff,
            CharSequence pathToTable,
            long partitionTimestamp,
            long srcDataTxn,
            long txn,
            TableWriter tableWriter
    ) {
        if (Os.type == Os.WINDOWS) {
            swapPartitionFiles(ff, pathToTable, partitionTimestamp, srcDataTxn, txn, tableWriter);
        } else {
            swapPartitionDirectories(ff, pathToTable, partitionTimestamp, tableWriter);
        }
    }

    private static void swapPartitionDirectories(FilesFacade ff, CharSequence pathToTable, long partitionTimestamp, TableWriter tableWriter) {
        final long txn = tableWriter.getTxn();
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp);
        final int plen = path.length();
        path.$();
        final Path other = Path.getThreadLocal2(path);
        TableUtils.oldPartitionName(other, txn);
        if (ff.rename(path, other.$())) {
            TableUtils.txnPartition(other.trimTo(plen), txn);
            if (ff.rename(other.$(), path)) {
                LOG.info().$("renamed").$();
                return;
            }
            throw CairoException.instance(ff.errno())
                    .put("could not rename [from=").put(other)
                    .put(", to=").put(path).put(']');
        } else {
            throw CairoException.instance(ff.errno())
                    .put("could not rename [from=").put(path)
                    .put(", to=").put(other).put(']');
        }
    }

    private static void swapPartitionFiles(
            FilesFacade ff,
            CharSequence pathToTable,
            long partitionTimestamp,
            long srcDataTxn,
            long txn,
            TableWriter tableWriter
    ) {
        // source path - original data partition
        // this partition may already be on non-initial txn
        // todo: test that this code handles data txn > -1
        final Path srcPath = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(srcPath, tableWriter.getPartitionBy(), partitionTimestamp);
        int coreLen = srcPath.length();

        TableUtils.txnPartitionConditionally(srcPath, srcDataTxn);
        int srcLen = srcPath.length();

        final Path dstPath = Path.getThreadLocal2(srcPath).concat("backup");
        TableUtils.txnPartitionConditionally(dstPath, txn);
        int dstLen = dstPath.length();

        srcPath.put(Files.SEPARATOR);
        srcPath.$();

        dstPath.put(Files.SEPARATOR);
        dstPath.$();

        if (ff.mkdir(dstPath, 502) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directory [path").put(dstPath).put(']');
        }

        // move all files to "backup-txn"
        moveFiles(ff, srcPath, srcLen, dstPath, dstLen);

        // not move files from "XXX-n-txn" to original partition
        srcPath.trimTo(coreLen);
        TableUtils.txnPartition(srcPath, txn);
        srcLen = srcPath.length();

        dstPath.trimTo(coreLen);
        TableUtils.txnPartitionConditionally(dstPath, srcDataTxn);
        dstLen = dstPath.length();

        moveFiles(ff, srcPath.put(Files.SEPARATOR).$(), srcLen, dstPath.$(), dstLen);
    }

    private static void moveFiles(FilesFacade ff, Path srcPath, int srcLen, Path dstPath, int dstLen) {
        long p = ff.findFirst(srcPath);
        if (p > 0) {
            try {
                do {
                    int type  = ff.findType(p);
                    if (type == Files.DT_REG) {
                        long lpszName = ff.findName(p);
                        srcPath.trimTo(srcLen).concat(lpszName).$();
                        dstPath.trimTo(dstLen).concat(lpszName).$();
                        if (ff.rename(srcPath, dstPath)) {
                            LOG.debug().$("renamed [from=").$(srcPath).$(", to=").$(dstPath, dstLen, dstPath.length()).$(']').$();
                        } else {
                            throw CairoException.instance(ff.errno()).put("could not rename file [from=").put(srcPath).put(", to=").put(dstPath).put(']');
                        }
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }
    }

    private static void oooCopyData(
            int columnType,
            long srcFixAddr,
            long srcFixSize,
            long srcVarAddr,
            long srcVarSize,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                oooCopyVarSizeCol(
                        srcFixAddr,
                        srcFixSize,
                        srcVarAddr,
                        srcVarSize,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            default:
                oooCopyFixedSizeCol(
                        srcFixAddr,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        ColumnType.pow2SizeOf(Math.abs(columnType))
                );
                break;
        }
    }

    private static void oooCopyFixedSizeCol(long src, long srcLo, long srcHi, long dst, final int shl) {
        Vect.memcpy(src + (srcLo << shl), dst, (srcHi - srcLo + 1) << shl);
    }

    private static void oooCopyOOO(
            int columnType,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                // we can find out the edge of string column in one of two ways
                // 1. if srcOooHi is at the limit of the page - we need to copy the whole page of strings
                // 2  if there are more items behind srcOooHi we can get offset of srcOooHi+1
                oooCopyVarSizeCol(
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 1);
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 2);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
            case ColumnType.TIMESTAMP:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, 3);
                break;
            case -ColumnType.TIMESTAMP:
                OutOfOrderUtils.copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr);
                break;
            default:
                break;
        }
    }

    private static void oooCopyVarSizeCol(
            long srcFixAddr,
            long srcFixSize,
            long srcVarAddr,
            long srcVarSize,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset

    ) {
        final long lo = OutOfOrderUtils.findVarOffset(srcFixAddr, srcLo, srcHi, srcVarSize);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = OutOfOrderUtils.findVarOffset(srcFixAddr, srcHi + 1, srcFixSize / Long.BYTES, srcVarSize);
        }
        // copy this before it changes
        final long dest = dstVarAddr + dstVarOffset;
        final long len = hi - lo;
        Vect.memcpy(srcVarAddr + lo, dest, len);
        if (lo == dstVarOffset) {
            oooCopyFixedSizeCol(srcFixAddr, srcLo, srcHi, dstFixAddr, 3);
        } else {
            OutOfOrderUtils.shiftCopyFixedSizeColumnData(lo - dstVarOffset, srcFixAddr, srcLo, srcHi, dstFixAddr);
        }
    }

    private static void oooMergeCopy(
            int columnType,
            long mergeIndexAddr,
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
            long dstVarOffset
    ) {
        final long rowCount = srcOooHi - srcOooLo + 1 + srcDataHi - srcDataLo + 1;
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Vect.mergeShuffle8Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, mergeIndexAddr, rowCount);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                Vect.mergeShuffle16Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, mergeIndexAddr, rowCount);
                break;
            case ColumnType.STRING:
                Vect.oooMergeCopyStrColumn(
                        mergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.BINARY:
                Vect.oooMergeCopyBinColumn(
                        mergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                Vect.mergeShuffle32Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, mergeIndexAddr, rowCount);
                break;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                Vect.mergeShuffle64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, mergeIndexAddr, rowCount);
                break;
            case -ColumnType.TIMESTAMP:
                Vect.oooCopyIndex(mergeIndexAddr, rowCount, dstFixAddr);
                break;
            default:
                break;
        }
    }

    private static void oooMergeCopyWithOffset(
            int columnType,
            long timestampMergeIndexAddr,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataLo,
            long srcDataHi,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        final long rowCount = srcOooHi - srcOooLo + 1 + srcDataHi - srcDataLo + 1;
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Vect.mergeShuffleWithTop8Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount, srcDataFixOffset);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                Vect.mergeShuffleWithTop16Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount, srcDataFixOffset);
                break;
            case ColumnType.STRING:
                Vect.oooMergeCopyStrColumnWithTop(
                        timestampMergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataFixOffset,
                        srcDataVarAddr + srcDataVarOffset,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.BINARY:
                Vect.oooMergeCopyBinColumnWithTop(
                        timestampMergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataFixOffset,
                        srcDataVarAddr + srcDataVarOffset,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                Vect.mergeShuffleWithTop32Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount, srcDataFixOffset);
                break;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                Vect.mergeShuffleWithTop64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount, srcDataFixOffset);
                break;
            default:
                break;
        }
    }

    private static void updateIndex(
            CairoConfiguration configuration,
            long dstFixAddr,
            long dstFixSize,
            long dstKFd,
            long dskVFd,
            long dstIndexOffset
    ) {
        try (BitmapIndexWriter w = new BitmapIndexWriter()) {
            long row = dstIndexOffset / Integer.BYTES;
            w.of(configuration, dstKFd, dskVFd, row == 0);
            w.rollbackConditionally(row);
            final long count = dstFixSize / Integer.BYTES;
            for (; row < count; row++) {
                w.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(dstFixAddr + row * Integer.BYTES)), row);
            }
            w.setMaxValue(count - 1);
        }
    }

    private void copy(OutOfOrderCopyTask task, long cursor, Sequence subSeq) {
        copy(
                configuration,
                updPartitionSizeQueue,
                updPartitionSizePubSeq,
                task,
                cursor,
                subSeq
        );
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        copy(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
