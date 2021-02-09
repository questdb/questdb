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
import io.questdb.std.Vect;
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
        switch (blockType) {
            case OO_BLOCK_MERGE:
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
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarAddr,
                        srcDataVarSize,
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
                updateIndex(configuration, dstFixAddr, dstFixSize, dstKFd, dstVFd, dstIndexOffset);
            }

            // unmap memory
            unmapAndClose(srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            unmapAndClose(srcDataVarFd, srcDataVarAddr, srcDataVarSize);
            unmapAndClose(dstFixFd, dstFixAddr, dstFixSize);
            unmapAndClose(dstVarFd, dstVarAddr, dstVarSize);
            if (columnCounter.decrementAndGet() == 0) {
                // last part of the last column
                touchPartition(
                        ff,
                        updPartitionSizeTaskQueue,
                        updPartitionSizePubSeq,
                        pathToTable,
                        srcOooPartitionLo,
                        srcOooPartitionHi,
                        oooTimestampMin,
                        oooTimestampMax,
                        oooTimestampHi,
                        srcOooMax,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        tableWriter
                );

                if (timestampMergeIndexAddr != 0) {
                    Vect.freeMergedIndex(timestampMergeIndexAddr);
                }
                doneLatch.countDown();
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
        final long srcDataTop = task.getSrcDataTop();
        final long srcDataFixFd = task.getSrcDataFixFd();
        final long srcDataFixAddr = task.getSrcDataFixAddr();
        final long srcDataFixSize = task.getSrcDataFixSize();
        final long srcDataVarFd = task.getSrcDataVarFd();
        final long srcDataVarAddr = task.getSrcDataVarAddr();
        final long srcDataVarSize = task.getSrcDataVarSize();
        final long srcDataLo = task.getSrcDataLo();
        final long srcDataMax = task.getSrcDataMax();
        final long srcDataHi = task.getSrcDataHi();
        final long tableFloorOfMaxTimestamp = task.getTableFloorOfMaxTimestamp();
        final long dataTimestampHi = task.getDataTimestampHi();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooFixSize = task.getSrcOooFixSize();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooVarSize = task.getSrcOooVarSize();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long srcOooPartitionLo = task.getSrcOooPartitionLo();
        final long srcOooPartitionHi = task.getSrcOooPartitionHi();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long oooTimestampMax = task.getOooTimestampMax();
        final long oooTimestampHi = task.getOooTimestampHi();
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

    private static void touchPartition(
            FilesFacade ff,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq,
            CharSequence pathToTable,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi, // local (partition bound) maximum of OOO timestamp
            long srcOooMax,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
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

            renamePartition(
                    ff,
                    pathToTable,
                    oooTimestampHi,
                    tableWriter
            );

        } else if (srcTimestampFd > 0) {
            ff.close(srcTimestampFd);
        }

        if (srcTimestampAddr != 0) {
            ff.munmap(srcTimestampAddr, srcTimestampSize);
        }

        final long cursor = updPartitionPubSeq.next();
        if (cursor > -1) {
            publishUpdPartitionSizeTaskHarmonized(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampHi,
                    srcDataMax,
                    dataTimestampHi
            );
        } else {
            publishUpdPartitionSizeTaskContended(
                    updPartitionSizeQueue,
                    updPartitionPubSeq,
                    cursor,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    srcOooMax,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
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
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcOooMax,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
                    oooTimestampHi,
                    srcDataMax,
                    dataTimestampHi
            );
        } else {
            tableWriter.oooUpdatePartitionSizeSynchronized(
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampHi,
                    srcOooPartitionLo,
                    srcOooPartitionHi,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
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
            long oooTimestampHi,
            long srcDataMax,
            long dataTimestampHi
    ) {
        final OutOfOrderUpdPartitionSizeTask task = updPartitionSizeQueue.get(cursor);
        task.of(
                oooTimestampHi,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcDataMax,
                dataTimestampHi
        );
        updPartitionPubSeq.done(cursor);
    }

    private static void renamePartition(FilesFacade ff, CharSequence pathToTable, long oooTimestampHi, TableWriter tableWriter) {
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), oooTimestampHi);
        final int plen = path.length();

        path.$();
        final Path other = Path.getThreadLocal2(path).put("x-").put(tableWriter.getTxn()).$();
        boolean renamed;
        if (renamed = ff.rename(path, other)) {
            TableUtils.appendTxnToPath(other.trimTo(plen), tableWriter.getTxn());
            renamed = ff.rename(other.$(), path);
        }

        if (!renamed) {
            throw CairoException.instance(ff.errno())
                    .put("could not rename [from=").put(other)
                    .put(", to=").put(path).put(']');
        }
    }

    private static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        final int shl = 4;
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long start = src + lo;
        final long len = hi - lo;
        for (long l = 0; l < len; l += 16) {
            Unsafe.getUnsafe().putLong(dstAddr + l / 2, Unsafe.getUnsafe().getLong(start + l));
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
        Unsafe.getUnsafe().copyMemory(src + (srcLo << shl), dst, (srcHi - srcLo + 1) << shl);
    }

    private static void oooCopyIndex(long mergeIndexAddr, long mergeIndexSize, long dstAddr) {
        for (long l = 0; l < mergeIndexSize; l++) {
            Unsafe.getUnsafe().putLong(dstAddr + l * Long.BYTES, getTimestampIndexValue(mergeIndexAddr, l));
        }
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
                copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr);
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
        final long lo = TableUtils.findVarOffset(srcFixAddr, srcLo, srcHi, srcVarSize);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = TableUtils.findVarOffset(srcFixAddr, srcHi + 1, srcFixSize / Long.BYTES, srcVarSize);
        }
        // copy this before it changes
        final long dest = dstVarAddr + dstVarOffset;
        final long len = hi - lo;
        Unsafe.getUnsafe().copyMemory(srcVarAddr + lo, dest, len);
        if (lo == dstVarOffset) {
            oooCopyFixedSizeCol(srcFixAddr, srcLo, srcHi, dstFixAddr, 3);
        } else {
            shiftCopyFixedSizeColumnData(lo - dstVarOffset, srcFixAddr, srcLo, srcHi, dstFixAddr);
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
                oooCopyIndex(mergeIndexAddr, rowCount, dstFixAddr);
                break;
            default:
                break;
        }
    }

    private static void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        final long lo = srcLo * Long.BYTES;
        final long hi = (srcHi + 1) * Long.BYTES;
        final long slo = src + lo;
        final long len = hi - lo;
        for (long o = 0; o < len; o += Long.BYTES) {
            Unsafe.getUnsafe().putLong(dstAddr + o, Unsafe.getUnsafe().getLong(slo + o) - shift);
        }
    }

    private static void unmapAndClose(long dstFixFd, long dstFixAddr, long dstFixSize) {
        if (dstFixAddr != 0 && dstFixSize != 0) {
            Files.munmap(dstFixAddr, dstFixSize);
        }

        if (dstFixFd > 0) {
            LOG.debug().$("closed [fd=").$(dstFixFd).$(']').$();
            Files.close(dstFixFd);
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
            final long count = dstFixSize / Integer.BYTES;
            for (; row < count; row++) {
                assert row * Integer.BYTES < dstFixSize;
                w.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(dstFixAddr + row * Integer.BYTES)), row);
            }
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
