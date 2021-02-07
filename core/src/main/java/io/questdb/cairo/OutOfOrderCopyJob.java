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
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long dataTimestampHi,
            long tableFloorOfMaxTimestamp,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long srcOooMax,
            long oooTimestampMin,
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
            long timestampFd,
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
                        dstFixAddr,
                        dstFixOffset,
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
                        dstFixAddr,
                        dstFixOffset,
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
                        dstFixAddr,
                        dstFixOffset,
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
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampHi,
                        srcDataMax,
                        tableFloorOfMaxTimestamp,
                        dataTimestampHi,
                        timestampMergeIndexAddr,
                        timestampFd,
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

    private static void touchPartition(
            FilesFacade ff,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionPubSeq,
            CharSequence pathToTable,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long srcOooMax,
            long oooTimestampMin, // absolute minimum of OOO timestamp
            long oooTimestampHi, // local (partition bound) maximum of OOO timestamp
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long mergeIndexAddr,
            long timestampFd,
            boolean partitionMutates,
            TableWriter tableWriter
    ) {
        if (partitionMutates) {
            if (timestampFd < 0) {
                // timestampFd negative indicates that we are reusing existing file descriptor
                // as opposed to opening file by name. This also indicated that "this" partition
                // is, or used to be, active for the writer. So we have to close existing files so thatT
                // rename on Windows does not fall flat.
                tableWriter.closeActivePartition();
            } else {
                // this timestamp column was opened by file name
                // so we can close it as not needed (and to enable table rename on Windows)
                ff.close(timestampFd);
            }

            final Path path = Path.getThreadLocal(pathToTable);
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), oooTimestampHi);
            final int plen = path.length();

            path.$();
            final Path other = Path.getThreadLocal2(path).put("x-").put(tableWriter.getTxn()).$();
            boolean renamed;
            if (renamed = ff.rename(path, other)) {
                OutOfOrderUtils.appendTxnToPath(other.trimTo(plen), tableWriter.getTxn());
                renamed = ff.rename(other.$(), path);
            }

            if (!renamed) {
                throw CairoException.instance(ff.errno())
                        .put("could not rename [from=").put(other)
                        .put(", to=").put(path).put(']');
            }
            LOG.info().$("will rename").$();

/*
            // rename after we closed all FDs associated with source partition
            path.trimTo(plen).$();
            other.of(path).put("x-").put(txn).$();
            if (ff.rename(path, other)) {
                appendTxnToPath(other.trimTo(plen));
                if (!ff.rename(other.$(), path)) {
                    throw CairoException.instance(ff.errno())
                            .put("could not rename [from=").put(other)
                            .put(", to=").put(path).put(']');
                }
            } else {
                // todo: we could not move old partition, which means
                //    we have to rollback all partitions that we moved in this
                //    transaction
                throw CairoException.instance(ff.errno())
                        .put("could not rename [from=").put(path)
                        .put(", to=").put(other).put(']');
            }
*/
        } else if (timestampFd > 0) {
            ff.close(timestampFd);
        }

        final long cursor = updPartitionPubSeq.nextBully();
        OutOfOrderUpdPartitionSizeTask task = updPartitionSizeQueue.get(cursor);
        task.of(
                oooTimestampHi,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcDataMax,
                dataTimestampHi
        );
        updPartitionPubSeq.done(cursor);

        final long partitionSize = srcDataMax + srcOooPartitionHi - srcOooPartitionLo + 1;
        if (srcOooPartitionHi + 1 < srcOooMax || oooTimestampHi < tableFloorOfMaxTimestamp) {
/*

            this.fixedRowCount += partitionSize;
            if (oooTimestampHi < tableFloorOfMaxTimestamp) {
                this.fixedRowCount -= srcDataMax;
            }

            // We just updated non-last partition. It is possible that this partition
            // has already been updated by transaction or out of order logic was the only
            // code that updated it. To resolve this fork we need to check if "txPendingPartitionSizes"
            // contains timestamp of this partition. If it does - we don't increment "txPartitionCount"
            // (it has been incremented before out-of-order logic kicked in) and
            // we use partition size from "txPendingPartitionSizes" to subtract from "txPartitionCount"

            boolean missing = true;
            long x = this.txPendingPartitionSizes.getAppendOffset() / 16;
            for (long l = 0; l < x; l++) {
                long ts = this.txPendingPartitionSizes.getLong(l * 16 + Long.BYTES);
                if (ts == oooTimestampHi) {
                    this.fixedRowCount -= this.txPendingPartitionSizes.getLong(l * 16);
                    this.txPendingPartitionSizes.putLong(l * 16, partitionSize);
                    missing = false;
                    break;
                }
            }

            if (missing) {
                this.txPartitionCount++;
                this.txPendingPartitionSizes.putLong128(partitionSize, oooTimestampHi);
            }
*/

            if (srcOooPartitionHi + 1 >= srcOooMax) {
                LOG.info().$("strange one").$();
/*
                // no more out of order data and we just pre-pended data to existing
                // partitions
                this.minTimestamp = oooTimestampMin;
                // when we exit here we need to rollback transientRowCount we've been incrementing
                // while adding out-of-order data
                this.transientRowCount = this.transientRowCountBeforeOutOfOrder;
*/
//                tableWriter.updateActivePartitionDetails(
//                        oooTimestampMin,
//                        Math.max(dataTimestampHi, oooTimestampHi),
//                        partitionSize
//                );
            }
        } else {
            // this was taking max between existing timestamp and ooo
//            tableWriter.updateActivePartitionDetails(
//                    oooTimestampMin,
//                    Math.max(dataTimestampHi, oooTimestampHi),
//                    partitionSize
//            );
        }
    }

    private static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstOffset
    ) {
        final int shl = 4;
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long start = src + lo;
        final long dest = dstAddr + dstOffset;
        final long len = hi - lo;
        for (long l = 0; l < len; l += 16) {
            Unsafe.getUnsafe().putLong(dest + l / 2, Unsafe.getUnsafe().getLong(start + l));
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
            long dstFixOffset,
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
                        dstFixOffset,
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
                        dstFixOffset,
                        ColumnType.pow2SizeOf(Math.abs(columnType))
                );
                break;
        }
    }

    private static void oooCopyFixedSizeCol(long src, long srcLo, long srcHi, long dst, long dstOffset, final int shl) {
        final long len = (srcHi - srcLo + 1) << shl;
        if (len < 0) {
            System.out.println("ok");
        }
        Unsafe.getUnsafe().copyMemory(src + (srcLo << shl), dst + dstOffset, len);
    }

    private static void oooCopyIndex(long mergeIndexAddr, long mergeIndexSize, long dstAddr, long dstOffset) {
        final long dst = dstAddr + dstOffset;
        for (long l = 0; l < mergeIndexSize; l++) {
            Unsafe.getUnsafe().putLong(dst + l * Long.BYTES, getTimestampIndexValue(mergeIndexAddr, l));
        }
    }

    private static void oooCopyOOO(
            int columnType, long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo, long srcOooHi, long dstFixAddr, long dstFixOffset,
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
                        dstFixOffset,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, dstFixOffset, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, dstFixOffset, 1);
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, dstFixOffset, 2);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
            case ColumnType.TIMESTAMP:
                oooCopyFixedSizeCol(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, dstFixOffset, 3);
                break;
            case -ColumnType.TIMESTAMP:
                copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr, dstFixOffset);
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
            long dstFixOffset,
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
        Unsafe.getUnsafe().copyMemory(srcVarAddr + lo, dest, len);
        if (lo == dstVarOffset) {
            oooCopyFixedSizeCol(srcFixAddr, srcLo, srcHi, dstFixAddr, dstFixOffset, 3);
        } else {
            shiftCopyFixedSizeColumnData(lo - dstVarOffset, srcFixAddr, srcLo, srcHi, dstFixAddr, dstFixOffset);
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
            long dstFixOffset,
            long dstVarAddr,
            long dstVarOffset
    ) {
        final long rowCount = srcOooHi - srcOooLo + 1 + srcDataHi - srcDataLo + 1;
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Vect.mergeShuffle8Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr + dstFixOffset, mergeIndexAddr, rowCount);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                Vect.mergeShuffle16Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr + dstFixOffset, mergeIndexAddr, rowCount);
                break;
            case ColumnType.STRING:
                oooMergeCopyStrColumn(
                        mergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstFixOffset,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.BINARY:
                oooMergeCopyBinColumn(
                        mergeIndexAddr,
                        rowCount,
                        srcDataFixAddr,
                        srcDataVarAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        dstFixAddr,
                        dstFixOffset,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                Vect.mergeShuffle32Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr + dstFixOffset, mergeIndexAddr, rowCount);
                break;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                Vect.mergeShuffle64Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr + dstFixOffset, mergeIndexAddr, rowCount);
                break;
            case -ColumnType.TIMESTAMP:
                oooCopyIndex(mergeIndexAddr, rowCount, dstFixAddr, dstFixOffset);
                break;
            default:
                break;
        }
    }

    private static void oooMergeCopyBinColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstFixOffset,
            long dstVarAddr,
            long dstVarOffset
    ) {
        // destination of variable length data
        long destVarOffset = dstVarOffset;
        final long dstFix = dstFixAddr + dstFixOffset;

        // reverse order
        // todo: cache?
        long[] srcFix = new long[]{srcOooFixAddr, srcDataFixAddr};
        long[] srcVar = new long[]{srcOooVarAddr, srcDataVarAddr};

        for (long l = 0; l < mergeIndexSize; l++) {
            final long row = getTimestampIndexRow(mergeIndexAddr, l);
            // high bit in the index in the source array [0,1]
            final int bit = (int) (row >>> 63);
            // row number is "row" with high bit removed
            final long rr = row & ~(1L << 63);
            Unsafe.getUnsafe().putLong(dstFix + l * Long.BYTES, destVarOffset);
            long offset = Unsafe.getUnsafe().getLong(srcFix[bit] + rr * Long.BYTES);
            long addr = srcVar[bit] + offset;
            long len = Unsafe.getUnsafe().getLong(addr);
            if (len > 0) {
                Unsafe.getUnsafe().copyMemory(addr, dstVarAddr + destVarOffset, len + Long.BYTES);
                destVarOffset += len + Long.BYTES;
            } else {
                Unsafe.getUnsafe().putLong(dstVarAddr + destVarOffset, len);
                destVarOffset += Long.BYTES;
            }
        }
    }

    private static void oooMergeCopyStrColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstFixOffset,
            long dstVarAddr,
            long dstVarOffset
    ) {
        // destination of variable length data
        long destVarOffset = dstVarOffset;
        final long dstFix = dstFixAddr + dstFixOffset;

        // reverse order
        // todo: cache?
        long[] srcFix = new long[]{srcOooFixAddr, srcDataFixAddr};
        long[] srcVar = new long[]{srcOooVarAddr, srcDataVarAddr};

        for (long l = 0; l < mergeIndexSize; l++) {
            final long row = getTimestampIndexRow(mergeIndexAddr, l);
            // high bit in the index in the source array [0,1]
            final int bit = (int) (row >>> 63);
            // row number is "row" with high bit removed
            final long rr = row & ~(1L << 63);
            Unsafe.getUnsafe().putLong(dstFix + l * Long.BYTES, destVarOffset);
            long offset = Unsafe.getUnsafe().getLong(srcFix[bit] + rr * Long.BYTES);
            long addr = srcVar[bit] + offset;
            int len = Unsafe.getUnsafe().getInt(addr);
            Unsafe.getUnsafe().putInt(dstVarAddr + destVarOffset, len);
            len = Math.max(0, len);
            Unsafe.getUnsafe().copyMemory(addr + 4, dstVarAddr + destVarOffset + 4, (long) len * Character.BYTES);
            destVarOffset += (long) len * Character.BYTES + Integer.BYTES;
        }
    }

    private static void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr,
            long dstOffset
    ) {
        final int shl = ColumnType.pow2SizeOf(ColumnType.LONG);
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long slo = src + lo;
        final long dest = dstAddr + dstOffset;
        final long len = hi - lo;
        for (long o = 0; o < len; o += Long.BYTES) {
            Unsafe.getUnsafe().putLong(dest + o, Unsafe.getUnsafe().getLong(slo + o) - shift);
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
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final FilesFacade ff = task.getFf();
        final CharSequence pathToTable = task.getPathToTable();
        final int columnType = task.getColumnType();
        final int blockType = task.getBlockType();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final long srcDataFixFd = task.getSrcDataFixFd();
        final long srcDataFixAddr = task.getSrcDataFixAddr();
        final long srcDataFixSize = task.getSrcDataFixSize();
        final long srcDataVarFd = task.getSrcDataVarFd();
        final long srcDataVarAddr = task.getSrcDataVarAddr();
        final long srcDataVarSize = task.getSrcDataVarSize();
        final long srcDataLo = task.getSrcDataLo();
        final long srcDataMax = task.getSrcDataMax();
        final long srcDataHi = task.getSrcDataHi();
        final long dataTimestampHi = task.getDataTimestampHi();
        final long tableFloorOfMaxTimestamp = task.getTableFloorOfMaxTimestamp();
        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooFixSize = task.getSrcOooFixSize();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooVarSize = task.getSrcOooVarSize();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooPartitionLo = task.getSrcOooPartitionLo();
        final long srcOooPartitionHi = task.getSrcOooPartitionHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
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
        final long timestampFd = task.getTimestampFd();
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
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcDataLo,
                srcDataHi,
                srcDataMax,
                dataTimestampHi,
                tableFloorOfMaxTimestamp,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcOooMax,
                oooTimestampMin,
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
                timestampFd,
                partitionMutates,
                tableWriter,
                doneLatch
        );
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        OutOfOrderCopyTask task = queue.get(cursor);
        // copy task on stack so that publisher has fighting chance of
        // publishing all it has to the queue

//        final boolean locked = task.tryLock();
//        if (locked) {
        copy(task, cursor, subSeq);
//        } else {
//            subSeq.done(cursor);
//        }

        return true;
    }
}
