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

import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.tasks.OutOfOrderCopyTask;

import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderCopyJob extends AbstractQueueConsumerJob<OutOfOrderCopyTask> {

    public OutOfOrderCopyJob(RingQueue<OutOfOrderCopyTask> queue, Sequence subSeq) {
        super(queue, subSeq);
    }

    private void copy(OutOfOrderCopyTask task, long cursor, Sequence subSeq) {
        final int blockType = task.getBlockType();
        final long srcDataFixAddr = task.getSrcDataFixAddr();
        final long srcDataFixSize = task.getSrcDataFixSize();
        final long srcDataVarAddr = task.getSrcDataVarAddr();
        final long srcDataVarSize = task.getSrcDataVarSize();
        final long srcDataLo = task.getSrcDataLo();
        final long srcDataHi = task.getSrcDataHi();

        final long srcOooFixAddr = task.getSrcOooFixAddr();
        final long srcOooFixSize = task.getSrcOooFixSize();
        final long srcOooVarAddr = task.getSrcOooVarAddr();
        final long srcOooVarSize = task.getSrcOooVarSize();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();


        final long dstFixAddr = task.getDstFixAddr();
        final long dstFixOffset = task.getDstFixOffset();
        final long dstVarAddr = task.getDstVarAddr();
        final long dstVarOffset = task.getDstVarOffset();
        final int columnType = task.getColumnType();
        final long mergeIndexAddr = task.getMergeIndexAddr();

        switch (blockType) {
            case OO_BLOCK_MERGE:
                oooMergeCopy(
                        columnType,
                        mergeIndexAddr,
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
            case OO_BLOCK_OO:
                oooCopyOOO(
                        columnType, srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        srcOooLo, srcOooHi, dstFixAddr, dstFixOffset,
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
    }

    private void copyFromTimestampIndex(
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

    @Override
    protected boolean doRun(int workerId, long cursor) {
        OutOfOrderCopyTask task = queue.get(cursor);
        // copy task on stack so that publisher has fighting chance of
        // publishing all it has to the queue

        final boolean locked = task.tryLock();
        if (locked) {
            copy(task, cursor, subSeq);
        } else {
            subSeq.done(cursor);
        }

        return true;
    }

    private void oooCopyData(
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
                        ColumnType.pow2SizeOf(columnType)
                );
                break;
        }
    }

    private void oooCopyFixedSizeCol(
            long src,
            long srcLo,
            long srcHi,
            long dst,
            long dstOffset,
            final int shl
    ) {
        final long len = (srcHi - srcLo + 1) << shl;
        Unsafe.getUnsafe().copyMemory(src + (srcLo << shl), dst + dstOffset, len);
    }

    private void oooCopyIndex(
            long mergeIndex,
            long mergeIndexSize,
            long dstAddr,
            long dstOffset
    ) {
        final long dst = dstAddr + dstOffset;
        for (long l = 0; l < mergeIndexSize; l++) {
            Unsafe.getUnsafe().putLong(dst + l * Long.BYTES, getTimestampIndexValue(mergeIndex, l));
        }
    }

    private void oooCopyOOO(
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

    private void oooCopyVarSizeCol(
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
        final long lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = Unsafe.getUnsafe().getLong(srcFixAddr + (srcHi + 1) * Long.BYTES);
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

    private void oooMergeCopyStrColumn(
            long mergeIndex,
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
            final long row = getTimestampIndexRow(mergeIndex, l);
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

    private void oooMergeCopyBinColumn(
            long mergeIndex,
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
            final long row = getTimestampIndexRow(mergeIndex, l);
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

    private void oooMergeCopy(
            int columnType,
            // todo: merge index has to be freed by last column copy
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
        try {
            switch (columnType) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    // todo: check if we only need to merge subset of data and out of order (i suspect "mergeIndexAddr" has indices of rows in either, so we're supplying both fully)
                    Vect.mergeShuffle8Bit(
                            srcDataFixAddr,
                            srcOooFixAddr,
                            dstFixAddr + dstFixOffset,
                            mergeIndexAddr,
                            rowCount
                    );
                    break;
                case ColumnType.SHORT:
                case ColumnType.CHAR:
                    Vect.mergeShuffle16Bit(
                            srcDataFixAddr,
                            srcOooFixAddr,
                            dstFixAddr + dstFixOffset,
                            mergeIndexAddr,
                            rowCount
                    );
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
                    Vect.mergeShuffle32Bit(
                            srcDataFixAddr,
                            srcOooFixAddr,
                            dstFixAddr + dstFixOffset,
                            mergeIndexAddr,
                            rowCount
                    );
                    break;
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    Vect.mergeShuffle64Bit(
                            srcDataFixAddr,
                            srcOooFixAddr,
                            dstFixAddr + dstFixOffset,
                            mergeIndexAddr,
                            rowCount
                    );
                    break;
                case -ColumnType.TIMESTAMP:
                    oooCopyIndex(mergeIndexAddr, rowCount, dstFixAddr, dstFixOffset);
                    break;
            }
        } finally {
            Vect.freeMergedIndex(mergeIndexAddr);
        }
    }

    private void shiftCopyFixedSizeColumnData(
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
}
