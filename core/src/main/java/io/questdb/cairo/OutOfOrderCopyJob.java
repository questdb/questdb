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
        final long srcLo = task.getSrcLo();
        final long srcHi = task.getSrcHi();
        final long dstFixAddr = task.getDstFixAddr();
        final long dstFixOffset = task.getDstFixOffset();
        final long dstVarAddr = task.getDstVarAddr();
        final long dstVarOffset = task.getDstVarOffset();
        final long srcFixAddr= task.getSrcFixAddr();
        final long srcFixSize = task.getSrcFixSize();
        final long srcVarAddr = task.getSrcVarAddr();
        final long srcVarSize = task.getSrcVarSize();
        final int columnType = task.getColumnType();
        final ContiguousVirtualMemory oooFixColumn = task.getOooFixColumn();
        final ContiguousVirtualMemory oooVarColumn = task.getOooVarColumn();

        switch (blockType) {
            case OO_BLOCK_MERGE:

            case OO_BLOCK_OO:
                oooCopyOOO(
                        oooFixColumn,
                        oooVarColumn,
                        dstFixOffset,
                        srcLo,
                        srcHi,
                        columnType,
                        dstFixAddr,
                        dstVarAddr,
                        dstVarOffset
                );
                break;
            case OO_BLOCK_DATA:
                oooCopyData(
                        columnType,
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

    private void oooCopyOOO(
            ContiguousVirtualMemory oooFixColumn,
            ContiguousVirtualMemory oooVarColumn,
            long dstFixOffset,
            long indexLo,
            long indexHi,
            int columnType,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                // we can find out the edge of string column in one of two ways
                // 1. if indexHi is at the limit of the page - we need to copy the whole page of strings
                // 2  if there are more items behind indexHi we can get offset of indexHi+1
                oooCopyVarSizeCol(
                        oooVarColumn.addressOf(0),
                        oooVarColumn.getAppendOffset(),
                        oooFixColumn.addressOf(0),
                        oooFixColumn.getAppendOffset(),
                        indexLo,
                        indexHi,
                        dstFixAddr, dstFixOffset, dstVarAddr, dstVarOffset
                );
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                oooCopyFixedSizeCol(oooFixColumn.addressOf(0), indexLo, indexHi, dstFixAddr, dstFixOffset, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                oooCopyFixedSizeCol(oooFixColumn.addressOf(0), indexLo, indexHi, dstFixAddr, dstFixOffset, 1);
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
                oooCopyFixedSizeCol(oooFixColumn.addressOf(0), indexLo, indexHi, dstFixAddr, dstFixOffset, 2);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
            case ColumnType.TIMESTAMP:
                oooCopyFixedSizeCol(oooFixColumn.addressOf(0), indexLo, indexHi, dstFixAddr, dstFixOffset, 3);
                break;
            case -ColumnType.TIMESTAMP:
                copyFromTimestampIndex(oooFixColumn.addressOf(0), indexLo, indexHi, dstFixAddr, dstFixOffset);
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

    private void oooMergeCopyFixColumn(
            long[] mergeStruct,
            long mergeIndex,
            long count,
            int columnIndex,
            int fixColumnOffset,
            MergeShuffleOutOfOrderDataInternal shuffleFunc
    ) {
        shuffleFunc.shuffle(
                MergeStruct.getSrcAddressFromOffset(mergeStruct, fixColumnOffset),
                oooColumns.getQuick(getPrimaryColumnIndex(columnIndex)).addressOf(0),
                MergeStruct.getDestAddressFromOffset(mergeStruct, fixColumnOffset)
                        + MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnOffset, MergeStruct.STAGE_MERGE),
                mergeIndex,
                count
        );
    }


    private void oooMergeCopy(
            int columnType,
            // todo: merge index has to be freed by last column copy
            long mergeIndex,
            long mergedTimestamps,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi
    ) {
        final long dataOOMergeIndexLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        try {
                switch (columnType) {
                    case ColumnType.BOOLEAN:
                    case ColumnType.BYTE:
                        oooMergeCopyFixColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, MERGE_SHUFFLE_8);
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.CHAR:
                        oooMergeCopyFixColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, MERGE_SHUFFLE_16);
                        break;
                    case ColumnType.STRING:
                        oooMergeCopyStrColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, varColumnOffset);
                        break;
                    case ColumnType.BINARY:
                        oooMergeCopyBinColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, varColumnOffset);
                        break;
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.SYMBOL:
                        oooMergeCopyFixColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, MERGE_SHUFFLE_32);
                        break;
                    case ColumnType.DOUBLE:
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        if (i == timestampIndex) {
                            // copy timestamp values from the merge index
                            oooCopyIndex(mergeStruct, mergeIndex, dataOOMergeIndexLen, fixColumnOffset);
                            break;
                        }
                        oooMergeCopyFixColumn(mergeStruct, mergeIndex, dataOOMergeIndexLen, i, fixColumnOffset, MERGE_SHUFFLE_64);
                        break;
                }
        } finally {
            Vect.freeMergedIndex(mergeIndex);
        }
    }
}
