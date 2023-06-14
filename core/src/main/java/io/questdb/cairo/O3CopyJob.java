/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.*;
import io.questdb.tasks.O3CopyTask;
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
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            int srcDataVarFd,
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
            long partitionTimestamp, // <-- this is used to determine if partition is last or not as well as partition dir
            int dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixFileOffset,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarOffsetEnd,
            long dstVarAdjust,
            long dstVarSize,
            int dstKFd,
            int dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long newPartitionSize,
            long oldPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr
    ) {
        final boolean mixedIOFlag = tableWriter.allowMixedIO();
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
                .$(", mixedIOFlag=").$(mixedIOFlag)
                .I$();

        try {
            switch (blockType) {
                case O3_BLOCK_MERGE:
                    mergeCopy(
                            columnType,
                            timestampMergeIndexAddr,
                            // this is a hack, when we have column top we can have only of the two:
                            // srcDataFixOffset, when we had to shift data to back-fill nulls or
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
                            tableWriter.getFilesFacade(),
                            columnType,
                            srcOooFixAddr,
                            srcOooVarAddr,
                            srcOooLo,
                            srcOooHi,
                            dstFixFd,
                            dstFixAddr + dstFixOffset,
                            dstFixFileOffset,
                            dstVarAddr,
                            dstVarFd,
                            dstVarOffset,
                            dstVarAdjust,
                            dstVarSize,
                            mixedIOFlag
                    );
                    break;
                case O3_BLOCK_DATA:
                    copyData(
                            tableWriter.getFilesFacade(),
                            columnType,
                            srcDataFixAddr + srcDataFixOffset,
                            srcDataVarAddr + srcDataVarOffset,
                            srcDataLo,
                            srcDataHi,
                            dstFixAddr + dstFixOffset,
                            dstFixFd,
                            dstFixFileOffset,
                            dstVarAddr,
                            dstVarFd,
                            dstVarOffset,
                            dstVarAdjust,
                            dstVarSize,
                            mixedIOFlag
                    );
                    break;
                default:
                    break;
            }
        } catch (Throwable th) {
            FilesFacade ff = tableWriter.getFilesFacade();
            O3Utils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            O3Utils.unmapAndClose(ff, srcDataVarFd, srcDataVarAddr, srcDataVarSize);
            O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff, dstVarFd, dstVarAddr, dstVarSize);

            closeColumnIdle(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
            throw th;
        }

        copyTail(
                columnCounter,
                partCounter,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcOooMax,
                srcOooPartitionHi,
                timestampMin,
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
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                newPartitionSize,
                oldPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr
        );
    }

    public static void copy(O3CopyTask task, long cursor, Sequence subSeq) {
        final AtomicInteger columnCounter = task.getColumnCounter();
        final AtomicInteger partCounter = task.getPartCounter();
        final int columnType = task.getColumnType();
        final int blockType = task.getBlockType();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final long timestampMergeIndexSize = task.getTimestampMergeIndexSize();
        final int srcDataFixFd = task.getSrcDataFixFd();
        final long srcDataFixAddr = task.getSrcDataFixAddr();
        final long srcDataFixOffset = task.getSrcDataFixOffset();
        final long srcDataFixSize = task.getSrcDataFixSize();
        final int srcDataVarFd = task.getSrcDataVarFd();
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
        final long partitionTimestamp = task.getPartitionTimestamp();
        final int dstFixFd = task.getDstFixFd();
        final long dstFixAddr = task.getDstFixAddr();
        final long dstFixOffset = task.getDstFixOffset();
        final long dstFixFileOffset = task.getDstFixFileOffset();
        final long dstFixSize = task.getDstFixSize();
        final int dstVarFd = task.getDstVarFd();
        final long dstVarAddr = task.getDstVarAddr();
        final long dstVarOffset = task.getDstVarOffset();
        final long dstVarOffsetEnd = task.getDstVarOffsetEnd();
        final long dstVarAdjust = task.getDstVarAdjust();
        final long dstVarSize = task.getDstVarSize();
        final int dstKFd = task.getDstKFd();
        final int dskVFd = task.getDstVFd();
        final long dstIndexOffset = task.getDstIndexOffset();
        final long dstIndexAdjust = task.getDstIndexAdjust();
        final int indexBlockCapacity = task.getIndexBlockCapacity();
        final int srcTimestampFd = task.getSrcTimestampFd();
        final long srcTimestampAddr = task.getSrcTimestampAddr();
        final long srcTimestampSize = task.getSrcTimestampSize();
        final boolean partitionMutates = task.isPartitionMutates();
        final long newPartitionSize = task.getNewPartitionSize();
        final long oldPartitionSize = task.getOldPartitionSize();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();

        subSeq.done(cursor);

        copy(
                columnCounter,
                partCounter,
                columnType,
                blockType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
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
                partitionTimestamp,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixFileOffset,
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
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                partitionMutates,
                newPartitionSize,
                oldPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr
        );
    }

    private static void copyData(
            FilesFacade ff,
            int columnType,
            long srcFixAddr,
            long srcVarAddr,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            int dstFixFd,
            long dstFixFileOffset,
            long dstVarAddr,
            int dstVarFd,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            boolean mixedIOFlag
    ) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                copyVarSizeCol(
                        ff,
                        srcFixAddr,
                        srcVarAddr,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        dstFixFd,
                        dstFixFileOffset,
                        dstVarAddr,
                        dstVarFd,
                        dstVarOffset,
                        dstVarAdjust,
                        dstVarSize,
                        mixedIOFlag
                );
                break;
            default:
                copyFixedSizeCol(
                        ff,
                        srcFixAddr,
                        srcLo,
                        srcHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        ColumnType.pow2SizeOf(Math.abs(columnType)),
                        mixedIOFlag
                );
                break;
        }
    }

    private static void copyFixedSizeCol(
            FilesFacade ff,
            long src,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            long dstFixFileOffset,
            int dstFd,
            final int shl,
            boolean mixedIOFlag
    ) {
        final long len = (srcHi - srcLo + 1) << shl;
        final long fromAddress = src + (srcLo << shl);
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstFd), fromAddress, len, dstFixFileOffset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy fixed column prefix [fd=")
                        .put(dstFd).put(", len=").put(len).put(", offset=").put(fromAddress).put(']');
            }
        } else {
            Vect.memcpy(dstFixAddr, fromAddress, len);
        }
    }

    private static void copyTail(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcOooMax,
            long srcOooPartitionHi,
            long timestampMin,
            long partitionTimestamp,
            int dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            int dstKFd,
            int dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long newPartitionSize,
            long oldPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr
    ) {
        if (partCounter == null || partCounter.decrementAndGet() == 0) {
            final FilesFacade ff = tableWriter.getFilesFacade();
            if (indexBlockCapacity > -1) {
                updateIndex(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
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
                        indexWriter,
                        indexBlockCapacity
                );
            }

            final int commitMode = tableWriter.getConfiguration().getCommitMode();
            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
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
                        tableWriter,
                        ff,
                        commitMode
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
                        timestampMergeIndexSize,
                        srcOooMax,
                        srcOooPartitionHi,
                        timestampMin,
                        partitionTimestamp,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        partitionMutates,
                        newPartitionSize,
                        oldPartitionSize,
                        partitionUpdateSinkAddr,
                        tableWriter
                );
            }
        }
    }

    private static void copyVarSizeCol(
            FilesFacade ff,
            long srcFixAddr,
            long srcVarAddr,
            long srcLo,
            long srcHi,
            long dstFixAddr,
            int dstFixFd,
            long dstFixFileOffset,
            long dstVarAddr,
            int dstVarFd,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            boolean mixedIOFlag
    ) {
        final long lo = O3Utils.findVarOffset(srcFixAddr, srcLo);
        assert lo >= 0;
        final long hi = O3Utils.findVarOffset(srcFixAddr, srcHi + 1);
        assert hi >= lo;
        // copy this before it changes
        final long len = hi - lo;
        assert len <= Math.abs(dstVarSize) - dstVarOffset;
        final long offset = dstVarOffset + dstVarAdjust;
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstVarFd), srcVarAddr + lo, len, offset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy var data column prefix [fd=").put(dstVarFd).put(", offset=").put(offset).put(", len=").put(len).put(']');
            }
        } else {
            Vect.memcpy(dstVarAddr + dstVarOffset, srcVarAddr + lo, len);
        }
        if (lo == offset) {
            copyFixedSizeCol(
                    ff,
                    srcFixAddr,
                    srcLo,
                    srcHi + 1,
                    dstFixAddr,
                    dstFixFileOffset,
                    dstFixFd,
                    3,
                    mixedIOFlag
            );
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
            case ColumnType.UUID:
            case ColumnType.LONG128:
                Vect.mergeShuffle128Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            case ColumnType.LONG256:
                Vect.mergeShuffle256Bit(srcDataFixAddr, srcOooFixAddr, dstFixAddr, timestampMergeIndexAddr, rowCount);
                break;
            default:
                break;
        }
    }

    private static void syncColumns(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            int dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            FilesFacade ff,
            int commitMode
    ) {
        try {
            boolean async = commitMode == CommitMode.ASYNC;
            if (dstFixAddr != 0 && dstFixSize > 0) {
                ff.msync(dstFixAddr, dstFixSize, async);
                // sync FD in case we wrote data not via mmap
                if (dstFixFd != -1 && dstFixFd != 0) {
                    ff.fsync(Math.abs(dstFixFd));
                }
            }
            if (dstVarAddr != 0 && dstVarSize > 0) {
                ff.msync(dstVarAddr, dstVarSize, async);
                if (dstVarFd != -1 && dstVarFd != 0) {
                    ff.fsync(Math.abs(dstVarFd));
                }
            }
        } catch (Throwable e) {
            LOG.error()
                    .$("sync error [table=").utf8(tableWriter.getTableToken().getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
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

    private static void updateIndex(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            int dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            int dstKFd,
            int dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            int indexBlockCapacity
    ) {
        // dstKFd & dstVFd are closed by the indexer
        try {
            long row = dstIndexOffset / Integer.BYTES;
            boolean closed = !indexWriter.isOpen();
            if (closed) {
                indexWriter.of(tableWriter.getConfiguration(), dstKFd, dstVFd, row == 0, indexBlockCapacity);
            }
            try {
                updateIndex(dstFixAddr, dstFixSize, indexWriter, dstIndexOffset / Integer.BYTES, dstIndexAdjust);
                indexWriter.commit();
            } finally {
                if (closed) {
                    Misc.free(indexWriter);
                }
            }
        } catch (Throwable e) {
            LOG.error()
                    .$("index error [table=").utf8(tableWriter.getTableToken().getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
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

    private static void updateIndex(long dstFixAddr, long dstFixSize, BitmapIndexWriter w, long row, long rowAdjust) {
        w.rollbackConditionally(row + rowAdjust);
        final long count = dstFixSize / Integer.BYTES;
        for (; row < count; row++) {
            w.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(dstFixAddr + row * Integer.BYTES)), row + rowAdjust);
        }
        w.setMaxValue(row + rowAdjust);
    }

    // lowest timestamp of partition where data is headed

    private static void updatePartition(
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooMax,
            long srcOooPartitionHi,
            long timestampMin,
            long partitionTimestamp,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            long newPartitionSize,
            long oldPartitionSize,
            long partitionUpdateSinkAddr,
            TableWriter tableWriter
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        try {
            try {
                O3Utils.close(ff, srcTimestampFd);
            } finally {
                o3NotifyPartitionUpdate(
                        tableWriter,
                        partitionUpdateSinkAddr,
                        timestampMin,
                        partitionTimestamp,
                        newPartitionSize,
                        oldPartitionSize,
                        partitionMutates,
                        srcOooPartitionHi + 1 == srcOooMax
                );
            }
        } finally {
            if (timestampMergeIndexAddr != 0) {
                Vect.freeMergedIndex(timestampMergeIndexAddr, timestampMergeIndexSize);
            }
            tableWriter.o3CountDownDoneLatch();
        }
    }

    static void closeColumnIdle(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter
    ) {
        final int columnsRemaining = columnCounter.decrementAndGet();
        LOG.debug()
                .$("idle [table=").utf8(tableWriter.getTableToken().getTableName())
                .$(", columnsRemaining=").$(columnsRemaining)
                .I$();
        if (columnsRemaining == 0) {
            closeColumnIdleQuick(
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    static void closeColumnIdleQuick(
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter
    ) {
        try {
            final FilesFacade ff = tableWriter.getFilesFacade();
            O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
            O3Utils.close(ff, srcTimestampFd);
            if (timestampMergeIndexAddr != 0) {
                Vect.freeMergedIndex(timestampMergeIndexAddr, timestampMergeIndexSize);
            }
        } finally {
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
        }
    }

    static void copyIdle(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            int dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int dstKFd,
            int dstVFd,
            TableWriter tableWriter
    ) {
        if (partCounter == null || partCounter.decrementAndGet() == 0) {
            // unmap memory
            copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
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
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            int dstKFd,
            int dstVFd,
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
                    timestampMergeIndexSize,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter
            );
        }
    }

    static void copyO3(
            FilesFacade ff,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            int dstFixFd,
            long dstFixAddr,
            long dstFixFileOffset,
            long dstVarAddr,
            int dstVarFd,
            long dstVarOffset,
            long dstVarAdjust,
            long dstVarSize,
            boolean mixedIOFlag
    ) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                // we can find out the edge of string column in one of two ways
                // 1. if srcOooHi is at the limit of the page - we need to copy the whole page of strings
                // 2  if there are more items behind srcOooHi we can get offset of srcOooHi+1
                copyVarSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFd,
                        dstFixFileOffset,
                        dstVarAddr,
                        dstVarFd,
                        dstVarOffset,
                        dstVarAdjust,
                        dstVarSize,
                        mixedIOFlag
                );
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        0,
                        mixedIOFlag
                );
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        1,
                        mixedIOFlag
                );
                break;
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.SYMBOL:
            case ColumnType.GEOINT:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        2,
                        mixedIOFlag
                );
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
            case ColumnType.GEOLONG:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        3,
                        mixedIOFlag
                );
                break;
            case ColumnType.TIMESTAMP:
                final boolean designated = ColumnType.isDesignatedTimestamp(columnType);
                if (designated) {
                    O3Utils.copyFromTimestampIndex(srcOooFixAddr, srcOooLo, srcOooHi, dstFixAddr);
                } else {
                    copyFixedSizeCol(
                            ff,
                            srcOooFixAddr,
                            srcOooLo,
                            srcOooHi,
                            dstFixAddr,
                            dstFixFileOffset,
                            dstFixFd,
                            3,
                            mixedIOFlag
                    );
                }
                break;
            case ColumnType.UUID:
            case ColumnType.LONG128:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        4,
                        mixedIOFlag
                );
                break;
            case ColumnType.LONG256:
                copyFixedSizeCol(
                        ff,
                        srcOooFixAddr,
                        srcOooLo,
                        srcOooHi,
                        dstFixAddr,
                        dstFixFileOffset,
                        dstFixFd,
                        5,
                        mixedIOFlag
                );
                break;
            default:
                // we have exhausted all supported types in "case" clauses
                break;
        }
    }

    static void o3NotifyPartitionUpdate(
            TableWriter tableWriter,
            final long partitionUpdateSinkAddr,
            long timestampMin,
            long partitionTimestamp,
            final long newPartitionSize,
            final long oldPartitionSize,
            boolean partitionMutates,
            boolean isLastWrittenPartition
    ) {
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + Long.BYTES, timestampMin);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
        long flags = Numbers.encodeLowHighInts(partitionMutates ? 1 : 0, isLastWrittenPartition ? 1 : 0);
        Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, flags);

        tableWriter.o3ClockDownPartitionUpdateCount();
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        copy(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
