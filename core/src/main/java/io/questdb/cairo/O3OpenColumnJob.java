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
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;
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
    private final static Log LOG = LogFactory.getLog(O3OpenColumnJob.class);

    public O3OpenColumnJob(MessageBus messageBus) {
        super(messageBus.getO3OpenColumnQueue(), messageBus.getO3OpenColumnSubSeq());
    }

    public static void appendLastPartition(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            boolean isIndexed,
            MemoryMAR dstFixMem,
            MemoryMAR dstVarMem,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                appendVarColumn(
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        -dstFixMem.getFd(),
                        -dstVarMem.getFd(),
                        dstFixMem,
                        dstVarMem,
                        dstLen,
                        tableWriter
                );
                break;
            case ColumnType.TIMESTAMP:
                final boolean designated = ColumnType.isDesignatedTimestamp(columnType);
                if (designated) {
                    appendTimestampColumn(
                            columnCounter,
                            columnType,
                            srcOooFixAddr,
                            srcOooVarAddr,
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
                            dstFixMem,
                            dstLen,
                            tableWriter
                    );
                    break;
                } // else fall through
            default:
                appendFixColumn(
                        pathToPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        -dstFixMem.getFd(),
                        dstFixMem,
                        dstLen,
                        tableWriter,
                        indexWriter
                );
                break;
        }
    }

    public static void openColumn(O3OpenColumnTask task, long cursor, Sequence subSeq, long tmpBuf) {
        final int openColumnMode = task.getOpenColumnMode();
        final CharSequence pathToTable = task.getPathToTable();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
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
        final long srcOooVarAddr = task.getSrcOooVarAddr();
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

        subSeq.done(cursor);

        openColumn(
                openColumnMode,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                srcOooFixAddr,
                srcOooVarAddr,
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
                tmpBuf
        );
    }

    public static void openColumn(
            int openColumnMode,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long tmpBuf
    ) {
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final Path pathToPartition = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(pathToPartition, tableWriter.getPartitionBy(), oooTimestampLo, false);
        final int pplen = pathToPartition.length();
        TableUtils.txnPartitionConditionally(pathToPartition, srcDataTxn);
        final int plen = pathToPartition.length();
        // append jobs do not set value of part counter, we do it here for those
        switch (openColumnMode) {
            case OPEN_MID_PARTITION_FOR_APPEND:
                appendMidPartition(
                        pathToPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        tmpBuf
                );
                break;
            case OPEN_MID_PARTITION_FOR_MERGE:
                mergeMidPartition(
                        pathToPartition,
                        plen,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        tmpBuf
                );
                break;
            case OPEN_LAST_PARTITION_FOR_MERGE:
                mergeLastPartition(
                        pathToPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        tmpBuf
                );
                break;
            case OPEN_NEW_PARTITION_FOR_APPEND:
                appendNewPartition(
                        pathToPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataMax,
                        isIndexed,
                        tableWriter,
                        indexWriter
                );
                break;
            default:
                break;
        }
    }

    private static void appendMidPartition(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long tmpBuf
    ) {
        long dstFixFd = 0;
        long dstVarFd = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        if (srcDataTop == -1) {
            try {
                srcDataTop = getSrcDataTop(ff, pathToPartition, plen, columnName, srcDataMax, tmpBuf);
                if (srcDataTop == srcDataMax) {
                    TableUtils.writeColumnTop(
                            ff,
                            pathToPartition.trimTo(plen),
                            columnName,
                            srcDataMax,
                            tmpBuf
                    );
                }
            } catch (Throwable e) {
                LOG.error().$("append mid partition error 1 [table=").$(tableWriter.getTableName())
                        .$(", e=").$(e)
                        .I$();
                tableWriter.o3BumpErrorCount();
                O3CopyJob.copyIdleQuick(
                        columnCounter,
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
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        tableWriter
                );
                throw e;
            }
        }

        final long dstLen = srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop;
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                try {
                    // index files are opened as normal
                    iFile(pathToPartition.trimTo(plen), columnName);
                    dstFixFd = openRW(ff, pathToPartition, LOG);
                    // open data file now
                    dFile(pathToPartition.trimTo(plen), columnName);
                    dstVarFd = openRW(ff, pathToPartition, LOG);
                } catch (Throwable e) {
                    LOG.error().$("append mid partition error 2 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
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
                            dstFixFd,
                            0,
                            0,
                            dstVarFd,
                            0,
                            0,
                            0,
                            0,
                            tableWriter
                    );
                    throw e;
                }
                appendVarColumn(
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        dstFixFd,
                        dstVarFd,
                        null,
                        null,
                        dstLen,
                        tableWriter
                );
                break;
            case ColumnType.TIMESTAMP:
                final boolean designated = ColumnType.isDesignatedTimestamp(columnType);
                if (designated) {
                    appendTimestampColumn(
                            columnCounter,
                            columnType,
                            srcOooFixAddr,
                            srcOooVarAddr,
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
                            null,
                            dstLen,
                            tableWriter
                    );
                    break;
                } // else fall through
            default:
                try {
                    dFile(pathToPartition.trimTo(plen), columnName);
                    dstFixFd = openRW(ff, pathToPartition, LOG);
                } catch (Throwable e) {
                    LOG.error().$("append mid partition error 3 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
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
                            dstFixFd,
                            0,
                            0,
                            dstVarFd,
                            0,
                            0,
                            0,
                            0,
                            tableWriter
                    );
                    throw e;
                }
                appendFixColumn(
                        pathToPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        dstFixFd,
                        null,
                        dstLen,
                        tableWriter,
                        indexWriter
                );
                break;
        }
    }

    private static void appendFixColumn(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long dstFixFd,
            MemoryMAR dstFixMem,
            long dstLen,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        long dstKFd = 0;
        long dstVFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstIndexOffset;
        long dstIndexAdjust;
        long dstFixSize;
        final int shl = ColumnType.pow2SizeOf(columnType);
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            dstFixSize = dstLen << shl;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = (srcDataMax - srcDataTop) << shl;
                dstFixAddr = mapRW(ff, Math.abs(dstFixFd), dstFixSize, MemoryTag.MMAP_O3);
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
                BitmapIndexUtils.keyFileName(pathToPartition.trimTo(plen), columnName);
                dstKFd = openRW(ff, pathToPartition, LOG);
                BitmapIndexUtils.valueFileName(pathToPartition.trimTo(plen), columnName);
                dstVFd = openRW(ff, pathToPartition, LOG);
            }
        } catch (Throwable e) {
            LOG.error().$("append fix error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
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
                    dstFixFd,
                    0,
                    0,
                    0,
                    0,
                    0,
                    dstKFd,
                    dstVFd,
                    tableWriter
            );
            throw e;
        }

        publishCopyTask(
                columnCounter,
                null,
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
                srcOooVarAddr,
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
                indexWriter
        );
    }

    private static void appendTimestampColumn(
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            MemoryMA dstFixMem,
            long dstLen,
            TableWriter tableWriter
    ) {
        long dstFixFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixSize;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            dstFixSize = dstLen * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFd = -Math.abs(srcTimestampFd);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixSize = -dstFixSize;
            }
        } catch (Throwable e) {
            LOG.error().$("append ts error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
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
                    dstFixFd,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    tableWriter
            );
            throw e;
        }

        publishCopyTask(
                columnCounter,
                null,
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
                srcOooVarAddr,
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
                0,
                isIndexed,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                false,
                tableWriter,
                null
        );
    }

    private static void appendVarColumn(
            AtomicInteger columnCounter,
            int columnType,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long activeFixFd,
            long activeVarFd,
            MemoryMA dstFixMem,
            MemoryMA dstVarMem,
            long dstLen,
            TableWriter tableWriter
    ) {
        long dstFixAddr = 0;
        long dstFixOffset;
        long dstVarAddr = 0;
        long dstVarOffset;
        long dstVarAdjust;
        long dstVarSize = 0;
        long dstFixSize = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            long l = O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr);
            dstFixSize = (dstLen + 1) * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize || dstVarMem.getAppendAddressSize() < l) {
                dstFixOffset = (srcDataMax - srcDataTop) * Long.BYTES;
                dstFixAddr = mapRW(ff, Math.abs(activeFixFd), dstFixSize, MemoryTag.MMAP_O3);

                if (dstFixOffset > 0) {
                    dstVarOffset = Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset);
                } else {
                    dstVarOffset = 0;
                }

                dstVarSize = l + dstVarOffset;
                dstVarAddr = mapRW(ff, Math.abs(activeVarFd), dstVarSize, MemoryTag.MMAP_O3);
                dstVarAdjust = 0;
            } else {
                dstFixAddr = dstFixMem.getAppendAddress() - Long.BYTES;
                dstVarAddr = dstVarMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixSize = -dstFixSize;
                dstVarOffset = 0;
                dstVarSize = -l;
                dstVarAdjust = dstVarMem.getAppendOffset();
            }
        } catch (Throwable e) {
            LOG.error().$("append var error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
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
                    activeFixFd,
                    dstFixAddr,
                    dstFixSize,
                    activeVarFd,
                    dstVarAddr,
                    dstVarSize,
                    0,
                    0,
                    tableWriter
            );
            throw e;
        }
        publishCopyTask(
                columnCounter,
                null,
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
                srcOooVarAddr,
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
                0,
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
                null
        );
    }

    private static void publishCopyTask(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
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
            long srcOooVarAddr,
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
            long dstVarOffsetEnd,
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
            BitmapIndexWriter indexWriter
    ) {
        long cursor = tableWriter.getO3CopyPubSeq().next();
        if (cursor > -1) {
            publishCopyTaskHarmonized(
                    columnCounter,
                    partCounter,
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
                    srcOooVarAddr,
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
                    dstVarOffsetEnd,
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
                    indexWriter
            );
        } else {
            publishCopyTaskContended(
                    cursor,
                    columnCounter,
                    partCounter,
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
                    srcOooVarAddr,
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
                    dstVarOffsetEnd,
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
                    indexWriter
            );
        }
    }

    private static void publishCopyTaskContended(
            long cursor,
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
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
            long srcOooVarAddr,
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
            long dstVarOffsetEnd,
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
            BitmapIndexWriter indexWriter
    ) {
        while (cursor == -2) {
            cursor = tableWriter.getO3CopyPubSeq().next();
        }

        if (cursor == -1) {
            O3CopyJob.copy(
                    columnCounter,
                    partCounter,
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
                    srcOooVarAddr,
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
                    dstVarOffsetEnd,
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
                    indexWriter
            );
        } else {
            publishCopyTaskHarmonized(
                    columnCounter,
                    partCounter,
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
                    srcOooVarAddr,
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
                    dstVarOffsetEnd,
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
                    indexWriter
            );
        }
    }

    private static void publishCopyTaskHarmonized(
            AtomicInteger columnCounter,
            @Nullable AtomicInteger partCounter,
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
            long srcOooVarAddr,
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
            long dstVarOffsetEnd,
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
            BitmapIndexWriter indexWriter
    ) {
        final O3CopyTask task = tableWriter.getO3CopyQueue().get(cursor);
        task.of(
                columnCounter,
                partCounter,
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
                srcOooVarAddr,
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
                dstVarOffsetEnd,
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
                indexWriter
        );
        tableWriter.getO3CopyPubSeq().done(cursor);
    }

    private static void mergeLastPartition(
            Path pathToPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long tmpBuf
    ) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                mergeVarColumn(
                        pathToPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        -activeFixFd,
                        -activeVarFd,
                        tableWriter,
                        tmpBuf
                );
                break;
            default:
                mergeFixColumn(
                        pathToPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        srcDataTop,
                        -activeFixFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
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
                        tableWriter,
                        indexWriter,
                        tmpBuf
                );
                break;
        }
    }

    private static void mergeMidPartition(
            Path pathToPartition,
            int plen,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long tmpBuf
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        // not set, we need to check file existence and read
        if (srcDataTop == -1) {
            try {
                srcDataTop = getSrcDataTop(ff, pathToPartition, plen, columnName, srcDataMax, tmpBuf);
            } catch (Throwable e) {
                LOG.error().$("merge mid partition error 1 [table=").$(tableWriter.getTableName())
                        .$(", e=").$(e)
                        .I$();
                tableWriter.o3BumpErrorCount();
                O3CopyJob.copyIdleQuick(
                        columnCounter,
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
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        tableWriter
                );
                throw e;
            }
        }

        long srcDataFixFd = 0;
        long srcDataVarFd = 0;
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                try {
                    iFile(pathToPartition.trimTo(plen), columnName);
                    srcDataFixFd = openRW(ff, pathToPartition, LOG);
                    dFile(pathToPartition.trimTo(plen), columnName);
                    srcDataVarFd = openRW(ff, pathToPartition, LOG);
                } catch (Throwable e) {
                    LOG.error().$("merge mid partition error 2 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
                            0,
                            srcDataFixFd,
                            0,
                            0,
                            srcDataVarFd,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            tableWriter
                    );
                    throw e;
                }

                mergeVarColumn(
                        pathToPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
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
                        srcDataFixFd,
                        srcDataVarFd,
                        tableWriter,
                        tmpBuf
                );
                break;
            default:
                try {
                    if (columnType < 0 && srcTimestampFd > 0) {
                        // ensure timestamp srcDataFixFd is always negative, we will close it externally
                        srcDataFixFd = -srcTimestampFd;
                    } else {
                        dFile(pathToPartition.trimTo(plen), columnName);
                        srcDataFixFd = openRW(ff, pathToPartition, LOG);
                    }
                } catch (Throwable e) {
                    LOG.error().$("merge mid partition error 3 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount();
                    O3CopyJob.copyIdleQuick(
                            columnCounter,
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
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            tableWriter
                    );
                    throw e;
                }
                mergeFixColumn(
                        pathToPartition,
                        pplen,
                        columnName,
                        columnCounter,
                        partCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        oooPartitionMin,
                        oooPartitionMax,
                        oooPartitionHi,
                        srcDataMax,
                        srcDataTop,
                        srcDataFixFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
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
                        tableWriter,
                        indexWriter,
                        tmpBuf
                );
                break;
        }
    }

    private static long getSrcDataTop(
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence columnName,
            long srcDataMax,
            long tmpBuf
    ) {
        boolean dFileExists = ff.exists(dFile(path.trimTo(plen), columnName));
        topFile(path.trimTo(plen), columnName);
        if (dFileExists && ff.exists(path)) {
            long topFd = openRW(ff, path, LOG);
            try {
                if (ff.read(topFd, tmpBuf, Long.BYTES, 0) == Long.BYTES) {
                    return Unsafe.getUnsafe().getLong(tmpBuf);
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
            Path pathToPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooPartitionMin,
            long oooPartitionMax,
            long oooPartitionHi,
            long srcDataMax,
            long srcDataTop,
            long srcDataFixFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
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
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long tmpBuf
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
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            txnPartition(pathToPartition.trimTo(pplen), txn);
            pDirNameLen = pathToPartition.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) << shl;
                final long srcDataMaxBytes = srcDataMax << shl;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    setNull(columnType, srcDataFixAddr + srcDataActualBytes, srcDataTop);
                    Vect.memcpy(srcDataFixAddr, srcDataFixAddr + srcDataMaxBytes, srcDataActualBytes);
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    TableUtils.writeColumnTop(
                            ff,
                            pathToPartition.trimTo(pDirNameLen),
                            columnName,
                            srcDataTop,
                            tmpBuf
                    );
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    srcDataFixOffset = 0;
                }
            } else {
                srcDataFixSize = srcDataMax << shl;
                srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                srcDataFixOffset = 0;
            }

            srcDataTopOffset = srcDataTop << shl;

            pathToPartition.trimTo(pDirNameLen).concat(columnName).put(FILE_SUFFIX_D).$();
            dstFixFd = openRW(ff, pathToPartition, LOG);
            dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - srcDataTop) << shl;
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);

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
                BitmapIndexUtils.keyFileName(pathToPartition.trimTo(pDirNameLen), columnName);
                dstKFd = openRW(ff, pathToPartition, LOG);
                BitmapIndexUtils.valueFileName(pathToPartition.trimTo(pDirNameLen), columnName);
                dstVFd = openRW(ff, pathToPartition, LOG);
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
            LOG.error().$("merge fix error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
                    srcDataFixFd,
                    srcDataFixAddr,
                    srcDataFixSize,
                    0,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    dstFixFd,
                    dstFixAddr,
                    dstFixSize,
                    0,
                    0,
                    0,
                    dstKFd,
                    dstVFd,
                    tableWriter
            );
            throw e;
        }

        partCounter.set(partCount);
        publishMultiCopyTasks(
                columnCounter,
                partCounter,
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
                srcOooVarAddr,
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
                indexWriter
        );
    }

    private static void mergeVarColumn(
            Path pathToPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
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
            long srcDataFixFd,
            long srcDataVarFd,
            TableWriter tableWriter,
            long tmpBuf
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
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            txnPartition(pathToPartition.trimTo(pplen), txn);
            pDirNameLen = pathToPartition.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) * Long.BYTES;
                final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);

                    if (srcDataActualBytes > 0) {
                        srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataActualBytes);
                    }

                    // at bottom of source var column set length of strings to null (-1) for as many strings
                    // as srcDataTop value.
                    if (ColumnType.isString(columnType)) {
                        srcDataVarOffset = srcDataVarSize;
                        srcDataVarSize += srcDataTop * Integer.BYTES + srcDataVarSize;
                        srcDataVarAddr = mapRW(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                        Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        O3Utils.shiftCopyFixedSizeColumnData(-srcDataTop * Integer.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop + 1, srcDataFixAddr + srcDataMaxBytes);

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);

                        Vect.memcpy(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Integer.BYTES, srcDataVarOffset);
                    } else {
                        srcDataVarOffset = srcDataVarSize;
                        srcDataVarSize += srcDataTop * Long.BYTES + srcDataVarSize;
                        srcDataVarAddr = mapRW(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);

                        Vect.setMemoryLong(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);

                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        O3Utils.shiftCopyFixedSizeColumnData(-srcDataTop * Long.BYTES, srcDataFixAddr, 0, srcDataMax - srcDataTop + 1, srcDataFixAddr + srcDataMaxBytes);
                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs64Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);

                        Vect.memcpy(srcDataVarAddr, srcDataVarAddr + srcDataVarOffset + srcDataTop * Long.BYTES, srcDataVarOffset);
                    }
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    TableUtils.writeColumnTop(
                            ff,
                            pathToPartition.trimTo(pDirNameLen),
                            columnName,
                            srcDataTop,
                            tmpBuf
                    );
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    srcDataFixOffset = 0;

                    srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - srcDataFixOffset);
                    srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                }
            } else {
                // var index column is n+1
                srcDataFixSize = (srcDataMax + 1) * Long.BYTES;
                srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                srcDataFixOffset = 0;

                srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES);
                srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = srcDataTop * Long.BYTES;

            pathToPartition.trimTo(pDirNameLen).concat(columnName);
            int pColNameLen = pathToPartition.length();
            pathToPartition.put(FILE_SUFFIX_I).$();
            dstFixFd = openRW(ff, pathToPartition, LOG);
            dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop + 1) * Long.BYTES;
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);

            pathToPartition.trimTo(pColNameLen);
            pathToPartition.put(FILE_SUFFIX_D).$();
            dstVarFd = openRW(ff, pathToPartition, LOG);
            dstVarSize = srcDataVarSize - srcDataVarOffset
                    + O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr);
            dstVarAddr = mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3);

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
                    dstVarAppendOffset1 = O3Utils.getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr);
                    partCount++;
                    break;
                case O3_BLOCK_DATA:
                    dstVarAppendOffset1 = O3Utils.getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr + srcDataFixOffset);
                    partCount++;
                    break;
                default:
                    break;
            }

            // offset 2
            if (mergeDataLo > -1 && mergeOOOLo > -1) {
                long oooLen = O3Utils.getVarColumnLength(
                        mergeOOOLo,
                        mergeOOOHi,
                        srcOooFixAddr
                );
                long dataLen = O3Utils.getVarColumnLength(
                        mergeDataLo,
                        mergeDataHi,
                        srcDataFixAddr + srcDataFixOffset - srcDataTop * 8
                );
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
            LOG.error().$("merge var error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
                    timestampMergeIndexAddr,
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

        partCounter.set(partCount);
        publishMultiCopyTasks(
                columnCounter,
                partCounter,
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
                srcOooVarAddr,
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
                null
        );
    }

    private static void setNull(int columnType, long addr, long count) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.GEOBYTE:
                Vect.memset(addr, count, 0);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
                Vect.setMemoryShort(addr, (short) 0, count);
                break;
            case ColumnType.INT:
            case ColumnType.GEOINT:
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
            case ColumnType.TIMESTAMP:
            case ColumnType.GEOLONG:
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

    private static void appendNewPartition(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataMax,
            boolean isIndexed,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        long dstFixFd = 0;
        long dstFixAddr = 0;
        long dstFixSize = 0;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            if (ColumnType.isVariableLength(columnType)) {
                setPath(pathToPartition.trimTo(plen), columnName, FILE_SUFFIX_I);
                dstFixFd = openRW(ff, pathToPartition, LOG);
                dstFixSize = (srcOooHi - srcOooLo + 1 + 1) * Long.BYTES;
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);

                setPath(pathToPartition.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstVarFd = openRW(ff, pathToPartition, LOG);
                dstVarSize = O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3);
            } else {
                setPath(pathToPartition.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstFixFd = openRW(ff, pathToPartition, LOG);
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(pathToPartition.trimTo(plen), columnName);
                    dstKFd = openRW(ff, pathToPartition, LOG);
                    BitmapIndexUtils.valueFileName(pathToPartition.trimTo(plen), columnName);
                    dstVFd = openRW(ff, pathToPartition, LOG);
                }
            }
        } catch (Throwable e) {
            LOG.error().$("append new partition error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            O3CopyJob.copyIdleQuick(
                    columnCounter,
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
            throw e;
        }

        publishCopyTask(
                columnCounter,
                null,
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
                srcOooVarAddr,
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
                indexWriter
        );
    }

    private static void setPath(Path path, CharSequence columnName, CharSequence suffix) {
        path.concat(columnName).put(suffix).$();
    }

    private static void publishMultiCopyTasks(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
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
            long srcOooVarAddr,
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
            BitmapIndexWriter indexWriter
    ) {
        final boolean partitionMutates = true;
        switch (prefixType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcOooVarAddr,
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
                        indexWriter
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcDataTopOffset,
                        srcDataMax,
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
                        indexWriter
                );
                break;
            default:
                break;
        }

        switch (mergeType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcOooVarAddr,
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
                        indexWriter
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcDataTopOffset,
                        srcDataMax,
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
                        indexWriter
                );
                break;
            case O3_BLOCK_MERGE:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcOooVarAddr,
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
                        indexWriter
                );
                break;
            default:
                break;
        }

        switch (suffixType) {
            case O3_BLOCK_O3:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        srcOooVarAddr,
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
                        indexWriter
                );
                break;
            case O3_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
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
                        indexWriter
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
                task,
                cursor,
                subSeq,
                get8ByteBuf(workerId)
        );
    }
}
