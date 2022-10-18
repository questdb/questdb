/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
            int indexBlockCapacity,
            MemoryMA dstFixMem,
            MemoryMA dstVarMem,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long columnNameTxn
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
                        indexBlockCapacity,
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
                            indexBlockCapacity,
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
                        indexBlockCapacity,
                        0,
                        0,
                        0,
                        -dstFixMem.getFd(),
                        dstFixMem,
                        dstLen,
                        tableWriter,
                        indexWriter,
                        columnNameTxn
                );
                break;
        }
    }

    public static boolean isOpenColumnModeForAppend(int openColumnMode) {
        switch (openColumnMode) {
            case OPEN_MID_PARTITION_FOR_APPEND:
            case OPEN_LAST_PARTITION_FOR_APPEND:
            case OPEN_NEW_PARTITION_FOR_APPEND:
                return true;
            default:
                return false;
        }
    }

    public static void openColumn(O3OpenColumnTask task, long cursor, Sequence subSeq) {
        final int openColumnMode = task.getOpenColumnMode();
        final Path pathToTable = task.getPathToTable();
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
        final int indexBlockCapacity = task.getIndexBlockCapacity();
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
        final long timestampMergeIndexSize = task.getTimestampMergeIndexSize();
        final long activeFixFd = task.getActiveFixFd();
        final long activeVarFd = task.getActiveVarFd();
        final long srcDataTop = task.getSrcDataTop();
        final TableWriter tableWriter = task.getTableWriter();
        final BitmapIndexWriter indexWriter = task.getIndexWriter();
        final long colTopSinkAddr = task.getColTopSinkAddr();
        final int columnIndex = task.getColumnIndex();
        final long columnNameTxn = task.getColumnNameTxn();

        subSeq.done(cursor);

        openColumn(
                openColumnMode,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
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
                indexBlockCapacity,
                activeFixFd,
                activeVarFd,
                tableWriter,
                indexWriter,
                colTopSinkAddr,
                columnIndex,
                columnNameTxn
        );
    }

    public static void openColumn(
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            int columnIndex,
            long columnNameTxn
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
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        columnIndex,
                        columnNameTxn
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        partitionTimestamp,
                        columnIndex,
                        columnNameTxn
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        activeFixFd,
                        activeVarFd,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        columnNameTxn
                );
                break;
            case OPEN_NEW_PARTITION_FOR_APPEND:
                // mark the fact that the column is touched in the partition to the column version file
                // It's fine to overwrite this value if needed inside the job branches.
                Unsafe.getUnsafe().putLong(colTopSinkAddr, 0L);
                appendNewPartition(
                        pathToPartition,
                        plen,
                        columnName,
                        columnCounter,
                        columnType,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcOooFixAddr,
                        srcOooVarAddr,
                        srcOooLo,
                        srcOooHi,
                        srcOooMax,
                        timestampMin,
                        timestampMax,
                        partitionTimestamp,
                        srcDataMax,
                        indexBlockCapacity,
                        tableWriter,
                        indexWriter,
                        columnNameTxn
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
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        long dstFixFd = 0;
        long dstVarFd = 0;
        final FilesFacade ff = tableWriter.getFilesFacade();
        if (srcDataTop == -1) {
            try {
                srcDataTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, srcDataMax);
                if (srcDataTop == srcDataMax) {
                    Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataMax);
                }
            } catch (Throwable e) {
                LOG.error().$("append mid partition error 1 [table=").$(tableWriter.getTableName())
                        .$(", e=").$(e)
                        .I$();
                freeTs(
                        columnCounter,
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff
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
                    iFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    // open data file now
                    dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    dstVarFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                } catch (Throwable e) {
                    LOG.error().$("append mid partition error 2 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    O3Utils.close(ff, dstFixFd);
                    O3Utils.close(ff, dstVarFd);
                    freeTs(
                            columnCounter,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            tableWriter,
                            ff
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
                        indexBlockCapacity,
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
                            indexBlockCapacity,
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
                    dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                } catch (Throwable e) {
                    LOG.error().$("append mid partition error 3 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    O3Utils.close(ff, dstFixFd);
                    O3Utils.close(ff, dstVarFd);
                    freeTs(
                            columnCounter,
                            0,
                            0,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            tableWriter,
                            ff
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
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        dstFixFd,
                        null,
                        dstLen,
                        tableWriter,
                        indexWriter,
                        columnNameTxn
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
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long dstFixFd,
            MemoryMA dstFixMem,
            long dstLen,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long columnNameTxn
    ) {
        long dstKFd = 0;
        long dstVFd = 0;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixFileOffset;
        long dstIndexOffset;
        long dstIndexAdjust;
        long dstFixSize;
        final int shl = ColumnType.pow2SizeOf(columnType);
        final FilesFacade ff = tableWriter.getFilesFacade();

        try {
            dstFixSize = dstLen << shl;
            dstFixOffset = (srcDataMax - srcDataTop) << shl;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                // Area we want to write is not mapped
                dstFixAddr = mapRW(ff, Math.abs(dstFixFd), dstFixSize, MemoryTag.MMAP_O3);
            } else {
                // Area we want to write is mapped.
                // Set dstFixAddr to Append Address with adjustment that dstFixOffset offset points to offset 0.
                dstFixAddr = dstFixMem.getAppendAddress() - dstFixOffset;
                // Set size negative meaning it will not be freed
                dstFixSize = -dstFixSize;
            }
            dstIndexOffset = dstFixOffset;
            dstIndexAdjust = srcDataTop;
            dstFixFileOffset = dstFixOffset;

            if (indexBlockCapacity > -1 && !indexWriter.isOpen()) {
                BitmapIndexUtils.keyFileName(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                dstKFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                BitmapIndexUtils.valueFileName(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                dstVFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            }
        } catch (Throwable e) {
            LOG.error().$("append fix error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.unmapAndClose(ff, dstFixFd, 0, 0);
            O3Utils.close(ff, dstKFd);
            O3Utils.close(ff, dstVFd);
            freeTs(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff
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
                0,
                srcOooLo,
                srcOooHi,
                srcDataTop << shl,
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
                dstFixFileOffset,
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
                indexBlockCapacity,
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
            int indexBlockCapacity,
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
        long dstFixFileOffset;
        long dstFixSize;
        final FilesFacade ff = tableWriter.getFilesFacade();
        try {
            dstFixSize = dstLen * Long.BYTES;
            if (dstFixMem == null || dstFixMem.getAppendAddressSize() < dstFixSize) {
                dstFixOffset = srcDataMax * Long.BYTES;
                dstFixFileOffset = dstFixOffset;
                dstFixFd = -Math.abs(srcTimestampFd);
                dstFixAddr = mapRW(ff, -dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            } else {
                dstFixAddr = dstFixMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixFileOffset = dstFixMem.getAppendOffset();
                dstFixSize = -dstFixSize;
            }
        } catch (Throwable e) {
            LOG.error().$("append ts error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.close(ff, dstFixFd);
            freeTs(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff
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
                dstFixFileOffset,
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
                indexBlockCapacity,
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
            int indexBlockCapacity,
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
        long dstFixFileOffset;
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
                assert dstFixMem == null || dstFixMem.getAppendOffset() - Long.BYTES == (srcDataMax - srcDataTop) * Long.BYTES;

                dstFixOffset = (srcDataMax - srcDataTop) * Long.BYTES;
                dstFixFileOffset = dstFixOffset;
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
                assert dstFixMem.getAppendOffset() >= Long.BYTES;
                assert dstFixMem.getAppendOffset() - Long.BYTES == (srcDataMax - srcDataTop) * Long.BYTES;

                dstFixAddr = dstFixMem.getAppendAddress() - Long.BYTES;
                dstVarAddr = dstVarMem.getAppendAddress();
                dstFixOffset = 0;
                dstFixFileOffset = dstFixMem.getAppendOffset() - Long.BYTES;
                dstFixSize = -dstFixSize;
                dstVarOffset = 0;
                dstVarSize = -l;
                dstVarAdjust = dstVarMem.getAppendOffset();
            }
        } catch (Throwable e) {
            LOG.error().$("append var error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            O3Utils.unmapAndClose(ff, activeFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff, activeVarFd, dstVarAddr, dstVarSize);
            freeTs(
                    columnCounter,
                    0,
                    0,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    tableWriter,
                    ff
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
                dstFixFileOffset,
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
                indexBlockCapacity,
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
            long timestampMergeIndexSize,
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
            long dstFixFileOffset,
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
            int indexBlockCapacity,
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
                    timestampMax,
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
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
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
                    timestampMergeIndexSize,
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
                    dstFixFileOffset,
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
                    indexBlockCapacity,
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
            long timestampMergeIndexSize,
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
            long dstFixFileOffset,
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
            int indexBlockCapacity,
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
                    timestampMax,
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
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
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
                    timestampMax,
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
                    dstVFd,
                    dstIndexOffset,
                    dstIndexAdjust,
                    indexBlockCapacity,
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
            long timestampMergeIndexSize,
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
            long dstFixFileOffset,
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
            int indexBlockCapacity,
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
                timestampMax,
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
                dstVFd,
                dstIndexOffset,
                dstIndexAdjust,
                indexBlockCapacity,
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
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long columnNameTxn
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        -activeFixFd,
                        -activeVarFd,
                        tableWriter,
                        colTopSinkAddr,
                        columnNameTxn
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        columnNameTxn
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
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long partitionTimestamp,
            int columnIndex,
            long columnNameTxn
    ) {
        final FilesFacade ff = tableWriter.getFilesFacade();
        // not set, we need to check file existence and read
        if (srcDataTop == -1) {
            try {
                srcDataTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, srcDataMax);
            } catch (Throwable e) {
                LOG.error().$("merge mid partition error 1 [table=").$(tableWriter.getTableName())
                        .$(", e=").$(e)
                        .I$();
                freeTs(
                        columnCounter,
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter,
                        ff
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
                    iFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    srcDataFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    srcDataVarFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                } catch (Throwable e) {
                    LOG.error().$("merge mid partition error 2 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    O3Utils.close(ff, srcDataFixFd);
                    O3Utils.close(ff, srcDataVarFd);
                    freeTs(
                            columnCounter,
                            timestampMergeIndexAddr,
                            timestampMergeIndexSize,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            tableWriter,
                            ff
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        srcDataFixFd,
                        srcDataVarFd,
                        tableWriter,
                        colTopSinkAddr,
                        columnNameTxn
                );
                break;
            default:
                try {
                    if (columnType < 0 && srcTimestampFd > 0) {
                        // ensure timestamp srcDataFixFd is always negative, we will close it externally
                        srcDataFixFd = -srcTimestampFd;
                    } else {
                        dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                        srcDataFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    }
                } catch (Throwable e) {
                    LOG.error().$("merge mid partition error 3 [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    freeTs(
                            columnCounter,
                            timestampMergeIndexAddr,
                            timestampMergeIndexSize,
                            srcTimestampFd,
                            srcTimestampAddr,
                            srcTimestampSize,
                            tableWriter,
                            ff
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
                        timestampMergeIndexSize,
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
                        indexBlockCapacity,
                        tableWriter,
                        indexWriter,
                        colTopSinkAddr,
                        columnNameTxn
                );
                break;
        }
    }

    private static void freeTs(
            AtomicInteger columnCounter,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            TableWriter tableWriter,
            FilesFacade ff
    ) {
        tableWriter.o3BumpErrorCount();
        if (columnCounter.decrementAndGet() == 0) {
            O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
            O3Utils.close(ff, srcTimestampFd);
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
            if (timestampMergeIndexAddr != 0) {
                Vect.freeMergedIndex(timestampMergeIndexAddr, timestampMergeIndexSize);
            }
        }
    }

    private static void mergeFixColumn(
            Path pathToPartition,
            int pplen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long colTopSinkAddr,
            long columnNameTxn
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
        final boolean directIoFlag = tableWriter.preferDirectIO();

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
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    TableUtils.setNull(columnType, srcDataFixAddr + srcDataActualBytes, srcDataTop);
                    Vect.memcpy(srcDataFixAddr + srcDataMaxBytes, srcDataFixAddr, srcDataActualBytes);
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataTop);
                    srcDataFixSize = srcDataActualBytes;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    srcDataFixOffset = 0;
                }
            } else {
                srcDataFixSize = srcDataMax << shl;
                srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                srcDataFixOffset = 0;
            }

            srcDataTopOffset = srcDataTop << shl;

            dFile(pathToPartition.trimTo(pDirNameLen), columnName, columnNameTxn);
            dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax - srcDataTop) << shl;
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            if (directIoFlag) {
                ff.fadvise(dstFixFd, 0, dstFixSize, Files.POSIX_FADV_RANDOM);
            } else {
                ff.madvise(dstFixAddr, dstFixSize, Files.POSIX_MADV_RANDOM);
            }

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

            if (indexBlockCapacity > -1) {
                BitmapIndexUtils.keyFileName(pathToPartition.trimTo(pDirNameLen), columnName, columnNameTxn);
                dstKFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                BitmapIndexUtils.valueFileName(pathToPartition.trimTo(pDirNameLen), columnName, columnNameTxn);
                dstVFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
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
            O3Utils.unmapAndClose(ff, srcDataFixFd, srcDataFixAddr, srcDataFixSize);
            O3Utils.unmapAndClose(ff, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.close(ff, dstKFd);
            O3Utils.close(ff, dstVFd);
            tableWriter.o3BumpErrorCount();
            if (columnCounter.decrementAndGet() == 0) {
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                Vect.freeMergedIndex(timestampMergeIndexAddr, timestampMergeIndexSize);
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
            }
            throw e;
        }

        partCounter.set(partCount);
        publishMultiCopyTasks(
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
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
                indexBlockCapacity,
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
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcDataFixFd,
            long srcDataVarFd,
            TableWriter tableWriter,
            long colTopSinkAddr,
            long columnNameTxn
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
        final boolean directIoFlag = tableWriter.preferDirectIO();

        try {
            txnPartition(pathToPartition.trimTo(pplen), txn);
            pDirNameLen = pathToPartition.length();

            if (srcDataTop > 0) {
                final long srcDataActualBytes = (srcDataMax - srcDataTop) * Long.BYTES;
                final long srcDataMaxBytes = srcDataMax * Long.BYTES;
                if (srcDataTop > prefixHi || prefixType == O3_BLOCK_O3) {
                    // extend the existing column down, we will be discarding it anyway
                    srcDataFixSize = srcDataActualBytes + srcDataMaxBytes + Long.BYTES;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);

                    if (srcDataActualBytes > 0) {
                        srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataActualBytes);
                    }

                    // at bottom of source var column set length of strings to null (-1) for as many strings
                    // as srcDataTop value.
                    srcDataVarOffset = srcDataVarSize;
                    long reservedBytesForColTopNulls;
                    if (ColumnType.isString(columnType)) {
                        // We need to reserve null values for every column top value
                        // in the variable len file. Each null value takes 4 bytes for string
                        reservedBytesForColTopNulls = srcDataTop * Integer.BYTES;
                        srcDataVarSize += reservedBytesForColTopNulls + srcDataVarSize;
                        srcDataVarAddr = mapRW(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                        ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);

                        // Set var column values to null first srcDataTop times
                        // Next line should be:
                        // Vect.setMemoryInt(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);
                        // But we can replace it with memset setting each byte to -1
                        // because binary repr of int -1 is 4 bytes of -1
                        // memset is faster than any SIMD implementation we can come with
                        Vect.memset(srcDataVarAddr + srcDataVarOffset, (int) reservedBytesForColTopNulls, -1);

                        // Copy var column data
                        Vect.memcpy(srcDataVarAddr + srcDataVarOffset + reservedBytesForColTopNulls, srcDataVarAddr, srcDataVarOffset);

                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        long hiInclusive = srcDataMax - srcDataTop; // STOP. DON'T ADD +1 HERE. srcHi is inclusive, no need to do +1
                        assert srcDataFixSize >= srcDataMaxBytes + (hiInclusive + 1) * 8; // make sure enough len mapped
                        O3Utils.shiftCopyFixedSizeColumnData(
                                -reservedBytesForColTopNulls,
                                srcDataFixAddr,
                                0,
                                hiInclusive,
                                srcDataFixAddr + srcDataMaxBytes
                        );

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        // Call to setVarColumnRefs32Bit must be after shiftCopyFixedSizeColumnData
                        // because data first have to be shifted before overwritten
                        Vect.setVarColumnRefs32Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                    } else {
                        // We need to reserve null values for every column top value
                        // in the variable len file. Each null value takes 8 bytes for binary
                        reservedBytesForColTopNulls = srcDataTop * Long.BYTES;
                        srcDataVarSize += reservedBytesForColTopNulls + srcDataVarSize;
                        srcDataVarAddr = mapRW(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                        ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);

                        // Set var column values to null first srcDataTop times
                        // Next line should be:
                        // Vect.setMemoryLong(srcDataVarAddr + srcDataVarOffset, -1, srcDataTop);
                        // But we can replace it with memset setting each byte to -1
                        // because binary repr of int -1 is 4 bytes of -1
                        // memset is faster than any SIMD implementation we can come with
                        Vect.memset(srcDataVarAddr + srcDataVarOffset, (int) reservedBytesForColTopNulls, -1);

                        // Copy var column data
                        Vect.memcpy(srcDataVarAddr + srcDataVarOffset + reservedBytesForColTopNulls, srcDataVarAddr, srcDataVarOffset);

                        // we need to shift copy the original column so that new block points at strings "below" the
                        // nulls we created above
                        long hiInclusive = srcDataMax - srcDataTop; // STOP. DON'T ADD +1 HERE. srcHi is inclusive, no need to do +1
                        assert srcDataFixSize >= srcDataMaxBytes + (hiInclusive + 1) * 8; // make sure enough len mapped
                        O3Utils.shiftCopyFixedSizeColumnData(
                                -reservedBytesForColTopNulls,
                                srcDataFixAddr,
                                0,
                                hiInclusive,
                                srcDataFixAddr + srcDataMaxBytes
                        );

                        // now set the "empty" bit of fixed size column with references to those
                        // null strings we just added
                        Vect.setVarColumnRefs64Bit(srcDataFixAddr + srcDataActualBytes, 0, srcDataTop);
                    }
                    srcDataTop = 0;
                    srcDataFixOffset = srcDataActualBytes;
                } else {
                    // when we are shuffling "empty" space we can just reduce column top instead
                    // of moving data
                    Unsafe.getUnsafe().putLong(colTopSinkAddr, srcDataTop);
                    srcDataFixSize = srcDataActualBytes + Long.BYTES;
                    srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                    srcDataFixOffset = 0;

                    srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES);
                    srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                    ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);
                }
            } else {
                // var index column is n+1
                srcDataFixSize = (srcDataMax + 1) * Long.BYTES;
                srcDataFixAddr = mapRW(ff, srcFixFd, srcDataFixSize, MemoryTag.MMAP_O3);
                ff.madvise(srcDataFixAddr, srcDataFixSize, Files.POSIX_MADV_SEQUENTIAL);
                srcDataFixOffset = 0;

                srcDataVarSize = Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES);
                srcDataVarAddr = mapRO(ff, srcVarFd, srcDataVarSize, MemoryTag.MMAP_O3);
                ff.madvise(srcDataVarAddr, srcDataVarSize, Files.POSIX_MADV_SEQUENTIAL);
            }

            // upgrade srcDataTop to offset
            srcDataTopOffset = srcDataTop * Long.BYTES;

            iFile(pathToPartition.trimTo(pDirNameLen), columnName, columnNameTxn);
            dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax - srcDataTop + 1) * Long.BYTES;
            dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
            if (directIoFlag) {
                ff.fadvise(dstFixFd, 0, dstFixSize, Files.POSIX_FADV_RANDOM);
            } else {
                ff.madvise(dstFixAddr, dstFixSize, Files.POSIX_MADV_RANDOM);
            }

            dFile(pathToPartition.trimTo(pDirNameLen), columnName, columnNameTxn);
            dstVarFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
            dstVarSize = srcDataVarSize - srcDataVarOffset
                    + O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr);
            dstVarAddr = mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3);
            if (directIoFlag) {
                ff.fadvise(dstVarFd, 0, dstVarSize, Files.POSIX_FADV_RANDOM);
            } else {
                ff.madvise(dstVarAddr, dstVarSize, Files.POSIX_MADV_RANDOM);
            }

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

        partCounter.set(partCount);
        publishMultiCopyTasks(
                columnCounter,
                partCounter,
                columnType,
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
                indexBlockCapacity,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                tableWriter,
                null
        );
    }

    private static void appendNewPartition(
            Path pathToPartition,
            int plen,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcDataMax,
            int indexBlockCapacity,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long columnNameTxn
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
                iFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstFixSize = (srcOooHi - srcOooLo + 1 + 1) * Long.BYTES;
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);

                dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                dstVarFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstVarSize = O3Utils.getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr);
                dstVarAddr = mapRW(ff, dstVarFd, dstVarSize, MemoryTag.MMAP_O3);
            } else {
                dFile(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                dstFixFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(Math.abs(columnType));
                dstFixAddr = mapRW(ff, dstFixFd, dstFixSize, MemoryTag.MMAP_O3);
                if (indexBlockCapacity > -1) {
                    BitmapIndexUtils.keyFileName(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    dstKFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    BitmapIndexUtils.valueFileName(pathToPartition.trimTo(plen), columnName, columnNameTxn);
                    dstVFd = openRW(ff, pathToPartition, LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                }
            }
        } catch (Throwable e) {
            LOG.error().$("append new partition error [table=").$(tableWriter.getTableName())
                    .$(", e=").$(e)
                    .I$();
            tableWriter.o3BumpErrorCount();
            final FilesFacade ff1 = tableWriter.getFilesFacade();
            O3Utils.unmapAndClose(ff1, dstFixFd, dstFixAddr, dstFixSize);
            O3Utils.unmapAndClose(ff1, dstVarFd, dstVarAddr, dstVarSize);
            O3Utils.close(ff1, dstKFd);
            O3Utils.close(ff1, dstVFd);
            if (columnCounter.decrementAndGet() == 0) {
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
            }
            throw e;
        }

        publishCopyTask(
                columnCounter,
                null,
                columnType,
                O3_BLOCK_O3,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
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
                indexBlockCapacity,
                0,
                0,
                0,
                false, // partition does not mutate above the append line
                tableWriter,
                indexWriter
        );
    }

    private static void publishMultiCopyTasks(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
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
            int indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        timestampMergeIndexSize,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
                        srcDataTopOffset >> 2,
                        indexBlockCapacity,
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
        openColumn(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
