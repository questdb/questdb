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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.O3OpenColumnJob.*;
import static io.questdb.cairo.O3Utils.get8ByteBuf;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3PartitionJob extends AbstractQueueConsumerJob<O3PartitionTask> {

    private static final Log LOG = LogFactory.getLog(O3PartitionJob.class);

    public O3PartitionJob(MessageBus messageBus) {
        super(messageBus.getO3PartitionQueue(), messageBus.getO3PartitionSubSeq());
    }

    public static void processPartition(
            CharSequence pathToTable,
            int partitionBy,
            ObjList<MemoryMAR> columns,
            ObjList<MemoryCARW> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long o3TimestampMin,
            long o3TimestampMax,
            long partitionTimestamp,
            long maxTimestamp,
            long srcDataMax,
            long srcDataTxn,
            boolean last,
            long txn,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long tmpBuf
    ) {
        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and write to existing file descriptors
        final long o3TimestampLo = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
        final RecordMetadata metadata = tableWriter.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, partitionBy, o3TimestampLo, false);
        final int pplen = path.length();
        TableUtils.txnPartitionConditionally(path, srcDataTxn);
        final int plen = path.length();
        long srcTimestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        final FilesFacade ff = tableWriter.getFilesFacade();

        if (srcDataMax < 1) {

            // this has to be a brand new partition for either of two cases:
            // - this partition is above min partition of the table
            // - this partition is below max partition of the table
            // - this is last partition that is empty
            // pure OOO data copy into new partition

            if (!last) {
                try {
                    LOG.debug().$("would create [path=").$(path.chop$().slash$()).$(']').$();
                    createDirsOrFail(ff, path, tableWriter.getConfiguration().getMkDirMode());
                } catch (Throwable e) {
                    LOG.error().$("process new partition error [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount();
                    tableWriter.o3ClockDownPartitionUpdateCount();
                    tableWriter.o3CountDownDoneLatch();
                    throw e;
                }
            }

            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    o3TimestampMax,
                    o3TimestampLo,
                    partitionTimestamp,
                    // below parameters are unused by this type of append
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
                    0,
                    srcDataTxn,
                    OPEN_NEW_PARTITION_FOR_APPEND,
                    0,  // timestamp fd
                    0,
                    0,
                    timestampIndex,
                    sortedTimestampsAddr,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    tmpBuf
            );
        } else {
            long srcTimestampAddr = 0;
            long srcTimestampSize = 0;
            int prefixType;
            long prefixLo;
            long prefixHi;
            int mergeType;
            long mergeDataLo;
            long mergeDataHi;
            long mergeO3Lo;
            long mergeO3Hi;
            int suffixType;
            long suffixLo;
            long suffixHi;
            final int openColumnMode;

            try {
                // out of order is hitting existing partition
                // partitionTimestamp is in fact a ceil of ooo timestamp value for the given partition
                // so this check is for matching ceilings
                if (last) {
                    dataTimestampHi = maxTimestamp;
                    srcTimestampSize = srcDataMax * 8L;
                    // negative fd indicates descriptor reuse
                    srcTimestampFd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                    srcTimestampAddr = mapRW(ff, -srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                } else {
                    srcTimestampSize = srcDataMax * 8L;
                    // out of order data is going into archive partition
                    // we need to read "low" and "high" boundaries of the partition. "low" being oldest timestamp
                    // and "high" being newest

                    dFile(path.trimTo(plen), metadata.getColumnName(timestampIndex));

                    // also track the fd that we need to eventually close
                    srcTimestampFd = openRW(ff, path, LOG);
                    srcTimestampAddr = mapRW(ff, srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                    dataTimestampHi = Unsafe.getUnsafe().getLong(srcTimestampAddr + srcTimestampSize - Long.BYTES);
                }
                dataTimestampLo = Unsafe.getUnsafe().getLong(srcTimestampAddr);


                // create copy jobs
                // we will have maximum of 3 stages:
                // - prefix data
                // - merge job
                // - suffix data
                //
                // prefix and suffix can be sourced either from OO fully or from Data (written to disk) fully
                // so for prefix and suffix we will need a flag indicating source of the data
                // as well as range of rows in that source

                prefixType = O3_BLOCK_NONE;
                prefixLo = -1;
                prefixHi = -1;
                mergeType = O3_BLOCK_NONE;
                mergeDataLo = -1;
                mergeDataHi = -1;
                mergeO3Lo = -1;
                mergeO3Hi = -1;
                suffixType = O3_BLOCK_NONE;
                suffixLo = -1;
                suffixHi = -1;

                assert srcTimestampFd != -1 && srcTimestampFd != 1;

                int branch;

                if (o3TimestampLo > dataTimestampLo) {
                    //   +------+
                    //   | data |  +-----+
                    //   |      |  | OOO |
                    //   |      |  |     |

                    if (o3TimestampLo >= dataTimestampHi) {

                        // +------+
                        // | data |
                        // |      |
                        // +------+
                        //
                        //           +-----+
                        //           | OOO |
                        //           |     |
                        //
                        branch = 1;
                        suffixType = O3_BLOCK_O3;
                        suffixLo = srcOooLo;
                        suffixHi = srcOooHi;
                    } else {

                        //
                        // +------+
                        // |      |
                        // |      | +-----+
                        // | data | | OOO |
                        // +------+

                        prefixType = O3_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo,
                                0,
                                srcDataMax - 1,
                                BinarySearch.SCAN_DOWN
                        );
                        mergeDataLo = prefixHi + 1;
                        mergeO3Lo = srcOooLo;

                        if (o3TimestampMax < dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | +-----+
                            // +------+

                            branch = 2;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampMax - 1,
                                    mergeDataLo,
                                    srcDataMax - 1,
                                    BinarySearch.SCAN_DOWN
                            );

                            if (mergeDataLo > mergeDataHi) {
                                // the OO data implodes right between rows of existing data
                                // so we will have both data prefix and suffix and the middle bit

                                // is the out of order
                                mergeType = O3_BLOCK_O3;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            suffixType = O3_BLOCK_DATA;
                            suffixLo = mergeDataHi + 1;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi;

                        } else if (o3TimestampMax > dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ |     |
                            //          |     |
                            //          +-----+

                            branch = 3;
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi,
                                    srcOooLo,
                                    srcOooHi,
                                    BinarySearch.SCAN_UP
                            );

                            mergeDataHi = srcDataMax - 1;

                            mergeType = O3_BLOCK_MERGE;
                            suffixType = O3_BLOCK_O3;
                            suffixLo = mergeO3Hi + 1;
                            suffixHi = srcOooHi;
                        } else {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ +-----+
                            //

                            branch = 4;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                        }
                    }
                } else {

                    //            +-----+
                    //            | OOO |
                    //
                    //  +------+
                    //  | data |


                    prefixType = O3_BLOCK_O3;
                    prefixLo = srcOooLo;
                    if (dataTimestampLo < o3TimestampMax) {

                        //
                        //  +------+  | OOO |
                        //  | data |  +-----+
                        //  |      |

                        mergeDataLo = 0;
                        prefixHi = Vect.boundedBinarySearchIndexT(
                                sortedTimestampsAddr,
                                dataTimestampLo,
                                srcOooLo,
                                srcOooHi,
                                BinarySearch.SCAN_DOWN
                        );
                        mergeO3Lo = prefixHi + 1;

                        if (o3TimestampMax < dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | +-----+
                            // |      |
                            // +------+

                            branch = 5;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampMax,
                                    0,
                                    srcDataMax - 1,
                                    BinarySearch.SCAN_DOWN
                            );

                            suffixLo = mergeDataHi + 1;
                            suffixType = O3_BLOCK_DATA;
                            suffixHi = srcDataMax - 1;

                        } else if (o3TimestampMax > dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ |     |
                            //          +-----+

                            branch = 6;
                            mergeDataHi = srcDataMax - 1;
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi - 1,
                                    mergeO3Lo,
                                    srcOooHi,
                                    BinarySearch.SCAN_DOWN
                            );

                            if (mergeO3Lo > mergeO3Hi) {
                                mergeType = O3_BLOCK_DATA;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            if (mergeO3Hi < srcOooHi) {
                                suffixLo = mergeO3Hi + 1;
                                suffixType = O3_BLOCK_O3;
                                suffixHi = Math.max(suffixLo, srcOooHi);
                            }
                        } else {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ +-----+

                            branch = 7;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                        }
                    } else {
                        //            +-----+
                        //            | OOO |
                        //            +-----+
                        //
                        //  +------+
                        //  | data |
                        //
                        branch = 8;
                        prefixHi = srcOooHi;
                        suffixType = O3_BLOCK_DATA;
                        suffixLo = 0;
                        suffixHi = srcDataMax - 1;
                    }
                }

                LOG.debug()
                        .$("o3 merge [branch=").$(branch)
                        .$(", prefixType=").$(prefixType)
                        .$(", prefixLo=").$(prefixLo)
                        .$(", prefixHi=").$(prefixHi)
                        .$(", o3TimestampLo=").$ts(o3TimestampLo)
                        .$(", o3TimestampMin=").$ts(o3TimestampMin)
                        .$(", o3TimestampMax=").$ts(o3TimestampMax)
                        .$(", dataTimestampLo=").$ts(dataTimestampLo)
                        .$(", dataTimestampHi=").$ts(dataTimestampHi)
                        .$(", partitionTimestamp=").$ts(partitionTimestamp)
                        .$(", srcDataMax=").$(srcDataMax)
                        .$(", mergeType=").$(mergeType)
                        .$(", mergeDataLo=").$(mergeDataLo)
                        .$(", mergeDataHi=").$(mergeDataHi)
                        .$(", mergeO3Lo=").$(mergeO3Lo)
                        .$(", mergeO3Hi=").$(mergeO3Hi)
                        .$(", suffixType=").$(suffixType)
                        .$(", suffixLo=").$(suffixLo)
                        .$(", suffixHi=").$(suffixHi)
                        .$(", table=").$(pathToTable)
                        .I$();

                if (prefixType == O3_BLOCK_NONE) {
                    // We do not need to create a copy of partition when we simply need to append
                    // existing the one.
                    openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                } else {
                    txnPartition(path.trimTo(pplen), txn);
                    createDirsOrFail(ff, path.slash$(), tableWriter.getConfiguration().getMkDirMode());
                    if (last) {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_MERGE;
                    } else {
                        openColumnMode = OPEN_MID_PARTITION_FOR_MERGE;
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("process existing partition error [table=").$(tableWriter.getTableName())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                tableWriter.o3BumpErrorCount();
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
                throw e;
            }

            // Compute max timestamp as maximum of out of order data and
            // data in existing partition.
            // When partition is new, the data timestamp is MIN_LONG
            final long timestampMax = Math.max(o3TimestampMax, dataTimestampHi);

            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    timestampMax, // <-- this is max of OOO and data chunk
                    o3TimestampLo,
                    partitionTimestamp,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeDataLo,
                    mergeDataHi,
                    mergeO3Lo,
                    mergeO3Hi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    srcDataMax,
                    srcDataTxn,
                    openColumnMode,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    timestampIndex,
                    sortedTimestampsAddr,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    tmpBuf
            );
        }
    }

    public static void processPartition(
            long tmpBuf,
            O3PartitionTask task,
            long cursor,
            Sequence subSeq
    ) {
        // find "current" partition boundary in the out of order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        final CharSequence pathToTable = task.getPathToTable();
        final int partitionBy = task.getPartitionBy();
        final ObjList<MemoryMAR> columns = task.getColumns();
        final ObjList<MemoryCARW> oooColumns = task.getO3Columns();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long oooTimestampMax = task.getOooTimestampMax();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long maxTimestamp = task.getMaxTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcDataTxn = task.getSrcNameTxn();
        final boolean last = task.isLast();
        final long txn = task.getTxn();
        final long sortedTimestampsAddr = task.getSortedTimestampsAddr();
        final TableWriter tableWriter = task.getTableWriter();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final O3Basket o3Basket = task.getO3Basket();

        subSeq.done(cursor);

        processPartition(
                pathToTable,
                partitionBy,
                columns,
                oooColumns,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                oooTimestampMax,
                partitionTimestamp,
                maxTimestamp,
                srcDataMax,
                srcDataTxn,
                last,
                txn,
                sortedTimestampsAddr,
                tableWriter,
                columnCounter,
                o3Basket,
                tmpBuf
        );
    }

    private static long createMergeIndex(
            long srcDataTimestampAddr,
            long sortedTimestampsAddr,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi
    ) {
        // Create "index" for existing timestamp column. When we reshuffle timestamps during merge we will
        // have to go back and find data rows we need to move accordingly
        final long indexSize = (mergeDataHi - mergeDataLo + 1) * TIMESTAMP_MERGE_ENTRY_BYTES;
        assert indexSize > 0; // avoid SIGSEGV
        final long index = Unsafe.malloc(indexSize, MemoryTag.NATIVE_O3);
        Vect.makeTimestampIndex(srcDataTimestampAddr, mergeDataLo, mergeDataHi, index);
        final long result = Vect.mergeTwoLongIndexesAsc(
                index,
                mergeDataHi - mergeDataLo + 1,
                sortedTimestampsAddr + mergeOOOLo * 16,
                mergeOOOHi - mergeOOOLo + 1
        );
        Unsafe.free(index, indexSize, MemoryTag.NATIVE_O3);
        return result;
    }

    private static void publishOpenColumnTaskHarmonized(
            long cursor,
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
            long oooTimestampMin,
            long oooTimestampMax,
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
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long activeFixFd,
            long activeVarFd,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        final O3OpenColumnTask openColumnTask = tableWriter.getO3OpenColumnQueue().get(cursor);
        openColumnTask.of(
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
                oooTimestampMin,
                oooTimestampMax,
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
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
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
                indexWriter
        );
        tableWriter.getO3OpenColumnPubSeq().done(cursor);
    }

    private static void publishOpenColumnTasks(
            long txn,
            ObjList<MemoryMAR> columns,
            ObjList<MemoryCARW> oooColumns,
            CharSequence pathToTable,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampLo,
            long partitionTimestamp,
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
            long srcDataMax,
            long srcDataTxn,
            int openColumnMode,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int timestampIndex,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long tmpBuf
    ) {
        LOG.debug().$("partition [ts=").$ts(oooTimestampLo).$(']').$();

        final long timestampMergeIndexAddr;
        if (mergeType == O3_BLOCK_MERGE) {
            timestampMergeIndexAddr = createMergeIndex(
                    srcTimestampAddr,
                    sortedTimestampsAddr,
                    mergeDataLo,
                    mergeDataHi,
                    mergeOOOLo,
                    mergeOOOHi
            );
        } else {
            timestampMergeIndexAddr = 0;
        }

        final RecordMetadata metadata = tableWriter.getMetadata();
        final int columnCount = metadata.getColumnCount();
        columnCounter.set(columnCount);
        int columnsInFlight = columnCount;
        try {
            for (int i = 0; i < columnCount; i++) {
                final int colOffset = TableWriter.getPrimaryColumnIndex(i);
                final boolean notTheTimestamp = i != timestampIndex;
                final int columnType = metadata.getColumnType(i);
                final MemoryARW oooMem1 = oooColumns.getQuick(colOffset);
                final MemoryARW oooMem2 = oooColumns.getQuick(colOffset + 1);
                final MemoryMA mem1 = columns.getQuick(colOffset);
                final MemoryMA mem2 = columns.getQuick(colOffset + 1);
                final long activeFixFd;
                final long activeVarFd;
                final long srcDataTop;
                final long srcOooFixAddr;
                final long srcOooVarAddr;
                if (!ColumnType.isVariableLength(columnType)) {
                    activeFixFd = mem1.getFd();
                    activeVarFd = 0;
                    srcOooFixAddr = oooMem1.addressOf(0);
                    srcOooVarAddr = 0;
                } else {
                    activeFixFd = mem2.getFd();
                    activeVarFd = mem1.getFd();
                    srcOooFixAddr = oooMem2.addressOf(0);
                    srcOooVarAddr = oooMem1.addressOf(0);
                }

                final CharSequence columnName = metadata.getColumnName(i);
                final boolean isIndexed = metadata.isColumnIndexed(i);
                if (openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND || openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                    srcDataTop = tableWriter.getColumnTop(i);
                } else {
                    srcDataTop = -1; // column open job will have to find out if top exists and its value
                }

                final BitmapIndexWriter indexWriter;
                if (isIndexed) {
                    indexWriter = o3Basket.nextIndexer();
                } else {
                    indexWriter = null;
                }

                try {
                    final long cursor = tableWriter.getO3OpenColumnPubSeq().next();
                    if (cursor > -1) {
                        publishOpenColumnTaskHarmonized(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                oooTimestampMax,
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
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
                                suffixType,
                                suffixLo,
                                suffixHi,
                                isIndexed,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                activeFixFd,
                                activeVarFd,
                                tableWriter,
                                indexWriter
                        );
                    } else {
                        publishOpenColumnTaskContended(
                                tmpBuf,
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                oooTimestampMax,
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
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
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
                                indexWriter
                        );
                    }
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount();
                    LOG.error().$("open column error [table=").$(tableWriter.getTableName())
                            .$(", e=").$(e)
                            .I$();
                    columnsInFlight = i + 1;
                    throw e;
                }
            }
        } finally {
            final int delta = columnsInFlight - columnCount;
            LOG.debug().$("idle [delta=").$(delta).$(']').$();
            if (delta < 0 && columnCounter.addAndGet(delta) == 0) {
                O3CopyJob.closeColumnIdleQuick(
                        timestampMergeIndexAddr,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
            }
        }
    }

    private static void publishOpenColumnTaskContended(
            long tmpBuf,
            long cursor,
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
            long oooTimestampMin,
            long oooTimestampMax,
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
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
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
            BitmapIndexWriter indexWriter
    ) {
        while (cursor == -2) {
            cursor = tableWriter.getO3OpenColumnPubSeq().next();
        }

        if (cursor > -1) {
            publishOpenColumnTaskHarmonized(
                    cursor,
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
                    oooTimestampMin,
                    oooTimestampMax,
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
                    mergeDataLo,
                    mergeDataHi,
                    mergeOOOLo,
                    mergeOOOHi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    isIndexed,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    activeFixFd,
                    activeVarFd,
                    tableWriter,
                    indexWriter
            );
        } else {
            O3OpenColumnJob.openColumn(
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
                    oooTimestampMin,
                    oooTimestampMax,
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
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        processPartition(workerId + 1, queue.get(cursor), cursor, subSeq);
        return true;
    }

    private void processPartition(int workerId, O3PartitionTask task, long cursor, Sequence subSeq) {
        processPartition(get8ByteBuf(workerId), task, cursor, subSeq);
    }
}
