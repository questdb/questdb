/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
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
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3PartitionJob extends AbstractQueueConsumerJob<O3PartitionTask> {

    private static final Log LOG = LogFactory.getLog(O3PartitionJob.class);

    public O3PartitionJob(MessageBus messageBus) {
        super(messageBus.getO3PartitionQueue(), messageBus.getO3PartitionSubSeq());
    }

    public static void processPartition(
            Path pathToTable,
            int partitionBy,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long o3TimestampMin,
            long partitionTimestamp,
            long maxTimestamp,
            long srcDataMax,
            long srcNameTxn,
            boolean last,
            long txn,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            final long newPartitionSize,
            final long oldPartitionSize,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr
    ) {
        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and write to existing file descriptors
        final long o3TimestampLo = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
        final long o3TimestampHi = getTimestampIndexValue(sortedTimestampsAddr, srcOooHi);
        final RecordMetadata metadata = tableWriter.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        final Path path = Path.getThreadLocal(pathToTable);

        int srcTimestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        final FilesFacade ff = tableWriter.getFilesFacade();
        long oldPartitionTimestamp;

        // partition might be subject to split
        // if this happens the size of the original (srcDataPartition) partition will decrease
        // and the size of new (o3Partition) will be non-zero
        long srcDataNewPartitionSize = newPartitionSize;

        long o3SplitPartitionSize = 0;

        if (srcDataMax < 1) {

            // This has to be a brand-new partition for any of three cases:
            // - This partition is above min partition of the table.
            // - This partition is below max partition of the table.
            // - This is last partition that is empty.
            // pure OOO data copy into new partition

            if (!last) {
                try {
                    LOG.debug().$("would create [path=").$(path.slash$()).I$();
                    TableUtils.setPathForPartition(path.trimTo(pathToTable.size()), partitionBy, partitionTimestamp, txn - 1);
                    createDirsOrFail(ff, path.slash(), tableWriter.getConfiguration().getMkDirMode());
                } catch (Throwable e) {
                    LOG.error().$("process new partition error [table=").utf8(tableWriter.getTableToken().getTableName())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    tableWriter.o3ClockDownPartitionUpdateCount();
                    tableWriter.o3CountDownDoneLatch();
                    throw e;
                }
            }

            assert oldPartitionSize == 0;

            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    o3TimestampLo,
                    partitionTimestamp,
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
                    srcNameTxn,
                    OPEN_NEW_PARTITION_FOR_APPEND,
                    0,  // timestamp fd
                    0,
                    0,
                    timestampIndex,
                    sortedTimestampsAddr,
                    newPartitionSize,
                    oldPartitionSize,
                    0,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
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

                    TableUtils.setPathForPartition(path.trimTo(pathToTable.size()), partitionBy, partitionTimestamp, srcNameTxn);

                    // also track the fd that we need to eventually close
                    // Open src timestamp column as RW in case append happens
                    srcTimestampFd = openRW(ff, dFile(path, metadata.getColumnName(timestampIndex), COLUMN_NAME_TXN_NONE), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
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

                // When deduplication is enabled, we want to take into the merge
                // the rows from the partition which equals to the O3 min and O3 max.
                // Without taking equal rows, deduplication will be incorrect.
                // Without deduplication, taking timestamp == o3TimestampHi into merge
                // can result into unnecessary partition rewrites, when instead of appending
                // rows with equal timestamp a merge is triggered.
                long mergeEquals = tableWriter.isDeduplicationEnabled() ? 1 : 0;

                if (o3TimestampLo >= dataTimestampLo) {
                    //   +------+
                    //   | data |  +-----+
                    //   |      |  | OOO |
                    //   |      |  |     |

                    // When deduplication is enabled, take into the merge the rows which are equals
                    // to the dataTimestampHi in the else block
                    if (o3TimestampLo >= dataTimestampHi + mergeEquals) {

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
                        // When deduplication is enabled, take into the merge the rows which are equals
                        // to the o3TimestampLo in the else block, e.g. reduce the prefix size
                        prefixHi = Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo - mergeEquals,
                                0,
                                srcDataMax - 1,
                                BinarySearch.SCAN_DOWN
                        );
                        mergeDataLo = prefixHi + 1;
                        mergeO3Lo = srcOooLo;

                        if (o3TimestampHi < dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | +-----+
                            // +------+

                            branch = 2;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    mergeDataLo,
                                    srcDataMax - 1,
                                    BinarySearch.SCAN_DOWN
                            );
                            assert mergeDataHi > -1;

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

                        } else if (o3TimestampHi > dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ |     |
                            //          |     |
                            //          +-----+

                            branch = 3;
                            // When deduplication is enabled, take in to the merge
                            // all OOO rows that are equal to the last row in the data
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi,
                                    srcOooLo,
                                    srcOooHi,
                                    tableWriter.isDeduplicationEnabled() ? BinarySearch.SCAN_DOWN : BinarySearch.SCAN_UP
                            );

                            mergeDataHi = srcDataMax - 1;
                            assert mergeDataLo <= mergeDataHi;

                            mergeType = O3_BLOCK_MERGE;
                            suffixType = O3_BLOCK_O3;
                            suffixLo = mergeO3Hi + 1;
                            suffixHi = srcOooHi;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
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
                            assert mergeDataLo <= mergeDataHi;
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
                    if (dataTimestampLo <= o3TimestampHi) {

                        //
                        //  +------+  | OOO |
                        //  | data |  +-----+
                        //  |      |

                        mergeDataLo = 0;
                        // To make inserts stable o3 rows with timestamp == dataTimestampLo
                        // should go into the merge section.
                        prefixHi = Vect.boundedBinarySearchIndexT(
                                sortedTimestampsAddr,
                                dataTimestampLo - 1,
                                srcOooLo,
                                srcOooHi,
                                BinarySearch.SCAN_DOWN
                        );
                        mergeO3Lo = prefixHi + 1;

                        if (o3TimestampHi < dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | +-----+
                            // |      |
                            // +------+

                            branch = 5;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            // To make inserts stable table rows with timestamp == o3TimestampHi
                            // should go into the merge section.
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    0,
                                    srcDataMax - 1,
                                    BinarySearch.SCAN_DOWN
                            );

                            suffixLo = mergeDataHi + 1;
                            suffixType = O3_BLOCK_DATA;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
                        } else if (o3TimestampHi > dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ |     |
                            //          +-----+

                            branch = 6;
                            mergeDataHi = srcDataMax - 1;
                            // To deduplicate O3 rows with timestamp == dataTimestampHi
                            // should go into the merge section.
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi - 1 + mergeEquals,
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
                        .$(", o3TimestampHi=").$ts(o3TimestampHi)
                        .$(", o3TimestampMin=").$ts(o3TimestampMin)
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

                oldPartitionTimestamp = partitionTimestamp;
                boolean partitionSplit = false;

                if (
                        prefixType == O3_BLOCK_DATA
                                && prefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                && prefixHi > 2 * (mergeDataHi - mergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                ) {
                    // large prefix copy, better to split the partition
                    long maxSourceTimestamp = Unsafe.getUnsafe().getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                    assert maxSourceTimestamp <= o3TimestampLo;
                    boolean canSplit = true;

                    if (maxSourceTimestamp == o3TimestampLo) {
                        // We cannot split the partition if existing data has timestamp with exactly same value
                        // because 2 partition parts cannot have data with exactly same timestamp.
                        // To make this work, we can reduce the prefix by the size of the rows which equals to o3TimestampLo.
                        long newPrefixHi = -1 + Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo,
                                prefixLo,
                                prefixHi - 1,
                                BinarySearch.SCAN_UP
                        );

                        if (newPrefixHi > -1L) {
                            long shiftLeft = prefixHi - newPrefixHi;
                            long newMergeDataLo = mergeDataLo - shiftLeft;
                            // Check that splitting still makes sense
                            if (newPrefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                    && newPrefixHi > 2 * (mergeDataHi - newMergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                            ) {
                                prefixHi = newPrefixHi;
                                mergeDataLo = newMergeDataLo;
                                maxSourceTimestamp = Unsafe.getUnsafe().getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                                mergeType = O3_BLOCK_MERGE;
                                assert maxSourceTimestamp < o3TimestampLo;
                            } else {
                                canSplit = false;
                            }
                        } else {
                            canSplit = false;
                        }
                    }

                    if (canSplit) {
                        partitionSplit = true;
                        partitionTimestamp = maxSourceTimestamp + 1;
                        prefixType = O3_BLOCK_NONE;

                        // we are splitting existing partition along the "prefix" line
                        // this action creates two partitions:
                        // 1. Prefix of the srcDataPartition
                        // 2. Merge and suffix of srcDataPartition

                        // size of old data partition will be reduced
                        srcDataNewPartitionSize = prefixHi + 1;

                        // size of the new partition, old (0) and new
                        o3SplitPartitionSize = newPartitionSize - srcDataNewPartitionSize;

                        // large prefix copy, better to split the partition
                        LOG.info().$("o3 split partition [table=").$(tableWriter.getTableToken())
                                .$(", timestamp=").$ts(oldPartitionTimestamp)
                                .$(", nameTxn=").$(srcNameTxn)
                                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                                .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                                .$(", newPartitionTimestamp=").$ts(partitionTimestamp)
                                .$(", nameTxn=").$(txn)
                                .I$();
                    }
                }

                if (!partitionSplit && prefixType == O3_BLOCK_NONE) {
                    // We do not need to create a copy of partition when we simply need to append
                    // to the existing one.
                    openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                } else {
                    TableUtils.setPathForPartition(path.trimTo(pathToTable.size()), partitionBy, partitionTimestamp, txn);
                    createDirsOrFail(ff, path.slash(), tableWriter.getConfiguration().getMkDirMode());
                    if (last) {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_MERGE;
                    } else {
                        openColumnMode = OPEN_MID_PARTITION_FOR_MERGE;
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("process existing partition error [table=").utf8(tableWriter.getTableToken().getTableName())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
                throw e;
            }

            // Compute max timestamp as maximum of out of order data and
            // data in existing partition.
            // When partition is new, the data timestamp is MIN_LONG

            Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    o3TimestampLo,
                    partitionTimestamp,
                    oldPartitionTimestamp,
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
                    srcNameTxn,
                    openColumnMode,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    timestampIndex,
                    sortedTimestampsAddr,
                    srcDataNewPartitionSize,
                    oldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
            );
        }
    }

    public static void processPartition(
            O3PartitionTask task,
            long cursor,
            Sequence subSeq
    ) {
        // find "current" partition boundary in the out-of-order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        final Path pathToTable = task.getPathToTable();
        final int partitionBy = task.getPartitionBy();
        final ObjList<MemoryMA> columns = task.getColumns();
        final ReadOnlyObjList<? extends MemoryCR> oooColumns = task.getO3Columns();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long maxTimestamp = task.getMaxTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcNameTxn = task.getSrcNameTxn();
        final boolean last = task.isLast();
        final long txn = task.getTxn();
        final long sortedTimestampsAddr = task.getSortedTimestampsAddr();
        final TableWriter tableWriter = task.getTableWriter();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final O3Basket o3Basket = task.getO3Basket();
        final long newPartitionSize = task.getNewPartitionSize();
        final long oldPartitionSize = task.getOldPartitionSize();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();
        final long dedupColSinkAddr = task.getDedupColSinkAddr();

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
                partitionTimestamp,
                maxTimestamp,
                srcDataMax,
                srcNameTxn,
                last,
                txn,
                sortedTimestampsAddr,
                tableWriter,
                columnCounter,
                o3Basket,
                newPartitionSize,
                oldPartitionSize,
                partitionUpdateSinkAddr,
                dedupColSinkAddr
        );
    }

    private static long createMergeIndex(
            long srcDataTimestampAddr,
            long sortedTimestampsAddr,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            long indexSize
    ) {
        // Create "index" for existing timestamp column. When we reshuffle timestamps during merge we will
        // have to go back and find data rows we need to move accordingly
        long timestampIndexAddr = Unsafe.malloc(indexSize, MemoryTag.NATIVE_O3);
        Vect.mergeTwoLongIndexesAsc(
                srcDataTimestampAddr,
                mergeDataLo,
                mergeDataHi - mergeDataLo + 1,
                sortedTimestampsAddr + mergeOOOLo * 16,
                mergeOOOHi - mergeOOOLo + 1,
                timestampIndexAddr
        );
        return timestampIndexAddr;
    }

    private static long getDedupRows(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        if (dedupCommitAddresses == null || dedupCommitAddresses.getColumnCount() == 0) {
            return Vect.mergeDedupTimestampWithLongIndexAsc(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr
            );
        } else {
            return getDedupRowsWithAdditionalKeys(
                    partitionTimestamp,
                    srcNameTxn,
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    oooColumns,
                    dedupCommitAddresses,
                    dedupColSinkAddr,
                    tableWriter,
                    tableRootPath,
                    tempIndexAddr
            );
        }
    }

    private static long getDedupRowsWithAdditionalKeys(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        LOG.info().$("merge dedup with additional keys [table=").$(tableWriter.getTableToken())
                .$(", columnRowCount=").$(mergeDataHi - mergeDataLo + 1)
                .$(", o3RowCount=").$(mergeOOOHi - mergeOOOLo + 1)
                .I$();
        TableRecordMetadata metadata = tableWriter.getMetadata();
        int dedupColumnIndex = 0;
        int tableRootPathLen = tableRootPath.size();
        FilesFacade ff = tableWriter.getFilesFacade();

        int mapMemTag = MemoryTag.MMAP_O3;
        try {
            dedupCommitAddresses.clear(dedupColSinkAddr);
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0 && metadata.isDedupKey(i) && i != metadata.getTimestampIndex()) {
                    final int columnSize = ColumnType.sizeOf(columnType);

                    final long columnTop = tableWriter.getColumnTop(partitionTimestamp, i, mergeDataHi + 1);
                    final int fd;
                    final long mapSize, mappedAddress;

                    if (columnTop < mergeDataLo + 1) {
                        CharSequence columnName = metadata.getColumnName(i);
                        long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, i);
                        TableUtils.setSinkForPartition(tableRootPath.trimTo(tableRootPathLen).slash(), tableWriter.getPartitionBy(), partitionTimestamp, srcNameTxn);
                        TableUtils.dFile(tableRootPath, columnName, columnNameTxn);
                        fd = TableUtils.openRO(ff, tableRootPath.$(), LOG);

                        mapSize = (mergeDataHi + 1 - columnTop) * columnSize;
                        mappedAddress = TableUtils.mapAppendColumnBuffer(
                                ff,
                                fd,
                                0,
                                mapSize,
                                false,
                                mapMemTag
                        );
                    } else {
                        // column is all nulls because of column top
                        fd = -1;
                        mapSize = 0;
                        mappedAddress = 0;
                    }

                    final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                    dedupCommitAddresses.setArrayValues(
                            dedupColSinkAddr,
                            dedupColumnIndex,
                            columnType,
                            columnSize,
                            columnTop,
                            mappedAddress - columnTop * columnSize,
                            oooColAddress,
                            mappedAddress,
                            mapSize,
                            fd
                    );
                    dedupColumnIndex++;
                }
            }

            return Vect.mergeDedupTimestampWithLongIndexIntKeys(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr,
                    dedupCommitAddresses.getColumnCount(),
                    dedupCommitAddresses.getAddress(dedupColSinkAddr)
            );
        } finally {
            for (int i = 0, n = dedupCommitAddresses.getColumnCount(); i < n; i++) {
                final long mappedAddress = dedupCommitAddresses.getColReserved1(dedupColSinkAddr, i);
                final long mappedAddressSize = dedupCommitAddresses.getColReserved2(dedupColSinkAddr, i);
                if (mappedAddressSize > 0) {
                    TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, 0, mappedAddressSize, mapMemTag);
                }
                final int fd = (int) dedupCommitAddresses.getColReserved3(dedupColSinkAddr, i);
                if (fd > 0) {
                    ff.close(fd);
                }
            }
        }
    }

    private static void publishOpenColumnTaskContended(
            long cursor,
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
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
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
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int indexBlockCapacity,
            int activeFixFd,
            int activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
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
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
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
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    activeFixFd,
                    activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
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
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
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
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
            );
        }
    }

    private static void publishOpenColumnTaskHarmonized(
            long cursor,
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
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
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
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int activeFixFd,
            int activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
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
                timestampMergeIndexSize,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                partitionTimestamp,
                oldPartitionTimestamp,
                srcDataTop,
                srcDataMax,
                srcNameTxn,
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
                indexBlockCapacity,
                activeFixFd,
                activeVarFd,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr,
                columnIndex,
                columnNameTxn
        );
        tableWriter.getO3OpenColumnPubSeq().done(cursor);
    }

    private static void publishOpenColumnTasks(
            long txn,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            Path pathToTable,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampLo,
            long partitionTimestamp,
            long oldPartitionTimestamp,
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
            long srcNameTxn,
            int openColumnMode,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int timestampIndex,
            long sortedTimestampsAddr,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr
    ) {
        // Number of rows to insert from the O3 segment into this partition.
        final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

        tableWriter.addPhysicallyWrittenRows(
                isOpenColumnModeForAppend(openColumnMode)
                        ? srcOooBatchRowSize
                        : o3SplitPartitionSize == 0 ? srcDataNewPartitionSize : o3SplitPartitionSize
        );

        LOG.debug().$("partition [ts=").$ts(oooTimestampLo).I$();

        final long timestampMergeIndexAddr;
        final long timestampMergeIndexSize;
        final TableRecordMetadata metadata = tableWriter.getMetadata();
        if (mergeType == O3_BLOCK_MERGE) {
            long mergeRowCount = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
            long tempIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
            assert tempIndexSize > 0; // avoid SIGSEGV

            try {
                if (!tableWriter.isDeduplicationEnabled()) {
                    timestampMergeIndexSize = tempIndexSize;

                    timestampMergeIndexAddr = createMergeIndex(
                            srcTimestampAddr,
                            sortedTimestampsAddr,
                            mergeDataLo,
                            mergeDataHi,
                            mergeOOOLo,
                            mergeOOOHi,
                            timestampMergeIndexSize
                    );
                } else {
                    final long tempIndexAddr = Unsafe.malloc(tempIndexSize, MemoryTag.NATIVE_O3);
                    final DedupColumnCommitAddresses dedupCommitAddresses = tableWriter.getDedupCommitAddresses();
                    final Path tempTablePath = Path.getThreadLocal(tableWriter.getConfiguration().getRoot()).concat(tableWriter.getTableToken());

                    final long dedupRows = getDedupRows(
                            oldPartitionTimestamp,
                            srcNameTxn,
                            srcTimestampAddr,
                            mergeDataLo,
                            mergeDataHi,
                            sortedTimestampsAddr,
                            mergeOOOLo,
                            mergeOOOHi,
                            oooColumns,
                            dedupCommitAddresses,
                            dedupColSinkAddr,
                            tableWriter,
                            tempTablePath,
                            tempIndexAddr
                    );
                    timestampMergeIndexSize = dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES;
                    timestampMergeIndexAddr = Unsafe.realloc(tempIndexAddr, tempIndexSize, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                    final long duplicateCount = mergeRowCount - dedupRows;
                    if (duplicateCount > 0) {
                        // we could be de-duping a split partition
                        // in which case only its size will be affected
                        if (o3SplitPartitionSize > 0) {
                            o3SplitPartitionSize -= duplicateCount;
                        } else {
                            srcDataNewPartitionSize -= duplicateCount;
                        }
                        LOG.info()
                                .$("dedup row reduction [table=").utf8(tableWriter.getTableToken().getTableName())
                                .$(", partition=").$ts(partitionTimestamp)
                                .$(", duplicateCount=").$(duplicateCount)
                                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                                .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                                .$(", o3SplitPartitionSize=").$(srcDataOldPartitionSize)
                                .$(", mergeDataLo=").$(mergeDataLo)
                                .$(", mergeDataHi=").$(mergeDataHi)
                                .$(", mergeOOOLo=").$(mergeOOOLo)
                                .$(", mergeOOOHi=").$(mergeOOOHi)
                                .I$();
                    }
                }
            } catch (Throwable e) {
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                LOG.error().$("open column error [table=").utf8(tableWriter.getTableToken().getTableName())
                        .$(", e=").$(e)
                        .I$();
                O3CopyJob.closeColumnIdleQuick(
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
                throw e;
            }
        } else {
            timestampMergeIndexAddr = 0;
            timestampMergeIndexSize = 0;
        }

        final int columnCount = metadata.getColumnCount();
        columnCounter.set(compressColumnCount(metadata));
        int columnsInFlight = columnCount;
        if (openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE || openColumnMode == OPEN_MID_PARTITION_FOR_MERGE) {
            // Partition will be re-written. Jobs will set new column top values but by default they are 0
            Vect.memset(partitionUpdateSinkAddr + PARTITION_SINK_COL_TOP_OFFSET, (long) Long.BYTES * columnCount, 0);
        }

        try {
            for (int i = 0; i < columnCount; i++) {
                final int columnType = metadata.getColumnType(i);
                if (columnType < 0) {
                    continue;
                }
                final int colOffset = getPrimaryColumnIndex(i);
                final boolean notTheTimestamp = i != timestampIndex;
                final MemoryCR oooMem1 = oooColumns.getQuick(colOffset);
                final MemoryCR oooMem2 = oooColumns.getQuick(colOffset + 1);
                final MemoryMA mem1 = columns.getQuick(colOffset);
                final MemoryMA mem2 = columns.getQuick(colOffset + 1);
                final int activeFixFd;
                final int activeVarFd;
                final long srcDataTop;
                final long srcOooFixAddr;
                final long srcOooVarAddr;
                if (!ColumnType.isVarSize(columnType)) {
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
                final int indexBlockCapacity = isIndexed ? metadata.getIndexValueBlockCapacity(i) : -1;
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
                    final long columnNameTxn = tableWriter.getColumnNameTxn(oldPartitionTimestamp, i);
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
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
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
                                indexBlockCapacity,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    } else {
                        publishOpenColumnTaskContended(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
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
                                indexBlockCapacity,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    }
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    LOG.critical().$("open column error [table=").utf8(tableWriter.getTableToken().getTableName())
                            .$(", e=").$(e)
                            .I$();
                    columnsInFlight = i + 1;
                    throw e;
                }
            }
        } finally {
            final int delta = columnsInFlight - columnCount;
            LOG.debug().$("idle [delta=").$(delta).I$();
            if (delta < 0 && columnCounter.addAndGet(delta) == 0) {
                O3CopyJob.closeColumnIdleQuick(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
            }
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        processPartition(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
