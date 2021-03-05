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
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cairo.vm.ContiguousVirtualMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderCopyTask;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import io.questdb.tasks.OutOfOrderPartitionTask;
import io.questdb.tasks.OutOfOrderUpdPartitionSizeTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.OutOfOrderOpenColumnJob.*;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderPartitionJob extends AbstractQueueConsumerJob<OutOfOrderPartitionTask> {

    private static final Log LOG = LogFactory.getLog(OutOfOrderPartitionJob.class);
    private final CairoConfiguration configuration;
    private final RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue;
    private final Sequence openColumnPubSeq;
    private final RingQueue<OutOfOrderCopyTask> copyTaskOutboundQueue;
    private final Sequence copyTaskPubSeq;
    private final RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue;
    private final MPSequence updPartitionSizePubSeq;

    public OutOfOrderPartitionJob(MessageBus messageBus) {
        super(messageBus.getOutOfOrderPartitionQueue(), messageBus.getOutOfOrderPartitionSubSeq());
        this.configuration = messageBus.getConfiguration();
        this.openColumnTaskOutboundQueue = messageBus.getOutOfOrderOpenColumnQueue();
        this.openColumnPubSeq = messageBus.getOutOfOrderOpenColumnPubSequence();
        this.copyTaskOutboundQueue = messageBus.getOutOfOrderCopyQueue();
        this.copyTaskPubSeq = messageBus.getOutOfOrderCopyPubSequence();
        this.updPartitionSizeQueue = messageBus.getOutOfOrderUpdPartitionSizeQueue();
        this.updPartitionSizePubSeq = messageBus.getOutOfOrderUpdPartitionSizePubSequence();
    }

    public static void processPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue,
            Sequence openColumnPubSeq,
            RingQueue<OutOfOrderCopyTask> copyTaskOutboundQueue,
            Sequence copyTaskPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            CharSequence pathToTable,
            int partitionBy,
            ObjList<AppendOnlyVirtualMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long txn,
            long sortedTimestampsAddr,
            long lastPartitionSize,
            long tableCeilOfMaxTimestamp,
            long tableFloorOfMinTimestamp,
            long tableFloorOfMaxTimestamp,
            long tableMaxTimestamp,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        final Path path = Path.getThreadLocal(pathToTable);
        final long oooTimestampLo = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
        final long partitionSize = tableWriter.getPartitionSizeByTimestamp(oooTimestampLo);
        TableUtils.setPathForPartition(path, partitionBy, oooTimestampLo);
        final RecordMetadata metadata = tableWriter.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        final int plen = path.length();
        long srcTimestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        long srcDataMax;

        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and and write to existing file descriptors

        if (partitionSize == -1) {

            // this has to be a brand new partition for either of two cases:
            // - this partition is above min partition of the table
            // - this partition is below max partition of the table
            // pure OOO data copy into new partition

            try {
                LOG.debug().$("would create [path=").$(path.chopZ().put(Files.SEPARATOR).$()).$(']').$();
                createDirsOrFail(ff, path, configuration.getMkDirMode());
            } catch (Throwable e) {
                LOG.debug().$("idle new").$();
                tableWriter.bumpOooErrorCount();
                tableWriter.bumpPartitionUpdateCount();
                doneLatch.countDown();
                throw e;
            }

            publishOpenColumnTasks(
                    workerId,
                    configuration,
                    openColumnTaskOutboundQueue,
                    openColumnPubSeq,
                    copyTaskOutboundQueue,
                    copyTaskPubSeq,
                    updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    ff,
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampLo,
                    oooTimestampHi,
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
                    tableFloorOfMaxTimestamp,
                    0,
                    OPEN_NEW_PARTITION_FOR_APPEND,
                    -1,  // timestamp fd
                    0,
                    0,
                    timestampIndex,
                    sortedTimestampsAddr,
                    tableWriter,
                    doneLatch
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
            long mergeOOOLo;
            long mergeOOOHi;
            int suffixType;
            long suffixLo;
            long suffixHi;
            final int openColumnMode;

            try {
                // out of order is hitting existing partition
                // oooTimestampHi is in fact a ceil of ooo timestamp value for the given partition
                // so this check is for matching ceilings
                if (oooTimestampHi == tableCeilOfMaxTimestamp) {
                    dataTimestampHi = tableMaxTimestamp;
                    srcDataMax = lastPartitionSize;
                    srcTimestampSize = srcDataMax * 8L;
                    // negative fd indicates descriptor reuse
                    srcTimestampFd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                    srcTimestampAddr = OutOfOrderUtils.mapRO(ff, -srcTimestampFd, srcTimestampSize);
                } else {
                    srcDataMax = partitionSize;
                    srcTimestampSize = srcDataMax * 8L;
                    // out of order data is going into archive partition
                    // we need to read "low" and "high" boundaries of the partition. "low" being oldest timestamp
                    // and "high" being newest

                    dFile(path.trimTo(plen), metadata.getColumnName(timestampIndex));

                    // also track the fd that we need to eventually close
                    srcTimestampFd = OutOfOrderUtils.openRW(ff, path);
                    srcTimestampAddr = OutOfOrderUtils.mapRW(ff, srcTimestampFd, srcTimestampSize);
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

                prefixType = OO_BLOCK_NONE;
                prefixLo = -1;
                prefixHi = -1;
                mergeType = OO_BLOCK_NONE;
                mergeDataLo = -1;
                mergeDataHi = -1;
                mergeOOOLo = -1;
                mergeOOOHi = -1;
                suffixType = OO_BLOCK_NONE;
                suffixLo = -1;
                suffixHi = -1;

                if (oooTimestampLo > dataTimestampLo) {
                    //   +------+
                    //   | data |  +-----+
                    //   |      |  | OOO |
                    //   |      |  |     |

                    if (oooTimestampLo > dataTimestampHi) {

                        // +------+
                        // | data |
                        // |      |
                        // +------+
                        //
                        //           +-----+
                        //           | OOO |
                        //           |     |
                        //
                        suffixType = OO_BLOCK_OO;
                        suffixLo = srcOooLo;
                        suffixHi = srcOooHi;
                    } else {

                        //
                        // +------+
                        // |      |
                        // |      | +-----+
                        // | data | | OOO |
                        // +------+

                        prefixType = OO_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = Vect.binarySearch64Bit(srcTimestampAddr, oooTimestampLo, 0, srcDataMax - 1, BinarySearch.SCAN_DOWN);
                        mergeDataLo = prefixHi + 1;
                        mergeOOOLo = srcOooLo;

                        if (oooTimestampMax < dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | +-----+
                            // +------+

                            mergeOOOHi = srcOooHi;
                            mergeDataHi = Vect.binarySearch64Bit(srcTimestampAddr, oooTimestampMax - 1, mergeDataLo, srcDataMax - 1, BinarySearch.SCAN_DOWN) + 1;

                            if (mergeDataLo < mergeDataHi) {
                                mergeType = OO_BLOCK_MERGE;
                            } else {
                                // the OO data implodes right between rows of existing data
                                // so we will have both data prefix and suffix and the middle bit

                                // is the out of order
                                mergeType = OO_BLOCK_OO;
                                mergeDataHi--;
                            }

                            suffixType = OO_BLOCK_DATA;
                            suffixLo = mergeDataHi + 1;
                            suffixHi = srcDataMax - 1;
                        } else if (oooTimestampMax > dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ |     |
                            //          |     |
                            //          +-----+

                            mergeOOOHi = Vect.binarySearchIndexT(sortedTimestampsAddr, dataTimestampHi, srcOooLo, srcOooHi, BinarySearch.SCAN_UP);
                            mergeDataHi = srcDataMax - 1;

                            mergeType = OO_BLOCK_MERGE;
                            suffixType = OO_BLOCK_OO;
                            suffixLo = mergeOOOHi + 1;
                            suffixHi = srcOooHi;
                        } else {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ +-----+
                            //

                            mergeType = OO_BLOCK_MERGE;
                            mergeOOOHi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                        }
                    }
                } else {

                    //            +-----+
                    //            | OOO |
                    //
                    //  +------+
                    //  | data |


                    prefixType = OO_BLOCK_OO;
                    prefixLo = srcOooLo;
                    if (dataTimestampLo < oooTimestampMax) {

                        //
                        //  +------+  | OOO |
                        //  | data |  +-----+
                        //  |      |

                        mergeDataLo = 0;
                        prefixHi = Vect.binarySearchIndexT(sortedTimestampsAddr, dataTimestampLo, srcOooLo, srcOooHi, BinarySearch.SCAN_DOWN);
                        mergeOOOLo = prefixHi + 1;

                        if (oooTimestampMax < dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | +-----+
                            // |      |
                            // +------+

                            mergeType = OO_BLOCK_MERGE;
                            mergeOOOHi = srcOooHi;
                            mergeDataHi = Vect.binarySearch64Bit(srcTimestampAddr, oooTimestampMax, 0, srcDataMax - 1, BinarySearch.SCAN_DOWN);

                            suffixLo = mergeDataHi + 1;
                            suffixType = OO_BLOCK_DATA;
                            suffixHi = srcDataMax - 1;

                        } else if (oooTimestampMax > dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ |     |
                            //          +-----+

                            mergeDataHi = srcDataMax - 1;
                            mergeOOOHi = Vect.binarySearchIndexT(sortedTimestampsAddr, dataTimestampHi - 1, mergeOOOLo, srcOooHi, BinarySearch.SCAN_DOWN) + 1;

                            if (mergeOOOLo < mergeOOOHi) {
                                mergeType = OO_BLOCK_MERGE;
                            } else {
                                mergeType = OO_BLOCK_DATA;
                                mergeOOOHi--;
                            }

                            if (mergeOOOHi < srcOooHi) {
                                suffixLo = mergeOOOHi + 1;
                                suffixType = OO_BLOCK_OO;
                                suffixHi = Math.max(suffixLo, srcOooHi);
                            } else {
                                suffixType = OO_BLOCK_NONE;
                            }
                        } else {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ +-----+

                            mergeType = OO_BLOCK_MERGE;
                            mergeOOOHi = srcOooHi;
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
                        prefixHi = srcOooHi;
                        suffixType = OO_BLOCK_DATA;
                        suffixLo = 0;
                        suffixHi = srcDataMax - 1;
                    }
                }

                path.trimTo(plen);
                if (prefixType == OO_BLOCK_NONE) {
                    // We do not need to create a copy of partition when we simply need to append
                    // existing the one.
                    if (oooTimestampHi < tableFloorOfMaxTimestamp) {
                        openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                    } else {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_APPEND;
                    }
                } else {
                    newPartitionName(path.trimTo(plen), txn);
                    createDirsOrFail(ff, path.put(Files.SEPARATOR).$(), configuration.getMkDirMode());
                    if (srcTimestampFd > -1) {
                        openColumnMode = OPEN_MID_PARTITION_FOR_MERGE;
                    } else {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_MERGE;
                    }
                }
            } catch (Throwable e) {
                LOG.debug().$("idle existing").$();
                OutOfOrderUtils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                OutOfOrderUtils.close(ff, srcTimestampFd);
                tableWriter.bumpOooErrorCount();
                tableWriter.bumpPartitionUpdateCount();
                doneLatch.countDown();
                throw e;
            }

            publishOpenColumnTasks(
                    workerId,
                    configuration,
                    openColumnTaskOutboundQueue,
                    openColumnPubSeq,
                    copyTaskOutboundQueue,
                    copyTaskPubSeq,
                    updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    ff,
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampLo,
                    oooTimestampHi,
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
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
                    openColumnMode,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    timestampIndex,
                    sortedTimestampsAddr,
                    tableWriter,
                    doneLatch
            );
        }
    }

    public static void processPartition(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue,
            Sequence openColumnPubSeq,
            RingQueue<OutOfOrderCopyTask> copyTaskOutboundQueue,
            Sequence copyTaskPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeQueue,
            MPSequence updPartitionSizePubSeq,
            OutOfOrderPartitionTask task,
            long cursor,
            Sequence subSeq
    ) {
        final FilesFacade ff = task.getFf();
        // find "current" partition boundary in the out of order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        final CharSequence pathToTable = task.getPathToTable();
        final int partitionBy = task.getPartitionBy();
        final ObjList<AppendOnlyVirtualMemory> columns = task.getColumns();
        final ObjList<ContiguousVirtualMemory> oooColumns = task.getOooColumns();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long oooTimestampMax = task.getOooTimestampMax();
        final long oooTimestampHi = task.getOooTimestampHi();
        final long txn = task.getTxn();
        final long sortedTimestampsAddr = task.getSortedTimestampsAddr();
        final long lastPartitionSize = task.getLastPartitionSize();
        final long tableCeilOfMaxTimestamp = task.getTableCeilOfMaxTimestamp();
        final long tableFloorOfMinTimestamp = task.getTableFloorOfMinTimestamp();
        final long tableFloorOfMaxTimestamp = task.getTableFloorOfMaxTimestamp();
        final long tableMaxTimestamp = task.getTableMaxTimestamp();
        final TableWriter tableWriter = task.getTableWriter();
        final SOUnboundedCountDownLatch doneLatch = task.getDoneLatch();

        subSeq.done(cursor);

        processPartition(
                workerId,
                configuration,
                openColumnTaskOutboundQueue,
                openColumnPubSeq,
                copyTaskOutboundQueue,
                copyTaskPubSeq,
                updPartitionSizeQueue,
                updPartitionSizePubSeq,
                ff,
                pathToTable,
                partitionBy,
                columns,
                oooColumns,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                txn,
                sortedTimestampsAddr,
                lastPartitionSize,
                tableCeilOfMaxTimestamp,
                tableFloorOfMinTimestamp,
                tableFloorOfMaxTimestamp,
                tableMaxTimestamp,
                tableWriter,
                doneLatch
        );

    }

    private static long oooCreateMergeIndex(
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
        final long indexStruct = Unsafe.malloc(TIMESTAMP_MERGE_ENTRY_BYTES * 2);
        final long index = Unsafe.malloc(indexSize);
        Vect.makeTimestampIndex(srcDataTimestampAddr, mergeDataLo, mergeDataHi, index);
        Unsafe.getUnsafe().putLong(indexStruct, index);
        Unsafe.getUnsafe().putLong(indexStruct + Long.BYTES, mergeDataHi - mergeDataLo + 1);
        Unsafe.getUnsafe().putLong(indexStruct + 2 * Long.BYTES, sortedTimestampsAddr + mergeOOOLo * 16);
        Unsafe.getUnsafe().putLong(indexStruct + 3 * Long.BYTES, mergeOOOHi - mergeOOOLo + 1);
        final long result = Vect.mergeLongIndexesAsc(indexStruct, 2);
        Unsafe.free(index, indexSize);
        Unsafe.free(indexStruct, TIMESTAMP_MERGE_ENTRY_BYTES * 2);
        return result;
    }

    private static void publishOpenColumnTaskHarmonized(
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue,
            Sequence openColumnPubSeq,
            long cursor,
            int openColumnMode,
            FilesFacade ff,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampLo,
            long oooTimestampHi,
            long srcDataTop, long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
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
            SOUnboundedCountDownLatch doneLatch
    ) {
        final OutOfOrderOpenColumnTask openColumnTask = openColumnTaskOutboundQueue.get(cursor);
        openColumnTask.of(
                openColumnMode,
                ff,
                pathToTable,
                columnName,
                columnCounter,
                columnType,
                timestampMergeIndexAddr,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampLo,
                oooTimestampHi,
                srcDataMax,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
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
                srcDataTop,
                tableWriter,
                doneLatch
        );
        openColumnPubSeq.done(cursor);
    }

    private static void publishOpenColumnTasks(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue,
            Sequence openColumnPubSeq,
            RingQueue<OutOfOrderCopyTask> copyTaskOutboundQueue,
            Sequence copyTaskPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            FilesFacade ff,
            long txn,
            ObjList<AppendOnlyVirtualMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            CharSequence pathToTable,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampLo,
            long oooTimestampHi,
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
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            int openColumnMode,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int timestampIndex,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        LOG.debug().$("partition [ts=").$ts(oooTimestampLo).$(']').$();

        final long timestampMergeIndexAddr;
        if (mergeType == OO_BLOCK_MERGE) {
            timestampMergeIndexAddr = oooCreateMergeIndex(
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
        // todo: cache
        final AtomicInteger columnCounter = new AtomicInteger(columnCount);
        int columnsInFlight = columnCount;
        try {
            for (int i = 0; i < columnCount; i++) {
                final int colOffset = TableWriter.getPrimaryColumnIndex(i);
                final boolean notTheTimestamp = i != timestampIndex;
                final int columnType = metadata.getColumnType(i);
                final ContiguousVirtualMemory oooMem1 = oooColumns.getQuick(colOffset);
                final ContiguousVirtualMemory oooMem2 = oooColumns.getQuick(colOffset + 1);
                final AppendOnlyVirtualMemory mem1 = columns.getQuick(colOffset);
                final AppendOnlyVirtualMemory mem2 = columns.getQuick(colOffset + 1);
                final long activeFixFd;
                final long activeVarFd;
                final long srcDataTop;
                final long srcOooFixAddr;
                final long srcOooFixSize;
                final long srcOooVarAddr;
                final long srcOooVarSize;
                if (columnType != ColumnType.STRING && columnType != ColumnType.BINARY) {
                    activeFixFd = mem1.getFd();
                    activeVarFd = 0;
                    srcOooFixAddr = oooMem1.addressOf(0);
                    srcOooFixSize = oooMem1.getAppendOffset();
                    srcOooVarAddr = 0;
                    srcOooVarSize = 0;
                } else {
                    activeFixFd = mem2.getFd();
                    activeVarFd = mem1.getFd();
                    srcOooFixAddr = oooMem2.addressOf(0);
                    srcOooFixSize = oooMem2.getAppendOffset();
                    srcOooVarAddr = oooMem1.addressOf(0);
                    srcOooVarSize = oooMem1.getAppendOffset();
                }

                final CharSequence columnName = metadata.getColumnName(i);
                final boolean isIndexed = metadata.isColumnIndexed(i);
                if (openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND || openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                    srcDataTop = tableWriter.getColumnTop(i);
                } else {
                    srcDataTop = -1; // column open job will have to find out if top exists and its value
                }

                try {
                    final long cursor = openColumnPubSeq.next();
                    if (cursor > -1) {
                        publishOpenColumnTaskHarmonized(
                                openColumnTaskOutboundQueue,
                                openColumnPubSeq,
                                cursor,
                                openColumnMode,
                                ff,
                                pathToTable,
                                columnName,
                                columnCounter,
                                notTheTimestamp ? columnType : -columnType,
                                timestampMergeIndexAddr,
                                srcOooFixAddr,
                                srcOooFixSize,
                                srcOooVarAddr,
                                srcOooVarSize,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                oooTimestampMax,
                                oooTimestampLo,
                                oooTimestampHi,
                                srcDataTop,
                                srcDataMax,
                                tableFloorOfMaxTimestamp,
                                dataTimestampHi,
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
                                doneLatch
                        );
                    } else {
                        publishOpenColumnTaskContended(
                                workerId,
                                configuration,
                                openColumnTaskOutboundQueue,
                                openColumnPubSeq,
                                cursor,
                                copyTaskOutboundQueue,
                                copyTaskPubSeq,
                                updPartitionSizeTaskQueue,
                                updPartitionSizePubSeq,
                                openColumnMode,
                                ff,
                                pathToTable,
                                columnName,
                                columnCounter,
                                notTheTimestamp ? columnType : -columnType,
                                timestampMergeIndexAddr,
                                srcOooFixAddr,
                                srcOooFixSize,
                                srcOooVarAddr,
                                srcOooVarSize,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                oooTimestampMax,
                                oooTimestampLo,
                                oooTimestampHi,
                                tableFloorOfMaxTimestamp,
                                dataTimestampHi,
                                srcDataTop,
                                srcDataMax,
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
                                doneLatch
                        );
                    }
                } catch (Throwable e) {
                    columnsInFlight = i + 1;
                    throw e;
                }
            }
        } finally {
            final int delta = columnsInFlight - columnCount;
            LOG.debug().$("idle [delta=").$(delta).$(']').$();
            if (delta < 0 && columnCounter.addAndGet(delta) == 0) {
                OutOfOrderCopyJob.closeColumnIdleQuick(
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
    }

    private static void publishOpenColumnTaskContended(
            int workerId,
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskOutboundQueue,
            Sequence openColumnPubSeq,
            long cursor,
            RingQueue<OutOfOrderCopyTask> copyTaskOutboundQueue,
            Sequence copyTaskPubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updPartitionSizeTaskQueue,
            MPSequence updPartitionSizePubSeq,
            int openColumnMode,
            FilesFacade ff,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampLo,
            long oooTimestampHi,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcDataTop,
            long srcDataMax,
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
            SOUnboundedCountDownLatch doneLatch
    ) {
        while (cursor == -2) {
            cursor = openColumnPubSeq.next();
        }

        if (cursor > -1) {
            publishOpenColumnTaskHarmonized(
                    openColumnTaskOutboundQueue,
                    openColumnPubSeq,
                    cursor,
                    openColumnMode,
                    ff,
                    pathToTable,
                    columnName,
                    columnCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampLo,
                    oooTimestampHi,
                    srcDataTop, srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
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
                    doneLatch
            );
        } else {
            OutOfOrderOpenColumnJob.openColumn(
                    workerId,
                    configuration,
                    copyTaskOutboundQueue,
                    copyTaskPubSeq,
                    updPartitionSizeTaskQueue,
                    updPartitionSizePubSeq,
                    openColumnMode,
                    ff,
                    pathToTable,
                    columnName,
                    columnCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    srcOooFixAddr,
                    srcOooFixSize,
                    srcOooVarAddr,
                    srcOooVarSize,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    oooTimestampLo,
                    oooTimestampHi,
                    srcDataTop,
                    srcDataMax,
                    tableFloorOfMaxTimestamp,
                    dataTimestampHi,
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
                    doneLatch
            );
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        processPartition(workerId + 1, queue.get(cursor), cursor, subSeq);
        return true;
    }

    private void processPartition(int workerId, OutOfOrderPartitionTask task, long cursor, Sequence subSeq) {
        processPartition(
                workerId,
                configuration,
                openColumnTaskOutboundQueue,
                openColumnPubSeq,
                copyTaskOutboundQueue,
                copyTaskPubSeq,
                updPartitionSizeQueue,
                updPartitionSizePubSeq,
                task,
                cursor,
                subSeq
        );
    }
}
