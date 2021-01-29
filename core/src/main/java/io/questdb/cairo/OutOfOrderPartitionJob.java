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
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import io.questdb.tasks.OutOfOrderPartitionTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderPartitionJob extends AbstractQueueConsumerJob<OutOfOrderPartitionTask> {

    private final RingQueue<OutOfOrderOpenColumnTask> openColumnTaskQueue;
    private final Sequence openColumnPubSeq;

    public OutOfOrderPartitionJob(
            RingQueue<OutOfOrderPartitionTask> partitionTaskQueue,
            Sequence partitionTaskSubSeq,
            RingQueue<OutOfOrderOpenColumnTask> openColumnTaskQueue,
            Sequence openColumnPubSeq
    ) {
        super(partitionTaskQueue, partitionTaskSubSeq);
        this.openColumnTaskQueue = openColumnTaskQueue;
        this.openColumnPubSeq = openColumnPubSeq;
    }

    private static long oooMapTimestampRO(FilesFacade ff, long fd, long size) {
        final long address = ff.mmap(fd, size, 0, Files.MAP_RO);
        if (address == FilesFacade.MAP_FAILED) {
            throw CairoException.instance(ff.errno())
                    .put("Could not mmap ")
                    .put(" [size=").put(size)
                    .put(", fd=").put(fd)
                    .put(", memUsed=").put(Unsafe.getMemUsed())
                    .put(", fileLen=").put(ff.length(fd))
                    .put(']');
        }
        return address;
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        OutOfOrderPartitionTask task = queue.get(cursor);
        // copy task on stack so that publisher has fighting chance of
        // publishing all it has to the queue

        final boolean locked = task.tryLock();
        if (locked) {
            processPartition(task, cursor, subSeq);
        } else {
            subSeq.done(cursor);
        }

        return true;
    }

    private long oooCreateMergeIndex(
            long srcDataTimestampAddr,
            long mergedTimestamps,
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
        Unsafe.getUnsafe().putLong(indexStruct + 2 * Long.BYTES, mergedTimestamps + mergeOOOLo * 16);
        Unsafe.getUnsafe().putLong(indexStruct + 3 * Long.BYTES, mergeOOOHi - mergeOOOLo + 1);
        final long result = Vect.mergeLongIndexesAsc(indexStruct, 2);
        Unsafe.free(index, indexSize);
        Unsafe.free(indexStruct, TIMESTAMP_MERGE_ENTRY_BYTES * 2);
        return result;
    }

    private void processPartition(OutOfOrderPartitionTask task, long cursor, Sequence subSeq) {
        // find "current" partition boundary in the out of order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        // todo: rename this
        final long timestampMergeIndex = task.getSortedTimestamps();
        final long srcOooLo = task.getOooIndexLo();
        final long srcOooHi = task.getOooIndexHi();
        final long lastPartitionIndexMax = task.getLastPartitionIndexMax();
        final long partitionTimestampHi = task.getPartitionTimestampHi();
        final long oooTimestampMax = task.getOooTimestampMax();
        final long oooPartitionTimestampMin = getTimestampIndexValue(timestampMergeIndex, srcOooLo);
        final FilesFacade ff = task.getFf();
        final long tableCeilOfMaxTimestamp = task.getTableCeilOfMaxTimestamp();
        final long tableFloorOfMinTimestamp = task.getTableFloorOfMinTimestamp();
        final long tableFloorOfMaxTimestamp = task.getTableFloorOfMaxTimestamp();
        final long tableMaxTimestamp = task.getTableMaxTimestamp();
        final ObjList<AppendMemory> columns = task.getColumns();
        final ObjList<ContiguousVirtualMemory> oooColumns = task.getOooColumns();
        final TableWriterMetadata metadata = task.getMetadata();
        final Path path = task.getPath();
        final int rootLen = task.getRootLen();
        final int partitionBy = task.getPartitionBy();
        final int timestampIndex = task.getTimestampIndex();
        final long sortedTimestampsAddr = task.getSortedTimestamps();
        final long txn = task.getTxn();
        TableUtils.setPathForPartition(path.trimTo(rootLen), partitionBy, oooPartitionTimestampMin);
        final int plen = path.length();

        long timestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        long dataIndexMax;

        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and and write to existing file descriptors

        if (partitionTimestampHi > tableCeilOfMaxTimestamp || partitionTimestampHi < tableFloorOfMinTimestamp) {

            // this has to be a brand new partition for either of two cases:
            // - this partition is above min partition of the table
            // - this partition is below max partition of the table
            // pure OOO data copy into new partition
            publishOpenColumnTasks(
                    ff,
                    txn,
                    metadata,
                    columns,
                    oooColumns,
                    path,
                    srcOooLo,
                    srcOooHi,
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
                    5, //oooOpenNewPartitionForAppend
                    -1,  // timestamp fd
                    timestampIndex,
                    sortedTimestampsAddr
            );
        } else {

            // out of order is hitting existing partition
            final long timestampColumnAddr;
            final long timestampColumnSize;
            try {
                if (partitionTimestampHi == tableCeilOfMaxTimestamp) {
                    dataTimestampHi = tableMaxTimestamp;
                    dataIndexMax = lastPartitionIndexMax;
                    timestampColumnSize = dataIndexMax * 8L;
                    // negative fd indicates descriptor reuse
                    timestampFd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                    timestampColumnAddr = oooMapTimestampRO(ff, -timestampFd, timestampColumnSize);
                } else {
                    long tempMem8b = Unsafe.malloc(Long.BYTES);
                    try {
                        dataIndexMax = readPartitionSize(ff, path, tempMem8b);
                    } finally {
                        Unsafe.free(tempMem8b, Long.BYTES);
                    }
                    timestampColumnSize = dataIndexMax * 8L;
                    // out of order data is going into archive partition
                    // we need to read "low" and "high" boundaries of the partition. "low" being oldest timestamp
                    // and "high" being newest

                    dFile(path.trimTo(plen), metadata.getColumnName(timestampIndex));

                    // also track the fd that we need to eventually close
                    timestampFd = ff.openRW(path);
                    if (timestampFd == -1) {
                        throw CairoException.instance(ff.errno()).put("could not open `").put(path).put('`');
                    }
                    timestampColumnAddr = oooMapTimestampRO(ff, timestampFd, timestampColumnSize);
                    dataTimestampHi = Unsafe.getUnsafe().getLong(timestampColumnAddr + timestampColumnSize - Long.BYTES);
                }
                dataTimestampLo = Unsafe.getUnsafe().getLong(timestampColumnAddr);


                // create copy jobs
                // we will have maximum of 3 stages:
                // - prefix data
                // - merge job
                // - suffix data
                //
                // prefix and suffix can be sourced either from OO fully or from Data (written to disk) fully
                // so for prefix and suffix we will need a flag indicating source of the data
                // as well as range of rows in that source

                int prefixType = OO_BLOCK_NONE;
                long prefixLo = -1;
                long prefixHi = -1;
                int mergeType = OO_BLOCK_NONE;
                long mergeDataLo = -1;
                long mergeDataHi = -1;
                long mergeOOOLo = -1;
                long mergeOOOHi = -1;
                int suffixType = OO_BLOCK_NONE;
                long suffixLo = -1;
                long suffixHi = -1;

                try {
                    if (oooPartitionTimestampMin < dataTimestampLo) {

                        prefixType = OO_BLOCK_OO;
                        prefixLo = srcOooLo;
                        if (dataTimestampLo < oooTimestampMax) {

                            //            +-----+
                            //            | OOO |
                            //
                            //  +------+
                            //  | data |

                            mergeDataLo = 0;
                            prefixHi = Vect.binarySearchIndexT(timestampMergeIndex, dataTimestampLo, srcOooLo, srcOooHi, BinarySearch.SCAN_DOWN);
                            mergeOOOLo = prefixHi + 1;
                        } else {
                            //            +-----+
                            //            | OOO |
                            //            +-----+
                            //
                            //  +------+
                            //  | data |
                            //
                            prefixHi = srcOooHi;
                        }

                        if (oooTimestampMax >= dataTimestampLo) {

                            //
                            //  +------+  | OOO |
                            //  | data |  +-----+
                            //  |      |

                            if (oooTimestampMax < dataTimestampHi) {

                                // |      | |     |
                                // |      | | OOO |
                                // | data | +-----+
                                // |      |
                                // +------+

                                mergeType = OO_BLOCK_MERGE;
                                mergeOOOHi = srcOooHi;
                                mergeDataHi = Vect.binarySearch64Bit(timestampColumnAddr, oooTimestampMax, 0, dataIndexMax - 1, BinarySearch.SCAN_DOWN);

                                suffixLo = mergeDataHi + 1;
                                suffixType = OO_BLOCK_DATA;
                                suffixHi = dataIndexMax - 1;

                            } else if (oooTimestampMax > dataTimestampHi) {

                                // |      | |     |
                                // |      | | OOO |
                                // | data | |     |
                                // +------+ |     |
                                //          +-----+

                                mergeDataHi = dataIndexMax - 1;
                                mergeOOOHi = Vect.binarySearchIndexT(timestampMergeIndex, dataTimestampHi - 1, mergeOOOLo, srcOooHi, BinarySearch.SCAN_DOWN) + 1;

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
                                mergeDataHi = dataIndexMax - 1;
                            }
                        } else {

                            //            +-----+
                            //            | OOO |
                            //            +-----+
                            //
                            //  +------+
                            //  | data |

                            suffixType = OO_BLOCK_DATA;
                            suffixLo = 0;
                            suffixHi = dataIndexMax - 1;
                        }
                    } else {
                        if (oooPartitionTimestampMin <= dataTimestampLo) {
                            //            +-----+
                            //            | OOO |
                            //            +-----+
                            //
                            //  +------+
                            //  | data |
                            //

                            prefixType = OO_BLOCK_OO;
                            prefixLo = srcOooLo;
                            prefixHi = srcOooHi;

                            suffixType = OO_BLOCK_DATA;
                            suffixLo = 0;
                            suffixHi = dataIndexMax - 1;
                        } else {
                            //   +------+                +------+  +-----+
                            //   | data |  +-----+       | data |  |     |
                            //   |      |  | OOO |  OR   |      |  | OOO |
                            //   |      |  |     |       |      |  |     |

                            if (oooPartitionTimestampMin <= dataTimestampHi) {

                                //
                                // +------+
                                // |      |
                                // |      | +-----+
                                // | data | | OOO |
                                // +------+

                                prefixType = OO_BLOCK_DATA;
                                prefixLo = 0;
                                prefixHi = Vect.binarySearch64Bit(timestampColumnAddr, oooPartitionTimestampMin, 0, dataIndexMax - 1, BinarySearch.SCAN_DOWN);
                                mergeDataLo = prefixHi + 1;
                                mergeOOOLo = srcOooLo;

                                if (oooTimestampMax < dataTimestampHi) {

                                    //
                                    // |      | +-----+
                                    // | data | | OOO |
                                    // |      | +-----+
                                    // +------+

                                    mergeOOOHi = srcOooHi;
                                    mergeDataHi = Vect.binarySearch64Bit(timestampColumnAddr, oooTimestampMax - 1, mergeDataLo, dataIndexMax - 1, BinarySearch.SCAN_DOWN) + 1;

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
                                    suffixHi = dataIndexMax - 1;
                                } else if (oooTimestampMax > dataTimestampHi) {

                                    //
                                    // |      | +-----+
                                    // | data | | OOO |
                                    // |      | |     |
                                    // +------+ |     |
                                    //          |     |
                                    //          +-----+

                                    mergeOOOHi = Vect.binarySearchIndexT(timestampMergeIndex, dataTimestampHi, srcOooLo, srcOooHi, BinarySearch.SCAN_UP);
                                    mergeDataHi = dataIndexMax - 1;

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
                                    mergeDataHi = dataIndexMax - 1;
                                }
                            } else {

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
                            }
                        }
                    }
                } finally {
                    ff.munmap(timestampColumnAddr, timestampColumnSize);
                }

                path.trimTo(plen);
                final int openColumnMode;
                if (prefixType == OO_BLOCK_NONE && mergeType == OO_BLOCK_NONE) {
                    // We do not need to create a copy of partition when we simply need to append
                    // existing the one.
                    if (partitionTimestampHi < tableFloorOfMaxTimestamp) {
                        openColumnMode = 1; // oooOpenMidPartitionForAppend
                    } else {
                        openColumnMode = 2; // oooOpenLastPartitionForAppend
                    }
                } else {
                    if (timestampFd > -1) {
                        openColumnMode = 3; // oooOpenMidPartitionForMerge
                    } else {
                        openColumnMode = 4; // oooOpenLastPartitionForMerge
                    }
                }

                publishOpenColumnTasks(
                        ff,
                        txn,
                        metadata,
                        columns,
                        oooColumns,
                        path,
                        srcOooLo,
                        srcOooHi,
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
                        dataIndexMax,
                        openColumnMode,
                        timestampFd,
                        timestampIndex,
                        sortedTimestampsAddr
                );

            } finally {
                if (timestampFd > 0) {
                    ff.close(timestampFd);
                }
            }
        }
    }

    private void publishOpenColumnTasks(
            FilesFacade ff,
            long txn,
            TableWriterMetadata metadata,
            ObjList<AppendMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            Path path,
            long srcOooLo,
            long srcOooHi,
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
            int openColumnMode,
            long timestampFd,
            int timestampIndex,
            long sortedTimestampsAddr
    ) {
        final long timestampMergeIndexAddr;
        if (mergeType == OO_BLOCK_MERGE) {
            timestampMergeIndexAddr = oooCreateMergeIndex(
                    columns.getQuick(TableWriter.getPrimaryColumnIndex(timestampIndex)).addressOf(0),
                    sortedTimestampsAddr,
                    mergeDataLo,
                    mergeDataHi,
                    mergeOOOLo,
                    mergeOOOHi
            );
        } else {
            timestampMergeIndexAddr = 0;
        }

        final int columnCount = metadata.getColumnCount();
        AtomicInteger columnCounter = new AtomicInteger(columnCount);
        for (int i = 0; i < columnCount; i++) {
            long c = openColumnPubSeq.next();
            // todo: check if c is valid
            final OutOfOrderOpenColumnTask openColumnTask = openColumnTaskQueue.get(c);
            final int colOffset = TableWriter.getPrimaryColumnIndex(i);
            final boolean notTheTimestamp = i != timestampIndex;
            final int columnType = metadata.getColumnType(i);
            final ContiguousVirtualMemory srcOooFixColumn = oooColumns.getQuick(colOffset);
            final ContiguousVirtualMemory srcOooVarColumn = oooColumns.getQuick(colOffset + 1);
            final AppendMemory mem1 = columns.getQuick(colOffset);
            final AppendMemory mem2 = columns.getQuick(colOffset + 1);
            final AppendMemory srcDataFixMemory;
            final AppendMemory srcDataVarMemory;
            if (columnType != ColumnType.STRING && columnType != ColumnType.BINARY) {
                srcDataFixMemory = mem1;
                srcDataVarMemory = mem2;
            } else {
                srcDataFixMemory = mem2;
                srcDataVarMemory = mem1;
            }
            openColumnTask.of(
                    ff,
                    columnCounter,
                    txn,
                    openColumnMode,
                    metadata.getColumnName(i),
                    notTheTimestamp ? columnType : -columnType,
                    metadata.isColumnIndexed(i),
                    notTheTimestamp ? -1 : timestampFd,
                    timestampMergeIndexAddr,
                    srcDataFixMemory,
                    srcDataVarMemory,
                    srcOooFixColumn.addressOf(0),
                    srcOooFixColumn.getAppendOffset(),
                    srcOooVarColumn == null ? 0 : srcOooVarColumn.addressOf(0),
                    srcOooVarColumn == null ? 0 : srcOooVarColumn.getAppendOffset(),
                    path,
                    srcOooLo,
                    srcOooHi,
                    srcDataMax,
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
                    suffixHi
            );
            openColumnPubSeq.done(c);
        }
    }
}
