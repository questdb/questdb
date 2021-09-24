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
import io.questdb.cairo.vm.Vm;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static io.questdb.cairo.TableUtils.iFile;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private static final Timestamps.TimestampFloorMethod NO_PARTITIONING_FLOOR = (ts) -> 0;
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final RingQueue<TableBlockWriterTaskHolder> queue;
    private final Sequence pubSeq;
    private final LongList columnRowsAdded = new LongList();
    private final LongObjHashMap<PartitionBlockWriter> partitionBlockWriterByTimestamp = new LongObjHashMap<>();
    private final ObjList<PartitionBlockWriter> partitionBlockWriters = new ObjList<>();
    private final ObjList<TableBlockWriterTask> concurrentTasks = new ObjList<>();
    private final AtomicInteger nCompletedConcurrentTasks = new AtomicInteger();
    private TableWriter writer;
    private RecordMetadata metadata;
    private int columnCount;
    private int partitionBy;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;
    private int timestampColumnIndex;
    private long firstTimestamp;
    private long lastTimestamp;
    private int nextPartitionBlockWriterIndex;
    private int nEnqueuedConcurrentTasks;
    private PartitionBlockWriter partWriter;

    TableBlockWriter(CairoConfiguration configuration, MessageBus messageBus) {
        root = configuration.getRoot();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        queue = messageBus.getTableBlockWriterQueue();
        pubSeq = messageBus.getTableBlockWriterPubSeq();
    }

    public void appendPageFrameColumn(int columnIndex, long pageFrameSize, long sourceAddress) {
        LOG.info().$("appending data").$(" [tableName=").$(writer.getTableName()).$(", columnIndex=").$(columnIndex).$(", pageFrameSize=").$(pageFrameSize).$(']').$();
        if (columnIndex == timestampColumnIndex) {
            long firstBlockTimestamp = Unsafe.getUnsafe().getLong(sourceAddress);
            if (firstBlockTimestamp < firstTimestamp) {
                firstTimestamp = firstBlockTimestamp;
            }
            long addr = sourceAddress + pageFrameSize - Long.BYTES;
            long lastBlockTimestamp = Unsafe.getUnsafe().getLong(addr);
            if (lastBlockTimestamp > lastTimestamp) {
                lastTimestamp = lastBlockTimestamp;
            }
        }
        partWriter.appendPageFrameColumn(columnIndex, pageFrameSize, sourceAddress);
    }

    public void cancel() {
        completePendingConcurrentTasks(true);
        writer.cancelRow();
        for (int n = 0; n < nextPartitionBlockWriterIndex; n++) {
            partitionBlockWriters.getQuick(n).cancel();
        }
        writer.purgeUnusedPartitions();
        LOG.info().$("cancelled new block [table=").$(writer.getTableName()).$(']').$();
        clear();
    }

    @Override
    public void close() {
        clear();
        Misc.freeObjList(partitionBlockWriters);
        partitionBlockWriters.clear();
    }

    public void commit() {
        LOG.info().$("committing block write").$(" [tableName=").$(writer.getTableName()).$(", firstTimestamp=").$ts(firstTimestamp).$(", lastTimestamp=").$ts(lastTimestamp).$(']').$();
        // Need to complete all data tasks before we can start index tasks
        completePendingConcurrentTasks(false);
        for (int n = 0; n < nextPartitionBlockWriterIndex; n++) {
            partitionBlockWriters.getQuick(n).startCommitAppendedBlock();
        }
        completePendingConcurrentTasks(false);
        for (int n = 0; n < nextPartitionBlockWriterIndex; n++) {
            partitionBlockWriters.getQuick(n).completeCommitAppendedBlock();
        }
        writer.commitBlock(firstTimestamp);
        LOG.info().$("committed new block [table=").$(writer.getTableName()).$(']').$();
        clear();
    }

    public void startPageFrame(long timestampLo) {
        partWriter = getPartitionBlockWriter(timestampLo);
        partWriter.startPageFrame(timestampLo);
    }

    private static long mapFile(FilesFacade ff, long fd, final long mapOffset, final long mapSz) {
        long alignedMapOffset = (mapOffset / ff.getPageSize()) * ff.getPageSize();
        long addressOffsetDueToAlignment = mapOffset - alignedMapOffset;
        long alignedMapSz = mapSz + addressOffsetDueToAlignment;
        long address = TableUtils.mapRW(ff, fd, alignedMapSz, alignedMapOffset, MemoryTag.MMAP_DEFAULT);
        assert (address / ff.getPageSize()) * ff.getPageSize() == address; // address MUST be page aligned
        return address + addressOffsetDueToAlignment;
    }

    private static void unmapFile(FilesFacade ff, final long address, final long mapSz) {
        long alignedAddress = (address / ff.getPageSize()) * ff.getPageSize();
        long alignedMapSz = mapSz + address - alignedAddress;
        ff.munmap(alignedAddress, alignedMapSz, MemoryTag.MMAP_DEFAULT);
    }

    void clear() {
        if (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            LOG.error().$("new block should have been either committed or cancelled [table=").$(writer.getTableName()).$(']').$();
            completePendingConcurrentTasks(true);
        }
        metadata = null;
        writer = null;
        partWriter = null;
        for (int i = 0; i < nextPartitionBlockWriterIndex; i++) {
            partitionBlockWriters.getQuick(i).clear();
        }
        nextPartitionBlockWriterIndex = 0;
        partitionBlockWriterByTimestamp.clear();
    }

    private void completePendingConcurrentTasks(boolean cancel) {
        if (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            for (int n = 0; n < nEnqueuedConcurrentTasks; n++) {
                TableBlockWriterTask task = concurrentTasks.getQuick(n);
                if (cancel) {
                    task.cancel();
                } else {
                    task.run();
                }
            }
        }

        while (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            LockSupport.parkNanos(0);
        }
        nEnqueuedConcurrentTasks = 0;
        nCompletedConcurrentTasks.set(0);
    }

    private void enqueueConcurrentTask(TableBlockWriterTask task) {
        assert concurrentTasks.getQuick(nEnqueuedConcurrentTasks) == task;
        assert !task.ready.get();
        task.ready.set(true);
        nEnqueuedConcurrentTasks++;

        do {
            long seq = pubSeq.next();
            if (seq >= 0) {
                try {
                    queue.get(seq).task = task;
                } finally {
                    pubSeq.done(seq);
                }
                return;
            }
            if (seq == -1) {
                task.run();
                return;
            }
        } while (true);
    }

    private TableBlockWriterTask getConcurrentTask() {
        if (concurrentTasks.size() <= nEnqueuedConcurrentTasks) {
            concurrentTasks.extendAndSet(nEnqueuedConcurrentTasks, new TableBlockWriterTask());
        }
        return concurrentTasks.getQuick(nEnqueuedConcurrentTasks);
    }

    private PartitionBlockWriter getPartitionBlockWriter(long timestamp) {
        long timestampLo = timestampFloorMethod.floor(timestamp);
        PartitionBlockWriter partWriter = partitionBlockWriterByTimestamp.get(timestampLo);
        if (null == partWriter) {
            assert nextPartitionBlockWriterIndex <= partitionBlockWriters.size();
            if (nextPartitionBlockWriterIndex == partitionBlockWriters.size()) {
                partWriter = new PartitionBlockWriter();
                partitionBlockWriters.extendAndSet(nextPartitionBlockWriterIndex, partWriter);
            } else {
                partWriter = partitionBlockWriters.getQuick(nextPartitionBlockWriterIndex);
            }
            nextPartitionBlockWriterIndex++;
            partitionBlockWriterByTimestamp.put(timestampLo, partWriter);
            partWriter.of(timestampLo);
        }

        return partWriter;
    }

    void open(TableWriter writer) {
        this.writer = writer;
        metadata = writer.getMetadata();
        columnCount = metadata.getColumnCount();
        partitionBy = writer.getPartitionBy();
        columnRowsAdded.ensureCapacity(columnCount);
        timestampColumnIndex = metadata.getTimestampIndex();
        firstTimestamp = timestampColumnIndex >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        lastTimestamp = timestampColumnIndex >= 0 ? Long.MIN_VALUE : 0;
        nEnqueuedConcurrentTasks = 0;
        nCompletedConcurrentTasks.set(0);
        switch (partitionBy) {
            case PartitionBy.DAY:
                timestampFloorMethod = Timestamps.FLOOR_DD;
                break;
            case PartitionBy.MONTH:
                timestampFloorMethod = Timestamps.FLOOR_MM;
                break;
            case PartitionBy.YEAR:
                timestampFloorMethod = Timestamps.FLOOR_YYYY;
                break;
            default:
                timestampFloorMethod = NO_PARTITIONING_FLOOR;
                break;
        }
        LOG.info().$("started new block [table=").$(writer.getTableName()).$(']').$();
    }

    private enum TaskType {
        AppendBlock, GenerateStringIndex, GenerateBinaryIndex
    }

    private static class PartitionStruct {
        private static final int MAPPING_STRUCT_ENTRY_P2 = 3;
        private static final int INITIAL_ADDITIONAL_MAPPINGS = 4;
        private long[] mappingData = null;
        private int columnCount;
        private int nAdditionalMappings;

        private void addAdditionalMapping(long start, long size) {
            int i = getMappingDataIndex(columnCount, nAdditionalMappings << 1);
            nAdditionalMappings++;
            int minSz = i + nAdditionalMappings << 1;
            if (mappingData.length < minSz) {
                long[] newMappingData = new long[minSz + (INITIAL_ADDITIONAL_MAPPINGS << 1)];
                System.arraycopy(mappingData, 0, newMappingData, 0, mappingData.length);
                mappingData = newMappingData;
            }
            mappingData[i++] = start;
            mappingData[i] = size;
        }

        private void clear() {
            Arrays.fill(mappingData, 0);
        }

        private long getAdditionalMappingSize(int nMapping) {
            int i = getMappingDataIndex(columnCount, (nMapping << 1) + 1);
            return mappingData[i];
        }

        private long getAdditionalMappingStart(int nMapping) {
            int i = getMappingDataIndex(columnCount, nMapping << 1);
            return mappingData[i];
        }

        private long getColumnAppendOffset(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 5)];
        }

        private long getColumnDataFd(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 0)];
        }

        private int getColumnFieldSizePow2(int columnIndex) {
            return (int) mappingData[getMappingDataIndex(columnIndex, 7)];
        }

        private long getColumnIndexFd(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 1)];
        }

        private long getColumnMappingSize(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 3)];
        }

        private long getColumnMappingStart(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 2)];
        }

        private long getColumnNRowsAdded(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 6)];
        }

        private long getColumnStartOffset(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 4)];
        }

        private int getMappingDataIndex(int columnIndex, int fieldIndex) {
            return (columnIndex << MAPPING_STRUCT_ENTRY_P2) + fieldIndex;
        }

        private int getnAdditionalMappings() {
            return nAdditionalMappings;
        }

        private void of(int columnCount) {
            this.columnCount = columnCount;
            nAdditionalMappings = 0;
            int MAPPING_STRUCT_ENTRY_SIZE = 1 << MAPPING_STRUCT_ENTRY_P2;
            int sz = columnCount * MAPPING_STRUCT_ENTRY_SIZE;
            if (mappingData == null || mappingData.length < sz) {
                sz += INITIAL_ADDITIONAL_MAPPINGS << 1;
                mappingData = new long[sz];
            }
        }

        private void setColumnAppendOffset(int columnIndex, long offset) {
            mappingData[getMappingDataIndex(columnIndex, 5)] = offset;
        }

        private void setColumnDataFd(int columnIndex, long fd) {
            mappingData[getMappingDataIndex(columnIndex, 0)] = fd;
        }

        private void setColumnFieldSizePow2(int columnIndex, int fieldSizePow2) {
            mappingData[getMappingDataIndex(columnIndex, 7)] = fieldSizePow2;
        }

        private void setColumnIndexFd(int columnIndex, long fd) {
            mappingData[getMappingDataIndex(columnIndex, 1)] = fd;
        }

        private void setColumnMappingSize(int columnIndex, long size) {
            mappingData[getMappingDataIndex(columnIndex, 3)] = size;
        }

        private void setColumnMappingStart(int columnIndex, long address) {
            mappingData[getMappingDataIndex(columnIndex, 2)] = address;
        }

        private void setColumnNRowsAdded(int columnIndex, long nRowsAdded) {
            mappingData[getMappingDataIndex(columnIndex, 6)] = nRowsAdded;
        }

        private void setColumnStartOffset(int columnIndex, long offset) {
            mappingData[getMappingDataIndex(columnIndex, 4)] = offset;
        }
    }

    public static class TableBlockWriterTaskHolder {
        private TableBlockWriterTask task;
    }

    public static class TableBlockWriterJob extends AbstractQueueConsumerJob<TableBlockWriterTaskHolder> {
        public TableBlockWriterJob(MessageBus messageBus) {
            super(messageBus.getTableBlockWriterQueue(), messageBus.getTableBlockWriterSubSeq());
        }

        @Override
        protected boolean doRun(int workerId, long cursor) {
            try {
                final TableBlockWriterTaskHolder holder = queue.get(cursor);
                boolean useful = holder.task.run();
                holder.task = null;
                return useful;
            } finally {
                subSeq.done(cursor);
            }
        }
    }

    private class PartitionBlockWriter implements Closeable {
        private final PartitionStruct partitionStruct = new PartitionStruct();
        private final LongList columnTops = new LongList();
        private final Path path = new Path();
        private long timestampLo;
        private long timestampHi;
        private boolean opened;

        @Override
        public void close() {
            clear();
            path.close();
        }

        private void openPartition() {
            assert !opened;
            partitionStruct.of(columnCount);
            path.of(root).concat(writer.getTableName());
            timestampHi = TableUtils.setPathForPartition(path, partitionBy, timestampLo, true);
            int plen = path.length();
            try {
                if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
                    throw CairoException.instance(ff.errno()).put("Could not create directory: ").put(path);
                }

                assert columnCount > 0;
                columnTops.setAll(columnCount, -1);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    final CharSequence name = metadata.getColumnName(columnIndex);
                    final long appendOffset = writer.getPrimaryAppendOffset(timestampLo, columnIndex);
                    partitionStruct.setColumnStartOffset(columnIndex, appendOffset);
                    partitionStruct.setColumnAppendOffset(columnIndex, appendOffset);

                    partitionStruct.setColumnDataFd(columnIndex, TableUtils.openFileRWOrFail(ff, TableUtils.dFile(path.trimTo(plen), name)));
                    int columnType = metadata.getColumnType(columnIndex);
                    if (ColumnType.isVariableLength(columnType)) {
                        partitionStruct.setColumnIndexFd(columnIndex, TableUtils.openFileRWOrFail(ff, iFile(path.trimTo(plen), name)));
                        partitionStruct.setColumnFieldSizePow2(columnIndex, -1);
                    } else {
                        partitionStruct.setColumnIndexFd(columnIndex, -1);
                        partitionStruct.setColumnFieldSizePow2(columnIndex, ColumnType.pow2SizeOf(columnType));
                    }
                }

                opened = true;
                LOG.info().$("opened partition to '").$(path).$('\'').$();
            } catch (Throwable ex) {
                closePartition();
                throw ex;
            } finally {
                path.trimTo(plen);
            }
        }

        private void closePartition() {
            try {
                int i;
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    long fd = partitionStruct.getColumnDataFd(columnIndex);
                    if (fd > 0) {
                        ff.close(fd);
                    }
                    fd = partitionStruct.getColumnIndexFd(columnIndex);
                    if (fd > 0) {
                        ff.close(fd);
                    }
                    long address = partitionStruct.getColumnMappingStart(columnIndex);
                    if (address != 0) {
                        long sz = partitionStruct.getColumnMappingSize(columnIndex);
                        unmapFile(ff, address, sz);
                        partitionStruct.setColumnMappingStart(columnIndex, 0);
                    }
                }
                int nAdditionalMappings = partitionStruct.getnAdditionalMappings();
                for (i = 0; i < nAdditionalMappings; i++) {
                    long address = partitionStruct.getAdditionalMappingStart(i);
                    long sz = partitionStruct.getAdditionalMappingSize(i);
                    unmapFile(ff, address, sz);
                }
            } finally {
                partitionStruct.clear();
                opened = false;
            }
        }

        private void appendPageFrameColumn(int columnIndex, long pageFrameSize, long sourceAddress) {
            if (sourceAddress != 0) {
                long appendOffset = partitionStruct.getColumnAppendOffset(columnIndex);
                long nextAppendOffset = appendOffset + pageFrameSize;
                partitionStruct.setColumnAppendOffset(columnIndex, nextAppendOffset);

                long destAddress;
                long columnStartAddress = partitionStruct.getColumnMappingStart(columnIndex);
                if (columnStartAddress == 0) {
                    assert appendOffset == partitionStruct.getColumnStartOffset(columnIndex);
                    long mapSz = Math.max(pageFrameSize, ff.getMapPageSize());
                    long address = mapFile(ff, partitionStruct.getColumnDataFd(columnIndex), appendOffset, mapSz);
                    partitionStruct.setColumnMappingStart(columnIndex, address);
                    partitionStruct.setColumnMappingSize(columnIndex, mapSz);
                    columnStartAddress = address;
                    destAddress = columnStartAddress;
                } else {
                    long initialOffset = partitionStruct.getColumnStartOffset(columnIndex);
                    assert initialOffset < appendOffset;
                    final long minMapSz = nextAppendOffset - initialOffset;
                    if (minMapSz > partitionStruct.getColumnMappingSize(columnIndex)) {
                        partitionStruct.addAdditionalMapping(
                                partitionStruct.getColumnMappingStart(columnIndex),
                                partitionStruct.getColumnMappingSize(columnIndex)
                        );
                        final long address = mapFile(
                                ff,
                                partitionStruct.getColumnDataFd(columnIndex),
                                partitionStruct.getColumnStartOffset(columnIndex),
                                minMapSz
                        );
                        partitionStruct.setColumnMappingStart(columnIndex, address);
                        partitionStruct.setColumnMappingSize(columnIndex, minMapSz);
                    }
                    destAddress = partitionStruct.getColumnMappingStart(columnIndex) + appendOffset - initialOffset;
                }

                TableBlockWriterTask task = getConcurrentTask();
                task.assignAppendPageFrameColumn(destAddress, pageFrameSize, sourceAddress);
                enqueueConcurrentTask(task);
            } else {
                partWriter.setColumnTop(columnIndex, pageFrameSize);
            }
        }

        private void cancel() {
            clear();
        }

        private void clear() {
            if (opened) {
                closePartition();
            }
            columnTops.clear();
        }

        private void completeCommitAppendedBlock() {
            long nRowsAdded = 0;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                long nColRowsAdded = partitionStruct.getColumnNRowsAdded(columnIndex);
                assert nColRowsAdded >= 0;
                if (nColRowsAdded > nRowsAdded) {
                    nRowsAdded = nColRowsAdded;
                }
            }
            long blockLastTimestamp = Math.min(timestampHi, lastTimestamp);
            LOG.info().$("committing ").$(nRowsAdded).$(" rows to partition at ").$(path).$(" [firstTimestamp=").$ts(timestampLo).$(", lastTimestamp=").$ts(timestampHi).$(']').$();
            writer.startAppendedBlock(timestampLo, blockLastTimestamp, nRowsAdded, columnTops);
        }

        private void completeUpdateSymbolCache(int columnIndex, long colNRowsAdded) {
            final long address = partitionStruct.getColumnMappingStart(columnIndex);
            assert address > 0;
            final int nSymbols = Vect.maxInt(address, colNRowsAdded) + 1;
            SymbolMapWriter symWriter = writer.getSymbolMapWriter(columnIndex);
            if (nSymbols > symWriter.getSymbolCount()) {
                symWriter.commitAppendedBlock(nSymbols - symWriter.getSymbolCount());
            }
        }

        private void of(long timestampLo) {
            this.timestampLo = timestampLo;
            openPartition();
            columnTops.ensureCapacity(columnCount);
        }

        private void setColumnTop(int columnIndex, long columnTop) {
            columnTops.set(columnIndex, columnTop);
        }

        private void startCommitAppendedBlock() {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int columnType = metadata.getColumnType(columnIndex);
                long offsetLo = partitionStruct.getColumnStartOffset(columnIndex);
                long offsetHi = partitionStruct.getColumnAppendOffset(columnIndex);

                // Add binary and string indexes
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.STRING:
                    case ColumnType.BINARY: {
                        TableBlockWriterTask task = getConcurrentTask();
                        if (offsetHi != offsetLo) {
                            long columnDataAddressLo = partitionStruct.getColumnMappingStart(columnIndex);
                            assert offsetHi - offsetLo <= partitionStruct.getColumnMappingSize(columnIndex);
                            long columnDataAddressHi = columnDataAddressLo + offsetHi - offsetLo;

                            long indexFd = partitionStruct.getColumnIndexFd(columnIndex);
                            long indexOffsetLo = writer.getSecondaryAppendOffset(timestampLo, columnIndex);

                            if (ColumnType.isString(columnType)) {
                                task.assignUpdateStringIndex(columnDataAddressLo, columnDataAddressHi, offsetLo, indexFd, indexOffsetLo, columnIndex, partitionStruct);
                            } else {
                                task.assignUpdateBinaryIndex(columnDataAddressLo, columnDataAddressHi, offsetLo, indexFd, indexOffsetLo, columnIndex, partitionStruct);
                            }
                            partitionStruct.setColumnNRowsAdded(columnIndex, -1);
                            enqueueConcurrentTask(task);
                        } else {
                            partitionStruct.setColumnNRowsAdded(columnIndex, 0);
                        }
                        break;
                    }

                    case ColumnType.SYMBOL: {
                        long colNRowsAdded = (offsetHi - offsetLo) >> partitionStruct.getColumnFieldSizePow2(columnIndex);
                        partitionStruct.setColumnNRowsAdded(columnIndex, colNRowsAdded);
                        completeUpdateSymbolCache(columnIndex, colNRowsAdded);
                        break;
                    }

                    default: {
                        long colNRowsAdded = (offsetHi - offsetLo) >> partitionStruct.getColumnFieldSizePow2(columnIndex);
                        partitionStruct.setColumnNRowsAdded(columnIndex, colNRowsAdded);
                        break;
                    }
                }
            }
        }

        private void startPageFrame(long timestamp) {
            assert opened;
            assert timestamp == Long.MIN_VALUE || timestamp >= timestampLo;
            assert timestamp <= timestampHi;
            timestampLo = timestamp;
        }
    }

    private class TableBlockWriterTask {
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private TaskType taskType;
        private long sourceAddress;
        private long sourceSizeOrEnd;
        private long destAddress;
        private long sourceInitialOffset;
        private long indexFd;
        private long indexOffsetLo;
        private int columnIndex;
        private PartitionStruct partitionStruct;

        private void assignAppendPageFrameColumn(long destAddress, long pageFrameLength, long sourceAddress) {
            taskType = TaskType.AppendBlock;
            this.destAddress = destAddress;
            this.sourceSizeOrEnd = pageFrameLength;
            this.sourceAddress = sourceAddress;
        }

        private void assignUpdateBinaryIndex(
                long columnDataAddressLo,
                long columnDataAddressHi,
                long columnDataOffsetLo,
                long indexFd,
                long indexOffsetLo,
                int columnIndex,
                PartitionStruct partitionStruct
        ) {
            taskType = TaskType.GenerateBinaryIndex;
            this.sourceAddress = columnDataAddressLo;
            this.sourceSizeOrEnd = columnDataAddressHi;
            this.sourceInitialOffset = columnDataOffsetLo;
            this.indexFd = indexFd;
            this.indexOffsetLo = indexOffsetLo;
            this.columnIndex = columnIndex;
            this.partitionStruct = partitionStruct;
        }

        private void assignUpdateStringIndex(
                long columnDataAddressLo,
                long columnDataAddressHi,
                long columnDataOffsetLo,
                long indexFd,
                long indexOffsetLo,
                int columnIndex,
                PartitionStruct partitionStruct
        ) {
            taskType = TaskType.GenerateStringIndex;
            this.sourceAddress = columnDataAddressLo;
            this.sourceSizeOrEnd = columnDataAddressHi;
            this.sourceInitialOffset = columnDataOffsetLo;
            this.indexFd = indexFd;
            this.indexOffsetLo = indexOffsetLo;
            this.columnIndex = columnIndex;
            this.partitionStruct = partitionStruct;
        }

        private void cancel() {
            if (ready.compareAndSet(true, false)) {
                nCompletedConcurrentTasks.incrementAndGet();
            }
        }

        private void completeUpdateBinaryIndex(
                long columnDataAddressLo,
                long columnDataAddressHi,
                long columnDataOffsetLo,
                long indexFd,
                long indexOffsetLo,
                int columnIndex,
                PartitionStruct partitionStruct
        ) {
            long indexMappingSz = (columnDataAddressHi - columnDataAddressLo);
            long indexMappingStart = mapFile(ff, indexFd, indexOffsetLo, indexMappingSz);

            long offset = columnDataOffsetLo;
            long columnDataAddress = columnDataAddressLo;
            long columnIndexAddress = indexMappingStart;
            long nRowsAdded = 0;
            while (columnDataAddress < columnDataAddressHi) {
                assert columnIndexAddress + Long.BYTES <= (indexMappingStart + indexMappingSz);
                nRowsAdded++;
                Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
                columnIndexAddress += Long.BYTES;
                // TODO: remove branching similar to how this is done for strings
                long binLen = Unsafe.getUnsafe().getLong(columnDataAddress);
                long sz;
                if (binLen == TableUtils.NULL_LEN) {
                    sz = Long.BYTES;
                } else {
                    sz = Long.BYTES + binLen;
                }
                columnDataAddress += sz;
                offset += sz;
            }
            Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
            partitionStruct.setColumnNRowsAdded(columnIndex, nRowsAdded);
            unmapFile(ff, indexMappingStart, indexMappingSz);
        }

        private void completeUpdateStringIndex(
                long columnDataAddressLo,
                long columnDataAddressHi,
                long columnDataOffsetLo,
                long indexFd,
                long indexOffsetLo,
                int columnIndex,
                PartitionStruct partitionStruct
        ) {
            final long indexMappingSz = (columnDataAddressHi - columnDataAddressLo) * 2;
            final long indexMappingStart = mapFile(ff, indexFd, indexOffsetLo, indexMappingSz);
            long offset = columnDataOffsetLo;
            long columnDataAddress = columnDataAddressLo;
            long columnIndexAddress = indexMappingStart;
            long nRowsAdded = 0;
            while (columnDataAddress < columnDataAddressHi) {
                assert columnIndexAddress + Long.BYTES <= (indexMappingStart + indexMappingSz);
                nRowsAdded++;
                Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
                columnIndexAddress += Long.BYTES;
                final int strLen = Unsafe.getUnsafe().getInt(columnDataAddress);
                // +1 the length will turn NULL_LEN into 0
                final long bit = ((strLen >>> 30) & 0x02) ^ 0x02; // our sign bit is now bit #1
                // so null will evaluate to just VirtualMemory.STRING_LENGTH_BYTES
                // but for positive length values we need to subtract 2
                // how do we do that? Lets use inverted sign bit
                final long sz = (Vm.STRING_LENGTH_BYTES + 2L * (strLen + 1) - bit);
                columnDataAddress += sz;
                offset += sz;
            }
            Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
            partitionStruct.setColumnNRowsAdded(columnIndex, nRowsAdded);
            unmapFile(ff, indexMappingStart, indexMappingSz);
        }

        private boolean run() {
            if (ready.compareAndSet(true, false)) {
                try {
                    switch (taskType) {
                        case AppendBlock:
                            Vect.memcpy(sourceAddress, destAddress, sourceSizeOrEnd);
                            return true;

                        case GenerateStringIndex:
                            completeUpdateStringIndex(sourceAddress, sourceSizeOrEnd, sourceInitialOffset, indexFd, indexOffsetLo, columnIndex, partitionStruct);
                            return true;

                        case GenerateBinaryIndex:
                            completeUpdateBinaryIndex(sourceAddress, sourceSizeOrEnd, sourceInitialOffset, indexFd, indexOffsetLo, columnIndex, partitionStruct);
                            return true;
                    }
                } finally {
                    nCompletedConcurrentTasks.incrementAndGet();
                }
            }

            return false;
        }
    }
}
