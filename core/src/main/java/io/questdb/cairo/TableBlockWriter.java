package io.questdb.cairo;

import static io.questdb.cairo.TableUtils.iFile;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private static final Timestamps.TimestampFloorMethod NO_PARTITIONING_FLOOR = (ts) -> 0;

    private TableWriter writer;
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final RingQueue<TableBlockWriterTaskHolder> queue;
    private final Sequence pubSeq;
    private final LongList columnRowsAdded = new LongList();

    private RecordMetadata metadata;
    private int columnCount;
    private int partitionBy;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;
    private int timestampColumnIndex;
    private long firstTimestamp;
    private long lastTimestamp;

    private final LongObjHashMap<PartitionBlockWriter> partitionBlockWriterByTimestamp = new LongObjHashMap<>();
    private final ObjList<PartitionBlockWriter> partitionBlockWriters = new ObjList<>();
    private int nextPartitionBlockWriterIndex;
    private final ObjList<TableBlockWriterTask> concurrentTasks = new ObjList<>();
    private int nEnqueuedConcurrentTasks;
    private final AtomicInteger nCompletedConcurrentTasks = new AtomicInteger();
    private PartitionBlockWriter partWriter;

    TableBlockWriter(CairoConfiguration configuration, MessageBus messageBus) {
        root = configuration.getRoot();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        queue = messageBus.getTableBlockWriterQueue();
        pubSeq = messageBus.getTableBlockWriterPubSequence();
    }

    public void startPageFrame(long timestampLo) {
        partWriter = getPartitionBlockWriter(timestampLo);
        partWriter.startPageFrame(timestampLo);
    }

    public void appendPageFrameColumn(int columnIndex, long pageFrameLength, long sourceAddress, long pageFrameNRows) {
        LOG.info().$("appending data").$(" [tableName=").$(writer.getName()).$(", columnIndex=").$(columnIndex).$(", pageFrameLength=").$(pageFrameLength).$(", pageFrameNRows=")
                .$(pageFrameNRows).$(']').$();
        if (columnIndex == timestampColumnIndex) {
            long firstBlockTimestamp = Unsafe.getUnsafe().getLong(sourceAddress);
            if (firstBlockTimestamp < firstTimestamp) {
                firstTimestamp = firstBlockTimestamp;
            }
            long addr = sourceAddress + pageFrameLength - Long.BYTES;
            long lastBlockTimestamp = Unsafe.getUnsafe().getLong(addr);
            if (lastBlockTimestamp > lastTimestamp) {
                lastTimestamp = lastBlockTimestamp;
            }
        }
        partWriter.appendPageFrameColumn(columnIndex, pageFrameLength, sourceAddress, pageFrameNRows);
    }

    private TableBlockWriterTask getConcurrentTask() {
        if (concurrentTasks.size() <= nEnqueuedConcurrentTasks) {
            concurrentTasks.extendAndSet(nEnqueuedConcurrentTasks, new TableBlockWriterTask());
        }
        return concurrentTasks.getQuick(nEnqueuedConcurrentTasks);
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

    public void commit() {
        LOG.info().$("committing block write").$(" [tableName=").$(writer.getName()).$(", firstTimestamp=").$ts(firstTimestamp).$(", lastTimestamp=").$ts(lastTimestamp).$(']').$();
        // Need to complete all data tasks before we can start index tasks
        completePendingConcurrentTasks(false);
        long nTotalRowsAdded = 0;
        for (int n = 0; n < nextPartitionBlockWriterIndex; n++) {
            PartitionBlockWriter partWriter = partitionBlockWriters.get(n);
            partWriter.startCommitAppendedBlock();
            nTotalRowsAdded += partWriter.nRowsAdded;
        }
        completePendingConcurrentTasks(false);
        writer.commitBlock(firstTimestamp, lastTimestamp, nTotalRowsAdded);
        LOG.info().$("committed new block [table=").$(writer.getName()).$(']').$();
        clear();
    }

    public void cancel() {
        completePendingConcurrentTasks(true);
        writer.cancelRow();
        for (int n = 0; n < nextPartitionBlockWriterIndex; n++) {
            PartitionBlockWriter partWriter = partitionBlockWriters.get(n);
            partWriter.cancel();
        }
        writer.purgeUnusedPartitions();
        LOG.info().$("cancelled new block [table=").$(writer.getName()).$(']').$();
        clear();
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
        LOG.info().$("started new block [table=").$(writer.getName()).$(']').$();
    }

    void clear() {
        if (nCompletedConcurrentTasks.get() < nEnqueuedConcurrentTasks) {
            LOG.error().$("new block should have been either committed or cancelled [table=").$(writer.getName()).$(']').$();
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

    @Override
    public void close() {
        clear();
        for (int i = 0, sz = partitionBlockWriters.size(); i < sz; i++) {
            partitionBlockWriters.getQuick(i).close();
        }
        partitionBlockWriters.clear();
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

    private class PartitionBlockWriter {
        private final PartitionStruct partitionStruct = new PartitionStruct();
        private final LongList columnTops = new LongList();
        private final Path path = new Path();
        private long timestampLo;
        private long timestampHi;
        private long nRowsAdded;
        private long pageFrameMaxNRows;
        private boolean opened;

        private void of(long timestampLo) {
            this.timestampLo = timestampLo;
            opened = false;
            columnTops.ensureCapacity(columnCount);
        }

        private void startPageFrame(long timestamp) {
            if (!opened) {
                partitionStruct.of(columnCount);
                path.of(root).concat(writer.getName());
                timestampHi = TableUtils.setPathForPartition(path, partitionBy, timestampLo);
                int plen = path.length();
                if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                    throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
                }

                assert columnCount > 0;
                columnTops.setAll(columnCount, -1);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    final CharSequence name = metadata.getColumnName(columnIndex);
                    long appendOffset = writer.getPrimaryAppendOffset(timestampLo, columnIndex);
                    partitionStruct.setColumnStartOffset(columnIndex, appendOffset);
                    partitionStruct.setColumnAppendOffset(columnIndex, appendOffset);

                    long fd = ff.openRW(TableUtils.dFile(path.trimTo(plen), name));
                    if (fd == -1) {
                        throw CairoException.instance(ff.errno()).put("Cannot open ").put(name);
                    }
                    partitionStruct.setColumnDataFd(columnIndex, fd);
                    switch (metadata.getColumnType(columnIndex)) {
                        case ColumnType.STRING:
                        case ColumnType.BINARY:
                            fd = ff.openRW(iFile(path.trimTo(plen), name));
                            if (fd == -1) {
                                throw CairoException.instance(ff.errno()).put("Cannot open ").put(name);
                            }
                            partitionStruct.setColumnIndexFd(columnIndex, fd);
                            break;
                        default:
                            partitionStruct.setColumnIndexFd(columnIndex, -1);
                    }
                }

                nRowsAdded = 0;
                pageFrameMaxNRows = 0;
                path.trimTo(plen);
                opened = true;
                LOG.info().$("opened partition to '").$(path).$('\'').$();
            } else {
                nRowsAdded += pageFrameMaxNRows;
                pageFrameMaxNRows = 0;
            }
            assert timestamp == Long.MIN_VALUE || timestamp >= timestampLo;
            assert timestamp <= timestampHi;
            timestampLo = timestamp;
        }

        private void appendPageFrameColumn(int columnIndex, long pageFrameLength, long sourceAddress, long pageFrameNRows) {
            if (pageFrameNRows > pageFrameMaxNRows) {
                pageFrameMaxNRows = pageFrameNRows;
            }
            if (sourceAddress != 0) {
                long appendOffset = partitionStruct.getColumnAppendOffset(columnIndex);
                long nextAppendOffset = appendOffset + pageFrameLength;
                partitionStruct.setColumnAppendOffset(columnIndex, nextAppendOffset);

                long destAddress;
                long columnStartAddress = partitionStruct.getColumnMappingStart(columnIndex);
                if (columnStartAddress == 0) {
                    assert appendOffset == partitionStruct.getColumnStartOffset(columnIndex);
                    long mapSz = Math.max(pageFrameLength, ff.getMapPageSize());
                    long address = mapFile(partitionStruct.getColumnDataFd(columnIndex), appendOffset, mapSz);
                    partitionStruct.setColumnMappingStart(columnIndex, address);
                    partitionStruct.setColumnMappingSize(columnIndex, mapSz);
                    columnStartAddress = address;
                    destAddress = columnStartAddress;
                } else {
                    long initialOffset = partitionStruct.getColumnStartOffset(columnIndex);
                    assert initialOffset < appendOffset;
                    long minMapSz = nextAppendOffset - initialOffset;
                    if (minMapSz > partitionStruct.getColumnMappingSize(columnIndex)) {
                        partitionStruct.addAdditionalMapping(partitionStruct.getColumnMappingStart(columnIndex), partitionStruct.getColumnMappingSize(columnIndex));
                        long address = mapFile(partitionStruct.getColumnDataFd(columnIndex), partitionStruct.getColumnStartOffset(columnIndex), minMapSz);
                        partitionStruct.setColumnMappingStart(columnIndex, address);
                        partitionStruct.setColumnMappingSize(columnIndex, minMapSz);
                    }
                    destAddress = partitionStruct.getColumnMappingStart(columnIndex) + appendOffset - initialOffset;
                }

                TableBlockWriterTask task = getConcurrentTask();
                task.assignAppendPageFrameColumn(destAddress, pageFrameLength, sourceAddress);
                enqueueConcurrentTask(task);
            } else {
                partWriter.setColumnTop(columnIndex, pageFrameLength);
            }
        }

        private void setColumnTop(int columnIndex, long columnTop) {
            columnTops.set(columnIndex, columnTop);
        }

        private void startCommitAppendedBlock() {
            nRowsAdded += pageFrameMaxNRows;
            pageFrameMaxNRows = 0;
            long blockLastTimestamp = Math.min(timestampHi, lastTimestamp);
            LOG.info().$("committing ").$(nRowsAdded).$(" rows to partition at ").$(path).$(" [firstTimestamp=").$ts(timestampLo).$(", lastTimestamp=").$ts(timestampHi).$(']').$();
            writer.startAppendedBlock(timestampLo, blockLastTimestamp, nRowsAdded, columnTops);

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int columnType = metadata.getColumnType(columnIndex);
                long columnTop = columnTops.getQuick(columnIndex);
                long colNRowsAdded = columnTop > 0 ? nRowsAdded - columnTop : nRowsAdded;
                if (colNRowsAdded > 0) {
                    // Add binary and string indexes
                    switch (columnType) {
                        case ColumnType.STRING:
                        case ColumnType.BINARY: {
                            TableBlockWriterTask task = getConcurrentTask();
                            long offsetLo = partitionStruct.getColumnStartOffset(columnIndex);
                            long offsetHi = partitionStruct.getColumnAppendOffset(columnIndex);

                            long columnDataAddressLo = partitionStruct.getColumnMappingStart(columnIndex);
                            assert offsetHi - offsetLo <= partitionStruct.getColumnMappingSize(columnIndex);
                            long columnDataAddressHi = columnDataAddressLo + offsetHi - offsetLo;

                            long indexFd = partitionStruct.getColumnIndexFd(columnIndex);
                            long indexOffsetLo = writer.getSecondaryAppendOffset(timestampLo, columnIndex);
                            long indexMappingSz = Long.BYTES * colNRowsAdded;
                            long indexMappingStart = mapFile(indexFd, indexOffsetLo, indexMappingSz);
                            partitionStruct.addAdditionalMapping(indexMappingStart, indexMappingSz);

                            if (columnType == ColumnType.STRING) {
                                task.assignUpdateStringIndex(columnDataAddressLo, columnDataAddressHi, offsetLo, indexMappingStart, indexMappingStart + indexMappingSz);
                            } else {
                                task.assignUpdateBinaryIndex(columnDataAddressLo, columnDataAddressHi, offsetLo, indexMappingStart);
                            }
                            enqueueConcurrentTask(task);
                            break;
                        }

                        case ColumnType.SYMBOL: {
                            completeUpdateSymbolCache(columnIndex, colNRowsAdded);
                            break;
                        }

                        default:
                    }
                }
            }
        }

        private void completeUpdateSymbolCache(int columnIndex, long colNRowsAdded) {
            long address = partitionStruct.getColumnMappingStart(columnIndex);
            assert address > 0;
            int nSymbols = Vect.maxInt(address, colNRowsAdded);
            nSymbols++;
            SymbolMapWriter symWriter = writer.getSymbolMapWriter(columnIndex);
            if (nSymbols > symWriter.getSymbolCount()) {
                symWriter.commitAppendedBlock(nSymbols - symWriter.getSymbolCount());
            }
        }

        private void cancel() {
            clear();
        }

        private void clear() {
            if (opened) {
                int i = 0;
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    long fd = partitionStruct.getColumnDataFd(columnIndex);
                    ff.close(fd);
                    fd = partitionStruct.getColumnIndexFd(columnIndex);
                    if (fd != -1) {
                        ff.close(fd);
                    }
                    long address = partitionStruct.getColumnMappingStart(columnIndex);
                    if (address != 0) {
                        long sz = partitionStruct.getColumnMappingSize(columnIndex);
                        unmapFile(address, sz);
                        partitionStruct.setColumnMappingStart(columnIndex, 0);
                    }
                }
                int nAdditionalMappings = partitionStruct.getnAdditionalMappings();
                for (i = 0; i < nAdditionalMappings; i++) {
                    long address = partitionStruct.getAdditionalMappingStart(i);
                    long sz = partitionStruct.getAdditionalMappingSize(i);
                    unmapFile(address, sz);
                }
                partitionStruct.clear();
            }
            columnTops.clear();
            opened = false;
        }

        private long mapFile(long fd, final long mapOffset, final long mapSz) {
            long alignedMapOffset = (mapOffset / ff.getPageSize()) * ff.getPageSize();
            long addressOffsetDueToAlignment = mapOffset - alignedMapOffset;
            long alignedMapSz = mapSz + addressOffsetDueToAlignment;
            long fileSz = ff.length(fd);
            long minFileSz = mapOffset + alignedMapSz;
            if (fileSz < minFileSz) {
                if (!ff.truncate(fd, minFileSz)) {
                    throw CairoException.instance(ff.errno()).put("Could not truncate file for append fd=").put(fd).put(", offset=").put(mapOffset).put(", size=")
                            .put(mapSz);
                }
            }
            long address = ff.mmap(fd, alignedMapSz, alignedMapOffset, Files.MAP_RW);
            if (address == -1) {
                int errno = ff.errno();
                throw CairoException.instance(ff.errno()).put("Could not mmap append fd=").put(fd).put(", offset=").put(mapOffset).put(", size=").put(mapSz).put(", errno=")
                        .put(errno);
            }
            assert (address / ff.getPageSize()) * ff.getPageSize() == address; // address MUST be page aligned
            return address + addressOffsetDueToAlignment;
        }

        private void unmapFile(final long address, final long mapSz) {
            long alignedAddress = (address / ff.getPageSize()) * ff.getPageSize();
            long alignedMapSz = mapSz + address - alignedAddress;
            ff.munmap(alignedAddress, alignedMapSz);
        }

        private void close() {
            timestampLo = 0;
            path.close();
            opened = false;
        }
    }

    private static class PartitionStruct {
        private static int MAPPING_STRUCT_ENTRY_SIZE = 8;
        private static int INITIAL_ADDITIONAL_MAPPINGS = 4;
        private long[] mappingData = null;
        private int columnCount;
        private int nAdditionalMappings;

        private void of(int columnCount) {
            this.columnCount = columnCount;
            nAdditionalMappings = 0;
            int sz = columnCount * MAPPING_STRUCT_ENTRY_SIZE;
            if (mappingData == null || mappingData.length < sz) {
                sz += INITIAL_ADDITIONAL_MAPPINGS << 1;
                mappingData = new long[sz];
            }
        }

        private void clear() {
            // No need
        }

        private void setColumnDataFd(int columnIndex, long fd) {
            mappingData[getMappingDataIndex(columnIndex, 0)] = fd;
        }

        private long getColumnDataFd(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 0)];
        }

        private void setColumnIndexFd(int columnIndex, long fd) {
            mappingData[getMappingDataIndex(columnIndex, 1)] = fd;
        }

        private long getColumnIndexFd(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 1)];
        }

        private void setColumnMappingStart(int columnIndex, long address) {
            mappingData[getMappingDataIndex(columnIndex, 2)] = address;
        }

        private long getColumnMappingStart(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 2)];
        }

        private void setColumnMappingSize(int columnIndex, long size) {
            mappingData[getMappingDataIndex(columnIndex, 3)] = size;
        }

        private long getColumnMappingSize(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 3)];
        }

        private void setColumnStartOffset(int columnIndex, long offset) {
            mappingData[getMappingDataIndex(columnIndex, 4)] = offset;
        }

        private long getColumnStartOffset(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 4)];
        }

        private void setColumnAppendOffset(int columnIndex, long offset) {
            mappingData[getMappingDataIndex(columnIndex, 5)] = offset;
        }

        private long getColumnAppendOffset(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 5)];
        }

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

        private int getnAdditionalMappings() {
            return nAdditionalMappings;
        }

        private long getAdditionalMappingStart(int nMapping) {
            int i = getMappingDataIndex(columnCount, nMapping << 1);
            return mappingData[i];
        }

        private long getAdditionalMappingSize(int nMapping) {
            int i = getMappingDataIndex(columnCount, (nMapping << 1) + 1);
            return mappingData[i];
        }

        private int getMappingDataIndex(int columnIndex, int fieldIndex) {
            return columnIndex * MAPPING_STRUCT_ENTRY_SIZE + fieldIndex;
        }
    }

    public static class TableBlockWriterTaskHolder {
        private TableBlockWriterTask task;
    }

    private enum TaskType {
        AppendBlock, GenerateStringIndex, GenerateBinaryIndex
    }

    private class TableBlockWriterTask {
        private TaskType taskType;
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private long sourceAddress;
        private long sourceSizeOrEnd;
        private long destAddress;
        private long destAddressLimit;
        private long sourceInitialOffset;

        private boolean run() {
            if (ready.compareAndSet(true, false)) {
                try {
                    switch (taskType) {
                        case AppendBlock:
                            Unsafe.getUnsafe().copyMemory(sourceAddress, destAddress, sourceSizeOrEnd);
                            return true;

                        case GenerateStringIndex:
                            completeUpdateStringIndex(sourceAddress, sourceSizeOrEnd, sourceInitialOffset, destAddress, destAddressLimit);
                            return true;

                        case GenerateBinaryIndex:
                            completeUpdateBinaryIndex(sourceAddress, sourceSizeOrEnd, sourceInitialOffset, destAddress);
                            return true;
                    }
                } finally {
                    nCompletedConcurrentTasks.incrementAndGet();
                }
            }

            return false;
        }

        private void cancel() {
            if (ready.compareAndSet(true, false)) {
                nCompletedConcurrentTasks.incrementAndGet();
            }
        }

        private void assignAppendPageFrameColumn(long destAddress, long pageFrameLength, long sourceAddress) {
            taskType = TaskType.AppendBlock;
            this.destAddress = destAddress;
            this.sourceSizeOrEnd = pageFrameLength;
            this.sourceAddress = sourceAddress;
        }

        private void assignUpdateStringIndex(long columnDataAddressLo, long columnDataAddressHi, long columnDataOffsetLo, long columnIndexAddressLo, long columnIndexAddressLimit) {
            taskType = TaskType.GenerateStringIndex;
            this.sourceAddress = columnDataAddressLo;
            this.destAddress = columnIndexAddressLo;
            this.destAddressLimit = columnIndexAddressLimit;
            this.sourceSizeOrEnd = columnDataAddressHi;
            this.sourceInitialOffset = columnDataOffsetLo;
        }

        private void completeUpdateStringIndex(long columnDataAddressLo, long columnDataAddressHi, long columnDataOffsetLo, long columnIndexAddressLo, long columnIndexAddressLimit) {
            long offset = columnDataOffsetLo;
            long columnDataAddress = columnDataAddressLo;
            long columnIndexAddress = columnIndexAddressLo;
            while (columnDataAddress < columnDataAddressHi) {
                assert columnIndexAddress + Long.BYTES <= columnIndexAddressLimit;
                Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
                columnIndexAddress += Long.BYTES;
                long strLen = Unsafe.getUnsafe().getInt(columnDataAddress);
                long sz;
                if (strLen == TableUtils.NULL_LEN) {
                    sz = VirtualMemory.STRING_LENGTH_BYTES;
                } else {
                    sz = VirtualMemory.STRING_LENGTH_BYTES + 2 * strLen;
                }
                columnDataAddress += sz;
                offset += sz;
            }
        }

        private void assignUpdateBinaryIndex(long columnDataAddressLo, long columnDataAddressHi, long columnDataOffsetLo, long columnIndexAddressLo) {
            taskType = TaskType.GenerateBinaryIndex;
            this.sourceAddress = columnDataAddressLo;
            this.destAddress = columnIndexAddressLo;
            this.sourceSizeOrEnd = columnDataAddressHi;
            this.sourceInitialOffset = columnDataOffsetLo;
        }

        private void completeUpdateBinaryIndex(long columnDataAddressLo, long columnDataAddressHi, long columnDataOffsetLo, long columnIndexAddressLo) {
            long offset = columnDataOffsetLo;
            long columnDataAddress = columnDataAddressLo;
            long columnIndexAddress = columnIndexAddressLo;
            while (columnDataAddress < columnDataAddressHi) {
                Unsafe.getUnsafe().putLong(columnIndexAddress, offset);
                columnIndexAddress += Long.BYTES;
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
        }
    }

    public static class TableBlockWriterJob implements Job {
        private final RingQueue<TableBlockWriterTaskHolder> queue;
        private final Sequence subSeq;

        public TableBlockWriterJob(MessageBus messageBus) {
            this.queue = messageBus.getTableBlockWriterQueue();
            this.subSeq = messageBus.getTableBlockWriterSubSequence();
        }

        @Override
        public boolean run(int workerId) {
            boolean useful = false;
            while (true) {
                long cursor = subSeq.next();
                if (cursor >= 0) {
                    try {
                        final TableBlockWriterTaskHolder holder = queue.get(cursor);
                        useful |= holder.task.run();
                        holder.task = null;
                    } finally {
                        subSeq.done(cursor);
                    }
                }

                if (cursor == -1) {
                    return useful;
                }
            }
        }
    }
}
