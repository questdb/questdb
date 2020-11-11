package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableBlockWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

// TODO: Implement fine grain locking
public class SlaveWriterImpl implements SlaveWriter, Closeable {
    private static final Log LOG = LogFactory.getLog(SlaveWriterImpl.class);
    private static final Timestamps.TimestampFloorMethod NO_PARTITIONING_FLOOR = (ts) -> Long.MIN_VALUE;
    private static final Comparator<PartitionDetails> ORDER_PARTITIONS_IN_TIME_COMPARATOR = new Comparator<SlaveWriterImpl.PartitionDetails>() {
        @Override
        public int compare(PartitionDetails o1, PartitionDetails o2) {
            return Long.compare(o1.timestampLo, o2.timestampHi);
        }
    };
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final long pageSize;
    private Path path = new Path();
    private int pathRootLen;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;
    private ObjList<ColumnDetails> columns = new ObjList<>();
    private LongObjHashMap<PartitionDetails> partitionByTimestamp = new LongObjHashMap<>();
    private ObjList<PartitionDetails> usedPartitions = new ObjList<>();
    private ObjList<PartitionDetails> partitionCache = new ObjList<>();
    private TableWriter writer;
    private TableBlockWriter blockWriter;
    private final AtomicInteger nRemainingFrames = new AtomicInteger();

    public SlaveWriterImpl(CairoConfiguration configuration) {
        root = configuration.getRoot();
        ff = configuration.getFilesFacade();
        mkDirMode = configuration.getMkDirMode();
        pageSize = configuration.getAppendPageSize();
    }

    public SlaveWriterImpl of(TableWriter writer) {
        this.writer = writer;
        nRemainingFrames.set(0);
        path.of(root).concat(writer.getName());
        pathRootLen = path.length();
        blockWriter = writer.newBlock();

        switch (writer.getPartitionBy()) {
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

        int columnCount = writer.getMetadata().getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (columns.size() <= columnIndex) {
                columns.add(new ColumnDetails());
            }
            columns.get(columnIndex).of(columnIndex);
        }
        return this;
    }

    @Override
    public void commit() {
        lock();
        try {
            blockWriter.commit();
        } finally {
            unlock();
        }
    }

    @Override
    public void cancel() {
        // TODO
    }

    private void clear() {
        if (null != writer) {
            for (int i = 0, sz = usedPartitions.size(); i < sz; i++) {
                PartitionDetails partitionDetails = usedPartitions.getQuick(i);
                partitionDetails.clear();
                partitionCache.add(partitionDetails);
            }
            usedPartitions.clear();
            partitionByTimestamp.clear();
            blockWriter.close();
            blockWriter = null;
            writer.close();
            writer = null;
        }
    }

    @Override
    public void close() {
        clear();
        if (null != path) {
            for (int i = 0, sz = partitionCache.size(); i < sz; i++) {
                PartitionDetails partitionDetails = partitionCache.getQuick(i);
                partitionDetails.close();
            }
            partitionCache.clear();
            path.close();
            path = null;
        }
    }

    @Override
    public long mapColumnData(long timestamp, int columnIndex, long offset, long size) {
        return columns.get(columnIndex).mapColumnData(timestamp, offset, size);
    }

    private void mapColumnData(ColumnDetails column, long timestamp, long offset, long size, long minOffset) {
        lock();
        try {
            getPartitionDetails(timestamp).mapColumnData(column, offset, size, minOffset);
        } finally {
            unlock();
        }
    }

    @Override
    public boolean completeFrame() {
        if (nRemainingFrames.incrementAndGet() != 0) {
            return false;
        }
        preCommit();
        return true;
    }

    @Override
    public boolean markBlockNFrames(int nFrames) {
        if (nRemainingFrames.addAndGet(-1 * nFrames) != 0) {
            return false;
        }
        preCommit();
        return true;
    }

    private void preCommit() {
        lock();
        try {
            PartitionDetails lastPartition = null;
            usedPartitions.sort(ORDER_PARTITIONS_IN_TIME_COMPARATOR);
            int columnCount = writer.getMetadata().getColumnCount();
            int nPartitions = usedPartitions.size();
            int nPartition = 0;
            while (nPartition < nPartitions) {
                PartitionDetails partitionDetails = usedPartitions.getQuick(nPartition++);
                blockWriter.startPageFrame(partitionDetails.timestampLo);
                boolean isLastPartition = nPartition == nPartitions;
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    ColumnDetails columnDetails = columns.getQuick(columnIndex);
                    long dataFd = isLastPartition ? -1 : partitionDetails.partitionStruct.getColumnDataFd(columnIndex);
                    blockWriter.appendCompletedPageFrameColumn(columnIndex, columnDetails.writtenSize, partitionDetails.partitionStruct.getColumnMappingStart(columnIndex), dataFd,
                            partitionDetails.partitionStruct.getColumnMappingSize(columnIndex));
                }
                if (isLastPartition) {
                    partitionDetails.preCommit(true);
                    lastPartition = partitionDetails;
                } else {
                    partitionDetails.preCommit(false);
                    partitionCache.add(partitionDetails);
                }
            }
            usedPartitions.clear();
            partitionByTimestamp.clear();
            if (null != lastPartition) {
                partitionByTimestamp.put(lastPartition.timestampHi, lastPartition);
                usedPartitions.add(lastPartition);
            }
            LOG.info().$("pre-commit complete [table=").$(writer.getName()).$(']').$();
        } finally {
            unlock();
        }
    }

    private PartitionDetails getPartitionDetails(long timestamp) {
        try {
            long timestampHi = TableUtils.setPathForPartition(path, writer.getPartitionBy(), timestamp);
            PartitionDetails partitionDetails = partitionByTimestamp.get(timestampHi);
            if (null == partitionDetails) {
                int sz = partitionCache.size();
                if (sz > 0) {
                    sz--;
                    partitionDetails = partitionCache.get(sz);
                    partitionCache.remove(sz);
                } else {
                    partitionDetails = new PartitionDetails();
                }
                partitionDetails.of(timestampFloorMethod.floor(timestamp), timestampHi, path);
                partitionByTimestamp.put(timestampHi, partitionDetails);
                usedPartitions.add(partitionDetails);
            }
            return partitionDetails;
        } finally {
            path.trimTo(pathRootLen);
        }
    }

    private final AtomicBoolean lock = new AtomicBoolean(false);

    private void lock() {
        while (!lock.compareAndSet(false, true)) {
            ;
        }
    }

    private void unlock() {
        lock.set(false);
    }

    private class ColumnDetails implements Closeable {
        private int columnIndex;
        private long openTimestampLo;
        private long openTimestampHi;
        private long openMappingStart;
        private long openMappingOffset;
        private long openMappingEndOffset;
        private long writtenMinOffset;;
        private long writtenSize;;

        private ColumnDetails of(int columnIndex) {
            this.columnIndex = columnIndex;
            openMappingOffset = -1;
            resetWriting();
            return this;
        }

        private void resetWriting() {
            writtenMinOffset = Long.MAX_VALUE;
            writtenSize = 0;
        }

        private long mapColumnData(long timestamp, long offset, long size) {
            if (offset < writtenMinOffset) {
                writtenMinOffset = offset;
            }
            writtenSize += size;
            if (timestamp > openTimestampHi || timestamp < openTimestampLo || offset < openMappingOffset || (offset + size) > openMappingEndOffset) {
                SlaveWriterImpl.this.mapColumnData(this, timestamp, offset, size, writtenMinOffset);
            }
            return openMappingStart - openMappingOffset + offset;
        }

        @Override
        public void close() {
        }
    }

    private class PartitionDetails implements Closeable {
        private Path path = new Path();
        private long timestampLo;
        private long timestampHi;
        private int pathPartitionLen;
        private final PartitionStruct partitionStruct = new PartitionStruct();

        private PartitionDetails of(long timestampLo, long timestampHi, Path path) {
            this.timestampLo = timestampLo;
            this.timestampHi = timestampHi;
            this.path.of(path);
            this.pathPartitionLen = path.length();
            partitionStruct.of(writer.getMetadata().getColumnCount());
            if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Could not create directory: ").put(path);
            }
            LOG.info().$("opening partition [table=").$(writer.getName()).$(", path=").$(path).$(']').$();
            return this;
        }

        public void clear() {
            if (writer != null) {
                for (int columnIndex = 0, sz = writer.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                    long mappingAddress = partitionStruct.getColumnMappingStart(columnIndex);
                    if (mappingAddress != 0) {
                        long mappingSize = partitionStruct.getColumnMappingSize(columnIndex);
                        TableBlockWriter.unmapFile(ff, mappingAddress, mappingSize);
                        ff.close(partitionStruct.getColumnDataFd(columnIndex));
                    }
                }
                partitionStruct.clear();
            }
        }

        @Override
        public void close() {
            clear();
            if (null != path) {
                path.close();
                path = null;
            }
        }

        private void mapColumnData(ColumnDetails column, long offset, long size, long minOffset) {
            long mappingAddress = partitionStruct.getColumnMappingStart(column.columnIndex);
            long mappingOffset;
            if (mappingAddress != 0) {
                mappingOffset = partitionStruct.getColumnMappingStartOffset(column.columnIndex);
                long mappingSize = partitionStruct.getColumnMappingSize(column.columnIndex);
                if (mappingOffset <= offset) {
                    long mappingEnd = mappingOffset + mappingSize;
                    long requiredEnd = offset + size;
                    if (requiredEnd <= mappingEnd) {
                        return;
                    }
                }
                partitionStruct.addAdditionalMapping(mappingAddress, mappingSize);
            }

            mappingOffset = minOffset;
            long fd = partitionStruct.getColumnDataFd(column.columnIndex);
            if (fd == -1) {
                final CharSequence name = writer.getMetadata().getColumnName(column.columnIndex);
                fd = TableUtils.openFileRWOrFail(ff, TableUtils.dFile(path.trimTo(pathPartitionLen), name));
                partitionStruct.setColumnDataFd(column.columnIndex, fd);
            }

            long mappingSize = pageSize * (size / pageSize + 1);
            mappingAddress = TableBlockWriter.mapFile(ff, fd, mappingOffset, mappingSize);
            partitionStruct.setColumnMappingStart(column.columnIndex, mappingAddress);
            partitionStruct.setColumnMappingSize(column.columnIndex, mappingSize);
            partitionStruct.setColumnMappingStartOffset(column.columnIndex, mappingOffset);
            updateColumnDataMapping(column);
        }

        private void preCommit(boolean lastPartition) {
            LOG.info().$("pre-commit partition [table=").$(writer.getName()).$(", lastPartition=").$(lastPartition).$(", path=").$(path).$(']').$();
            for (int nMapping = 0; nMapping < partitionStruct.nAdditionalMappings; nMapping++) {
                TableBlockWriter.unmapFile(ff, partitionStruct.getAdditionalMappingStart(nMapping), partitionStruct.getAdditionalMappingSize(nMapping));
            }
            partitionStruct.clearAdditionalMappings();
            if (lastPartition) {
                for (int columnIndex = 0, sz = writer.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                    ColumnDetails column = columns.getQuick(columnIndex);
                    updateColumnDataMapping(column);
                    column.resetWriting();
                }
            } else {
                LOG.info().$("closing partition [table=").$(writer.getName()).$(", path=").$(path).$(']').$();
                clear();
            }
        }

        private void updateColumnDataMapping(ColumnDetails column) {
            column.openTimestampLo = timestampLo;
            column.openTimestampHi = timestampHi;
            column.openMappingStart = partitionStruct.getColumnMappingStart(column.columnIndex);
            column.openMappingOffset = partitionStruct.getColumnMappingStartOffset(column.columnIndex);
            column.openMappingEndOffset = column.openMappingOffset + partitionStruct.getColumnMappingSize(column.columnIndex);
        }
    }

    // TODO: Remove unused
    private static class PartitionStruct {
        private static final int MAPPING_STRUCT_ENTRY_P2 = 3;
        private static final int INITIAL_ADDITIONAL_MAPPINGS = 4;
        private long[] mappingData = null;
        private int columnCount;
        private int nAdditionalMappings;

        private void of(int columnCount) {
            this.columnCount = columnCount;
            nAdditionalMappings = 0;
            int MAPPING_STRUCT_ENTRY_SIZE = 1 << MAPPING_STRUCT_ENTRY_P2;
            int sz = columnCount * MAPPING_STRUCT_ENTRY_SIZE;
            if (mappingData == null || mappingData.length < sz) {
                sz += INITIAL_ADDITIONAL_MAPPINGS << 1;
                mappingData = new long[sz];
            }

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                setColumnDataFd(columnIndex, -1);
            }
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

        private void clearAdditionalMappings() {
            nAdditionalMappings = 0;
        }

        private void clear() {
            // No need
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

        private long getColumnWriteSize(int columnIndex) {
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

        private long getColumnMappingStartOffset(int columnIndex) {
            return mappingData[getMappingDataIndex(columnIndex, 4)];
        }

        private int getMappingDataIndex(int columnIndex, int fieldIndex) {
            return (columnIndex << MAPPING_STRUCT_ENTRY_P2) + fieldIndex;
        }

        private int getnAdditionalMappings() {
            return nAdditionalMappings;
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

        private void setColumnWriteSize(int columnIndex, long size) {
            mappingData[getMappingDataIndex(columnIndex, 1)] = size;
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

        private void setColumnMappingStartOffset(int columnIndex, long offset) {
            mappingData[getMappingDataIndex(columnIndex, 4)] = offset;
        }
    }
}
