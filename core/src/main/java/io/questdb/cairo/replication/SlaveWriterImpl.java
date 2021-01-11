package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableBlockWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

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
    private ObjList<SymbolDetails> symbolDetailsByColumnIndex = new ObjList<>();
    private LongObjHashMap<PartitionDetails> partitionByTimestamp = new LongObjHashMap<>();
    private ObjList<PartitionDetails> usedPartitions = new ObjList<>();
    private ObjList<PartitionDetails> partitionCache = new ObjList<>();
    private TableWriter writer;
    private int columnCount;
    private TableBlockWriter blockWriter;
    private final AtomicInteger nRemainingFrames = new AtomicInteger();
    private volatile PartitionDetails cachedPartition;
    private long firstTimeStamp;
    private final AtomicBoolean writerLock = new AtomicBoolean(false);


    public SlaveWriterImpl(CairoConfiguration configuration) {
        root = configuration.getRoot();
        ff = configuration.getFilesFacade();
        mkDirMode = configuration.getMkDirMode();
        pageSize = configuration.getAppendPageSize();
    }

    public SlaveWriterImpl of(TableWriter writer) {
        assert usedPartitions.size() == 0; // Expect to be be cleared
        this.writer = writer;
        columnCount = writer.getMetadata().getColumnCount();
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

        for (int columnIndex = 0, sz = columnCount; columnIndex < sz; columnIndex++) {
            SymbolDetails symDetails = columnIndex < symbolDetailsByColumnIndex.size() ? symbolDetailsByColumnIndex.getQuick(columnIndex) : null;
            if (writer.getMetadata().getColumnType(columnIndex) == ColumnType.SYMBOL) {
                if (null == symDetails) {
                    symDetails = new SymbolDetails();
                    symbolDetailsByColumnIndex.extendAndSet(columnIndex, symDetails);

                }
                symDetails.of(writer.getMetadata().getColumnName(columnIndex), writer.getSymbolMapWriter(columnIndex).getCharMemSize());
            } else {
                if (null != symDetails) {
                    symDetails.of(null, -1);
                }
            }
        }

        resetCommit();
        return this;
    }

    private void resetCommit() {
        firstTimeStamp = Long.MAX_VALUE;
    }

    @Override
    public void commit() {
        lockWriter();
        try {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                SymbolDetails symbolDetails = columnIndex < symbolDetailsByColumnIndex.size() ? symbolDetailsByColumnIndex.get(columnIndex) : null;
                if (null != symbolDetails) {
                    symbolDetails.commit();
                }
            }
            blockWriter.commit();
            resetCommit();
        } finally {
            unlockWriter();
        }
    }

    @Override
    public void cancel() {
        // TODO
    }

    @Override
    public void clear() {
        if (null != writer) {
            for (int i = 0, sz = usedPartitions.size(); i < sz; i++) {
                PartitionDetails partitionDetails = usedPartitions.getQuick(i);
                partitionDetails.clear();
                partitionCache.add(partitionDetails);
            }
            usedPartitions.clear();
            partitionByTimestamp.clear();
            cachedPartition = null;
            for (int columnIndex = 0, sz = symbolDetailsByColumnIndex.size(); columnIndex < sz; columnIndex++) {
                SymbolDetails symDetails = symbolDetailsByColumnIndex.getQuick(columnIndex);
                if (null != symDetails) {
                    symDetails.clear();
                }
            }
            blockWriter.close();
            blockWriter = null;
            // writer.close();
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
    public long getDataMap(long timestamp, int columnIndex, long offset, long size) {
        if (timestamp < firstTimeStamp) {
            firstTimeStamp = timestamp;
        }
        PartitionDetails partition = cachedPartition;
        if (null == partition || timestamp > partition.timestampHi || timestamp < partition.timestampLo) {
            partition = getPartitionDetails(timestamp);
        }
        if (offset !=  Long.MIN_VALUE) {
            return partition.getDataMap(timestamp, columnIndex, offset, size);
        } else {
            // This is column top, create .top file
            partition.writeColumnTop(columnIndex, size);
            return Long.MIN_VALUE;
        }
    }

    @Override
    public long getSymbolDataMap(int columnIndex, long offset, long size) {
        return symbolDetailsByColumnIndex.get(columnIndex).getSymbolDataMap(offset, size);
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
        lockWriter();
        try {
            PartitionDetails lastPartition = null;
            usedPartitions.sort(ORDER_PARTITIONS_IN_TIME_COMPARATOR);
            int nPartitions = usedPartitions.size();
            int nPartition = 0;
            while (nPartition < nPartitions) {
                PartitionDetails partition = usedPartitions.getQuick(nPartition);
                if (nPartition == 0) {
                    blockWriter.startPageFrame(firstTimeStamp);
                } else {
                    blockWriter.startPageFrame(partition.timestampLo);
                }
                boolean isLastPartition = nPartition == nPartitions;
                if (isLastPartition) {
                    partition.preCommit(true);
                    lastPartition = partition;
                } else {
                    LOG.info().$("closing partition [table=").$(writer.getName()).$(", path=").$(path).$(']').$();
                    partition.preCommit(false);
                    partition.clear();
                    partitionCache.add(partition);
                }
                nPartition++;
            }
            usedPartitions.clear();
            partitionByTimestamp.clear();
            if (null != lastPartition) {
                partitionByTimestamp.put(lastPartition.timestampHi, lastPartition);
                usedPartitions.add(lastPartition);
            }
            LOG.info().$("pre-commit complete [table=").$(writer.getName()).$(']').$();
        } finally {
            unlockWriter();
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
            cachedPartition = partitionDetails;
            return partitionDetails;
        } finally {
            path.trimTo(pathRootLen);
        }
    }

    private void lockWriter() {
        while (!writerLock.compareAndSet(false, true)) {
            ;
        }
    }

    private void unlockWriter() {
        writerLock.set(false);
    }

    private class PartitionDetails implements Closeable {
        private Path path = new Path();
        private long timestampLo;
        private long timestampHi;
        private int pathPartitionLen;
        private ObjList<ColumnDetails> columns = new ObjList<>();
        private final PartitionStruct partitionStruct = new PartitionStruct();

        public void writeColumnTop(int columnIndex, long size) {
            columns.getQuick(columnIndex).writeColumnTop(size);
        }

        private PartitionDetails of(long timestampLo, long timestampHi, Path path) {
            this.timestampLo = timestampLo;
            this.timestampHi = timestampHi;
            this.path.of(path);
            this.pathPartitionLen = path.length();
            partitionStruct.of(columnCount);
            if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Could not create directory: ").put(path);
            }
            LOG.info().$("opening partition [table=").$(writer.getName()).$(", path=").$(path).$(']').$();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (columns.size() <= columnIndex) {
                    columns.add(new ColumnDetails(columnIndex));
                }
                columns.get(columnIndex).of();
            }
            return this;
        }

        public void clear() {
            if (writer != null) {
                for (int columnIndex = 0, sz = columnCount; columnIndex < sz; columnIndex++) {
                    long mappingAddress = partitionStruct.getColumnMappingStart(columnIndex);
                    if (mappingAddress != 0) {
                        long mappingSize = partitionStruct.getColumnMappingSize(columnIndex);
                        TableBlockWriter.unmapFile(ff, mappingAddress, mappingSize);
                        ff.close(partitionStruct.getColumnDataFd(columnIndex));
                        partitionStruct.setColumnMappingStart(columnIndex, 0);
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

        private long getDataMap(long timestamp, int columnIndex, long offset, long size) {
            return columns.getQuick(columnIndex).getDataMap(timestamp, offset, size);
        }

        private void preCommit(boolean lastPartition) {
            LOG.info().$("pre-commit partition [table=").$(writer.getName()).$(", lastPartition=").$(lastPartition).$(", path=").$(path).$(']').$();
            for (int nMapping = 0; nMapping < partitionStruct.nAdditionalMappings; nMapping++) {
                TableBlockWriter.unmapFile(ff, partitionStruct.getAdditionalMappingStart(nMapping), partitionStruct.getAdditionalMappingSize(nMapping));
            }
            partitionStruct.clearAdditionalMappings();
            for (int columnIndex = 0, sz = columnCount; columnIndex < sz; columnIndex++) {
                ColumnDetails column = columns.getQuick(columnIndex);
                long dataFd = lastPartition ? -1 : partitionStruct.getColumnDataFd(columnIndex);
                blockWriter.appendCompletedPageFrameColumn(columnIndex, column.writtenSize, partitionStruct.getColumnMappingStart(columnIndex), dataFd,
                        partitionStruct.getColumnMappingSize(columnIndex));
                if (lastPartition) {
                    column.updateColumnDataMapping();
                    column.resetWriting();
                } else {
                    // Mappings and file descriptors will be closed by the block writer on commit
                    partitionStruct.setColumnMappingStart(columnIndex, 0);
                }
            }
        }

        private final AtomicBoolean partitionLock = new AtomicBoolean(false);

        private void lockPartition() {
            while (!partitionLock.compareAndSet(false, true)) {
                ;
            }
        }

        private void unlockPartition() {
            partitionLock.set(false);
        }

        private class ColumnDetails {
            private final int columnIndex;
            private long openMappingStart;
            private long openMappingOffset;
            private long openMappingEndOffset;
            private long writtenMinOffset;;
            private long writtenSize;

            private ColumnDetails(int columnIndex) {
                this.columnIndex = columnIndex;
            }

            public void writeColumnTop(long size) {
                final CharSequence name = writer.getMetadata().getColumnName(columnIndex);
                LPSZ filename = path.trimTo(pathPartitionLen).concat(name).put(".top").$();
                long fd = TableUtils.openFileRWOrFail(ff, filename);
                long tempMem8b = Unsafe.malloc(8);
                try {
                    Unsafe.getUnsafe().putLong(tempMem8b, size);
                    if (ff.append(fd, tempMem8b, Long.BYTES) != Long.BYTES) {
                        throw CairoException.instance(Os.errno()).put("Cannot append ").put(path);
                    }
                } finally {
                    Unsafe.free(tempMem8b, 8);
                    ff.close(fd);
                }
            }

            private ColumnDetails of() {
                openMappingOffset = -1;
                resetWriting();
                return this;
            }

            private void resetWriting() {
                writtenMinOffset = Long.MAX_VALUE;
                writtenSize = 0;
            }

            private long getDataMap(long timestamp, long offset, long size) {
                if (offset < writtenMinOffset) {
                    writtenMinOffset = offset;
                }
                writtenSize += size;
                if (offset < openMappingOffset || (offset + size) > openMappingEndOffset) {
                    mapColumnData(offset, size, writtenMinOffset);
                }
                return openMappingStart - openMappingOffset + offset;
            }

            private void mapColumnData(long offset, long size, long minOffset) {
                lockPartition();
                try {
                    long mappingAddress = partitionStruct.getColumnMappingStart(columnIndex);
                    long mappingOffset;
                    if (mappingAddress != 0) {
                        mappingOffset = partitionStruct.getColumnMappingStartOffset(columnIndex);
                        long mappingSize = partitionStruct.getColumnMappingSize(columnIndex);
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
                    long fd = partitionStruct.getColumnDataFd(columnIndex);
                    if (fd == -1) {
                        final CharSequence name = writer.getMetadata().getColumnName(columnIndex);
                        fd = TableUtils.openFileRWOrFail(ff, TableUtils.dFile(path.trimTo(pathPartitionLen), name));
                        partitionStruct.setColumnDataFd(columnIndex, fd);
                    }

                    long mappingSize = pageSize * (size / pageSize + 1);
                    mappingAddress = TableBlockWriter.mapFile(ff, fd, mappingOffset, mappingSize);
                    partitionStruct.setColumnMappingStart(columnIndex, mappingAddress);
                    partitionStruct.setColumnMappingSize(columnIndex, mappingSize);
                    partitionStruct.setColumnMappingStartOffset(columnIndex, mappingOffset);
                    updateColumnDataMapping();
                } finally {
                    unlockPartition();
                }
            }

            private void updateColumnDataMapping() {
                openMappingStart = partitionStruct.getColumnMappingStart(columnIndex);
                openMappingOffset = partitionStruct.getColumnMappingStartOffset(columnIndex);
                openMappingEndOffset = openMappingOffset + partitionStruct.getColumnMappingSize(columnIndex);
            }
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

    private class SymbolDetails {
        private CharSequence name;
        private long fd = -1;
        private long originalCharSize;
        private long charMappingAddress;
        private long charMappingSize;
        private long newCharSize;

        private SymbolDetails of(CharSequence name, long charSize) {
            assert fd == -1; // Should be cleared
            this.name = name;
            originalCharSize = charSize;
            newCharSize = 0;
            return this;
        }

        private long getSymbolDataMap(long offset, long size) {
            long charSize = offset + size;
            if (newCharSize < charSize) {
                newCharSize = charSize;
            }
            if (fd != -1) {
                assert charMappingAddress != 0;
                if (charSize < originalCharSize + charMappingSize) {
                    return charMappingAddress + offset - originalCharSize;
                }
                TableBlockWriter.unmapFile(ff, charMappingAddress, charMappingSize);
            } else {
                try {
                    fd = ff.openRW(SymbolMapWriter.charFileName(path, name));
                    if (fd < 0) {
                        throw CairoException.instance(ff.errno()).put("could not open char file [table=").put(writer.getName()).put(", name=").put(name).put(", fd=").put(fd)
                                .put(", size=").put(originalCharSize);
                    }
                } finally {
                    path.trimTo(pathRootLen);
                }
            }
            charMappingSize = offset + size - originalCharSize;
            assert charMappingSize > 0;
            charMappingSize = pageSize * (charMappingSize / pageSize + 1);
            charMappingAddress = TableBlockWriter.mapFile(ff, fd, originalCharSize, charMappingSize);
            return charMappingAddress + offset - originalCharSize;
        }

        private void commit() {
            if (fd != -1 && newCharSize > originalCharSize) {
                originalCharSize = newCharSize;
            }
        }

        private void clear() {
            if (fd != -1) {
                if (charMappingAddress != 0) {
                    TableBlockWriter.unmapFile(ff, charMappingAddress, charMappingSize);
                }
                try {
                    if (!ff.truncate(fd, originalCharSize)) {
                        throw CairoException.instance(ff.errno()).put("could not truncate char file for clear [table=").put(writer.getName()).put(", name=").put(name).put(", fd=").put(fd)
                                .put(", size=").put(originalCharSize).put(", error=").put(ff.errno());
                    }
                } finally {
                    charMappingAddress = 0;
                    ff.close(fd);
                    fd = -1;
                }
            }
        }
    }
}
