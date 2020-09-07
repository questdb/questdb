package io.questdb.cairo;

import java.io.Closeable;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private static final Timestamps.TimestampFloorMethod NO_PARTITIONING_FLOOR = (ts) -> {
        return 0;
    };

    private TableWriter writer;
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final Path path = new Path();

    private int rootLen;
    private RecordMetadata metadata;
    private int columnCount;
    private int partitionBy;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;

    private final LongObjHashMap<PartitionBlockWriter> partitionBlockWriterByTimestamp = new LongObjHashMap<>();
    private final ObjList<PartitionBlockWriter> partitionBlockWriters = new ObjList<>();
    private int nextPartitionBlockWriterIndex;

    TableBlockWriter(CairoConfiguration configuration) {
        root = configuration.getRoot();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
    }

    public void appendBlock(long timestamp, int columnIndex, long blockLength, long sourceAddress) {
        PartitionBlockWriter partWriter = getPartitionBlockWriter(timestamp);
        partWriter.appendBlock(columnIndex, blockLength, sourceAddress);
    }

    public void appendSymbolCharsBlock(int columnIndex, long blockLength, long sourceAddress) {
        writer.getSymbolMapWriter(columnIndex).appendSymbolCharsBlock(blockLength, sourceAddress);
    }

    public void commitAppendedBlock(long firstTimestamp, long lastTimestamp, long nRowsAdded, LongList columnTops) {
        LOG.info().$("committing block write of ").$(nRowsAdded).$(" rows to ").$(path).$(" [firstTimestamp=").$ts(firstTimestamp).$(", lastTimestamp=").$ts(lastTimestamp).$(']').$();
        writer.commitAppendedBlock(firstTimestamp, lastTimestamp, nRowsAdded, columnTops);
        PartitionBlockWriter partWriter = getPartitionBlockWriter(firstTimestamp);
        partWriter.clear();
    }

    void open(TableWriter writer) {
        clear();
        this.writer = writer;
        metadata = writer.getMetadata();
        path.of(root).concat(writer.getName());
        rootLen = path.length();
        columnCount = metadata.getColumnCount();
        partitionBy = writer.getPartitionBy();
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
    }

    void clear() {
        if (null != writer) {
            metadata = null;
            writer = null;
        }
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
        path.close();
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

        partWriter.open();
        return partWriter;
    }

    private class PartitionBlockWriter {
        private final ObjList<AppendMemory> columns = new ObjList<>();
        private long timestampLo;
        private boolean opened;

        private void of(long timestampLo) {
            this.timestampLo = timestampLo;
            opened = false;
            int columnsSize = columns.size();
            while (columnsSize < columnCount) {
                columns.extendAndSet(columnsSize++, new AppendMemory());
            }
        }

        private void open() {
            if (!opened) {
                try {
                    TableUtils.setPathForPartition(path, partitionBy, timestampLo);
                    int plen = path.length();
                    if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                        throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
                    }

                    assert columnCount > 0;
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        final CharSequence name = metadata.getColumnName(columnIndex);
                        AppendMemory mem = columns.getQuick(columnIndex);
                        mem.of(ff, TableUtils.dFile(path.trimTo(plen), name), ff.getMapPageSize());
                        mem.jumpTo(writer.getPrimaryAppendOffset(timestampLo, columnIndex));
                    }
                    opened = true;
                    LOG.info().$("opened partition to '").$(path).$('\'').$();
                } finally {
                    path.trimTo(rootLen);
                }
            }
        }

        private void appendBlock(int columnIndex, long blockLength, long sourceAddress) {
            AppendMemory mem = columns.getQuick(columnIndex);
            long appendOffset = mem.getAppendOffset();
            try {
                mem.putBlockOfBytes(sourceAddress, blockLength);
            } finally {
                mem.jumpTo(appendOffset);
            }
        }

        private void clear() {
            for (int i = 0, sz = columns.size(); i < sz; i++) {
                columns.getQuick(i).close(false);
            }
            opened = false;
        }

        private void close() {
            columns.clear();
            timestampLo = 0;
            opened = false;
        }
    }
}
