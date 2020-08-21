package io.questdb.cairo;

import java.io.Closeable;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private TableWriter writer;
    private final ObjList<AppendMemory> columns = new ObjList<>();
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final Path path = new Path();

    private int rootLen;
    private RecordMetadata metadata;
    private int columnCount;
    private int partitionBy;
    private long partitionLo;
    private long partitionHi;

    public TableBlockWriter(CairoConfiguration configuration) {
        root = configuration.getRoot();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
    }

    public void appendBlock(long timestamp, int columnIndex, long blockLength, long sourceAddress) {
        if (timestamp < partitionLo && partitionLo != Long.MAX_VALUE) {
            throw CairoException.instance(0).put("can only append [timestamp=").put(timestamp).put(", partitionLo=").put(partitionLo).put(']');
        }

        if (timestamp > partitionHi || partitionLo == Long.MAX_VALUE) {
            openPartition(timestamp);
        }

        AppendMemory mem = columns.getQuick(columnIndex);
        long appendOffset = mem.getAppendOffset();
        try {
            mem.putBlockOfBytes(sourceAddress, blockLength);
        } finally {
            mem.jumpTo(appendOffset);
        }
    }

    public void appendSymbolCharsBlock(int columnIndex, long blockLength, long sourceAddress) {
        writer.getSymbolMapWriter(columnIndex).appendSymbolCharsBlock(blockLength, sourceAddress);
    }

    public void commitAppendedBlock(long firstTimestamp, long lastTimestamp, long nRowsAdded) {
        LOG.info().$("committing block write of ").$(nRowsAdded).$(" rows to ").$(path).$(" [firstTimestamp=").$ts(firstTimestamp).$(", lastTimestamp=").$ts(lastTimestamp).$(']').$();
        writer.commitAppendedBlock(firstTimestamp, lastTimestamp, nRowsAdded);
        reset();
    }

    private void openPartition(long timestamp) {
        try {
            partitionLo = timestamp;
            partitionHi = TableUtils.setPathForPartition(path, partitionBy, timestamp);
            int plen = path.length();
            if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
            }

            assert columnCount > 0;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final CharSequence name = metadata.getColumnName(columnIndex);
                AppendMemory mem = columns.getQuick(columnIndex);
                mem.of(ff, TableUtils.dFile(path.trimTo(plen), name), ff.getMapPageSize());
                mem.jumpTo(writer.getPrimaryAppendOffset(timestamp, columnIndex));
            }
            LOG.info().$("switched partition to '").$(path).$('\'').$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    public void of(TableWriter writer) {
        clear();
        this.writer = writer;
        metadata = writer.getMetadata();
        path.of(root).concat(writer.getName());
        rootLen = path.length();
        columnCount = metadata.getColumnCount();
        partitionBy = writer.getPartitionBy();
        int columnsSize = columns.size();
        while (columnsSize < columnCount) {
            columns.extendAndSet(columnsSize++, new AppendMemory());
        }
    }

    public void clear() {
        if (null != writer) {
            writer.close();
            metadata = null;
            writer = null;
        }
        reset();
    }

    private void reset() {
        for (int i = 0, sz = columns.size(); i < sz; i++) {
            columns.getQuick(i).close(false);
        }
        partitionLo = Long.MAX_VALUE;
        partitionHi = Long.MIN_VALUE;
    }

    @Override
    public void close() {
        clear();
        columns.clear();
        path.close();
    }
}
