package io.questdb.cairo;

import java.io.Closeable;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class TableBlockWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableBlockWriter.class);
    private TableWriter writer;
    private final ObjList<AppendMemory> columns = new ObjList<>();
    private final CharSequence root;
    private final FilesFacade ff;
    private final int mkDirMode;
    private final Path path = new Path();
    private long tempMem8b = Unsafe.malloc(8);

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

    public void putBlock(long timestamp, int columnIndex, long blockOffset, long blockLength, long sourceAddress) {
        if (timestamp < partitionLo && partitionLo != Long.MAX_VALUE) {
            throw CairoException.instance(0).put("can only append [timestamp=").put(timestamp).put(", partitionLo=").put(partitionLo).put(']');
        }

        if (timestamp > partitionHi || partitionLo == Long.MAX_VALUE) {
            openPartition(timestamp);
        }

        AppendMemory mem = columns.getQuick(columnIndex);
        long appendOffset = mem.getAppendOffset();
        try {
            mem.jumpTo(blockOffset);
            mem.putBlockOfBytes(sourceAddress, blockLength);
            long currentOffset = mem.getAppendOffset();
            if (currentOffset > appendOffset) {
                appendOffset = currentOffset;
            }
        } finally {
            // Make sure append offset is always at the end even if the block was not, this ensures that out of order calls to putBlock dont
            // lead to a truncate part way through the data
            mem.jumpTo(appendOffset);
        }
    }

    public void commitAppendedBlock(long firstTimestamp, long lastTimestamp, long nRowsAdded) {
        for (int i = 0; i < columnCount; i++) {
            columns.get(i).close();
        }
        try {
            TableUtils.setPathForPartition(path, partitionBy, firstTimestamp);
            long nFirstRow = TableUtils.readPartitionSize(ff, path, tempMem8b);
            TableUtils.writePartitionSize(ff, path, tempMem8b, nFirstRow + nRowsAdded);
            writer.commitAppendedBlock(firstTimestamp, lastTimestamp, nFirstRow, nRowsAdded);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartition(long timestamp) {
        try {
            partitionLo = timestamp;
            partitionHi = TableUtils.setPathForPartition(path, partitionBy, timestamp);
            int plen = path.length();
            if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
            }
            if (!ff.exists(path.trimTo(plen).concat(TableUtils.ARCHIVE_FILE_NAME).$())) {
                TableUtils.writePartitionSize(ff, path, tempMem8b, 0);
            }

            assert columnCount > 0;

            for (int i = 0; i < columnCount; i++) {
                final CharSequence name = metadata.getColumnName(i);
                AppendMemory mem = columns.getQuick(i);
                mem.of(ff, TableUtils.dFile(path.trimTo(plen), name), ff.getMapPageSize());
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
        partitionLo = Long.MAX_VALUE;
        partitionHi = Long.MIN_VALUE;
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
    }

    @Override
    public void close() {
        if (tempMem8b != 0) {
            clear();
            for (int i = 0, sz = columns.size(); i < sz; i++) {
                columns.getQuick(i).close();
            }
            columns.clear();
            path.close();
            Unsafe.free(tempMem8b, 8);
            tempMem8b = 0;
        }
    }
}
