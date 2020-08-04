package io.questdb.cairo;

import java.io.Closeable;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.microtime.Timestamps;
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
            setStateForTimestamp(firstTimestamp, false);
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
            setStateForTimestamp(timestamp, true);
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

    /**
     * Sets path member variable to partition directory for the given timestamp and
     * partitionLo and partitionHi to partition interval in millis. These values are
     * determined based on input timestamp and value of partitionBy. For any given
     * timestamp this method will determine either day, month or year interval timestamp falls to.
     * Partition directory name is ISO string of interval start.
     * <p>
     * Because this method modifies "path" member variable, be sure path is trimmed to original
     * state within try..finally block.
     *
     * @param timestamp               to determine interval for
     * @param updatePartitionInterval flag indicating that partition interval partitionLo and
     *                                partitionHi have to be updated as well.
     */
    private void setStateForTimestamp(long timestamp, boolean updatePartitionInterval) {
        int y, m, d;
        boolean leap;
        path.put(Files.SEPARATOR);
        switch (partitionBy) {
            case PartitionBy.DAY:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                d = Timestamps.getDayOfMonth(timestamp, y, m, leap);
                TimestampFormatUtils.append000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);
                path.put('-');
                TimestampFormatUtils.append0(path, d);

                if (updatePartitionInterval) {
                    partitionHi = Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + (d - 1) * Timestamps.DAY_MICROS + 24 * Timestamps.HOUR_MICROS - 1;
                }
                break;
            case PartitionBy.MONTH:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                TimestampFormatUtils.append000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);

                if (updatePartitionInterval) {
                    partitionHi = Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + Timestamps.getDaysPerMonth(m, leap) * 24L * Timestamps.HOUR_MICROS - 1;
                }
                break;
            case PartitionBy.YEAR:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                TimestampFormatUtils.append000(path, y);
                if (updatePartitionInterval) {
                    partitionHi = Timestamps.addYear(Timestamps.yearMicros(y, leap), 1) - 1;
                }
                break;
            default:
                path.put(TableUtils.DEFAULT_PARTITION_NAME);
                partitionHi = Long.MAX_VALUE;
                break;
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
