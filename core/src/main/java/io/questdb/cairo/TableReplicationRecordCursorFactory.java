package io.questdb.cairo;

import org.jetbrains.annotations.Nullable;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

public class TableReplicationRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TableReplicationRecordCursor cursor;
    private final CairoEngine engine;
    private final CharSequence tableName;

    private static final RecordMetadata createMetadata(CairoEngine engine, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName, -1)) {
            return GenericRecordMetadata.copyOf(reader.getMetadata());
        }
    }

    public TableReplicationRecordCursorFactory(CairoEngine engine, CharSequence tableName) {
        super(createMetadata(engine, tableName));
        this.cursor = new TableReplicationRecordCursor();
        this.engine = engine;
        this.tableName = tableName;
    }

    @Override
    public TableReplicationRecordCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        return cursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName));
    }

    public TableReplicationRecordCursor getPageFrameCursor() {
        return cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName));
    }

    @Override
    public void close() {
        Misc.free(cursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    public static class TableReplicationRecordCursor implements PageFrameCursor {
        private final LongList columnFrameAddresses = new LongList();
        private final LongList columnFramesLengths = new LongList();
        private final ReplicationPageFrame frame = new ReplicationPageFrame();
        private TableReader reader;
        private int partitionIndex;
        private int partitionCount;
        private int columnCount;
        private int timestampColumnIndex;
        private long nFrameRows;
        private long firstTimestamp = Long.MIN_VALUE;
        private long lastTimestamp = Numbers.LONG_NaN;;

        private TableReplicationRecordCursor() {
            super();
        }

        @Override
        public void close() {
            if (null != reader) {
                reader = Misc.free(reader);
                reader = null;
            }
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolMapReader(columnIndex);
        }

        private TableReplicationRecordCursor of(TableReader reader) {
            this.reader = reader;
            columnCount = reader.getMetadata().getColumnCount();
            timestampColumnIndex = reader.getMetadata().getTimestampIndex();
            columnFrameAddresses.ensureCapacity(columnCount);
            columnFramesLengths.ensureCapacity(columnCount);
            toTop();
            return this;
        }

        @Override
        public @Nullable ReplicationPageFrame next() {
            while (++partitionIndex < partitionCount) {
                nFrameRows = reader.openPartition(partitionIndex);
                if (nFrameRows > 0) {
                    final int base = reader.getColumnBase(partitionIndex);
                    for (int i = 0; i < columnCount; i++) {
                        final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, i));
                        assert col.getPageCount() == 1;
                        long columnPageAddress = col.getPageAddress(0);
                        long columnPageLength;

                        int columnType = reader.getMetadata().getColumnType(i);
                        switch (columnType) {
                            case ColumnType.STRING: {
                                final ReadOnlyColumn strLenCol = reader.getColumn(TableReader.getPrimaryColumnIndex(base, i) + 1);
                                long lastStrLenOffset = (nFrameRows - 1) << 3;
                                long lastStrOffset = strLenCol.getLong(lastStrLenOffset);
                                int lastStrLen = col.getStrLen(lastStrOffset);
                                if (lastStrLen == TableUtils.NULL_LEN) {
                                    lastStrLen = 0;
                                }
                                columnPageLength = lastStrOffset + VirtualMemory.STRING_LENGTH_BYTES + lastStrLen * 2;
                                break;
                            }

                            case ColumnType.BINARY: {
                                final ReadOnlyColumn strLenCol = reader.getColumn(TableReader.getPrimaryColumnIndex(base, i) + 1);
                                long lastBinLenOffset = (nFrameRows - 1) << 3;
                                long lastBinOffset = strLenCol.getLong(lastBinLenOffset);
                                long lastBinLen = col.getBinLen(lastBinOffset);
                                if (lastBinLen == TableUtils.NULL_LEN) {
                                    lastBinLen = 0;
                                }
                                columnPageLength = lastBinOffset + Long.BYTES + lastBinLen;
                                break;
                            }

                            default: {
                                int columnSizeBinaryPower = Numbers.msb(ColumnType.sizeOf(reader.getMetadata().getColumnType(i)));
                                columnPageLength = nFrameRows << columnSizeBinaryPower;
                            }

                        }

                        columnFrameAddresses.setQuick(i, columnPageAddress);
                        columnFramesLengths.setQuick(i, columnPageLength);

                        if (timestampColumnIndex == i) {
                            firstTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress);
                            lastTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress + columnPageLength - Long.BYTES);
                        }
                    }
                    return frame;
                }
            }
            return null;
        }

        @Override
        public void toTop() {
            partitionIndex = -1;
            partitionCount = reader.getPartitionCount();
            firstTimestamp = Long.MIN_VALUE;
            lastTimestamp = 0;
        }

        @Override
        public long size() {
            return reader.size();
        }

        public void from(int partitionIndex, long partitionRow) {
            // TODO: Replication
            if (partitionIndex != 0 || partitionRow != 0) {
                throw new RuntimeException("Not implemented");
            }
        }

        public class ReplicationPageFrame implements PageFrame {

            @Override
            public long getPageAddress(int columnIndex) {
                return columnFrameAddresses.getQuick(columnIndex);
            }

            @Override
            public long getPageValueCount(int columnIndex) {
                return nFrameRows;
            }

            @Override
            public long getFirstTimestamp() {
                return firstTimestamp;
            }

            @Override
            public long getLastTimestamp() {
                return lastTimestamp;
            }

            public int getPartitionIndex() {
                return partitionIndex;
            }

            @Override
            public long getPageLength(int columnIndex) {
                return columnFramesLengths.getQuick(columnIndex);
            }
        }
    }
}
