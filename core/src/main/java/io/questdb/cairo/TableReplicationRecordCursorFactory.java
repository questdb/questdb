package io.questdb.cairo;

import org.jetbrains.annotations.Nullable;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
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
        this.cursor = new TableReplicationRecordCursor(getMetadata());
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
        private final LongList columnPageNextAddress = new LongList();
        private final LongList columnPageAddress = new LongList();
        private final ReplicationPageFrame frame = new ReplicationPageFrame();
        private final LongList topsRemaining = new LongList();
        private final IntList pages = new IntList();
        private final RecordMetadata metadata;
        private TableReader reader;
        private int partitionIndex;
        private int partitionCount;
        private final LongList pageSizes = new LongList();
        private long pageValueCount;
        private long partitionRemaining = 0L;
        private long firstTimestamp = Long.MIN_VALUE;
        private long lastTimestamp = Numbers.LONG_NaN;;

        private TableReplicationRecordCursor(RecordMetadata metadata) {
            super();
            this.metadata = metadata;
        }

        @Override
        public void close() {
            reader = Misc.free(reader);
            reader = null;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolMapReader(columnIndex);
        }

        @Override
        public @Nullable ReplicationPageFrame next() {

            if (partitionIndex > -1) {
                final long m = computePageMin(reader.getColumnBase(partitionIndex));
                if (m < Long.MAX_VALUE) {
                    return computeFrame(m);
                }
            }

            while (++partitionIndex < partitionCount) {
                partitionRemaining = reader.openPartition(partitionIndex);
                if (partitionRemaining > 0) {
                    final int base = reader.getColumnBase(partitionIndex);
                    // copy table tops
                    for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                        final int columnIndex = i;
                        topsRemaining.setQuick(i, reader.getColumnTop(base, columnIndex));
                        pages.setQuick(i, 0);
                        pageSizes.setQuick(i, -1L);
                    }

                    return computeFrame(computePageMin(base));
                }
            }
            return null;
        }

        @Override
        public void toTop() {
            this.partitionIndex = -1;
            this.partitionCount = reader.getPartitionCount();
            pages.setAll(metadata.getColumnCount(), 0);
            topsRemaining.setAll(metadata.getColumnCount(), 0);
            columnPageAddress.setAll(metadata.getColumnCount(), 0);
            columnPageNextAddress.setAll(metadata.getColumnCount(), 0);
            pageSizes.setAll(metadata.getColumnCount(), -1L);
            pageValueCount = 0;
            firstTimestamp = Long.MIN_VALUE;
            lastTimestamp = 0;
        }

        @Override
        public long size() {
            return reader.size();
        }

        private TableReplicationRecordCursor of(TableReader reader) {
            this.reader = reader;
            toTop();
            return this;
        }

        private TableReplicationRecordCursor of(TableReader reader, long fromRow, long toRow) {
            this.reader = reader;
            toTop();
            return this;
        }

        private ReplicationPageFrame computeFrame(long min) {
            int timestampIndex = reader.getMetadata().getTimestampIndex();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                final int columnIndex = i;
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    topsRemaining.setQuick(i, top - min);
                    columnPageAddress.setQuick(columnIndex, 0);
                } else {
                    long addr = columnPageNextAddress.getQuick(i);
                    long psz = pageSizes.getQuick(i);
                    pageSizes.setQuick(i, psz - min);
                    columnPageAddress.setQuick(i, addr);
                    int columnSize = Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(columnIndex)));
                    columnPageNextAddress.setQuick(i, addr + (min << columnSize));
                }

                if (timestampIndex == columnIndex) {
                    firstTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress.get(timestampIndex));
                    lastTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress.get(timestampIndex) + (Long.BYTES * (min - 1)));
                }
            }

            pageValueCount = min;
            partitionRemaining -= min;
            return frame;
        }

        private long computePageMin(int base) {
            // find min frame length
            long min = Long.MAX_VALUE;
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    if (min > top) {
                        min = top;
                    }
                } else {
                    long psz = pageSizes.getQuick(i);
                    if (psz > 0) {
                        if (min > psz) {
                            min = psz;
                        }
                    } else if (partitionRemaining > 0) {
                        final int page = pages.getQuick(i);
                        pages.setQuick(i, page + 1);
                        final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, i));
                        // page size is liable to change after it is mapped
                        // it is important to map page first and call pageSize() after
                        columnPageNextAddress.setQuick(i, col.getPageAddress(page));
                        int columnSize = Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i)));
                        psz = !(col instanceof NullColumn) ? col.getPageSize(page) >> columnSize : partitionRemaining;
                        final long m = Math.min(psz, partitionRemaining);
                        pageSizes.setQuick(i, m);
                        if (min > m) {
                            min = m;
                        }
                    }
                }
            }
            return min;
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
                return columnPageAddress.getQuick(columnIndex);
            }

            @Override
            public long getPageValueCount(int columnIndex) {
                return pageValueCount;
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
                return 0;
            }
        }
    }
}
