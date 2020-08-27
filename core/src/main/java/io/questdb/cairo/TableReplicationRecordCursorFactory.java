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
    private final long maxRowsPerFrame;
    private final IntList columnIndexes;
    private final IntList columnSizes;

    private static final RecordMetadata createMetadata(CairoEngine engine, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName, -1)) {
            return GenericRecordMetadata.copyOf(reader.getMetadata());
        }
    }

    public TableReplicationRecordCursorFactory(CairoEngine engine, CharSequence tableName, long maxRowsPerFrame) {
        super(createMetadata(engine, tableName));
        this.maxRowsPerFrame = maxRowsPerFrame;
        this.cursor = new TableReplicationRecordCursor();
        this.engine = engine;
        this.tableName = tableName;

        int nCols = getMetadata().getColumnCount();
        columnIndexes = new IntList(nCols);
        columnSizes = new IntList(nCols);
        for (int columnIndex = 0; columnIndex < nCols; columnIndex++) {
            int type = getMetadata().getColumnType(columnIndex);
            int typeSize = ColumnType.sizeOf(type);
            columnIndexes.add(columnIndex);
            columnSizes.add((Numbers.msb(typeSize)));
        }
    }

    @Override
    public TableReplicationRecordCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        return cursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName), maxRowsPerFrame, -1, columnIndexes, columnSizes);
    }

    public TableReplicationRecordCursor getPageFrameCursorFrom(SqlExecutionContext executionContext, int timestampColumnIndex, long nFirstRow) {
        TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName);
        int partitionIndex = 0;
        int partitionCount = reader.getPartitionCount();
        while (partitionIndex < partitionCount) {
            long partitionRowCount = reader.openPartition(partitionIndex);
            if (nFirstRow < partitionRowCount) {
                break;
            }
            partitionIndex++;
            nFirstRow -= partitionRowCount;
        }
        return cursor.of(reader, maxRowsPerFrame, timestampColumnIndex, columnIndexes, columnSizes, partitionIndex, nFirstRow);
    }

    public TableReplicationRecordCursor getPageFrameCursor(int timestampColumnIndex, int partitionIndex, long partitionRowCount) {
        return cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName), maxRowsPerFrame, timestampColumnIndex, columnIndexes, columnSizes, partitionIndex,
                partitionRowCount);
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
        private final LongList columnFrameLengths = new LongList();
        private final LongList columnTops = new LongList();
        private final ReplicationPageFrame frame = new ReplicationPageFrame();

        private TableReader reader;
        private long maxRowsPerFrame;
        private IntList columnIndexes;
        private IntList columnSizes;
        private int columnCount;

        private boolean moveToNextPartition;
        private int partitionIndex;
        private int partitionCount;
        private int timestampColumnIndex;
        private long frameFirstRow;
        private long nFrameRows;
        private long nPartitionRows;
        private long firstTimestamp = Long.MIN_VALUE;
        private long lastTimestamp = Numbers.LONG_NaN;;
        private int columnBase;
        private boolean checkNFrameRowsForColumnTops;

        public TableReplicationRecordCursor() {
            super();
        }

        @Override
        public void close() {
            if (null != reader) {
                reader = Misc.free(reader);
                reader = null;
                columnIndexes = null;
                columnSizes = null;
            }
        }

        @Override
        public SymbolTable getSymbolTable(int i) {
            return reader.getSymbolMapReader(columnIndexes.getQuick(i));
        }

        private TableReplicationRecordCursor of(
                TableReader reader, long maxRowsPerFrame, int timestampColumnIndex, IntList columnIndexes, IntList columnSizes, int partitionIndex, long partitionRowCount
        ) {
            of(reader, maxRowsPerFrame, timestampColumnIndex, columnIndexes, columnSizes);
            this.partitionIndex = partitionIndex - 1;
            frameFirstRow = partitionRowCount;

            return this;
        }

        public TableReplicationRecordCursor of(TableReader reader, long maxRowsPerFrame, int timestampColumnIndex, IntList columnIndexes, IntList columnSizes) {
            this.reader = reader;
            this.maxRowsPerFrame = maxRowsPerFrame;
            this.columnIndexes = columnIndexes;
            this.columnSizes = columnSizes;
            columnCount = columnIndexes.size();
            this.timestampColumnIndex = timestampColumnIndex;
            columnFrameAddresses.ensureCapacity(columnCount);
            columnFrameLengths.ensureCapacity(columnCount);
            columnTops.ensureCapacity(columnCount);
            toTop();
            return this;
        }

        @Override
        public @Nullable ReplicationPageFrame next() {
            while (!moveToNextPartition || ++partitionIndex < partitionCount) {
                if (moveToNextPartition) {
                    nPartitionRows = reader.openPartition(partitionIndex);
                    if (nPartitionRows <= frameFirstRow) {
                        frameFirstRow = 0;
                        continue;
                    }
                    columnBase = reader.getColumnBase(partitionIndex);
                    checkNFrameRowsForColumnTops = false;
                    for (int i = 0; i < columnCount; i++) {
                        int columnIndex = columnIndexes.get(i);
                        long columnTop = reader.getColumnTop(columnBase, columnIndex);
                        columnTops.setQuick(i, columnTop);
                        if (columnTop > 0) {
                            checkNFrameRowsForColumnTops = true;
                        }
                    }
                }
                nFrameRows = nPartitionRows - frameFirstRow;
                if (nFrameRows > maxRowsPerFrame) {
                    nFrameRows = maxRowsPerFrame;
                }

                if (checkNFrameRowsForColumnTops) {
                    checkNFrameRowsForColumnTops = false;
                    long frameLastRow = frameFirstRow + nFrameRows - 1;
                    for (int i = 0; i < columnCount; i++) {
                        long columnTop = columnTops.getQuick(i);
                        if (columnTop > frameFirstRow) {
                            checkNFrameRowsForColumnTops = true;
                            if (columnTop <= frameLastRow) {
                                nFrameRows = columnTop - frameFirstRow;
                                frameLastRow = frameFirstRow + nFrameRows - 1;
                            }
                        }
                    }
                }

                for (int i = 0; i < columnCount; i++) {
                    int columnIndex = columnIndexes.get(i);
                    final long columnTop = columnTops.getQuick(i);
                    final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, columnIndex));

                    if (columnTop <= frameFirstRow && col.getPageCount() > 0) {
                        assert col.getPageCount() == 1;
                        final long colFrameFirstRow = frameFirstRow - columnTop;
                        final long colFrameLastRow = colFrameFirstRow + nFrameRows;
                        final long colMaxRow = nPartitionRows - columnTop;

                        long columnPageAddress = col.getPageAddress(0);
                        long columnPageLength;

                        int columnType = reader.getMetadata().getColumnType(columnIndex);
                        switch (columnType) {
                            case ColumnType.STRING: {
                                final ReadOnlyColumn strLenCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, columnIndex) + 1);
                                columnPageLength = calculateStringPagePosition(col, strLenCol, colFrameLastRow, colMaxRow);

                                if (colFrameFirstRow > 0) {
                                    long columnPageBegin = calculateStringPagePosition(col, strLenCol, colFrameFirstRow, colMaxRow);
                                    columnPageAddress += columnPageBegin;
                                    columnPageLength -= columnPageBegin;
                                }

                                break;
                            }

                            case ColumnType.BINARY: {
                                final ReadOnlyColumn binLenCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, columnIndex) + 1);
                                columnPageLength = calculateBinaryPagePosition(col, binLenCol, colFrameLastRow, colMaxRow);

                                if (colFrameFirstRow > 0) {
                                    long columnPageBegin = calculateBinaryPagePosition(col, binLenCol, colFrameFirstRow, colMaxRow);
                                    columnPageAddress += columnPageBegin;
                                    columnPageLength -= columnPageBegin;
                                }

                                break;
                            }

                            default: {
                                int columnSizeBinaryPower = columnSizes.getQuick(columnIndex);
                                columnPageLength = colFrameLastRow << columnSizeBinaryPower;
                                if (colFrameFirstRow > 0) {
                                    long columnPageBegin = colFrameFirstRow << columnSizeBinaryPower;
                                    columnPageAddress += columnPageBegin;
                                    columnPageLength -= columnPageBegin;
                                }
                            }
                        }

                        columnFrameAddresses.setQuick(i, columnPageAddress);
                        columnFrameLengths.setQuick(i, columnPageLength);

                        if (timestampColumnIndex == columnIndex) {
                            firstTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress);
                            lastTimestamp = Unsafe.getUnsafe().getLong(columnPageAddress + columnPageLength - Long.BYTES);
                        }
                    } else {
                        columnFrameAddresses.setQuick(i, 0);
                        columnFrameLengths.setQuick(i, 0);
                    }
                }

                frameFirstRow += nFrameRows;
                assert frameFirstRow <= nPartitionRows;
                if (frameFirstRow == nPartitionRows) {
                    moveToNextPartition = true;
                    frameFirstRow = 0;
                } else {
                    moveToNextPartition = false;
                }
                return frame;
            }
            return null;
        }

        private long calculateBinaryPagePosition(final ReadOnlyColumn col, final ReadOnlyColumn binLenCol, long row, long maxRows) {
            assert row > 0;

            if (row < (maxRows - 1)) {
                long binLenOffset = row << 3;
                return binLenCol.getLong(binLenOffset);
            }

            return col.getGrownLength();
        }

        private long calculateStringPagePosition(final ReadOnlyColumn col, final ReadOnlyColumn strLenCol, long row, long maxRows) {
            assert row > 0;

            if (row < (maxRows - 1)) {
                long strLenOffset = row << 3;
                return strLenCol.getLong(strLenOffset);
            }

            return col.getGrownLength();
        }

        @Override
        public void toTop() {
            partitionIndex = -1;
            moveToNextPartition = true;
            partitionCount = reader.getPartitionCount();
            firstTimestamp = Long.MIN_VALUE;
            lastTimestamp = 0;
        }

        @Override
        public long size() {
            return reader.size();
        }

        public class ReplicationPageFrame implements PageFrame {

            @Override
            public long getPageAddress(int i) {
                return columnFrameAddresses.getQuick(i);
            }

            @Override
            public long getPageValueCount(int i) {
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
            public long getPageLength(int i) {
                return columnFrameLengths.getQuick(i);
            }

            @Override
            public long getColumnTop(int i) {
                return columnTops.getQuick(i);
            }
        }
    }
}
