/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.*;

public class SampleByFirstLastRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final TimestampSampler timestampSampler;
    private final GenericRecordMetadata groupByMetadata;
    private final int[] recordFirstLastIndex;
    private final int timestampIndex;
    private static final long BUFF_SIZE = 4096;
    private final DirectLongList startTimestampOutAddress;
    private final DirectLongList lastTimestampOutAddress;
    private final DirectLongList firstRowIdOutAddress;
    private final DirectLongList lastRowIdOutAddress;
    private final SingleSymbolFilter symbolFilter;
    private final int gropuBySymbolColIndex;
    private final DirectLongList crossFrameRow;

    public SampleByFirstLastRecordCursorFactory(
            RecordCursorFactory base,
            TimestampSampler timestampSampler,
            GenericRecordMetadata groupByMetadata,
            ObjList<QueryColumn> columns,
            int timestampIndex,
            SingleSymbolFilter symbolFilter) throws SqlException {
        this.base = base;
        this.timestampSampler = timestampSampler;
        this.groupByMetadata = groupByMetadata;
        this.timestampIndex = timestampIndex;
        this.startTimestampOutAddress = new DirectLongList(BUFF_SIZE).setSize(BUFF_SIZE);
        this.firstRowIdOutAddress = new DirectLongList(BUFF_SIZE).setSize(BUFF_SIZE);
        this.lastRowIdOutAddress = new DirectLongList(BUFF_SIZE).setSize(BUFF_SIZE);
        this.lastTimestampOutAddress = new DirectLongList(BUFF_SIZE).setSize(BUFF_SIZE);
        this.symbolFilter = symbolFilter;
        this.recordFirstLastIndex = buildFirstLastIndex(columns, symbolFilter.getColumnIndex(), timestampIndex);
        gropuBySymbolColIndex = symbolFilter.getColumnIndex();
        crossFrameRow = new DirectLongList(recordFirstLastIndex.length).setSize(recordFirstLastIndex.length);
    }

    private int[] buildFirstLastIndex(ObjList<QueryColumn> columns, int symbolIndex, int timestampIndex) throws SqlException {
        int[] colFirstLastIndex = new int[columns.size()];
        for (int i = 0; i < colFirstLastIndex.length; i++) {
            if (i != symbolIndex && i != timestampIndex) {
                QueryColumn column = columns.getQuick(i);
                ExpressionNode ast = column.getAst();
                if (Chars.equalsIgnoreCase(ast.token, "last")) {
                    colFirstLastIndex[i] = 1;
                } else if (!Chars.equalsIgnoreCase(ast.token, "first")) {
                    throw SqlException.$(ast.position, "expected first() or last() functions but got ").put(ast.token);
                }
            }
        }
        return colFirstLastIndex;
    }

    @Override
    public void close() {
        base.close();
        Misc.free(startTimestampOutAddress);
        Misc.free(lastTimestampOutAddress);
        Misc.free(firstRowIdOutAddress);
        Misc.free(lastRowIdOutAddress);
        Misc.free(crossFrameRow);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext);
        int groupByIndexKey = symbolFilter.getSymbolFilterKey();
        if (groupByIndexKey == SymbolMapReader.VALUE_NOT_FOUND) {
            Misc.free(pageFrameCursor);
            return EmptyTableRecordCursor.INSTANCE;
        }
        return new SampleByFirstLastRecordCursor(pageFrameCursor, groupByIndexKey);
    }

    @Override
    public RecordMetadata getMetadata() {
        return groupByMetadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class SampleByFirstLastRecordCursor implements RecordCursor {
        private final int groupBySymbolKey;
        private PageFrameCursor pageFrameCursor;
        private final SymbolMapReader symbolsReader;
        private PageFrame currentFrame;
        private final SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private long currentRow;
        private long rowsFound;
        private long outStartIndex;
        private IndexFrameCursor indexCursor;
        private long frameFirstRowId = -1;
        private long firstTimestamp = -1;
        private long pageRowSize = -1;
        private IndexFrame indexFrame;
        private long indexFrameIndex = -1;
        private BitmapIndexReader symbolIndexReader;
        private long frameNextRowId = -1;
        private int partitionIndex = Numbers.INT_NaN;
        private final long[] firstLastRowId = new long[2];

        public SampleByFirstLastRecordCursor(PageFrameCursor pageFrameCursor, int groupBySymbolKey) {
            this.pageFrameCursor = pageFrameCursor;
            this.symbolsReader = pageFrameCursor.getSymbolMapReader(gropuBySymbolColIndex);
            this.groupBySymbolKey = groupBySymbolKey;
        }

        @Override
        public void close() {
            pageFrameCursor = Misc.free(pageFrameCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return pageFrameCursor.getSymbolMapReader(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (++currentRow < rowsFound - 1) {
                record.of(currentRow);
                return true;
            }

            do {
                if (rowsFound > outStartIndex) {
                    saveFirstLastValuesToBuffer(rowsFound - 1);
                }

                if (pageRowSize <= (frameNextRowId - frameFirstRowId)) {
                    currentFrame = pageFrameCursor.next();
                    if (currentFrame != null) {
                        // Switch to new data frame
                        pageRowSize = currentFrame.getPageSize(timestampIndex) / Long.BYTES;
                        frameNextRowId = frameFirstRowId = currentFrame.getFirstRowId();

                        if (partitionIndex != currentFrame.getPartitionIndex()) {
                            // Switch index to next partition
                            symbolIndexReader = currentFrame.getBitmapIndexReader(gropuBySymbolColIndex, BitmapIndexReader.DIR_FORWARD);
                            indexCursor = null;
                            partitionIndex = currentFrame.getPartitionIndex();
                        }
                    }
                }

                if (currentFrame == null) {
                    // return cross border final row if exists
                    if (rowsFound > 0) {
                        record.of(0);
                        currentRow = 0;
                        rowsFound = 0;
                        pageRowSize = Long.MAX_VALUE;
                        return true;
                    } else {
                        return false;
                    }
                }

                currentRow = 0;
            } while (!findInFrame(currentFrame));

            if (currentRow < rowsFound - 1) {
                record.of(currentRow);
                return true;
            }

            // bottom row of the result set
            // save values and don't return unless it's last row ever

            return false;
        }

        private boolean findInFrame(PageFrame currentFrame) {
            long timestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - frameFirstRowId * Long.BYTES;
            long lastFrameRowId = frameFirstRowId + pageRowSize;

            if (indexCursor == null) {
                indexCursor = symbolIndexReader.getFrameCursor(groupBySymbolKey, frameFirstRowId, lastFrameRowId);
                indexFrame = null;
            }

            if (indexFrame == null || indexFrameIndex >= indexFrame.getSize()) {
                indexFrame = indexCursor.getNext();
                indexFrameIndex = 0;
                if (indexFrame.getSize() == 0) {
                    // Go to next data frame
                    frameNextRowId = frameFirstRowId + pageRowSize;
                    return false;
                }
            }

            if (firstTimestamp == -1 && indexFrame.getSize() > 0) {
                long rowId = Unsafe.getUnsafe().getLong(indexFrame.getAddress());
                firstTimestamp = Unsafe.getUnsafe().getLong(timestampColumnAddress + rowId * Long.BYTES);
            }

            outStartIndex = Math.min(rowsFound - outStartIndex, 1);
            if (outStartIndex > 0 && rowsFound > 1) {
                // Copy last set of first(), last() rowids from bottom of the previous output to the top
                long lastFoundIndex = rowsFound - 1;
                firstRowIdOutAddress.set(0, firstRowIdOutAddress.get(lastFoundIndex));
                lastRowIdOutAddress.set(0, lastRowIdOutAddress.get(lastFoundIndex));
                startTimestampOutAddress.set(0, startTimestampOutAddress.get(lastFoundIndex));
            }

            long lastRowUpdateCheck = lastTimestampOutAddress.get(0);
            rowsFound = Vect.findFirstLastInFrame(
                    outStartIndex,
                    firstTimestamp,
                    frameFirstRowId,
                    pageRowSize + frameFirstRowId,
                    timestampColumnAddress,
                    timestampSampler,
                    indexFrame.getAddress(),
                    indexFrame.getSize(),
                    indexFrameIndex,
                    startTimestampOutAddress.getAddress(),
                    lastTimestampOutAddress.getAddress(),
                    firstRowIdOutAddress.getAddress(),
                    lastRowIdOutAddress.getAddress(),
                    BUFF_SIZE);

            // When first() and last() rowids are on different index or data pages
            // we need to do more passes before knowing that last() rowid is really last
            // and next data frame / index frame will not change it
            // Hide last returned row from the cursor output
            // and copy it back to index 0 for next frame result set setting outIndex to 1
            // so that Vect code continues setting last() to the top output row
            if (outStartIndex > 0) {
                if (lastRowUpdateCheck != lastTimestampOutAddress.get(0)) {
                    saveLastValuesToBuffer(0);
                }
            } else if (rowsFound > 0) {
                saveFirstLastValuesToBuffer(0);
            }

            if (rowsFound > outStartIndex) {
                assert rowsFound < BUFF_SIZE;

                // Read output timestamp and rowId to resume at the end of the output buffers
                indexFrameIndex = firstRowIdOutAddress.get(rowsFound);
                if (indexFrameIndex >= indexFrame.getSize() - 1) {
                    indexFrame = null;
                }
                long nextFirstRowId = lastRowIdOutAddress.get(rowsFound);
                assert nextFirstRowId > frameNextRowId;
                frameNextRowId = nextFirstRowId;
                firstTimestamp = startTimestampOutAddress.get(rowsFound);

                return rowsFound > 1;
            }

            // index frame done but nothing found
            indexFrame = null;
            return false;
        }

        private void saveFirstLastValuesToBuffer(long outRow) {
            firstLastRowId[0] = firstRowIdOutAddress.get(outRow) - frameFirstRowId;
            firstLastRowId[1] = lastRowIdOutAddress.get(outRow) - frameFirstRowId;
            for (int i = 0; i < recordFirstLastIndex.length; i++) {
                if (i == timestampIndex) {
                    crossFrameRow.set(i, startTimestampOutAddress.get(outRow));
                } else {
                    saveFixedColToBufferWithLongAlignment(i, crossFrameRow, groupByMetadata.getColumnType(i), currentFrame.getPageAddress(i), firstLastRowId[recordFirstLastIndex[i]]);
                }
            }
        }


        private void saveLastValuesToBuffer(long outRow) {
            long lastRowId = lastRowIdOutAddress.get(outRow) - frameFirstRowId;
            for (int i = 0; i < recordFirstLastIndex.length; i++) {
                if (recordFirstLastIndex[i] == 1) {
                    // last() values only
                    assert currentFrame.getPageSize(i) > lastRowId;
                    saveFixedColToBufferWithLongAlignment(i, crossFrameRow, groupByMetadata.getColumnType(i), currentFrame.getPageAddress(i), lastRowId);
                }
            }
        }

        private void saveFixedColToBufferWithLongAlignment(int index, DirectLongList saveBufferAddress, int columnType, long pageAddress, long rowId) {
            switch (ColumnType.pow2SizeOf(columnType)) {
                case 3:
                    saveBufferAddress.set(index, Unsafe.getUnsafe().getLong(pageAddress + rowId * Long.BYTES));
                    break;
                case 2:
                    saveBufferAddress.set(index, Unsafe.getUnsafe().getInt(pageAddress + rowId * Integer.BYTES));
                    break;
                case 1:
                    saveBufferAddress.set(index, Unsafe.getUnsafe().getShort(pageAddress + rowId * Short.BYTES));
                    break;
                case 0:
                    saveBufferAddress.set(index, Unsafe.getUnsafe().getByte(pageAddress + rowId));
                    break;

                default:
                    throw new CairoException().put("first(), last() cannot be used with column type ").put(ColumnType.nameOf(columnType));
            }
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            currentFrame = null;
            currentRow = 0;
            rowsFound = 0;
            frameNextRowId = frameFirstRowId = -1;
            firstTimestamp = -1;
            pageRowSize = -1;
            indexFrame = null;
            indexFrameIndex = -1;
            indexCursor = null;
            outStartIndex = 0;
            pageFrameCursor.toTop();
        }

        @Override
        public long size() {
            return -1;
        }

        private class SampleByFirstLastRecord implements Record {
            private long currentRow;

            public SampleByFirstLastRecord of(long currentRow) {
                this.currentRow = currentRow;
                firstLastRowId[0] = firstRowIdOutAddress.get(currentRow) - frameFirstRowId;
                firstLastRowId[1] = lastRowIdOutAddress.get(currentRow) - frameFirstRowId;
                return this;
            }

            @Override
            public long getTimestamp(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow.getAddress() + col * Long.BYTES);
                }

                if (col == timestampIndex) {
                    // Special case - timestamp the sample by runs on
                    // Take it from timestampOutBuff instead of column
                    // It's the value of the beginning of the group, not where the first row found
                    return Unsafe.getUnsafe().getLong(startTimestampOutAddress.getAddress() + currentRow * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
            }

            @Override
            public long getLong(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow.getAddress() + col * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
            }

            @Override
            public double getDouble(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getDouble(crossFrameRow.getAddress() + col * Long.BYTES);
                }

                return Unsafe.getUnsafe().getDouble(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Double.BYTES);
            }

            @Override
            public CharSequence getSym(int col) {
                int symbolId;
                if (currentRow == 0) {
                    symbolId = Unsafe.getUnsafe().getInt(crossFrameRow.getAddress() + col * Long.BYTES);
                } else {
                    symbolId = Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
                }
                return symbolsReader.valueBOf(symbolId);
            }

            @Override
            public int getInt(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getInt(crossFrameRow.getAddress() + col * Long.BYTES);
                }
                return Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
            }
        }
    }
}
