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
    private static final int FILTER_KEY_IS_NULL = 0;
    private final RecordCursorFactory base;
    private final TimestampSampler timestampSampler;
    private final GenericRecordMetadata groupByMetadata;
    private final int[] recordFirstLastIndex;
    private final int[] queryToFrameColumnMapping;
    private final int pageSize;
    private final SingleSymbolFilter symbolFilter;
    private final int groupBySymbolColIndex;
    private final int timestampIndex;
    private DirectLongList startTimestampOutAddress;
    private DirectLongList firstRowIdOutAddress;
    private DirectLongList lastRowIdOutAddress;
    private DirectLongList samplePeriodAddress;
    private DirectLongList crossFrameRow;
    private int groupByTimestampIndex = -1;

    public SampleByFirstLastRecordCursorFactory(
            RecordCursorFactory base,
            TimestampSampler timestampSampler,
            GenericRecordMetadata groupByMetadata,
            ObjList<QueryColumn> columns,
            RecordMetadata metadata,
            int timestampIndex,
            SingleSymbolFilter symbolFilter,
            int pageSize) throws SqlException {
        this.base = base;
        assert pageSize > 2;
        this.pageSize = pageSize;
        this.timestampSampler = timestampSampler;
        this.groupByMetadata = groupByMetadata;
        this.timestampIndex = timestampIndex;
        this.startTimestampOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.firstRowIdOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.lastRowIdOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.samplePeriodAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.symbolFilter = symbolFilter;
        this.queryToFrameColumnMapping = new int[columns.size()];
        this.recordFirstLastIndex = new int[columns.size()];
        buildFirstLastIndex(recordFirstLastIndex, queryToFrameColumnMapping, metadata, columns, timestampIndex);
        groupBySymbolColIndex = symbolFilter.getColumnIndex();
        crossFrameRow = new DirectLongList(recordFirstLastIndex.length).setSize(recordFirstLastIndex.length);
    }

    @Override
    public void close() {
        base.close();
        startTimestampOutAddress = Misc.free(startTimestampOutAddress);
        firstRowIdOutAddress = Misc.free(firstRowIdOutAddress);
        lastRowIdOutAddress = Misc.free(lastRowIdOutAddress);
        crossFrameRow = Misc.free(crossFrameRow);
        samplePeriodAddress = Misc.free(samplePeriodAddress);
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

    private void buildFirstLastIndex(
            int[] recordFirstLastIndex,
            int[] queryToFrameColumnMapping,
            RecordMetadata metadata,
            ObjList<QueryColumn> columns,
            int timestampIndex
    ) throws SqlException {
        for (int i = 0; i < recordFirstLastIndex.length; i++) {
            QueryColumn column = columns.getQuick(i);
            ExpressionNode ast = column.getAst();
            if (ast.rhs != null) {
                if (Chars.equalsIgnoreCase(ast.token, "last")) {
                    recordFirstLastIndex[i] = 1;
                } else if (!Chars.equalsIgnoreCase(ast.token, "first")) {
                    throw SqlException.$(ast.position, "expected first() or last() functions but got ").put(ast.token);
                }
                int underlyingColIndex = metadata.getColumnIndex(ast.rhs.token);
                queryToFrameColumnMapping[i] = underlyingColIndex;

                int underlyingType = metadata.getColumnType(underlyingColIndex);
                if (underlyingType != groupByMetadata.getColumnType(i)) {
                    throw SqlException.$(ast.position, "column \"")
                            .put(metadata.getColumnName(underlyingColIndex))
                            .put("\": first(), last() is not supported on data type ")
                            .put(ColumnType.nameOf(underlyingType))
                            .put(" ");
                }
            } else {
                int underlyingColIndex = metadata.getColumnIndex(ast.token);
                queryToFrameColumnMapping[i] = underlyingColIndex;
                if (underlyingColIndex == timestampIndex) {
                    groupByTimestampIndex = i;
                }
            }
        }
    }

    private class SampleByFirstLastRecordCursor implements RecordCursor {
        private final static int STATE_START = 0;
        private final static int STATE_FETCH_NEXT_DATA_FRAME = 1;
        private final static int STATE_FETCH_NEXT_INDEX_FRAME = 3;
        private final static int STATE_OUT_BUFFER_FULL = 4;
        private final static int STATE_SEARCH = 5;
        private final static int STATE_RETURN_LAST_ROW = 6;
        private final static int STATE_DONE = 7;
        private final static int NONE = 0;
        private final static int CROSS_ROW_STATE_SAVED = 1;
        private final static int CROSS_ROW_STATE_REFS_UPDATED = 2;
        private final int groupBySymbolKey;
        private final SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private final long[] firstLastRowId = new long[2];
        private int state;
        private int crossRowState;
        private PageFrameCursor pageFrameCursor;
        private PageFrame currentFrame;
        private long currentRow;
        private long rowsFound;
        private IndexFrameCursor indexCursor;
        private long dataFrameLo = -1;
        private long dataFrameHi = -1;
        private IndexFrame indexFrame;
        private int indexFramePosition = -1;
        private long frameNextRowId = -1;
        private long samplePeriodIndexOffset = 0;
        private long prevSamplePeriodOffset = 0;
        private long samplePeriodStart;

        public SampleByFirstLastRecordCursor(PageFrameCursor pageFrameCursor, int groupBySymbolKey) {
            this.pageFrameCursor = pageFrameCursor;
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
            return pageFrameCursor.getSymbolMapReader(queryToFrameColumnMapping[columnIndex]);
        }

        @Override
        public boolean hasNext() {
            // This loop never returns last found sample by row.
            // The reason is that last row last() value can be changed on next data frame pass.
            // That's why the last row values are buffered
            // (not only rowid stored but all the values needed) in crossFrameRow.
            // Buffering values are unavoidable since rowids of last() and first() are from different data frames
            if (++currentRow < rowsFound - 1) {
                record.of(currentRow);
                return true;
            }

            return hasNext0();
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
            currentRow = rowsFound = 0;
            frameNextRowId = dataFrameLo = dataFrameHi = -1;
            state = STATE_START;
            crossRowState = NONE;
            pageFrameCursor.toTop();
        }

        @Override
        public long size() {
            return -1;
        }

        private void checkCrossRowAfterFoundBufferIterated() {
            if (crossRowState != NONE && rowsFound > 1) {
                // Copy last set of first(), last() RowIds from bottom of the previous output to the top
                long lastFoundIndex = rowsFound - 1;
                firstRowIdOutAddress.set(0, firstRowIdOutAddress.get(lastFoundIndex));
                lastRowIdOutAddress.set(0, lastRowIdOutAddress.get(lastFoundIndex));
                startTimestampOutAddress.set(0, startTimestampOutAddress.get(lastFoundIndex));
                crossRowState = CROSS_ROW_STATE_REFS_UPDATED;
            }

            if (crossRowState == CROSS_ROW_STATE_REFS_UPDATED) {
                saveFirstLastValuesToCrossFrameRowBuffer();
                crossRowState = CROSS_ROW_STATE_SAVED;
            }
        }

        private void checkSaveLastValues(boolean zeroRowLastIdUpdated) {
            if (crossRowState != NONE) {
                if (zeroRowLastIdUpdated) {
                    saveLastValuesToBuffer();
                    crossRowState = CROSS_ROW_STATE_SAVED;
                }
            } else if (rowsFound > 0) {
                saveFirstLastValuesToCrossFrameRowBuffer();
                crossRowState = CROSS_ROW_STATE_SAVED;
            }
        }

        private int fillSamplePeriodsUntil(long lastInDataTimestamp) {
            long currentTs;
            long nextTs = samplePeriodStart;
            int index = 0;

            do {
                currentTs = nextTs;
                nextTs = timestampSampler.nextTimestamp(currentTs);
                samplePeriodAddress.set(index++, currentTs);
            } while (currentTs <= lastInDataTimestamp && index < pageSize);
            return index;
        }

        // This method evaluates RecordCursor state
        // by using state machine.
        // Possible important states are:
        // - START
        // - FETCH_NEXT_DATA_FRAME
        // - FETCH_NEXT_INDEX_FRAME
        // - SEARCH
        // - RETURN_LAST_ROW
        // - DONE
        // State machine can switch states non-linear until DONE reached.
        private int getNextState(final int state) {
            // This method should not change this.state field
            // but instead produce next state given incoming state parameter.
            // This way the method is enforced to change state and less the caller to loop forever.
            switch (state) {
                case STATE_START:
                    prevSamplePeriodOffset = samplePeriodIndexOffset = 0;
                    crossRowState = NONE;
                    samplePeriodStart = Numbers.LONG_NaN;
                    // Fall trough to STATE_FETCH_NEXT_DATA_FRAME;

                case STATE_FETCH_NEXT_DATA_FRAME:
                    currentFrame = pageFrameCursor.next();
                    if (currentFrame != null) {
                        // Switch to new data frame
                        frameNextRowId = dataFrameLo = currentFrame.getFirstRowId();
                        dataFrameHi = dataFrameLo + currentFrame.getPageSize(timestampIndex) / Long.BYTES;

                        // Re-fetch index cursor to correctly position it to frameNextRowId
                        BitmapIndexReader symbolIndexReader = currentFrame.getBitmapIndexReader(groupBySymbolColIndex, BitmapIndexReader.DIR_FORWARD);
                        indexCursor = symbolIndexReader.getFrameCursor(groupBySymbolKey, dataFrameLo, dataFrameHi);

                        // Fall through to STATE_FETCH_NEXT_INDEX_FRAME;
                    } else {
                        return STATE_RETURN_LAST_ROW;
                    }

                case STATE_FETCH_NEXT_INDEX_FRAME:
                    indexFrame = indexCursor.getNext();
                    indexFramePosition = 0;

                    if (indexFrame.getSize() == 0) {
                        if (indexFrame.getAddress() != 0 || groupBySymbolKey != FILTER_KEY_IS_NULL) {
                            // No rows in index for this dataframe left, go to next data frame
                            frameNextRowId = dataFrameHi;
                            // Jump back to fetch next data frame
                            return STATE_FETCH_NEXT_DATA_FRAME;
                        }
                        // Special case - searching with `where symbol = null` on the partition where this column has not been added
                        // Effectively all rows in data frame are the match to the symbol filter
                        // Fall through, search code will figure that this is special case
                    }

                    if (samplePeriodStart == Numbers.LONG_NaN) {
                        long rowId = indexFrame.getAddress() > 0 ? Unsafe.getUnsafe().getLong(indexFrame.getAddress()) : dataFrameLo;
                        long offsetTimestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - dataFrameLo * Long.BYTES;
                        samplePeriodStart = Unsafe.getUnsafe().getLong(offsetTimestampColumnAddress + rowId * Long.BYTES);
                    }
                    // Fall to STATE_SEARCH;

                case STATE_OUT_BUFFER_FULL:
                case STATE_SEARCH:
                    int outPosition = crossRowState == NONE ? 0 : 1;
                    long offsetTimestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - dataFrameLo * Long.BYTES;
                    long lastIndexRowId = indexFrame.getAddress() > 0
                            ? Unsafe.getUnsafe().getLong(indexFrame.getAddress() + (indexFrame.getSize() - 1) * Long.BYTES)
                            : Long.MAX_VALUE;
                    long lastInDataRowId = Math.min(lastIndexRowId, dataFrameHi - 1);
                    long lastInDataTimestamp = Unsafe.getUnsafe().getLong(offsetTimestampColumnAddress + lastInDataRowId * Long.BYTES);
                    int samplePeriodCount = fillSamplePeriodsUntil(lastInDataTimestamp);

                    if (indexFrame.getAddress() > 0) {
                        rowsFound = BitmapIndexUtilsNative.findFirstLastInFrame(
                                outPosition,
                                frameNextRowId,
                                dataFrameHi,
                                offsetTimestampColumnAddress,
                                indexFrame.getAddress(),
                                indexFrame.getSize(),
                                indexFramePosition,
                                samplePeriodAddress.getAddress(),
                                samplePeriodCount,
                                samplePeriodIndexOffset,
                                startTimestampOutAddress.getAddress(),
                                firstRowIdOutAddress.getAddress(),
                                lastRowIdOutAddress.getAddress(),
                                pageSize);
                    } else {
                        // Special case, search on partition where indexed column is not added (or partially added)
                        // with filter `where symbol = null`
                        // It means all rows in the data frame should be treated as in index
                        rowsFound = BitmapIndexUtilsNative.findFirstLastInFrameNoFilter(
                                outPosition,
                                frameNextRowId,
                                dataFrameHi,
                                offsetTimestampColumnAddress,
                                samplePeriodAddress.getAddress(),
                                samplePeriodCount,
                                samplePeriodIndexOffset,
                                startTimestampOutAddress.getAddress(),
                                firstRowIdOutAddress.getAddress(),
                                lastRowIdOutAddress.getAddress(),
                                pageSize);
                    }

                    boolean firstRowLastRowIdUpdated = rowsFound < 0;
                    rowsFound = Math.abs(rowsFound);

                    // If first row last() RowId is updated
                    // re-copy last values to crossFrameRow
                    checkSaveLastValues(firstRowLastRowIdUpdated);

                    if (rowsFound > outPosition) {
                        assert rowsFound < pageSize;

                        // Read positions to resume from the point last search finished
                        indexFramePosition = (int) firstRowIdOutAddress.get(rowsFound);
                        frameNextRowId = lastRowIdOutAddress.get(rowsFound);
                        prevSamplePeriodOffset = samplePeriodIndexOffset;
                        samplePeriodIndexOffset = (int) startTimestampOutAddress.get(rowsFound);
                        samplePeriodStart = samplePeriodAddress.get(samplePeriodIndexOffset - prevSamplePeriodOffset);

                        // decide what to do next
                        int newState;
                        if (rowsFound == pageSize - 1) {
                            // Return values and next pass start from STATE_OUT_BUFFER_FULL
                            newState = STATE_OUT_BUFFER_FULL;
                        } else if (indexFramePosition >= indexFrame.getSize()) {
                            // Index frame exhausted. Next time start from fetching new index frame
                            newState = STATE_FETCH_NEXT_INDEX_FRAME;
                        } else {
                            // Data frame exhausted. Next time start from fetching new data frame
                            newState = STATE_FETCH_NEXT_DATA_FRAME;
                        }

                        if (rowsFound > 1) {
                            // return negative to returns rows back to signal that there are rows to iterate
                            record.of(currentRow = 0);
                            return -newState;
                        }
                        // No rows to iterate, return where to continue from (fetching next data or index frame)
                        return newState;
                    } else {
                        // Noting found, continue search
                        return indexFrame.getAddress() > 0 ? STATE_FETCH_NEXT_INDEX_FRAME : STATE_FETCH_NEXT_DATA_FRAME;
                    }

                case STATE_RETURN_LAST_ROW:
                    if (crossRowState != NONE) {
                        record.of(currentRow = 0);
                        // Signal there is a row by returning negative value
                        return -STATE_DONE;
                    }
                    // Fall through to STATE_DONE;

                case STATE_DONE:
                    return STATE_DONE;

                default:
                    throw new UnsupportedOperationException("Invalid state " + state);
            }
        }

        private boolean hasNext0() {
            // Check if values from the last found sample by row have to be saved in crossFrameRow
            checkCrossRowAfterFoundBufferIterated();
            rowsFound = 0;

            while (state != STATE_DONE) {
                state = getNextState(state);
                if (state < 0) {
                    state = -state;
                    return true;
                }
            }
            return false;
        }

        private void saveFirstLastValuesToCrossFrameRowBuffer() {
            // Copies column values of all columns to cross frame row buffer for found row with index 0
            firstLastRowId[0] = firstRowIdOutAddress.get(0) - dataFrameLo;
            firstLastRowId[1] = lastRowIdOutAddress.get(0) - dataFrameLo;
            for (int i = 0; i < recordFirstLastIndex.length; i++) {
                if (i == groupByTimestampIndex) {
                    long tsIndex = startTimestampOutAddress.get(0) - prevSamplePeriodOffset;
                    crossFrameRow.set(i, samplePeriodAddress.get(tsIndex));
                } else {
                    long rowId = firstLastRowId[recordFirstLastIndex[i]];
                    saveRowIdValueToCrossRow(rowId, i);
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

        private void saveLastValuesToBuffer() {
            // Copies only last() column values of all columns to cross frame row buffer for found row with index 0
            long lastRowId = lastRowIdOutAddress.get(0) - dataFrameLo;
            for (int columnIndex = 0; columnIndex < recordFirstLastIndex.length; columnIndex++) {
                if (recordFirstLastIndex[columnIndex] == 1) {
                    // last() values only
                    int frameColIndex = queryToFrameColumnMapping[columnIndex];
                    assert currentFrame.getPageSize(frameColIndex) > lastRowId;
                    saveRowIdValueToCrossRow(lastRowId, columnIndex);
                }
            }
        }

        private void saveRowIdValueToCrossRow(long lastRowId, int columnIndex) {
            int columnType = groupByMetadata.getColumnType(columnIndex);
            int frameColIndex = queryToFrameColumnMapping[columnIndex];
            long pageAddress = currentFrame.getPageAddress(frameColIndex);
            if (pageAddress > 0) {
                saveFixedColToBufferWithLongAlignment(columnIndex, crossFrameRow, columnType, pageAddress, lastRowId);
            } else {
                crossFrameRow.set(columnIndex, LongNullUtils.LONG_NULLs[columnType]);
            }
        }

        private class SampleByFirstLastRecord implements Record {
            private final SampleByCrossRecord crossRecord = new SampleByCrossRecord();
            private final SampleByDataRecord dataRecord = new SampleByDataRecord();

            private Record currentRecord;

            @Override
            public byte getByte(int col) {
                return currentRecord.getByte(col);
            }

            @Override
            public char getChar(int col) {
                return currentRecord.getChar(col);
            }

            @Override
            public double getDouble(int col) {
                return currentRecord.getDouble(col);
            }

            @Override
            public float getFloat(int col) {
                return currentRecord.getFloat(col);
            }

            @Override
            public int getInt(int col) {
                return currentRecord.getInt(col);
            }

            @Override
            public long getLong(int col) {
                return currentRecord.getLong(col);
            }

            @Override
            public CharSequence getSym(int col) {
                return currentRecord.getSym(col);
            }

            @Override
            public short getShort(int col) {
                return currentRecord.getShort(col);
            }

            @Override
            public long getTimestamp(int col) {
                return currentRecord.getTimestamp(col);
            }

            public void of(long index) {
                if (index == 0) {
                    currentRecord = crossRecord;
                } else {
                    currentRecord = dataRecord.of(currentRow);
                }
            }

            private class SampleByCrossRecord implements Record {
                @Override
                public byte getByte(int col) {
                    return Unsafe.getUnsafe().getByte(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public char getChar(int col) {
                    return Unsafe.getUnsafe().getChar(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public double getDouble(int col) {
                    return Unsafe.getUnsafe().getDouble(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public float getFloat(int col) {
                    return Unsafe.getUnsafe().getFloat(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public int getInt(int col) {
                    return Unsafe.getUnsafe().getInt(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public long getLong(int col) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public CharSequence getSym(int col) {
                    int symbolId = Unsafe.getUnsafe().getInt(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                    return pageFrameCursor.getSymbolMapReader(queryToFrameColumnMapping[col]).valueBOf(symbolId);
                }

                @Override
                public long getTimestamp(int col) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }

                @Override
                public short getShort(int col) {
                    return Unsafe.getUnsafe().getShort(crossFrameRow.getAddress() + (long) col * Long.BYTES);
                }
            }

            private class SampleByDataRecord implements Record {
                private long currentRow;

                @Override
                public byte getByte(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getByte(pageAddress + firstLastRowId[recordFirstLastIndex[col]]);
                    } else {
                        return 0;
                    }
                }

                @Override
                public char getChar(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getChar(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * 2);
                    } else {
                        return 0;
                    }
                }

                @Override
                public double getDouble(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getDouble(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Double.BYTES);
                    } else {
                        return Double.NaN;
                    }
                }

                @Override
                public float getFloat(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getFloat(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Float.BYTES);
                    } else {
                        return Float.NaN;
                    }
                }

                @Override
                public int getInt(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getInt(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
                    } else {
                        return Numbers.INT_NaN;
                    }
                }

                @Override
                public long getLong(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getLong(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
                    } else {
                        return Numbers.LONG_NaN;
                    }
                }

                @Override
                public CharSequence getSym(int col) {
                    int symbolId;
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        symbolId = Unsafe.getUnsafe().getInt(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
                    } else {
                        symbolId = SymbolTable.VALUE_IS_NULL;
                    }
                    return pageFrameCursor.getSymbolMapReader(queryToFrameColumnMapping[col]).valueBOf(symbolId);
                }

                @Override
                public short getShort(int col) {
                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getShort(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Short.BYTES);
                    } else {
                        return 0;
                    }
                }

                @Override
                public long getTimestamp(int col) {
                    if (col == timestampIndex) {
                        // Special case - timestamp the sample by runs on
                        // Take it from timestampOutBuff instead of column
                        // It's the value of the beginning of the group, not where the first row found
                        long tsIndex = startTimestampOutAddress.get(currentRow) - prevSamplePeriodOffset;
                        return samplePeriodAddress.get(tsIndex);
                    }

                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    return Unsafe.getUnsafe().getLong(pageAddress + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
                }

                public SampleByDataRecord of(long currentRow) {
                    this.currentRow = currentRow;
                    firstLastRowId[0] = firstRowIdOutAddress.get(currentRow) - dataFrameLo;
                    firstLastRowId[1] = lastRowIdOutAddress.get(currentRow) - dataFrameLo;
                    return this;
                }
            }
        }
    }
}
