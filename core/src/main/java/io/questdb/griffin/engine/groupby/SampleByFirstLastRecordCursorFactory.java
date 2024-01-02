/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.*;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class SampleByFirstLastRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int FILTER_KEY_IS_NULL = 0;
    private static final int FIRST_OUT_INDEX = 0;
    private static final int ITEMS_PER_OUT_ARRAY_SHIFT = 2;
    private static final int LAST_OUT_INDEX = 1;
    private static final int TIMESTAMP_OUT_INDEX = 2;
    private final RecordCursorFactory base;
    private final LongList crossFrameRow;
    private final int[] firstLastIndexByCol;
    private final int groupBySymbolColIndex;
    private final boolean[] isKeyColumn;
    private final int maxSamplePeriodSize;
    private final int pageSize;
    private final int[] queryToFrameColumnMapping;
    private final SampleByFirstLastRecordCursor sampleByFirstLastRecordCursor;
    private final SingleSymbolFilter symbolFilter;
    private final int timestampIndex;
    private int groupByTimestampIndex = -1;
    private DirectLongList rowIdOutAddress;
    private DirectLongList samplePeriodAddress;

    public SampleByFirstLastRecordCursorFactory(
            RecordCursorFactory base,
            TimestampSampler timestampSampler,
            GenericRecordMetadata groupByMetadata,
            ObjList<QueryColumn> columns,
            RecordMetadata metadata,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            int timestampIndex,
            SingleSymbolFilter symbolFilter,
            int configPageSize
    ) throws SqlException {
        super(groupByMetadata);
        this.base = base;
        groupBySymbolColIndex = symbolFilter.getColumnIndex();
        queryToFrameColumnMapping = new int[columns.size()];
        firstLastIndexByCol = new int[columns.size()];
        isKeyColumn = new boolean[columns.size()];
        crossFrameRow = new LongList(columns.size());
        crossFrameRow.setPos(columns.size());
        this.timestampIndex = timestampIndex;
        buildFirstLastIndex(firstLastIndexByCol, queryToFrameColumnMapping, metadata, columns, timestampIndex, isKeyColumn);
        int blockSize = metadata.getIndexValueBlockCapacity(groupBySymbolColIndex);
        pageSize = configPageSize < 16 ? Math.max(blockSize, 16) : configPageSize;
        maxSamplePeriodSize = pageSize * 4;
        int outSize = pageSize << ITEMS_PER_OUT_ARRAY_SHIFT;
        rowIdOutAddress = new DirectLongList(outSize, MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST);
        rowIdOutAddress.setPos(outSize);
        samplePeriodAddress = new DirectLongList(pageSize, MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST);
        this.symbolFilter = symbolFilter;
        sampleByFirstLastRecordCursor = new SampleByFirstLastRecordCursor(
                timestampSampler,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext, ORDER_ASC);
        int groupByIndexKey = symbolFilter.getSymbolFilterKey();
        if (groupByIndexKey == SymbolMapReader.VALUE_NOT_FOUND) {
            Misc.free(pageFrameCursor);
            return EmptyTableRecordCursor.INSTANCE;
        }
        try {
            sampleByFirstLastRecordCursor.of(
                    pageFrameCursor,
                    groupByIndexKey,
                    executionContext
            );
            return sampleByFirstLastRecordCursor;
        } catch (Throwable e) {
            Misc.free(pageFrameCursor);
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("SampleByFirstLast");
        sink.attr("keys");
        boolean first = true;
        sink.val('[');
        for (int i = 0; i < isKeyColumn.length; i++) {
            if (isKeyColumn[i]) {
                if (first) {
                    first = false;
                } else {
                    sink.val(", ");
                }
                sink.putBaseColumnName(i);
            }
        }
        sink.val(']');
        sink.attr("values");
        first = true;
        sink.val('[');
        for (int i = 0; i < isKeyColumn.length; i++) {
            if (!isKeyColumn[i]) {
                if (first) {
                    first = false;
                } else {
                    sink.val(", ");
                }
                sink.val(firstLastIndexByCol[i] == LAST_OUT_INDEX ? "last" : "first").val('(');
                sink.putBaseColumnName(i);
                sink.val(')');
            }
        }
        sink.val(']');
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private void buildFirstLastIndex(
            int[] firstLastIndex,
            int[] queryToFrameColumnMapping,
            RecordMetadata metadata,
            ObjList<QueryColumn> columns,
            int timestampIndex,
            boolean[] isKeyColumn
    ) throws SqlException {
        for (int i = 0, n = firstLastIndex.length; i < n; i++) {
            QueryColumn column = columns.getQuick(i);
            ExpressionNode ast = column.getAst();
            int resultSetColumnType = getMetadata().getColumnType(i);
            if (ast.rhs != null) {
                if (SqlKeywords.isLastKeyword(ast.token)) {
                    firstLastIndex[i] = LAST_OUT_INDEX;
                } else if (SqlKeywords.isFirstKeyword(ast.token)) {
                    firstLastIndex[i] = FIRST_OUT_INDEX;
                } else {
                    throw SqlException.$(ast.position, "expected first() or last() functions but got ").put(ast.token);
                }
                int underlyingColIndex = metadata.getColumnIndex(ast.rhs.token);
                queryToFrameColumnMapping[i] = underlyingColIndex;

                int underlyingType = metadata.getColumnType(underlyingColIndex);
                if (underlyingType != resultSetColumnType || ColumnType.pow2SizeOf(resultSetColumnType) > 3) {
                    throw SqlException.$(ast.position, "column \"")
                            .put(metadata.getColumnName(underlyingColIndex))
                            .put("\": first(), last() is not supported on data type ")
                            .put(ColumnType.nameOf(underlyingType))
                            .put(" ");
                }
            } else {
                int underlyingColIndex = metadata.getColumnIndex(ast.token);
                isKeyColumn[i] = true;
                queryToFrameColumnMapping[i] = underlyingColIndex;
                if (underlyingColIndex == timestampIndex) {
                    groupByTimestampIndex = i;
                }
            }
        }
    }

    @Override
    protected void _close() {
        base.close();
        rowIdOutAddress = Misc.free(rowIdOutAddress);
        samplePeriodAddress = Misc.free(samplePeriodAddress);
    }

    private class SampleByFirstLastRecordCursor extends AbstractSampleByCursor {
        private final static int CROSS_ROW_STATE_REFS_UPDATED = 2;
        private final static int CROSS_ROW_STATE_SAVED = 1;
        private final static int NONE = 0;
        private final static int STATE_DONE = 7;
        private final static int STATE_FETCH_NEXT_DATA_FRAME = 1;
        private final static int STATE_FETCH_NEXT_INDEX_FRAME = 3;
        private final static int STATE_OUT_BUFFER_FULL = 4;
        private final static int STATE_RETURN_LAST_ROW = 6;
        private final static int STATE_SEARCH = 5;
        private final static int STATE_START = 0;
        private final SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private int crossRowState;
        private PageFrame currentFrame;
        private long currentRow;
        private long dataFrameHi = -1;
        private long dataFrameLo = -1;
        private long frameNextRowId = -1;
        private int groupBySymbolKey;
        private IndexFrameCursor indexCursor;
        private IndexFrame indexFrame;
        private int indexFramePosition = -1;
        private boolean initialized;
        private PageFrameCursor pageFrameCursor;
        private long prevSamplePeriodOffset = 0;
        private int rowsFound;
        private long samplePeriodIndexOffset = 0;
        private long samplePeriodStart;
        private int state;

        public SampleByFirstLastRecordCursor(
                TimestampSampler timestampSampler,
                Function timezoneNameFunc,
                int timezoneNameFuncPos,
                Function offsetFunc,
                int offsetFuncPos
        ) {
            super(timestampSampler, timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos);
        }

        @Override
        public void close() {
            pageFrameCursor = Misc.free(pageFrameCursor);
        }

        public long getNextTimestamp() {
            long lastTimestampLocUtc = localEpoch - tzOffset;
            long nextLastTimestampLoc = timestampSampler.nextTimestamp(localEpoch);
            if (nextLastTimestampLoc - tzOffset >= nextDstUtc) {
                tzOffset = rules.getOffset(nextLastTimestampLoc - tzOffset);
                nextDstUtc = rules.getNextDST(nextLastTimestampLoc - tzOffset);
                while (nextLastTimestampLoc - tzOffset <= lastTimestampLocUtc) {
                    nextLastTimestampLoc = timestampSampler.nextTimestamp(nextLastTimestampLoc);
                }
            }
            localEpoch = nextLastTimestampLoc;
            return localEpoch - tzOffset;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return pageFrameCursor.getSymbolTable(queryToFrameColumnMapping[columnIndex]);
        }

        @Override
        public boolean hasNext() {
            // This loop never returns last found sample by row.
            // The reason is that last row() value can be changed on next data frame pass.
            // That's why the last row values are buffered
            // (not only row id stored but all the values needed) in crossFrameRow.
            // Buffering values are unavoidable since row ids of last() and first() are from different data frames
            if (++currentRow < rowsFound - 1) {
                record.of(currentRow);
                return true;
            }

            return hasNext0();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return pageFrameCursor.newSymbolTable(queryToFrameColumnMapping[columnIndex]);
        }

        @Override
        public long size() {
            return -1;
        }

        public long startFrom(long timestamp) {
            // Skip initialization if timestamp is the same as last returned
            if (!initialized || timestamp != localEpoch - tzOffset) {
                // null rules means the timezone does not have daylight time changes
                if (rules != null) {
                    tzOffset = rules.getOffset(timestamp);
                    // we really need UTC timestamp to get offset correctly
                    // this will converge to UTC timestamp
                    tzOffset = rules.getOffset(timestamp + tzOffset);
                    nextDstUtc = rules.getNextDST(timestamp + tzOffset);
                }
                if (fixedOffset != Long.MIN_VALUE) {
                    localEpoch = timestampSampler.round(timestamp + tzOffset - fixedOffset) + fixedOffset;
                } else {
                    localEpoch = timestamp + tzOffset;
                }
                initialized = true;
            }
            return localEpoch - tzOffset;
        }

        @Override
        public void toTop() {
            currentRow = rowsFound = 0;
            frameNextRowId = dataFrameLo = dataFrameHi = -1;
            state = STATE_START;
            crossRowState = NONE;
            pageFrameCursor.toTop();
        }

        private void checkCrossRowAfterFoundBufferIterated() {
            if (crossRowState != NONE && rowsFound > 1) {
                // Copy last set of first(), last() RowIds from bottom of the previous output to the top
                long lastFoundIndex = (long) (rowsFound - 1) << ITEMS_PER_OUT_ARRAY_SHIFT;
                rowIdOutAddress.set(FIRST_OUT_INDEX, rowIdOutAddress.get(lastFoundIndex + FIRST_OUT_INDEX));
                rowIdOutAddress.set(LAST_OUT_INDEX, rowIdOutAddress.get(lastFoundIndex + LAST_OUT_INDEX));
                rowIdOutAddress.set(TIMESTAMP_OUT_INDEX, rowIdOutAddress.get(lastFoundIndex + TIMESTAMP_OUT_INDEX));
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
            long nextTs = samplePeriodStart;
            long currentTs = Long.MIN_VALUE;
            nextTs = startFrom(nextTs);

            samplePeriodAddress.clear();
            for (int i = 0; i < maxSamplePeriodSize && currentTs <= lastInDataTimestamp; i++) {
                currentTs = nextTs;
                nextTs = getNextTimestamp();
                assert nextTs != currentTs : "uh-oh, we may have got into an infinite loop with ts " + nextTs;
                samplePeriodAddress.add(currentTs);
            }
            return (int) samplePeriodAddress.size();
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
        private int getNextState(int state) {
            // This method should not change this.state field
            // but instead produce next state given incoming state parameter.
            // This way the method is enforced to change state and less the caller to loop forever.
            switch (state) {
                case STATE_START:
                    prevSamplePeriodOffset = samplePeriodIndexOffset = 0;
                    crossRowState = NONE;
                    samplePeriodStart = Numbers.LONG_NaN;
                    // Fall through to STATE_FETCH_NEXT_DATA_FRAME;

                case STATE_FETCH_NEXT_DATA_FRAME:
                    currentFrame = pageFrameCursor.next();
                    if (currentFrame != null) {
                        record.switchFrame();

                        // Switch to new data frame
                        frameNextRowId = dataFrameLo = currentFrame.getPartitionLo();
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

                    long indexFrameAddress = indexFrame.getAddress();
                    if (indexFrame.getSize() == 0) {
                        if (indexFrameAddress != 0 || groupBySymbolKey != FILTER_KEY_IS_NULL) {
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
                        long rowId = indexFrameAddress > 0 ? Unsafe.getUnsafe().getLong(indexFrameAddress) : dataFrameLo;
                        long offsetTimestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - dataFrameLo * Long.BYTES;
                        samplePeriodStart = Unsafe.getUnsafe().getLong(offsetTimestampColumnAddress + rowId * Long.BYTES);
                        startFrom(samplePeriodStart);
                    }
                    // Fall to STATE_SEARCH;

                case STATE_OUT_BUFFER_FULL:
                case STATE_SEARCH:
                    int outPosition = crossRowState == NONE ? 0 : 1;
                    long offsetTimestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - dataFrameLo * Long.BYTES;
                    long iFrameAddress = indexFrame.getAddress();
                    long iFrameSize = indexFrame.getSize();
                    long lastIndexRowId = iFrameAddress > 0
                            ? Unsafe.getUnsafe().getLong(iFrameAddress + (iFrameSize - 1) * Long.BYTES)
                            : Long.MAX_VALUE;
                    long lastInDataRowId = Math.min(lastIndexRowId, dataFrameHi - 1);
                    long lastInDataTimestamp = Unsafe.getUnsafe().getLong(offsetTimestampColumnAddress + lastInDataRowId * Long.BYTES);
                    int samplePeriodCount = fillSamplePeriodsUntil(lastInDataTimestamp);

                    rowsFound = BitmapIndexUtilsNative.findFirstLastInFrame(
                            outPosition,
                            frameNextRowId,
                            dataFrameHi,
                            offsetTimestampColumnAddress,
                            dataFrameLo,
                            iFrameAddress,
                            iFrameSize,
                            indexFramePosition,
                            samplePeriodAddress.getAddress(),
                            samplePeriodCount,
                            samplePeriodIndexOffset,
                            rowIdOutAddress.getAddress(),
                            pageSize);

                    boolean firstRowLastRowIdUpdated = rowsFound < 0;
                    rowsFound = Math.abs(rowsFound);

                    // If first row last() RowId is updated
                    // re-copy last values to crossFrameRow
                    checkSaveLastValues(firstRowLastRowIdUpdated);

                    int lastOutIndex = rowsFound << ITEMS_PER_OUT_ARRAY_SHIFT;
                    prevSamplePeriodOffset = samplePeriodIndexOffset;
                    indexFramePosition = (int) rowIdOutAddress.get(lastOutIndex + FIRST_OUT_INDEX);
                    frameNextRowId = rowIdOutAddress.get(lastOutIndex + LAST_OUT_INDEX);
                    samplePeriodIndexOffset = rowIdOutAddress.get(lastOutIndex + TIMESTAMP_OUT_INDEX);
                    samplePeriodStart = samplePeriodAddress.get(samplePeriodIndexOffset - prevSamplePeriodOffset);

                    // decide what to do next
                    int newState;
                    if (frameNextRowId >= dataFrameHi) {
                        // Data frame exhausted. Next time start from fetching new data frame
                        newState = STATE_FETCH_NEXT_DATA_FRAME;
                    } else if (indexFramePosition >= iFrameSize) {
                        // Index frame exhausted. Next time start from fetching new index frame
                        newState = iFrameAddress > 0 ? STATE_FETCH_NEXT_INDEX_FRAME : STATE_FETCH_NEXT_DATA_FRAME;
                    } else if (rowsFound == pageSize - 1) {
                        // output rows filled the output buffers or
                        newState = STATE_OUT_BUFFER_FULL;
                    } else if (samplePeriodIndexOffset - prevSamplePeriodOffset == maxSamplePeriodSize - 1) {
                        //  search came to the end of sample by periods
                        // re-fill periods and search again
                        newState = STATE_SEARCH;
                    } else {
                        // Data frame exhausted. Next time start from fetching new data frame
                        newState = STATE_FETCH_NEXT_DATA_FRAME;
                    }

                    if (rowsFound > 1) {
                        record.of(currentRow = 0);
                        newState = -newState;
                    }

                    // No rows to iterate, return where to continue from (fetching next data or index frame)
                    return newState;

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
            for (int i = 0, length = firstLastIndexByCol.length; i < length; i++) {
                if (i == groupByTimestampIndex) {
                    long tsIndex = rowIdOutAddress.get(TIMESTAMP_OUT_INDEX) - prevSamplePeriodOffset;
                    crossFrameRow.set(i, samplePeriodAddress.get(tsIndex));
                } else {
                    long rowId = rowIdOutAddress.get(firstLastIndexByCol[i]);
                    saveRowIdValueToCrossRow(rowId, i);
                }
            }
        }

        private void saveFixedColToBufferWithLongAlignment(int index, LongList crossFrameRow, int columnType, long pageAddress, long rowId) {
            switch (ColumnType.pow2SizeOf(columnType)) {
                case 3:
                    crossFrameRow.set(index, Unsafe.getUnsafe().getLong(pageAddress + (rowId << 3)));
                    break;
                case 2:
                    crossFrameRow.set(index, Unsafe.getUnsafe().getInt(pageAddress + (rowId << 2)));
                    break;
                case 1:
                    crossFrameRow.set(index, Unsafe.getUnsafe().getShort(pageAddress + (rowId << 1)));
                    break;
                case 0:
                    crossFrameRow.set(index, Unsafe.getUnsafe().getByte(pageAddress + rowId));
                    break;
                default:
                    throw new CairoException().put("first(), last() cannot be used with column type ").put(ColumnType.nameOf(columnType));
            }
        }

        private void saveLastValuesToBuffer() {
            // Copies only last() column values of all columns to cross frame row buffer for found row with index 0
            for (int columnIndex = 0, length = firstLastIndexByCol.length; columnIndex < length; columnIndex++) {
                if (firstLastIndexByCol[columnIndex] == LAST_OUT_INDEX) {
                    // last() values only
                    int frameColIndex = queryToFrameColumnMapping[columnIndex];
                    assert currentFrame.getPageSize(frameColIndex) > rowIdOutAddress.get(LAST_OUT_INDEX);
                    saveRowIdValueToCrossRow(rowIdOutAddress.get(LAST_OUT_INDEX), columnIndex);
                }
            }
        }

        private void saveRowIdValueToCrossRow(long rowId, int columnIndex) {
            int columnType = getMetadata().getColumnType(columnIndex);
            int frameColIndex = queryToFrameColumnMapping[columnIndex];
            long pageAddress = currentFrame.getPageAddress(frameColIndex);
            if (pageAddress > 0) {
                saveFixedColToBufferWithLongAlignment(columnIndex, crossFrameRow, columnType, pageAddress, rowId);
            } else {
                crossFrameRow.set(columnIndex, LongNullUtils.getLongNull(columnType));
            }
        }

        void of(
                PageFrameCursor pageFrameCursor,
                int groupBySymbolKey,
                SqlExecutionContext sqlExecutionContext
        ) throws SqlException {
            this.pageFrameCursor = pageFrameCursor;
            this.groupBySymbolKey = groupBySymbolKey;
            toTop();
            parseParams(this, sqlExecutionContext);
            initialized = false;
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
            public byte getGeoByte(int col) {
                return currentRecord.getGeoByte(col);
            }

            @Override
            public int getGeoInt(int col) {
                return currentRecord.getGeoInt(col);
            }

            @Override
            public long getGeoLong(int col) {
                return currentRecord.getGeoLong(col);
            }

            @Override
            public short getGeoShort(int col) {
                return currentRecord.getGeoShort(col);
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
            public short getShort(int col) {
                return currentRecord.getShort(col);
            }

            @Override
            public CharSequence getSym(int col) {
                return currentRecord.getSym(col);
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

            public void switchFrame() {
                dataRecord.switchFrame();
            }

            private class SampleByCrossRecord implements Record {
                @Override
                public byte getByte(int col) {
                    return (byte) crossFrameRow.getQuick(col);
                }

                @Override
                public char getChar(int col) {
                    return (char) crossFrameRow.getQuick(col);
                }

                @Override
                public double getDouble(int col) {
                    return Double.longBitsToDouble(crossFrameRow.getQuick(col));
                }

                @Override
                public float getFloat(int col) {
                    return Float.intBitsToFloat((int) crossFrameRow.getQuick(col));
                }

                @Override
                public byte getGeoByte(int col) {
                    return getByte(col);
                }

                @Override
                public int getGeoInt(int col) {
                    return getInt(col);
                }

                @Override
                public long getGeoLong(int col) {
                    return getLong(col);
                }

                @Override
                public short getGeoShort(int col) {
                    return getShort(col);
                }

                @Override
                public int getInt(int col) {
                    return (int) crossFrameRow.getQuick(col);
                }

                @Override
                public long getLong(int col) {
                    return crossFrameRow.getQuick(col);
                }

                @Override
                public short getShort(int col) {
                    return (short) crossFrameRow.getQuick(col);
                }

                @Override
                public CharSequence getSym(int col) {
                    int symbolId = (int) crossFrameRow.getQuick(col);
                    return pageFrameCursor.getSymbolTable(queryToFrameColumnMapping[col]).valueBOf(symbolId);
                }

                @Override
                public long getTimestamp(int col) {
                    return getLong(col);
                }
            }

            private class SampleByDataRecord implements Record {
                private final long[] pageAddresses = new long[queryToFrameColumnMapping.length];
                private long currentRow;

                @Override
                public byte getByte(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getByte(pageAddress + getRowId(firstLastIndexByCol[col]));
                    } else {
                        return 0;
                    }
                }

                @Override
                public char getChar(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getChar(pageAddress + (getRowId(firstLastIndexByCol[col]) << 1));
                    } else {
                        return 0;
                    }
                }

                @Override
                public double getDouble(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getDouble(pageAddress + (getRowId(firstLastIndexByCol[col]) << 3));
                    } else {
                        return Double.NaN;
                    }
                }

                @Override
                public float getFloat(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getFloat(pageAddress + (getRowId(firstLastIndexByCol[col]) << 2));
                    } else {
                        return Float.NaN;
                    }
                }

                @Override
                public byte getGeoByte(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getByte(pageAddress + getRowId(firstLastIndexByCol[col]));
                    } else {
                        return GeoHashes.BYTE_NULL;
                    }
                }

                @Override
                public int getGeoInt(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getInt(pageAddress + (getRowId(firstLastIndexByCol[col]) << 2));
                    } else {
                        return GeoHashes.INT_NULL;
                    }
                }

                @Override
                public long getGeoLong(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getLong(pageAddress + (getRowId(firstLastIndexByCol[col]) << 3));
                    } else {
                        return GeoHashes.NULL;
                    }
                }

                @Override
                public short getGeoShort(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getShort(pageAddress + (getRowId(firstLastIndexByCol[col]) << 1));
                    } else {
                        return GeoHashes.SHORT_NULL;
                    }
                }

                @Override
                public int getIPv4(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getInt(pageAddress + (getRowId(firstLastIndexByCol[col]) << 2));
                    } else {
                        return Numbers.IPv4_NULL;
                    }
                }

                @Override
                public int getInt(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getInt(pageAddress + (getRowId(firstLastIndexByCol[col]) << 2));
                    } else {
                        return Numbers.INT_NaN;
                    }
                }

                @Override
                public long getLong(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        if (col != timestampIndex) {
                            return Unsafe.getUnsafe().getLong(pageAddress + (getRowId(firstLastIndexByCol[col]) << 3));
                        }
                        // Special case - timestamp the sample by runs on
                        // Take it from timestampOutBuff instead of column
                        // It's the value of the beginning of the group, not where the first row found
                        return samplePeriodAddress.get(getRowId(TIMESTAMP_OUT_INDEX) - prevSamplePeriodOffset);
                    } else {
                        return Numbers.LONG_NaN;
                    }
                }

                @Override
                public short getShort(int col) {
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        return Unsafe.getUnsafe().getShort(pageAddress + (getRowId(firstLastIndexByCol[col]) << 1));
                    } else {
                        return 0;
                    }
                }

                @Override
                public CharSequence getSym(int col) {
                    int symbolId;
                    long pageAddress = pageAddresses[col];
                    if (pageAddress > 0) {
                        symbolId = Unsafe.getUnsafe().getInt(pageAddress + (getRowId(firstLastIndexByCol[col]) << 2));
                    } else {
                        symbolId = SymbolTable.VALUE_IS_NULL;
                    }
                    return pageFrameCursor.getSymbolTable(queryToFrameColumnMapping[col]).valueBOf(symbolId);
                }

                @Override
                public long getTimestamp(int col) {
                    return getLong(col);
                }

                public SampleByDataRecord of(long currentRow) {
                    this.currentRow = currentRow;
                    return this;
                }

                public void switchFrame() {
                    for (int i = 0, length = pageAddresses.length; i < length; i++) {
                        pageAddresses[i] = currentFrame.getPageAddress(queryToFrameColumnMapping[i]);
                    }
                }

                private long getRowId(int col) {
                    return rowIdOutAddress.get((currentRow << ITEMS_PER_OUT_ARRAY_SHIFT) + col);
                }
            }
        }
    }
}
