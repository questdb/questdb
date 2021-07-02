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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

import java.util.Arrays;

public class SampleByFirstLastRecordCursorFactory implements RecordCursorFactory {
    private static final Log LOG = LogFactory.getLog(SampleByFirstLastRecordCursorFactory.class);
    private static final int FILTER_KEY_IS_NULL = 0;
    private final RecordCursorFactory base;
    private final TimestampSampler timestampSampler;
    private final GenericRecordMetadata groupByMetadata;
    private final boolean[] isColumnLast;
    private final int[] queryToFrameColumnMapping;
    private final int pageSize;
    private final int maxSamplePeriodSize;
    private final SingleSymbolFilter symbolFilter;
    private final int groupBySymbolColIndex;
    private final int timestampIndex;
    private DirectLongList startTimestampOutAddress;
    private DirectLongList firstRowIdOutAddress;
    private DirectLongList lastRowIdOutAddress;
    private DirectLongList samplePeriodAddress;
    private DirectLongList crossFrameRow;
    private DirectLongList nullBuffer;
    private int groupByTimestampIndex = -1;
    private final int[] sampleByColShift;

    public SampleByFirstLastRecordCursorFactory(
            RecordCursorFactory base,
            TimestampSampler timestampSampler,
            GenericRecordMetadata groupByMetadata,
            ObjList<QueryColumn> columns,
            RecordMetadata metadata,
            int timestampIndex,
            SingleSymbolFilter symbolFilter,
            int configPageSize) throws SqlException {
        this.base = base;
        this.groupBySymbolColIndex = symbolFilter.getColumnIndex();

        this.queryToFrameColumnMapping = new int[columns.size()];
        this.isColumnLast = new boolean[columns.size()];
        this.nullBuffer = new DirectLongList(columns.size()).setSize(columns.size());
        this.crossFrameRow = new DirectLongList(columns.size()).setSize(columns.size());
        this.sampleByColShift = new int[columns.size()];

        this.groupByMetadata = groupByMetadata;
        this.timestampIndex = timestampIndex;
        buildFirstLastIndex(isColumnLast, queryToFrameColumnMapping, metadata, columns, timestampIndex);
        int blockSize = metadata.getIndexValueBlockCapacity(groupBySymbolColIndex);
        this.pageSize = configPageSize < 16 ? Math.max(blockSize, 16) : configPageSize;

        this.maxSamplePeriodSize = this.pageSize * 4;
        this.timestampSampler = timestampSampler;
        this.startTimestampOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.firstRowIdOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.lastRowIdOutAddress = new DirectLongList(pageSize).setSize(pageSize);
        this.samplePeriodAddress = new DirectLongList(maxSamplePeriodSize).setSize(maxSamplePeriodSize);
        this.symbolFilter = symbolFilter;
    }

    @Override
    public void close() {
        base.close();
        startTimestampOutAddress = Misc.free(startTimestampOutAddress);
        firstRowIdOutAddress = Misc.free(firstRowIdOutAddress);
        lastRowIdOutAddress = Misc.free(lastRowIdOutAddress);
        crossFrameRow = Misc.free(crossFrameRow);
        samplePeriodAddress = Misc.free(samplePeriodAddress);
        nullBuffer = Misc.free(nullBuffer);
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
            boolean[] isColumnLast,
            int[] queryToFrameColumnMapping,
            RecordMetadata metadata,
            ObjList<QueryColumn> columns,
            int timestampIndex
    ) throws SqlException {
        for (int i = 0; i < isColumnLast.length; i++) {
            QueryColumn column = columns.getQuick(i);
            ExpressionNode ast = column.getAst();
            int resultSetColumnType = groupByMetadata.getColumnType(i);
            if (ast.rhs != null) {
                if (Chars.equalsIgnoreCase(ast.token, "last")) {
                    isColumnLast[i] = true;
                } else if (!Chars.equalsIgnoreCase(ast.token, "first")) {
                    throw SqlException.$(ast.position, "expected first() or last() functions but got ").put(ast.token);
                }
                int underlyingColIndex = metadata.getColumnIndex(ast.rhs.token);
                queryToFrameColumnMapping[i] = underlyingColIndex;

                int underlyingType = metadata.getColumnType(underlyingColIndex);
                if (underlyingType != resultSetColumnType) {
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
            nullBuffer.set(i, LongNullUtils.LONG_NULLs[resultSetColumnType]);
            sampleByColShift[i] = ColumnType.pow2SizeOf(resultSetColumnType);
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

        private final static int ROW_SWITCH = 8;
        private final static int HAS_NEXT_TOTAL = 6;
        private final static int C_SEARCH = 4;
        private final static int STATE_MACHINE = 9;

        private final static int NONE = 0;
        private final static int CROSS_ROW_STATE_SAVED = 1;
        private final static int CROSS_ROW_STATE_REFS_UPDATED = 2;
        private final int groupBySymbolKey;
        private final SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private int state;
        private int crossRowState;
        private PageFrameCursor pageFrameCursor;
        private PageFrame currentFrame;
        private long currentRow;
        private int rowsFound;
        private IndexFrameCursor indexCursor;
        private long dataFrameLo = -1;
        private long dataFrameHi = -1;
        private IndexFrame indexFrame;
        private int indexFramePosition = -1;
        private long frameNextRowId = -1;
        private long samplePeriodIndexOffset = 0;
        private long prevSamplePeriodOffset = 0;
        private long samplePeriodStart;
        private final String[] benchmarksNames = new String[]{
                "BUFFER_CROSS_ROW", "STATE_FETCH_NEXT_DATA_FRAME", "",
                "STATE_FETCH_NEXT_INDEX_FRAME", "C_SEARCH", "JAVA_SEARCH",
                "HAS_NEXT_TOTAL", "STATE_DONE", "ROW_SWITCH", "STATE_MACHINE"
        };
        private final long[] benchmarks = new long[benchmarksNames.length];

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
            long timer = System.nanoTime();

            // This loop never returns last found sample by row.
            // The reason is that last row last() value can be changed on next data frame pass.
            // That's why the last row values are buffered
            // (not only rowid stored but all the values needed) in crossFrameRow.
            // Buffering values are unavoidable since rowids of last() and first() are from different data frames
            if (++currentRow < rowsFound - 1) {
                record.of(currentRow);

                long delta = System.nanoTime() - timer;
                benchmarks[ROW_SWITCH] += delta;
                benchmarks[HAS_NEXT_TOTAL] += delta;

                return true;
            }

            boolean found = hasNext0();
            benchmarks[HAS_NEXT_TOTAL] += System.nanoTime() - timer;
            return found;
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
            Arrays.fill(benchmarks, 0);
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
            long nextTs = samplePeriodStart;
            long currentTs = Long.MIN_VALUE;
            samplePeriodAddress.clear();
            for (int i = 0; i < maxSamplePeriodSize && currentTs <= lastInDataTimestamp; i++) {
                currentTs = nextTs;
                nextTs = timestampSampler.nextTimestamp(currentTs);
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
                    // Fall trough to STATE_FETCH_NEXT_DATA_FRAME;

                case STATE_FETCH_NEXT_DATA_FRAME:
                    long mc = System.nanoTime();
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
                        benchmarks[STATE_FETCH_NEXT_DATA_FRAME] += System.nanoTime() - mc;
                        return STATE_RETURN_LAST_ROW;
                    }
                    benchmarks[STATE_FETCH_NEXT_DATA_FRAME] += System.nanoTime() - mc;

                case STATE_FETCH_NEXT_INDEX_FRAME:
                    long mc1 = System.nanoTime();
                    indexFrame = indexCursor.getNext();
                    indexFramePosition = 0;

                    if (indexFrame.getSize() == 0) {
                        if (indexFrame.getAddress() != 0 || groupBySymbolKey != FILTER_KEY_IS_NULL) {
                            // No rows in index for this dataframe left, go to next data frame
                            frameNextRowId = dataFrameHi;
                            // Jump back to fetch next data frame

                            benchmarks[STATE_FETCH_NEXT_INDEX_FRAME] += System.nanoTime() - mc1;
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
                    benchmarks[STATE_FETCH_NEXT_INDEX_FRAME] += System.nanoTime() - mc1;
                    // Fall to STATE_SEARCH;

                case STATE_OUT_BUFFER_FULL:
                case STATE_SEARCH:
                    long mc3 = System.nanoTime();
                    int outPosition = crossRowState == NONE ? 0 : 1;
                    long offsetTimestampColumnAddress = currentFrame.getPageAddress(timestampIndex) - dataFrameLo * Long.BYTES;
                    long lastIndexRowId = indexFrame.getAddress() > 0
                            ? Unsafe.getUnsafe().getLong(indexFrame.getAddress() + (indexFrame.getSize() - 1) * Long.BYTES)
                            : Long.MAX_VALUE;
                    long lastInDataRowId = Math.min(lastIndexRowId, dataFrameHi - 1);
                    long lastInDataTimestamp = Unsafe.getUnsafe().getLong(offsetTimestampColumnAddress + lastInDataRowId * Long.BYTES);
                    int samplePeriodCount = fillSamplePeriodsUntil(lastInDataTimestamp);

                    long cCodeTiming = System.nanoTime();
                    benchmarks[STATE_SEARCH] += cCodeTiming - mc3;
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

                    mc3 = System.nanoTime();
                    benchmarks[C_SEARCH] += mc3 - cCodeTiming;

                    boolean firstRowLastRowIdUpdated = rowsFound < 0;
                    rowsFound = Math.abs(rowsFound);

                    // If first row last() RowId is updated
                    // re-copy last values to crossFrameRow
                    checkSaveLastValues(firstRowLastRowIdUpdated);

                    indexFramePosition = (int) firstRowIdOutAddress.get(rowsFound);
                    frameNextRowId = lastRowIdOutAddress.get(rowsFound);
                    prevSamplePeriodOffset = samplePeriodIndexOffset;
                    samplePeriodIndexOffset = (int) startTimestampOutAddress.get(rowsFound);
                    samplePeriodStart = samplePeriodAddress.get(samplePeriodIndexOffset - prevSamplePeriodOffset);

                    // decide what to do next
                    int newState;
                    if (frameNextRowId >= dataFrameHi) {
                        // Data frame exhausted. Next time start from fetching new data frame
                        newState = STATE_FETCH_NEXT_DATA_FRAME;
                    } else if (indexFramePosition >= indexFrame.getSize()) {
                        // Index frame exhausted. Next time start from fetching new index frame
                        newState = indexFrame.getAddress() > 0 ? STATE_FETCH_NEXT_INDEX_FRAME : STATE_FETCH_NEXT_DATA_FRAME;
                    } else if (rowsFound == startTimestampOutAddress.size() - 1) {
                        // output rows filled the output buffers or
                        newState = STATE_OUT_BUFFER_FULL;
                    } else if (samplePeriodIndexOffset - prevSamplePeriodOffset == maxSamplePeriodSize - 1) {
                        //  search came to the end of sample by by periods
                        // re-fill periods and search again
                        newState = STATE_SEARCH;
                    } else {
                        // Data frame exhausted. Next time start from fetching new data frame
                        newState = STATE_FETCH_NEXT_DATA_FRAME;
                    }

                    if (rowsFound > 1) {
                        // return negative to returns rows back to signal that there are rows to iterate
                        long mcOf = System.nanoTime();
                        benchmarks[STATE_SEARCH] += mcOf - mc3;

                        record.of(currentRow = 0);

                        mc3 = System.nanoTime();
                        benchmarks[ROW_SWITCH] += mc3 - mcOf;

                        newState = -newState;
                    }

                    // No rows to iterate, return where to continue from (fetching next data or index frame)
                    benchmarks[STATE_SEARCH] += System.nanoTime() - mc3;
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
            long stateLoopMc = System.nanoTime();
            checkCrossRowAfterFoundBufferIterated();
            benchmarks[STATE_START] += System.nanoTime() - stateLoopMc;
            rowsFound = 0;

            while (state != STATE_DONE) {
                long stateMachStart = System.nanoTime();
                state = getNextState(state);
                benchmarks[STATE_MACHINE] += System.nanoTime() - stateMachStart;

                if (state < 0) {
                    state = -state;
                    return true;
                }
            }

            logTimings();
            return false;
        }

        private void logTimings() {
            int totalIndex = HAS_NEXT_TOTAL;
            long total = 0;
            for(int i = 0; i < benchmarks.length; i++) {
                if (benchmarks[i] > 0 && i != totalIndex) {
                    LOG.info().$("benchmarks[").$(benchmarksNames[i]).$("]=").$(benchmarks[i] / 1000).$(" micros").$();
                    if (i != STATE_MACHINE) {
                        total += benchmarks[i];
                    }
                }
            }
            LOG.info().$("benchmarks[STATE_UNCOUNTED]=").$((benchmarks[totalIndex] - total) / 1000).$(" micros").$();
            LOG.info().$("benchmarks[HAS_NEXT_TOTAL]=").$(benchmarks[totalIndex] / 1000).$(" micros").$();
        }

        private void saveFirstLastValuesToCrossFrameRowBuffer() {
            // Copies column values of all columns to cross frame row buffer for found row with index 0
            long firstRowId = firstRowIdOutAddress.get(0) - dataFrameLo;
            long lastRowId = lastRowIdOutAddress.get(0) - dataFrameLo;
            for (int i = 0; i < isColumnLast.length; i++) {
                if (i == groupByTimestampIndex) {
                    long tsIndex = startTimestampOutAddress.get(0) - prevSamplePeriodOffset;
                    crossFrameRow.set(i, samplePeriodAddress.get(tsIndex));
                } else {
                    long rowId = isColumnLast[i] ? lastRowId : firstRowId;
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
            for (int columnIndex = 0; columnIndex < isColumnLast.length; columnIndex++) {
                if (isColumnLast[columnIndex]) {
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
            private long[] addresses_point;
            private final long[] data_addresses = new long[queryToFrameColumnMapping.length];
            private final long[] crossFrameAddresses = new long[queryToFrameColumnMapping.length];
            private final long nullBufferAddress = nullBuffer.getAddress();

            SampleByFirstLastRecord() {
                for(int col = 0; col < crossFrameAddresses.length; col++) {
                    crossFrameAddresses[col] = crossFrameRow.getAddress() + ((long) col << 3);
                }
            }

            @Override
            public byte getByte(int col) {
                return Unsafe.getUnsafe().getByte(addresses_point[col]);
            }

            @Override
            public char getChar(int col) {
                return Unsafe.getUnsafe().getChar(addresses_point[col]);
            }

            @Override
            public double getDouble(int col) {
                return Unsafe.getUnsafe().getDouble(addresses_point[col]);
            }

            @Override
            public float getFloat(int col) {
                return Unsafe.getUnsafe().getFloat(addresses_point[col]);
            }

            @Override
            public int getInt(int col) {
                return Unsafe.getUnsafe().getInt(addresses_point[col]);
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(addresses_point[col]);
            }

            @Override
            public CharSequence getSym(int col) {
                int symbolId = Unsafe.getUnsafe().getInt(addresses_point[col]);
                return pageFrameCursor.getSymbolMapReader(queryToFrameColumnMapping[col]).valueBOf(symbolId);
            }

            @Override
            public long getTimestamp(int col) {
                return Unsafe.getUnsafe().getLong(addresses_point[col]);
            }

            @Override
            public short getShort(int col) {
                return Unsafe.getUnsafe().getShort(addresses_point[col]);
            }

            public void of(long index) {
                if (index == 0) {
                    // Cross frame row
                    addresses_point = crossFrameAddresses;
                    return;
                }

                addresses_point = data_addresses;
                long firstRowId = firstRowIdOutAddress.get(currentRow) - dataFrameLo;
                long lastRowId = lastRowIdOutAddress.get(currentRow) - dataFrameLo;

                for(int col = 0; col < queryToFrameColumnMapping.length; col++) {
                    if (col == groupByTimestampIndex) {
                        // Special case - timestamp the sample by runs on
                        // Take it from timestampOutBuff instead of column
                        // It's the value of the beginning of the group, not where the first row found
                        long tsIndex = startTimestampOutAddress.get(currentRow) - prevSamplePeriodOffset;
                        addresses_point[col] = samplePeriodAddress.getAddress() + (tsIndex << 3);
                        continue;
                    }

                    long pageAddress = currentFrame.getPageAddress(queryToFrameColumnMapping[col]);
                    if (pageAddress > 0) {
                        // Data from columns
                        long rowId = !isColumnLast[col] ? firstRowId : lastRowId;
                        int shift = sampleByColShift[col];
                        addresses_point[col] = pageAddress + (rowId << shift);
                    } else {
                        // Column top
                        addresses_point[col] = nullBufferAddress + ((long) col << 3);
                    }
                }
            }
        }
    }
}
