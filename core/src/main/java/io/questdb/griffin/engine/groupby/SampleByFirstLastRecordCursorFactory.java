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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class SampleByFirstLastRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final TimestampSampler timestampSampler;
    private final GenericRecordMetadata groupByMetadata;
    private final int timestampIndex;
    private static final long BUFF_SIZE = 4096;
    private final long timestampOutAddress;
    private final long firstRowIdOutAddress;
    private final long lastRowIdOutAddress;
    private final int gropuBySymbolColIndex;

    public SampleByFirstLastRecordCursorFactory(
            RecordCursorFactory base,
            TimestampSampler timestampSampler,
            GenericRecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex,
            int columnCount) {
        this.base = base;
        this.timestampSampler = timestampSampler;
        this.groupByMetadata = groupByMetadata;
        this.timestampIndex = timestampIndex;
        timestampOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        firstRowIdOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        lastRowIdOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        gropuBySymbolColIndex = 1;
    }

    @Override
    public void close() {
        base.close();
        Unsafe.free(timestampOutAddress, BUFF_SIZE * Long.BYTES);
        Unsafe.free(firstRowIdOutAddress, BUFF_SIZE * Long.BYTES);
        Unsafe.free(lastRowIdOutAddress, BUFF_SIZE * Long.BYTES);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return new SampleByFirstLastRecordCursor(base.getPageFrameCursor(executionContext));
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
        private final PageFrameCursor pageFrameCursor;
        private final SymbolMapReader symbolsReader;
        private PageFrame currentFrame;
        private final SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private long currentRow;
        private long rowsFound;
        private IndexFrameCursor indexCursor;
        private long firstRowId = -1;
        private long firstTimestamp = -1;
        private long pageRowSize = -1;
        private IndexFrame indexFrame;
        private long indexFrameIndex = -1;
        private BitmapIndexReader symbolIndexReader;

        public SampleByFirstLastRecordCursor(PageFrameCursor pageFrameCursor) {
            this.pageFrameCursor = pageFrameCursor;
            this.symbolsReader = pageFrameCursor.getSymbolMapReader(gropuBySymbolColIndex);
        }

        @Override
        public void close() {
            pageFrameCursor.close();
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
            if (currentRow >= rowsFound) {
                if (pageRowSize <= firstRowId) {
                    currentFrame = pageFrameCursor.next();
                    if (currentFrame != null) {
                        symbolIndexReader = currentFrame.getBitmapIndexReader(gropuBySymbolColIndex, BitmapIndexReader.DIR_FORWARD);
                        pageRowSize = currentFrame.getPageSize(timestampIndex) / Long.BYTES;
                        firstRowId = currentFrame.getFirstRowId();
                        indexCursor = null;
                    }
                }

                if (currentFrame == null) {
                    return false;
                }

                findInFrame(currentFrame);
                currentRow = -1;
            }

            // No else, first if sets currentRow
            if (++currentRow < rowsFound) {
                record.of(currentRow);
                return true;
            }
            return false;
        }

        private void findInFrame(PageFrame currentFrame) {
            long timestampColumnAddress = currentFrame.getPageAddress(timestampIndex);
            long lastFrameRowId = firstRowId + pageRowSize;

            // TODO: find key id from query
            int key = 1;

            if (indexCursor == null) {
                indexCursor = symbolIndexReader.getNextFrame(key, firstRowId, lastFrameRowId);
            }

            if (indexFrame == null || indexFrameIndex >= indexFrame.getSize()) {
                indexFrame = indexCursor.getNext();
                indexFrameIndex = 0;
            }

            if (firstTimestamp == -1 && indexFrame.getSize() > 0) {
                long rowId = Unsafe.getUnsafe().getLong(indexFrame.getAddress());
                firstTimestamp = Unsafe.getUnsafe().getLong(timestampColumnAddress + rowId * Long.BYTES);
            }

            rowsFound = Vect.findFirstLastInFrame(
                    firstTimestamp,
                    firstRowId,
                    pageRowSize,
                    timestampColumnAddress,
                    timestampSampler,
                    indexFrame.getAddress(),
                    indexFrame.getSize(),
                    indexFrameIndex,
                    timestampOutAddress,
                    firstRowIdOutAddress,
                    lastRowIdOutAddress,
                    BUFF_SIZE);

            if (rowsFound > 0) {
                assert rowsFound < BUFF_SIZE;

                // Read output timestamp and rowId to resume at the end of the output buffers
                indexFrameIndex = Unsafe.getUnsafe().getLong(firstRowIdOutAddress + rowsFound * Long.BYTES);
                long nextFirstRowId = Unsafe.getUnsafe().getLong(lastRowIdOutAddress + rowsFound * Long.BYTES);
                assert nextFirstRowId > firstRowId;
                firstRowId = nextFirstRowId;
                firstTimestamp = Unsafe.getUnsafe().getLong(timestampColumnAddress + rowsFound * Long.BYTES);
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
            firstRowId = -1;
            firstTimestamp = -1;
            pageRowSize = -1;
            indexFrame = null;
            indexFrameIndex = -1;
            indexCursor = null;
            pageFrameCursor.toTop();
        }

        @Override
        public long size() {
            return -1;
        }

        private class SampleByFirstLastRecord implements Record {
            private long rowId;

            public SampleByFirstLastRecord of(long currentRow) {
                rowId = Unsafe.getUnsafe().getLong(firstRowIdOutAddress + currentRow * Long.BYTES);
                return this;
            }

            @Override
            public long getTimestamp(int col) {
                if (col == timestampIndex) {
                    // Special case - timestamp the sample by runs on
                    // Take it from timestampOutBuff instead of column
                    // It's the value of the beginning of the group, not where the first row found
                    return Unsafe.getUnsafe().getLong(timestampOutAddress + currentRow * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + rowId * Long.BYTES);
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + rowId * Long.BYTES);
            }

            @Override
            public double getDouble(int col) {
                return Unsafe.getUnsafe().getDouble(currentFrame.getPageAddress(col) + rowId * Double.BYTES);
            }

            @Override
            public CharSequence getSym(int col) {
                int symbolId = Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + rowId * Integer.BYTES);
                return symbolsReader.valueBOf(symbolId);
            }

            @Override
            public int getInt(int col) {
                return Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + rowId * Integer.BYTES);
            }
        }
    }
}
