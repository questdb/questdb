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
    private long timestampOutBuff;
    private long firstRowIdOutBuff;
    private long lastRowIdOutBuff;
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
        timestampOutBuff = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        firstRowIdOutBuff = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        lastRowIdOutBuff = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        gropuBySymbolColIndex = 1;
    }

    @Override
    public void close() {
        base.close();
        Unsafe.free(timestampOutBuff, BUFF_SIZE * Long.BYTES);
        Unsafe.free(firstRowIdOutBuff, BUFF_SIZE * Long.BYTES);
        Unsafe.free(lastRowIdOutBuff, BUFF_SIZE * Long.BYTES);
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
        private SampleByFirstLastRecord record = new SampleByFirstLastRecord();
        private long currentRow;
        private long rowsFound;
        private IndexFrameCursor indexCursor;

        public SampleByFirstLastRecordCursor(PageFrameCursor pageFrameCursor) {
            this.pageFrameCursor = pageFrameCursor;
            this.symbolsReader = pageFrameCursor.getSymbolMapReader(gropuBySymbolColIndex);
            currentRow = -1;
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
        public boolean hasNext() {
            if (currentFrame == null) {
                currentFrame = pageFrameCursor.next();
                if (currentFrame == null) {
                    return false;
                }
                findInFrame(currentFrame);
            }

            if (++currentRow < rowsFound) {
                record.of(currentRow);
                if (currentRow == rowsFound) {
                    currentFrame = null;
                }
                return true;
            }
            return false;
        }

        private void findInFrame(PageFrame currentFrame) {
            long timestampBuff = currentFrame.getPageAddress(timestampIndex);
            long pageSize = currentFrame.getPageSize(timestampIndex);

            BitmapIndexReader symbolIndexReader = currentFrame.getBitmapIndexReader(gropuBySymbolColIndex, BitmapIndexReader.DIR_FORWARD);
            int key = 0;
            if (indexCursor == null) {
                indexCursor = symbolIndexReader.getNextFrame(key, currentFrame.getFirstRowId(), currentFrame.getFirstRowId() + pageSize);
            }

            IndexFrame frame = indexCursor.getNext();
            rowsFound = Vect.findFirstLastInFrame(
                    pageSize,
                    timestampBuff,
                    timestampSampler,
                    key,
                    frame.getAddress(),
                    frame.getSize(),
                    timestampOutBuff,
                    firstRowIdOutBuff,
                    lastRowIdOutBuff);

            currentRow = -1;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            currentFrame = null;
            pageFrameCursor.toTop();
        }

        @Override
        public long size() {
            return -1;
        }

        private class SampleByFirstLastRecord implements Record {
            private long firstRowIndex;

            public SampleByFirstLastRecord of(long currentRow) {
                firstRowIndex = Unsafe.getUnsafe().getLong(firstRowIdOutBuff + currentRow * Long.BYTES);
                return this;
            }

            @Override
            public long getTimestamp(int col) {
                if (col == timestampIndex) {
                    return Unsafe.getUnsafe().getLong(timestampOutBuff + currentRow * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstRowIndex * Long.BYTES);
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstRowIndex * Long.BYTES);
            }

            @Override
            public double getDouble(int col) {
                return Unsafe.getUnsafe().getDouble(currentFrame.getPageAddress(col) + firstRowIndex * Double.BYTES);
            }

            @Override
            public CharSequence getSym(int col) {
                int symbolId = Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + firstRowIndex * Double.BYTES);
                return symbolsReader.valueBOf(symbolId);
            }
        }
    }
}
