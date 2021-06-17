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
    private final long timestampOutAddress;
    private final long firstRowIdOutAddress;
    private final long lastRowIdOutAddress;
    private final SingleSymbolFilter symbolFilter;
    private final int gropuBySymbolColIndex;
    private final long crossFrameRow;

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
        timestampOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        firstRowIdOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        lastRowIdOutAddress = Unsafe.malloc(BUFF_SIZE * Long.BYTES);
        this.symbolFilter = symbolFilter;
        this.recordFirstLastIndex = buildFirstLastIndex(columns, symbolFilter.getColumnIndex(), timestampIndex);
        gropuBySymbolColIndex = symbolFilter.getColumnIndex();
        crossFrameRow = Unsafe.malloc(recordFirstLastIndex.length * Long.BYTES);
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
        Unsafe.free(timestampOutAddress, BUFF_SIZE * Long.BYTES);
        Unsafe.free(firstRowIdOutAddress, BUFF_SIZE * Long.BYTES);
        Unsafe.free(lastRowIdOutAddress, BUFF_SIZE * Long.BYTES);
        Unsafe.free(crossFrameRow, recordFirstLastIndex.length * Long.BYTES);
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
        private long outIndex;
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
        private boolean foundInDataPage;

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
                if (foundInDataPage) {
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
                        foundInDataPage = false;
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

            if (outIndex == 1 && rowsFound > 1) {
                // Copy last set of first(), last() rowids from bottom of the previous output to the top
                long lastFoundOffset = (rowsFound - 1) * Long.BYTES;
                Unsafe.getUnsafe().putLong(
                        firstRowIdOutAddress,
                        Unsafe.getUnsafe().getLong(firstRowIdOutAddress + lastFoundOffset)
                );
                Unsafe.getUnsafe().putLong(
                        lastRowIdOutAddress,
                        Unsafe.getUnsafe().getLong(lastRowIdOutAddress + lastFoundOffset)
                );
                Unsafe.getUnsafe().putLong(
                        timestampOutAddress,
                        Unsafe.getUnsafe().getLong(timestampOutAddress + lastFoundOffset)
                );
            }

            rowsFound = Vect.findFirstLastInFrame(
                    outIndex,
                    firstTimestamp,
                    frameFirstRowId,
                    pageRowSize + frameFirstRowId,
                    timestampColumnAddress,
                    timestampSampler,
                    indexFrame.getAddress(),
                    indexFrame.getSize(),
                    indexFrameIndex,
                    timestampOutAddress,
                    firstRowIdOutAddress,
                    lastRowIdOutAddress,
                    BUFF_SIZE);

            // When first() and last() rowids are on different index or data pages
            // we need to do more passes before knowing that last() rowid is really last
            // and next data frame / index frame will not change it
            // Hide last returned row from the cursor output
            // and copy it back to index 0 for next frame result set setting outIndex to 1
            // so that Vect code continues setting last() to the top output row
            if (rowsFound > outIndex || outIndex > 0) {
                saveLastValuesToBuffer(0);
            }
            foundInDataPage = rowsFound > outIndex;

            if (rowsFound > outIndex) {
                assert rowsFound < BUFF_SIZE;

                // Read output timestamp and rowId to resume at the end of the output buffers
                indexFrameIndex = Unsafe.getUnsafe().getLong(firstRowIdOutAddress + rowsFound * Long.BYTES);
                if (indexFrameIndex >= indexFrame.getSize() - 1) {
                    indexFrame = null;
                }
                long nextFirstRowId = Unsafe.getUnsafe().getLong(lastRowIdOutAddress + rowsFound * Long.BYTES);
                assert nextFirstRowId > frameNextRowId;
                frameNextRowId = nextFirstRowId;
                firstTimestamp = Unsafe.getUnsafe().getLong(timestampOutAddress + rowsFound * Long.BYTES);

                outIndex = Math.min(rowsFound - outIndex, 1);
                return rowsFound > 1;
            }

            // index frame done but nothing found
            indexFrame = null;
            return false;
        }

        private void saveFirstLastValuesToBuffer(long outRow) {
            firstLastRowId[0] = Unsafe.getUnsafe().getLong(firstRowIdOutAddress + outRow * Long.BYTES) - frameFirstRowId;
            firstLastRowId[1] = Unsafe.getUnsafe().getLong(lastRowIdOutAddress + outRow * Long.BYTES) - frameFirstRowId;
            for (int i = 0; i < recordFirstLastIndex.length; i++) {
                if (i == timestampIndex) {
                    Unsafe.getUnsafe().putLong(
                            crossFrameRow + i * Long.BYTES,
                            Unsafe.getUnsafe().getLong(timestampOutAddress + outRow * Long.BYTES)
                    );
                } else {
                    saveFixedColToBufferWithLongAlignment(i, crossFrameRow, groupByMetadata.getColumnType(i), currentFrame.getPageAddress(i), firstLastRowId[recordFirstLastIndex[i]]);
                }
            }
        }


        private void saveLastValuesToBuffer(long outRow) {
            long lastRowId = Unsafe.getUnsafe().getLong(lastRowIdOutAddress + outRow * Long.BYTES) - frameFirstRowId;
            for (int i = 0; i < recordFirstLastIndex.length; i++) {
                if (recordFirstLastIndex[i] == 1) {
                    // last()
                    saveFixedColToBufferWithLongAlignment(i, crossFrameRow, groupByMetadata.getColumnType(i), currentFrame.getPageAddress(i), lastRowId);
                }
            }
        }

        private void saveFixedColToBufferWithLongAlignment(int index, long saveBufferAddress, int columnType, long pageAddress, long rowId) {
            switch (ColumnType.pow2SizeOf(columnType)) {
                case 3:
                    Unsafe.getUnsafe().putLong(
                            saveBufferAddress + index * Long.BYTES,
                            Unsafe.getUnsafe().getLong(pageAddress + rowId * Long.BYTES)
                    );
                    break;
                case 2:
                    Unsafe.getUnsafe().putInt(
                            saveBufferAddress + index * Long.BYTES,
                            Unsafe.getUnsafe().getInt(pageAddress + rowId * Integer.BYTES)
                    );
                    break;
                case 1:
                    Unsafe.getUnsafe().putShort(
                            saveBufferAddress + index * Long.BYTES,
                            Unsafe.getUnsafe().getShort(pageAddress + rowId * Short.BYTES)
                    );
                    break;
                case 0:
                    Unsafe.getUnsafe().putByte(
                            saveBufferAddress + index * Long.BYTES,
                            Unsafe.getUnsafe().getByte(pageAddress + rowId)
                    );
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
            foundInDataPage = false;
            outIndex = 0;
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
                firstLastRowId[0] = Unsafe.getUnsafe().getLong(firstRowIdOutAddress + currentRow * Long.BYTES) - frameFirstRowId;
                firstLastRowId[1] = Unsafe.getUnsafe().getLong(lastRowIdOutAddress + currentRow * Long.BYTES) - frameFirstRowId;
                return this;
            }

            @Override
            public long getTimestamp(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow + col * Long.BYTES);
                }

                if (col == timestampIndex) {
                    // Special case - timestamp the sample by runs on
                    // Take it from timestampOutBuff instead of column
                    // It's the value of the beginning of the group, not where the first row found
                    return Unsafe.getUnsafe().getLong(timestampOutAddress + currentRow * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
            }

            @Override
            public long getLong(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getLong(crossFrameRow + col * Long.BYTES);
                }

                return Unsafe.getUnsafe().getLong(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Long.BYTES);
            }

            @Override
            public double getDouble(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getDouble(crossFrameRow + col * Long.BYTES);
                }

                return Unsafe.getUnsafe().getDouble(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Double.BYTES);
            }

            @Override
            public CharSequence getSym(int col) {
                int symbolId;
                if (currentRow == 0) {
                    symbolId = Unsafe.getUnsafe().getInt(crossFrameRow + col * Long.BYTES);
                } else {
                    symbolId = Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
                }
                return symbolsReader.valueBOf(symbolId);
            }

            @Override
            public int getInt(int col) {
                if (currentRow == 0) {
                    return Unsafe.getUnsafe().getInt(crossFrameRow + col * Long.BYTES);
                }
                return Unsafe.getUnsafe().getInt(currentFrame.getPageAddress(col) + firstLastRowId[recordFirstLastIndex[col]] * Integer.BYTES);
            }
        }
    }
}
