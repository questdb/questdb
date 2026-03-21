/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * A RecordCursorFactory that reads covered column values directly from the
 * posting index sidecar files, bypassing column files entirely.
 * <p>
 * Applicable when ALL selected columns are either:
 * - The indexed symbol column (value known from the WHERE key)
 * - Included in the INCLUDE list (values stored in .pc0, .pc1, ... sidecar files)
 * <p>
 * This transforms random column file reads into sequential sidecar reads,
 * grouped by index key.
 */
public class CoveringIndexRecordCursorFactory implements RecordCursorFactory {
    private final IntList columnIndexes;
    private final IntList columnSizeShifts;
    private final CoveringCursor cursor;
    private final PartitionFrameCursorFactory dfcFactory;
    private final int indexColumnIndex;
    private final RecordMetadata metadata;
    private final Function symbolFunction;

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            @NotNull Function symbolFunction,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            @NotNull int[] queryColToIncludeIdx
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx);
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
        Misc.free(symbolFunction);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        cursor.of(frameCursor, executionContext);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CoveringIndex");
        sink.meta("on").putColumnName(indexColumnIndex);
        sink.attr("filter").putColumnName(indexColumnIndex).val('=').val(symbolFunction);
    }

    private static class CoveringCursor implements RecordCursor {
        private final CoveringRecord record;
        private final int indexColumnIndex;
        private final int symbolKey;
        private CoveringRowCursor currentRowCursor;
        private PartitionFrameCursor frameCursor;
        private TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx) {
            this.indexColumnIndex = indexColumnIndex;
            this.symbolKey = TableUtils.toIndexKey(symbolKey);
            this.record = new CoveringRecord(queryColToIncludeIdx, symbolKey);
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
            currentRowCursor = null;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException("CoveringIndex does not support random access");
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return frameCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentRowCursor != null && currentRowCursor.hasNext()) {
                    record.setRowId(currentRowCursor.next());
                    return true;
                }
                if (!advanceToNextPartition()) {
                    return false;
                }
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(columnIndex);
        }

        void of(PartitionFrameCursor frameCursor, SqlExecutionContext executionContext) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentRowCursor = null;
            this.record.of(null);
            this.record.setSymbolTable(tableReader.getSymbolMapReader(indexColumnIndex));
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("CoveringIndex does not support random access");
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            frameCursor.toTop();
            currentRowCursor = null;
        }

        private boolean advanceToNextPartition() {
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return false;
                }
                int partitionIndex = frame.getPartitionIndex();
                BitmapIndexReader indexReader = tableReader.getBitmapIndexReader(
                        partitionIndex,
                        indexColumnIndex,
                        BitmapIndexReader.DIR_FORWARD
                );
                RowCursor rowCursor = indexReader.getCursor(
                        true,
                        symbolKey,
                        frame.getRowLo(),
                        frame.getRowHi() - 1
                );
                if (rowCursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                    currentRowCursor = crc;
                    record.of(crc);
                    return true;
                }
                // Fallback: not a covering reader — still iterate row IDs but without sidecar data
                if (rowCursor instanceof CoveringRowCursor crc2) {
                    currentRowCursor = crc2;
                    record.of(crc2);
                    return true;
                }
                // Non-CoveringRowCursor partition — skip to next
            }
        }
    }

    /**
     * Record implementation that reads covered column values from the posting
     * index sidecar files. The {@code queryColToIncludeIdx} mapping is keyed
     * by query column position (0 to queryColCount-1).
     * <p>
     * Mapping values: {@code >= 0} = sidecar include index,
     * {@code -1} = indexed symbol column.
     */
    private static class CoveringRecord implements Record {
        private final int[] queryColToIncludeIdx;
        private final int symbolKey;
        private CoveringRowCursor cursor;
        private long rowId;
        private SymbolTable symbolTable;

        CoveringRecord(int[] queryColToIncludeIdx, int symbolKey) {
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.symbolKey = symbolKey;
        }

        @Override
        public boolean getBool(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredByte(includeIdx) != 0;
            }
            return false;
        }

        @Override
        public byte getByte(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredByte(includeIdx);
            }
            return 0;
        }

        @Override
        public long getDate(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong(includeIdx);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public double getDouble(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredDouble(includeIdx);
            }
            return Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredFloat(includeIdx);
            }
            return Float.NaN;
        }

        @Override
        public int getInt(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredInt(includeIdx);
            }
            return Integer.MIN_VALUE;
        }

        @Override
        public long getLong(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong(includeIdx);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public short getShort(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredShort(includeIdx);
            }
            return 0;
        }

        @Override
        public CharSequence getStrA(int col) {
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            return null;
        }

        @Override
        public CharSequence getSymA(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx == -1 && symbolTable != null) {
                return symbolTable.valueOf(symbolKey);
            }
            return null;
        }

        @Override
        public CharSequence getSymB(int col) {
            return getSymA(col);
        }

        @Override
        public long getTimestamp(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong(includeIdx);
            }
            return Long.MIN_VALUE;
        }

        void of(CoveringRowCursor cursor) {
            this.cursor = cursor;
        }

        void setRowId(long rowId) {
            this.rowId = rowId;
        }

        void setSymbolTable(SymbolTable symbolTable) {
            this.symbolTable = symbolTable;
        }

        private int getIncludeIdx(int col) {
            if (col < 0 || col >= queryColToIncludeIdx.length) {
                return -2;
            }
            return queryColToIncludeIdx[col];
        }
    }
}
