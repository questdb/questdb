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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * A RecordCursorFactory that reads covered column values directly from the
 * posting index sidecar files, bypassing column files entirely.
 * <p>
 * Applicable when ALL selected columns are either:
 * - The indexed symbol column (value known from the WHERE key)
 * - Included in the INCLUDE list (values stored in .pc0, .pc1, ... sidecar files)
 * <p>
 * Supports single-key (WHERE sym = 'A'), bind variable (WHERE sym = $1),
 * and multi-key (WHERE sym IN ('A', 'B')) queries.
 */
public class CoveringIndexRecordCursorFactory implements RecordCursorFactory {
    private final IntList columnIndexes;
    private final IntList columnSizeShifts;
    private final CoveringCursor cursor;
    private final PartitionFrameCursorFactory dfcFactory;
    private final int indexColumnIndex;
    private final int keyQueryPosition;
    private final ObjList<Function> keyValueFuncs;
    private final RecordMetadata metadata;
    private final IntList resolvedKeys;
    private final Function symbolFunction;

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            @NotNull int[] queryColToIncludeIdx
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.keyValueFuncs = null;
        this.resolvedKeys = null;
        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, 0, symInclCols, columnIndexes);
    }

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            @NotNull int[] queryColToIncludeIdx,
            ObjList<Function> keyValueFuncs,
            TableReader reader
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.keyValueFuncs = keyValueFuncs;
        this.resolvedKeys = keyValueFuncs != null ? new IntList(keyValueFuncs.size()) : null;
        int multiKeyCapacity = 0;
        if (keyValueFuncs != null && reader != null) {
            multiKeyCapacity = keyValueFuncs.size();
            SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
            for (int i = 0, n = keyValueFuncs.size(); i < n; i++) {
                Function f = keyValueFuncs.getQuick(i);
                int key = f.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND : smr.keyOf(f.getStrA(null));
                resolvedKeys.add(key);
            }
        }
        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, multiKeyCapacity, symInclCols, columnIndexes);
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

        if (resolvedKeys != null) {
            // Multi-key path (IN list)
            SymbolMapReader smr = frameCursor.getTableReader().getSymbolMapReader(indexColumnIndex);
            cursor.multiKeys.clear();
            boolean hasAnyKey = false;
            for (int i = 0, n = resolvedKeys.size(); i < n; i++) {
                int key = resolvedKeys.getQuick(i);
                if (key == SymbolTable.VALUE_NOT_FOUND && keyValueFuncs != null) {
                    CharSequence symValue = keyValueFuncs.getQuick(i).getStrA(null);
                    key = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                }
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    cursor.multiKeys.add(key);
                    hasAnyKey = true;
                }
            }
            if (!hasAnyKey) {
                Misc.free(frameCursor);
                cursor.ofEmpty();
                return cursor;
            }
            cursor.of(frameCursor);
            return cursor;
        }

        // Single-key path
        int resolvedKey = cursor.symbolKey;
        if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
            SymbolMapReader symbolMapReader = frameCursor.getTableReader().getSymbolMapReader(indexColumnIndex);
            CharSequence symValue = symbolFunction.getStrA(null);
            resolvedKey = symValue != null ? symbolMapReader.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
            if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
                Misc.free(frameCursor);
                cursor.ofEmpty();
                return cursor;
            }
            cursor.resolveKey(resolvedKey);
        }
        cursor.of(frameCursor);
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
        sink.meta("on").putColumnName(keyQueryPosition);
        if (keyValueFuncs != null) {
            sink.attr("filter").putColumnName(keyQueryPosition).val(" IN ").val(keyValueFuncs);
        } else {
            sink.attr("filter").putColumnName(keyQueryPosition).val('=').val(symbolFunction);
        }
    }

    private static int findQueryPosition(IntList columnIndexes, int readerColIdx) {
        for (int q = 0, n = columnIndexes.size(); q < n; q++) {
            if (columnIndexes.getQuick(q) == readerColIdx) {
                return q;
            }
        }
        return 0;
    }

    private static int[] findSymbolIncludeCols(int[] queryColToIncludeIdx, RecordMetadata metadata) {
        int count = 0;
        for (int q = 0; q < queryColToIncludeIdx.length; q++) {
            if (queryColToIncludeIdx[q] >= 0 && ColumnType.tagOf(metadata.getColumnType(q)) == ColumnType.SYMBOL) {
                count++;
            }
        }
        if (count == 0) {
            return null;
        }
        int[] result = new int[count];
        int idx = 0;
        for (int q = 0; q < queryColToIncludeIdx.length; q++) {
            if (queryColToIncludeIdx[q] >= 0 && ColumnType.tagOf(metadata.getColumnType(q)) == ColumnType.SYMBOL) {
                result[idx++] = q;
            }
        }
        return result;
    }

    private static class CoveringCursor implements RecordCursor {
        private final IntList columnIndexes;
        private final int indexColumnIndex;
        private final IntList multiKeys;
        private final CoveringRecord record;
        private final int[] symbolIncludeCols;
        private final SymbolTable[] symTablesCache;
        private int cachedPartitionIndex;
        private long cachedRowHi;
        private long cachedRowLo;
        private CoveringRowCursor currentRowCursor;
        private int currentKeyIdx;
        private PartitionFrameCursor frameCursor;
        private int symbolKey;
        private TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                        int multiKeyCapacity, int[] symbolIncludeCols, IntList columnIndexes) {
            this.indexColumnIndex = indexColumnIndex;
            this.symbolKey = symbolKey;
            this.record = new CoveringRecord(queryColToIncludeIdx, symbolKey);
            this.multiKeys = multiKeyCapacity > 0 ? new IntList(multiKeyCapacity) : null;
            this.symbolIncludeCols = symbolIncludeCols;
            this.symTablesCache = symbolIncludeCols != null ? new SymbolTable[queryColToIncludeIdx.length] : null;
            this.columnIndexes = columnIndexes;
            this.cachedPartitionIndex = -1;
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
            if (frameCursor == null) {
                return false;
            }
            while (true) {
                if (currentRowCursor != null && currentRowCursor.hasNext()) {
                    record.setRowId(currentRowCursor.next());
                    return true;
                }
                if (multiKeys != null) {
                    if (!advanceMultiKey()) {
                        return false;
                    }
                } else {
                    if (!advanceSingleKey()) {
                        return false;
                    }
                }
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(columnIndex);
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
            if (frameCursor != null) {
                frameCursor.toTop();
            }
            currentRowCursor = null;
            currentKeyIdx = 0;
            cachedPartitionIndex = -1;
        }

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentRowCursor = null;
            this.currentKeyIdx = 0;
            this.cachedPartitionIndex = -1;
            this.record.of(null);
            this.record.setSymbolTable(tableReader.getSymbolMapReader(indexColumnIndex));
            if (symbolIncludeCols != null) {
                for (int i = 0, n = symbolIncludeCols.length; i < n; i++) {
                    int col = symbolIncludeCols[i];
                    symTablesCache[col] = tableReader.getSymbolMapReader(columnIndexes.getQuick(col));
                }
                record.setIncludeSymbolTables(symTablesCache);
            }
        }

        void ofEmpty() {
            this.frameCursor = null;
            this.tableReader = null;
            this.currentRowCursor = null;
            this.record.of(null);
        }

        void resolveKey(int resolvedKey) {
            this.symbolKey = resolvedKey;
            this.record.setSymbolKey(resolvedKey);
        }

        private boolean advanceMultiKey() {
            // Try remaining keys in cached partition
            if (cachedPartitionIndex >= 0) {
                currentKeyIdx++;
                while (currentKeyIdx < multiKeys.size()) {
                    if (tryOpenKey(cachedPartitionIndex, multiKeys.getQuick(currentKeyIdx), cachedRowLo, cachedRowHi)) {
                        return true;
                    }
                    currentKeyIdx++;
                }
            }
            // Advance to next partition and try all keys
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    cachedPartitionIndex = -1;
                    return false;
                }
                cachedPartitionIndex = frame.getPartitionIndex();
                cachedRowLo = frame.getRowLo();
                cachedRowHi = frame.getRowHi();
                currentKeyIdx = 0;
                while (currentKeyIdx < multiKeys.size()) {
                    if (tryOpenKey(cachedPartitionIndex, multiKeys.getQuick(currentKeyIdx), cachedRowLo, cachedRowHi)) {
                        return true;
                    }
                    currentKeyIdx++;
                }
            }
        }

        private boolean advanceSingleKey() {
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return false;
                }
                if (tryOpenKey(frame.getPartitionIndex(), symbolKey, frame.getRowLo(), frame.getRowHi())) {
                    return true;
                }
            }
        }

        private boolean tryOpenKey(int partitionIndex, int rawSymbolKey, long rowLo, long rowHi) {
            BitmapIndexReader indexReader = tableReader.getBitmapIndexReader(
                    partitionIndex,
                    indexColumnIndex,
                    BitmapIndexReader.DIR_FORWARD
            );
            RowCursor rowCursor = indexReader.getCursor(
                    true,
                    TableUtils.toIndexKey(rawSymbolKey),
                    rowLo,
                    rowHi - 1
            );
            if (rowCursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                currentRowCursor = crc;
                record.of(crc);
                record.setSymbolKey(rawSymbolKey);
                return true;
            }
            return false;
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
        private CoveringRowCursor cursor;
        private SymbolTable[] includeSymbolTables;
        private long rowId;
        private int symbolKey;
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
            if (includeIdx >= 0 && cursor != null && includeSymbolTables != null) {
                SymbolTable st = includeSymbolTables[col];
                if (st != null) {
                    return st.valueOf(cursor.getCoveredInt(includeIdx));
                }
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

        void setIncludeSymbolTables(SymbolTable[] tables) {
            this.includeSymbolTables = tables;
        }

        void setRowId(long rowId) {
            this.rowId = rowId;
        }

        void setSymbolKey(int key) {
            this.symbolKey = key;
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
