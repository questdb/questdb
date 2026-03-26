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
import io.questdb.cairo.vm.api.MemoryCR;
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
import io.questdb.std.str.Utf8Sequence;
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

    private final CoveringCursor cursor;
    private final PartitionFrameCursorFactory dfcFactory;
    private final int indexColumnIndex;
    private final int keyQueryPosition;
    private final boolean latestBy;
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
        this(metadata, dfcFactory, indexColumnIndex, symbolKey, symbolFunction, columnIndexes, columnSizeShifts, queryColToIncludeIdx, false);
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
            boolean latestBy
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;

        this.latestBy = latestBy;
        this.keyValueFuncs = null;
        this.resolvedKeys = null;
        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, 0, symInclCols, columnIndexes, latestBy);
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
        this(metadata, dfcFactory, indexColumnIndex, symbolKey, symbolFunction, columnIndexes, columnSizeShifts, queryColToIncludeIdx, keyValueFuncs, reader, false);
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
            TableReader reader,
            boolean latestBy
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;

        this.latestBy = latestBy;
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
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, multiKeyCapacity, symInclCols, columnIndexes, latestBy);
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
        Misc.free(symbolFunction);
        Misc.freeObjList(keyValueFuncs);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                latestBy ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
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
        } catch (Throwable th) {
            Misc.free(frameCursor);
            throw th;
        }
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
        sink.type(latestBy ? "CoveringIndex latest" : "CoveringIndex");
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
        assert false : "indexed column not found in columnIndexes";
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
        private Record activeRecord;
        private final IntList columnIndexes;
        private final CoveringRecord coveringRecord;
        private final FallbackRecord fallbackRecord;
        private final int indexColumnIndex;
        private final boolean latestBy;
        private final IntList multiKeys;
        private final int[] symbolIncludeCols;
        private final SymbolTable[] symTablesCache;
        private int cachedPartitionIndex;
        private long cachedRowHi;
        private long cachedRowLo;
        private CoveringRowCursor currentRowCursor;
        private int currentKeyIdx;
        private PartitionFrameCursor frameCursor;
        private boolean latestByDone;
        private int symbolKey;
        private TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                        int multiKeyCapacity, int[] symbolIncludeCols, IntList columnIndexes,
                        boolean latestBy) {
            this.indexColumnIndex = indexColumnIndex;
            this.symbolKey = symbolKey;
            this.coveringRecord = new CoveringRecord(queryColToIncludeIdx, symbolKey);
            this.fallbackRecord = new FallbackRecord(queryColToIncludeIdx, symbolKey, columnIndexes);
            this.multiKeys = multiKeyCapacity > 0 ? new IntList(multiKeyCapacity) : null;
            this.symbolIncludeCols = symbolIncludeCols;
            this.symTablesCache = symbolIncludeCols != null ? new SymbolTable[queryColToIncludeIdx.length] : null;
            this.columnIndexes = columnIndexes;
            this.latestBy = latestBy;
            this.cachedPartitionIndex = -1;
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
            currentRowCursor = null;
        }

        @Override
        public Record getRecord() {
            return activeRecord;
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
            if (latestBy) {
                return hasNextLatestBy();
            }
            while (true) {
                if (currentRowCursor != null && currentRowCursor.hasNext()) {
                    long rowId = currentRowCursor.next();
                    coveringRecord.setRowId(rowId);
                    fallbackRecord.setRowId(rowId);
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

        private boolean hasNextLatestBy() {
            if (multiKeys != null) {
                return hasNextLatestByMultiKey();
            }
            return hasNextLatestBySingleKey();
        }

        private boolean hasNextLatestByMultiKey() {
            while (currentKeyIdx < multiKeys.size()) {
                int rawSymbolKey = multiKeys.getQuick(currentKeyIdx);
                frameCursor.toTop(); // each key scans from the latest partition
                if (findLatestRow(rawSymbolKey)) {
                    currentKeyIdx++;
                    return true;
                }
                currentKeyIdx++;
            }
            return false;
        }

        private boolean hasNextLatestBySingleKey() {
            if (latestByDone) {
                return false;
            }
            if (findLatestRow(symbolKey)) {
                latestByDone = true;
                return true;
            }
            return false;
        }

        private boolean findLatestRow(int rawSymbolKey) {
            // Partitions iterate DESC (latest first). Find the first partition
            // with data for this key, iterate forward to the last row in that
            // partition, and return it as the LATEST ON result.
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return false;
                }
                BitmapIndexReader indexReader = tableReader.getBitmapIndexReader(
                        frame.getPartitionIndex(),
                        indexColumnIndex,
                        BitmapIndexReader.DIR_FORWARD
                );
                RowCursor rowCursor = indexReader.getCursor(
                        true,
                        TableUtils.toIndexKey(rawSymbolKey),
                        frame.getRowLo(),
                        frame.getRowHi() - 1
                );
                if (rowCursor instanceof CoveringRowCursor crc) {
                    long lastRowId = crc.seekToLast();
                    if (lastRowId >= 0) {
                        if (crc.hasCovering()) {
                            coveringRecord.of(crc);
                            coveringRecord.setSymbolKey(rawSymbolKey);
                            coveringRecord.setRowId(lastRowId);
                            activeRecord = coveringRecord;
                        } else {
                            fallbackRecord.of(tableReader, frame.getPartitionIndex());
                            fallbackRecord.setSymbolKey(rawSymbolKey);
                            fallbackRecord.setRowId(lastRowId);
                            activeRecord = fallbackRecord;
                        }
                        return true;
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
            latestByDone = false;
        }

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentRowCursor = null;
            this.currentKeyIdx = 0;
            this.cachedPartitionIndex = -1;
            this.latestByDone = false;
            this.activeRecord = coveringRecord;
            this.coveringRecord.of(null);
            SymbolTable indexSymbolTable = tableReader.getSymbolMapReader(indexColumnIndex);
            this.coveringRecord.setSymbolTable(indexSymbolTable);
            this.fallbackRecord.setSymbolTable(indexSymbolTable);
            if (symbolIncludeCols != null) {
                for (int i = 0, n = symbolIncludeCols.length; i < n; i++) {
                    int col = symbolIncludeCols[i];
                    symTablesCache[col] = tableReader.getSymbolMapReader(columnIndexes.getQuick(col));
                }
                coveringRecord.setIncludeSymbolTables(symTablesCache);
                fallbackRecord.setIncludeSymbolTables(symTablesCache);
            }
        }

        void ofEmpty() {
            this.frameCursor = null;
            this.tableReader = null;
            this.currentRowCursor = null;
            this.activeRecord = coveringRecord;
            this.coveringRecord.of(null);
        }

        void resolveKey(int resolvedKey) {
            this.symbolKey = resolvedKey;
            this.coveringRecord.setSymbolKey(resolvedKey);
            this.fallbackRecord.setSymbolKey(resolvedKey);
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
            if (rowCursor instanceof CoveringRowCursor crc) {
                currentRowCursor = crc;
                if (crc.hasCovering()) {
                    coveringRecord.of(crc);
                    coveringRecord.setSymbolKey(rawSymbolKey);
                    activeRecord = coveringRecord;
                } else {
                    fallbackRecord.of(tableReader, partitionIndex);
                    fallbackRecord.setSymbolKey(rawSymbolKey);
                    activeRecord = fallbackRecord;
                }
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
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredStrA(includeIdx);
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredStrB(includeIdx);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredVarcharA(includeIdx);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredVarcharB(includeIdx);
            }
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
            int includeIdx = getIncludeIdx(col);
            if (includeIdx == -1 && symbolTable != null) {
                return symbolTable.valueBOf(symbolKey);
            }
            if (includeIdx >= 0 && cursor != null && includeSymbolTables != null) {
                SymbolTable st = includeSymbolTables[col];
                if (st != null) {
                    return st.valueBOf(cursor.getCoveredInt(includeIdx));
                }
            }
            return null;
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

    /**
     * Fallback record that reads covered column values from column files via
     * TableReader when sidecar data is unavailable (unsealed partitions).
     * Zero per-row branching — the cursor swaps between CoveringRecord and
     * FallbackRecord at partition boundaries.
     */
    private static class FallbackRecord implements Record {
        private final IntList columnIndexes;
        private final int[] queryColToIncludeIdx;
        private SymbolTable[] includeSymbolTables;
        private int partitionIndex;
        private long rowId;
        private int symbolKey;
        private SymbolTable symbolTable;
        private TableReader tableReader;

        FallbackRecord(int[] queryColToIncludeIdx, int symbolKey, IntList columnIndexes) {
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.symbolKey = symbolKey;
            this.columnIndexes = columnIndexes;
        }

        @Override
        public boolean getBool(int col) {
            return getByte(col) != 0;
        }

        @Override
        public byte getByte(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getByte(rowId);
            }
            return 0;
        }

        @Override
        public long getDate(int col) {
            return getLong(col);
        }

        @Override
        public double getDouble(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getDouble(rowId * Double.BYTES);
            }
            return Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getFloat(rowId * Float.BYTES);
            }
            return Float.NaN;
        }

        @Override
        public int getInt(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getInt(rowId * Integer.BYTES);
            }
            return Integer.MIN_VALUE;
        }

        @Override
        public long getLong(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getLong(rowId * Long.BYTES);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public short getShort(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getShort(rowId * Short.BYTES);
            }
            return 0;
        }

        private final io.questdb.std.str.DirectString fallbackStrA = new io.questdb.std.str.DirectString();
        private final io.questdb.std.str.DirectString fallbackStrB = new io.questdb.std.str.DirectString();

        @Override
        public CharSequence getStrA(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(rowId * Long.BYTES);
                int len = dataMem.getInt(dataOffset);
                if (len == TableUtils.NULL_LEN) return null;
                fallbackStrA.of(dataMem.addressOf(dataOffset + Integer.BYTES), len);
                return fallbackStrA;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(rowId * Long.BYTES);
                int len = dataMem.getInt(dataOffset);
                if (len == TableUtils.NULL_LEN) return null;
                fallbackStrB.of(dataMem.addressOf(dataOffset + Integer.BYTES), len);
                return fallbackStrB;
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return io.questdb.cairo.VarcharTypeDriver.getSplitValue(auxMem, dataMem, rowId, 0);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return io.questdb.cairo.VarcharTypeDriver.getSplitValue(auxMem, dataMem, rowId, 1);
            }
            return null;
        }

        @Override
        public CharSequence getSymA(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx == -1 && symbolTable != null) {
                return symbolTable.valueOf(symbolKey);
            }
            if (includeIdx >= 0 && includeSymbolTables != null) {
                SymbolTable st = includeSymbolTables[col];
                if (st != null) {
                    return st.valueOf(getInt(col));
                }
            }
            return null;
        }

        @Override
        public CharSequence getSymB(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx == -1 && symbolTable != null) {
                return symbolTable.valueBOf(symbolKey);
            }
            if (includeIdx >= 0 && includeSymbolTables != null) {
                SymbolTable st = includeSymbolTables[col];
                if (st != null) {
                    return st.valueBOf(getInt(col));
                }
            }
            return null;
        }

        @Override
        public long getTimestamp(int col) {
            return getLong(col);
        }

        void of(TableReader reader, int partitionIndex) {
            this.tableReader = reader;
            this.partitionIndex = partitionIndex;
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

        private MemoryCR columnMem(int col) {
            int readerCol = columnIndexes.getQuick(col);
            int colBase = tableReader.getColumnBase(partitionIndex);
            return tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
        }

        private int getIncludeIdx(int col) {
            if (col < 0 || col >= queryColToIncludeIdx.length) {
                return -2;
            }
            return queryColToIncludeIdx[col];
        }
    }
}

