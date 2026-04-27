/*+*****************************************************************************
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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

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

    private final PartitionFrameCursorFactory dfcFactory;
    private final int indexColumnIndex;
    private final int keyQueryPosition;
    private final ObjList<Function> keyValueFuncs;
    private final boolean latestBy;
    private final Function latestByFilter;
    private final RecordMetadata metadata;
    private final MultiKeyCoveringCursor multiKeyCursor;
    private final MultiKeyCoveringPageFrameCursor multiKeyPageFrameCursor;
    private final int[] queryColToIncludeIdx;
    private final IntList resolvedKeys;
    private final SingleKeyCoveringCursor singleKeyCursor;
    private final SingleKeyCoveringPageFrameCursor singleKeyPageFrameCursor;
    private final Function symbolFunction;
    private final boolean symbolFunctionRuntimeConstant;

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            int @NotNull [] queryColToIncludeIdx,
            @Nullable ObjList<Function> keyValueFuncs,
            @Nullable TableReader reader,
            boolean latestBy,
            @Nullable Function latestByFilter
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;
        this.symbolFunctionRuntimeConstant = symbolKey == SymbolTable.VALUE_NOT_FOUND;
        this.latestBy = latestBy;
        this.latestByFilter = latestByFilter;
        this.queryColToIncludeIdx = queryColToIncludeIdx;
        this.keyValueFuncs = keyValueFuncs;
        int[] requiredIncludeIndices = buildRequiredIncludeIndices(queryColToIncludeIdx);

        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        if (keyValueFuncs != null) {
            this.resolvedKeys = new IntList(keyValueFuncs.size());
            int multiKeyCapacity = keyValueFuncs.size();
            if (reader != null) {
                SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                for (int i = 0, n = keyValueFuncs.size(); i < n; i++) {
                    Function f = keyValueFuncs.getQuick(i);
                    int key = f.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND : smr.keyOf(f.getStrA(null));
                    resolvedKeys.add(key);
                }
            }
            this.multiKeyCursor = new MultiKeyCoveringCursor(indexColumnIndex, multiKeyCapacity, queryColToIncludeIdx, requiredIncludeIndices, symInclCols, columnIndexes, latestBy, metadata);
            this.singleKeyCursor = null;
            this.multiKeyPageFrameCursor = !latestBy
                    ? new MultiKeyCoveringPageFrameCursor(indexColumnIndex, queryColToIncludeIdx, requiredIncludeIndices, metadata, columnIndexes)
                    : null;
            this.singleKeyPageFrameCursor = null;
        } else {
            this.resolvedKeys = null;
            this.singleKeyCursor = new SingleKeyCoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, requiredIncludeIndices, symInclCols, columnIndexes, latestBy, metadata);
            this.multiKeyCursor = null;
            this.singleKeyPageFrameCursor = !latestBy
                    ? new SingleKeyCoveringPageFrameCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, requiredIncludeIndices, metadata, columnIndexes)
                    : null;
            this.multiKeyPageFrameCursor = null;
        }
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
        Misc.free(latestByFilter);
        Misc.free(symbolFunction);
        Misc.freeObjList(keyValueFuncs);
        Misc.free(singleKeyCursor);
        Misc.free(multiKeyCursor);
        Misc.free(singleKeyPageFrameCursor);
        Misc.free(multiKeyPageFrameCursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                latestBy ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            if (multiKeyCursor != null) {
                if (keyValueFuncs != null) {
                    Function.init(keyValueFuncs, frameCursor, executionContext, null);
                }
                SymbolMapReader smr = frameCursor.getTableReader().getSymbolMapReader(indexColumnIndex);
                multiKeyCursor.multiKeys.clear();
                boolean hasAnyKey = false;
                for (int i = 0, n = resolvedKeys.size(); i < n; i++) {
                    int key = resolvedKeys.getQuick(i);
                    if (key == SymbolTable.VALUE_NOT_FOUND && keyValueFuncs != null) {
                        CharSequence symValue = keyValueFuncs.getQuick(i).getStrA(null);
                        key = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                    }
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        multiKeyCursor.multiKeys.add(key);
                        hasAnyKey = true;
                    }
                }
                if (!hasAnyKey) {
                    Misc.free(frameCursor);
                    multiKeyCursor.ofEmpty();
                    return multiKeyCursor;
                }
                multiKeyCursor.of(frameCursor);
                multiKeyCursor.latestByFilter = latestByFilter;
                if (latestByFilter != null) {
                    latestByFilter.init(multiKeyCursor, executionContext);
                }
                return multiKeyCursor;
            }

            int resolvedKey;
            if (!this.symbolFunctionRuntimeConstant) {
                resolvedKey = singleKeyCursor.symbolKey;
            } else {
                symbolFunction.init(frameCursor, executionContext);
                SymbolMapReader symbolMapReader = frameCursor.getTableReader().getSymbolMapReader(indexColumnIndex);
                CharSequence symValue = symbolFunction.getStrA(null);
                resolvedKey = symValue != null ? symbolMapReader.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
            }
            if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
                Misc.free(frameCursor);
                singleKeyCursor.ofEmpty();
                return singleKeyCursor;
            }
            singleKeyCursor.resolveKey(resolvedKey);
            singleKeyCursor.of(frameCursor);
            singleKeyCursor.latestByFilter = latestByFilter;
            if (latestByFilter != null) {
                latestByFilter.init(singleKeyCursor, executionContext);
            }
            return singleKeyCursor;
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
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (multiKeyPageFrameCursor == null && singleKeyPageFrameCursor == null) {
            return null;
        }
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            TableReader reader = frameCursor.getTableReader();
            if (multiKeyPageFrameCursor != null) {
                if (keyValueFuncs != null) {
                    Function.init(keyValueFuncs, frameCursor, executionContext, null);
                }
                SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                multiKeyPageFrameCursor.multiKeys.clear();
                for (int i = 0, n = resolvedKeys.size(); i < n; i++) {
                    int key = resolvedKeys.getQuick(i);
                    if (key == SymbolTable.VALUE_NOT_FOUND && keyValueFuncs != null) {
                        CharSequence symValue = keyValueFuncs.getQuick(i).getStrA(null);
                        key = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                    }
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        multiKeyPageFrameCursor.multiKeys.add(key);
                    }
                }
                if (multiKeyPageFrameCursor.multiKeys.size() == 0) {
                    Misc.free(frameCursor);
                    multiKeyPageFrameCursor.ofEmpty();
                    return multiKeyPageFrameCursor;
                }
                multiKeyPageFrameCursor.of(frameCursor);
                return multiKeyPageFrameCursor;
            }
            // Single-key path: see the matching block in getCursor().
            int resolvedKey;
            if (!this.symbolFunctionRuntimeConstant) {
                resolvedKey = singleKeyPageFrameCursor.symbolKey;
            } else {
                symbolFunction.init(frameCursor, executionContext);
                SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                CharSequence symValue = symbolFunction.getStrA(null);
                resolvedKey = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
            }
            if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
                Misc.free(frameCursor);
                singleKeyPageFrameCursor.ofEmpty();
                return singleKeyPageFrameCursor;
            }
            singleKeyPageFrameCursor.resolvedKey = resolvedKey;
            singleKeyPageFrameCursor.of(frameCursor);
            return singleKeyPageFrameCursor;
        } catch (Throwable th) {
            Misc.free(frameCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return singleKeyPageFrameCursor != null || multiKeyPageFrameCursor != null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CoveringIndex");
        if (latestBy) {
            sink.meta("op").val("latest");
        }
        sink.meta("on").putColumnName(keyQueryPosition);
        boolean first = true;
        for (int q = 0; q < queryColToIncludeIdx.length; q++) {
            if (queryColToIncludeIdx[q] >= 0) {
                if (first) {
                    sink.meta("with");
                    first = false;
                } else {
                    sink.val(", ");
                }
                sink.putColumnName(q);
            }
        }
        if (keyValueFuncs != null) {
            sink.attr("filter").putColumnName(keyQueryPosition).val(" IN ").val(keyValueFuncs);
        } else {
            sink.attr("filter").putColumnName(keyQueryPosition).val('=').val(symbolFunction);
        }
    }

    private static int[] buildRequiredIncludeIndices(int[] queryColToIncludeIdx) {
        int max = -1;
        for (int idx : queryColToIncludeIdx) {
            if (idx > max) {
                max = idx;
            }
        }
        if (max < 0) {
            return new int[0];
        }
        boolean[] seen = new boolean[max + 1];
        int count = 0;
        for (int idx : queryColToIncludeIdx) {
            if (idx >= 0 && !seen[idx]) {
                seen[idx] = true;
                count++;
            }
        }
        int[] result = new int[count];
        int w = 0;
        for (int i = 0; i <= max; i++) {
            if (seen[i]) {
                result[w++] = i;
            }
        }
        return result;
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

    private static abstract class CoveringCursor implements RecordCursor {
        protected final IntList columnIndexes;
        protected final CoveringRecord coveringRecord;
        protected final FallbackRecord fallbackRecord;
        protected final int indexColumnIndex;
        protected final boolean latestBy;
        protected final int[] requiredIncludeIndices;
        protected final SymbolTable[] symTablesCache;
        protected final int[] symbolIncludeCols;
        protected Record activeRecord;
        protected boolean allCovered;
        protected CoveringRowCursor currentRowCursor;
        protected PartitionFrameCursor frameCursor;
        protected Function latestByFilter;
        protected TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                       int[] requiredIncludeIndices, int[] symbolIncludeCols, IntList columnIndexes,
                       boolean latestBy, RecordMetadata metadata) {
            this.indexColumnIndex = indexColumnIndex;
            this.fallbackRecord = new FallbackRecord(queryColToIncludeIdx, symbolKey, columnIndexes, metadata);
            this.coveringRecord = new CoveringRecord(queryColToIncludeIdx, symbolKey, metadata, fallbackRecord);
            this.requiredIncludeIndices = requiredIncludeIndices;
            this.symbolIncludeCols = symbolIncludeCols;
            this.symTablesCache = symbolIncludeCols != null ? new SymbolTable[queryColToIncludeIdx.length] : null;
            this.columnIndexes = columnIndexes;
            this.latestBy = latestBy;
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
            this.currentRowCursor = Misc.free(currentRowCursor);
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
            return frameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
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
                    if (!allCovered) {
                        fallbackRecord.setRowId(rowId);
                    }
                    return true;
                }
                if (!advanceKey()) {
                    return false;
                }
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
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
        public void toTop() {
            if (frameCursor != null) {
                frameCursor.toTop();
            }
            currentRowCursor = Misc.free(currentRowCursor);
            resetIterationState();
        }

        abstract boolean advanceKey();

        protected boolean allRequiredCovered(CoveringRowCursor crc) {
            for (int requiredIncludeIndex : requiredIncludeIndices) {
                if (!crc.isCoveredAvailable(requiredIncludeIndex)) {
                    return false;
                }
            }
            return true;
        }

        boolean findLatestRow(int rawSymbolKey) {
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return false;
                }
                int partitionIndex = frame.getPartitionIndex();
                long rowLo = frame.getRowLo();
                long rowHi = frame.getRowHi() - 1;
                int indexKey = TableUtils.toIndexKey(rawSymbolKey);

                if (latestByFilter != null) {
                    IndexReader bwdReader = tableReader.getIndexReader(
                            partitionIndex, indexColumnIndex, IndexReader.DIR_BACKWARD);
                    RowCursor bwdCursor = bwdReader.getCursor(indexKey, rowLo, rowHi, requiredIncludeIndices);
                    try {
                        if (bwdCursor instanceof CoveringRowCursor crc) {
                            Misc.free(currentRowCursor);
                            currentRowCursor = crc;
                            bwdCursor = null;
                            coveringRecord.of(crc);
                            coveringRecord.setSymbolKey(rawSymbolKey);
                            allCovered = allRequiredCovered(crc);
                            coveringRecord.setAllCovered(allCovered);
                            if (!allCovered) {
                                // fallbackRecord serves per-column fallback only
                                // when some include slot is missing for this partition.
                                fallbackRecord.of(tableReader, partitionIndex);
                                fallbackRecord.setSymbolKey(rawSymbolKey);
                            }
                            while (crc.hasNext()) {
                                long rowId = crc.next();
                                coveringRecord.setRowId(rowId);
                                if (!allCovered) {
                                    fallbackRecord.setRowId(rowId);
                                }
                                if (latestByFilter.getBool(coveringRecord)) {
                                    activeRecord = coveringRecord;
                                    return true;
                                }
                            }
                        } else {
                            // Stale crc from a prior covering partition is no longer the
                            // source of the active record once we switch to fallbackRecord.
                            currentRowCursor = Misc.free(currentRowCursor);
                            fallbackRecord.of(tableReader, partitionIndex);
                            fallbackRecord.setSymbolKey(rawSymbolKey);
                            while (bwdCursor.hasNext()) {
                                long rowId = bwdCursor.next();
                                fallbackRecord.setRowId(rowId);
                                if (latestByFilter.getBool(fallbackRecord)) {
                                    activeRecord = fallbackRecord;
                                    return true;
                                }
                            }
                        }
                    } finally {
                        Misc.free(bwdCursor);
                    }
                } else {
                    IndexReader bwdReader = tableReader.getIndexReader(
                            partitionIndex, indexColumnIndex, IndexReader.DIR_BACKWARD);
                    RowCursor rowCursor = bwdReader.getCursor(indexKey, rowLo, rowHi, requiredIncludeIndices);
                    try {
                        if (rowCursor instanceof CoveringRowCursor crc) {
                            long lastRowId = crc.seekToLast();
                            if (lastRowId >= 0) {
                                Misc.free(currentRowCursor);
                                currentRowCursor = crc;
                                rowCursor = null;
                                coveringRecord.of(crc);
                                coveringRecord.setSymbolKey(rawSymbolKey);
                                coveringRecord.setRowId(lastRowId);
                                allCovered = allRequiredCovered(crc);
                                coveringRecord.setAllCovered(allCovered);
                                if (!allCovered) {
                                    fallbackRecord.of(tableReader, partitionIndex);
                                    fallbackRecord.setSymbolKey(rawSymbolKey);
                                    fallbackRecord.setRowId(lastRowId);
                                }
                                activeRecord = coveringRecord;
                                return true;
                            }
                            currentRowCursor = Misc.free(currentRowCursor);
                        }
                    } finally {
                        Misc.free(rowCursor);
                    }
                }
            }
        }

        abstract boolean hasNextLatestBy();

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentRowCursor = Misc.free(this.currentRowCursor);
            resetIterationState();
            this.activeRecord = coveringRecord;
            this.coveringRecord.of(null);
            SymbolTable indexSymbolTable = tableReader.getSymbolMapReader(indexColumnIndex);
            this.coveringRecord.setSymbolTable(indexSymbolTable);
            this.fallbackRecord.setSymbolTable(indexSymbolTable);
            if (symbolIncludeCols != null) {
                for (int col : symbolIncludeCols) {
                    symTablesCache[col] = tableReader.getSymbolMapReader(columnIndexes.getQuick(col));
                }
                coveringRecord.setIncludeSymbolTables(symTablesCache);
                fallbackRecord.setIncludeSymbolTables(symTablesCache);
            }
        }

        void ofEmpty() {
            this.frameCursor = null;
            this.tableReader = null;
            this.currentRowCursor = Misc.free(this.currentRowCursor);
            this.activeRecord = coveringRecord;
            this.coveringRecord.of(null);
        }

        abstract void resetIterationState();

        boolean tryOpenKey(int partitionIndex, int rawSymbolKey, long rowLo, long rowHi) {
            IndexReader indexReader = tableReader.getIndexReader(
                    partitionIndex,
                    indexColumnIndex,
                    IndexReader.DIR_FORWARD
            );
            RowCursor rowCursor = indexReader.getCursor(
                    TableUtils.toIndexKey(rawSymbolKey),
                    rowLo,
                    rowHi - 1,
                    requiredIncludeIndices
            );
            try {
                if (rowCursor instanceof CoveringRowCursor crc) {
                    Misc.free(currentRowCursor);
                    currentRowCursor = crc;
                    rowCursor = null;
                    coveringRecord.of(crc);
                    coveringRecord.setSymbolKey(rawSymbolKey);
                    allCovered = allRequiredCovered(crc);
                    coveringRecord.setAllCovered(allCovered);
                    if (!allCovered) {
                        fallbackRecord.of(tableReader, partitionIndex);
                        fallbackRecord.setSymbolKey(rawSymbolKey);
                    }
                    activeRecord = coveringRecord;
                    return true;
                }
                return false;
            } finally {
                Misc.free(rowCursor);
            }
        }
    }

    private static class CoveringPageFrame implements PageFrame {
        private final long[] auxPageAddresses;
        private final long[] auxPageSizes;
        private final int columnCount;
        private final long[] pageAddresses;
        private final long[] pageSizes;
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;

        CoveringPageFrame(int columnCount) {
            this.columnCount = columnCount;
            this.pageAddresses = new long[columnCount];
            this.pageSizes = new long[columnCount];
            this.auxPageAddresses = new long[columnCount];
            this.auxPageSizes = new long[columnCount];
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxPageAddresses[columnIndex];
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return auxPageSizes[columnIndex];
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses[columnIndex];
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes[columnIndex];
        }

        @Override
        public PartitionDecoder getParquetPartitionDecoder() {
            return null;
        }

        @Override
        public int getParquetRowGroup() {
            return 0;
        }

        @Override
        public int getParquetRowGroupHi() {
            return 0;
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
        }

        @Override
        public long getPartitionHi() {
            return partitionHi;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }
    }

    private static abstract class CoveringPageFrameCursor implements PageFrameCursor {
        private static final int INITIAL_CAPACITY = 4096;
        // Tracks all native allocations as (addr, size) pairs for bulk cleanup.
        // Each fillFrameForKey() call allocates fresh buffers so that
        // PageFrameAddressCache can hold addresses from multiple frames
        // simultaneously (vectorized GROUP BY collects all frames before processing).
        protected final LongList allocatedBuffers = new LongList();
        protected final DirectBinarySequence columnBin = new DirectBinarySequence();
        protected final IntList columnIndexes;
        protected final ColumnMapping columnMapping = new ColumnMapping();
        protected final int[] columnSizeBytes;
        protected final io.questdb.std.str.DirectString columnStrA = new io.questdb.std.str.DirectString();
        protected final int[] columnTypeTags;
        protected final CoveringPageFrame frame;
        // Reusable per-frame arrays (avoid per-frame heap allocation)
        protected final long[] frameAddrs;
        protected final long[] frameVarDataAddrs;
        protected final int[] frameVarDataCap;
        protected final int[] frameVarDataPos;
        protected final int indexColumnIndex;
        protected final int queryColCount;
        protected final int[] queryColToIncludeIdx;
        protected final int[] requiredIncludeIndices;
        protected PartitionFrameCursor frameCursor;
        protected boolean isExhausted;
        protected TableReader tableReader;

        CoveringPageFrameCursor(
                int indexColumnIndex,
                int[] queryColToIncludeIdx,
                int[] requiredIncludeIndices,
                RecordMetadata metadata,
                IntList columnIndexes
        ) {
            this.indexColumnIndex = indexColumnIndex;
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.requiredIncludeIndices = requiredIncludeIndices;
            this.queryColCount = queryColToIncludeIdx.length;
            this.columnIndexes = columnIndexes;
            this.frame = new CoveringPageFrame(queryColCount);
            this.columnSizeBytes = new int[queryColCount];
            this.columnTypeTags = new int[queryColCount];
            this.frameAddrs = new long[queryColCount + 1];
            this.frameVarDataAddrs = new long[queryColCount];
            this.frameVarDataPos = new int[queryColCount];
            this.frameVarDataCap = new int[queryColCount];
            for (int q = 0; q < queryColCount; q++) {
                int colType = metadata.getColumnType(q);
                this.columnTypeTags[q] = ColumnType.tagOf(colType);
                if (queryColToIncludeIdx[q] >= 0) {
                    this.columnSizeBytes[q] = ColumnType.sizeOf(colType);
                } else if (queryColToIncludeIdx[q] == -1) {
                    this.columnSizeBytes[q] = Integer.BYTES; // symbol key int
                }
            }
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            // not supported
        }

        @Override
        public void close() {
            frameCursor = Misc.free(frameCursor);
            freeBuffers();
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            if (tableReader != null) {
                return tableReader.getSymbolMapReader(columnIndexes.getQuick(columnIndex));
            }
            return null;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (tableReader != null) {
                return tableReader.newSymbolTable(columnIndexes.getQuick(columnIndex));
            }
            return null;
        }

        @Override
        public final @Nullable PageFrame next(long skipTarget) {
            if (frameCursor == null || isExhausted) {
                return null;
            }
            return nextImpl();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return false;
        }

        @Override
        public void toTop() {
            if (frameCursor != null) {
                frameCursor.toTop();
            }
            isExhausted = false;
            resetIterationState();
            freeBuffers();
        }

        private long allocBuffer(long bytes) {
            long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_INDEX_READER);
            allocatedBuffers.add(addr, bytes);
            return addr;
        }

        private void ensureVarDataCapacity(long[] varDataAddrs, int[] varDataPos, int[] varDataCap, int q, int needed) {
            if (varDataPos[q] + needed > varDataCap[q]) {
                int newCap = Math.max(varDataCap[q] * 2, varDataPos[q] + needed);
                long newAddr = allocBuffer(newCap);
                Unsafe.copyMemory(varDataAddrs[q], newAddr, varDataPos[q]);
                varDataAddrs[q] = newAddr;
                varDataCap[q] = newCap;
            }
        }

        private void freeBuffers() {
            for (int i = 0, n = allocatedBuffers.size(); i < n; i += 2) {
                Unsafe.free(allocatedBuffers.getQuick(i), allocatedBuffers.getQuick(i + 1), MemoryTag.NATIVE_INDEX_READER);
            }
            allocatedBuffers.clear();
        }

        /**
         * Grow all column and symbol buffers. addrs[0..queryColCount-1] are column
         * buffers; addrs[queryColCount] is the symbol buffer. Returns new capacity.
         */
        private int growFrameBuffers(long[] addrs, int count, int capacity) {
            int newCapacity = capacity * 2;
            for (int q = 0; q < queryColCount; q++) {
                if (queryColToIncludeIdx[q] >= 0) {
                    if (columnTypeTags[q] == ColumnType.VARCHAR) {
                        long newAuxBytes = (long) newCapacity * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                        long newAuxAddr = allocBuffer(newAuxBytes);
                        Unsafe.copyMemory(addrs[q], newAuxAddr, (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                        addrs[q] = newAuxAddr;
                    } else if (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY) {
                        long newAuxBytes = (long) (newCapacity + 1) * Long.BYTES;
                        long newAuxAddr = allocBuffer(newAuxBytes);
                        Unsafe.copyMemory(addrs[q], newAuxAddr, (long) count * Long.BYTES);
                        addrs[q] = newAuxAddr;
                    } else {
                        long newBytes = (long) newCapacity * columnSizeBytes[q];
                        long newAddr = allocBuffer(newBytes);
                        Unsafe.copyMemory(addrs[q], newAddr, (long) count * columnSizeBytes[q]);
                        addrs[q] = newAddr;
                    }
                }
            }
            addrs[queryColCount] = allocBuffer((long) newCapacity * Integer.BYTES);
            return newCapacity;
        }

        private void writeBinaryToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                        int q, int count, @Nullable BinarySequence value) {
            // BINARY aux: 8-byte offset per row into data vector
            long auxEntry = auxAddr + (long) count * Long.BYTES;
            long dataOffset = varDataPos[q];
            Unsafe.putLong(auxEntry, dataOffset);

            if (value == null) {
                // Write negative length as NULL marker
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, Long.BYTES);
                Unsafe.putLong(varDataAddrs[q] + varDataPos[q], TableUtils.NULL_LEN);
                varDataPos[q] += Long.BYTES;
            } else {
                long len = value.length();
                int totalBytes = (int) (Long.BYTES + len);
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, totalBytes);
                long dst = varDataAddrs[q] + varDataPos[q];
                Unsafe.putLong(dst, len);
                value.copyTo(dst + Long.BYTES, 0, len);
                varDataPos[q] += totalBytes;
            }
        }

        private void writeColumnRow(long[] addrs, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                    int count, long rowId, int colBase) {
            for (int q = 0; q < queryColCount; q++) {
                if (queryColToIncludeIdx[q] < 0) continue;
                long addr = addrs[q];
                int readerCol = columnIndexes.getQuick(q);
                // Columns added via ALTER TABLE ADD COLUMN have columnTop > 0
                // in partitions that already had rows. The column file is
                // indexed from 0 at the first row that exists (rowId == top);
                // rows below top must yield the type-specific NULL.
                long top = tableReader.getColumnTop(colBase, readerCol);
                long r = rowId - top;
                boolean isNull = r < 0;
                MemoryCR mem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                switch (columnTypeTags[q]) {
                    case ColumnType.DOUBLE -> Unsafe.putDouble(
                            addr + (long) count * Double.BYTES, isNull ? Double.NaN : mem.getDouble(r * Double.BYTES));
                    case ColumnType.FLOAT -> Unsafe.putFloat(
                            addr + (long) count * Float.BYTES, isNull ? Float.NaN : mem.getFloat(r * Float.BYTES));
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL64 ->
                            Unsafe.putLong(addr + (long) count * Long.BYTES,
                                    isNull ? Long.MIN_VALUE : mem.getLong(r * Long.BYTES));
                    case ColumnType.GEOLONG -> Unsafe.putLong(addr + (long) count * Long.BYTES,
                            isNull ? GeoHashes.NULL : mem.getLong(r * Long.BYTES));
                    case ColumnType.INT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                            Unsafe.putInt(addr + (long) count * Integer.BYTES,
                                    isNull ? Integer.MIN_VALUE : mem.getInt(r * Integer.BYTES));
                    case ColumnType.GEOINT -> Unsafe.putInt(addr + (long) count * Integer.BYTES,
                            isNull ? GeoHashes.INT_NULL : mem.getInt(r * Integer.BYTES));
                    case ColumnType.IPv4 -> Unsafe.putInt(addr + (long) count * Integer.BYTES,
                            isNull ? Numbers.IPv4_NULL : mem.getInt(r * Integer.BYTES));
                    case ColumnType.SHORT, ColumnType.CHAR, ColumnType.DECIMAL16 ->
                            Unsafe.putShort(addr + (long) count * Short.BYTES,
                                    isNull ? (short) 0 : mem.getShort(r * Short.BYTES));
                    case ColumnType.GEOSHORT -> Unsafe.putShort(addr + (long) count * Short.BYTES,
                            isNull ? GeoHashes.SHORT_NULL : mem.getShort(r * Short.BYTES));
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.DECIMAL8 ->
                            Unsafe.putByte(addr + count, isNull ? (byte) 0 : mem.getByte(r));
                    case ColumnType.GEOBYTE -> Unsafe.putByte(addr + count,
                            isNull ? GeoHashes.BYTE_NULL : mem.getByte(r));
                    case ColumnType.UUID, ColumnType.DECIMAL128 -> {
                        long off128 = (long) count * 16;
                        if (isNull) {
                            Unsafe.putLong(addr + off128, Long.MIN_VALUE);
                            Unsafe.putLong(addr + off128 + 8, Long.MIN_VALUE);
                        } else {
                            Unsafe.putLong(addr + off128, mem.getLong(r * 16));
                            Unsafe.putLong(addr + off128 + 8, mem.getLong(r * 16 + 8));
                        }
                    }
                    case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                        long off256 = (long) count * 32;
                        if (isNull) {
                            Unsafe.putLong(addr + off256, Long.MIN_VALUE);
                            Unsafe.putLong(addr + off256 + 8, Long.MIN_VALUE);
                            Unsafe.putLong(addr + off256 + 16, Long.MIN_VALUE);
                            Unsafe.putLong(addr + off256 + 24, Long.MIN_VALUE);
                        } else {
                            Unsafe.putLong(addr + off256, mem.getLong(r * 32));
                            Unsafe.putLong(addr + off256 + 8, mem.getLong(r * 32 + 8));
                            Unsafe.putLong(addr + off256 + 16, mem.getLong(r * 32 + 16));
                            Unsafe.putLong(addr + off256 + 24, mem.getLong(r * 32 + 24));
                        }
                    }
                    case ColumnType.VARCHAR -> {
                        Utf8Sequence value;
                        if (isNull) {
                            value = null;
                        } else {
                            int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                            MemoryCR dataMem = tableReader.getColumn(primaryIdx);
                            MemoryCR auxMem = tableReader.getColumn(primaryIdx + 1);
                            value = VarcharTypeDriver.getSplitValue(auxMem, dataMem, r, 0);
                        }
                        writeVarcharToFrame(addr, varDataAddrs, varDataPos, varDataCap, q, count, value);
                    }
                    case ColumnType.STRING -> {
                        CharSequence strValue;
                        if (isNull) {
                            strValue = null;
                        } else {
                            int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                            MemoryCR strDataMem = tableReader.getColumn(primaryIdx);
                            MemoryCR strAuxMem = tableReader.getColumn(primaryIdx + 1);
                            long dataOffset = strAuxMem.getLong(r * Long.BYTES);
                            int len = strDataMem.getInt(dataOffset);
                            strValue = (len == TableUtils.NULL_LEN)
                                    ? null : columnStrA.of(strDataMem.addressOf(dataOffset + Integer.BYTES), len);
                        }
                        writeStringToFrame(addr, varDataAddrs, varDataPos, varDataCap, q, count, strValue);
                    }
                    case ColumnType.BINARY -> {
                        BinarySequence binValue;
                        if (isNull) {
                            binValue = null;
                        } else {
                            int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                            MemoryCR binDataMem = tableReader.getColumn(primaryIdx);
                            MemoryCR binAuxMem = tableReader.getColumn(primaryIdx + 1);
                            long dataOffset = binAuxMem.getLong(r * Long.BYTES);
                            long binLen = binDataMem.getLong(dataOffset);
                            binValue = (binLen < 0)
                                    ? null : columnBin.of(binDataMem.addressOf(dataOffset + Long.BYTES), binLen);
                        }
                        writeBinaryToFrame(addr, varDataAddrs, varDataPos, varDataCap, q, count, binValue);
                    }
                    default -> {
                    }
                }
            }
        }

        private void writeCoveredRow(long[] addrs, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                     int count, CoveringRowCursor crc) {
            for (int q = 0; q < queryColCount; q++) {
                int includeIdx = queryColToIncludeIdx[q];
                if (includeIdx < 0) continue;
                long addr = addrs[q];
                switch (columnTypeTags[q]) {
                    case ColumnType.DOUBLE -> Unsafe.putDouble(
                            addr + (long) count * Double.BYTES, crc.getCoveredDouble(includeIdx));
                    case ColumnType.FLOAT -> Unsafe.putFloat(
                            addr + (long) count * Float.BYTES, crc.getCoveredFloat(includeIdx));
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG ->
                            Unsafe.putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL ->
                            Unsafe.putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
                    case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT ->
                            Unsafe.putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE ->
                            Unsafe.putByte(addr + count, crc.getCoveredByte(includeIdx));
                    case ColumnType.DECIMAL64 ->
                            Unsafe.putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
                    case ColumnType.DECIMAL32 ->
                            Unsafe.putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
                    case ColumnType.DECIMAL16 ->
                            Unsafe.putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
                    case ColumnType.DECIMAL8 -> Unsafe.putByte(addr + count, crc.getCoveredByte(includeIdx));
                    case ColumnType.UUID, ColumnType.DECIMAL128 -> {
                        long off128 = (long) count * 16;
                        Unsafe.putLong(addr + off128, crc.getCoveredLong128Lo(includeIdx));
                        Unsafe.putLong(addr + off128 + 8, crc.getCoveredLong128Hi(includeIdx));
                    }
                    case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                        long off256 = (long) count * 32;
                        Unsafe.putLong(addr + off256, crc.getCoveredLong256_0(includeIdx));
                        Unsafe.putLong(addr + off256 + 8, crc.getCoveredLong256_1(includeIdx));
                        Unsafe.putLong(addr + off256 + 16, crc.getCoveredLong256_2(includeIdx));
                        Unsafe.putLong(addr + off256 + 24, crc.getCoveredLong256_3(includeIdx));
                    }
                    case ColumnType.VARCHAR ->
                            writeVarcharToFrame(addrs[q], varDataAddrs, varDataPos, varDataCap, q, count, crc.getCoveredVarcharA(includeIdx));
                    case ColumnType.STRING ->
                            writeStringToFrame(addrs[q], varDataAddrs, varDataPos, varDataCap, q, count, crc.getCoveredStrA(includeIdx));
                    case ColumnType.BINARY ->
                            writeBinaryToFrame(addrs[q], varDataAddrs, varDataPos, varDataCap, q, count, crc.getCoveredBin(includeIdx));
                    default -> {
                    }
                }
            }
        }

        private void writeStringToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                        int q, int count, @Nullable CharSequence value) {
            // STRING aux: 8-byte offset per row into data vector
            long auxEntry = auxAddr + (long) count * Long.BYTES;
            long dataOffset = varDataPos[q];
            Unsafe.putLong(auxEntry, dataOffset);

            if (value == null) {
                // Write NULL_LEN (-1) as the length prefix
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, Integer.BYTES);
                Unsafe.putInt(varDataAddrs[q] + varDataPos[q], TableUtils.NULL_LEN);
                varDataPos[q] += Integer.BYTES;
            } else {
                int charCount = value.length();
                int totalBytes = Integer.BYTES + charCount * Character.BYTES;
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, totalBytes);
                long dst = varDataAddrs[q] + varDataPos[q];
                Unsafe.putInt(dst, charCount);
                for (int c = 0; c < charCount; c++) {
                    Unsafe.putChar(dst + Integer.BYTES + (long) c * Character.BYTES, value.charAt(c));
                }
                varDataPos[q] += totalBytes;
            }
        }

        private void writeVarcharToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                         int q, int count, @Nullable Utf8Sequence value) {
            long auxEntry = auxAddr + (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            long dataOffset = varDataPos[q];

            if (value == null) {
                Unsafe.putInt(auxEntry, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                Unsafe.putInt(auxEntry + 4, 0);
                Unsafe.putShort(auxEntry + 8, (short) 0);
                Unsafe.putShort(auxEntry + 10, (short) dataOffset);
                Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
            } else {
                int size = value.size();
                if (size <= VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    int header = (size << 4) | 1; // HEADER_FLAG_INLINED
                    if (value.isAscii()) header |= 2; // HEADER_FLAG_ASCII
                    Unsafe.putByte(auxEntry, (byte) header);
                    for (int b = 0; b < size; b++) {
                        Unsafe.putByte(auxEntry + 1 + b, value.byteAt(b));
                    }
                    for (int b = size; b < VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED; b++) {
                        Unsafe.putByte(auxEntry + 1 + b, (byte) 0);
                    }
                    Unsafe.putShort(auxEntry + 10, (short) dataOffset);
                    Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
                } else {
                    int header = (size << 4);
                    if (value.isAscii()) header |= 2;
                    Unsafe.putInt(auxEntry, header);
                    for (int b = 0; b < VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES; b++) {
                        Unsafe.putByte(auxEntry + 4 + b, value.byteAt(b));
                    }
                    ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, size);
                    // Use bulk copy when the Utf8Sequence has a stable native pointer
                    // (always true for DirectUtf8String from covering sidecar reads)
                    long srcPtr = value.ptr();
                    if (srcPtr != 0) {
                        Unsafe.copyMemory(srcPtr, varDataAddrs[q] + varDataPos[q], size);
                    } else for (int b = 0; b < size; b++) {
                        Unsafe.putByte(varDataAddrs[q] + varDataPos[q] + b, value.byteAt(b));
                    }
                    Unsafe.putShort(auxEntry + 10, (short) dataOffset);
                    Unsafe.putInt(auxEntry + 12, (int) (dataOffset >> 16));
                    varDataPos[q] += size;
                }
            }
        }

        protected static void fillSymbolKey(long addr, int rawSymbolKey, int count) {
            long longKey = Integer.toUnsignedLong(rawSymbolKey) | ((long) rawSymbolKey << 32);
            int i = 0;
            int pairs = count & ~1; // round down to even
            for (; i < pairs; i += 2) {
                Unsafe.putLong(addr + (long) i * Integer.BYTES, longKey);
            }
            if (i < count) {
                Unsafe.putInt(addr + (long) i * Integer.BYTES, rawSymbolKey);
            }
        }

        protected @Nullable PageFrame fillFrameForKey(int rawSymbolKey, int partitionIndex, long rowLo, long rowHi) {
            IndexReader indexReader = tableReader.getIndexReader(
                    partitionIndex,
                    indexColumnIndex,
                    IndexReader.DIR_FORWARD
            );
            final long[] addrs = frameAddrs;
            final long[] varDataAddrs = frameVarDataAddrs;
            final int[] varDataPos = frameVarDataPos;
            final int[] varDataCap = frameVarDataCap;
            int count;
            try (RowCursor rowCursor = indexReader.getCursor(
                    TableUtils.toIndexKey(rawSymbolKey),
                    rowLo,
                    rowHi - 1,
                    requiredIncludeIndices
            )) {
                CoveringRowCursor coveringCursor = null;
                if (rowCursor instanceof CoveringRowCursor crc) {
                    boolean allAvailable = true;
                    for (int requiredIncludeIndex : requiredIncludeIndices) {
                        if (!crc.isCoveredAvailable(requiredIncludeIndex)) {
                            allAvailable = false;
                            break;
                        }
                    }
                    if (allAvailable) {
                        coveringCursor = crc;
                    }
                }

                int capacity = INITIAL_CAPACITY;
                Arrays.fill(varDataAddrs, 0);
                Arrays.fill(varDataPos, 0);
                Arrays.fill(varDataCap, 0);
                for (int q = 0; q < queryColCount; q++) {
                    if (queryColToIncludeIdx[q] >= 0) {
                        if (columnTypeTags[q] == ColumnType.VARCHAR) {
                            addrs[q] = allocBuffer((long) capacity * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                            int initDataCap = capacity * 32;
                            varDataAddrs[q] = allocBuffer(initDataCap);
                            varDataCap[q] = initDataCap;
                        } else if (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY) {
                            // STRING/BINARY aux: 8 bytes per row (offset), plus sentinel at end
                            addrs[q] = allocBuffer((long) (capacity + 1) * Long.BYTES);
                            int initDataCap = capacity * 32;
                            varDataAddrs[q] = allocBuffer(initDataCap);
                            varDataCap[q] = initDataCap;
                        } else {
                            addrs[q] = allocBuffer((long) capacity * columnSizeBytes[q]);
                        }
                    }
                }
                addrs[queryColCount] = allocBuffer((long) capacity * Integer.BYTES);

                count = 0;
                if (coveringCursor != null) {
                    while (coveringCursor.hasNext()) {
                        coveringCursor.next();
                        if (count >= capacity) {
                            capacity = growFrameBuffers(addrs, count, capacity);
                        }
                        writeCoveredRow(addrs, varDataAddrs, varDataPos, varDataCap, count, coveringCursor);
                        count++;
                    }
                } else {
                    int colBase = tableReader.getColumnBase(partitionIndex);
                    while (rowCursor.hasNext()) {
                        long rowId = rowCursor.next();
                        if (count >= capacity) {
                            capacity = growFrameBuffers(addrs, count, capacity);
                        }
                        writeColumnRow(addrs, varDataAddrs, varDataPos, varDataCap, count, rowId, colBase);
                        count++;
                    }
                }
            }
            if (count == 0) {
                return null;
            }

            long symAddr = addrs[queryColCount];
            fillSymbolKey(symAddr, rawSymbolKey, count);

            for (int q = 0; q < queryColCount; q++) {
                int includeIdx = queryColToIncludeIdx[q];
                if (includeIdx >= 0 && columnTypeTags[q] == ColumnType.VARCHAR) {
                    frame.auxPageAddresses[q] = addrs[q];
                    frame.auxPageSizes[q] = (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                    frame.pageAddresses[q] = varDataAddrs[q];
                    frame.pageSizes[q] = varDataPos[q];
                } else if (includeIdx >= 0 && (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY)) {
                    // Write sentinel offset at [count] position
                    Unsafe.putLong(addrs[q] + (long) count * Long.BYTES, varDataPos[q]);
                    frame.auxPageAddresses[q] = addrs[q];
                    frame.auxPageSizes[q] = (long) (count + 1) * Long.BYTES;
                    frame.pageAddresses[q] = varDataAddrs[q];
                    frame.pageSizes[q] = varDataPos[q];
                } else if (includeIdx >= 0) {
                    frame.pageAddresses[q] = addrs[q];
                    frame.pageSizes[q] = (long) count * columnSizeBytes[q];
                    frame.auxPageAddresses[q] = 0;
                    frame.auxPageSizes[q] = 0;
                } else if (includeIdx == -1) {
                    frame.pageAddresses[q] = symAddr;
                    frame.pageSizes[q] = (long) count * Integer.BYTES;
                    frame.auxPageAddresses[q] = 0;
                    frame.auxPageSizes[q] = 0;
                }
            }
            frame.partitionLo = 0;
            frame.partitionHi = count;
            frame.partitionIndex = partitionIndex;
            return frame;
        }

        abstract @Nullable PageFrame nextImpl();

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.isExhausted = false;
            resetIterationState();
            columnMapping.clear();
            for (int i = 0, n = columnIndexes.size(); i < n; i++) {
                columnMapping.addColumn(columnIndexes.getQuick(i), columnIndexes.getQuick(i));
            }
            freeBuffers();
        }

        void ofEmpty() {
            this.frameCursor = null;
            this.tableReader = null;
            this.isExhausted = true;
            resetIterationState();
        }

        abstract void resetIterationState();
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
        private final FallbackRecord fallbackRecord;
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final RecordMetadata metadata;
        private final int[] queryColToIncludeIdx;
        private boolean allCovered;
        private CoveringRowCursor cursor;
        private SymbolTable[] includeSymbolTables;
        private long rowId;
        private int symbolKey;
        private SymbolTable symbolTable;

        CoveringRecord(int[] queryColToIncludeIdx, int symbolKey, RecordMetadata metadata, FallbackRecord fallbackRecord) {
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.symbolKey = symbolKey;
            this.metadata = metadata;
            this.fallbackRecord = fallbackRecord;
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredArray(includeIdx, columnType);
                }
                return fallbackRecord.getArray(col, columnType);
            }
            return null;
        }

        @Override
        public BinarySequence getBin(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredBin(includeIdx);
                }
                return fallbackRecord.getBin(col);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredBinLen(includeIdx);
                }
                return fallbackRecord.getBinLen(col);
            }
            return -1;
        }

        @Override
        public boolean getBool(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredByte(includeIdx) != 0;
                }
                return fallbackRecord.getBool(col);
            }
            return false;
        }

        @Override
        public byte getByte(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredByte(includeIdx);
                }
                return fallbackRecord.getByte(col);
            }
            return 0;
        }

        @Override
        public char getChar(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return (char) cursor.getCoveredShort(includeIdx);
                }
                return fallbackRecord.getChar(col);
            }
            return 0;
        }

        @Override
        public long getDate(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredLong(includeIdx);
                }
                return fallbackRecord.getDate(col);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    long high = cursor.getCoveredLong128Lo(includeIdx);
                    long low = cursor.getCoveredLong128Hi(includeIdx);
                    int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                    sink.of(high, low, scale);
                } else {
                    fallbackRecord.getDecimal128(col, sink);
                }
            } else {
                sink.of(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0);
            }
        }

        @Override
        public short getDecimal16(int col) {
            return getShort(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                    sink.of(
                            cursor.getCoveredLong256_0(includeIdx),
                            cursor.getCoveredLong256_1(includeIdx),
                            cursor.getCoveredLong256_2(includeIdx),
                            cursor.getCoveredLong256_3(includeIdx),
                            scale
                    );
                } else {
                    fallbackRecord.getDecimal256(col, sink);
                }
            } else {
                sink.of(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0);
            }
        }

        @Override
        public int getDecimal32(int col) {
            return getInt(col);
        }

        @Override
        public long getDecimal64(int col) {
            return getLong(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return getByte(col);
        }

        @Override
        public double getDouble(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredDouble(includeIdx);
                }
                return fallbackRecord.getDouble(col);
            }
            return Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredFloat(includeIdx);
                }
                return fallbackRecord.getFloat(col);
            }
            return Float.NaN;
        }

        @Override
        public int getIPv4(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredInt(includeIdx);
                }
                return fallbackRecord.getIPv4(col);
            }
            return Numbers.IPv4_NULL;
        }

        @Override
        public int getInt(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredInt(includeIdx);
                }
                return fallbackRecord.getInt(col);
            }
            return Integer.MIN_VALUE;
        }

        @Override
        public long getLong(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredLong(includeIdx);
                }
                return fallbackRecord.getLong(col);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Hi(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredLong128Hi(includeIdx);
                }
                return fallbackRecord.getLong128Hi(col);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Lo(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredLong128Lo(includeIdx);
                }
                return fallbackRecord.getLong128Lo(col);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Long256 val = getLong256A(col);
            Numbers.appendLong256(val.getLong0(), val.getLong1(), val.getLong2(), val.getLong3(), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    long256A.setAll(
                            cursor.getCoveredLong256_0(includeIdx),
                            cursor.getCoveredLong256_1(includeIdx),
                            cursor.getCoveredLong256_2(includeIdx),
                            cursor.getCoveredLong256_3(includeIdx)
                    );
                    return long256A;
                }
                return fallbackRecord.getLong256A(col);
            }
            long256A.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
            return long256A;
        }

        @Override
        public Long256 getLong256B(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    long256B.setAll(
                            cursor.getCoveredLong256_0(includeIdx),
                            cursor.getCoveredLong256_1(includeIdx),
                            cursor.getCoveredLong256_2(includeIdx),
                            cursor.getCoveredLong256_3(includeIdx)
                    );
                    return long256B;
                }
                return fallbackRecord.getLong256B(col);
            }
            long256B.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
            return long256B;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public short getShort(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredShort(includeIdx);
                }
                return fallbackRecord.getShort(col);
            }
            return 0;
        }

        @Override
        public CharSequence getStrA(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredStrA(includeIdx);
                }
                return fallbackRecord.getStrA(col);
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredStrB(includeIdx);
                }
                return fallbackRecord.getStrB(col);
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
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    SymbolTable st = includeSymbolTables[col];
                    if (st != null) {
                        return st.valueOf(cursor.getCoveredInt(includeIdx));
                    }
                } else {
                    return fallbackRecord.getSymA(col);
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
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    SymbolTable st = includeSymbolTables[col];
                    if (st != null) {
                        return st.valueBOf(cursor.getCoveredInt(includeIdx));
                    }
                } else {
                    return fallbackRecord.getSymB(col);
                }
            }
            return null;
        }

        @Override
        public long getTimestamp(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredLong(includeIdx);
                }
                return fallbackRecord.getTimestamp(col);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredVarcharA(includeIdx);
                }
                return fallbackRecord.getVarcharA(col);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (allCovered || cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredVarcharB(includeIdx);
                }
                return fallbackRecord.getVarcharB(col);
            }
            return null;
        }

        public void setAllCovered(boolean allCovered) {
            this.allCovered = allCovered;
        }

        private int getIncludeIdx(int col) {
            if (col < 0 || col >= queryColToIncludeIdx.length) {
                return -2;
            }
            return queryColToIncludeIdx[col];
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
    }

    /**
     * Fallback record that reads covered column values from column files via
     * TableReader when sidecar data is unavailable (unsealed partitions).
     * Zero per-row branching — the cursor swaps between CoveringRecord and
     * FallbackRecord at partition boundaries.
     */
    private static class FallbackRecord implements Record {
        private final IntList columnIndexes;
        private final BorrowedArray fallbackArray = new BorrowedArray();
        private final DirectBinarySequence fallbackBin = new DirectBinarySequence();
        private final io.questdb.std.str.DirectString fallbackStrA = new io.questdb.std.str.DirectString();
        private final io.questdb.std.str.DirectString fallbackStrB = new io.questdb.std.str.DirectString();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final RecordMetadata metadata;
        private final int[] queryColToIncludeIdx;
        private SymbolTable[] includeSymbolTables;
        private int partitionIndex;
        private long rowId;
        private int symbolKey;
        private SymbolTable symbolTable;
        private TableReader tableReader;

        FallbackRecord(int[] queryColToIncludeIdx, int symbolKey, IntList columnIndexes, RecordMetadata metadata) {
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.symbolKey = symbolKey;
            this.columnIndexes = columnIndexes;
            this.metadata = metadata;
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return fallbackArray.of(columnType, auxMem.addressOf(0), auxMem.addressOf(0) + auxMem.size(),
                        dataMem.addressOf(0), dataMem.addressOf(0) + dataMem.size(), r);
            }
            return null;
        }

        @Override
        public BinarySequence getBin(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(r * Long.BYTES);
                long len = dataMem.getLong(dataOffset);
                if (len < 0) return null;
                return fallbackBin.of(dataMem.addressOf(dataOffset + Long.BYTES), len);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return -1;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(r * Long.BYTES);
                return dataMem.getLong(dataOffset);
            }
            return -1;
        }

        @Override
        public boolean getBool(int col) {
            return getByte(col) != 0;
        }

        @Override
        public byte getByte(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return 0;
                return columnMem(col).getByte(r);
            }
            return 0;
        }

        @Override
        public char getChar(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return 0;
                return columnMem(col).getChar(r * Character.BYTES);
            }
            return 0;
        }

        @Override
        public long getDate(int col) {
            return getLong(col);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) {
                    sink.of(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0);
                    return;
                }
                MemoryCR mem = columnMem(col);
                int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                // DECIMAL128 stores high at offset 0, low at offset 8
                sink.of(mem.getLong(r * 16), mem.getLong(r * 16 + Long.BYTES), scale);
            } else {
                sink.of(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0);
            }
        }

        @Override
        public short getDecimal16(int col) {
            return getShort(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) {
                    sink.of(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0);
                    return;
                }
                MemoryCR mem = columnMem(col);
                long off = r * 32;
                int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                sink.of(mem.getLong(off), mem.getLong(off + 8), mem.getLong(off + 16), mem.getLong(off + 24), scale);
            } else {
                sink.of(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0);
            }
        }

        @Override
        public int getDecimal32(int col) {
            return getInt(col);
        }

        @Override
        public long getDecimal64(int col) {
            return getLong(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return getByte(col);
        }

        @Override
        public double getDouble(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Double.NaN;
                return columnMem(col).getDouble(r * Double.BYTES);
            }
            return Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Float.NaN;
                return columnMem(col).getFloat(r * Float.BYTES);
            }
            return Float.NaN;
        }

        @Override
        public int getIPv4(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Numbers.IPv4_NULL;
                return columnMem(col).getInt(r * Integer.BYTES);
            }
            return Numbers.IPv4_NULL;
        }

        @Override
        public int getInt(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Integer.MIN_VALUE;
                return columnMem(col).getInt(r * Integer.BYTES);
            }
            return Integer.MIN_VALUE;
        }

        @Override
        public long getLong(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Long.MIN_VALUE;
                return columnMem(col).getLong(r * Long.BYTES);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Hi(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Long.MIN_VALUE;
                return columnMem(col).getLong(r * 16 + Long.BYTES);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Lo(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return Long.MIN_VALUE;
                return columnMem(col).getLong(r * 16);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            Long256 val = getLong256A(col);
            Numbers.appendLong256(val.getLong0(), val.getLong1(), val.getLong2(), val.getLong3(), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) {
                    long256A.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                    return long256A;
                }
                MemoryCR mem = columnMem(col);
                long off = r * 32;
                long256A.setAll(
                        mem.getLong(off),
                        mem.getLong(off + Long.BYTES),
                        mem.getLong(off + 2 * Long.BYTES),
                        mem.getLong(off + 3 * Long.BYTES)
                );
            } else {
                long256A.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
            }
            return long256A;
        }

        @Override
        public Long256 getLong256B(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) {
                    long256B.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                    return long256B;
                }
                MemoryCR mem = columnMem(col);
                long off = r * 32;
                long256B.setAll(
                        mem.getLong(off),
                        mem.getLong(off + Long.BYTES),
                        mem.getLong(off + 2 * Long.BYTES),
                        mem.getLong(off + 3 * Long.BYTES)
                );
            } else {
                long256B.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
            }
            return long256B;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public short getShort(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return 0;
                return columnMem(col).getShort(r * Short.BYTES);
            }
            return 0;
        }

        @Override
        public CharSequence getStrA(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(r * Long.BYTES);
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
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(r * Long.BYTES);
                int len = dataMem.getInt(dataOffset);
                if (len == TableUtils.NULL_LEN) return null;
                fallbackStrB.of(dataMem.addressOf(dataOffset + Integer.BYTES), len);
                return fallbackStrB;
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

        @Override
        public Utf8Sequence getVarcharA(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return io.questdb.cairo.VarcharTypeDriver.getSplitValue(auxMem, dataMem, r, 0);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                long r = fileRowOf(col);
                if (r < 0) return null;
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return io.questdb.cairo.VarcharTypeDriver.getSplitValue(auxMem, dataMem, r, 1);
            }
            return null;
        }

        private MemoryCR columnMem(int col) {
            int readerCol = columnIndexes.getQuick(col);
            int colBase = tableReader.getColumnBase(partitionIndex);
            return tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
        }

        // Returns the file-relative row index for {@code col}, or -1 when the
        // logical row lives below the column's columnTop. Columns added after
        // a partition's first row have columnTop > 0, and their column file is
        // indexed from zero at the first row that actually exists; absolute
        // rowIds below columnTop have no data and must yield NULL.
        private long fileRowOf(int col) {
            int readerCol = columnIndexes.getQuick(col);
            int colBase = tableReader.getColumnBase(partitionIndex);
            long top = tableReader.getColumnTop(colBase, readerCol);
            return rowId < top ? -1 : rowId - top;
        }

        private int getIncludeIdx(int col) {
            if (col < 0 || col >= queryColToIncludeIdx.length) {
                return -2;
            }
            return queryColToIncludeIdx[col];
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
    }

    private static class MultiKeyCoveringCursor extends CoveringCursor {
        final IntList multiKeys;
        private int cachedPartitionIndex;
        private long cachedRowHi;
        private long cachedRowLo;
        private int currentKeyIdx;

        MultiKeyCoveringCursor(int indexColumnIndex, int multiKeyCapacity, int[] queryColToIncludeIdx,
                               int[] requiredIncludeIndices, int[] symbolIncludeCols, IntList columnIndexes,
                               boolean latestBy, RecordMetadata metadata) {
            super(indexColumnIndex, SymbolTable.VALUE_NOT_FOUND, queryColToIncludeIdx, requiredIncludeIndices, symbolIncludeCols, columnIndexes, latestBy, metadata);
            this.multiKeys = new IntList(multiKeyCapacity);
            this.cachedPartitionIndex = -1;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        boolean advanceKey() {
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

        @Override
        boolean hasNextLatestBy() {
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

        @Override
        void resetIterationState() {
            currentKeyIdx = 0;
            cachedPartitionIndex = -1;
        }
    }

    private static class MultiKeyCoveringPageFrameCursor extends CoveringPageFrameCursor {
        final IntList multiKeys = new IntList();
        private PartitionFrame cachedPartFrame;
        private int currentKeyIdx;

        MultiKeyCoveringPageFrameCursor(
                int indexColumnIndex,
                int[] queryColToIncludeIdx,
                int[] requiredIncludeIndices,
                RecordMetadata metadata,
                IntList columnIndexes
        ) {
            super(indexColumnIndex, queryColToIncludeIdx, requiredIncludeIndices, metadata, columnIndexes);
        }

        @Override
        @Nullable
        PageFrame nextImpl() {
            while (true) {
                while (currentKeyIdx < multiKeys.size()) {
                    if (cachedPartFrame != null) {
                        int rawKey = multiKeys.getQuick(currentKeyIdx);
                        currentKeyIdx++;
                        PageFrame result = fillFrameForKey(
                                rawKey,
                                cachedPartFrame.getPartitionIndex(),
                                cachedPartFrame.getRowLo(),
                                cachedPartFrame.getRowHi()
                        );
                        if (result != null) {
                            return result;
                        }
                    } else {
                        break;
                    }
                }
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                cachedPartFrame = partFrame;
                currentKeyIdx = 0;
            }
        }

        @Override
        void resetIterationState() {
            currentKeyIdx = 0;
            cachedPartFrame = null;
        }
    }

    private static class SingleKeyCoveringCursor extends CoveringCursor {
        int symbolKey;
        private boolean isLatestByDone;

        SingleKeyCoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                                int[] requiredIncludeIndices, int[] symbolIncludeCols, IntList columnIndexes,
                                boolean latestBy, RecordMetadata metadata) {
            super(indexColumnIndex, symbolKey, queryColToIncludeIdx, requiredIncludeIndices, symbolIncludeCols, columnIndexes, latestBy, metadata);
            this.symbolKey = symbolKey;
        }

        @Override
        public long size() {
            if (frameCursor == null || latestBy || symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return -1;
            }
            long total = 0;
            frameCursor.toTop();
            try {
                PartitionFrame frame;
                while ((frame = frameCursor.next()) != null) {
                    IndexReader reader = tableReader.getIndexReader(
                            frame.getPartitionIndex(), indexColumnIndex, IndexReader.DIR_FORWARD);
                    final long rowLo = frame.getRowLo();
                    final long rowHi = frame.getRowHi();
                    try (RowCursor rc = reader.getCursor(TableUtils.toIndexKey(symbolKey), rowLo, rowHi - 1)) {
                        if (rowLo == 0 && rowHi == tableReader.getPartitionRowCount(frame.getPartitionIndex())) {
                            long count = rc.size();
                            if (count >= 0) {
                                total += count;
                                continue;
                            }
                        }
                        while (rc.hasNext()) {
                            rc.next();
                            total++;
                        }
                    }
                }
            } finally {
                frameCursor.toTop();
            }
            return total;
        }

        @Override
        boolean advanceKey() {
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

        @Override
        boolean hasNextLatestBy() {
            if (isLatestByDone) {
                return false;
            }
            if (findLatestRow(symbolKey)) {
                isLatestByDone = true;
                return true;
            }
            return false;
        }

        @Override
        void resetIterationState() {
            isLatestByDone = false;
        }

        void resolveKey(int resolvedKey) {
            this.symbolKey = resolvedKey;
            this.coveringRecord.setSymbolKey(resolvedKey);
            this.fallbackRecord.setSymbolKey(resolvedKey);
        }
    }

    private static class SingleKeyCoveringPageFrameCursor extends CoveringPageFrameCursor {
        int resolvedKey;
        int symbolKey;

        SingleKeyCoveringPageFrameCursor(
                int indexColumnIndex,
                int symbolKey,
                int[] queryColToIncludeIdx,
                int[] requiredIncludeIndices,
                RecordMetadata metadata,
                IntList columnIndexes
        ) {
            super(indexColumnIndex, queryColToIncludeIdx, requiredIncludeIndices, metadata, columnIndexes);
            this.symbolKey = symbolKey;
            this.resolvedKey = symbolKey;
        }

        @Override
        @Nullable
        PageFrame nextImpl() {
            while (true) {
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                PageFrame result = fillFrameForKey(
                        resolvedKey,
                        partFrame.getPartitionIndex(),
                        partFrame.getRowLo(),
                        partFrame.getRowHi()
                );
                if (result != null) {
                    return result;
                }
            }
        }

        @Override
        void resetIterationState() {
        }
    }
}

