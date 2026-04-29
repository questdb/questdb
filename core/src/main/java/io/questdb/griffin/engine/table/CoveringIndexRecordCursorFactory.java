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
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
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
        protected final int indexColumnIndex;
        protected final boolean latestBy;
        protected final int[] requiredIncludeIndices;
        protected final SymbolTable[] symTablesCache;
        protected final int[] symbolIncludeCols;
        protected CoveringRowCursor currentRowCursor;
        protected PartitionFrameCursor frameCursor;
        protected Function latestByFilter;
        protected TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                       int[] requiredIncludeIndices, int[] symbolIncludeCols, IntList columnIndexes,
                       boolean latestBy, RecordMetadata metadata) {
            this.indexColumnIndex = indexColumnIndex;
            this.coveringRecord = new CoveringRecord(queryColToIncludeIdx, symbolKey, metadata);
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
            return coveringRecord;
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
                    coveringRecord.setRowId(currentRowCursor.next());
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
                        // Storage is transactional and writers seal sidecars
                        // before commit, so a partition that holds rows for
                        // this key always returns a CoveringRowCursor. The
                        // index reader returns EmptyRowCursor (which is not
                        // a CoveringRowCursor) when the key has no rows in
                        // this partition; in that case skip and advance.
                        if (bwdCursor instanceof CoveringRowCursor crc) {
                            Misc.free(currentRowCursor);
                            currentRowCursor = crc;
                            bwdCursor = null;
                            coveringRecord.of(crc);
                            coveringRecord.setSymbolKey(rawSymbolKey);
                            while (crc.hasNext()) {
                                coveringRecord.setRowId(crc.next());
                                if (latestByFilter.getBool(coveringRecord)) {
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
            this.coveringRecord.of(null);
            SymbolTable indexSymbolTable = tableReader.getSymbolMapReader(indexColumnIndex);
            this.coveringRecord.setSymbolTable(indexSymbolTable);
            if (symbolIncludeCols != null) {
                for (int col : symbolIncludeCols) {
                    symTablesCache[col] = tableReader.getSymbolMapReader(columnIndexes.getQuick(col));
                }
                coveringRecord.setIncludeSymbolTables(symTablesCache);
            }
        }

        void ofEmpty() {
            this.frameCursor = null;
            this.tableReader = null;
            this.currentRowCursor = Misc.free(this.currentRowCursor);
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
                // EmptyRowCursor (returned when the key has no rows in this
                // partition) is not a CoveringRowCursor; treat it as "no
                // rows, try the next partition" rather than a failure.
                if (rowCursor instanceof CoveringRowCursor crc) {
                    Misc.free(currentRowCursor);
                    currentRowCursor = crc;
                    rowCursor = null;
                    coveringRecord.of(crc);
                    coveringRecord.setSymbolKey(rawSymbolKey);
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
        protected final IntList columnIndexes;
        protected final ColumnMapping columnMapping = new ColumnMapping();
        protected final int[] columnSizeBytes;
        protected final int[] columnTypeTags;
        protected final int[] columnTypes;
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
            this.columnTypes = new int[queryColCount];
            this.frameAddrs = new long[queryColCount + 1];
            this.frameVarDataAddrs = new long[queryColCount];
            this.frameVarDataPos = new int[queryColCount];
            this.frameVarDataCap = new int[queryColCount];
            for (int q = 0; q < queryColCount; q++) {
                int colType = metadata.getColumnType(q);
                this.columnTypes[q] = colType;
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
                    } else if (columnTypeTags[q] == ColumnType.ARRAY) {
                        long newAuxBytes = (long) newCapacity * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                        long newAuxAddr = allocBuffer(newAuxBytes);
                        Unsafe.copyMemory(addrs[q], newAuxAddr, (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
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

        private void writeArrayToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                       int q, int count, @Nullable ArrayView value) {
            // ARRAY aux: 16 bytes per row [8-byte data offset][8-byte data size].
            // Layout matches ArrayTypeDriver.appendValue() so consumers reading
            // the page frame use the same decoding path as on-disk arrays.
            long auxEntry = auxAddr + (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
            long dataOffset = varDataPos[q];
            Unsafe.putLong(auxEntry, dataOffset);

            if (value == null || value.isNull()) {
                // NULL marker: size = 0
                Unsafe.putLong(auxEntry + Long.BYTES, 0L);
                return;
            }

            int nDims = value.getDimCount();
            short elemType = value.getElemType();
            int elemSize = ColumnType.sizeOf(elemType);
            long cardinality = value.getCardinality();
            int shapeBytes = nDims * Integer.BYTES;
            // ArrayTypeDriver pads the data section so element writes are aligned
            // to elemSize, then post-pads to Integer.BYTES for the next entry.
            int prePad = elemSize > 1
                    ? (int) ((-(dataOffset + shapeBytes)) & (elemSize - 1))
                    : 0;
            long dataBytes = cardinality * elemSize;
            int postPad = (int) ((-(dataOffset + shapeBytes + prePad + dataBytes)) & (Integer.BYTES - 1));
            int totalBytes = (int) (shapeBytes + prePad + dataBytes + postPad);

            Unsafe.putLong(auxEntry + Long.BYTES, totalBytes);
            ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, totalBytes);
            long dst = varDataAddrs[q] + dataOffset;

            for (int d = 0; d < nDims; d++) {
                Unsafe.putInt(dst, value.getDimLen(d));
                dst += Integer.BYTES;
            }
            if (prePad > 0) {
                Unsafe.setMemory(dst, prePad, (byte) 0);
                dst += prePad;
            }
            if (cardinality > 0 && value.isVanilla() && elemType == ColumnType.DOUBLE) {
                value.flatView().appendPlainDoubleValue(dst, value.getFlatViewOffset(), value.getFlatViewLength());
            } else if (dataBytes > 0) {
                // Fallback for non-vanilla or non-double element types: zero the
                // data section. Shape is preserved so consumers see a same-shaped
                // array. The covering page-frame path is currently only reached
                // for vanilla DOUBLE arrays in production planner output.
                Unsafe.setMemory(dst, dataBytes, (byte) 0);
            }
            dst += dataBytes;
            if (postPad > 0) {
                Unsafe.setMemory(dst, postPad, (byte) 0);
            }
            varDataPos[q] += totalBytes;
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
                    case ColumnType.ARRAY -> writeArrayToFrame(addrs[q], varDataAddrs, varDataPos, varDataCap, q, count,
                            crc.getCoveredArray(includeIdx, columnTypes[q]));
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
                // EmptyRowCursor (returned when the key has no rows in this
                // partition) is not a CoveringRowCursor; emit no frame.
                if (!(rowCursor instanceof CoveringRowCursor coveringCursor)) {
                    return null;
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
                        } else if (columnTypeTags[q] == ColumnType.ARRAY) {
                            // ARRAY aux: 16 bytes per row [offset][size]
                            addrs[q] = allocBuffer((long) capacity * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
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
                while (coveringCursor.hasNext()) {
                    coveringCursor.next();
                    if (count >= capacity) {
                        capacity = growFrameBuffers(addrs, count, capacity);
                    }
                    writeCoveredRow(addrs, varDataAddrs, varDataPos, varDataCap, count, coveringCursor);
                    count++;
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
                } else if (includeIdx >= 0 && columnTypeTags[q] == ColumnType.ARRAY) {
                    frame.auxPageAddresses[q] = addrs[q];
                    frame.auxPageSizes[q] = (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
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
     * <p>
     * Storage is transactional and writers seal sidecars before commit, so
     * readers never observe a covered include column without its sidecar.
     * The accessors therefore call {@link CoveringRowCursor} directly and
     * assume the row cursor and column data are present.
     */
    private static class CoveringRecord implements Record {
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final RecordMetadata metadata;
        private final int[] queryColToIncludeIdx;
        private CoveringRowCursor cursor;
        private SymbolTable[] includeSymbolTables;
        private long rowId;
        private int symbolKey;
        private SymbolTable symbolTable;

        CoveringRecord(int[] queryColToIncludeIdx, int symbolKey, RecordMetadata metadata) {
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.symbolKey = symbolKey;
            this.metadata = metadata;
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredArray(includeIdx, columnType);
            }
            return null;
        }

        @Override
        public BinarySequence getBin(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredBin(includeIdx);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredBinLen(includeIdx);
            }
            return -1;
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
        public char getChar(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return (char) cursor.getCoveredShort(includeIdx);
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
        public void getDecimal128(int col, Decimal128 sink) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                long high = cursor.getCoveredLong128Lo(includeIdx);
                long low = cursor.getCoveredLong128Hi(includeIdx);
                int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                sink.of(high, low, scale);
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
                int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                sink.of(
                        cursor.getCoveredLong256_0(includeIdx),
                        cursor.getCoveredLong256_1(includeIdx),
                        cursor.getCoveredLong256_2(includeIdx),
                        cursor.getCoveredLong256_3(includeIdx),
                        scale
                );
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
        public byte getGeoByte(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredByte(includeIdx);
            }
            return GeoHashes.BYTE_NULL;
        }

        @Override
        public int getGeoInt(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredInt(includeIdx);
            }
            return GeoHashes.INT_NULL;
        }

        @Override
        public long getGeoLong(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong(includeIdx);
            }
            return GeoHashes.NULL;
        }

        @Override
        public short getGeoShort(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredShort(includeIdx);
            }
            return GeoHashes.SHORT_NULL;
        }

        @Override
        public int getIPv4(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredInt(includeIdx);
            }
            return Numbers.IPv4_NULL;
        }

        @Override
        public int getInt(int col) {
            int includeIdx = getIncludeIdx(col);
            // queryColToIncludeIdx == -1 marks the indexed sym column. SYMBOL
            // columns return their key int via getInt(); this preserves the
            // contract relied on by testSymbolAPI's keyOf(getSymA)==getInt
            // assertion and by any consumer that reads the symbol key
            // directly.
            if (includeIdx == -1) {
                return symbolKey;
            }
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
        public long getLong128Hi(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong128Hi(includeIdx);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Lo(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                return cursor.getCoveredLong128Lo(includeIdx);
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
                long256A.setAll(
                        cursor.getCoveredLong256_0(includeIdx),
                        cursor.getCoveredLong256_1(includeIdx),
                        cursor.getCoveredLong256_2(includeIdx),
                        cursor.getCoveredLong256_3(includeIdx)
                );
            } else {
                long256A.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
            }
            return long256A;
        }

        @Override
        public Long256 getLong256B(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                long256B.setAll(
                        cursor.getCoveredLong256_0(includeIdx),
                        cursor.getCoveredLong256_1(includeIdx),
                        cursor.getCoveredLong256_2(includeIdx),
                        cursor.getCoveredLong256_3(includeIdx)
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
        public int getStrLen(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                CharSequence s = cursor.getCoveredStrA(includeIdx);
                return s == null ? TableUtils.NULL_LEN : s.length();
            }
            return TableUtils.NULL_LEN;
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
        public int getVarcharSize(int col) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                Utf8Sequence v = cursor.getCoveredVarcharA(includeIdx);
                return v == null ? TableUtils.NULL_LEN : v.size();
            }
            return TableUtils.NULL_LEN;
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

