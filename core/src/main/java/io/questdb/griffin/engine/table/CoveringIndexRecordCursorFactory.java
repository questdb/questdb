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

    private final CoveringCursor cursor;
    private final PartitionFrameCursorFactory dfcFactory;
    private final int indexColumnIndex;
    private final int keyQueryPosition;
    private final ObjList<Function> keyValueFuncs;
    private final boolean latestBy;
    private final Function latestByFilter;
    private final RecordMetadata metadata;
    private final CoveringPageFrameCursor pageFrameCursor;
    private final int[] queryColToIncludeIdx;
    private final IntList resolvedKeys;
    private final Function symbolFunction;

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            int @NotNull [] queryColToIncludeIdx
    ) {
        this(metadata, dfcFactory, indexColumnIndex, symbolKey, symbolFunction, columnIndexes, queryColToIncludeIdx, false, null);
    }

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            int @NotNull [] queryColToIncludeIdx,
            boolean latestBy,
            @Nullable Function latestByFilter
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;

        this.latestBy = latestBy;
        this.latestByFilter = latestByFilter;
        this.keyValueFuncs = null;
        this.resolvedKeys = null;

        this.queryColToIncludeIdx = queryColToIncludeIdx;

        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, 0, symInclCols, columnIndexes, latestBy, metadata);
        this.pageFrameCursor = !latestBy
                ? new CoveringPageFrameCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, metadata, columnIndexes)
                : null;
    }

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            int @NotNull [] queryColToIncludeIdx,
            ObjList<Function> keyValueFuncs,
            TableReader reader
    ) {
        this(metadata, dfcFactory, indexColumnIndex, symbolKey, symbolFunction, columnIndexes, queryColToIncludeIdx, keyValueFuncs, reader, false, null);
    }

    public CoveringIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory dfcFactory,
            int indexColumnIndex,
            int symbolKey,
            Function symbolFunction,
            @NotNull IntList columnIndexes,
            int @NotNull [] queryColToIncludeIdx,
            ObjList<Function> keyValueFuncs,
            TableReader reader,
            boolean latestBy,
            @Nullable Function latestByFilter
    ) {
        this.metadata = metadata;
        this.dfcFactory = dfcFactory;
        this.indexColumnIndex = indexColumnIndex;
        this.keyQueryPosition = findQueryPosition(columnIndexes, indexColumnIndex);
        this.symbolFunction = symbolFunction;
        this.columnIndexes = columnIndexes;

        this.latestBy = latestBy;
        this.latestByFilter = latestByFilter;

        this.queryColToIncludeIdx = queryColToIncludeIdx;

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
        this.cursor = new CoveringCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, multiKeyCapacity, symInclCols, columnIndexes, latestBy, metadata);
        this.pageFrameCursor = !latestBy
                ? new CoveringPageFrameCursor(indexColumnIndex, symbolKey, queryColToIncludeIdx, metadata, columnIndexes)
                : null;
    }

    @Override
    public void close() {
        Misc.free(dfcFactory);
        Misc.free(latestByFilter);
        Misc.free(symbolFunction);
        Misc.freeObjList(keyValueFuncs);
        if (pageFrameCursor != null) {
            pageFrameCursor.close();
        }
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
                cursor.latestByFilter = latestByFilter;
                if (latestByFilter != null) {
                    latestByFilter.init(cursor, executionContext);
                }
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
            cursor.latestByFilter = latestByFilter;
            if (latestByFilter != null) {
                latestByFilter.init(cursor, executionContext);
            }
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
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (pageFrameCursor == null) {
            return null;
        }
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            TableReader reader = frameCursor.getTableReader();
            if (resolvedKeys != null) {
                SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                pageFrameCursor.multiKeys.clear();
                for (int i = 0, n = resolvedKeys.size(); i < n; i++) {
                    int key = resolvedKeys.getQuick(i);
                    if (key == SymbolTable.VALUE_NOT_FOUND && keyValueFuncs != null) {
                        CharSequence symValue = keyValueFuncs.getQuick(i).getStrA(null);
                        key = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                    }
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        pageFrameCursor.multiKeys.add(key);
                    }
                }
                if (pageFrameCursor.multiKeys.size() == 0) {
                    Misc.free(frameCursor);
                    pageFrameCursor.ofEmpty();
                    return pageFrameCursor;
                }
            } else {
                int resolvedKey = pageFrameCursor.symbolKey;
                if (resolvedKey == SymbolTable.VALUE_NOT_FOUND && symbolFunction != null) {
                    SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                    CharSequence symValue = symbolFunction.getStrA(null);
                    resolvedKey = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                    if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
                        Misc.free(frameCursor);
                        pageFrameCursor.ofEmpty();
                        return pageFrameCursor;
                    }
                    pageFrameCursor.resolvedKey = resolvedKey;
                }
            }
            pageFrameCursor.of(frameCursor);
            return pageFrameCursor;
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
        return pageFrameCursor != null;
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

    private static class CoveringCursor implements RecordCursor {
        private final IntList columnIndexes;
        private final CoveringRecord coveringRecord;
        private final FallbackRecord fallbackRecord;
        private final int indexColumnIndex;
        private final boolean latestBy;
        private final IntList multiKeys;
        private final int[] requiredIncludeIndices;
        private final SymbolTable[] symTablesCache;
        private final int[] symbolIncludeCols;
        private Record activeRecord;
        private int cachedPartitionIndex;
        private long cachedRowHi;
        private long cachedRowLo;
        private int currentKeyIdx;
        private CoveringRowCursor currentRowCursor;
        private PartitionFrameCursor frameCursor;
        private boolean isLatestByDone;
        private Function latestByFilter;
        private int symbolKey;
        private TableReader tableReader;

        CoveringCursor(int indexColumnIndex, int symbolKey, int[] queryColToIncludeIdx,
                       int multiKeyCapacity, int[] symbolIncludeCols, IntList columnIndexes,
                       boolean latestBy, RecordMetadata metadata) {
            this.indexColumnIndex = indexColumnIndex;
            this.symbolKey = symbolKey;
            this.fallbackRecord = new FallbackRecord(queryColToIncludeIdx, symbolKey, columnIndexes, metadata);
            this.coveringRecord = new CoveringRecord(queryColToIncludeIdx, symbolKey, metadata, fallbackRecord);
            this.requiredIncludeIndices = buildRequiredIncludeIndices(queryColToIncludeIdx);
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
        public long size() {
            if (frameCursor == null || latestBy || multiKeys != null
                    || symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return -1;
            }
            // Sum posting index key counts across all partitions from generation
            // metadata, without iterating or decoding individual row values.
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
                        // rc.size() reports counts from gen prefix-sums, which span the whole
                        // partition. Only trust it for frames that cover the whole partition;
                        // otherwise iterate to honor the rowLo/rowHi bounds.
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
        public void toTop() {
            if (frameCursor != null) {
                frameCursor.toTop();
            }
            currentRowCursor = Misc.free(currentRowCursor);
            currentKeyIdx = 0;
            cachedPartitionIndex = -1;
            isLatestByDone = false;
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

        private boolean findLatestRow(int rawSymbolKey) {
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
                        if (bwdCursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                            Misc.free(currentRowCursor);
                            currentRowCursor = crc;
                            bwdCursor = null;
                            coveringRecord.of(crc);
                            coveringRecord.setSymbolKey(rawSymbolKey);
                            // Keep fallbackRecord in sync so per-column
                            // delegation from coveringRecord stays valid.
                            fallbackRecord.of(tableReader, partitionIndex);
                            fallbackRecord.setSymbolKey(rawSymbolKey);
                            while (crc.hasNext()) {
                                long rowId = crc.next();
                                coveringRecord.setRowId(rowId);
                                fallbackRecord.setRowId(rowId);
                                if (latestByFilter.getBool(coveringRecord)) {
                                    activeRecord = coveringRecord;
                                    return true;
                                }
                            }
                        } else {
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
                                if (crc.hasCovering()) {
                                    coveringRecord.of(crc);
                                    coveringRecord.setSymbolKey(rawSymbolKey);
                                    coveringRecord.setRowId(lastRowId);
                                    fallbackRecord.of(tableReader, partitionIndex);
                                    fallbackRecord.setSymbolKey(rawSymbolKey);
                                    fallbackRecord.setRowId(lastRowId);
                                    activeRecord = coveringRecord;
                                } else {
                                    fallbackRecord.of(tableReader, partitionIndex);
                                    fallbackRecord.setSymbolKey(rawSymbolKey);
                                    fallbackRecord.setRowId(lastRowId);
                                    activeRecord = fallbackRecord;
                                }
                                return true;
                            }
                        }
                    } finally {
                        Misc.free(rowCursor);
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
            if (isLatestByDone) {
                return false;
            }
            if (findLatestRow(symbolKey)) {
                isLatestByDone = true;
                return true;
            }
            return false;
        }

        private boolean tryOpenKey(int partitionIndex, int rawSymbolKey, long rowLo, long rowHi) {
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
                    if (crc.hasCovering()) {
                        coveringRecord.of(crc);
                        coveringRecord.setSymbolKey(rawSymbolKey);
                        // Keep fallbackRecord in sync for per-column delegation.
                        fallbackRecord.of(tableReader, partitionIndex);
                        fallbackRecord.setSymbolKey(rawSymbolKey);
                        activeRecord = coveringRecord;
                    } else {
                        fallbackRecord.of(tableReader, partitionIndex);
                        fallbackRecord.setSymbolKey(rawSymbolKey);
                        activeRecord = fallbackRecord;
                    }
                    return true;
                }
                return false;
            } finally {
                Misc.free(rowCursor);
            }
        }

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentRowCursor = Misc.free(this.currentRowCursor);
            this.currentKeyIdx = 0;
            this.cachedPartitionIndex = -1;
            this.isLatestByDone = false;
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

        void resolveKey(int resolvedKey) {
            this.symbolKey = resolvedKey;
            this.coveringRecord.setSymbolKey(resolvedKey);
            this.fallbackRecord.setSymbolKey(resolvedKey);
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
        public IndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return null;
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

    private static class CoveringPageFrameCursor implements PageFrameCursor {
        private static final int INITIAL_CAPACITY = 4096;
        // Tracks all native allocations as (addr, size) pairs for bulk cleanup.
        // Each fillFrameForKey() call allocates fresh buffers so that
        // PageFrameAddressCache can hold addresses from multiple frames
        // simultaneously (vectorized GROUP BY collects all frames before processing).
        private final LongList allocatedBuffers = new LongList();
        private final DirectBinarySequence columnBin = new DirectBinarySequence();
        private final IntList columnIndexes;
        private final ColumnMapping columnMapping = new ColumnMapping();
        private final int[] columnSizeBytes;
        private final io.questdb.std.str.DirectString columnStrA = new io.questdb.std.str.DirectString();
        private final int[] columnTypeTags;
        private final CoveringPageFrame frame;
        // Reusable per-frame arrays (avoid per-frame heap allocation)
        private final long[] frameAddrs;
        private final long[] frameVarDataAddrs;
        private final int[] frameVarDataCap;
        private final int[] frameVarDataPos;
        private final int indexColumnIndex;
        private final IntList multiKeys;
        private final int queryColCount;
        private final int[] queryColToIncludeIdx;
        private final int[] requiredIncludeIndices;
        int resolvedKey;
        int symbolKey;
        private PartitionFrame cachedPartFrame;
        private int currentKeyIdx;
        private PartitionFrameCursor frameCursor;
        private boolean isExhausted;
        private TableReader tableReader;

        CoveringPageFrameCursor(
                int indexColumnIndex,
                int symbolKey,
                int[] queryColToIncludeIdx,
                RecordMetadata metadata,
                IntList columnIndexes
        ) {
            this.indexColumnIndex = indexColumnIndex;
            this.symbolKey = symbolKey;
            this.resolvedKey = symbolKey;
            this.queryColToIncludeIdx = queryColToIncludeIdx;
            this.requiredIncludeIndices = buildRequiredIncludeIndices(queryColToIncludeIdx);
            this.queryColCount = queryColToIncludeIdx.length;
            this.columnIndexes = columnIndexes;
            this.frame = new CoveringPageFrame(queryColCount);
            this.columnSizeBytes = new int[queryColCount];
            this.columnTypeTags = new int[queryColCount];
            this.multiKeys = new IntList();
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
        public @Nullable PageFrame next(long skipTarget) {
            if (frameCursor == null || isExhausted) {
                return null;
            }

            if (multiKeys.size() > 0) {
                return nextMultiKey();
            }
            return nextSingleKey(resolvedKey);
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
            currentKeyIdx = 0;
            isExhausted = false;
            cachedPartFrame = null;
            freeBuffers();
        }

        private static void fillSymbolKey(long addr, int rawSymbolKey, int count) {
            // Pack two copies of the int key into a long for 2x fewer writes
            long longKey = Integer.toUnsignedLong(rawSymbolKey) | ((long) rawSymbolKey << 32);
            int i = 0;
            int pairs = count & ~1; // round down to even
            for (; i < pairs; i += 2) {
                Unsafe.getUnsafe().putLong(addr + (long) i * Integer.BYTES, longKey);
            }
            if (i < count) {
                Unsafe.getUnsafe().putInt(addr + (long) i * Integer.BYTES, rawSymbolKey);
            }
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
                Unsafe.getUnsafe().copyMemory(varDataAddrs[q], newAddr, varDataPos[q]);
                varDataAddrs[q] = newAddr;
                varDataCap[q] = newCap;
            }
        }

        private @Nullable PageFrame fillFrameForKey(int rawSymbolKey, int partitionIndex, long rowLo, long rowHi) {
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
                // Fast path: covering sidecar data available — read directly from sidecar.
                // Fallback: read matching rows from column files (unsealed partitions, etc.)
                CoveringRowCursor coveringCursor = null;
                if (rowCursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                    coveringCursor = crc;
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
                    Unsafe.getUnsafe().putLong(addrs[q] + (long) count * Long.BYTES, varDataPos[q]);
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
                        Unsafe.getUnsafe().copyMemory(addrs[q], newAuxAddr, (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                        addrs[q] = newAuxAddr;
                    } else if (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY) {
                        long newAuxBytes = (long) (newCapacity + 1) * Long.BYTES;
                        long newAuxAddr = allocBuffer(newAuxBytes);
                        Unsafe.getUnsafe().copyMemory(addrs[q], newAuxAddr, (long) count * Long.BYTES);
                        addrs[q] = newAuxAddr;
                    } else {
                        long newBytes = (long) newCapacity * columnSizeBytes[q];
                        long newAddr = allocBuffer(newBytes);
                        Unsafe.getUnsafe().copyMemory(addrs[q], newAddr, (long) count * columnSizeBytes[q]);
                        addrs[q] = newAddr;
                    }
                }
            }
            addrs[queryColCount] = allocBuffer((long) newCapacity * Integer.BYTES);
            return newCapacity;
        }

        private @Nullable PageFrame nextMultiKey() {
            // Iterate all partitions, for each partition try all keys.
            // This matches how CoveringCursor.advanceMultiKey() works.
            while (true) {
                // Try remaining keys in current partition
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
                // Advance to next partition
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                cachedPartFrame = partFrame;
                currentKeyIdx = 0;
            }
        }

        private @Nullable PageFrame nextSingleKey(int rawSymbolKey) {
            while (true) {
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                PageFrame result = fillFrameForKey(
                        rawSymbolKey,
                        partFrame.getPartitionIndex(),
                        partFrame.getRowLo(),
                        partFrame.getRowHi()
                );
                if (result != null) {
                    return result;
                }
            }
        }

        private void writeBinaryToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                        int q, int count, @Nullable BinarySequence value) {
            // BINARY aux: 8-byte offset per row into data vector
            long auxEntry = auxAddr + (long) count * Long.BYTES;
            long dataOffset = varDataPos[q];
            Unsafe.getUnsafe().putLong(auxEntry, dataOffset);

            if (value == null) {
                // Write negative length as NULL marker
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, Long.BYTES);
                Unsafe.getUnsafe().putLong(varDataAddrs[q] + varDataPos[q], TableUtils.NULL_LEN);
                varDataPos[q] += Long.BYTES;
            } else {
                long len = value.length();
                int totalBytes = (int) (Long.BYTES + len);
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, totalBytes);
                long dst = varDataAddrs[q] + varDataPos[q];
                Unsafe.getUnsafe().putLong(dst, len);
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
                MemoryCR mem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                switch (columnTypeTags[q]) {
                    case ColumnType.DOUBLE -> Unsafe.getUnsafe().putDouble(
                            addr + (long) count * Double.BYTES, mem.getDouble(rowId * Double.BYTES));
                    case ColumnType.FLOAT -> Unsafe.getUnsafe().putFloat(
                            addr + (long) count * Float.BYTES, mem.getFloat(rowId * Float.BYTES));
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG,
                         ColumnType.DECIMAL64 ->
                            Unsafe.getUnsafe().putLong(addr + (long) count * Long.BYTES, mem.getLong(rowId * Long.BYTES));
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL,
                         ColumnType.DECIMAL32 ->
                            Unsafe.getUnsafe().putInt(addr + (long) count * Integer.BYTES, mem.getInt(rowId * Integer.BYTES));
                    case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                            Unsafe.getUnsafe().putShort(addr + (long) count * Short.BYTES, mem.getShort(rowId * Short.BYTES));
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                            Unsafe.getUnsafe().putByte(addr + count, mem.getByte(rowId));
                    case ColumnType.UUID, ColumnType.DECIMAL128 -> {
                        long off128 = (long) count * 16;
                        Unsafe.getUnsafe().putLong(addr + off128, mem.getLong(rowId * 16));
                        Unsafe.getUnsafe().putLong(addr + off128 + 8, mem.getLong(rowId * 16 + 8));
                    }
                    case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                        long off256 = (long) count * 32;
                        Unsafe.getUnsafe().putLong(addr + off256, mem.getLong(rowId * 32));
                        Unsafe.getUnsafe().putLong(addr + off256 + 8, mem.getLong(rowId * 32 + 8));
                        Unsafe.getUnsafe().putLong(addr + off256 + 16, mem.getLong(rowId * 32 + 16));
                        Unsafe.getUnsafe().putLong(addr + off256 + 24, mem.getLong(rowId * 32 + 24));
                    }
                    case ColumnType.VARCHAR -> {
                        int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                        MemoryCR dataMem = tableReader.getColumn(primaryIdx);
                        MemoryCR auxMem = tableReader.getColumn(primaryIdx + 1);
                        Utf8Sequence value = VarcharTypeDriver.getSplitValue(auxMem, dataMem, rowId, 0);
                        writeVarcharToFrame(addr, varDataAddrs, varDataPos, varDataCap, q, count, value);
                    }
                    case ColumnType.STRING -> {
                        int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                        MemoryCR strDataMem = tableReader.getColumn(primaryIdx);
                        MemoryCR strAuxMem = tableReader.getColumn(primaryIdx + 1);
                        long dataOffset = strAuxMem.getLong(rowId * Long.BYTES);
                        int len = strDataMem.getInt(dataOffset);
                        CharSequence strValue = (len == TableUtils.NULL_LEN)
                                ? null : columnStrA.of(strDataMem.addressOf(dataOffset + Integer.BYTES), len);
                        writeStringToFrame(addr, varDataAddrs, varDataPos, varDataCap, q, count, strValue);
                    }
                    case ColumnType.BINARY -> {
                        int primaryIdx = TableReader.getPrimaryColumnIndex(colBase, readerCol);
                        MemoryCR binDataMem = tableReader.getColumn(primaryIdx);
                        MemoryCR binAuxMem = tableReader.getColumn(primaryIdx + 1);
                        long dataOffset = binAuxMem.getLong(rowId * Long.BYTES);
                        long binLen = binDataMem.getLong(dataOffset);
                        BinarySequence binValue = (binLen < 0)
                                ? null : columnBin.of(binDataMem.addressOf(dataOffset + Long.BYTES), binLen);
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
                    case ColumnType.DOUBLE -> Unsafe.getUnsafe().putDouble(
                            addr + (long) count * Double.BYTES, crc.getCoveredDouble(includeIdx));
                    case ColumnType.FLOAT -> Unsafe.getUnsafe().putFloat(
                            addr + (long) count * Float.BYTES, crc.getCoveredFloat(includeIdx));
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG ->
                            Unsafe.getUnsafe().putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL ->
                            Unsafe.getUnsafe().putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
                    case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT ->
                            Unsafe.getUnsafe().putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE ->
                            Unsafe.getUnsafe().putByte(addr + count, crc.getCoveredByte(includeIdx));
                    case ColumnType.DECIMAL64 ->
                            Unsafe.getUnsafe().putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
                    case ColumnType.DECIMAL32 ->
                            Unsafe.getUnsafe().putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
                    case ColumnType.DECIMAL16 ->
                            Unsafe.getUnsafe().putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
                    case ColumnType.DECIMAL8 ->
                            Unsafe.getUnsafe().putByte(addr + count, crc.getCoveredByte(includeIdx));
                    case ColumnType.UUID, ColumnType.DECIMAL128 -> {
                        long off128 = (long) count * 16;
                        Unsafe.getUnsafe().putLong(addr + off128, crc.getCoveredLong128Lo(includeIdx));
                        Unsafe.getUnsafe().putLong(addr + off128 + 8, crc.getCoveredLong128Hi(includeIdx));
                    }
                    case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                        long off256 = (long) count * 32;
                        Unsafe.getUnsafe().putLong(addr + off256, crc.getCoveredLong256_0(includeIdx));
                        Unsafe.getUnsafe().putLong(addr + off256 + 8, crc.getCoveredLong256_1(includeIdx));
                        Unsafe.getUnsafe().putLong(addr + off256 + 16, crc.getCoveredLong256_2(includeIdx));
                        Unsafe.getUnsafe().putLong(addr + off256 + 24, crc.getCoveredLong256_3(includeIdx));
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
            Unsafe.getUnsafe().putLong(auxEntry, dataOffset);

            if (value == null) {
                // Write NULL_LEN (-1) as the length prefix
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, Integer.BYTES);
                Unsafe.getUnsafe().putInt(varDataAddrs[q] + varDataPos[q], TableUtils.NULL_LEN);
                varDataPos[q] += Integer.BYTES;
            } else {
                int charCount = value.length();
                int totalBytes = Integer.BYTES + charCount * Character.BYTES;
                ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, totalBytes);
                long dst = varDataAddrs[q] + varDataPos[q];
                Unsafe.getUnsafe().putInt(dst, charCount);
                for (int c = 0; c < charCount; c++) {
                    Unsafe.getUnsafe().putChar(dst + Integer.BYTES + (long) c * Character.BYTES, value.charAt(c));
                }
                varDataPos[q] += totalBytes;
            }
        }

        private void writeVarcharToFrame(long auxAddr, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
                                         int q, int count, @Nullable Utf8Sequence value) {
            long auxEntry = auxAddr + (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            long dataOffset = varDataPos[q];

            if (value == null) {
                Unsafe.getUnsafe().putInt(auxEntry, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                Unsafe.getUnsafe().putInt(auxEntry + 4, 0);
                Unsafe.getUnsafe().putShort(auxEntry + 8, (short) 0);
                Unsafe.getUnsafe().putShort(auxEntry + 10, (short) dataOffset);
                Unsafe.getUnsafe().putInt(auxEntry + 12, (int) (dataOffset >> 16));
            } else {
                int size = value.size();
                if (size <= VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    int header = (size << 4) | 1; // HEADER_FLAG_INLINED
                    if (value.isAscii()) header |= 2; // HEADER_FLAG_ASCII
                    Unsafe.getUnsafe().putByte(auxEntry, (byte) header);
                    for (int b = 0; b < size; b++) {
                        Unsafe.getUnsafe().putByte(auxEntry + 1 + b, value.byteAt(b));
                    }
                    for (int b = size; b < VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED; b++) {
                        Unsafe.getUnsafe().putByte(auxEntry + 1 + b, (byte) 0);
                    }
                    Unsafe.getUnsafe().putShort(auxEntry + 10, (short) dataOffset);
                    Unsafe.getUnsafe().putInt(auxEntry + 12, (int) (dataOffset >> 16));
                } else {
                    int header = (size << 4);
                    if (value.isAscii()) header |= 2;
                    Unsafe.getUnsafe().putInt(auxEntry, header);
                    for (int b = 0; b < VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES; b++) {
                        Unsafe.getUnsafe().putByte(auxEntry + 4 + b, value.byteAt(b));
                    }
                    ensureVarDataCapacity(varDataAddrs, varDataPos, varDataCap, q, size);
                    // Use bulk copy when the Utf8Sequence has a stable native pointer
                    // (always true for DirectUtf8String from covering sidecar reads)
                    long srcPtr = value.ptr();
                    if (srcPtr != 0) {
                        Unsafe.getUnsafe().copyMemory(srcPtr, varDataAddrs[q] + varDataPos[q], size);
                    } else for (int b = 0; b < size; b++) {
                        Unsafe.getUnsafe().putByte(varDataAddrs[q] + varDataPos[q] + b, value.byteAt(b));
                    }
                    Unsafe.getUnsafe().putShort(auxEntry + 10, (short) dataOffset);
                    Unsafe.getUnsafe().putInt(auxEntry + 12, (int) (dataOffset >> 16));
                    varDataPos[q] += size;
                }
            }
        }

        void of(PartitionFrameCursor frameCursor) {
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.currentKeyIdx = 0;
            this.isExhausted = false;
            this.cachedPartFrame = null;
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
            this.cachedPartFrame = null;
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
        private final FallbackRecord fallbackRecord;
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final RecordMetadata metadata;
        private final int[] queryColToIncludeIdx;
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

        // Per-column dispatch: each getXxx reads from the sidecar when
        // cursor.isCoveredAvailable(includeIdx) is true, otherwise forwards
        // to fallbackRecord which reads from the partition's main column file.

        @Override
        public ArrayView getArray(int col, int columnType) {
            int includeIdx = getIncludeIdx(col);
            if (includeIdx >= 0 && cursor != null) {
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
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
                if (cursor.isCoveredAvailable(includeIdx)) {
                    return cursor.getCoveredVarcharB(includeIdx);
                }
                return fallbackRecord.getVarcharB(col);
            }
            return null;
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
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                return fallbackArray.of(columnType, auxMem.addressOf(0), auxMem.addressOf(0) + auxMem.size(),
                        dataMem.addressOf(0), dataMem.addressOf(0) + dataMem.size(), rowId);
            }
            return null;
        }

        @Override
        public BinarySequence getBin(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(rowId * Long.BYTES);
                long len = dataMem.getLong(dataOffset);
                if (len < 0) return null;
                return fallbackBin.of(dataMem.addressOf(dataOffset + Long.BYTES), len);
            }
            return null;
        }

        @Override
        public long getBinLen(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                int readerCol = columnIndexes.getQuick(col);
                int colBase = tableReader.getColumnBase(partitionIndex);
                MemoryCR dataMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol));
                MemoryCR auxMem = tableReader.getColumn(TableReader.getPrimaryColumnIndex(colBase, readerCol) + 1);
                long dataOffset = auxMem.getLong(rowId * Long.BYTES);
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
                return columnMem(col).getByte(rowId);
            }
            return 0;
        }

        @Override
        public char getChar(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getChar(rowId * Character.BYTES);
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
                MemoryCR mem = columnMem(col);
                int scale = ColumnType.getDecimalScale(metadata.getColumnType(col));
                // DECIMAL128 stores high at offset 0, low at offset 8
                sink.of(mem.getLong(rowId * 16), mem.getLong(rowId * 16 + Long.BYTES), scale);
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
                MemoryCR mem = columnMem(col);
                long off = rowId * 32;
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
        public int getIPv4(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getInt(rowId * Integer.BYTES);
            }
            return Numbers.IPv4_NULL;
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
        public long getLong128Hi(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getLong(rowId * 16 + Long.BYTES);
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Lo(int col) {
            if (getIncludeIdx(col) >= 0 && tableReader != null) {
                return columnMem(col).getLong(rowId * 16);
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
                MemoryCR mem = columnMem(col);
                long off = rowId * 32;
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
                MemoryCR mem = columnMem(col);
                long off = rowId * 32;
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
                return columnMem(col).getShort(rowId * Short.BYTES);
            }
            return 0;
        }

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
}

