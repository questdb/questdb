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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptySymbolMapReader;
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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import org.jetbrains.annotations.TestOnly;

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
        // Defensive copy. The caller passes intrinsicModel.keyValueFuncs, which is a
        // POOLED ObjList owned by the compiler's WhereClauseParser (ObjectPool<IntrinsicModel>).
        // SqlCompilers are pooled and shared across threads/connections, so when another
        // thread borrows the same compiler and recompiles, models.next() -> IntrinsicModel.clear()
        // -> keyValueFuncs.clear() nulls the backing array (Arrays.fill BEFORE pos=0). A concurrent
        // getCursor() on this still-cached factory would then read a stale size() (> 0) and a null
        // slot in Function.init(...), producing the intermittent NPE in issue #7294. We keep our own
        // list of the same Function instances -- which this factory owns and frees in close() (the
        // pooled model only clears references, never frees) -- to decouple from the model's lifecycle.
        this.keyValueFuncs = keyValueFuncs != null ? new ObjList<>(keyValueFuncs) : null;
        int[] requiredIncludeIndices = buildRequiredIncludeIndices(queryColToIncludeIdx);

        int[] symInclCols = findSymbolIncludeCols(queryColToIncludeIdx, metadata);
        // Read the owned defensive copy (this.keyValueFuncs), never the pooled parameter. The two are
        // content-identical here (compiling thread owns the model exclusively during construction), but
        // the copy is the reference this factory owns and frees in close(); using it consistently avoids
        // a refactor hazard if the defensive copy above is ever changed or removed.
        final ObjList<Function> keyValueFuncsCopy = this.keyValueFuncs;
        if (keyValueFuncsCopy != null) {
            this.resolvedKeys = new IntList(keyValueFuncsCopy.size());
            int multiKeyCapacity = keyValueFuncsCopy.size();
            if (reader != null) {
                SymbolMapReader smr = reader.getSymbolMapReader(indexColumnIndex);
                for (int i = 0, n = keyValueFuncsCopy.size(); i < n; i++) {
                    Function f = keyValueFuncsCopy.getQuick(i);
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

    /**
     * Test-only hook that overrides the per-frame row cap so multi-frame /
     * resume code paths in {@link CoveringPageFrameCursor} can be exercised
     * with small inputs. Pass {@code -1} to clear the override and revert
     * to the engine configuration value.
     */
    @TestOnly
    public static void setMaxRowsPerFrameForTesting(int newCap) {
        CoveringPageFrameCursor.maxRowsPerFrameOverride = newCap;
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
                for (int i = 0, n = resolvedKeys.size(); i < n; i++) {
                    int key = resolvedKeys.getQuick(i);
                    if (key == SymbolTable.VALUE_NOT_FOUND && keyValueFuncs != null) {
                        CharSequence symValue = keyValueFuncs.getQuick(i).getStrA(null);
                        key = symValue != null ? smr.keyOf(symValue) : SymbolTable.VALUE_NOT_FOUND;
                    }
                    // Bind-variable / runtime-constant list elements may resolve
                    // to the same symbol key; dedup so the multi-key merge does
                    // not open a duplicate posting cursor per key and merge the
                    // same row-id stream twice (duplicate rows / inflated
                    // aggregates).
                    if (key != SymbolTable.VALUE_NOT_FOUND && !multiKeyCursor.multiKeys.contains(key)) {
                        multiKeyCursor.multiKeys.add(key);
                    }
                }
                // Always wire up the frame cursor and table reader, even when no
                // keys resolve. Callers wrap us in operators (e.g. ORDER BY on a
                // SYMBOL column) that probe baseCursor.getSymbolTable() during
                // init, before any iteration. With an empty multiKeys list,
                // hasNext()'s merge finds no per-key heads and
                // openNextPartitionCursors() opens nothing, so it reports no rows.
                multiKeyCursor.of(frameCursor);
                multiKeyCursor.circuitBreaker = executionContext.getCircuitBreaker();
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
            singleKeyCursor.resolveKey(resolvedKey);
            singleKeyCursor.of(frameCursor);
            singleKeyCursor.circuitBreaker = executionContext.getCircuitBreaker();
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
        // A negative LIMIT routes the async filter through its backward
        // (negative-limit) path, which asks us for ORDER_DESC frames: highest
        // timestamps first. We honor it with a genuine backward scan -- DESC
        // partition iteration plus high row-range sub-frames emitted first --
        // rather than silently returning ascending frames.
        final boolean descending = order == PartitionFrameCursorFactory.ORDER_DESC;
        if (descending && multiKeyPageFrameCursor != null) {
            // The multi-key page-frame cursor merges keys forward only; it has no
            // backward scan. Codegen therefore routes multi-key negative-limit
            // queries to the serial path, so a backward scan must never reach here.
            throw CairoException.nonCritical().put("backward covering scan is not supported for multi-key index queries");
        }
        int configMaxRows = executionContext.getPageFrameMaxRows();
        PartitionFrameCursor frameCursor = dfcFactory.getCursor(
                executionContext,
                columnIndexes,
                descending ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC
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
                    // See getCursor(): dedup duplicate resolved keys so the
                    // parallel GROUP BY page-frame path does not over-count.
                    if (key != SymbolTable.VALUE_NOT_FOUND && !multiKeyPageFrameCursor.multiKeys.contains(key)) {
                        multiKeyPageFrameCursor.multiKeys.add(key);
                    }
                }
                // Always wire the frame cursor; callers may probe getSymbolTable()
                // before iteration. Empty multiKeys list yields no frames.
                multiKeyPageFrameCursor.of(frameCursor, configMaxRows, false);
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
            singleKeyPageFrameCursor.resolvedKey = resolvedKey;
            singleKeyPageFrameCursor.of(frameCursor, configMaxRows, descending);
            return singleKeyPageFrameCursor;
        } catch (Throwable th) {
            Misc.free(frameCursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        // Non-latestBy: partition iteration is ASC, and within each
        // partition rows are emitted in row-id ascending order (single
        // key directly; multi key via the row-id merge across per-key
        // posting cursors). Row-id is ts-ascending by the designated
        // timestamp contract, so the overall stream is ts-ascending and
        // SAMPLE BY / ORDER-BY-ts elision can trust this advertisement.
        // Single-key latestBy returns a single row (the latest for the one
        // resolved key), so it is trivially ts-ordered. Only multi-key
        // latestBy breaks the order: it emits one row per key in key order,
        // not ts order, so it alone advertises no ordering.
        return latestBy && multiKeyCursor != null ? SCAN_DIRECTION_OTHER : SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    /**
     * Whether this factory can serve a backward (negative-limit) page-frame scan.
     * Only single-key queries qualify: the single-key cursor implements a genuine
     * backward scan (DESC partitions, high row-range sub-frames first). The
     * multi-key cursor merges keys forward only -- it is globally
     * timestamp-ordered ascending, but has no backward scan -- so codegen routes
     * its negative limits to the serial path, where LimitRecordCursorFactory
     * computes last-N via size + skip over the ascending merge.
     */
    public boolean supportsNegativeLimitPageFrame() {
        return singleKeyPageFrameCursor != null;
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

    /**
     * Open a forward {@link CoveringRowCursor} for a single key over a partition's
     * row range, or null when the key has no rows there (the index reader returns
     * EmptyRowCursor, which is not a CoveringRowCursor). Shared by the record and
     * page-frame multi-key mergers, which hold one such cursor per key at a time.
     */
    private static CoveringRowCursor openForwardCoveringCursor(
            TableReader tableReader,
            int indexColumnIndex,
            int[] requiredIncludeIndices,
            int partitionIndex,
            int rawSymbolKey,
            long rowLo,
            long rowHi
    ) {
        IndexReader indexReader = tableReader.getIndexReader(partitionIndex, indexColumnIndex, IndexReader.DIR_FORWARD);
        RowCursor rowCursor = indexReader.getCursor(
                TableUtils.toIndexKey(rawSymbolKey),
                rowLo,
                rowHi - 1,
                requiredIncludeIndices
        );
        if (rowCursor instanceof CoveringRowCursor crc) {
            return crc;
        }
        Misc.free(rowCursor);
        return null;
    }

    private static abstract class CoveringCursor implements RecordCursor {
        protected final IntList columnIndexes;
        protected final CoveringRecord coveringRecord;
        protected final int indexColumnIndex;
        protected final boolean latestBy;
        protected final int[] requiredIncludeIndices;
        protected final SymbolTable[] symTablesCache;
        protected final int[] symbolIncludeCols;
        protected SqlExecutionCircuitBreaker circuitBreaker;
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
            // Free the row cursor BEFORE the frame cursor. The frame cursor owns the
            // TableReader and Misc.free(frameCursor) returns it to the pool; once pooled,
            // another thread can acquire+reload the reader and close the per-partition
            // PostingIndex*Reader that currentRowCursor was checked out from. Closing the
            // row cursor first guarantees its owning reader is still open (this thread
            // still holds it), so the cursor re-pools into a live reader rather than a
            // stale/closed one. Mirrors CoveringPageFrameCursor.close() (closePendingCursor
            // before freeing frameCursor).
            this.currentRowCursor = Misc.free(currentRowCursor);
            frameCursor = Misc.free(frameCursor);
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
            if (frameCursor == null) {
                return EmptySymbolMapReader.INSTANCE;
            }
            return frameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            // Consult the breaker at the top, so empty/no-match scans (frameCursor null, or no rows for
            // the key) still observe cancellation, and long index scans stay cancellable.
            circuitBreaker.statefulThrowExceptionIfTripped();
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
            if (frameCursor == null) {
                return EmptySymbolMapReader.INSTANCE;
            }
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

    private static abstract class CoveringPageFrameCursor implements TablePageFrameCursor {
        private static final int INITIAL_CAPACITY = 4096;
        private static int maxRowsPerFrameOverride = -1;
        protected int maxRowsPerFrame;
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
        // When true, emit frames in descending timestamp order (DESC partition
        // iteration, high row-range sub-frames first) to serve a negative LIMIT.
        protected boolean descending;
        protected PartitionFrameCursor frameCursor;
        protected boolean isExhausted;
        // Resume state for chunked fillFrameForKey. When a key+partition
        // has more rows than maxRowsPerFrame, the open RowCursor is
        // kept here and the next fillFrameForKey call continues from
        // where the previous returned. pendingRowCursor == null means
        // no fill is in progress.
        protected CoveringRowCursor pendingCoveringCursor;
        protected int pendingPartitionIndex = -1;
        protected RowCursor pendingRowCursor;
        protected int pendingSymbolKey = -1;
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
            closePendingCursor();
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
        public TableReader getTableReader() {
            return tableReader;
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

        // Initialized via the package-private of(PartitionFrameCursor, int, boolean) below.
        @Override
        public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) {
            throw new UnsupportedOperationException();
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
            closePendingCursor();
            if (frameCursor != null) {
                frameCursor.toTop();
            }
            isExhausted = false;
            resetIterationState();
            freeBuffers();
        }

        protected long allocBuffer(long bytes) {
            long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_INDEX_READER);
            allocatedBuffers.add(addr, bytes);
            return addr;
        }

        private void ensureVarDataCapacity(long[] varDataAddrs, int[] varDataPos, int[] varDataCap, int q, int needed) {
            if (varDataPos[q] + needed > varDataCap[q]) {
                int newCap = Math.max(varDataCap[q] * 2, varDataPos[q] + needed);
                long newAddr = growBuffer(varDataAddrs[q], varDataCap[q], newCap, varDataPos[q]);
                varDataAddrs[q] = newAddr;
                varDataCap[q] = newCap;
            }
        }

        private void freeBuffers() {
            for (int i = 0, n = allocatedBuffers.size(); i < n; i += 2) {
                long addr = allocatedBuffers.getQuick(i);
                if (addr != 0) {
                    Unsafe.free(addr, allocatedBuffers.getQuick(i + 1), MemoryTag.NATIVE_INDEX_READER);
                }
            }
            allocatedBuffers.clear();
        }

        /**
         * Replace an already-tracked buffer with a larger one, freeing the
         * old buffer immediately. The previous pattern allocated each
         * growth step via {@link #allocBuffer} and only freed all of them
         * at cursor close, so for an N-step exponential growth the
         * allocator held the SUM of all prior sizes (= 2 * current size)
         * in addition to the new buffer. For large result sets that
         * doubled the per-cursor anonymous-heap footprint and tripped
         * RSS_MEM_LIMIT well before the working set actually exceeded it.
         * <p>
         * This swap-in-place pattern keeps the cursor's anonymous heap
         * bounded to (current size + new size) during the copy, then
         * just (new size) once the old buffer is released.
         */
        private long growBuffer(long oldAddr, long oldSize, long newSize, long usedBytes) {
            long newAddr = Unsafe.malloc(newSize, MemoryTag.NATIVE_INDEX_READER);
            if (usedBytes > 0) {
                Unsafe.copyMemory(oldAddr, newAddr, usedBytes);
            }
            int n = allocatedBuffers.size();
            for (int i = 0; i < n; i += 2) {
                if (allocatedBuffers.getQuick(i) == oldAddr) {
                    allocatedBuffers.setQuick(i, newAddr);
                    allocatedBuffers.setQuick(i + 1, newSize);
                    Unsafe.free(oldAddr, oldSize, MemoryTag.NATIVE_INDEX_READER);
                    return newAddr;
                }
            }
            // Untracked old address. Should not happen for buffers
            // allocated via allocBuffer; defensive path keeps the new
            // buffer reachable so freeBuffers cleans it up at close.
            allocatedBuffers.add(newAddr, newSize);
            Unsafe.free(oldAddr, oldSize, MemoryTag.NATIVE_INDEX_READER);
            return newAddr;
        }

        /**
         * Grow all column and symbol buffers. addrs[0..queryColCount-1] are column
         * buffers; addrs[queryColCount] is the symbol buffer. Returns new capacity.
         * <p>
         * Uses {@link #growBuffer} for in-place tracking swap so prior-generation
         * buffers are freed immediately rather than pinned in anonymous heap until
         * cursor close -- the same leak-on-grow that {@link #ensureVarDataCapacity}
         * fixes.
         */
        protected int growFrameBuffers(long[] addrs, int count, int capacity) {
            int newCapacity = capacity * 2;
            for (int q = 0; q < queryColCount; q++) {
                if (queryColToIncludeIdx[q] >= 0) {
                    if (columnTypeTags[q] == ColumnType.VARCHAR) {
                        long oldBytes = (long) capacity * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                        long newBytes = (long) newCapacity * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                        long copyBytes = (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                        addrs[q] = growBuffer(addrs[q], oldBytes, newBytes, copyBytes);
                    } else if (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY) {
                        long oldBytes = (long) (capacity + 1) * Long.BYTES;
                        long newBytes = (long) (newCapacity + 1) * Long.BYTES;
                        long copyBytes = (long) count * Long.BYTES;
                        addrs[q] = growBuffer(addrs[q], oldBytes, newBytes, copyBytes);
                    } else if (columnTypeTags[q] == ColumnType.ARRAY) {
                        long oldBytes = (long) capacity * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                        long newBytes = (long) newCapacity * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                        long copyBytes = (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                        addrs[q] = growBuffer(addrs[q], oldBytes, newBytes, copyBytes);
                    } else {
                        long oldBytes = (long) capacity * columnSizeBytes[q];
                        long newBytes = (long) newCapacity * columnSizeBytes[q];
                        long copyBytes = (long) count * columnSizeBytes[q];
                        addrs[q] = growBuffer(addrs[q], oldBytes, newBytes, copyBytes);
                    }
                }
            }
            long symOldBytes = (long) capacity * Integer.BYTES;
            long symNewBytes = (long) newCapacity * Integer.BYTES;
            long symCopyBytes = (long) count * Integer.BYTES;
            addrs[queryColCount] = growBuffer(addrs[queryColCount], symOldBytes, symNewBytes, symCopyBytes);
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

        protected void writeCoveredRow(long[] addrs, long[] varDataAddrs, int[] varDataPos, int[] varDataCap,
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
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG,
                         ColumnType.DECIMAL64 ->
                            Unsafe.putLong(addr + (long) count * Long.BYTES, crc.getCoveredLong(includeIdx));
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                            Unsafe.putInt(addr + (long) count * Integer.BYTES, crc.getCoveredInt(includeIdx));
                    case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                            Unsafe.putShort(addr + (long) count * Short.BYTES, crc.getCoveredShort(includeIdx));
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                            Unsafe.putByte(addr + count, crc.getCoveredByte(includeIdx));
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

        /**
         * Allocate the per-frame column and symbol buffers at
         * {@link #INITIAL_CAPACITY} and reset var-data positions. Returns the
         * starting row capacity; {@link #growFrameBuffers} grows it as rows are
         * written. Buffers stay reachable via {@code allocatedBuffers} until the
         * AsyncFilter dispatch frees them.
         */
        protected int allocFrameBuffers() {
            int capacity = INITIAL_CAPACITY;
            Arrays.fill(frameVarDataAddrs, 0);
            Arrays.fill(frameVarDataPos, 0);
            Arrays.fill(frameVarDataCap, 0);
            for (int q = 0; q < queryColCount; q++) {
                if (queryColToIncludeIdx[q] >= 0) {
                    if (columnTypeTags[q] == ColumnType.VARCHAR) {
                        frameAddrs[q] = allocBuffer((long) capacity * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                        int initDataCap = capacity * 32;
                        frameVarDataAddrs[q] = allocBuffer(initDataCap);
                        frameVarDataCap[q] = initDataCap;
                    } else if (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY) {
                        // STRING/BINARY aux: 8 bytes per row (offset), plus sentinel at end
                        frameAddrs[q] = allocBuffer((long) (capacity + 1) * Long.BYTES);
                        int initDataCap = capacity * 32;
                        frameVarDataAddrs[q] = allocBuffer(initDataCap);
                        frameVarDataCap[q] = initDataCap;
                    } else if (columnTypeTags[q] == ColumnType.ARRAY) {
                        // ARRAY aux: 16 bytes per row [offset][size]
                        frameAddrs[q] = allocBuffer((long) capacity * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
                        int initDataCap = capacity * 32;
                        frameVarDataAddrs[q] = allocBuffer(initDataCap);
                        frameVarDataCap[q] = initDataCap;
                    } else {
                        frameAddrs[q] = allocBuffer((long) capacity * columnSizeBytes[q]);
                    }
                }
            }
            frameAddrs[queryColCount] = allocBuffer((long) capacity * Integer.BYTES);
            return capacity;
        }

        /**
         * Produce up to {@code rowCap} rows for {@code rawSymbolKey} in the given
         * partition's row range. If the key has more rows than the cap, the open
         * {@link RowCursor} is parked in {@link #pendingRowCursor}; the caller is
         * expected to call {@code fillFrameForKey} again with the SAME
         * key/partition until it returns {@code null} (or {@link #pendingRowCursor}
         * clears) before advancing to the next partition.
         * {@link SingleKeyCoveringPageFrameCursor#nextImpl} /
         * {@link MultiKeyCoveringPageFrameCursor#nextImpl} drive that loop.
         * <p>
         * Each call allocates a fresh set of frame buffers via
         * {@link #allocFrameBuffers()}; very large keys produce multiple frames
         * instead of one GiB-sized frame.
         */
        protected @Nullable PageFrame fillFrameForKey(int rawSymbolKey, int partitionIndex, long rowLo, long rowHi, int rowCap) {
            final CoveringRowCursor coveringCursor = openOrContinueCoveringCursor(rawSymbolKey, partitionIndex, rowLo, rowHi);
            if (coveringCursor == null) {
                return null;
            }
            int capacity = allocFrameBuffers();
            int count = 0;
            boolean cursorExhausted = true;
            try {
                while (coveringCursor.hasNext()) {
                    coveringCursor.next();
                    if (count >= capacity) {
                        capacity = growFrameBuffers(frameAddrs, count, capacity);
                    }
                    writeCoveredRow(frameAddrs, frameVarDataAddrs, frameVarDataPos, frameVarDataCap, count, coveringCursor);
                    count++;
                    if (count >= rowCap) {
                        cursorExhausted = false;
                        break;
                    }
                }
            } catch (Throwable t) {
                // Drop the parked cursor on error so the caller's outer
                // close() path doesn't double-free or operate on a
                // half-consumed cursor.
                closePendingCursor();
                throw t;
            }
            if (cursorExhausted) {
                closePendingCursor();
            }
            if (count == 0) {
                return null;
            }
            // Single key per frame: fill the symbol column uniformly.
            fillSymbolKey(frameAddrs[queryColCount], rawSymbolKey, count);
            return finalizeFrame(count, partitionIndex);
        }

        /**
         * Point the reusable {@link #frame} at the filled buffers for {@code count}
         * rows in {@code partitionIndex}. The symbol buffer
         * ({@code frameAddrs[queryColCount]}) must already be populated -- uniformly
         * by {@link #fillSymbolKey} for a single-key frame, or per row by the
         * multi-key merge.
         */
        protected PageFrame finalizeFrame(int count, int partitionIndex) {
            long symAddr = frameAddrs[queryColCount];
            for (int q = 0; q < queryColCount; q++) {
                int includeIdx = queryColToIncludeIdx[q];
                if (includeIdx >= 0 && columnTypeTags[q] == ColumnType.VARCHAR) {
                    frame.auxPageAddresses[q] = frameAddrs[q];
                    frame.auxPageSizes[q] = (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                    frame.pageAddresses[q] = frameVarDataAddrs[q];
                    frame.pageSizes[q] = frameVarDataPos[q];
                } else if (includeIdx >= 0 && (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY)) {
                    // Write sentinel offset at [count] position
                    Unsafe.putLong(frameAddrs[q] + (long) count * Long.BYTES, frameVarDataPos[q]);
                    frame.auxPageAddresses[q] = frameAddrs[q];
                    frame.auxPageSizes[q] = (long) (count + 1) * Long.BYTES;
                    frame.pageAddresses[q] = frameVarDataAddrs[q];
                    frame.pageSizes[q] = frameVarDataPos[q];
                } else if (includeIdx >= 0 && columnTypeTags[q] == ColumnType.ARRAY) {
                    frame.auxPageAddresses[q] = frameAddrs[q];
                    frame.auxPageSizes[q] = (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                    frame.pageAddresses[q] = frameVarDataAddrs[q];
                    frame.pageSizes[q] = frameVarDataPos[q];
                } else if (includeIdx >= 0) {
                    frame.pageAddresses[q] = frameAddrs[q];
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

        /**
         * Either return the already-open {@link CoveringRowCursor} parked
         * across a previous fillFrameForKey call, or open a new one for the
         * given key + partition range. The caller MUST advance the (key,
         * partition, rowLo, rowHi) tuple atomically with this call --
         * {@link #pendingPartitionIndex} / {@link #pendingSymbolKey} are
         * consulted to confirm the cached cursor matches; a mismatch means
         * the caller advanced past the parked cursor without draining it
         * (a bug in nextImpl), and we defensively close + re-open.
         */
        private CoveringRowCursor openOrContinueCoveringCursor(int rawSymbolKey, int partitionIndex, long rowLo, long rowHi) {
            if (pendingRowCursor != null) {
                if (pendingSymbolKey == rawSymbolKey && pendingPartitionIndex == partitionIndex) {
                    return pendingCoveringCursor;
                }
                // Defensive: parked cursor doesn't match. Close and re-open.
                closePendingCursor();
            }
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
            // EmptyRowCursor (returned when the key has no rows in this
            // partition) is not a CoveringRowCursor; emit no frame.
            if (!(rowCursor instanceof CoveringRowCursor coveringCursor)) {
                Misc.free(rowCursor);
                return null;
            }
            pendingRowCursor = rowCursor;
            pendingCoveringCursor = coveringCursor;
            pendingSymbolKey = rawSymbolKey;
            pendingPartitionIndex = partitionIndex;
            return coveringCursor;
        }

        /**
         * Close and clear the parked cursor. Safe to call when no cursor
         * is parked (no-op).
         */
        protected final void closePendingCursor() {
            if (pendingRowCursor != null) {
                Misc.free(pendingRowCursor);
                pendingRowCursor = null;
                pendingCoveringCursor = null;
                pendingSymbolKey = -1;
                pendingPartitionIndex = -1;
            }
        }

        void of(PartitionFrameCursor frameCursor, int configMaxRows, boolean descending) {
            closePendingCursor();
            this.frameCursor = frameCursor;
            this.tableReader = frameCursor.getTableReader();
            this.maxRowsPerFrame = maxRowsPerFrameOverride >= 0 ? maxRowsPerFrameOverride : configMaxRows;
            this.descending = descending;
            this.isExhausted = false;
            resetIterationState();
            columnMapping.clear();
            for (int i = 0, n = columnIndexes.size(); i < n; i++) {
                columnMapping.addColumn(columnIndexes.getQuick(i), columnIndexes.getQuick(i), columnIndexes.getQuick(i));
            }
            freeBuffers();
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
        // Row-id sentinel: the per-key cursor is exhausted, or the key is absent
        // from the current partition. Real row ids are non-negative.
        private static final long NO_ROW = -1;
        final IntList multiKeys;
        // latestBy iteration cursor over multiKeys (used only by hasNextLatestBy).
        private int currentKeyIdx;
        // Per-key open cursors for the current partition and their peeked head
        // row ids. The cursors are merged by row id so the record cursor emits
        // rows in global (ascending designated-timestamp) order within each
        // partition -- the same result order HeapRowCursorFactory produces on the
        // non-covering path -- instead of draining one key's posting list before
        // the next. The merge is a linear min-scan over the open heads (O(R*N) for
        // R rows and N keys), not HeapRowCursorFactory's O(log N) heap poll; fine
        // for the small IN-lists this serves.
        private CoveringRowCursor[] keyCursors;
        private long[] keyHeads;
        // The key whose head was emitted last; advanced on the next hasNext().
        private int selectedKeyIdx = -1;

        MultiKeyCoveringCursor(int indexColumnIndex, int multiKeyCapacity, int[] queryColToIncludeIdx,
                               int[] requiredIncludeIndices, int[] symbolIncludeCols, IntList columnIndexes,
                               boolean latestBy, RecordMetadata metadata) {
            super(indexColumnIndex, SymbolTable.VALUE_NOT_FOUND, queryColToIncludeIdx, requiredIncludeIndices, symbolIncludeCols, columnIndexes, latestBy, metadata);
            this.multiKeys = new IntList(multiKeyCapacity);
        }

        @Override
        public void close() {
            closeKeyCursors();
            super.close();
        }

        @Override
        public boolean hasNext() {
            // Consult the breaker at the top, so empty/no-match scans (frameCursor null, or no resolved
            // keys) still observe cancellation, and long multi-key merges stay cancellable.
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (frameCursor == null) {
                return false;
            }
            if (latestBy) {
                return hasNextLatestBy();
            }
            final int n = multiKeys.size();
            while (true) {
                // Advance the cursor we emitted last; we deferred this so its
                // covered values stayed readable until the caller consumed them.
                if (selectedKeyIdx >= 0) {
                    CoveringRowCursor c = keyCursors[selectedKeyIdx];
                    keyHeads[selectedKeyIdx] = c.hasNext() ? c.next() : NO_ROW;
                    selectedKeyIdx = -1;
                }
                // Pick the smallest head row id across the open per-key cursors.
                // Two keys never share a row id (a row has one symbol value), so
                // no tie-breaking is needed.
                int best = -1;
                long bestRow = NO_ROW;
                for (int i = 0; i < n; i++) {
                    long h = keyHeads[i];
                    if (h != NO_ROW && (best < 0 || h < bestRow)) {
                        best = i;
                        bestRow = h;
                    }
                }
                if (best >= 0) {
                    selectedKeyIdx = best;
                    coveringRecord.of(keyCursors[best]);
                    coveringRecord.setSymbolKey(multiKeys.getQuick(best));
                    coveringRecord.setRowId(bestRow);
                    return true;
                }
                if (!openNextPartitionCursors()) {
                    return false;
                }
            }
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        boolean advanceKey() {
            // Multi-key iteration is driven by the k-way merge in hasNext(); the
            // per-key drain model of the base class is not used here.
            throw new UnsupportedOperationException();
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
            closeKeyCursors();
            final int n = multiKeys.size();
            if (keyCursors == null || keyCursors.length < n) {
                keyCursors = new CoveringRowCursor[n];
                keyHeads = new long[n];
            }
            for (int i = 0; i < n; i++) {
                keyCursors[i] = null;
                keyHeads[i] = NO_ROW;
            }
        }

        private void closeKeyCursors() {
            if (keyCursors != null) {
                for (int i = 0; i < keyCursors.length; i++) {
                    keyCursors[i] = Misc.free(keyCursors[i]);
                    keyHeads[i] = NO_ROW;
                }
            }
            selectedKeyIdx = -1;
        }

        /**
         * Closes the current partition's per-key cursors and opens the next
         * partition that holds at least one matching row for any key, priming
         * each cursor's head row id. Returns false when partitions are exhausted.
         */
        private boolean openNextPartitionCursors() {
            if (multiKeys.size() == 0) {
                return false;
            }
            closeKeyCursors();
            final int n = multiKeys.size();
            while (true) {
                PartitionFrame frame = frameCursor.next();
                if (frame == null) {
                    return false;
                }
                final int partitionIndex = frame.getPartitionIndex();
                final long rowLo = frame.getRowLo();
                final long rowHi = frame.getRowHi();
                boolean any = false;
                for (int i = 0; i < n; i++) {
                    CoveringRowCursor c = openForwardCoveringCursor(tableReader, indexColumnIndex, requiredIncludeIndices, partitionIndex, multiKeys.getQuick(i), rowLo, rowHi);
                    // Park the cursor before probing it: the cursor owns native
                    // memory and its index reader stops tracking it once checked
                    // out, so a throw from hasNext()/next() before the store
                    // would orphan it. closeKeyCursors() frees keyCursors[i] on
                    // the error path.
                    keyCursors[i] = c;
                    if (c != null && c.hasNext()) {
                        keyHeads[i] = c.next();
                        any = true;
                    } else {
                        keyCursors[i] = Misc.free(c);
                        keyHeads[i] = NO_ROW;
                    }
                }
                if (any) {
                    return true;
                }
                closeKeyCursors();
            }
        }
    }

    private static class MultiKeyCoveringPageFrameCursor extends CoveringPageFrameCursor {
        // Row-id sentinel: per-key cursor exhausted, or key absent from the
        // partition being merged. Real row ids are non-negative.
        private static final long NO_ROW = -1;
        final IntList multiKeys = new IntList();
        // Per-key open cursors for the partition currently being merged and their
        // peeked head row ids. Merging the per-key cursors by row id makes each
        // emitted frame hold rows in ascending designated-timestamp order (with
        // keys interleaved), instead of one key's posting list per frame -- so the
        // parallel filter, LIMIT, and elided ORDER BY ts all see timestamp order.
        private CoveringRowCursor[] mergeCursors;
        private long[] mergeHeads;
        // The partition currently being merged, or -1 when none is open. The merge
        // state persists across nextImpl() calls so a partition that exceeds
        // maxRowsPerFrame resumes in the next frame.
        private int mergePartitionIndex = -1;

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
        public void close() {
            closeMergeCursors();
            super.close();
        }

        @Override
        @Nullable
        PageFrame nextImpl() {
            if (multiKeys.size() == 0) {
                isExhausted = true;
                return null;
            }
            while (true) {
                if (mergePartitionIndex >= 0) {
                    // Continue merging the current partition into the next frame.
                    PageFrame result = fillMergedFrame(mergePartitionIndex, maxRowsPerFrame);
                    if (result != null) {
                        return result;
                    }
                    closeMergeCursors(); // partition fully merged
                }
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                if (openMergeCursors(partFrame)) {
                    mergePartitionIndex = partFrame.getPartitionIndex();
                }
            }
        }

        @Override
        void resetIterationState() {
            closeMergeCursors();
        }

        private void closeMergeCursors() {
            if (mergeCursors != null) {
                for (int i = 0; i < mergeCursors.length; i++) {
                    mergeCursors[i] = Misc.free(mergeCursors[i]);
                    mergeHeads[i] = NO_ROW;
                }
            }
            mergePartitionIndex = -1;
        }

        /**
         * Build one frame of up to {@code rowCap} rows by k-way merging the open
         * per-key cursors on row id. Each row's covered values come from the
         * winning key's cursor and its symbol key is written per row. Returns null
         * once the partition's merge is fully drained.
         */
        @Nullable
        private PageFrame fillMergedFrame(int partitionIndex, int rowCap) {
            final int n = multiKeys.size();
            // nextImpl re-enters once more per partition to learn it is drained:
            // the prior call emitted the last rows but left mergePartitionIndex
            // set. Detect the all-heads-drained case here and return before
            // allocFrameBuffers() allocates buffers the merge loop would only
            // discard (it would otherwise break on best < 0, count == 0, null).
            boolean anyHead = false;
            for (int i = 0; i < n; i++) {
                if (mergeHeads[i] != NO_ROW) {
                    anyHead = true;
                    break;
                }
            }
            if (!anyHead) {
                return null;
            }
            int capacity = allocFrameBuffers();
            long symAddr = frameAddrs[queryColCount];
            int count = 0;
            while (count < rowCap) {
                // Two keys never share a row id, so the smallest head is unique.
                int best = -1;
                long bestRow = NO_ROW;
                for (int i = 0; i < n; i++) {
                    long h = mergeHeads[i];
                    if (h != NO_ROW && (best < 0 || h < bestRow)) {
                        best = i;
                        bestRow = h;
                    }
                }
                if (best < 0) {
                    break; // partition drained
                }
                if (count >= capacity) {
                    capacity = growFrameBuffers(frameAddrs, count, capacity);
                    symAddr = frameAddrs[queryColCount]; // the symbol buffer may have moved
                }
                final CoveringRowCursor c = mergeCursors[best];
                writeCoveredRow(frameAddrs, frameVarDataAddrs, frameVarDataPos, frameVarDataCap, count, c);
                Unsafe.putInt(symAddr + (long) count * Integer.BYTES, multiKeys.getQuick(best));
                count++;
                // Covered values are copied into the frame buffer, so the winning
                // cursor can be advanced to its next row immediately.
                mergeHeads[best] = c.hasNext() ? c.next() : NO_ROW;
            }
            if (count == 0) {
                return null;
            }
            return finalizeFrame(count, partitionIndex);
        }

        /**
         * Open a forward covering cursor for every key over {@code partFrame}'s row
         * range and prime each cursor's head row id. Returns true when at least one
         * key has a matching row in this partition.
         */
        private boolean openMergeCursors(PartitionFrame partFrame) {
            closeMergeCursors();
            final int n = multiKeys.size();
            if (mergeCursors == null || mergeCursors.length < n) {
                mergeCursors = new CoveringRowCursor[n];
                mergeHeads = new long[n];
            }
            final int partitionIndex = partFrame.getPartitionIndex();
            final long rowLo = partFrame.getRowLo();
            final long rowHi = partFrame.getRowHi();
            boolean any = false;
            for (int i = 0; i < n; i++) {
                CoveringRowCursor c = openForwardCoveringCursor(
                        tableReader, indexColumnIndex, requiredIncludeIndices,
                        partitionIndex, multiKeys.getQuick(i), rowLo, rowHi);
                // Park the cursor before probing it: the cursor owns native
                // memory and its index reader stops tracking it once checked
                // out, so a throw from hasNext()/next() before the store would
                // orphan it. closeMergeCursors() frees mergeCursors[i] on the
                // error path.
                mergeCursors[i] = c;
                if (c != null && c.hasNext()) {
                    mergeHeads[i] = c.next();
                    any = true;
                } else {
                    mergeCursors[i] = Misc.free(c);
                    mergeHeads[i] = NO_ROW;
                }
            }
            return any;
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
            // Skip iteration entirely when the literal did not resolve to any
            // known symbol; otherwise we would open every partition's index
            // reader to read empty cursors.
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return false;
            }
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
            if (isLatestByDone || symbolKey == SymbolTable.VALUE_NOT_FOUND) {
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
        // Backward-scan state: the partition currently being drained from its
        // high row-range downward, and the exclusive upper bound of the next
        // sub-frame to emit. descPartitionIndex < 0 means no partition is open.
        private int descPartitionIndex = -1;
        private long descPartitionLo;
        private long descSubHi;

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
            // See SingleKeyCoveringCursor.advanceKey(): skip iteration when
            // the literal did not resolve, instead of scanning every partition
            // for empty cursors.
            if (resolvedKey == SymbolTable.VALUE_NOT_FOUND) {
                isExhausted = true;
                return null;
            }
            if (descending) {
                return nextImplDescending();
            }
            // If a previous fillFrameForKey parked a partially-drained cursor,
            // resume it before advancing the partition iterator. The row
            // range we pass is unused on the resume path (the parked cursor
            // already owns the range), but we keep them in agreement so a
            // mismatch-detection fallback inside openOrContinueCoveringCursor
            // would re-open with the correct range.
            if (pendingRowCursor != null) {
                PageFrame result = fillFrameForKey(
                        pendingSymbolKey,
                        pendingPartitionIndex,
                        0L, 0L, maxRowsPerFrame);
                if (result != null) {
                    return result;
                }
            }
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
                        partFrame.getRowHi(),
                        maxRowsPerFrame
                );
                if (result != null) {
                    return result;
                }
            }
        }

        @Override
        void resetIterationState() {
            descPartitionIndex = -1;
        }

        /**
         * Backward scan for a negative LIMIT. The partition frame cursor was
         * opened ORDER_DESC, so it yields partitions from latest to earliest.
         * Within each partition we split the row range [partitionLo,
         * partitionHi) into sub-frames at most {@code maxRowsPerFrame} rows
         * wide and emit the highest sub-frame first. Each sub-frame is read
         * forward (ascending row ids), so rows stay ascending WITHIN a frame
         * while the frames themselves arrive in descending order -- exactly
         * what {@link AsyncFilteredNegativeLimitRecordCursor} expects from a
         * forward-scan-direction base. Because a sub-frame is at most
         * maxRowsPerFrame row ids wide it holds at most that many matching
         * rows, so a single fill (rowCap = MAX_VALUE) never parks a cursor.
         */
        @Nullable
        private PageFrame nextImplDescending() {
            while (true) {
                if (descPartitionIndex >= 0) {
                    while (descSubHi > descPartitionLo) {
                        // Math.max(1, ...) keeps each sub-frame at least one row wide
                        // so a (test-only) cap of 0 cannot stall the loop.
                        long subLo = Math.max(descPartitionLo, descSubHi - Math.max(1, maxRowsPerFrame));
                        PageFrame result = fillFrameForKey(
                                resolvedKey,
                                descPartitionIndex,
                                subLo,
                                descSubHi,
                                Integer.MAX_VALUE
                        );
                        descSubHi = subLo;
                        if (result != null) {
                            return result;
                        }
                    }
                    descPartitionIndex = -1;
                }
                PartitionFrame partFrame = frameCursor.next();
                if (partFrame == null) {
                    isExhausted = true;
                    return null;
                }
                descPartitionIndex = partFrame.getPartitionIndex();
                descPartitionLo = partFrame.getRowLo();
                descSubHi = partFrame.getRowHi();
            }
        }
    }
}

