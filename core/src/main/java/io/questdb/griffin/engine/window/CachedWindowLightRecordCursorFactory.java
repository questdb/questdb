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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.function.UnaryOperator;

public class CachedWindowLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int SELECTION_CHECK_MASK = 1023;
    private static final int SMALL_SELECTION_SORT_THRESHOLD = 16;
    // Keep the bitmap path for dense selections. At most one selected row per
    // 64 input rows leaves ample room for the sparse path's O(K log K) merge work.
    private static final int SPARSE_SELECTION_DENSITY_SHIFT = 6;
    private final ObjList<WindowFunction> backwardUnorderedFunctions;
    private final GenericRecordMetadata chainMetadata;
    private final ObjList<WindowFunction> forwardUnorderedFunctions;
    private final ObjList<ObjList<WindowFunction>> ordered2PassFunctions;
    // Parallel to ordered2PassFunctions: precomputed once, true iff at least one function in the
    // group reads the base Record in pass2. When false the pass2 loop skips positionRecordABaseOnly.
    private final boolean[] ordered2PassNeedsRecord;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction> unordered2PassFunctions;
    // True iff at least one unordered two-pass function reads the base Record in pass2 (precomputed).
    private final boolean unordered2PassNeedsRecord;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    // The window Map groups this factory's functions form, arranged by the traversal that
    // drives them, or null when they form none. Owned by this factory; the functions the
    // groups bind are owned as they always were.
    @Nullable
    private final CachedWindowMapGroups windowMapGroups;
    private ObjList<WindowFunction> allFunctions;
    private RecordCursorFactory base;
    private CachedWindowLightRecordCursor cursor;
    private boolean isClosed;
    // Keep-flag filter fusion (row-selecting mode): when enabled, the cursor runs the window compute
    // (buffer + pass1 + preparePass2) but SKIPS the per-row boolean pass2 write and emits ONLY the
    // rows the sole row-selecting window function keeps (see enableRowSelecting). Default off - the
    // normal path (materialize boolean, then a separate Filter) is byte-identical and untouched.
    private boolean rowSelecting;
    private WindowFunction selectingFunction;

    public CachedWindowLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes narrowChainTypes,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata,
            @NotNull IntList sourceMap,
            @Nullable CachedWindowMapGroups windowMapGroups
    ) {
        super(metadata);
        RecordArray narrowChain = null;
        ObjList<WindowSortBuffer> sortBuffers = null;
        DirectLongList baseRowIds = null;
        // Adopted before anything below can throw, so a failed construction frees the groups
        // through this factory's own close() rather than leaving them to the compiler's catch.
        this.windowMapGroups = windowMapGroups;
        try {
            this.base = base;
            this.orderedGroupCount = sortKeys.size();
            assert orderedGroupCount == orderedFunctions.size();
            this.orderedFunctions = orderedFunctions;
            narrowChain = new RecordArray(
                    narrowChainTypes,
                    null,
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowCacheMaxPagesResolved(),
                    configuration.getSqlWindowCacheMaxPagesConfigKey()
            );
            this.sortKeys = sortKeys;
            this.chainMetadata = chainMetadata;
            this.allFunctions = new ObjList<>();

            // Caller guarantees every group is encoded-sort-eligible; the LIGHT factory does not
            // accept the tree fallback (see SqlCodeGenerator's isAllGroupsEncodedEligible gate).
            sortBuffers = new ObjList<>(orderedGroupCount);
            for (int i = 0; i < orderedGroupCount; i++) {
                sortBuffers.add(new EncodedWindowSortBuffer(configuration, chainMetadata, sortKeys.getQuick(i)));
            }
            baseRowIds = new DirectLongList(
                    Math.max(configuration.getSqlWindowStorePageSize() / Long.BYTES, 1),
                    MemoryTag.NATIVE_DEFAULT,
                    // Lazy: reopen() allocates under the tracker bound by the first of().
                    true
            );
            this.cursor = new CachedWindowLightRecordCursor(
                    columnIndexes,
                    narrowChain,
                    sortBuffers,
                    sourceMap,
                    baseRowIds
            );
            narrowChain = null;
            sortBuffers = null;
            baseRowIds = null;

            ObjList<ObjList<WindowFunction>> orderedTmp = null;
            for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                allFunctions.addAll(functions);

                ObjList<WindowFunction> twoPassFunctions = null;
                for (int j = 0, k = functions.size(); j < k; j++) {
                    WindowFunction function = functions.getQuick(j);
                    if (function.getPassCount() > WindowFunction.ONE_PASS) {
                        if (twoPassFunctions == null) {
                            twoPassFunctions = new ObjList<>();
                        }
                        twoPassFunctions.add(function);
                    }
                }
                if (twoPassFunctions != null) {
                    if (orderedTmp == null) {
                        orderedTmp = new ObjList<>();
                    }
                    orderedTmp.extendAndSet(i, twoPassFunctions);
                }
            }

            ordered2PassFunctions = orderedTmp;
            if (orderedTmp != null) {
                ordered2PassNeedsRecord = new boolean[orderedTmp.size()];
                for (int i = 0, n = orderedTmp.size(); i < n; i++) {
                    ordered2PassNeedsRecord[i] = groupNeedsBaseRecord(orderedTmp.getQuiet(i));
                }
            } else {
                ordered2PassNeedsRecord = null;
            }

            ObjList<WindowFunction> unorderedTmp = null;
            ObjList<WindowFunction> forwardTmp = null;
            ObjList<WindowFunction> backwardTmp = null;
            if (unorderedFunctions != null) {
                allFunctions.addAll(unorderedFunctions);

                for (int i = 0, n = unorderedFunctions.size(); i < n; i++) {
                    WindowFunction function = unorderedFunctions.getQuick(i);
                    if (function.getPassCount() > WindowFunction.ONE_PASS) {
                        if (unorderedTmp == null) {
                            unorderedTmp = new ObjList<>();
                        }
                        unorderedTmp.add(function);
                    }
                    if (function.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.FORWARD) {
                        if (forwardTmp == null) {
                            forwardTmp = new ObjList<>();
                        }
                        forwardTmp.add(function);
                    } else {
                        if (backwardTmp == null) {
                            backwardTmp = new ObjList<>();
                        }
                        backwardTmp.add(function);
                    }
                }
            }
            this.unordered2PassFunctions = unorderedTmp;
            this.unordered2PassNeedsRecord = groupNeedsBaseRecord(unorderedTmp);
            this.forwardUnorderedFunctions = forwardTmp;
            this.backwardUnorderedFunctions = backwardTmp;

            this.unorderedFunctions = unorderedFunctions;
        } catch (Throwable th) {
            Misc.free(narrowChain);
            Misc.freeObjList(sortBuffers);
            Misc.free(baseRowIds);
            close();
            throw th;
        }
    }

    // Precompute a two-pass group's base-record need once (not per row): the pass2 loop can skip
    // the per-row random-access base re-read iff EVERY function in the group opts out via
    // WindowFunction.pass2NeedsBaseRecord(). A null/empty group needs no record. Correctness over
    // micro-opt: any single function that still reads the record forces repositioning for the group.
    private static boolean groupNeedsBaseRecord(@Nullable ObjList<WindowFunction> functions) {
        if (functions == null) {
            return false;
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (functions.getQuick(i).pass2NeedsBaseRecord()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean followedOrderByAdvice() {
        // A fused SUBSAMPLE replaces a filter, which forms an order-advice boundary. The base
        // may have followed an inner query's advice (e.g. ts DESC), not the post-sampling sort.
        return !rowSelecting && base.followedOrderByAdvice();
    }

    public ObjList<WindowFunction> getAllWindowFunctions() {
        return allFunctions;
    }

    @Override
    public String getBaseColumnName(int idx) {
        return chainMetadata.getColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    /**
     * Returns the sole window function iff this factory has EXACTLY one window function and it is a
     * row-selecting keep flag ({@link WindowFunction#isRowSelecting()}); otherwise {@code null}.
     * The keep-flag filter fusion in code generation uses this to decide whether the exact single
     * keep-flag shape is present before enabling {@link #enableRowSelecting(WindowFunction)}.
     */
    public WindowFunction getSingleRowSelectingFunction() {
        if (allFunctions != null && allFunctions.size() == 1) {
            final WindowFunction fn = allFunctions.getQuick(0);
            // Fuse ONLY the internal desugared SUBSAMPLE keep flag (isSubsampleKeepFlag). Its boolean
            // is guaranteed dropped by the outer projection, so skipping the per-row boolean write is
            // safe. A hand-written window query may also produce a row-selecting function but is never
            // marked; fusing it would zero out a projected keep boolean (all rows read false), so it
            // must stay on the Filter + CachedWindowLight path.
            if (fn.isRowSelecting() && fn.isSubsampleKeepFlag()) {
                return fn;
            }
        }
        return null;
    }

    /**
     * Switches this factory into row-selecting mode: the cursor fuses the keep-flag filter, skipping
     * the per-row boolean pass2 write and emitting only the rows the sole row-selecting window
     * function keeps. Must only be called after {@link #getSingleRowSelectingFunction()} returns
     * non-null (verified by the caller). Idempotent.
     */
    public void enableRowSelecting(WindowFunction selectingFunction) {
        // The caller (SqlCodeGenerator.tryFuseKeepFlagFilter) has already resolved and validated the
        // sole row-selecting keep-flag function via getSingleRowSelectingFunction(); take it directly
        // rather than recomputing behind an assert (a no-op under -da, which would leave a null
        // selectingFunction and NPE at cursor time). Guard defensively: a null here means the caller's
        // contract was violated, so stay on the normal boolean-materializing path.
        if (selectingFunction == null) {
            return;
        }
        this.selectingFunction = selectingFunction;
        this.rowSelecting = true;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            // free partial allocations under the still-bound per-query tracker on a failed open
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    /**
     * Returns the window Map groups this factory's functions form, or null when they form
     * none. A group compiled but left unbound - which is what
     * {@code cairo.sql.window.map.fusion.enabled} off produces - is still reported here.
     */
    public @Nullable CachedWindowMapGroups getWindowMapGroups() {
        return windowMapGroups;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        // Distinct node name in row-selecting mode so the fused plan (no separate Filter, keep flag
        // consumed) is visibly different from the normal materialize-boolean-then-Filter path.
        sink.type(rowSelecting ? "CachedWindowLightSelect" : "CachedWindowLight");

        boolean oldVal = sink.getUseBaseMetadata();
        try {
            if (orderedFunctions.size() > 0) {
                sink.attr("orderedFunctions");
                sink.val("[");

                sink.useBaseMetadata(true);

                for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                    if (i > 0) {
                        sink.val(',');
                    }
                    sink.val('[');

                    addSortKeys(sink, sortKeys.getQuick(i));

                    sink.val("] => [");
                    ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        if (j > 0) {
                            sink.val(',');
                        }
                        sink.val(functions.getQuick(j));
                    }

                    sink.val("]");
                }
                sink.val(']');
            }

            sink.optAttr("unorderedFunctions", unorderedFunctions, true);
        } finally {
            sink.useBaseMetadata(oldVal);
        }

        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    /**
     * Installs a test decorator before the first cursor opens. The decorator must preserve the
     * base factory's metadata and record-cursor contract and take ownership of it, including close().
     */
    @TestOnly
    public void wrapBaseFactory(UnaryOperator<RecordCursorFactory> decorator) {
        base = decorator.apply(base);
    }

    private void addSortKeys(PlanSink sink, IntList list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            int colIdx = list.get(i);
            int col = (colIdx > 0 ? colIdx : -colIdx) - 1;
            if (i > 0) {
                sink.val(", ");
            }
            sink.val(chainMetadata.getColumnName(col));
            if (colIdx < 0) {
                sink.val(" ").val("desc");
            }
        }
    }

    // Symmetric with the streaming WindowRecordCursor, whose AbstractVirtualFunctionRecordCursor
    // base notifies every function on cursor close. Window function args cache cursor-scoped
    // native state (e.g. json_extract's UTF-8 sink); cursorClosed() releases it and the next
    // execution's init() re-inflates it.
    private void cursorClosedFunctions() {
        for (int i = 0, n = allFunctions.size(); i < n; i++) {
            allFunctions.getQuick(i).cursorClosed();
        }
    }

    private void resetFunctions() {
        for (int i = 0, n = allFunctions.size(); i < n; i++) {
            allFunctions.getQuick(i).reset();
        }
    }

    @Override
    protected void _close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        final ObjList<WindowFunction> allFunctions = this.allFunctions;
        this.allFunctions = null;
        final RecordCursorFactory base = this.base;
        this.base = null;
        final CachedWindowLightRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        // Before the functions rather than after: a group owns only its own map and its key
        // projection over chain columns, so freeing it touches nothing a function owns - but
        // ordering it first keeps that independence obvious rather than incidental.
        failure = Misc.freeBestEffort(failure, windowMapGroups);
        failure = Misc.freeObjListBestEffort(failure, allFunctions);
        CairoException.rethrowCleanupFailure(failure);
    }

    class CachedWindowLightRecordCursor implements RecordCursor {
        private final DirectLongList baseRowIds;
        private final IntList columnIndexes;
        private final LightWindowSPI lightSpi;
        private final RecordArray narrowChain;
        private final WindowLightRecord recordA;
        private final WindowLightRecord recordB;
        // Dense row-selecting ordered traversal only: one bit per buffered row. Scanning it
        // ascending restores incoming order without sort scratch. Sparse selections instead
        // sort selectedRowIds using selectedTraversalRows as tracker-bound merge scratch.
        private final DirectLongList selectedRowBits;
        // Row-selecting mode only: ascending ABSOLUTE incoming-row indices emitted by this cursor.
        // Forward functions write directly here. Ordered/backward traversal ordinals use
        // selectedTraversalRows for translation before emission. Select-all uses neither list.
        private final DirectLongList selectedRowIds;
        // After sparse translation consumes the ordinals, this list doubles as merge scratch.
        private final DirectLongList selectedTraversalRows;
        private final ObjList<WindowSortBuffer> sortBuffers;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long currentRowIndex;
        private boolean isOpen;
        private boolean isSelectionAllRows;
        private boolean isWindowComputed;
        private long outputSize;
        private long size;

        CachedWindowLightRecordCursor(
                IntList columnIndexes,
                RecordArray narrowChain,
                ObjList<WindowSortBuffer> sortBuffers,
                IntList sourceMap,
                DirectLongList baseRowIds
        ) {
            this.columnIndexes = columnIndexes;
            this.narrowChain = narrowChain;
            this.sortBuffers = sortBuffers;
            this.baseRowIds = baseRowIds;
            this.recordA = new WindowLightRecord(sourceMap);
            this.recordB = new WindowLightRecord(sourceMap);
            this.lightSpi = new LightWindowSPI(sourceMap, narrowChain, baseRowIds);
            // Lazy (matches baseRowIds): reopen() under the tracker bound by the first of(). Only
            // allocated/used in row-selecting mode.
            this.selectedRowBits = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.selectedRowIds = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.selectedTraversalRows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            // Lazy: the first of() binds the tracker and reopens the chain, row-id lists,
            // sort buffers and window-function maps. Starting open would skip that first
            // reopen() and read a closed partition map.
            this.isOpen = false;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isWindowComputed) {
                computeWindow();
            }
            final long total = rowSelecting ? outputSize : size;
            counter.add(total - currentRowIndex);
            currentRowIndex = total;
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(narrowChain);
                Misc.free(baseRowIds);
                Misc.free(selectedRowBits);
                Misc.free(selectedRowIds);
                Misc.free(selectedTraversalRows);
                for (int i = 0, n = sortBuffers.size(); i < n; i++) {
                    Misc.free(sortBuffers.getQuick(i));
                }
                resetFunctions();
                cursorClosedFunctions();
                // Symmetric with the reopen in of(): each group hands its map backing back to
                // the tracker that was bound when it was allocated. Reached on a failed open
                // too, where a group that never got as far as reopen() frees a closed map.
                if (windowMapGroups != null) {
                    windowMapGroups.reset();
                }
                isSelectionAllRows = false;
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isWindowComputed) {
                computeWindow();
            }
            if (rowSelecting) {
                if (currentRowIndex < outputSize) {
                    // Emit only the kept rows: index the base at the selected absolute row id.
                    positionRecordA(isSelectionAllRows ? currentRowIndex : selectedRowIds.get(currentRowIndex));
                    currentRowIndex++;
                    return true;
                }
                return false;
            }
            if (currentRowIndex < size) {
                positionRecordA(currentRowIndex);
                currentRowIndex++;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public long preComputedStateSize() {
            return size;
        }

        @Override
        public void recordAt(Record record, long rowIndex) {
            // rowIndex is a getRowId() value, which for this cursor is the ABSOLUTE base-row index
            // (set via recordA.setRowIndex during iteration) in BOTH modes - not an output position -
            // so it positions directly, exactly as the non-selecting path does.
            if (record == recordA) {
                positionRecordA(rowIndex);
            } else {
                positionRecordB(rowIndex);
            }
        }

        @Override
        public long size() {
            if (rowSelecting) {
                // Mirror the FilteredRecordCursor contract this fusion replaces (size not advertised
                // ahead of iteration), so the fused query is size-identical, not just row-identical,
                // to the untouched Filter + CachedWindowLight path. calculateSize() still returns the
                // exact kept-row count.
                return -1;
            }
            return isWindowComputed ? size : -1;
        }

        @Override
        public void toTop() {
            currentRowIndex = 0;
        }

        private void computeWindow() {
            final Record baseRecord = baseCursor.getRecord();
            // recordA pre-positioned so encoded sort key encoders can read base columns through it.
            recordA.of(baseRecord, narrowChain.getRecord(), -1);

            final long baseSize = baseCursor.size();
            if (baseSize > 0) {
                baseRowIds.setCapacity(baseSize);
            }

            long rowIndex = 0;
            final boolean hasOrdered = orderedGroupCount > 0;
            final int forwardFnCount = forwardUnorderedFunctions != null ? forwardUnorderedFunctions.size() : 0;
            final ObjList<WindowMapState> forwardStates =
                    windowMapGroups != null ? windowMapGroups.getForwardUnorderedStates() : null;
            final int forwardStateCount = forwardStates != null ? forwardStates.size() : 0;
            if (hasOrdered || forwardFnCount > 0) {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    // Fused row-selecting mode: the sole function's boolean output is never
                    // materialized (see the "Row-selecting fusion" comment below) nor read back
                    // (positionRecordA/positionRecordB skip repositioning the chain too), so
                    // allocating a per-row narrow-chain slot here would be pure overhead. Skip it.
                    if (!rowSelecting) {
                        narrowChain.beginRecord();
                    }
                    baseRowIds.add(baseRecord.getRowId());
                    if (hasOrdered) {
                        for (int i = 0; i < orderedGroupCount; i++) {
                            sortBuffers.getQuick(i).put(recordA, rowIndex);
                        }
                    }
                    if (forwardFnCount > 0) {
                        recordA.setRowIndex(rowIndex);
                        // Groups first, and the whole of a group before any of it is read: a
                        // bound function's pass1 is a no-op computeNext followed by the write
                        // of what the group's projection loop has just materialized.
                        for (int g = 0; g < forwardStateCount; g++) {
                            forwardStates.getQuick(g).computeNext(recordA);
                        }
                        for (int j = 0; j < forwardFnCount; j++) {
                            forwardUnorderedFunctions.getQuick(j).pass1(recordA, rowIndex, lightSpi);
                        }
                    }
                    rowIndex++;
                }
                if (hasOrdered) {
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        sortBuffers.getQuick(i).finishPut(circuitBreaker);
                    }
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    if (!rowSelecting) {
                        narrowChain.beginRecord();
                    }
                    baseRowIds.add(baseRecord.getRowId());
                    rowIndex++;
                }
            }
            size = rowIndex;

            if (hasOrdered) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final WindowSortBuffer group = sortBuffers.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    final int functionCount = functions.size();
                    // This sort group's own Map subgroups: sharing a sort is not sharing a
                    // map, so a bucket holding several window specs drives one group each.
                    final ObjList<WindowMapState> states =
                            windowMapGroups != null ? windowMapGroups.getOrderedStates(i) : null;
                    final int stateCount = states != null ? states.size() : 0;
                    // Decide once per sort group, not per row. Map states read the record
                    // independently of the functions, so they always require positioning.
                    boolean isRecordRequired = stateCount > 0;
                    for (int j = 0; j < functionCount && !isRecordRequired; j++) {
                        isRecordRequired = functions.getQuick(j).isPass1RecordRequired();
                    }
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        long rIdx = group.next();
                        if (isRecordRequired) {
                            positionRecordABaseOnly(rIdx);
                        }
                        for (int g = 0; g < stateCount; g++) {
                            states.getQuick(g).computeNext(recordA);
                        }
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass1(recordA, rIdx, lightSpi);
                        }
                    }
                    if (ordered2PassFunctions == null || ordered2PassFunctions.getQuiet(i) == null) {
                        Misc.free(group);
                    }
                }
            }

            if (backwardUnorderedFunctions != null) {
                final int fnCount = backwardUnorderedFunctions.size();
                final ObjList<WindowMapState> backwardStates =
                        windowMapGroups != null ? windowMapGroups.getBackwardUnorderedStates() : null;
                final int backwardStateCount = backwardStates != null ? backwardStates.size() : 0;
                for (long rIdx = size - 1; rIdx >= 0; rIdx--) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    positionRecordABaseOnly(rIdx);
                    for (int g = 0; g < backwardStateCount; g++) {
                        backwardStates.getQuick(g).computeNext(recordA);
                    }
                    for (int j = 0; j < fnCount; j++) {
                        backwardUnorderedFunctions.getQuick(j).pass1(recordA, rIdx, lightSpi);
                    }
                }
            }

            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        functions.getQuick(j).preparePass2();
                    }
                }
            }
            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    unordered2PassFunctions.getQuick(j).preparePass2();
                }
            }

            // Row-selecting fusion: the function reports selected ordinals in its pass1 traversal.
            // Translate ordered traversal ordinals through the retained sort buffer to absolute
            // incoming rows. Sparse selections sort only those rows; dense selections use a bitset
            // scan. The forward branch validates its identity mapping as strictly ascending and
            // needs neither. This still skips the O(N) boolean pass2 write and downstream Filter.
            if (rowSelecting) {
                isSelectionAllRows = selectingFunction.isSelectionAllRows();
                if (isSelectionAllRows) {
                    outputSize = size;
                } else {
                    mapSelectedRows();
                    outputSize = selectedRowIds.size();
                }
            } else {
                if (ordered2PassFunctions != null) {
                    for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                        final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                        if (functions == null) {
                            continue;
                        }
                        final WindowSortBuffer group = sortBuffers.getQuick(i);
                        final int functionCount = functions.size();
                        // This sort group's two-pass Map subgroups, whose accumulators pass 1 left
                        // final. Driven per row before the pass2 loop, for the same reason the
                        // pass-1 loops drive theirs first: a bound function's pass2 is the write of
                        // what the group's projection loop has just materialized.
                        final ObjList<WindowMapState> states =
                                windowMapGroups != null ? windowMapGroups.getOrderedPass2States(i) : null;
                        final int stateCount = states != null ? states.size() : 0;
                        // Skip the per-row random-access base re-read entirely when no function in this
                        // group reads the base Record in pass2 (need-flag precomputed once in the ctor).
                        // A group's key projection reads base columns too, so any state here forces the
                        // positioning back on regardless of what the functions need.
                        final boolean needsRecord = ordered2PassNeedsRecord[i] || stateCount > 0;
                        group.toTop();
                        while (group.hasNext()) {
                            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                            long rIdx = group.next();
                            // pass2 reads only base columns through recordA and reads/writes its own
                            // output via spi.getAddress (position-independent), so narrow positioning
                            // would be wasted work over millions of rows. And when no function even reads
                            // the base Record, the base-only re-read itself is skipped.
                            if (needsRecord) {
                                positionRecordABaseOnly(rIdx);
                            }
                            for (int g = 0; g < stateCount; g++) {
                                states.getQuick(g).projectPass2(recordA);
                            }
                            for (int j = 0; j < functionCount; j++) {
                                functions.getQuick(j).pass2(recordA, rIdx, lightSpi);
                            }
                        }
                    }
                }

                if (unordered2PassFunctions != null) {
                    final int funcCount = unordered2PassFunctions.size();
                    final ObjList<WindowMapState> pass2States =
                            windowMapGroups != null ? windowMapGroups.getUnorderedPass2States() : null;
                    final int pass2StateCount = pass2States != null ? pass2States.size() : 0;
                    // Skip the per-row random-access base re-read entirely when no function reads the
                    // base Record in pass2 (need-flag precomputed once in the ctor). This is the hot
                    // keep-flag path (m4/minmax/lttb): pass2 drives off pass1's cached buffers only.
                    // A map-state projection reads base columns, so any state forces positioning on.
                    final boolean needsRecord = unordered2PassNeedsRecord || pass2StateCount > 0;
                    for (long rIdx = 0; rIdx < size; rIdx++) {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        // see the ordered pass2 loop: base-only positioning suffices here too.
                        if (needsRecord) {
                            positionRecordABaseOnly(rIdx);
                        }
                        for (int g = 0; g < pass2StateCount; g++) {
                            pass2States.getQuick(g).projectPass2(recordA);
                        }
                        for (int j = 0; j < funcCount; j++) {
                            unordered2PassFunctions.getQuick(j).pass2(recordA, rIdx, lightSpi);
                        }
                    }
                }
            }

            currentRowIndex = 0;
            isWindowComputed = true;
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            isWindowComputed = false;
            isSelectionAllRows = false;
            currentRowIndex = 0;
            size = 0;
            outputSize = 0;
            circuitBreaker = executionContext.getCircuitBreaker();
            narrowChain.clear();
            baseRowIds.clear();
            if (rowSelecting) {
                selectedRowBits.clear();
                selectedRowIds.clear();
                selectedTraversalRows.clear();
            }
            if (!isOpen) {
                isOpen = true;
                // Bind the per-query tracker on the narrow chain, row-id list, sort
                // buffers and window-function maps before their backing is allocated.
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                narrowChain.setMemoryTracker(memoryTracker);
                baseRowIds.setMemoryTracker(memoryTracker);
                baseRowIds.reopen();
                if (rowSelecting) {
                    selectedRowBits.setMemoryTracker(memoryTracker);
                    selectedRowBits.reopen();
                    selectedRowIds.setMemoryTracker(memoryTracker);
                    selectedRowIds.reopen();
                    selectedTraversalRows.setMemoryTracker(memoryTracker);
                    selectedTraversalRows.reopen();
                }
                reopenSortBuffers(memoryTracker);
                for (int i = 0, n = allFunctions.size(); i < n; i++) {
                    allFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                reopen(allFunctions);
                if (windowMapGroups != null) {
                    // After the functions and needing nothing from Function.init below: what
                    // this allocates is map backing, and nothing here evaluates a key. An
                    // expression-keyed group does read through compiled PARTITION BY terms,
                    // and they are a member function's own, borrowed - so the Function.init
                    // below binds them along with that function, well before the first
                    // traversal reads a row.
                    windowMapGroups.reopen(memoryTracker);
                }
            }
            recordA.of(baseCursor.getRecord(), narrowChain.getRecord(), -1);
            recordB.of(baseCursor.getRecordB(), narrowChain.getRecordB(), -1);
            lightSpi.of(baseCursor);
            Function.init(allFunctions, this, executionContext, null);
            final long expectedRows = baseCursor.size();
            for (int i = 0; i < orderedGroupCount; i++) {
                sortBuffers.getQuick(i).of(this, expectedRows);
            }
        }

        private void mapSelectedRows() {
            selectedRowIds.clear();
            selectedTraversalRows.clear();

            int orderedGroup = -1;
            for (int i = 0, n = orderedFunctions.size(); i < n && orderedGroup < 0; i++) {
                final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                for (int j = 0, k = functions.size(); j < k; j++) {
                    if (functions.getQuick(j) == selectingFunction) {
                        orderedGroup = i;
                        break;
                    }
                }
            }

            final boolean isForward = orderedGroup < 0 && containsFunction(forwardUnorderedFunctions, selectingFunction);
            selectingFunction.getSelectedRows(isForward ? selectedRowIds : selectedTraversalRows);
            if (orderedGroup >= 0) {
                // The LIGHT factory constructs only encoded sort buffers.
                final EncodedWindowSortBuffer group = (EncodedWindowSortBuffer) sortBuffers.getQuick(orderedGroup);
                final long selectedCount = selectedTraversalRows.size();
                if (selectedCount <= (size >>> SPARSE_SELECTION_DENSITY_SHIFT)) {
                    mapSparseSelectedRows(group, selectedCount);
                    return;
                }
                // The sorted traversal visits every buffered row exactly once, so the selected
                // absolute rows are distinct values in [0, size). Flagging each in a bitset and
                // scanning the words ascending is a linear-time sort whose only memory, one bit
                // per buffered row (at most 1/64 of baseRowIds), the tracker charges up front.
                final long wordCount = (size + 63) >>> 6;
                if (wordCount > 0) {
                    if (selectedRowBits.getCapacity() < wordCount) {
                        selectedRowBits.setCapacity(wordCount);
                    }
                    Vect.memset(selectedRowBits.getAddress(), wordCount << 3, 0);
                }
                // Translate only the selected ordinals; do not replay discarded sort entries.
                long prevOrdinal = -1;
                for (long selectedIndex = 0; selectedIndex < selectedCount; selectedIndex++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final long ordinal = selectedTraversalRows.get(selectedIndex);
                    if (ordinal < 0 || ordinal >= size) {
                        throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                    }
                    if (ordinal <= prevOrdinal) {
                        throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                    }
                    final long absoluteRow = group.getRowIdAt(ordinal);
                    if (absoluteRow < 0 || absoluteRow >= size) {
                        throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                    }
                    final long word = absoluteRow >>> 6;
                    final long bits = selectedRowBits.get(word);
                    final long mask = 1L << (absoluteRow & 63);
                    if ((bits & mask) != 0) {
                        throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                    }
                    selectedRowBits.set(word, bits | mask);
                    prevOrdinal = ordinal;
                }
                // Emit each flagged row once, ascending: word order times bit order.
                selectedRowIds.ensureCapacity(selectedCount);
                for (long word = 0; word < wordCount; word++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long bits = selectedRowBits.get(word);
                    while (bits != 0) {
                        selectedRowIds.add((word << 6) + Long.numberOfTrailingZeros(bits));
                        bits &= bits - 1;
                    }
                }
            } else if (isForward) {
                // Forward pass1 traversal ordinal == absolute buffered-row index, so this identity
                // mapping preserves the getSelectedRows contract order: strictly ascending ordinals
                // in, strictly ascending row ids out. Enforce that strictness in-loop (prevOrdinal
                // starts at -1 so ordinal 0 passes); nothing downstream re-sorts the list.
                long prevOrdinal = -1;
                for (long i = 0, n = selectedRowIds.size(); i < n; i++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final long traversalOrdinal = selectedRowIds.get(i);
                    if (traversalOrdinal < 0 || traversalOrdinal >= size) {
                        throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                    }
                    if (traversalOrdinal <= prevOrdinal) {
                        throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                    }
                    prevOrdinal = traversalOrdinal;
                }
            } else if (containsFunction(backwardUnorderedFunctions, selectingFunction)) {
                // Backward pass1 ordinal k is absolute row size-1-k, so walking the strictly
                // ascending ordinals from the end emits absolute rows ascending without a sort.
                long prevOrdinal = size;
                for (long i = selectedTraversalRows.size() - 1; i >= 0; i--) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final long traversalOrdinal = selectedTraversalRows.get(i);
                    if (traversalOrdinal < 0 || traversalOrdinal >= size) {
                        throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                    }
                    if (traversalOrdinal >= prevOrdinal) {
                        throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                    }
                    prevOrdinal = traversalOrdinal;
                    selectedRowIds.add(size - 1 - traversalOrdinal);
                }
            } else {
                throw CairoException.nonCritical().put("row-selecting function has no traversal group");
            }
        }

        private void mapSparseSelectedRows(EncodedWindowSortBuffer group, long selectedCount) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            selectedRowIds.ensureCapacity(selectedCount);
            long prevOrdinal = -1;
            for (long i = 0; i < selectedCount; i++) {
                if (i > 0 && (i & SELECTION_CHECK_MASK) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                final long ordinal = selectedTraversalRows.get(i);
                if (ordinal < 0 || ordinal >= size) {
                    throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                }
                if (ordinal <= prevOrdinal) {
                    throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                }
                final long absoluteRow = group.getRowIdAt(ordinal);
                if (absoluteRow < 0 || absoluteRow >= size) {
                    throw CairoException.nonCritical().put("row-selecting traversal index out of bounds");
                }
                selectedRowIds.add(absoluteRow);
                prevOrdinal = ordinal;
            }
            sortSparseSelectedRows(selectedCount);
        }

        private void sortSparseSelectedRows(long selectedCount) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            DirectLongList source = selectedRowIds;
            if (selectedCount <= SMALL_SELECTION_SORT_THRESHOLD) {
                // Bound the quadratic work: no scratch or merge setup for a handful of points.
                for (long i = 1; i < selectedCount; i++) {
                    final long row = selectedRowIds.get(i);
                    long j = i;
                    while (j > 0 && selectedRowIds.get(j - 1) > row) {
                        selectedRowIds.set(j, selectedRowIds.get(j - 1));
                        j--;
                    }
                    selectedRowIds.set(j, row);
                }
            } else {
                // Translation has consumed every traversal ordinal. Reuse that list's existing
                // K slots rather than allocating scratch (sortAsUnsigned mallocs an untracked
                // native copy at 600 rows). Neither list grows during this bottom-up merge sort.
                DirectLongList dest = selectedTraversalRows;
                for (long width = 1; width < selectedCount; width <<= 1) {
                    for (long lo = 0; lo < selectedCount; lo += width << 1) {
                        final long mid = Math.min(lo + width, selectedCount);
                        final long hi = Math.min(mid + width, selectedCount);
                        long left = lo;
                        long right = mid;
                        for (long i = lo; i < hi; i++) {
                            if ((i & SELECTION_CHECK_MASK) == 0) {
                                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                            }
                            if (right >= hi || (left < mid && source.get(left) <= source.get(right))) {
                                dest.set(i, source.get(left++));
                            } else {
                                dest.set(i, source.get(right++));
                            }
                        }
                    }
                    final DirectLongList tmp = source;
                    source = dest;
                    dest = tmp;
                }
            }
            // Preserve the bitmap path's uniqueness check. Copy back only when the last merge
            // wrote to scratch; selectedRowIds must always hold the final incoming-order rows.
            long prevRow = -1;
            for (long i = 0; i < selectedCount; i++) {
                if ((i & SELECTION_CHECK_MASK) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                final long row = source.get(i);
                if (row <= prevRow) {
                    throw CairoException.nonCritical().put("invalid row-selecting traversal order");
                }
                if (source != selectedRowIds) {
                    selectedRowIds.set(i, row);
                }
                prevRow = row;
            }
        }

        private boolean containsFunction(ObjList<WindowFunction> functions, WindowFunction target) {
            if (functions != null) {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    if (functions.getQuick(i) == target) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void positionRecordA(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecord(), baseRowIds.get(rowIndex));
            // Fused row-selecting mode never wrote a narrow-chain slot for any row (see
            // computeWindow's buffering loop), so repositioning into it here would read
            // unallocated/out-of-bounds native memory. Safe to skip: projected columns resolve
            // to base columns and the sole function's boolean output is dropped by the outer
            // projection (see getSingleRowSelectingFunction/tryFuseKeepFlagFilter).
            if (!rowSelecting) {
                narrowChain.recordAtRowIndex(narrowChain.getRecord(), rowIndex);
            }
            recordA.setRowIndex(rowIndex);
        }

        private void positionRecordABaseOnly(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecord(), baseRowIds.get(rowIndex));
            recordA.setRowIndex(rowIndex);
        }

        private void positionRecordB(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecordB(), baseRowIds.get(rowIndex));
            // See positionRecordA: no narrow-chain row exists in fused mode.
            if (!rowSelecting) {
                narrowChain.recordAtRowIndex(narrowChain.getRecordB(), rowIndex);
            }
            recordB.setRowIndex(rowIndex);
        }

        private void reopen(ObjList<?> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                if (list.getQuick(i) instanceof Reopenable r) {
                    r.reopen();
                }
            }
        }

        private void reopenSortBuffers(MemoryTracker memoryTracker) {
            for (int i = 0; i < orderedGroupCount; i++) {
                final WindowSortBuffer buffer = sortBuffers.getQuick(i);
                // Bind before reopen() so the first allocation is charged to the tracker.
                buffer.setMemoryTracker(memoryTracker);
                buffer.reopen();
            }
        }
    }
}
