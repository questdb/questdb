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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedWindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<WindowFunction> backwardUnorderedFunctions;
    private final GenericRecordMetadata chainMetadata;
    private final ObjList<WindowFunction> forwardUnorderedFunctions;
    private final ObjList<ObjList<WindowFunction>> ordered2PassFunctions;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction> unordered2PassFunctions;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    private ObjList<WindowFunction> allFunctions;
    private RecordCursorFactory base;
    private CachedWindowRecordCursor cursor;
    private boolean isClosed;

    public CachedWindowRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes chainTypes,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata
    ) {
        super(metadata);
        RecordArray recordChain = null;
        ObjList<WindowSortBuffer> sortBuffers = null;
        try {
            this.base = base;
            this.orderedGroupCount = comparators.size();
            assert orderedGroupCount == orderedFunctions.size();
            this.orderedFunctions = orderedFunctions;
            recordChain = new RecordArray(
                    chainTypes,
                    recordSink,
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowCacheMaxPagesResolved(),
                    configuration.getSqlWindowCacheMaxPagesConfigKey()
            );
            this.sortKeys = sortKeys;
            this.chainMetadata = chainMetadata;
            this.allFunctions = new ObjList<>();

            sortBuffers = new ObjList<>(orderedGroupCount);
            for (int i = 0; i < orderedGroupCount; i++) {
                final IntList groupKeys = sortKeys.getQuick(i);
                if (comparators.getQuiet(i) == null) {
                    sortBuffers.add(new EncodedWindowSortBuffer(configuration, chainMetadata, groupKeys));
                } else {
                    sortBuffers.add(new TreeWindowSortBuffer(
                            configuration,
                            comparators.getQuick(i),
                            SortKeyEncoder.createRankMaps(chainMetadata, groupKeys)
                    ));
                }
            }
            this.cursor = new CachedWindowRecordCursor(columnIndexes, recordChain, sortBuffers);
            recordChain = null;
            sortBuffers = null;

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
            this.forwardUnorderedFunctions = forwardTmp;
            this.backwardUnorderedFunctions = backwardTmp;

            this.unorderedFunctions = unorderedFunctions;
        } catch (Throwable th) {
            Misc.free(recordChain);
            Misc.freeObjList(sortBuffers);
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return chainMetadata.getColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
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

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CachedWindow");

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
        final CachedWindowRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeObjListBestEffort(failure, allFunctions);
        CairoException.rethrowCleanupFailure(failure);
    }

    class CachedWindowRecordCursor implements RecordCursor {
        private final IntList columnIndexes;
        private final RecordArray recordChain;
        private final ObjList<WindowSortBuffer> sortBuffers;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private boolean isRecordChainBuilt;
        private long recordChainOffset;

        public CachedWindowRecordCursor(IntList columnIndexes, RecordArray recordChain, ObjList<WindowSortBuffer> sortBuffers) {
            this.columnIndexes = columnIndexes;
            this.recordChain = recordChain;
            this.recordChain.setSymbolTableResolver(this);
            // Lazy: the first of() binds the MemoryTracker on each sort buffer and
            // reopens it, so the per-query counter charges every byte symmetrically.
            this.isOpen = false;
            this.sortBuffers = sortBuffers;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            recordChain.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(recordChain);
                for (int i = 0, n = sortBuffers.size(); i < n; i++) {
                    Misc.free(sortBuffers.getQuick(i));
                }
                resetFunctions();
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            return recordChain.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public long preComputedStateSize() {
            return recordChain.size();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return isRecordChainBuilt ? recordChain.size() : -1;
        }

        @Override
        public void toTop() {
            recordChain.toTop();
        }

        private void buildRecordChain() {
            // Consult the breaker before building, so even an empty base scan still observes cancellation.
            // Runs once per cursor open, guarded by isRecordChainBuilt.
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            final Record record = baseCursor.getRecord();
            final Record chainRecord = recordChain.getRecord();
            final boolean hasOrdered = orderedGroupCount > 0;
            final int forwardFnCount = forwardUnorderedFunctions != null ? forwardUnorderedFunctions.size() : 0;
            if (hasOrdered || forwardFnCount > 0) {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record);
                    recordChain.recordAt(chainRecord, recordChainOffset);
                    if (hasOrdered) {
                        for (int i = 0; i < orderedGroupCount; i++) {
                            sortBuffers.getQuick(i).put(chainRecord, recordChainOffset);
                        }
                    }
                    for (int j = 0; j < forwardFnCount; j++) {
                        forwardUnorderedFunctions.getQuick(j).pass1(chainRecord, recordChainOffset, recordChain);
                    }
                }
                if (hasOrdered) {
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        sortBuffers.getQuick(i).finishPut(circuitBreaker);
                    }
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record);
                }
            }

            long offset;
            if (hasOrdered) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final WindowSortBuffer group = sortBuffers.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    final int functionCount = functions.size();
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = group.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass1(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            if (backwardUnorderedFunctions != null) {
                final int fnCount = backwardUnorderedFunctions.size();
                recordChain.toBottom();
                while (recordChain.hasPrev()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final long rowId = chainRecord.getRowId();
                    for (int j = 0; j < fnCount; j++) {
                        backwardUnorderedFunctions.getQuick(j).pass1(chainRecord, rowId, recordChain);
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

            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    final WindowSortBuffer group = sortBuffers.getQuick(i);
                    final int functionCount = functions.size();
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = group.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass2(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            if (unordered2PassFunctions != null) {
                final int fnCount = unordered2PassFunctions.size();
                recordChain.toTop();
                while (recordChain.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final long rowId = chainRecord.getRowId();
                    for (int j = 0; j < fnCount; j++) {
                        unordered2PassFunctions.getQuick(j).pass2(chainRecord, rowId, recordChain);
                    }
                }
            }

            recordChain.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            isRecordChainBuilt = false;
            recordChainOffset = -1;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                recordChain.setSymbolTableResolver(this);
                // Bind the per-query tracker on the record store, sort buffers and each
                // window function's map before their backing is allocated under it.
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                recordChain.setMemoryTracker(memoryTracker);
                reopenSortBuffers(memoryTracker);
                for (int i = 0, n = allFunctions.size(); i < n; i++) {
                    allFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                reopen(allFunctions);
            }
            Function.init(allFunctions, this, executionContext, null);
            final long expectedRows = baseCursor.size();
            for (int i = 0; i < orderedGroupCount; i++) {
                sortBuffers.getQuick(i).of(this, expectedRows);
            }
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
