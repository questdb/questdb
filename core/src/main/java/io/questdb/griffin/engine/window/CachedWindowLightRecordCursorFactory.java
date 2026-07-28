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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedWindowLightRecordCursorFactory extends AbstractRecordCursorFactory {
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
    private CachedWindowLightRecordCursor cursor;
    private boolean isClosed;

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
            @NotNull IntList sourceMap
    ) {
        super(metadata);
        RecordArray narrowChain = null;
        ObjList<WindowSortBuffer> sortBuffers = null;
        DirectLongList baseRowIds = null;
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
            Misc.free(narrowChain);
            Misc.freeObjList(sortBuffers);
            Misc.free(baseRowIds);
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
        sink.type("CachedWindowLight");

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
        final CachedWindowLightRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
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
        private final ObjList<WindowSortBuffer> sortBuffers;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long currentRowIndex;
        private boolean isOpen;
        private boolean isWindowComputed;
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
            // Lazy: the first of() binds the tracker and reopens the chain, row-id list,
            // sort buffers and window-function maps. Starting open would skip that first
            // reopen() and read a closed partition map.
            this.isOpen = false;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isWindowComputed) {
                computeWindow();
            }
            counter.add(size - currentRowIndex);
            currentRowIndex = size;
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(narrowChain);
                Misc.free(baseRowIds);
                for (int i = 0, n = sortBuffers.size(); i < n; i++) {
                    Misc.free(sortBuffers.getQuick(i));
                }
                resetFunctions();
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
            if (record == recordA) {
                positionRecordA(rowIndex);
            } else {
                positionRecordB(rowIndex);
            }
        }

        @Override
        public long size() {
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
            if (hasOrdered || forwardFnCount > 0) {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    narrowChain.beginRecord();
                    baseRowIds.add(baseRecord.getRowId());
                    if (hasOrdered) {
                        for (int i = 0; i < orderedGroupCount; i++) {
                            sortBuffers.getQuick(i).put(recordA, rowIndex);
                        }
                    }
                    if (forwardFnCount > 0) {
                        recordA.setRowIndex(rowIndex);
                        for (int j = 0; j < forwardFnCount; j++) {
                            forwardUnorderedFunctions.getQuick(j).pass1(recordA, rowIndex, lightSpi);
                        }
                    }
                    rowIndex++;
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
                    narrowChain.beginRecord();
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
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        long rIdx = group.next();
                        positionRecordABaseOnly(rIdx);
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
                for (long rIdx = size - 1; rIdx >= 0; rIdx--) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    positionRecordABaseOnly(rIdx);
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
                        long rIdx = group.next();
                        // pass2 reads only base columns through recordA and reads/writes its own
                        // output via spi.getAddress (position-independent), so narrow positioning
                        // would be wasted work over millions of rows.
                        positionRecordABaseOnly(rIdx);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass2(recordA, rIdx, lightSpi);
                        }
                    }
                }
            }

            if (unordered2PassFunctions != null) {
                final int funcCount = unordered2PassFunctions.size();
                for (long rIdx = 0; rIdx < size; rIdx++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    // see the ordered pass2 loop: base-only positioning suffices here too.
                    positionRecordABaseOnly(rIdx);
                    for (int j = 0; j < funcCount; j++) {
                        unordered2PassFunctions.getQuick(j).pass2(recordA, rIdx, lightSpi);
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
            currentRowIndex = 0;
            size = 0;
            circuitBreaker = executionContext.getCircuitBreaker();
            narrowChain.clear();
            baseRowIds.clear();
            if (!isOpen) {
                isOpen = true;
                // Bind the per-query tracker on the narrow chain, row-id list, sort
                // buffers and window-function maps before their backing is allocated.
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                narrowChain.setMemoryTracker(memoryTracker);
                baseRowIds.setMemoryTracker(memoryTracker);
                baseRowIds.reopen();
                reopenSortBuffers(memoryTracker);
                for (int i = 0, n = allFunctions.size(); i < n; i++) {
                    allFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                reopen(allFunctions);
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

        private void positionRecordA(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecord(), baseRowIds.get(rowIndex));
            narrowChain.recordAtRowIndex(narrowChain.getRecord(), rowIndex);
            recordA.setRowIndex(rowIndex);
        }

        private void positionRecordABaseOnly(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecord(), baseRowIds.get(rowIndex));
            recordA.setRowIndex(rowIndex);
        }

        private void positionRecordB(long rowIndex) {
            baseCursor.recordAt(baseCursor.getRecordB(), baseRowIds.get(rowIndex));
            narrowChain.recordAtRowIndex(narrowChain.getRecordB(), rowIndex);
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
