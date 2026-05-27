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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordArray;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedWindowLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<WindowFunction> allFunctions;
    private final RecordCursorFactory base;
    private final GenericRecordMetadata chainMetadata;
    private final CachedWindowLightRecordCursor cursor;
    private final ObjList<ObjList<WindowFunction>> ordered2PassFunctions;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction> unordered2PassFunctions;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    private boolean closed = false;

    public CachedWindowLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes narrowChainTypes,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata,
            @NotNull IntList sourceMap
    ) {
        super(metadata);
        try {
            this.base = base;
            this.orderedGroupCount = comparators.size();
            assert orderedGroupCount == orderedFunctions.size();
            this.orderedFunctions = orderedFunctions;
            RecordArray narrowChain = new RecordArray(
                    narrowChainTypes,
                    null,
                    configuration.getSqlWindowTreeKeyPageSize(),
                    configuration.getSqlWindowTreeKeyMaxPages()
            );
            this.sortKeys = sortKeys;
            this.chainMetadata = chainMetadata;

            ObjList<WindowSortBuffer> sortBuffers = new ObjList<>(orderedGroupCount);
            try {
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
            } catch (Throwable t) {
                Misc.freeObjList(sortBuffers);
                Misc.free(narrowChain);
                throw t;
            }

            this.cursor = new CachedWindowLightRecordCursor(
                    columnIndexes,
                    narrowChain,
                    sortBuffers,
                    sourceMap
            );
            this.allFunctions = new ObjList<>();

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
                }
            }
            this.unordered2PassFunctions = unorderedTmp;

            this.unorderedFunctions = unorderedFunctions;
        } catch (Throwable th) {
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
        cursor.of(baseCursor, executionContext);
        return cursor;
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
        if (closed) {
            return;
        }
        closed = true;
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(allFunctions);
    }

    class CachedWindowLightRecordCursor implements RecordCursor {
        private final IntList columnIndexes;
        private final LightWindowSPI lightSpi;
        private final RecordArray narrowChain;
        private final ObjList<WindowSortBuffer> sortBuffers;
        private final WindowLightRecord recordA;
        private final WindowLightRecord recordB;
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
                IntList sourceMap
        ) {
            this.columnIndexes = columnIndexes;
            this.narrowChain = narrowChain;
            this.sortBuffers = sortBuffers;
            this.recordA = new WindowLightRecord(sourceMap);
            this.recordB = new WindowLightRecord(sourceMap);
            this.lightSpi = new LightWindowSPI(sourceMap, narrowChain);
            this.isOpen = true;
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
            // Pre-position recordA so encoded sort key encoders / tree comparators
            // can read sort columns (all in the base range) through it.
            recordA.of(baseRecord, narrowChain.getRecord(), -1);

            // Step 1: scan base, record (baseRowId, narrow slot) per row, feed
            // each row into every ordered group's sorter.
            long rowIndex = 0;
            if (orderedGroupCount > 0) {
                while (baseCursor.hasNext()) {
                    narrowChain.beginRecord();
                    narrowChain.putLong(baseRecord.getRowId());
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        sortBuffers.getQuick(i).put(recordA, rowIndex);
                    }
                    rowIndex++;
                }
                for (int i = 0; i < orderedGroupCount; i++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    sortBuffers.getQuick(i).finishPut(circuitBreaker);
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    narrowChain.beginRecord();
                    narrowChain.putLong(baseRecord.getRowId());
                    rowIndex++;
                }
            }
            size = rowIndex;

            // Step 2: pass1 for ordered functions, in sorted order per group.
            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final WindowSortBuffer group = sortBuffers.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    final int functionCount = functions.size();
                    group.toTop();
                    while (group.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        long rIdx = group.next();
                        positionRecordA(rIdx);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass1(recordA, rIdx, lightSpi);
                        }
                    }
                    if (ordered2PassFunctions == null || ordered2PassFunctions.getQuiet(i) == null) {
                        Misc.free(group);
                    }
                }
            }

            // Pass1 for unordered functions, in base order.
            if (unorderedFunctions != null) {
                for (int j = 0, n = unorderedFunctions.size(); j < n; j++) {
                    final WindowFunction f = unorderedFunctions.getQuick(j);
                    if (f.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.FORWARD) {
                        for (long rIdx = 0; rIdx < size; rIdx++) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            positionRecordA(rIdx);
                            f.pass1(recordA, rIdx, lightSpi);
                        }
                    } else {
                        for (long rIdx = size - 1; rIdx >= 0; rIdx--) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            positionRecordA(rIdx);
                            f.pass1(recordA, rIdx, lightSpi);
                        }
                    }
                }
            }

            // Pass2 preparation.
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

            // Pass2 for ordered functions.
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
                        positionRecordA(rIdx);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass2(recordA, rIdx, lightSpi);
                        }
                    }
                }
            }

            // Pass2 for unordered functions.
            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    final WindowFunction f = unordered2PassFunctions.getQuick(j);
                    for (long rIdx = 0; rIdx < size; rIdx++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        positionRecordA(rIdx);
                        f.pass2(recordA, rIdx, lightSpi);
                    }
                }
            }

            currentRowIndex = 0;
            isWindowComputed = true;
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            isWindowComputed = false;
            currentRowIndex = 0;
            size = 0;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                reopenSortBuffers();
                reopen(allFunctions);
            }
            recordA.of(baseCursor.getRecord(), narrowChain.getRecord(), -1);
            recordB.of(baseCursor.getRecordB(), narrowChain.getRecordB(), -1);
            lightSpi.of(baseCursor);
            Function.init(allFunctions, this, executionContext, null);
            for (int i = 0; i < orderedGroupCount; i++) {
                sortBuffers.getQuick(i).of(this);
            }
        }

        private void positionRecordA(long rowIndex) {
            final Record narrow = narrowChain.getRecord();
            narrowChain.recordAtRowIndex(narrow, rowIndex);
            baseCursor.recordAt(baseCursor.getRecord(), narrow.getLong(0));
            recordA.setRowIndex(rowIndex);
        }

        private void positionRecordB(long rowIndex) {
            final Record narrow = narrowChain.getRecordB();
            narrowChain.recordAtRowIndex(narrow, rowIndex);
            baseCursor.recordAt(baseCursor.getRecordB(), narrow.getLong(0));
            recordB.setRowIndex(rowIndex);
        }

        private void reopen(ObjList<?> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                if (list.getQuick(i) instanceof Reopenable) {
                    ((Reopenable) list.getQuick(i)).reopen();
                }
            }
        }

        private void reopenSortBuffers() {
            for (int i = 0; i < orderedGroupCount; i++) {
                sortBuffers.getQuick(i).reopen();
            }
        }
    }
}
