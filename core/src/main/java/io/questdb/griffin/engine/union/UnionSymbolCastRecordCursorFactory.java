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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Re-exposes STRING-downcast all-SYMBOL UNION columns as SYMBOL without routing unrelated
 * columns through a VirtualRecord. Native source keys are translated into the merged result
 * dictionary once per source dictionary and then served from a query-accounted native cache.
 */
public class UnionSymbolCastRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final IntList columnToFunctionIndex;
    private final UnionSymbolCastRecordCursor cursor;
    private final ObjList<Function> functions;

    public UnionSymbolCastRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            IntList columnToFunctionIndex,
            ObjList<Function> functions
    ) {
        super(metadata);
        this.base = base;
        this.columnToFunctionIndex = columnToFunctionIndex;
        this.functions = functions;
        this.cursor = new UnionSymbolCastRecordCursor(columnToFunctionIndex, functions);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    public ObjList<Function> getFunctions() {
        return functions;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            Function.init(functions, baseCursor, executionContext, null);
            cursor.of(baseCursor, executionContext.getMemoryTracker());
            return cursor;
        } catch (Throwable th) {
            try {
                Misc.free(baseCursor);
            } finally {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getQuick(i).cursorClosed();
                }
            }
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        // Retain the established plan shape while the implementation uses a symbol-only record.
        sink.type("VirtualRecord");
        sink.attr("functions").val('[');
        for (int i = 0, n = columnToFunctionIndex.size(); i < n; i++) {
            if (i > 0) {
                sink.val(',');
            }
            sink.putColumnName(i);
            if (columnToFunctionIndex.getQuick(i) > -1) {
                sink.val("::symbol");
            }
        }
        sink.val(']');
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

    @Override
    protected void _close() {
        Misc.freeObjList(functions);
        Misc.free(base);
    }

    private static class KeyValueSymbolTable implements SymbolTable {
        private final SymbolTable delegate;

        private KeyValueSymbolTable(SymbolTable delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean supportsKeyValueAccess() {
            return true;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return delegate.valueBOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return delegate.valueOf(key);
        }
    }

    private static class NativeKeyMap implements QuietCloseable {
        private static final int NOT_FOUND = -1;
        private final DirectIntIntHashMap map = new DirectIntIntHashMap(
                4,
                0.5,
                SymbolTable.VALUE_IS_NULL,
                NOT_FOUND,
                MemoryTag.NATIVE_FUNC_RSS,
                false
        );

        @Override
        public void close() {
            map.close();
            map.setMemoryTracker(null);
        }

        private int get(int sourceKey) {
            return map.isOpen() ? map.get(sourceKey) : NOT_FOUND;
        }

        private void of(MemoryTracker memoryTracker) {
            map.setMemoryTracker(memoryTracker);
        }

        private void put(int sourceKey, int resultKey) {
            if (sourceKey < 0) {
                throw CairoException.nonCritical().put("invalid union symbol key [key=").put(sourceKey).put(']');
            }
            if (!map.isOpen()) {
                map.reopen();
            }
            map.put(sourceKey, resultKey);
        }
    }

    private static class SourceColumn implements QuietCloseable {
        private final NativeKeyMap keyMap = new NativeKeyMap();
        @Nullable
        private final SymbolTable symbolTable;

        private SourceColumn(@Nullable SymbolTable symbolTable, MemoryTracker memoryTracker) {
            this.symbolTable = symbolTable;
            keyMap.of(memoryTracker);
        }

        @Override
        public void close() {
            keyMap.close();
        }
    }

    private static class SourceState implements QuietCloseable {
        private final ObjList<SourceColumn> columns;
        private final Record record;

        private SourceState(
                RecordCursor sourceCursor,
                IntList symbolColumns,
                MemoryTracker memoryTracker
        ) {
            this.record = sourceCursor.getRecord();
            this.columns = new ObjList<>(symbolColumns.size());
            try {
                for (int i = 0, n = symbolColumns.size(); i < n; i++) {
                    SymbolTable symbolTable = null;
                    try {
                        symbolTable = sourceCursor.getSymbolTable(symbolColumns.getQuick(i));
                        if (symbolTable instanceof SymbolFunction symbolFunction) {
                            final StaticSymbolTable staticSymbolTable = symbolFunction.getStaticSymbolTable();
                            if (staticSymbolTable != null) {
                                symbolTable = staticSymbolTable;
                            }
                        }
                        if (symbolTable == null || !symbolTable.supportsKeyValueAccess()) {
                            symbolTable = null;
                        }
                    } catch (UnsupportedOperationException ignored) {
                        // Dynamic expressions and cursors without symbol tables use text fallback.
                    }
                    columns.add(new SourceColumn(symbolTable, memoryTracker));
                }
            } catch (RuntimeException | Error th) {
                Misc.freeObjListIfCloseable(columns);
                throw th;
            }
        }

        @Override
        public void close() {
            Misc.freeObjListIfCloseable(columns);
        }
    }

    private static class UnionSymbolCastRecord extends UnionRecord {
        private final IntList columnToFunctionIndex;
        private final UnionSymbolCastRecordCursor cursor;
        private final ObjList<Function> functions;

        private UnionSymbolCastRecord(
                IntList columnToFunctionIndex,
                UnionSymbolCastRecordCursor cursor,
                ObjList<Function> functions
        ) {
            this.columnToFunctionIndex = columnToFunctionIndex;
            this.cursor = cursor;
            this.functions = functions;
        }

        @Override
        public int getInt(int col) {
            final int functionIndex = columnToFunctionIndex.getQuick(col);
            if (functionIndex < 0) {
                return super.getInt(col);
            }
            final CastStrToSymbolFunctionFactory.Func function = symbolFunction(functions.getQuick(functionIndex));
            final SourceState sourceState = cursor.getCurrentSourceState();
            final SourceColumn sourceColumn = sourceState.columns.getQuick(functionIndex);
            if (sourceColumn.symbolTable == null) {
                return function.intern(function.getSymbol(recordA));
            }

            final int sourceKey = sourceState.record.getInt(col);
            if (sourceKey == SymbolTable.VALUE_IS_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            int resultKey = sourceColumn.keyMap.get(sourceKey);
            if (resultKey == NativeKeyMap.NOT_FOUND) {
                resultKey = function.intern(sourceColumn.symbolTable.valueOf(sourceKey));
                sourceColumn.keyMap.put(sourceKey, resultKey);
            }
            return resultKey;
        }

        @Override
        public CharSequence getSymA(int col) {
            final int functionIndex = columnToFunctionIndex.getQuick(col);
            return functionIndex < 0
                    ? recordA.getSymA(col)
                    : symbolFunction(functions.getQuick(functionIndex)).getSymbol(recordA);
        }

        @Override
        public CharSequence getSymB(int col) {
            final int functionIndex = columnToFunctionIndex.getQuick(col);
            return functionIndex < 0
                    ? recordA.getSymB(col)
                    : symbolFunction(functions.getQuick(functionIndex)).getSymbolB(recordA);
        }

        private void of(Record baseRecord) {
            super.of(baseRecord, null);
            super.setAb(true);
        }
    }

    private static class UnionSymbolCastRecordCursor implements NoRandomAccessRecordCursor {
        private final IntList columnToFunctionIndex;
        private final ObjList<Function> functions;
        private final UnionSymbolCastRecord record;
        private final UnionSymbolSourceCursor.SymbolSourceTracker sourceTracker = new UnionSymbolSourceCursor.SymbolSourceTracker();
        private final IntList symbolColumns = new IntList();
        private final ObjList<SymbolTable> symbolTables = new ObjList<>();
        private RecordCursor baseCursor;
        private int currentSourceIndex = -1;
        private SourceState currentSourceState;
        private MemoryTracker memoryTracker;
        @Nullable
        private IntObjHashMap<SourceState> sourceStates;

        private UnionSymbolCastRecordCursor(
                IntList columnToFunctionIndex,
                ObjList<Function> functions
        ) {
            this.columnToFunctionIndex = columnToFunctionIndex;
            this.functions = functions;
            this.record = new UnionSymbolCastRecord(columnToFunctionIndex, this, functions);
            for (int column = 0, n = columnToFunctionIndex.size(); column < n; column++) {
                final int functionIndex = columnToFunctionIndex.getQuick(column);
                if (functionIndex > -1) {
                    symbolColumns.add(column);
                    symbolTables.add(new KeyValueSymbolTable(symbolFunction(functions.getQuick(functionIndex))));
                }
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            try {
                closeSourceStates();
            } finally {
                try {
                    baseCursor = Misc.free(baseCursor);
                } finally {
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getQuick(i).cursorClosed();
                    }
                    currentSourceIndex = -1;
                    currentSourceState = null;
                    memoryTracker = null;
                    sourceTracker.clear();
                }
            }
        }

        @Override
        public void expectLimitedIteration() {
            baseCursor.expectLimitedIteration();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            final int functionIndex = columnToFunctionIndex.getQuick(columnIndex);
            return functionIndex < 0
                    ? baseCursor.getSymbolTable(columnIndex)
                    : symbolTables.getQuick(functionIndex);
        }

        @Override
        public boolean hasNext() {
            return baseCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int functionIndex = columnToFunctionIndex.getQuick(columnIndex);
            if (functionIndex < 0) {
                return baseCursor.newSymbolTable(columnIndex);
            }
            final CastStrToSymbolFunctionFactory.Func function = symbolFunction(functions.getQuick(functionIndex));
            SymbolTable symbolTable = function.newSymbolTable();
            if (symbolTable == null) {
                symbolTable = function;
            }
            return new KeyValueSymbolTable(symbolTable);
        }

        private void of(RecordCursor baseCursor, MemoryTracker memoryTracker) {
            this.baseCursor = baseCursor;
            this.memoryTracker = memoryTracker;
            if (baseCursor instanceof UnionSymbolSourceCursor sourceCursor) {
                sourceCursor.bindSymbolSourceTracker(sourceTracker, 0);
            }
            this.record.of(baseCursor.getRecord());
            toTop();
        }

        @Override
        public long preComputedStateSize() {
            return baseCursor.preComputedStateSize();
        }

        @Override
        public void setParentUsedColumns(@Nullable IntHashSet columnIndexes) {
            baseCursor.setParentUsedColumns(columnIndexes);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            baseCursor.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            currentSourceIndex = -1;
            currentSourceState = null;
            sourceTracker.clear();
            if (baseCursor instanceof UnionSymbolSourceCursor sourceCursor) {
                sourceCursor.updateSymbolSource();
            } else {
                sourceTracker.of(baseCursor, 0);
            }
        }

        private void closeSourceStates() {
            if (sourceStates != null) {
                final Object[] states = sourceStates.getValues();
                for (int i = 0, n = states.length; i < n; i++) {
                    states[i] = Misc.free((SourceState) states[i]);
                }
                sourceStates.clear();
            }
        }

        private SourceState getCurrentSourceState() {
            final int sourceIndex = sourceTracker.getSourceIndex();
            if (sourceIndex == currentSourceIndex) {
                return currentSourceState;
            }
            currentSourceIndex = sourceIndex;
            if (sourceStates == null) {
                sourceStates = new IntObjHashMap<>();
            }
            currentSourceState = sourceStates.get(sourceIndex);
            if (currentSourceState == null) {
                final SourceState state = new SourceState(sourceTracker.getCursor(), symbolColumns, memoryTracker);
                try {
                    sourceStates.put(sourceIndex, state);
                    currentSourceState = state;
                } catch (RuntimeException | Error th) {
                    currentSourceState = null;
                    state.close();
                    throw th;
                }
            }
            return currentSourceState;
        }
    }

    private static CastStrToSymbolFunctionFactory.Func symbolFunction(Function function) {
        return (CastStrToSymbolFunctionFactory.Func) function;
    }
}
