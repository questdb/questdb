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
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
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
    private final ObjList<Plannable> planColumns;

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
        this.planColumns = new ObjList<>(metadata.getColumnCount());
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            planColumns.add(new ProjectedColumn(i, columnToFunctionIndex.getQuick(i) > -1));
        }
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
        sink.optAttr("functions", planColumns);
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
        private long address;
        private int capacity;
        private MemoryTracker memoryTracker;

        @Override
        public void close() {
            address = Unsafe.free(
                    address,
                    (long) capacity * Integer.BYTES,
                    MemoryTag.NATIVE_FUNC_RSS,
                    memoryTracker
            );
            capacity = 0;
            memoryTracker = null;
        }

        private int get(int sourceKey) {
            if (sourceKey < 0 || sourceKey >= capacity) {
                return NOT_FOUND;
            }
            return Unsafe.getInt(address + (long) sourceKey * Integer.BYTES);
        }

        private void of(MemoryTracker memoryTracker) {
            this.memoryTracker = memoryTracker;
        }

        private void put(int sourceKey, int resultKey) {
            if (sourceKey < 0) {
                throw CairoException.nonCritical().put("invalid union symbol key [key=").put(sourceKey).put(']');
            }
            if (sourceKey >= capacity) {
                int newCapacity = Math.max(4, capacity);
                while (newCapacity <= sourceKey) {
                    if (newCapacity > Integer.MAX_VALUE / 2) {
                        throw CairoException.nonCritical().put("union symbol key cache capacity overflow");
                    }
                    newCapacity *= 2;
                }
                final long oldSize = (long) capacity * Integer.BYTES;
                final long newSize = (long) newCapacity * Integer.BYTES;
                if (address == 0) {
                    address = Unsafe.malloc(newSize, MemoryTag.NATIVE_FUNC_RSS, memoryTracker);
                } else {
                    address = Unsafe.realloc(
                            address,
                            oldSize,
                            newSize,
                            MemoryTag.NATIVE_FUNC_RSS,
                            memoryTracker
                    );
                }
                Unsafe.setMemory(address + oldSize, newSize - oldSize, (byte) 0xff);
                capacity = newCapacity;
            }
            Unsafe.putInt(address + (long) sourceKey * Integer.BYTES, resultKey);
        }
    }

    private static class ProjectedColumn implements Plannable {
        private final int columnIndex;
        private final boolean symbolCast;

        private ProjectedColumn(int columnIndex, boolean symbolCast) {
            this.columnIndex = columnIndex;
            this.symbolCast = symbolCast;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
            if (symbolCast) {
                sink.val("::symbol");
            }
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
        private final RecordCursor sourceCursor;

        private SourceState(
                RecordCursor sourceCursor,
                IntList symbolColumns,
                MemoryTracker memoryTracker
        ) {
            this.sourceCursor = sourceCursor;
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
        private final ObjList<Function> functions;
        @Nullable
        private SourceState sourceState;

        private UnionSymbolCastRecord(
                IntList columnToFunctionIndex,
                ObjList<Function> functions
        ) {
            this.columnToFunctionIndex = columnToFunctionIndex;
            this.functions = functions;
        }

        @Override
        public int getInt(int col) {
            final int functionIndex = columnToFunctionIndex.getQuick(col);
            if (functionIndex < 0) {
                return super.getInt(col);
            }
            final CastStrToSymbolFunctionFactory.Func function = symbolFunction(functions.getQuick(functionIndex));
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

        private void of(SourceState sourceState) {
            this.sourceState = sourceState;
        }
    }

    private static class UnionSymbolCastRecordCursor implements NoRandomAccessRecordCursor {
        private final IntList columnToFunctionIndex;
        private final ObjList<Function> functions;
        private final UnionSymbolCastRecord record;
        private final ObjList<SourceState> sourceStates = new ObjList<>();
        private final IntList symbolColumns = new IntList();
        private final ObjList<SymbolTable> symbolTables = new ObjList<>();
        private RecordCursor baseCursor;
        private RecordCursor currentSourceCursor;
        private SourceState currentSourceState;
        private MemoryTracker memoryTracker;

        private UnionSymbolCastRecordCursor(
                IntList columnToFunctionIndex,
                ObjList<Function> functions
        ) {
            this.columnToFunctionIndex = columnToFunctionIndex;
            this.functions = functions;
            this.record = new UnionSymbolCastRecord(columnToFunctionIndex, functions);
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
                Misc.freeObjListIfCloseable(sourceStates);
                sourceStates.clear();
            } finally {
                try {
                    baseCursor = Misc.free(baseCursor);
                } finally {
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        functions.getQuick(i).cursorClosed();
                    }
                    currentSourceCursor = null;
                    currentSourceState = null;
                    memoryTracker = null;
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
            if (!baseCursor.hasNext()) {
                return false;
            }
            RecordCursor sourceCursor = baseCursor;
            if (baseCursor instanceof UnionSymbolSourceCursor source) {
                sourceCursor = source.getCurrentSymbolSourceCursor();
            }
            if (sourceCursor != currentSourceCursor) {
                currentSourceCursor = sourceCursor;
                currentSourceState = null;
                for (int i = 0, n = sourceStates.size(); i < n; i++) {
                    final SourceState candidate = sourceStates.getQuick(i);
                    if (candidate.sourceCursor == sourceCursor) {
                        currentSourceState = candidate;
                        break;
                    }
                }
                if (currentSourceState == null) {
                    currentSourceState = new SourceState(sourceCursor, symbolColumns, memoryTracker);
                    try {
                        sourceStates.add(currentSourceState);
                    } catch (RuntimeException | Error th) {
                        currentSourceState.close();
                        currentSourceState = null;
                        throw th;
                    }
                }
            }
            record.of(currentSourceState);
            return true;
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
            currentSourceCursor = null;
            currentSourceState = null;
            record.sourceState = null;
        }
    }

    private static CastStrToSymbolFunctionFactory.Func symbolFunction(Function function) {
        return (CastStrToSymbolFunctionFactory.Func) function;
    }
}
