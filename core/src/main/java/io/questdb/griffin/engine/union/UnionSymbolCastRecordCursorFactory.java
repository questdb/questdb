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
import io.questdb.cairo.sql.DelegatingRecord;
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
import io.questdb.griffin.engine.functions.columns.StrColumn;
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
import org.jetbrains.annotations.TestOnly;

/**
 * Re-exposes STRING-downcast all-SYMBOL UNION columns as SYMBOL without routing unrelated
 * columns through a VirtualRecord. Native source keys are translated into the merged result
 * dictionary once per source dictionary and then served from a query-accounted native cache.
 */
public class UnionSymbolCastRecordCursorFactory extends AbstractRecordCursorFactory {
    private final IntList columnToFunctionIndex;
    // _close() detaches these three before releasing them, so they are not final.
    private RecordCursorFactory base;
    private UnionSymbolCastRecordCursor cursor;
    private ObjList<Function> functions;

    /**
     * @param metadata              result metadata, exposing the re-symbolised columns as SYMBOL
     * @param base                  the union factory whose all-SYMBOL columns were downcast to STRING
     * @param columnToFunctionIndex for each result column, the index into {@code functions} of the
     *                              function that re-symbolises it, or -1 to pass the base column
     *                              through untouched. Function indices must ascend with column
     *                              index: the cursor's per-column source state and symbol tables are
     *                              built by walking the columns in order, so it indexes both by
     *                              function index and would otherwise pair a column with another
     *                              column's dictionary.
     * @param functions             one {@link CastStrToSymbolFunctionFactory.Func} per re-symbolised
     *                              column, serving as that column's merged dictionary. The record
     *                              reads text straight off the base record and uses the function
     *                              only to mint and resolve integer keys.
     */
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

    @TestOnly
    public Function getFunction(int index) {
        return functions.getQuick(index);
    }

    @TestOnly
    public int getFunctionCount() {
        return functions.size();
    }

    @TestOnly
    public void setCursorTestHook(@Nullable CursorTestHook testHook) {
        cursor.testHook = testHook;
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
        // AbstractRecordCursorFactory runs this at most once, so detach every owned reference before
        // the first close and attempt them all, rethrowing the first failure with the rest suppressed.
        // The cursor goes first: it owns the per-source native key maps, and its close() calls back
        // into the function list, which must still be populated when it does.
        final UnionSymbolCastRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<Function> functions = this.functions;
        this.functions = null;
        final RecordCursorFactory base = this.base;
        this.base = null;

        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeObjListBestEffort(failure, functions);
        failure = Misc.freeBestEffort(failure, base);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class KeyValueSymbolTable implements SymbolTable {
        private final SymbolTable delegate;

        private KeyValueSymbolTable(SymbolTable delegate) {
            this.delegate = delegate;
        }

        // The union answers for the whole result, but its cost is per leg: a leg backed by a table
        // dictionary translates by key and never touches text, while a dynamic leg has to intern its
        // text to mint one. True is the better approximation - the text fallback re-encodes UTF-8 on
        // every row, which the key path does only on first sight of a key - and it is what keeps a
        // static leg on two int probes per row. It costs an all-dynamic union the merged dictionary
        // it would otherwise never build.
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

    // The projection sits on a single union cursor, so it delegates to one base record. Extending
    // UnionRecord instead would carry an A/B pair whose B side is permanently null and whose useA
    // flag is permanently true, taxing every inherited getter with a branch that can never be taken.
    private static class UnionSymbolCastRecord extends DelegatingRecord {
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
                return base.getInt(col);
            }
            final CastStrToSymbolFunctionFactory.Func function = symbolFunction(functions.getQuick(functionIndex));
            final SourceState sourceState = cursor.getCurrentSourceState();
            final SourceColumn sourceColumn = sourceState.columns.getQuick(functionIndex);
            if (sourceColumn.symbolTable == null) {
                return function.intern(base.getStrA(col));
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

        // A re-symbolised column projects CastStrToSymbol(StrColumn(col)), and that function's
        // getSymbol/getSymbolB are pass-throughs onto its argument. Routing a text read through it
        // therefore only lands back on the base record's getStrA/getStrB for the same column, at the
        // cost of a list lookup, a checkcast and two virtual calls on every row. Read the base
        // directly; the function still mints and resolves integer keys in getInt/valueOf.
        @Override
        public CharSequence getSymA(int col) {
            return columnToFunctionIndex.getQuick(col) < 0 ? base.getSymA(col) : base.getStrA(col);
        }

        @Override
        public CharSequence getSymB(int col) {
            return columnToFunctionIndex.getQuick(col) < 0 ? base.getSymB(col) : base.getStrB(col);
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
        @Nullable
        private CursorTestHook testHook;

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
                    // Both lookups this cursor makes by function index - the per-source state and the
                    // symbol table below - are built by walking the columns in order, so a function
                    // index that does not ascend with its column would pair a column with another
                    // column's dictionary.
                    assert functionIndex == symbolColumns.size();
                    // The record serves a re-symbolised column's text straight off the base record,
                    // so the function must stand for that very column rather than an expression over it.
                    assert isProjectionOfColumn(functions.getQuick(functionIndex), column);
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
            if (sourceStates == null) {
                sourceStates = new IntObjHashMap<>();
            }
            SourceState sourceState = sourceStates.get(sourceIndex);
            if (sourceState == null) {
                SourceState state = null;
                try {
                    state = new SourceState(sourceTracker.getCursor(), symbolColumns, memoryTracker);
                    if (testHook != null) {
                        testHook.onSourceStateRegistration();
                    }
                    sourceStates.put(sourceIndex, state);
                    sourceState = state;
                } catch (RuntimeException | Error th) {
                    Misc.free(state);
                    throw th;
                }
            }
            currentSourceIndex = sourceIndex;
            return currentSourceState = sourceState;
        }
    }

    private static boolean isProjectionOfColumn(Function function, int column) {
        return symbolFunction(function).getArg() instanceof StrColumn strColumn
                && strColumn.getColumnIndex() == column;
    }

    private static CastStrToSymbolFunctionFactory.Func symbolFunction(Function function) {
        return (CastStrToSymbolFunctionFactory.Func) function;
    }

    @TestOnly
    public interface CursorTestHook {
        void onSourceStateRegistration();
    }
}
