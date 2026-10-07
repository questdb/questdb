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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One step of the per-row pipeline an {@link AsyncWindowRecordCursorFactory} computes after its
 * window functions, in the order the serial plan's factories would apply it: a projection
 * ({@code VirtualRecord}), another window over the rows so far, or a filter. Each worker slot
 * holds a copy of its own, compiled separately, so the steps of one key run on the thread that
 * computes the key, row by row, exactly as the serial chain of factories computes them.
 * <p>
 * A stage reads the record of the stage before it, the window's own output for the first stage,
 * and exposes its output as a record of its own. A filter stage has no record of its own: it
 * passes its input on, or drops the row.
 */
public class AsyncWindowStage implements QuietCloseable, SymbolTableSource {
    public static final int KIND_FILTER = 2;
    public static final int KIND_GROUP_BY = 3;
    public static final int KIND_VIRTUAL = 0;
    public static final int KIND_WINDOW = 1;
    private final ObjList<Function> functions;
    private final int kind;
    private final ObjList<WindowMapState> mapStates;
    private final int mapStatesCount;
    private final ObjList<MemoizerFunction> memoizers = new ObjList<>();
    private final ProjectionSymbols projectionSymbols = new ProjectionSymbols();
    private final Record record;
    private final int reservedSlots;
    private final ObjList<WindowFunction> windowFunctions = new ObjList<>();
    private Record input;
    // the symbol tables of the input, which a projection reads past its own columns
    private SymbolTableSource inputSymbols;
    private boolean isOpen;
    // names the columns its functions read, for EXPLAIN
    private RecordMetadata planMetadata;

    protected AsyncWindowStage(
            int kind,
            @NotNull ObjList<Function> functions,
            @Nullable ObjList<WindowMapState> mapStates,
            int reservedSlots
    ) {
        this.kind = kind;
        this.functions = functions;
        this.mapStates = mapStates;
        this.mapStatesCount = mapStates != null ? mapStates.size() : 0;
        this.reservedSlots = reservedSlots;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (function instanceof WindowFunction wf) {
                windowFunctions.add(wf);
            }
            if (function instanceof MemoizerFunction mf) {
                memoizers.add(mf);
            }
        }
        this.record = switch (kind) {
            case KIND_VIRTUAL -> new VirtualFunctionRecord(functions, reservedSlots);
            case KIND_WINDOW -> new VirtualRecord(functions);
            default -> null;
        };
    }

    /**
     * Ends the rows of a task, a stream or a scan: a stage that outputs a row only once it has
     * seen the rows after it, a GROUP BY, outputs its last one now. Returns whether it did; the
     * record the next stage reads then holds it.
     */
    public boolean flush() {
        return false;
    }

    /**
     * Whether the stage outputs one row per input row it keeps; a GROUP BY outputs one per group.
     */
    public boolean isRowPreserving() {
        return kind != KIND_GROUP_BY;
    }

    /**
     * A filter over the rows so far: a row it rejects is not output, and no later stage sees it.
     */
    public static AsyncWindowStage filter(@NotNull Function filter) {
        final ObjList<Function> functions = new ObjList<>(1);
        functions.add(filter);
        return new AsyncWindowStage(KIND_FILTER, functions, null, 0);
    }

    /**
     * A projection, as {@code VirtualRecordCursorFactory} computes it: its functions read their
     * own columns below {@code reservedSlots} and the input's columns from there on.
     */
    public static AsyncWindowStage virtual(@NotNull ObjList<Function> functions, int reservedSlots) {
        return new AsyncWindowStage(KIND_VIRTUAL, functions, null, reservedSlots);
    }

    /**
     * Streaming window functions over the rows so far, one function per output column, as
     * {@link WindowRecordCursorFactory} computes them, with their window Map groups.
     */
    public static AsyncWindowStage window(@NotNull ObjList<Function> functions, @Nullable ObjList<WindowMapState> mapStates) {
        return new AsyncWindowStage(KIND_WINDOW, functions, mapStates, 0);
    }

    /**
     * Binds the stage to the record of the stage before it. Returns the record the next stage
     * reads: this stage's output, or the input itself for a filter.
     */
    public Record bind(Record input, SymbolTableSource inputSymbols) {
        this.input = input;
        this.inputSymbols = inputSymbols;
        switch (kind) {
            case KIND_VIRTUAL -> ((VirtualFunctionRecord) record).of(input);
            case KIND_WINDOW -> ((VirtualRecord) record).of(input);
            default -> {
                return input;
            }
        }
        return record;
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, mapStates);
        failure = Misc.freeObjListBestEffort(failure, functions);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Releases what one execution held, as {@code AsyncWindowAtom.Slot.closeCursor()} does for
     * the window's own functions; the functions stay compiled.
     */
    public void closeCursor() {
        if (isOpen) {
            isOpen = false;
            for (int i = 0, n = functions.size(); i < n; i++) {
                final Function function = functions.getQuick(i);
                if (function != null) {
                    function.cursorClosed();
                }
            }
            for (int i = 0, n = windowFunctions.size(); i < n; i++) {
                windowFunctions.getQuick(i).reset();
            }
            for (int i = 0; i < mapStatesCount; i++) {
                mapStates.getQuick(i).reset();
            }
        }
    }

    /**
     * Computes the stage for the row its input record stands on. Returns false when a filter
     * drops the row; the stages after it are then not computed for that row.
     */
    public boolean computeNext() {
        switch (kind) {
            case KIND_VIRTUAL -> {
                for (int i = 0, n = memoizers.size(); i < n; i++) {
                    memoizers.getQuick(i).clearMemo();
                }
                return true;
            }
            case KIND_WINDOW -> {
                // groups first, as WindowRecordCursorFactory does
                for (int i = 0; i < mapStatesCount; i++) {
                    mapStates.getQuick(i).computeNext(input);
                }
                for (int i = 0, n = windowFunctions.size(); i < n; i++) {
                    windowFunctions.getQuick(i).computeNext(input);
                }
                return true;
            }
            default -> {
                return functions.getQuick(0).getBool(input);
            }
        }
    }

    /**
     * The functions of the output columns, or the filter alone.
     */
    public ObjList<Function> getFunctions() {
        return functions;
    }

    public int getKind() {
        return kind;
    }

    /**
     * The metadata that names the columns the stage's functions read, for EXPLAIN.
     */
    public RecordMetadata getPlanMetadata() {
        return planMetadata;
    }

    public void setPlanMetadata(RecordMetadata planMetadata) {
        this.planMetadata = planMetadata;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return switch (kind) {
            case KIND_FILTER -> inputSymbols.getSymbolTable(columnIndex);
            default -> (SymbolTable) functions.getQuick(columnIndex);
        };
    }

    public ObjList<WindowFunction> getWindowFunctions() {
        return windowFunctions;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return switch (kind) {
            case KIND_FILTER -> inputSymbols.newSymbolTable(columnIndex);
            default -> ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
        };
    }

    /**
     * Binds the functions to an execution. A projection's functions read symbol tables of its
     * own columns below the reserved slots and of the input from there on, as
     * {@code VirtualRecordCursorFactory} arranges them.
     */
    public void open(SqlExecutionContext executionContext, boolean cloneSymbolTables) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
            for (int i = 0, n = windowFunctions.size(); i < n; i++) {
                windowFunctions.getQuick(i).setMemoryTracker(memoryTracker);
            }
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functions.getQuick(i) instanceof Reopenable reopenable) {
                    reopenable.reopen();
                }
            }
            for (int i = 0; i < mapStatesCount; i++) {
                final WindowMapState state = mapStates.getQuick(i);
                state.setMemoryTracker(memoryTracker);
                state.reopen();
            }
        }
        final SymbolTableSource source = kind == KIND_VIRTUAL ? projectionSymbols : inputSymbols;
        final boolean current = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(cloneSymbolTables);
        try {
            Function.init(functions, source, executionContext, null);
        } finally {
            executionContext.setCloneSymbolTables(current);
        }
    }

    /**
     * Forgets every partition's state, so that the next row starts its keys from scratch.
     */
    public void toTop() {
        if (!isOpen) {
            return;
        }
        GroupByUtils.toTop(functions);
        for (int i = 0; i < mapStatesCount; i++) {
            mapStates.getQuick(i).clear();
        }
    }

    // A projection's columns below the reserved slots are its own, the rest the input's.
    private class ProjectionSymbols implements SymbolTableSource {
        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < reservedSlots) {
                return (SymbolTable) functions.getQuick(columnIndex);
            }
            return inputSymbols.getSymbolTable(columnIndex - reservedSlots);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < reservedSlots) {
                return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
            }
            return inputSymbols.newSymbolTable(columnIndex - reservedSlots);
        }
    }
}
