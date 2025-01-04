/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

public class AsyncJitFilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncJitFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final SCSequence collectSubSeq = new SCSequence();
    private final CompiledFilter compiledFilter;
    private final AsyncFilteredRecordCursor cursor;
    private final Function filter;
    private final PageFrameSequence<AsyncJitFilterAtom> frameSequence;
    private final Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final int workerCount;
    private DirectLongList negativeLimitRows;

    public AsyncJitFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull CompiledFilter compiledFilter,
            @NotNull Function filter,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            boolean preTouchColumns,
            int workerCount
    ) {
        super(base.getMetadata());
        assert !(base instanceof FilteredRecordCursorFactory);
        assert !(base instanceof AsyncJitFilteredRecordCursorFactory);
        this.base = base;
        this.compiledFilter = compiledFilter;
        this.filter = filter;
        this.cursor = new AsyncFilteredRecordCursor(configuration, filter, base.getScanDirection());
        this.negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(configuration, base.getScanDirection());
        this.bindVarMemory = Vm.getCARWInstance(
                configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(),
                MemoryTag.NATIVE_JIT
        );
        this.bindVarFunctions = bindVarFunctions;
        final int columnCount = base.getMetadata().getColumnCount();
        final IntList columnTypes = new IntList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int columnType = base.getMetadata().getColumnType(i);
            columnTypes.add(columnType);
        }
        AsyncJitFilterAtom atom = new AsyncJitFilterAtom(
                configuration,
                filter,
                perWorkerFilters,
                compiledFilter,
                bindVarMemory,
                bindVarFunctions,
                columnTypes,
                !preTouchColumns
        );
        this.frameSequence = new PageFrameSequence<>(
                configuration,
                messageBus,
                atom,
                REDUCER,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_FILTER
        );
        this.limitLoFunction = limitLoFunction;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        this.workerCount = workerCount;
    }

    public static void prepareBindVarMemory(
            SqlExecutionContext executionContext,
            SymbolTableSource symbolTableSource,
            ObjList<Function> bindVarFunctions,
            MemoryCARW bindVarMemory
    ) throws SqlException {
        // don't trigger memory allocation if there are no variables
        if (bindVarFunctions.size() > 0) {
            bindVarMemory.truncate();
            for (int i = 0, n = bindVarFunctions.size(); i < n; i++) {
                Function function = bindVarFunctions.getQuick(i);
                writeBindVarFunction(bindVarMemory, function, symbolTableSource, executionContext);
            }
        }
    }

    @Override
    public PageFrameSequence<AsyncJitFilterAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return limitLoFunction != null;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    @Override
    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    @Override
    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        long rowsRemaining;
        int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final int order;
        if (limitLoFunction != null) {
            limitLoFunction.init(frameSequence.getSymbolTableSource(), executionContext);
            rowsRemaining = limitLoFunction.getLong(null);
            // on negative limit we will be looking for positive number of rows
            // while scanning table from the highest timestamp to the lowest
            if (rowsRemaining > -1) {
                order = baseOrder;
            } else {
                order = reverse(baseOrder);
                rowsRemaining = -rowsRemaining;
            }
        } else {
            rowsRemaining = Long.MAX_VALUE;
            order = baseOrder;
        }

        if (order != baseOrder && rowsRemaining != Long.MAX_VALUE) {
            if (rowsRemaining > maxNegativeLimit) {
                throw SqlException.position(limitLoPos).put("absolute LIMIT value is too large, maximum allowed value: ").put(maxNegativeLimit);
            }
            if (negativeLimitRows == null) {
                negativeLimitRows = new DirectLongList(maxNegativeLimit, MemoryTag.NATIVE_OFFLOAD);
            }
            negativeLimitCursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining, negativeLimitRows);
            return negativeLimitCursor;
        }

        cursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining);
        return cursor;
    }

    @Override
    public @NotNull Function getFilter() {
        return filter;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public void halfClose() {
        Misc.free(frameSequence);
        cursor.freeRecords();
        negativeLimitCursor.freeRecords();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsFilterStealing() {
        return limitLoFunction == null;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async JIT Filter");
        sink.meta("workers").val(workerCount);
        // calc order and limit if possible
        long rowsRemaining;
        int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        int order;
        if (limitLoFunction != null) {
            try {
                limitLoFunction.init(frameSequence.getSymbolTableSource(), sink.getExecutionContext());
                rowsRemaining = limitLoFunction.getLong(null);
            } catch (Exception e) {
                rowsRemaining = Long.MAX_VALUE;
            }
            if (rowsRemaining > -1) {
                order = baseOrder;
            } else {
                order = reverse(baseOrder);
                rowsRemaining = -rowsRemaining;
            }
        } else {
            rowsRemaining = Long.MAX_VALUE;
            order = baseOrder;
        }
        if (rowsRemaining != Long.MAX_VALUE) {
            sink.attr("limit").val(rowsRemaining);
        }
        sink.attr("filter").val(frameSequence.getAtom());
        sink.child(base, order);
    }

    @Override
    public boolean usesCompiledFilter() {
        return true;
    }

    private static void filter(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getFilteredRows();
        final long frameRowCount = task.getFrameRowCount();
        final PageFrameSequence<AsyncJitFilterAtom> frameSequence = task.getFrameSequence(AsyncJitFilterAtom.class);
        final AsyncJitFilterAtom atom = frameSequence.getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        rows.clear();

        if (frameMemory.hasColumnTops()) {
            // Use Java-based filter in case of a page frame with column tops.
            final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
            final int filterId = atom.maybeAcquireFilter(workerId, owner, circuitBreaker);
            final Function filter = atom.getFilter(filterId);
            try {
                for (long r = 0; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    if (filter.getBool(record)) {
                        rows.add(r);
                    }
                }
                return;
            } finally {
                atom.releaseFilter(filterId);
            }
        }

        // Use JIT-compiled filter.

        task.populateJitData();
        final DirectLongList dataAddresses = task.getDataAddresses();
        final DirectLongList auxAddresses = task.getAuxAddresses();

        long hi = atom.compiledFilter.call(
                dataAddresses.getAddress(),
                dataAddresses.size(),
                auxAddresses.getAddress(),
                atom.bindVarMemory.getAddress(),
                atom.bindVarFunctions.size(),
                rows.getAddress(),
                frameRowCount,
                0
        );
        rows.setPos(hi);

        // Pre-touch native columns, if asked.
        if (frameMemory.getFrameFormat() == PartitionFormat.NATIVE) {
            atom.preTouchColumns(record, rows);
        }
    }

    private static void writeBindVarFunction(
            MemoryCARW bindVarMemory,
            Function function,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int columnType = function.getType();
        final int columnTypeTag = ColumnType.tagOf(columnType);
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
                bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                return;
            case ColumnType.BYTE:
                bindVarMemory.putLong(function.getByte(null));
                return;
            case ColumnType.GEOBYTE:
                bindVarMemory.putLong(function.getGeoByte(null));
                return;
            case ColumnType.SHORT:
                bindVarMemory.putLong(function.getShort(null));
                return;
            case ColumnType.GEOSHORT:
                bindVarMemory.putLong(function.getGeoShort(null));
                return;
            case ColumnType.CHAR:
                bindVarMemory.putLong(function.getChar(null));
                return;
            case ColumnType.INT:
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.IPv4:
                bindVarMemory.putLong(function.getIPv4(null));
                return;
            case ColumnType.GEOINT:
                bindVarMemory.putLong(function.getGeoInt(null));
                return;
            case ColumnType.SYMBOL:
                assert function instanceof CompiledFilterSymbolBindVariable;
                function.init(symbolTableSource, executionContext);
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.FLOAT:
                // compiled filter function will read only the first word
                bindVarMemory.putFloat(function.getFloat(null));
                bindVarMemory.putFloat(Float.NaN);
                return;
            case ColumnType.LONG:
                bindVarMemory.putLong(function.getLong(null));
                return;
            case ColumnType.GEOLONG:
                bindVarMemory.putLong(function.getGeoLong(null));
                return;
            case ColumnType.DATE:
                bindVarMemory.putLong(function.getDate(null));
                return;
            case ColumnType.TIMESTAMP:
                bindVarMemory.putLong(function.getTimestamp(null));
                return;
            case ColumnType.DOUBLE:
                bindVarMemory.putDouble(function.getDouble(null));
                return;
            default:
                throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(columnTypeTag));
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(negativeLimitRows);
        halfClose();
        Misc.free(compiledFilter);
        Misc.free(filter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
    }

    public static class AsyncJitFilterAtom extends AsyncFilterAtom {

        final ObjList<Function> bindVarFunctions;
        final MemoryCARW bindVarMemory;
        final CompiledFilter compiledFilter;

        public AsyncJitFilterAtom(
                CairoConfiguration configuration,
                Function filter,
                ObjList<Function> perWorkerFilters,
                CompiledFilter compiledFilter,
                MemoryCARW bindVarMemory,
                ObjList<Function> bindVarFunctions,
                IntList columnTypes,
                boolean forceDisablePreTouch
        ) {
            super(configuration, filter, perWorkerFilters, columnTypes, forceDisablePreTouch);
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(bindVarFunctions, symbolTableSource, executionContext);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }
}
