/*******************************************************************************
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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredNoRandomAccessMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncGroupByNotKeyedRecordCursorFactory::aggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncGroupByNotKeyedRecordCursorFactory::filterAndAggregate;

    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncGroupByNotKeyedRecordCursor cursor;
    private final PageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int workerCount;

    public AsyncGroupByNotKeyedRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int valueCount,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            AsyncGroupByNotKeyedAtom atom = new AsyncGroupByNotKeyedAtom(
                    asm,
                    configuration,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    valueCount,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount
            );
            if (filter != null) {
                this.frameSequence = new PageFrameSequence<>(
                        engine,
                        configuration,
                        messageBus,
                        atom,
                        FILTER_AND_AGGREGATE,
                        reduceTaskFactory,
                        workerCount,
                        PageFrameReduceTask.TYPE_GROUP_BY_NOT_KEYED
                );
            } else {
                this.frameSequence = new PageFrameSequence<>(
                        engine,
                        configuration,
                        messageBus,
                        atom,
                        AGGREGATE,
                        reduceTaskFactory,
                        workerCount,
                        PageFrameReduceTask.TYPE_GROUP_BY_NOT_KEYED
                );
            }
            this.cursor = new AsyncGroupByNotKeyedRecordCursor(groupByFunctions);
            this.workerCount = workerCount;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public PageFrameSequence<AsyncGroupByNotKeyedAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        cursor.of(execute(executionContext, collectSubSeq, order), executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Group By");
        } else {
            sink.type("Async Group By");
        }
        sink.meta("workers").val(workerCount);
        sink.optAttr("values", groupByFunctions, true);
        sink.optAttr("filter", frameSequence.getAtom(), true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncGroupByNotKeyedAtom atom = task.getFrameSequence(AsyncGroupByNotKeyedAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final SimpleMapValue value = atom.getMapValue(slotId);
        try {
            record.setRowIndex(0);
            long rowId = record.getRowId();
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                if (value.isNew()) {
                    functionUpdater.updateNew(value, record, rowId++);
                    value.setNew(false);
                } else {
                    functionUpdater.updateExisting(value, record, rowId++);
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateFiltered(
            @NotNull PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            SimpleMapValue value,
            GroupByFunctionsUpdater functionUpdater
    ) {
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setRowIndex(r);
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
                value.setNew(false);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final PageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence = task.getFrameSequence(AsyncGroupByNotKeyedAtom.class);
        final AsyncGroupByNotKeyedAtom atom = frameSequence.getAtom();

        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);

        final PageFrameMemory frameMemory;
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final SimpleMapValue value = atom.getMapValue(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledFilter();
        final Function filter = atom.getFilter(slotId);
        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                AsyncFilterUtils.applyFilter(filter, rows, record, frameRowCount);
            } else {
                AsyncFilterUtils.applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, false)) {
                PageFrameFilteredNoRandomAccessMemoryRecord filteredMemoryRecord = atom.getPageFrameFilteredMemoryRecord(slotId);
                filteredMemoryRecord.of(frameMemory, record, atom.getFilterUsedColumnIndexes());
                record = filteredMemoryRecord;
            }
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            aggregateFiltered(record, rows, baseRowId, value, functionUpdater);
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Releases resources held by this factory.
     * <p>
     * Frees the underlying base factory, the prepared cursor, and the frame sequence,
     * and also frees and clears the list of group-by function instances to remove
     * references.
     */
    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
        Misc.freeObjListAndClear(groupByFunctions);
    }
}
