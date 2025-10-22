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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
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
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

// TODO(puzpuzpuz): add support for JIT-compiled filters
// TODO(puzpuzpuz): consider implementing limit and negative limit
public class AsyncWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncWindowJoinRecordCursorFactory::filterAndAggregate;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncWindowJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncWindowJoinAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncWindowJoinRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata joinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function joinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            long joinWindowLo,
            long joinWindowHi,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int valueCount,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(joinMetadata);

        assert masterFactory.supportsPageFrameCursor();
        assert slaveFactory.supportsTimeFrameCursor();

        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.groupByFunctions = groupByFunctions;
        this.cursor = new AsyncWindowJoinRecordCursor(configuration, groupByFunctions, slaveFactory.getMetadata());
        final ObjList<ConcurrentTimeFrameCursor> perWorkerTimeFrameCursors = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            perWorkerTimeFrameCursors.add(slaveFactory.newTimeFrameCursor());
        }
        final AsyncWindowJoinAtom atom = new AsyncWindowJoinAtom(
                asm,
                configuration,
                slaveFactory.newTimeFrameCursor(),
                perWorkerTimeFrameCursors,
                joinFilter,
                perWorkerJoinFilters,
                joinWindowLo,
                joinWindowHi,
                groupByFunctions,
                perWorkerGroupByFunctions,
                valueCount,
                compiledFilter,
                bindVarMemory,
                bindVarFunctions,
                filter,
                perWorkerFilters,
                workerCount
        );
        this.frameSequence = new PageFrameSequence<>(
                engine,
                configuration,
                messageBus,
                atom,
                REDUCER,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_WINDOW_JOIN
        );
        this.workerCount = workerCount;
        // these values are in slave's timestamp granularity
        this.joinWindowLo = joinWindowLo;
        this.joinWindowHi = joinWindowHi;
    }

    @Override
    public PageFrameSequence<AsyncWindowJoinAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(masterFactory, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int masterOrder = masterFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final int slaveOrder = slaveFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final TablePageFrameCursor slaveFrameCursor = (TablePageFrameCursor) slaveFactory.getPageFrameCursor(executionContext, slaveOrder);
        cursor.of(execute(executionContext, collectSubSeq, masterOrder), slaveFrameCursor, executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return masterFactory.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Window Join");
        sink.meta("workers").val(workerCount);
        sink.val(frameSequence.getAtom());
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
        // TODO(puzpuzpuz): add window lo/hi
    }

    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getFilteredRows();
        final long frameRowCount = task.getFrameRowCount();
        final AsyncFilterAtom atom = task.getFrameSequence(AsyncFilterAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        rows.clear();

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
        } finally {
            atom.releaseFilter(filterId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.freeObjListAndClear(groupByFunctions);
    }
}
