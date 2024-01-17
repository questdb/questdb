/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncGroupByNotKeyedRecordCursorFactory::aggregate;
    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncGroupByNotKeyedRecordCursor cursor;
    private final PageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int workerCount;

    public AsyncGroupByNotKeyedRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            @Nullable Function filter,
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
                    valueCount,
                    filter,
                    perWorkerFilters,
                    workerCount
            );
            this.frameSequence = new PageFrameSequence<>(configuration, messageBus, atom, REDUCER, reduceTaskFactory, PageFrameReduceTask.TYPE_GROUP_BY_NOT_KEYED);
            this.cursor = new AsyncGroupByNotKeyedRecordCursor(groupByFunctions, valueCount);
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
        sink.type("Async Group By");
        sink.meta("workers").val(workerCount);
        sink.optAttr("values", groupByFunctions, true);
        sink.optAttr("filter", frameSequence.getAtom(), true);
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

    private static void aggregate(
            int workerId,
            @NotNull PageAddressCacheRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        final AsyncGroupByNotKeyedAtom atom = task.getFrameSequence(AsyncGroupByNotKeyedAtom.class).getAtom();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.acquire(workerId, owner, circuitBreaker);
        final SimpleMapValue value = atom.getMapValue(slotId);
        final Function filter = atom.getFilter(slotId);
        try {
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                if (filter != null && !filter.getBool(record)) {
                    continue;
                }

                if (value.isNew()) {
                    functionUpdater.updateNew(value, record);
                    value.setNew(false);
                } else {
                    functionUpdater.updateExisting(value, record);
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(groupByFunctions);
        Misc.free(frameSequence);
    }
}
