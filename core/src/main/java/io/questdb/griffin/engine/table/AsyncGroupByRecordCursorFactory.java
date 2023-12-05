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
import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncGroupByRecordCursorFactory::aggregate;

    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncGroupByRecordCursor cursor;
    private final AsyncGroupByAtom filterAtom;
    private final PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> recordFunctions;
    private final int workerCount;

    // TODO(puzpuzpuz): make sure we close all functions properly
    public AsyncGroupByRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Nullable Function filter,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        super(groupByMetadata);
        try {
            assert !(base instanceof AsyncGroupByRecordCursorFactory);
            this.base = base;
            this.frameSequence = new PageFrameSequence<>(configuration, messageBus, REDUCER, reduceTaskFactory, PageFrameReduceTask.TYPE_GROUP_BY);
            frameSequence.prepareForGroupBy(keyTypes, valueTypes);
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            this.cursor = new AsyncGroupByRecordCursor(configuration, keyTypes, valueTypes, groupByFunctions, recordFunctions);
            this.filterAtom = new AsyncGroupByAtom(
                    asm,
                    configuration,
                    base.getMetadata(),
                    listColumnFilter,
                    groupByFunctions,
                    filter,
                    perWorkerFilters,
                    workerCount
            );
            this.workerCount = workerCount;
        } catch (Throwable e) {
            Misc.freeObjList(recordFunctions);
            throw e;
        }
    }

    @Override
    public PageFrameSequence<AsyncGroupByAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, filterAtom, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        execute(executionContext, collectSubSeq, order);
        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        Function.init(recordFunctions, frameSequence.getSymbolTableSource(), executionContext);
        cursor.of(frameSequence);
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
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", groupByFunctions, true);
        sink.attr("filter").val(filterAtom);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static void aggregate(
            int workerId,
            @NotNull PageAddressCacheRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final Map map = task.getGroupByMap();
        final long frameRowCount = task.getFrameRowCount();
        final AsyncGroupByAtom atom = task.getFrameSequence(AsyncGroupByAtom.class).getAtom();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater();
        final RecordSink mapSink = atom.getMapSink();

        map.clear();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.acquire(workerId, owner, circuitBreaker);
        final Function filter = atom.getFilter(slotId);
        try {
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                if (filter != null && !filter.getBool(record)) {
                    continue;
                }

                final MapKey key = map.withKey();
                mapSink.copy(record, key);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    functionUpdater.updateNew(value, record);
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
        Misc.freeObjList(groupByFunctions);
        Misc.freeObjList(recordFunctions);
        Misc.free(filterAtom);
        Misc.free(frameSequence);
        Misc.free(base);
        Misc.free(cursor);
    }
}
