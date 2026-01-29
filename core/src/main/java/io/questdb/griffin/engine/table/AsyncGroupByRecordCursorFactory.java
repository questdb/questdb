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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
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
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

public class AsyncGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncGroupByRecordCursorFactory::aggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncGroupByRecordCursorFactory::filterAndAggregate;

    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncGroupByRecordCursor cursor;
    private final PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private final ObjList<Function> recordFunctions; // includes groupByFunctions
    private final int workerCount;

    public AsyncGroupByRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.recordFunctions = recordFunctions;
            AsyncGroupByAtom atom = new AsyncGroupByAtom(
                    asm,
                    configuration,
                    base.getMetadata(),
                    keyTypes,
                    valueTypes,
                    listColumnFilter,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    keyFunctions,
                    perWorkerKeyFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount
            );
            this.frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_AGGREGATE : AGGREGATE,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY
            );
            this.cursor = new AsyncGroupByRecordCursor(engine, recordFunctions, messageBus);
            this.workerCount = workerCount;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public PageFrameSequence<AsyncGroupByAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
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
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        final int columnType = getMetadata().getColumnType(columnIndex);
        return columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Group By");
        } else {
            sink.type("Async Group By");
        }
        sink.meta("workers").val(workerCount);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions(), true);
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
        final AsyncGroupByAtom atom = task.getFrameSequence(AsyncGroupByAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final AsyncGroupByAtom.MapFragment fragment = atom.getFragment(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        try {
            fragment.resetLocalStats();

            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (fragment.isNotSharded()) {
                aggregateNonSharded(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
            } else {
                aggregateSharded(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.maybeEnableSharding(fragment);
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateFilteredNonSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Map map = fragment.reopenMap();
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setRowIndex(r);

            final MapKey key = map.withKey();
            mapSink.copy(record, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateFilteredSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setRowIndex(r);

            final MapKey lookupKey = lookupShard.withKey();
            mapSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        final Map map = fragment.reopenMap();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey key = map.withKey();
            mapSink.copy(record, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey lookupKey = lookupShard.withKey();
            mapSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
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
        final PageFrameSequence<AsyncGroupByAtom> frameSequence = task.getFrameSequence(AsyncGroupByAtom.class);
        final AsyncGroupByAtom atom = frameSequence.getAtom();

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
        final AsyncGroupByAtom.MapFragment fragment = atom.getFragment(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledFilter();
        final Function filter = atom.getFilter(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        try {
            fragment.resetLocalStats();

            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }
            if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, false)) {
                PageFrameFilteredNoRandomAccessMemoryRecord filteredMemoryRecord = atom.getPageFrameFilteredMemoryRecord(slotId);
                filteredMemoryRecord.of(frameMemory, record, atom.getFilterUsedColumnIndexes());
                record = filteredMemoryRecord;
            }

            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (fragment.isNotSharded()) {
                aggregateFilteredNonSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
            } else {
                aggregateFilteredSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.maybeEnableSharding(fragment);
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
        Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
    }
}
