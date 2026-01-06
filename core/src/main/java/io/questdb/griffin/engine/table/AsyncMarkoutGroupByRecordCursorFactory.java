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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Factory for parallel markout query execution using PageFrameSequence.
 * <p>
 * This factory creates the infrastructure for parallelizing:
 * Master -> MarkoutHorizon (CROSS JOIN time_offset_sequence) -> ASOF JOIN slave -> GROUP BY time_offset
 */
public class AsyncMarkoutGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer FILTER_AND_REDUCE = AsyncMarkoutGroupByRecordCursorFactory::filterAndReduce;
    private static final PageFrameReducer REDUCE = AsyncMarkoutGroupByRecordCursorFactory::reduce;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncMarkoutGroupByRecordCursor cursor;
    private final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence;
    private final RecordCursorFactory masterFactory;
    private final ObjList<Function> recordFunctions;
    private final RecordCursorFactory sequenceFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncMarkoutGroupByRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull RecordCursorFactory sequenceFactory,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int slaveTimestampIndex,
            @NotNull RecordSink groupByKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndices,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory
    ) {
        super(metadata);
        try {
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.sequenceFactory = sequenceFactory;
            this.recordFunctions = recordFunctions;
            this.workerCount = workerCount;

            AsyncMarkoutGroupByAtom atom = new AsyncMarkoutGroupByAtom(
                    asm,
                    configuration,
                    slaveFactory,
                    masterTimestampColumnIndex,
                    sequenceColumnIndex,
                    keyTypes,
                    valueTypes,
                    asOfJoinKeyTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveTimestampIndex,
                    groupByKeyCopier,
                    columnSources,
                    columnIndices,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
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
                    filter != null ? FILTER_AND_REDUCE : REDUCE,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY
            );

            this.cursor = new AsyncMarkoutGroupByRecordCursor(
                    configuration,
                    recordFunctions,
                    sequenceFactory,
                    slaveFactory
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public PageFrameSequence<AsyncMarkoutGroupByAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(masterFactory, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(execute(executionContext, collectSubSeq, ORDER_ASC), executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Markout GroupBy");
        } else {
            sink.type("Async Markout GroupBy");
        }
        sink.meta("workers").val(workerCount);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions(), true);
        sink.child(masterFactory);
        sink.child(sequenceFactory);
        sink.child(slaveFactory);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    /**
     * Page frame reducer for filtered markout GROUP BY.
     * <p>
     * Applies filter first, then for each filtered master row iterates through all sequence offsets,
     * performs ASOF JOIN lookup using MarkoutTimeFrameHelper, and aggregates results.
     */
    private static void filterAndReduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        if (frameRowCount == 0) {
            return;
        }

        final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence = task.getFrameSequence(AsyncMarkoutGroupByAtom.class);
        final AsyncMarkoutGroupByAtom atom = frameSequence.getAtom();

        final long sequenceRowCount = atom.getSequenceRowCount();
        if (sequenceRowCount == 0) {
            return;
        }

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final Map partialMap = atom.getMap(slotId);
            final RecordSink groupByKeyCopier = atom.getGroupByKeyCopier();
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final CombinedRecord combinedRecord = atom.getCombinedRecord(slotId);
            final CompiledFilter compiledFilter = atom.getCompiledFilter();
            final Function filter = atom.getFilter(slotId);

            // Apply filter to master rows
            final DirectLongList rows = task.getFilteredRows();
            rows.clear();
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            if (filteredRowCount == 0) {
                return;
            }

            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asofJoinMap = atom.getAsOfJoinMap(slotId);
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();

            slaveTimeFrameHelper.toTop();
            if (asofJoinMap != null) {
                asofJoinMap.clear();
            }

            // Get slave record from time frame helper
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process filtered rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            for (long i = 0; i < filteredRowCount; i++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final long r = rows.get(i);
                record.setRowIndex(r);
                long masterTimestamp = record.getTimestamp(masterTimestampColumnIndex);

                for (int seqIdx = 0; seqIdx < sequenceRowCount; seqIdx++) {
                    // sec_offs is stored, compute usec_offs dynamically (sec_offs * 1_000_000)
                    long secOffsValue = atom.getSequenceOffsetValue(seqIdx);
                    long usecOffsValue = secOffsValue * 1_000_000L;
                    long horizonTimestamp = masterTimestamp + usecOffsValue;

                    // ASOF JOIN lookup using time frame helper
                    Record matchedSlaveRecord = null;
                    if (asofJoinMap != null && masterKeyCopier != null) {
                        // Find the ASOF row (last row with timestamp <= horizonTimestamp)
                        long slaveRowIndex = slaveTimeFrameHelper.findAsOfRow(horizonTimestamp);
                        if (slaveRowIndex != Long.MIN_VALUE) {
                            // Position record at the found row
                            long slaveRowId = slaveTimeFrameHelper.toRowId(slaveRowIndex);
                            slaveTimeFrameHelper.recordAt(slaveRowId);

                            // Update ASOF map with this slave's key -> rowId
                            MapKey joinKey = asofJoinMap.withKey();
                            joinKey.put(slaveRecord, slaveKeyCopier);
                            MapValue joinValue = joinKey.createValue();
                            joinValue.putLong(0, slaveRowId);
                        }

                        // Look up master's join key in the ASOF map
                        MapKey lookupKey = asofJoinMap.withKey();
                        lookupKey.put(record, masterKeyCopier);
                        MapValue lookupValue = lookupKey.findValue();

                        if (lookupValue != null) {
                            long matchedRowId = lookupValue.getLong(0);
                            slaveTimeFrameHelper.recordAt(matchedRowId);
                            matchedSlaveRecord = slaveRecord;
                        }
                    }

                    // Set up combined record (pass sec_offs for GROUP BY key)
                    combinedRecord.of(record, secOffsValue, matchedSlaveRecord);

                    // Aggregate
                    MapKey key = partialMap.withKey();
                    key.put(combinedRecord, groupByKeyCopier);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        functionUpdater.updateNew(value, combinedRecord, baseRowId + r);
                    } else {
                        functionUpdater.updateExisting(value, combinedRecord, baseRowId + r);
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Page frame reducer for markout GROUP BY.
     * <p>
     * For each master row in the page frame, iterates through all sequence offsets,
     * performs ASOF JOIN lookup using MarkoutTimeFrameHelper, and aggregates results.
     */
    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        if (frameRowCount == 0) {
            return;
        }

        final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence = task.getFrameSequence(AsyncMarkoutGroupByAtom.class);
        final AsyncMarkoutGroupByAtom atom = frameSequence.getAtom();

        final long sequenceRowCount = atom.getSequenceRowCount();
        if (sequenceRowCount == 0) {
            return;
        }

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final Map partialMap = atom.getMap(slotId);
            final RecordSink groupByKeyCopier = atom.getGroupByKeyCopier();
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final CombinedRecord combinedRecord = atom.getCombinedRecord(slotId);

            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asOfJoinMap = atom.getAsOfJoinMap(slotId);
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();

            slaveTimeFrameHelper.toTop();
            if (asOfJoinMap != null) {
                asOfJoinMap.clear();
            }

            // Get slave record from time frame helper
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                long masterTimestamp = record.getTimestamp(masterTimestampColumnIndex);

                for (int seqIdx = 0; seqIdx < sequenceRowCount; seqIdx++) {
                    // sec_offs is stored, compute usec_offs dynamically (sec_offs * 1_000_000)
                    long secOffsValue = atom.getSequenceOffsetValue(seqIdx);
                    // TODO(puzpuzpuz): support nanos
                    long usecOffsValue = secOffsValue * 1_000_000L;
                    long horizonTimestamp = masterTimestamp + usecOffsValue;

                    // ASOF JOIN lookup using time frame helper
                    Record matchedSlaveRecord = null;
                    if (asOfJoinMap != null && masterKeyCopier != null) {
                        // Find the ASOF row (last row with timestamp <= horizonTimestamp)
                        long slaveRowIndex = slaveTimeFrameHelper.findAsOfRow(horizonTimestamp);
                        if (slaveRowIndex != Long.MIN_VALUE) {
                            // Position record at the found row
                            long slaveRowId = slaveTimeFrameHelper.toRowId(slaveRowIndex);
                            slaveTimeFrameHelper.recordAt(slaveRowId);

                            // Update ASOF map with this slave's key -> rowId
                            MapKey joinKey = asOfJoinMap.withKey();
                            joinKey.put(slaveRecord, slaveKeyCopier);
                            MapValue joinValue = joinKey.createValue();
                            joinValue.putLong(0, slaveRowId);
                        }

                        // Look up master's join key in the ASOF map
                        MapKey lookupKey = asOfJoinMap.withKey();
                        lookupKey.put(record, masterKeyCopier);
                        MapValue lookupValue = lookupKey.findValue();

                        if (lookupValue != null) {
                            long matchedRowId = lookupValue.getLong(0);
                            slaveTimeFrameHelper.recordAt(matchedRowId);
                            matchedSlaveRecord = slaveRecord;
                        }
                    }

                    // Set up combined record (pass sec_offs for GROUP BY key)
                    combinedRecord.of(record, secOffsValue, matchedSlaveRecord);

                    // Aggregate
                    MapKey key = partialMap.withKey();
                    key.put(combinedRecord, groupByKeyCopier);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        functionUpdater.updateNew(value, combinedRecord, baseRowId + r);
                    } else {
                        functionUpdater.updateExisting(value, combinedRecord, baseRowId + r);
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.free(sequenceFactory);
        Misc.free(slaveFactory);
        // Note: slaveFactory is owned by the atom and closed there
        Misc.freeObjList(recordFunctions);
    }
}
