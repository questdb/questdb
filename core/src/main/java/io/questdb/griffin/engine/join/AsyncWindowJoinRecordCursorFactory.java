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

package io.questdb.griffin.engine.join;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
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
import io.questdb.griffin.engine.groupby.DirectMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

// TODO(puzpuzpuz): add support for JIT-compiled filters
// TODO(puzpuzpuz): consider implementing limit and negative limit
// TODO(puzpuzpuz): consider applying time intrinsic intervals/table min/max ts from left-hand to right-hand
public class AsyncWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncWindowJoinRecordCursorFactory::aggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncWindowJoinRecordCursorFactory::filterAndAggregate;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncWindowJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncWindowJoinAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
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
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function masterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(joinMetadata);

        assert masterFactory.supportsPageFrameCursor();
        assert slaveFactory.supportsTimeFrameCursor();

        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.groupByFunctions = groupByFunctions;
        final int columnSplit = masterFactory.getMetadata().getColumnCount();
        this.cursor = new AsyncWindowJoinRecordCursor(
                configuration,
                groupByFunctions,
                slaveFactory.getMetadata(),
                columnSplit,
                masterFilter != null
        );
        final AsyncWindowJoinAtom atom = new AsyncWindowJoinAtom(
                asm,
                configuration,
                slaveFactory,
                joinFilter,
                perWorkerJoinFilters,
                joinWindowLo,
                joinWindowHi,
                columnSplit,
                masterFactory.getMetadata().getTimestampIndex(),
                valueTypes,
                groupByFunctions,
                perWorkerGroupByFunctions,
                compiledMasterFilter,
                bindVarMemory,
                bindVarFunctions,
                masterFilter,
                perWorkerMasterFilters,
                workerCount
        );
        this.frameSequence = new PageFrameSequence<>(
                engine,
                configuration,
                messageBus,
                atom,
                masterFilter != null ? FILTER_AND_AGGREGATE : AGGREGATE,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_WINDOW_JOIN
        );
        this.workerCount = workerCount;
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

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        // The list will hold only group by value slots.
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long joinWindowLo = atom.getJoinWindowLo();
        final long joinWindowHi = atom.getJoinWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final DirectMapValue value = atom.getMapValue(slotId);
        final AsyncWindowJoinAtom.TimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                // TODO(puzpuzpuz): rescale master timestamp to slave unit
                final long slaveTimestampLo = masterTimestamp - joinWindowLo;
                final long slaveTimestampHi = masterTimestamp + joinWindowHi;

                // Now we need to find slave rowid interval to scan.
                long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowId != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (slaveRecord.getTimestamp(slaveTimestampIndex) > slaveTimestampHi) {
                            break;
                        }
                        if (joinFilter.getBool(joinRecord)) {
                            if (value.isNew()) {
                                functionUpdater.updateNew(value, joinRecord, baseSlaveRowId + slaveRowId);
                                value.setNew(false);
                            } else {
                                functionUpdater.updateExisting(value, joinRecord, baseSlaveRowId + slaveRowId);
                            }
                        }
                        if (++slaveRowId >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowId = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final PageFrameSequence<AsyncWindowJoinAtom> frameSequence = task.getFrameSequence(AsyncWindowJoinAtom.class);
        final AsyncWindowJoinAtom atom = frameSequence.getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        // The list will row ids followed by group by value slots.
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long joinWindowLo = atom.getJoinWindowLo();
        final long joinWindowHi = atom.getJoinWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();
        final Function filter = atom.getMasterFilter(slotId);
        final DirectMapValue value = atom.getMapValue(slotId);
        final AsyncWindowJoinAtom.TimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }
            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);

            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

            for (long p = 0; p < filteredRowCount; p++) {
                long r = rows.get(p);
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                // TODO(puzpuzpuz): rescale master timestamp to slave unit
                final long slaveTimestampLo = masterTimestamp - joinWindowLo;
                final long slaveTimestampHi = masterTimestamp + joinWindowHi;

                // Now we need to find slave rowid interval to scan.
                long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowId != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (slaveRecord.getTimestamp(slaveTimestampIndex) > slaveTimestampHi) {
                            break;
                        }
                        if (joinFilter.getBool(joinRecord)) {
                            if (value.isNew()) {
                                functionUpdater.updateNew(value, joinRecord, baseSlaveRowId + slaveRowId);
                                value.setNew(false);
                            } else {
                                functionUpdater.updateExisting(value, joinRecord, baseSlaveRowId + slaveRowId);
                            }
                        }
                        if (++slaveRowId >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowId = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(frameSequence);
        Misc.free(cursor);
    }
}
