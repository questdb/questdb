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

package io.questdb.griffin.engine.join;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
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
import io.questdb.griffin.engine.groupby.FlyweightMapValue;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Multi-threaded WINDOW JOIN factory for general join conditions.
 * <p>
 * Processes master table page frames in parallel using worker threads while
 * joining against the slave table via time frame cursor. Unlike
 * {@link AsyncWindowJoinFastRecordCursorFactory}, does not require a symbol-based
 * join key and handles arbitrary join conditions.
 *
 * @see WindowJoinRecordCursorFactory for the single-threaded variant
 */
public class AsyncWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncWindowJoinRecordCursorFactory::aggregate;
    private static final PageFrameReducer AGGREGATE_DYNAMIC = AsyncWindowJoinRecordCursorFactory::aggregateDynamic;
    private static final PageFrameReducer AGGREGATE_DYNAMIC_PREVAILING = AsyncWindowJoinRecordCursorFactory::aggregateDynamicWithPrevailing;
    private static final PageFrameReducer AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED = AsyncWindowJoinRecordCursorFactory::aggregateDynamicWithPrevailingJoinFiltered;
    private static final PageFrameReducer AGGREGATE_PREVAILING = AsyncWindowJoinRecordCursorFactory::aggregateWithPrevailing;
    private static final PageFrameReducer AGGREGATE_PREVAILING_JOIN_FILTERED = AsyncWindowJoinRecordCursorFactory::aggregateWithPrevailingJoinFiltered;
    private static final PageFrameReducer AGGREGATE_VECT = AsyncWindowJoinRecordCursorFactory::aggregateVect;
    private static final PageFrameReducer AGGREGATE_VECT_PREVAILING = AsyncWindowJoinRecordCursorFactory::aggregateVectWithPrevailing;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncWindowJoinRecordCursorFactory::filterAndAggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_DYNAMIC = AsyncWindowJoinRecordCursorFactory::filterAndAggregateDynamic;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING = AsyncWindowJoinRecordCursorFactory::filterAndAggregateDynamicWithPrevailing;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED = AsyncWindowJoinRecordCursorFactory::filterAndAggregateDynamicWithPrevailingJoinFiltered;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_PREVAILING = AsyncWindowJoinRecordCursorFactory::filterAndAggregateWithPrevailing;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED = AsyncWindowJoinRecordCursorFactory::filterAndAggregateWithPrevailingJoinFiltered;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_VECT = AsyncWindowJoinRecordCursorFactory::filterAndAggregateVect;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_VECT_PREVAILING = AsyncWindowJoinRecordCursorFactory::filterAndAggregateVectWithPrevailing;

    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncWindowJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncWindowJoinAtom> frameSequence;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncWindowJoinRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull MessageBus messageBus,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordMetadata outMetadata,
            @Nullable IntList columnIndex,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            boolean includePrevailing,
            @Nullable Function joinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            long windowLo,
            long windowHi,
            @Nullable Function windowLoFunc,
            @Nullable Function windowHiFunc,
            @Nullable ObjList<Function> perWorkerWindowLoFuncs,
            @Nullable ObjList<Function> perWorkerWindowHiFuncs,
            int loSign,
            int hiSign,
            char loTimeUnit,
            char hiTimeUnit,
            @Nullable TimestampDriver timestampDriver,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function masterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean vectorized,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(outMetadata);
        assert masterFactory.supportsPageFrameCursor();
        assert slaveFactory.supportsTimeFrameCursor();

        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        final int columnSplit = masterFactory.getMetadata().getColumnCount();
        this.joinMetadata = joinMetadata;
        this.cursor = new AsyncWindowJoinRecordCursor(
                groupByFunctions,
                slaveFactory.getMetadata(),
                columnIndex,
                columnSplit,
                masterFilter != null
        );

        final int masterTsType = masterFactory.getMetadata().getTimestampType();
        final int slaveTsType = slaveFactory.getMetadata().getTimestampType();
        long masterTsScale = 1;
        long slaveTsScale = 1;
        if (masterTsType != slaveTsType) {
            masterTsScale = ColumnType.getTimestampDriver(masterTsType).toNanosScale();
            slaveTsScale = ColumnType.getTimestampDriver(slaveTsType).toNanosScale();
        }
        final boolean isDynamicWindow = windowLoFunc != null || windowHiFunc != null;
        final AsyncWindowJoinAtom atom = new AsyncWindowJoinAtom(
                asm,
                configuration,
                slaveFactory,
                joinFilter,
                perWorkerJoinFilters,
                windowLo,
                windowHi,
                windowLoFunc,
                windowHiFunc,
                perWorkerWindowLoFuncs,
                perWorkerWindowHiFuncs,
                loSign,
                hiSign,
                loTimeUnit,
                hiTimeUnit,
                timestampDriver,
                includePrevailing,
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
                filterUsedColumnIndexes,
                vectorized,
                masterTsScale,
                slaveTsScale,
                workerCount
        );

        PageFrameReducer reducer;
        if (isDynamicWindow) {
            if (includePrevailing) {
                if (joinFilter != null) {
                    reducer = masterFilter != null ? FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED : AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED;
                } else {
                    reducer = masterFilter != null ? FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING : AGGREGATE_DYNAMIC_PREVAILING;
                }
            } else {
                reducer = masterFilter != null ? FILTER_AND_AGGREGATE_DYNAMIC : AGGREGATE_DYNAMIC;
            }
        } else {
            if (includePrevailing) {
                if (masterFilter != null) {
                    reducer = atom.isVectorized()
                            ? FILTER_AND_AGGREGATE_VECT_PREVAILING
                            : (joinFilter == null ? FILTER_AND_AGGREGATE_PREVAILING : FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED);
                } else {
                    reducer = atom.isVectorized()
                            ? AGGREGATE_VECT_PREVAILING
                            : (joinFilter == null ? AGGREGATE_PREVAILING : AGGREGATE_PREVAILING_JOIN_FILTERED);
                }
            } else {
                reducer = masterFilter != null
                        ? (atom.isVectorized() ? FILTER_AND_AGGREGATE_VECT : FILTER_AND_AGGREGATE)
                        : (atom.isVectorized() ? AGGREGATE_VECT : AGGREGATE);
            }
        }
        this.frameSequence = new PageFrameSequence<>(
                engine,
                configuration,
                messageBus,
                atom,
                reducer,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_WINDOW_JOIN
        );
        this.workerCount = workerCount;
    }

    @Override
    public PageFrameSequence<AsyncWindowJoinAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        CairoConfiguration config = executionContext.getCairoEngine().getConfiguration();
        executionContext.changePageFrameSizes(config.getSqlSmallPageFrameMinRows(), config.getSqlSmallPageFrameMaxRows());
        try {
            return frameSequence.of(masterFactory, executionContext, collectSubSeq, order);
        } finally {
            executionContext.restoreToDefaultPageFrameSizes();
        }
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
        final AsyncWindowJoinAtom atom = frameSequence.getAtom();
        sink.attr("vectorized").val(atom.isVectorized());
        if (atom.getJoinFilter(0) != null) {
            sink.setMetadata(joinMetadata);
            sink.attr("join filter").val(atom.getJoinFilter(0));
            sink.setMetadata(null);
        }
        sink.val(atom);
        if (atom.getMasterFilter(0) != null) {
            sink.attr("master filter").val(atom.getMasterFilter(0), masterFactory);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                    slaveTimestamps.add(slaveTimestamp);

                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        // don't forget to switch the record to the new frame
                        slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    }
                }
            }

            long rowLo = 0;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        for (long i = rowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateDynamic(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        final Function windowLoFunc = atom.getWindowLoFunc(slotId);
        final Function windowHiFunc = atom.getWindowHiFunc(slotId);
        final int loSign = atom.getLoSign();
        final int hiSign = atom.getHiSign();
        final char loTimeUnit = atom.getLoTimeUnit();
        final char hiTimeUnit = atom.getHiTimeUnit();
        final TimestampDriver timestampDriver = atom.getTimestampDriver();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

            // First pass: compute overall slave bounds from all master rows.
            long overallSlaveLo = Long.MAX_VALUE;
            long overallSlaveHi = Long.MIN_VALUE;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }
                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                if (lo < overallSlaveLo) {
                    overallSlaveLo = lo;
                }
                if (hi > overallSlaveHi) {
                    overallSlaveHi = hi;
                }
            }

            // Pre-scan slave data in overall range.
            if (overallSlaveLo <= overallSlaveHi) {
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(overallSlaveLo, overallSlaveHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > overallSlaveHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            // Per-row aggregation with dynamic bounds.
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    // Search from 0 since dynamic bounds don't guarantee monotonic slave windows.
                    long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        for (long i = rowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateDynamicWithPrevailing(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        final Function windowLoFunc = atom.getWindowLoFunc(slotId);
        final Function windowHiFunc = atom.getWindowHiFunc(slotId);
        final int loSign = atom.getLoSign();
        final int hiSign = atom.getHiSign();
        final char loTimeUnit = atom.getLoTimeUnit();
        final char hiTimeUnit = atom.getHiTimeUnit();
        final TimestampDriver timestampDriver = atom.getTimestampDriver();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

            // First pass: compute overall slave bounds from all master rows.
            long overallSlaveLo = Long.MAX_VALUE;
            long overallSlaveHi = Long.MIN_VALUE;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }
                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                if (lo < overallSlaveLo) {
                    overallSlaveLo = lo;
                }
                if (hi > overallSlaveHi) {
                    overallSlaveHi = hi;
                }
            }

            // Pre-scan slave data in overall range (with prevailing).
            if (overallSlaveLo <= overallSlaveHi) {
                long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(overallSlaveLo, overallSlaveHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > overallSlaveHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            // Per-row aggregation with dynamic bounds (prevailing binary search).
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    // Search from 0 since dynamic bounds don't guarantee monotonic slave windows.
                    long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        for (long i = rowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (value.isNew()) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                            } else {
                                functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateDynamicWithPrevailingJoinFiltered(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        final Function windowLoFunc = atom.getWindowLoFunc(slotId);
        final Function windowHiFunc = atom.getWindowHiFunc(slotId);
        final int loSign = atom.getLoSign();
        final int hiSign = atom.getHiSign();
        final char loTimeUnit = atom.getLoTimeUnit();
        final char hiTimeUnit = atom.getHiTimeUnit();
        final TimestampDriver timestampDriver = atom.getTimestampDriver();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

            // First pass: compute overall slave bounds from all master rows.
            long overallSlaveLo = Long.MAX_VALUE;
            long overallSlaveHi = Long.MIN_VALUE;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }
                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                if (lo < overallSlaveLo) {
                    overallSlaveLo = lo;
                }
                if (hi > overallSlaveHi) {
                    overallSlaveHi = hi;
                }
            }

            int prevailingFrameIndex = -1;
            long prevailingRowIndex = Long.MIN_VALUE;

            // Pre-scan slave data in overall range, tracking prevailing position for backward scan fallback.
            if (overallSlaveLo <= overallSlaveHi) {
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(overallSlaveLo, overallSlaveHi, true);
                prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();

                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > overallSlaveHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            // Per-row aggregation with dynamic bounds + join filter prevailing scan.
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                    continue;
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                final long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                boolean needToFindPrevailing = true;
                if (slaveTimestamps.size() > 0) {
                    // Search from 0 since dynamic bounds don't guarantee monotonic slave windows.
                    long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        // Check if one of the first rows at slaveTimestampLo matches the join filter.
                        long adjustedRowLo = rowLo;
                        for (long i = rowLo; i < rowHi; i++) {
                            if (slaveTimestamps.get(i) > slaveTimestampLo) {
                                break;
                            }
                            adjustedRowLo++;
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                                needToFindPrevailing = false;
                                break;
                            }
                        }

                        // Do a backward scan to find the prevailing row.
                        if (needToFindPrevailing) {
                            // First check the accumulated row ids before the window.
                            for (long i = rowLo - 1; i >= 0; i--) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }
                            // If no luck, do the backward scan in the time frame cursor.
                            if (needToFindPrevailing) {
                                findPrevailingForMasterRow(
                                        slaveTimeFrameHelper,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        joinFilter,
                                        joinRecord,
                                        functionUpdater,
                                        value
                                );
                            }
                        }

                        // Aggregate the rows within the time window.
                        for (long i = adjustedRowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    } else {
                        // No slave rows in the window. Find the prevailing row.
                        for (long i = rowLo - 1; i >= 0; i--) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                                needToFindPrevailing = false;
                                break;
                            }
                        }
                        if (needToFindPrevailing) {
                            findPrevailingForMasterRow(
                                    slaveTimeFrameHelper,
                                    prevailingFrameIndex,
                                    prevailingRowIndex,
                                    joinFilter,
                                    joinRecord,
                                    functionUpdater,
                                    value
                            );
                        }
                    }
                } else {
                    // No slave rows at all. Find the prevailing row.
                    findPrevailingForMasterRow(
                            slaveTimeFrameHelper,
                            prevailingFrameIndex,
                            prevailingRowIndex,
                            joinFilter,
                            joinRecord,
                            functionUpdater,
                            value
                    );
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateVect(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
        final GroupByColumnSink columnSink = atom.getColumnSink(slotId);

        final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
        final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
        final int columnCount = groupByFuncArgs.size();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveColumnSinkPtrs = atom.getLongList(slotId);
        slaveColumnSinkPtrs.of(0);
        slaveColumnSinkPtrs.checkCapacity(columnCount);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    slaveTimestamps.add(slaveTimestamp);

                    for (int i = 0; i < columnCount; i++) {
                        var funcArg = groupByFuncArgs.getQuick(i);
                        if (funcArg != null) {
                            long ptr = slaveColumnSinkPtrs.get(i);
                            columnSink.of(ptr).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                            slaveColumnSinkPtrs.set(i, columnSink.ptr());
                        }
                    }

                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        // don't forget to switch the record to the new frame
                        slaveTimeFrameHelper.recordAt(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    }
                }
            }

            // Now iterate through master rows and perform batch aggregation with time window filtering
            long rowLo = 0;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                    groupByFunctions.getQuick(i).setEmpty(value);
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                    IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();

                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        final int mapIndex = mapIndexes.getQuick(i);
                        final long ptr = slaveColumnSinkPtrs.get(mapIndex);
                        if (ptr != 0) {
                            final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                            groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo), 0);
                        } else { // no-arg function, e.g. count()
                            groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo), 0);
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateVectWithPrevailing(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
        final GroupByColumnSink columnSink = atom.getColumnSink(slotId);

        final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
        final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
        final int columnCount = groupByFuncArgs.size();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveColumnSinkPtrs = atom.getLongList(slotId);
        slaveColumnSinkPtrs.of(0);
        slaveColumnSinkPtrs.checkCapacity(columnCount);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    slaveTimestamps.add(slaveTimestamp);

                    for (int i = 0; i < columnCount; i++) {
                        var funcArg = groupByFuncArgs.getQuick(i);
                        if (funcArg != null) {
                            long ptr = slaveColumnSinkPtrs.get(i);
                            columnSink.of(ptr).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                            slaveColumnSinkPtrs.set(i, columnSink.ptr());
                        }
                    }

                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        // don't forget to switch the record to the new frame
                        slaveTimeFrameHelper.recordAt(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    }
                }
            }

            long rowLo = 0;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                    groupByFunctions.getQuick(i).setEmpty(value);
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();
                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            final int mapIndex = mapIndexes.getQuick(i);
                            final long ptr = slaveColumnSinkPtrs.get(mapIndex);
                            if (ptr != 0) {
                                final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo), 0);
                            } else { // no-arg function, e.g. count()
                                groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo), 0);
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateWithPrevailing(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                    slaveTimestamps.add(slaveTimestamp);

                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    }
                }
            }

            long rowLo = 0;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                if (slaveTimestamps.size() > 0) {
                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        for (long i = rowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (value.isNew()) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                            } else {
                                functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void aggregateWithPrevailingJoinFiltered(
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
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);

        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long windowLo = atom.getWindowLo();
        final long windowHi = atom.getWindowHi();
        final long valueSizeInBytes = atom.getValueSizeBytes();
        assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
        final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final Function joinFilter = atom.getJoinFilter(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        slaveRowIds.of(0);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
        slaveTimestamps.of(0);

        try {
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);
            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
            final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
            final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();

            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                    slaveTimestamps.add(slaveTimestamp);

                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        // don't forget to switch the record to the new frame
                        slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    }
                }
            }

            long rowLo = 0;
            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                boolean needToFindPrevailing = true;
                if (slaveTimestamps.size() > 0) {
                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo < rowHi) {
                        // First, check if one of the first rows matching the join filter is also at the masterSlaveTimestampLo timestamp.
                        // If so, we don't need to do backward scan to find the prevailing row.
                        long adjustedRowLo = rowLo;
                        for (long i = rowLo; i < rowHi; i++) {
                            if (slaveTimestamps.get(i) > masterSlaveTimestampLo) {
                                break;
                            }
                            adjustedRowLo++;
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                                needToFindPrevailing = false;
                                break;
                            }
                        }

                        // Do a backward scan to find the prevailing row.
                        if (needToFindPrevailing) {
                            // First check the accumulated row ids.
                            for (long i = rowLo - 1; i >= 0; i--) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }
                            // If no luck, do the backward scan.
                            if (needToFindPrevailing) {
                                findPrevailingForMasterRow(
                                        slaveTimeFrameHelper,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        joinFilter,
                                        joinRecord,
                                        functionUpdater,
                                        value
                                );
                            }
                        }

                        // At last, process time window rows.
                        for (long i = adjustedRowLo; i < rowHi; i++) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    } else {
                        // There are no slave rows corresponding to the time window.
                        // Let's find the prevailing value.

                        // First, check the accumulated row ids.
                        for (long i = rowLo - 1; i >= 0; i--) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(joinRecord)) {
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                                needToFindPrevailing = false;
                                break;
                            }
                        }
                        // If the prevailing row is not there, we have to do the backward scan.
                        if (needToFindPrevailing) {
                            findPrevailingForMasterRow(
                                    slaveTimeFrameHelper,
                                    prevailingFrameIndex,
                                    prevailingRowIndex,
                                    joinFilter,
                                    joinRecord,
                                    functionUpdater,
                                    value
                            );
                        }
                    }
                } else {
                    // There are no slave rows corresponding to the master page frame.
                    // Let's find the prevailing value.
                    findPrevailingForMasterRow(
                            slaveTimeFrameHelper,
                            prevailingFrameIndex,
                            prevailingRowIndex,
                            joinFilter,
                            joinRecord,
                            functionUpdater,
                            value
                    );
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
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final Function joinFilter = atom.getJoinFilter(slotId);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

                // Collect all slave data for the filtered frame's timestamp range
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }

                // Now iterate through filtered master rows and perform batch aggregation
                long rowLo = 0;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            for (long i = rowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                    if (value.isNew()) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                    } else {
                                        functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateDynamic(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final Function joinFilter = atom.getJoinFilter(slotId);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                final Function windowLoFunc = atom.getWindowLoFunc(slotId);
                final Function windowHiFunc = atom.getWindowHiFunc(slotId);
                final int loSign = atom.getLoSign();
                final int hiSign = atom.getHiSign();
                final char loTimeUnit = atom.getLoTimeUnit();
                final char hiTimeUnit = atom.getHiTimeUnit();
                final TimestampDriver timestampDriver = atom.getTimestampDriver();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                // First pass: compute overall slave bounds from filtered master rows.
                long overallSlaveLo = Long.MAX_VALUE;
                long overallSlaveHi = Long.MIN_VALUE;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    record.setRowIndex(rows.get(p));
                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }
                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                    if (lo < overallSlaveLo) {
                        overallSlaveLo = lo;
                    }
                    if (hi > overallSlaveHi) {
                        overallSlaveHi = hi;
                    }
                }

                // Pre-scan slave data in overall range.
                if (overallSlaveLo <= overallSlaveHi) {
                    long slaveRowIndex = slaveTimeFrameHelper.findRowLo(overallSlaveLo, overallSlaveHi);
                    if (slaveRowIndex != Long.MIN_VALUE) {
                        long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        for (; ; ) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                            final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                            if (slaveTimestamp > overallSlaveHi) {
                                break;
                            }
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);

                            if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                                if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                    break;
                                }
                                slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                                baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                                slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                            }
                        }
                    }
                }

                // Per-row aggregation with dynamic bounds.
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            for (long i = rowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                    if (value.isNew()) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                    } else {
                                        functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateDynamicWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                final Function windowLoFunc = atom.getWindowLoFunc(slotId);
                final Function windowHiFunc = atom.getWindowHiFunc(slotId);
                final int loSign = atom.getLoSign();
                final int hiSign = atom.getHiSign();
                final char loTimeUnit = atom.getLoTimeUnit();
                final char hiTimeUnit = atom.getHiTimeUnit();
                final TimestampDriver timestampDriver = atom.getTimestampDriver();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                // First pass: compute overall slave bounds from filtered master rows.
                long overallSlaveLo = Long.MAX_VALUE;
                long overallSlaveHi = Long.MIN_VALUE;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    record.setRowIndex(rows.get(p));
                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }
                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                    if (lo < overallSlaveLo) {
                        overallSlaveLo = lo;
                    }
                    if (hi > overallSlaveHi) {
                        overallSlaveHi = hi;
                    }
                }

                // Pre-scan slave data in overall range (with prevailing).
                if (overallSlaveLo <= overallSlaveHi) {
                    long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(overallSlaveLo, overallSlaveHi);
                    if (slaveRowIndex != Long.MIN_VALUE) {
                        long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        for (; ; ) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                            final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                            if (slaveTimestamp > overallSlaveHi) {
                                break;
                            }
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);

                            if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                                if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                    break;
                                }
                                slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                                baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                                slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                            }
                        }
                    }
                }

                // Per-row aggregation with dynamic bounds (prevailing binary search).
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            for (long i = rowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateDynamicWithPrevailingJoinFiltered(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final Function joinFilter = atom.getJoinFilter(slotId);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                final Function windowLoFunc = atom.getWindowLoFunc(slotId);
                final Function windowHiFunc = atom.getWindowHiFunc(slotId);
                final int loSign = atom.getLoSign();
                final int hiSign = atom.getHiSign();
                final char loTimeUnit = atom.getLoTimeUnit();
                final char hiTimeUnit = atom.getHiTimeUnit();
                final TimestampDriver timestampDriver = atom.getTimestampDriver();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                // First pass: compute overall slave bounds from filtered master rows.
                long overallSlaveLo = Long.MAX_VALUE;
                long overallSlaveHi = Long.MIN_VALUE;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    record.setRowIndex(rows.get(p));
                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }
                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    long lo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    long hi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);
                    if (lo < overallSlaveLo) {
                        overallSlaveLo = lo;
                    }
                    if (hi > overallSlaveHi) {
                        overallSlaveHi = hi;
                    }
                }

                int prevailingFrameIndex = -1;
                long prevailingRowIndex = Long.MIN_VALUE;

                // Pre-scan slave data in overall range, tracking prevailing position.
                if (overallSlaveLo <= overallSlaveHi) {
                    long slaveRowIndex = slaveTimeFrameHelper.findRowLo(overallSlaveLo, overallSlaveHi, true);
                    prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                    prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();

                    if (slaveRowIndex != Long.MIN_VALUE) {
                        long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        for (; ; ) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                            final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                            if (slaveTimestamp > overallSlaveHi) {
                                break;
                            }
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);

                            if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                                if (!slaveTimeFrameHelper.nextFrame(overallSlaveHi)) {
                                    break;
                                }
                                slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                                baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                                slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                            }
                        }
                    }
                }

                // Per-row aggregation with dynamic bounds + join filter prevailing scan.
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    long effectiveLo = computeEffectiveBound(windowLoFunc, windowLo, record, loSign, loTimeUnit, timestampDriver);
                    long effectiveHi = computeEffectiveBound(windowHiFunc, windowHi, record, hiSign, hiTimeUnit, timestampDriver);
                    if (effectiveLo == Long.MIN_VALUE || effectiveHi == Long.MIN_VALUE) {
                        continue;
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long slaveTimestampLo = scaleTimestamp(subtractSaturating(masterTimestamp, effectiveLo), masterTsScale);
                    final long slaveTimestampHi = scaleTimestamp(addSaturating(masterTimestamp, effectiveHi), masterTsScale);

                    boolean needToFindPrevailing = true;
                    if (slaveTimestamps.size() > 0) {
                        long rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, 0, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            // Check if one of the first rows at slaveTimestampLo matches the join filter.
                            long adjustedRowLo = rowLo;
                            for (long i = rowLo; i < rowHi; i++) {
                                if (slaveTimestamps.get(i) > slaveTimestampLo) {
                                    break;
                                }
                                adjustedRowLo++;
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }

                            // Do a backward scan to find the prevailing row.
                            if (needToFindPrevailing) {
                                for (long i = rowLo - 1; i >= 0; i--) {
                                    final long slaveRowId = slaveRowIds.get(i);
                                    slaveTimeFrameHelper.recordAt(slaveRowId);
                                    if (joinFilter.getBool(joinRecord)) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                        needToFindPrevailing = false;
                                        break;
                                    }
                                }
                                if (needToFindPrevailing) {
                                    findPrevailingForMasterRow(
                                            slaveTimeFrameHelper,
                                            prevailingFrameIndex,
                                            prevailingRowIndex,
                                            joinFilter,
                                            joinRecord,
                                            functionUpdater,
                                            value
                                    );
                                }
                            }

                            // Aggregate the rows within the time window.
                            for (long i = adjustedRowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    if (value.isNew()) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                    } else {
                                        functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                    }
                                }
                            }
                        } else {
                            // No slave rows in the window. Find the prevailing row.
                            for (long i = rowLo - 1; i >= 0; i--) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }
                            if (needToFindPrevailing) {
                                findPrevailingForMasterRow(
                                        slaveTimeFrameHelper,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        joinFilter,
                                        joinRecord,
                                        functionUpdater,
                                        value
                                );
                            }
                        }
                    } else {
                        // No slave rows at all. Find the prevailing row.
                        findPrevailingForMasterRow(
                                slaveTimeFrameHelper,
                                prevailingFrameIndex,
                                prevailingRowIndex,
                                joinFilter,
                                joinRecord,
                                functionUpdater,
                                value
                        );
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
                final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
                final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
                final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
                final int columnCount = groupByFuncArgs.size();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveColumnSinkPtrs = atom.getLongList(slotId);
                slaveColumnSinkPtrs.of(0);
                slaveColumnSinkPtrs.checkCapacity(columnCount);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

                // Collect all slave data for the filtered frame's timestamp range
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        slaveTimestamps.add(slaveTimestamp);

                        for (int i = 0; i < columnCount; i++) {
                            var funcArg = groupByFuncArgs.getQuick(i);
                            if (funcArg != null) {
                                long ptr = slaveColumnSinkPtrs.get(i);
                                columnSink.of(ptr).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                slaveColumnSinkPtrs.set(i, columnSink.ptr());
                            }
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        }
                    }
                }

                // Now iterate through filtered master rows and perform batch aggregation
                long rowLo = 0;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        groupByFunctions.getQuick(i).setEmpty(value);
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        // Use binary search to find the range of slave rows for this master row
                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            final int mapIndex = mapIndexes.getQuick(i);
                            final long ptr = slaveColumnSinkPtrs.get(mapIndex);
                            if (ptr != 0) {
                                final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo), 0);
                            } else { // no-arg function, e.g. count()
                                groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo), 0);
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateVectWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
                final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
                final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
                final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
                final int columnCount = groupByFuncArgs.size();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveColumnSinkPtrs = atom.getLongList(slotId);
                slaveColumnSinkPtrs.of(0);
                slaveColumnSinkPtrs.checkCapacity(columnCount);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

                long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        slaveTimestamps.add(slaveTimestamp);

                        for (int i = 0; i < columnCount; i++) {
                            var funcArg = groupByFuncArgs.getQuick(i);
                            if (funcArg != null) {
                                long ptr = slaveColumnSinkPtrs.get(i);
                                columnSink.of(ptr).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                slaveColumnSinkPtrs.set(i, columnSink.ptr());
                            }
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                        }
                    }
                }

                long rowLo = 0;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        groupByFunctions.getQuick(i).setEmpty(value);
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();
                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                                final int mapIndex = mapIndexes.getQuick(i);
                                final long ptr = slaveColumnSinkPtrs.get(mapIndex);
                                if (ptr != 0) {
                                    final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                    groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo), 0);
                                } else { // no-arg function, e.g. count()
                                    groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo), 0);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);

                long slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }

                // Process filtered master rows
                long rowLo = 0;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                    if (slaveTimestamps.size() > 0) {
                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? Math.max(-rowLo - 2, 0) : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            for (long i = rowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (value.isNew()) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                } else {
                                    functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    private static void filterAndAggregateWithPrevailingJoinFiltered(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinAtom atom = task.getFrameSequence(AsyncWindowJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final PageFrameMemory frameMemory;
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);

        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.needsColumnTypeCast()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long windowLo = atom.getWindowLo();
                final long windowHi = atom.getWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final Function joinFilter = atom.getJoinFilter(slotId);

                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                slaveRowIds.of(0);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);
                slaveTimestamps.of(0);

                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - windowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + windowHi, masterTsScale);
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();

                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }

                // Process filtered master rows
                long rowLo = 0;
                for (long p = 0; p < filteredRowCount; p++) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTsScale);

                    boolean needToFindPrevailing = true;
                    if (slaveTimestamps.size() > 0) {
                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo < rowHi) {
                            // First, check if one of the first rows matching the join filter is also at the masterSlaveTimestampLo timestamp.
                            // If so, we don't need to do backward scan to find the prevailing row.
                            long adjustedRowLo = rowLo;
                            for (long i = rowLo; i < rowHi; i++) {
                                if (slaveTimestamps.get(i) > masterSlaveTimestampLo) {
                                    break;
                                }
                                adjustedRowLo++;
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }

                            // Do a backward scan to find the prevailing row.
                            if (needToFindPrevailing) {
                                // First check the accumulated row ids.
                                for (long i = rowLo - 1; i >= 0; i--) {
                                    final long slaveRowId = slaveRowIds.get(i);
                                    slaveTimeFrameHelper.recordAt(slaveRowId);
                                    if (joinFilter.getBool(joinRecord)) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                        needToFindPrevailing = false;
                                        break;
                                    }
                                }
                                // If no luck, do the backward scan.
                                if (needToFindPrevailing) {
                                    findPrevailingForMasterRow(
                                            slaveTimeFrameHelper,
                                            prevailingFrameIndex,
                                            prevailingRowIndex,
                                            joinFilter,
                                            joinRecord,
                                            functionUpdater,
                                            value
                                    );
                                }
                            }

                            // At last, process time window rows.
                            for (long i = adjustedRowLo; i < rowHi; i++) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    if (value.isNew()) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        value.setNew(false);
                                    } else {
                                        functionUpdater.updateExisting(value, joinRecord, slaveRowId);
                                    }
                                }
                            }
                        } else {
                            // There are no slave rows corresponding to the time window.
                            // Let's find the prevailing value.

                            // First, check the accumulated row ids.
                            for (long i = rowLo - 1; i >= 0; i--) {
                                final long slaveRowId = slaveRowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter.getBool(joinRecord)) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                    needToFindPrevailing = false;
                                    break;
                                }
                            }
                            // If the prevailing row is not there, we have to do the backward scan.
                            if (needToFindPrevailing) {
                                findPrevailingForMasterRow(
                                        slaveTimeFrameHelper,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        joinFilter,
                                        joinRecord,
                                        functionUpdater,
                                        value
                                );
                            }
                        }
                    } else {
                        // There are no slave rows corresponding to the master page frame for the symbol.
                        // Let's find the prevailing value.
                        findPrevailingForMasterRow(
                                slaveTimeFrameHelper,
                                prevailingFrameIndex,
                                prevailingRowIndex,
                                joinFilter,
                                joinRecord,
                                functionUpdater,
                                value
                        );
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Returns {@code a + b}, clamping to {@link Long#MAX_VALUE} on positive overflow
     * and {@link Long#MIN_VALUE} on negative overflow instead of wrapping.
     */
    static long addSaturating(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            // Positive overflow (b > 0): clamp to the largest timestamp (end of time).
            // Negative overflow (b <= 0): clamp to the smallest timestamp (start of time).
            return b > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
    }

    /**
     * Computes the effective window bound value for a dynamic WINDOW JOIN bound.
     * <p>
     * If the bound function is null, this method returns the pre-computed constant value.
     * Otherwise, it evaluates the function against the current master record,
     * applies the time unit conversion, and signs the result.
     *
     * @param func          the dynamic bound function, or null for static bounds
     * @param constantValue the pre-computed constant value (used when func is null)
     * @param masterRecord  the current master row record
     * @param sign          +1 for lo-PRECEDING / hi-FOLLOWING, -1 for lo-FOLLOWING / hi-PRECEDING
     * @param timeUnit      the time unit character (0 if no unit conversion is needed)
     * @param driver        the timestamp driver for unit conversion (may be null if timeUnit is 0)
     * @return the effective bound value, or {@link Long#MIN_VALUE} if the function returned NULL (empty window)
     */
    static long computeEffectiveBound(
            @Nullable Function func,
            long constantValue,
            Record masterRecord,
            int sign,
            char timeUnit,
            @Nullable TimestampDriver driver
    ) {
        if (func == null) {
            return constantValue;
        }
        long raw = func.getLong(masterRecord);
        if (raw == Numbers.LONG_NULL) {
            return Long.MIN_VALUE;
        }
        if (raw < 0) {
            raw = 0;
        }
        long scaled = (timeUnit != 0 && driver != null) ? driver.from(raw, timeUnit) : raw;
        // raw >= 0, and from() multiplies by a positive constant, so a negative
        // result means the multiplication overflowed. Saturate to Long.MAX_VALUE.
        if (scaled < 0) {
            scaled = Long.MAX_VALUE;
        }
        return sign * scaled;
    }

    static void findPrevailingForMasterRow(
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            int prevailingFrameIndex,
            long prevailingRowIndex,
            Function joinFilter,
            JoinRecord joinRecord,
            GroupByFunctionsUpdater functionUpdater,
            MapValue value
    ) {
        if (prevailingFrameIndex == -1) {
            return;
        }

        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();
        long scanStart = prevailingRowIndex;

        try {
            slaveTimeFrameHelper.restoreBookmark(prevailingFrameIndex, 0);
            do {
                // actual row index doesn't matter here due to the later recordAtRowIndex() call
                slaveTimeFrameHelper.recordAt(slaveTimeFrameHelper.getTimeFrameIndex(), 0);

                long rowLo = slaveTimeFrameHelper.getTimeFrameRowLo();
                long rowHi = slaveTimeFrameHelper.getTimeFrameRowHi();
                if (scanStart >= rowHi) {
                    scanStart = rowHi - 1;
                }
                if (scanStart < rowLo) {
                    scanStart = Long.MAX_VALUE;
                    continue;
                }

                for (long r = scanStart; r >= rowLo; r--) {
                    slaveTimeFrameHelper.recordAtRowIndex(r);
                    if (joinFilter.getBool(joinRecord)) {
                        functionUpdater.updateNew(value, joinRecord, Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), r));
                        value.setNew(false);
                        return;
                    }
                }
                scanStart = Long.MAX_VALUE;
            } while (slaveTimeFrameHelper.previousFrame());
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
        }
    }

    /**
     * Returns {@code a - b}, clamping to {@link Long#MIN_VALUE} on negative overflow
     * and {@link Long#MAX_VALUE} on positive overflow instead of wrapping.
     */
    static long subtractSaturating(long a, long b) {
        try {
            return Math.subtractExact(a, b);
        } catch (ArithmeticException e) {
            // Negative overflow (b > 0): clamp to the smallest timestamp (start of time).
            // Positive overflow (b <= 0): clamp to the largest timestamp (end of time).
            return b > 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    @Override
    protected void _close() {
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.free(joinMetadata);
    }
}
