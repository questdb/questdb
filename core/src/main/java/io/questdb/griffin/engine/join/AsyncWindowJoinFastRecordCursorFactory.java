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

package io.questdb.griffin.engine.join;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
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
import io.questdb.griffin.engine.table.AsyncFilterUtils;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.join.AsyncWindowJoinFastAtom.toSymbolMapKey;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;

/**
 * Multi-threaded WINDOW JOIN factory with symbol-based join key optimization.
 * <p>
 * Processes master table page frames in parallel using worker threads while
 * joining against the slave table via time frame cursor. Requires an equality
 * condition on symbol columns between master and slave tables.
 *
 * @see WindowJoinFastRecordCursorFactory for the single-threaded variant
 */
public class AsyncWindowJoinFastRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncWindowJoinFastRecordCursorFactory::aggregate;
    private static final PageFrameReducer AGGREGATE_PREVAILING = AsyncWindowJoinFastRecordCursorFactory::aggregateWithPrevailing;
    private static final PageFrameReducer AGGREGATE_PREVAILING_JOIN_FILTERED = AsyncWindowJoinFastRecordCursorFactory::aggregateWithPrevailingJoinFiltered;
    private static final PageFrameReducer AGGREGATE_VECT = AsyncWindowJoinFastRecordCursorFactory::aggregateVect;
    private static final PageFrameReducer AGGREGATE_VECT_PREVAILING = AsyncWindowJoinFastRecordCursorFactory::aggregateVectWithPrevailing;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncWindowJoinFastRecordCursorFactory::filterAndAggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_PREVAILING = AsyncWindowJoinFastRecordCursorFactory::filterAndAggregateWithPrevailing;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED = AsyncWindowJoinFastRecordCursorFactory::filterAndAggregateWithPrevailingJoinFiltered;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_VECT = AsyncWindowJoinFastRecordCursorFactory::filterAndAggregateVect;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_VECT_PREVAILING = AsyncWindowJoinFastRecordCursorFactory::filterAndAggregateVectWithPrevailing;

    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncWindowJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncWindowJoinFastAtom> frameSequence;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncWindowJoinFastRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordMetadata outMetadata,
            @Nullable IntList columnIndex,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function joinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            boolean includePrevailing,
            int masterSymbolIndex,
            int slaveSymbolIndex,
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
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean vectorized,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(outMetadata);
        this.joinMetadata = joinMetadata;
        assert masterFactory.supportsPageFrameCursor();
        assert slaveFactory.supportsTimeFrameCursor();

        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        final int columnSplit = masterFactory.getMetadata().getColumnCount();
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
        final AsyncWindowJoinFastAtom atom = new AsyncWindowJoinFastAtom(
                asm,
                configuration,
                slaveFactory,
                joinFilter,
                perWorkerJoinFilters,
                masterSymbolIndex,
                slaveSymbolIndex,
                joinWindowLo,
                joinWindowHi,
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
            reducer = atom.isVectorized()
                    ? (masterFilter != null ? FILTER_AND_AGGREGATE_VECT : AGGREGATE_VECT)
                    : (masterFilter != null ? FILTER_AND_AGGREGATE : AGGREGATE);
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
    public PageFrameSequence<AsyncWindowJoinFastAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
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
        sink.type("Async Window Fast Join");
        sink.meta("workers").val(workerCount);
        final AsyncWindowJoinFastAtom atom = frameSequence.getAtom();
        sink.attr("vectorized").val(atom.isVectorized());
        sink.attr("symbol")
                .val(masterFactory.getMetadata().getColumnName(atom.getMasterSymbolIndex()))
                .val("=")
                .val(slaveFactory.getMetadata().getColumnName(atom.getSlaveSymbolIndex()));
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

    /**
     * Aggregate (no join filter).
     */
    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();

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
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();
        final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
        final Function joinFilter = atom.getJoinFilter(slotId);

        atom.clearTemporaryData(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

        try {
            final int masterSymbolIndex = atom.getMasterSymbolIndex();
            final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            // First, build the in-memory index. For every master key in this page frame,
            // it stores rowids and timestamps of the matching slave rows.
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = toSymbolMapKey(matchingMasterKey);
                        slaveRowIds.of(slaveData.get(idx, 0));
                        slaveTimestamps.of(slaveData.get(idx, 1));
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);
                        slaveData.put(idx, 0, slaveRowIds.ptr());
                        slaveData.put(idx, 1, slaveTimestamps.ptr());
                    }
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

            // Next, iterate through the master rows looking into the index we've built.
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = toSymbolMapKey(masterKey);
                final long rowIdsPtr = slaveData.get(idx, 0);
                final long timestampsPtr = slaveData.get(idx, 1);
                long rowLo = slaveData.get(idx, 2);

                if (rowIdsPtr != 0) {
                    slaveRowIds.of(rowIdsPtr);
                    assert slaveRowIds.size() > 0;
                    slaveTimestamps.of(timestampsPtr);
                    assert slaveTimestamps.size() > 0;

                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    slaveData.put(idx, 2, rowLo);
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

    /**
     * Vectorized aggregate (no join filter).
     */
    private static void aggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();

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
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);

        final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
        final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
        final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
        final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
        final int groupByFuncCount = groupByFunctions.size();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();
        final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
        final IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();

        atom.clearTemporaryData(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

        try {
            final int masterSymbolIndex = atom.getMasterSymbolIndex();
            final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            // First, build the in-memory index. For every master key in this page frame,
            // it stores rowids and timestamps of the matching slave rows.
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowIndex != Long.MIN_VALUE) {
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = toSymbolMapKey(matchingMasterKey);
                        long timestampsPtr = slaveData.get(idx, 0);
                        slaveTimestamps.of(timestampsPtr);
                        slaveTimestamps.add(slaveTimestamp);
                        slaveData.put(idx, 0, slaveTimestamps.ptr());

                        // copy the column values to be aggregated
                        for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                            var funcArg = groupByFuncArgs.getQuick(i);
                            if (funcArg != null) {
                                columnSink.of(slaveData.get(idx, i + 2)).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                slaveData.put(idx, i + 2, columnSink.ptr());
                            }
                        }
                    }
                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                    }
                }
            }

            // Next, iterate through the master rows looking into the index we've built.
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                    groupByFunctions.getQuick(i).setEmpty(value);
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = toSymbolMapKey(masterKey);
                long timestampsPtr = slaveData.get(idx, 0);
                long rowLo = slaveData.get(idx, 1);

                if (timestampsPtr != 0) {
                    slaveTimestamps.of(timestampsPtr);
                    assert slaveTimestamps.size() > 0;

                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    slaveData.put(idx, 1, rowLo);
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                    for (int i = 0; i < groupByFuncCount; i++) {
                        final int mapIndex = mapIndexes.getQuick(i);
                        final long ptr = slaveData.get(idx, mapIndex + 2);
                        if (ptr != 0) {
                            final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                            groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo));
                        } else { // no-arg function, e.g. count()
                            groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo));
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Vectorized aggregate with include prevailing (no join filter).
     */
    private static void aggregateVectWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();

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
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);

        final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
        final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
        final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
        final int groupByFuncCount = groupByFunctions.size();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();
        final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
        final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
        final IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();

        atom.clearTemporaryData(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

        try {
            final int masterSymbolIndex = atom.getMasterSymbolIndex();
            final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            // First, build the in-memory index. For every master key in this page frame,
            // it stores timestamps and column values of the matching slave rows.
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
            final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
            final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
            prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

            // Scan forward to collect slave column values within all possible time windows.
            if (slaveRowIndex != Long.MIN_VALUE) {
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = toSymbolMapKey(matchingMasterKey);
                        long timestampsPtr = slaveData.get(idx, 0);
                        slaveTimestamps.of(timestampsPtr);
                        slaveTimestamps.add(slaveTimestamp);
                        slaveData.put(idx, 0, slaveTimestamps.ptr());

                        // Copy the column values to be aggregated.
                        for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                            var funcArg = groupByFuncArgs.getQuick(i);
                            if (funcArg != null) {
                                final long sinkPtr = slaveData.get(idx, i + 2);
                                columnSink.of(sinkPtr);
                                if (sinkPtr == 0) {
                                    // Reserve an extra slot for the prevailing value by writing the first value twice.
                                    columnSink.put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                }
                                columnSink.put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                slaveData.put(idx, i + 2, columnSink.ptr());
                            }
                        }
                    }
                    if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                    }
                }
            }

            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);

                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                    groupByFunctions.getQuick(i).setEmpty(value);
                }

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = toSymbolMapKey(masterKey);
                long timestampsPtr = slaveData.get(idx, 0);
                long rowLo = slaveData.get(idx, 1);

                if (timestampsPtr != 0) {
                    slaveTimestamps.of(timestampsPtr);
                    assert slaveTimestamps.size() > 0;

                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    slaveData.put(idx, 1, rowLo);
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo >= rowHi || slaveTimestamps.get(rowLo) > masterSlaveTimestampLo) {
                        // No rows in the time window or the first row has timestamp higher than the low window boundary.
                        // In both cases we need to include the prevailing value.
                        if (rowLo > 0) {
                            // Yay! We already have the prevailing value in the in-memory index.
                            rowLo--;
                        } else {
                            // There are no slave rows corresponding to the time window for the symbol.
                            // Let's find the prevailing value.
                            final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                    slaveTimeFrameHelper,
                                    slaveRecord,
                                    slaveSymbolIndex,
                                    slaveSymbolLookupMap,
                                    masterKey
                            );
                            if (slaveRowId != Long.MIN_VALUE) {
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                // Copy the column values for the prevailing row to zero slot.
                                for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                                    var funcArg = groupByFuncArgs.getQuick(i);
                                    if (funcArg != null) {
                                        final long sinkPtr = slaveData.get(idx, i + 2);
                                        assert sinkPtr != 0;
                                        columnSink.of(sinkPtr).putAt(0, joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    }
                                }
                                // Set rowLo to -1, so that the zero slot value is included into the aggregation.
                                rowLo = -1;
                            }
                        }
                    }

                    if (rowLo < rowHi) {
                        for (int i = 0; i < groupByFuncCount; i++) {
                            final int mapIndex = mapIndexes.getQuick(i);
                            final long ptr = slaveData.get(idx, mapIndex + 2);
                            if (ptr != 0) {
                                final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                // column values are shifted by one slot, hence `rowLo + 1`
                                groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * (rowLo + 1), (int) (rowHi - rowLo));
                            } else { // no-arg function, e.g. count()
                                groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo));
                            }
                        }
                    }
                } else {
                    // There are no slave rows corresponding to the master page frame for the symbol.
                    // Let's find the prevailing value.
                    final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                            slaveTimeFrameHelper,
                            slaveRecord,
                            slaveSymbolIndex,
                            slaveSymbolLookupMap,
                            masterKey
                    );
                    if (slaveRowId != Long.MIN_VALUE) {
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Aggregate with include prevailing (no join filter).
     */
    private static void aggregateWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();

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
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
        final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

        try {
            final int masterSymbolIndex = atom.getMasterSymbolIndex();
            final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            // First, build the in-memory index. For every master key in this page frame,
            // it stores rowids and timestamps of the matching slave rows.
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);
            final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
            final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
            final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
            prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = toSymbolMapKey(matchingMasterKey);
                        slaveRowIds.of(slaveData.get(idx, 0));
                        slaveTimestamps.of(slaveData.get(idx, 1));
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);
                        slaveData.put(idx, 0, slaveRowIds.ptr());
                        slaveData.put(idx, 1, slaveTimestamps.ptr());
                    }
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

            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = toSymbolMapKey(masterKey);
                final long rowIdsPtr = slaveData.get(idx, 0);
                final long timestampsPtr = slaveData.get(idx, 1);
                long rowLo = slaveData.get(idx, 2);

                if (rowIdsPtr != 0) {
                    slaveRowIds.of(rowIdsPtr);
                    assert slaveRowIds.size() > 0;
                    slaveTimestamps.of(timestampsPtr);
                    assert slaveTimestamps.size() > 0;

                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    slaveData.put(idx, 2, rowLo);
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    if (rowLo >= rowHi || slaveTimestamps.get(rowLo) > masterSlaveTimestampLo) {
                        // No rows in the time window or the first row has timestamp higher than the low window boundary.
                        // In both cases we need to include the prevailing value.
                        if (rowLo > 0) {
                            // Yay! We already have the prevailing value in the in-memory index.
                            rowLo--;
                        } else {
                            // There are no slave rows corresponding to the time window for the symbol.
                            // Let's find the prevailing value.
                            final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                    slaveTimeFrameHelper,
                                    slaveRecord,
                                    slaveSymbolIndex,
                                    slaveSymbolLookupMap,
                                    masterKey
                            );
                            if (slaveRowId != Long.MIN_VALUE) {
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                value.setNew(false);
                            }
                        }
                    }

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
                } else {
                    // There are no slave rows corresponding to the master page frame for the symbol.
                    // Let's find the prevailing value.
                    final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                            slaveTimeFrameHelper,
                            slaveRecord,
                            slaveSymbolIndex,
                            slaveSymbolLookupMap,
                            masterKey
                    );
                    if (slaveRowId != Long.MIN_VALUE) {
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Aggregate with include prevailing and join filter.
     */
    private static void aggregateWithPrevailingJoinFiltered(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();

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
        final FlyweightMapValue value = atom.getMapValue(slotId);
        final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
        final Function joinFilter = atom.getJoinFilter(slotId);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();

        atom.clearTemporaryData(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList slaveRowIds = atom.getLongList(slotId);
        final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

        try {
            final int masterSymbolIndex = atom.getMasterSymbolIndex();
            final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
            final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
            // First, build the in-memory index. For every master key in this page frame,
            // it stores rowids and timestamps of the matching slave rows.
            record.setRowIndex(0);
            final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
            record.setRowIndex(frameRowCount - 1);
            final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

            final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
            int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
            long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
            if (slaveRowIndex != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = toSymbolMapKey(matchingMasterKey);
                        slaveRowIds.of(slaveData.get(idx, 0));
                        slaveTimestamps.of(slaveData.get(idx, 1));
                        slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                        slaveTimestamps.add(slaveTimestamp);
                        slaveData.put(idx, 0, slaveRowIds.ptr());
                        slaveData.put(idx, 1, slaveTimestamps.ptr());
                    }
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

            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);
                value.setNew(true);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = toSymbolMapKey(masterKey);
                final long rowIdsPtr = slaveData.get(idx, 0);
                final long timestampsPtr = slaveData.get(idx, 1);
                long rowLo = slaveData.get(idx, 2);

                if (rowIdsPtr != 0) {
                    slaveRowIds.of(rowIdsPtr);
                    assert slaveRowIds.size() > 0;
                    slaveTimestamps.of(timestampsPtr);
                    assert slaveTimestamps.size() > 0;

                    rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                    slaveData.put(idx, 2, rowLo);
                    long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                    boolean needToFindPrevailing = true;
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
                                        slaveRecord,
                                        slaveSymbolIndex,
                                        slaveSymbolLookupMap,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        masterKey,
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
                        // There are no slave rows corresponding to the time window for the symbol.
                        // Let's find the prevailing value.

                        // First, check the in-memory index.
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
                        // If the index doesn't have the prevailing row, we have to do the backward scan.
                        if (needToFindPrevailing) {
                            findPrevailingForMasterRow(
                                    slaveTimeFrameHelper,
                                    slaveRecord,
                                    slaveSymbolIndex,
                                    slaveSymbolLookupMap,
                                    prevailingFrameIndex,
                                    prevailingRowIndex,
                                    masterKey,
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
                            slaveRecord,
                            slaveSymbolIndex,
                            slaveSymbolLookupMap,
                            prevailingFrameIndex,
                            prevailingRowIndex,
                            masterKey,
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

    private static long applyFilter(
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull AsyncWindowJoinFastAtom atom,
            int slotId,
            long frameRowCount
    ) {
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

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();
        if (compiledFilter == null || frameMemory.hasColumnTops()) {
            AsyncFilterUtils.applyFilter(filter, rows, record, frameRowCount);
        } else {
            applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
        }

        final long filteredRowCount = rows.size();
        task.setFilteredRowCount(filteredRowCount);
        if (isParquetFrame) {
            atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
        }
        if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
            record.init(frameMemory);
        }
        return filteredRowCount;
    }

    /**
     * Aggregate with master filter (no join filter).
     */
    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long filteredRowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            final DirectLongList rows = task.getFilteredRows();

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
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
                final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                        }
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
                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);
                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = toSymbolMapKey(masterKey);
                    final long rowIdsPtr = slaveData.get(idx, 0);
                    final long timestampsPtr = slaveData.get(idx, 1);
                    long rowLo = slaveData.get(idx, 2);

                    if (rowIdsPtr != 0) {
                        slaveRowIds.of(rowIdsPtr);
                        assert slaveRowIds.size() > 0;
                        slaveTimestamps.of(timestampsPtr);
                        assert slaveTimestamps.size() > 0;

                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        slaveData.put(idx, 2, rowLo);
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

    /**
     * Vectorized aggregate with master filter (no join filter).
     */
    private static void filterAndAggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long filteredRowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            final DirectLongList rows = task.getFilteredRows();

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
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
                columnSink.resetPtr();
                final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
                final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
                final int groupByFuncCount = groupByFunctions.size();
                final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
                final IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveTimestamps.of(slaveData.get(idx, 0));
                            slaveTimestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, slaveTimestamps.ptr());

                            // Copy column values to be aggregated
                            for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                                var funcArg = groupByFuncArgs.getQuick(i);
                                if (funcArg != null) {
                                    long ptr = slaveData.get(idx, i + 2);
                                    columnSink.of(ptr).put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    slaveData.put(idx, i + 2, columnSink.ptr());
                                }
                            }
                        }
                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        }
                    }
                }

                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        groupByFunctions.getQuick(i).setEmpty(value);
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = toSymbolMapKey(masterKey);
                    long timestampsPtr = slaveData.get(idx, 0);
                    long rowLo = slaveData.get(idx, 1);

                    if (timestampsPtr != 0) {
                        slaveTimestamps.of(timestampsPtr);
                        assert slaveTimestamps.size() > 0;

                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        slaveData.put(idx, 1, rowLo);
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                        for (int i = 0; i < groupByFuncCount; i++) {
                            final int mapIndex = mapIndexes.getQuick(i);
                            final long ptr = slaveData.get(idx, mapIndex + 2);
                            if (ptr != 0) {
                                final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo));
                            } else { // no-arg function, e.g. count()
                                groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo));
                            }
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Vectorized aggregate with include prevailing and master filter (no join filter).
     */
    private static void filterAndAggregateVectWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long filteredRowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            final DirectLongList rows = task.getFilteredRows();

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);

                final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
                columnSink.resetPtr();
                final ObjList<Function> groupByFuncArgs = atom.getGroupByFunctionArgs(slotId);
                final IntList groupByFuncTypes = atom.getGroupByFunctionTypes();
                final int groupByFuncCount = groupByFunctions.size();
                final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
                final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
                final IntList mapIndexes = atom.getGroupByFunctionToColumnIndex();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

                // Scan forward to collect slave column values within all possible time windows.
                if (slaveRowIndex != Long.MIN_VALUE) {
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveTimestamps.of(slaveData.get(idx, 0));
                            slaveTimestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, slaveTimestamps.ptr());

                            // Copy column values to be aggregated.
                            for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                                var funcArg = groupByFuncArgs.getQuick(i);
                                if (funcArg != null) {
                                    long sinkPtr = slaveData.get(idx, i + 2);
                                    columnSink.of(sinkPtr);
                                    if (sinkPtr == 0) {
                                        // Reserve an extra slot for the prevailing value by writing the first value twice.
                                        columnSink.put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    }
                                    columnSink.put(joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    slaveData.put(idx, i + 2, columnSink.ptr());
                                }
                            }
                        }
                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                        }
                    }
                }

                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);

                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        groupByFunctions.getQuick(i).setEmpty(value);
                    }

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = toSymbolMapKey(masterKey);
                    long timestampsPtr = slaveData.get(idx, 0);
                    long rowLo = slaveData.get(idx, 1);

                    if (timestampsPtr != 0) {
                        slaveTimestamps.of(timestampsPtr);
                        assert slaveTimestamps.size() > 0;

                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        slaveData.put(idx, 1, rowLo);
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo >= rowHi || slaveTimestamps.get(rowLo) > masterSlaveTimestampLo) {
                            // No rows in the time window or the first row has timestamp higher than the low window boundary.
                            // In both cases we need to include the prevailing value.
                            if (rowLo > 0) {
                                // Yay! We already have the prevailing value in the in-memory index.
                                rowLo--;
                            } else {
                                // There are no slave rows corresponding to the time window for the symbol.
                                // Let's find the prevailing value.
                                final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                        slaveTimeFrameHelper,
                                        slaveRecord,
                                        slaveSymbolIndex,
                                        slaveSymbolLookupMap,
                                        masterKey
                                );
                                if (slaveRowId != Long.MIN_VALUE) {
                                    slaveTimeFrameHelper.recordAt(slaveRowId);
                                    // Copy the column values for the prevailing row to zero slot.
                                    for (int i = 0, n = groupByFuncArgs.size(); i < n; i++) {
                                        var funcArg = groupByFuncArgs.getQuick(i);
                                        if (funcArg != null) {
                                            final long sinkPtr = slaveData.get(idx, i + 2);
                                            assert sinkPtr != 0;
                                            columnSink.of(sinkPtr).putAt(0, joinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                        }
                                    }
                                    // Set rowLo to -1, so that the zero slot value is included into the aggregation.
                                    rowLo = -1;
                                }
                            }
                        }

                        if (rowLo < rowHi) {
                            for (int i = 0; i < groupByFuncCount; i++) {
                                final int mapIndex = mapIndexes.getQuick(i);
                                final long ptr = slaveData.get(idx, mapIndex + 2);
                                if (ptr != 0) {
                                    final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                                    // column values are shifted by one slot, hence `rowLo + 1`
                                    groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * (rowLo + 1), (int) (rowHi - rowLo));
                                } else { // no-arg function, e.g. count()
                                    groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo));
                                }
                            }
                        }
                    } else {
                        // There are no slave rows corresponding to the master page frame for the symbol.
                        // Let's find the prevailing value.
                        final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                slaveTimeFrameHelper,
                                slaveRecord,
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                masterKey
                        );
                        if (slaveRowId != Long.MIN_VALUE) {
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            functionUpdater.updateNew(value, joinRecord, slaveRowId);
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Aggregate with include prevailing and master filter (no join filter).
     */
    private static void filterAndAggregateWithPrevailing(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long filteredRowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            final DirectLongList rows = task.getFilteredRows();

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final FlyweightMapValue value = atom.getMapValue(slotId);
                final WindowJoinTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
                final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                        }
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

                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);
                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = toSymbolMapKey(masterKey);
                    final long rowIdsPtr = slaveData.get(idx, 0);
                    final long timestampsPtr = slaveData.get(idx, 1);
                    long rowLo = slaveData.get(idx, 2);

                    if (rowIdsPtr != 0) {
                        slaveRowIds.of(rowIdsPtr);
                        assert slaveRowIds.size() > 0;
                        slaveTimestamps.of(timestampsPtr);
                        assert slaveTimestamps.size() > 0;

                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        slaveData.put(idx, 2, rowLo);
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        if (rowLo >= rowHi || slaveTimestamps.get(rowLo) > masterSlaveTimestampLo) {
                            // No rows in the time window or the first row has timestamp higher than the low window boundary.
                            // In both cases we need to include the prevailing value.
                            if (rowLo > 0) {
                                // Yay! We already have the prevailing value in the in-memory index.
                                rowLo--;
                            } else {
                                // There are no slave rows corresponding to the time window for the symbol.
                                // Let's find the prevailing value.
                                final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                        slaveTimeFrameHelper,
                                        slaveRecord,
                                        slaveSymbolIndex,
                                        slaveSymbolLookupMap,
                                        masterKey
                                );
                                if (slaveRowId != Long.MIN_VALUE) {
                                    slaveTimeFrameHelper.recordAt(slaveRowId);
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    value.setNew(false);
                                }
                            }
                        }

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
                    } else {
                        // There are no slave rows corresponding to the master page frame for the symbol.
                        // Let's find the prevailing value.
                        final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                slaveTimeFrameHelper,
                                slaveRecord,
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                masterKey
                        );
                        if (slaveRowId != Long.MIN_VALUE) {
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            functionUpdater.updateNew(value, joinRecord, slaveRowId);
                        }
                    }
                }
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Aggregate with include prevailing, master filter, and join filter.
     */
    private static void filterAndAggregateWithPrevailingJoinFiltered(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncWindowJoinFastAtom atom = task.getFrameSequence(AsyncWindowJoinFastAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long filteredRowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            final DirectLongList rows = task.getFilteredRows();

            if (filteredRowCount > 0 && !atom.isSkipAggregation()) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
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
                final DirectIntIntHashMap slaveSymbolLookupMap = atom.getSlaveSymbolLookupMap();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();

                atom.clearTemporaryData(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList slaveRowIds = atom.getLongList(slotId);
                final GroupByLongList slaveTimestamps = atom.getTimestampList(slotId);

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);

                final long slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                final long slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);

                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveTimestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                        }
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

                // Process filtered master rows
                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);
                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);
                    value.setNew(true);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    final long masterSlaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    final long masterSlaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);
                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = toSymbolMapKey(masterKey);
                    final long rowIdsPtr = slaveData.get(idx, 0);
                    final long timestampsPtr = slaveData.get(idx, 1);
                    long rowLo = slaveData.get(idx, 2);

                    if (rowIdsPtr != 0) {
                        slaveRowIds.of(rowIdsPtr);
                        assert slaveRowIds.size() > 0;
                        slaveTimestamps.of(timestampsPtr);
                        assert slaveTimestamps.size() > 0;

                        rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                        slaveData.put(idx, 2, rowLo);
                        long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterSlaveTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                        boolean needToFindPrevailing = true;
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
                                            slaveRecord,
                                            slaveSymbolIndex,
                                            slaveSymbolLookupMap,
                                            prevailingFrameIndex,
                                            prevailingRowIndex,
                                            masterKey,
                                            joinFilter,
                                            joinRecord,
                                            functionUpdater,
                                            value
                                    );
                                }
                            }

                            // Process time window rows.
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
                            // There are no slave rows corresponding to the time window for the symbol.
                            // Let's find the prevailing value.

                            // First, check the in-memory index.
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
                            // If the index doesn't have the prevailing row, we have to do the backward scan.
                            if (needToFindPrevailing) {
                                findPrevailingForMasterRow(
                                        slaveTimeFrameHelper,
                                        slaveRecord,
                                        slaveSymbolIndex,
                                        slaveSymbolLookupMap,
                                        prevailingFrameIndex,
                                        prevailingRowIndex,
                                        masterKey,
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
                                slaveRecord,
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                prevailingFrameIndex,
                                prevailingRowIndex,
                                masterKey,
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

    static void findPrevailingForMasterRow(
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap,
            int slaveRowFrameIndex,
            long scanStart,
            int masterKey,
            Function joinFilter,
            JoinRecord joinRecord,
            GroupByFunctionsUpdater functionUpdater,
            MapValue value
    ) {
        if (slaveRowFrameIndex == -1) {
            return;
        }
        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();

        try {
            slaveTimeFrameHelper.restoreBookmark(slaveRowFrameIndex, 0);
            do {
                // actual row index doesn't matter here due to the later recordAtRowIndex() call
                slaveTimeFrameHelper.recordAt(Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0));

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
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                    if (matchingMasterKey == StaticSymbolTable.VALUE_NOT_FOUND || matchingMasterKey != masterKey) {
                        continue;
                    }
                    if (!joinFilter.getBool(joinRecord)) {
                        continue;
                    }
                    functionUpdater.updateNew(value, joinRecord, Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), r));
                    value.setNew(false);
                    return;
                }
                scanStart = Long.MAX_VALUE;
            } while (slaveTimeFrameHelper.previousFrame());
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
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
