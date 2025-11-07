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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
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
import io.questdb.griffin.engine.groupby.DirectMapValue;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

// TODO(puzpuzpuz): support "small" page frame size mode in page frame cursor and use it for window join factories;
//                  when it's on, separate min/max page frame config props should be used,
//                  10x smaller than the current defaults, i.e. 10k/100k
public class AsyncFastWindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer AGGREGATE = AsyncFastWindowJoinRecordCursorFactory::aggregate;
    private static final PageFrameReducer AGGREGATE_VECT = AsyncFastWindowJoinRecordCursorFactory::aggregateVect;
    private static final PageFrameReducer FILTER_AND_AGGREGATE = AsyncFastWindowJoinRecordCursorFactory::filterAndAggregate;
    private static final PageFrameReducer FILTER_AND_AGGREGATE_VECT = AsyncFastWindowJoinRecordCursorFactory::filterAndAggregateVect;

    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncFastWindowJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncFastWindowJoinAtom> frameSequence;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncFastWindowJoinRecordCursorFactory(
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
        this.cursor = new AsyncFastWindowJoinRecordCursor(
                configuration,
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
        final AsyncFastWindowJoinAtom atom = new AsyncFastWindowJoinAtom(
                asm,
                configuration,
                slaveFactory,
                joinFilter,
                perWorkerJoinFilters,
                masterSymbolIndex,
                slaveSymbolIndex,
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
                masterTsScale,
                slaveTsScale,
                workerCount
        );
        this.frameSequence = new PageFrameSequence<>(
                engine,
                configuration,
                messageBus,
                atom,
                masterFilter != null
                        ? atom.isVectorized() ? FILTER_AND_AGGREGATE_VECT : FILTER_AND_AGGREGATE
                        : atom.isVectorized() ? AGGREGATE_VECT : AGGREGATE,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_WINDOW_JOIN
        );
        this.workerCount = workerCount;
    }

    @Override
    public PageFrameSequence<AsyncFastWindowJoinAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
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
        sink.type("Async Window Fast Join");
        sink.meta("workers").val(workerCount);
        final AsyncFastWindowJoinAtom atom = frameSequence.getAtom();
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

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncFastWindowJoinAtom atom = task.getFrameSequence(AsyncFastWindowJoinAtom.class).getAtom();

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
        final AsyncTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();
        final DirectIntIntHashMap slaveSymbolLookupTable = atom.getSlaveSymbolLookupTable();
        Function joinFilter = atom.getJoinFilter(workerId);

        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        final GroupByLongList rowIds = atom.getRowIdsGroupByList(slotId);
        final GroupByLongList timestamps = atom.getTimestampsGroupByList(slotId);
        long dataPtr = slaveData.ptr();
        if (dataPtr != 0) {
            long entrySize = slaveData.entrySize();
            long capacity = slaveData.capacity();

            // reuse exists GroupByLongList
            for (long p = dataPtr, lim = dataPtr + entrySize * capacity; p < lim; p += entrySize) {
                int key = Unsafe.getUnsafe().getInt(p);
                if (key != 0) {
                    long ptr1 = Unsafe.getUnsafe().getLong(p + 4);
                    if (ptr1 != 0) {
                        rowIds.of(ptr1).clear();
                    }
                    long ptr2 = Unsafe.getUnsafe().getLong(p + 12);
                    if (ptr2 != 0) {
                        timestamps.of(ptr2).clear();
                    }
                    Unsafe.getUnsafe().putInt(p, 0);
                    Unsafe.getUnsafe().putLong(p + 20, 0);
                }
            }
        }

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
            long slaveTimestampLo, slaveTimestampHi;

            if (joinWindowLo == Long.MAX_VALUE) {
                slaveTimestampLo = Long.MIN_VALUE;
            } else {
                slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            }

            if (joinWindowHi == Long.MAX_VALUE) {
                slaveTimestampHi = Long.MAX_VALUE;
            } else {
                slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
            }

            long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowId != Long.MIN_VALUE) {
                long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowId);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupTable.get(Math.max(slaveKey + 1, 0));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = Math.max(matchingMasterKey + 1, 0);
                        long rowIdsPtr = slaveData.get(idx, 0);
                        long timestampsPtr = slaveData.get(idx, 1);
                        rowIds.of(rowIdsPtr);
                        timestamps.of(timestampsPtr);

                        if (rowIdsPtr == 0) {
                            slaveData.put(idx, 0, rowIds.ptr());
                        }

                        if (timestampsPtr == 0) {
                            slaveData.put(idx, 1, timestamps.ptr());
                        }
                        rowIds.add(baseSlaveRowId + slaveRowId);
                        timestamps.add(slaveTimestamp);
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

            // Next, iterate through the master rows looking into the index we've built.
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                rows.ensureCapacity(valueSizeInLongs);
                value.of(rows.getAppendAddress());
                value.setNew(true);
                rows.skip(valueSizeInLongs);
                functionUpdater.updateEmpty(value);

                final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                if (joinWindowLo == Long.MAX_VALUE) {
                    slaveTimestampLo = Long.MIN_VALUE;
                } else {
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                }

                if (joinWindowHi == Long.MAX_VALUE) {
                    slaveTimestampHi = Long.MAX_VALUE;
                } else {
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);
                }

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = Math.max(masterKey + 1, 0);
                long rowIdsPtr = slaveData.get(idx, 0);
                long timestampsPtr = slaveData.get(idx, 1);
                long rowLo = slaveData.get(idx, 2);
                rowIds.of(rowIdsPtr);
                timestamps.of(timestampsPtr);

                if (rowIds.size() > 0) {
                    long newRowLo = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampLo, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    newRowLo = newRowLo < 0 ? -newRowLo - 1 : newRowLo;
                    slaveData.put(idx, 2, newRowLo);
                    rowLo = newRowLo;
                    long rowHi = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampHi, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                    if (rowLo < rowHi) {
                        boolean isNew = true;
                        for (long i = rowLo; i < rowHi; i++) {
                            slaveRowId = rowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                if (isNew) {
                                    functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                    isNew = false;
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

    private static void aggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncFastWindowJoinAtom atom = task.getFrameSequence(AsyncFastWindowJoinAtom.class).getAtom();

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
        final AsyncTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
        final Record slaveRecord = slaveTimeFrameHelper.getRecord();
        final JoinRecord joinRecord = atom.getJoinRecord(slotId);
        joinRecord.of(record, slaveRecord);

        final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
        final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
        final IntList columnIndexes = atom.getGroupByColumnIndexes();
        final int columnCount = columnIndexes.size();
        final var columnTags = atom.getGroupByColumnTags();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long masterTsScale = atom.getMasterTsScale();
        final DirectIntIntHashMap slaveSymbolLookupTable = atom.getSlaveSymbolLookupTable();
        final GroupByLongList timestamps = atom.getTimestampsGroupByList(slotId);
        final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
        long dataPtr = slaveData.ptr();
        if (dataPtr != 0) {
            long entrySize = slaveData.entrySize();
            long capacity = slaveData.capacity();

            for (long p = dataPtr, lim = dataPtr + entrySize * capacity; p < lim; p += entrySize) {
                int key = Unsafe.getUnsafe().getInt(p);
                if (key != 0) {
                    long ptr1 = Unsafe.getUnsafe().getLong(p + 4);
                    if (ptr1 != 0) {
                        timestamps.of(ptr1).clear();
                    }
                    Unsafe.getUnsafe().putInt(p, 0);
                    Unsafe.getUnsafe().putLong(p + 12, 0);
                    for (int i = 0; i < columnCount; i++) {
                        long ptr = Unsafe.getUnsafe().getLong(p + 20 + i * 8);
                        if (ptr != 0) {
                            columnSink.of(ptr).clear();
                        }
                    }
                }
            }
        }

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
            long slaveTimestampLo, slaveTimestampHi;

            if (joinWindowLo == Long.MAX_VALUE) {
                slaveTimestampLo = Long.MIN_VALUE;
            } else {
                slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
            }

            if (joinWindowHi == Long.MAX_VALUE) {
                slaveTimestampHi = Long.MAX_VALUE;
            } else {
                slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
            }

            long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowId != Long.MIN_VALUE) {
                for (; ; ) {
                    slaveTimeFrameHelper.recordAtRowIndex(slaveRowId);
                    final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTimestamp > slaveTimestampHi) {
                        break;
                    }
                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupTable.get(Math.max(slaveKey + 1, 0));
                    if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        final int idx = Math.max(matchingMasterKey + 1, 0);
                        long timestampsPtr = slaveData.get(idx, 0);
                        timestamps.of(timestampsPtr);
                        if (timestampsPtr == 0) {
                            slaveData.put(idx, 0, timestamps.ptr());
                        }
                        timestamps.add(slaveTimestamp);

                        // copy the column values to be aggregated
                        for (int i = 0; i < columnCount; i++) {
                            long ptr = slaveData.get(idx, 2 + i);
                            columnSink.of(ptr).put(joinRecord, columnIndexes.getQuick(i), columnTags[i]);
                            if (ptr == 0) {
                                slaveData.put(idx, 2 + i, columnSink.ptr());
                            }
                        }
                    }
                    if (++slaveRowId >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                        if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                            break;
                        }
                        slaveRowId = slaveTimeFrameHelper.getTimeFrameRowLo();
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
                if (joinWindowLo == Long.MAX_VALUE) {
                    slaveTimestampLo = Long.MIN_VALUE;
                } else {
                    slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                }

                if (joinWindowHi == Long.MAX_VALUE) {
                    slaveTimestampHi = Long.MAX_VALUE;
                } else {
                    slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);
                }

                final int masterKey = record.getInt(masterSymbolIndex);
                final int idx = Math.max(masterKey + 1, 0);
                long timestampsPtr = slaveData.get(idx, 0);
                long rowLo = slaveData.get(idx, 1);
                timestamps.of(timestampsPtr);

                if (timestamps != null && timestamps.size() > 0) {
                    long newRowLo = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampLo, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                    newRowLo = newRowLo < 0 ? -newRowLo - 1 : newRowLo;
                    slaveData.put(idx, 1, newRowLo);
                    rowLo = newRowLo;
                    long rowHi = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampHi, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                    rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                    for (int i = 0; i < columnCount; i++) {
                        final long ptr = slaveData.get(idx, 2 + i);
                        final long typeSize = ColumnType.sizeOfTag(columnTags[i]);
                        groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo));
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
        final AsyncFastWindowJoinAtom atom = task.getFrameSequence(AsyncFastWindowJoinAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();

        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);

            if (filteredRowCount > 0) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final DirectMapValue value = atom.getMapValue(slotId);
                final AsyncTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);
                Function joinFilter = atom.getJoinFilter(slotId);
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();
                final DirectIntIntHashMap slaveSymbolLookupTable = atom.getSlaveSymbolLookupTable();
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                final GroupByLongList rowIds = atom.getRowIdsGroupByList(slotId);
                final GroupByLongList timestamps = atom.getTimestampsGroupByList(slotId);
                long dataPtr = slaveData.ptr();
                if (dataPtr != 0) {
                    long entrySize = slaveData.entrySize();
                    long capacity = slaveData.capacity();

                    for (long p = dataPtr, lim = dataPtr + entrySize * capacity; p < lim; p += entrySize) {
                        int key = Unsafe.getUnsafe().getInt(p);
                        if (key != 0) {
                            long ptr1 = Unsafe.getUnsafe().getLong(p + 4);
                            if (ptr1 != 0) {
                                rowIds.of(ptr1).clear();
                            }
                            long ptr2 = Unsafe.getUnsafe().getLong(p + 12);
                            if (ptr2 != 0) {
                                timestamps.of(ptr2).clear();
                            }
                            Unsafe.getUnsafe().putInt(p, 0);
                            Unsafe.getUnsafe().putLong(p + 20, 0);
                        }
                    }
                }

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();

                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);
                long slaveTimestampLo, slaveTimestampHi;

                if (joinWindowLo == Long.MAX_VALUE) {
                    slaveTimestampLo = Long.MIN_VALUE;
                } else {
                    slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                }

                if (joinWindowHi == Long.MAX_VALUE) {
                    slaveTimestampHi = Long.MAX_VALUE;
                } else {
                    slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
                }

                long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowId != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowId);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupTable.get(Math.max(slaveKey + 1, 0));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = Math.max(matchingMasterKey + 1, 0);
                            long rowIdsPtr = slaveData.get(idx, 0);
                            long timestampsPtr = slaveData.get(idx, 1);
                            rowIds.of(rowIdsPtr);
                            timestamps.of(timestampsPtr);

                            if (rowIdsPtr == 0) {
                                slaveData.put(idx, 0, rowIds.ptr());
                            }

                            if (timestampsPtr == 0) {
                                slaveData.put(idx, 1, timestamps.ptr());
                            }
                            rowIds.add(baseSlaveRowId + slaveRowId);
                            timestamps.add(slaveTimestamp);
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

                // Process filtered master rows
                for (long p = 0; p < filteredRowCount; p++) {
                    long r = rows.get(p);
                    record.setRowIndex(r);
                    rows.ensureCapacity(valueSizeInLongs);
                    value.of(rows.getAppendAddress());
                    value.setNew(true);
                    rows.skip(valueSizeInLongs);
                    functionUpdater.updateEmpty(value);

                    final long masterTimestamp = record.getTimestamp(masterTimestampIndex);
                    if (joinWindowLo == Long.MAX_VALUE) {
                        slaveTimestampLo = Long.MIN_VALUE;
                    } else {
                        slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    }

                    if (joinWindowHi == Long.MAX_VALUE) {
                        slaveTimestampHi = Long.MAX_VALUE;
                    } else {
                        slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);
                    }

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = Math.max(masterKey + 1, 0);
                    long rowIdsPtr = slaveData.get(idx, 0);
                    long timestampsPtr = slaveData.get(idx, 1);
                    long rowLo = slaveData.get(idx, 2);
                    rowIds.of(rowIdsPtr);
                    timestamps.of(timestampsPtr);

                    if (rowIds.size() > 0) {
                        long newRowLo = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampLo, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        newRowLo = newRowLo < 0 ? -newRowLo - 1 : newRowLo;
                        slaveData.put(idx, 2, newRowLo);
                        rowLo = newRowLo;
                        long rowHi = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampHi, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                        if (rowLo < rowHi) {
                            boolean isNew = true;
                            for (long i = rowLo; i < rowHi; i++) {
                                slaveRowId = rowIds.get(i);
                                slaveTimeFrameHelper.recordAt(slaveRowId);
                                if (joinFilter == null || joinFilter.getBool(joinRecord)) {
                                    if (isNew) {
                                        functionUpdater.updateNew(value, joinRecord, slaveRowId);
                                        isNew = false;
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

    private static void filterAndAggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncFastWindowJoinAtom atom = task.getFrameSequence(AsyncFastWindowJoinAtom.class).getAtom();

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();
        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);

            if (filteredRowCount > 0) {
                final int masterTimestampIndex = atom.getMasterTimestampIndex();
                final long joinWindowLo = atom.getJoinWindowLo();
                final long joinWindowHi = atom.getJoinWindowHi();
                final long valueSizeInBytes = atom.getValueSizeBytes();
                assert valueSizeInBytes % Long.BYTES == 0 : "unexpected value size: " + valueSizeInBytes;
                final long valueSizeInLongs = valueSizeInBytes / Long.BYTES;

                final DirectMapValue value = atom.getMapValue(slotId);
                final AsyncTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                final JoinRecord joinRecord = atom.getJoinRecord(slotId);
                joinRecord.of(record, slaveRecord);

                final ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(slotId);
                final GroupByColumnSink columnSink = atom.getColumnSink(slotId);
                columnSink.resetPtr();
                final IntList columnIndexes = atom.getGroupByColumnIndexes();
                final int columnCount = columnIndexes.size();
                final var columnTags = atom.getGroupByColumnTags();
                final long slaveTsScale = atom.getSlaveTsScale();
                final long masterTsScale = atom.getMasterTsScale();
                final DirectIntIntHashMap slaveSymbolLookupTable = atom.getSlaveSymbolLookupTable();
                final GroupByLongList timestamps = atom.getTimestampsGroupByList(slotId);
                final DirectIntMultiLongHashMap slaveData = atom.getSlaveData(slotId);
                long dataPtr = slaveData.ptr();
                if (dataPtr != 0) {
                    long entrySize = slaveData.entrySize();
                    long capacity = slaveData.capacity();

                    for (long p = dataPtr, lim = dataPtr + entrySize * capacity; p < lim; p += entrySize) {
                        int key = Unsafe.getUnsafe().getInt(p);
                        if (key != 0) {
                            long ptr1 = Unsafe.getUnsafe().getLong(p + 4);
                            if (ptr1 != 0) {
                                timestamps.of(ptr1).clear();
                            }
                            Unsafe.getUnsafe().putInt(p, 0);
                            Unsafe.getUnsafe().putLong(p + 12, 0);
                            for (int i = 0; i < columnCount; i++) {
                                long ptr = Unsafe.getUnsafe().getLong(p + 20 + i * 8);
                                if (ptr != 0) {
                                    columnSink.of(ptr).clear();
                                }
                            }
                        }
                    }
                }

                final int masterSymbolIndex = atom.getMasterSymbolIndex();
                final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
                final int slaveTimestampIndex = slaveTimeFrameHelper.getTimestampIndex();
                record.setRowIndex(rows.get(0));
                final long masterTimestampLo = record.getTimestamp(masterTimestampIndex);
                record.setRowIndex(rows.get(filteredRowCount - 1));
                final long masterTimestampHi = record.getTimestamp(masterTimestampIndex);
                long slaveTimestampLo, slaveTimestampHi;

                if (joinWindowLo == Long.MAX_VALUE) {
                    slaveTimestampLo = Long.MIN_VALUE;
                } else {
                    slaveTimestampLo = scaleTimestamp(masterTimestampLo - joinWindowLo, masterTsScale);
                }

                if (joinWindowHi == Long.MAX_VALUE) {
                    slaveTimestampHi = Long.MAX_VALUE;
                } else {
                    slaveTimestampHi = scaleTimestamp(masterTimestampHi + joinWindowHi, masterTsScale);
                }

                long slaveRowId = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowId != Long.MIN_VALUE) {
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowId);
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupTable.get(Math.max(slaveKey + 1, 0));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = Math.max(matchingMasterKey + 1, 0);
                            long timestampsPtr = slaveData.get(idx, 0);
                            timestamps.of(timestampsPtr);
                            if (timestampsPtr == 0) {
                                slaveData.put(idx, 0, timestamps.ptr());
                            }
                            timestamps.add(slaveTimestamp);

                            // Copy column values to be aggregated
                            for (int i = 0; i < columnCount; i++) {
                                long ptr = slaveData.get(idx, 2 + i);
                                columnSink.of(ptr).put(joinRecord, columnIndexes.getQuick(i), columnTags[i]);
                                if (ptr == 0) {
                                    slaveData.put(idx, 2 + i, columnSink.ptr());
                                }
                            }
                        }
                        if (++slaveRowId >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowId = slaveTimeFrameHelper.getTimeFrameRowLo();
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
                    if (joinWindowLo == Long.MAX_VALUE) {
                        slaveTimestampLo = Long.MIN_VALUE;
                    } else {
                        slaveTimestampLo = scaleTimestamp(masterTimestamp - joinWindowLo, masterTsScale);
                    }

                    if (joinWindowHi == Long.MAX_VALUE) {
                        slaveTimestampHi = Long.MAX_VALUE;
                    } else {
                        slaveTimestampHi = scaleTimestamp(masterTimestamp + joinWindowHi, masterTsScale);
                    }

                    final int masterKey = record.getInt(masterSymbolIndex);
                    final int idx = Math.max(masterKey + 1, 0);
                    long timestampsPtr = slaveData.get(idx, 0);
                    long rowLo = slaveData.get(idx, 1);
                    timestamps.of(timestampsPtr);

                    if (timestamps != null && timestamps.size() > 0) {
                        long newRowLo = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampLo, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                        newRowLo = newRowLo < 0 ? -newRowLo - 1 : newRowLo;
                        slaveData.put(idx, 1, newRowLo);
                        rowLo = newRowLo;
                        long rowHi = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampHi, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                        rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                        for (int i = 0; i < columnCount; i++) {
                            final long ptr = slaveData.get(idx, 2 + i);
                            final long typeSize = ColumnType.sizeOfTag(columnTags[i]);
                            groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * rowLo, (int) (rowHi - rowLo));
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
        Misc.free(joinMetadata);
    }
}
