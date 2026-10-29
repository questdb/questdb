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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class AsyncStreamGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final SCSequence collectSubSeq = new SCSequence();
    private final int workerCount;
    private RecordCursorFactory base;
    private AsyncStreamGroupByRecordCursor cursor;
    private PageFrameSequence<AsyncStreamGroupByAtom> frameSequence;
    private ObjList<Function> recordFunctions;

    public AsyncStreamGroupByRecordCursorFactory(
            CairoEngine engine,
            BytecodeAssembler asm,
            CairoConfiguration configuration,
            MessageBus messageBus,
            RecordCursorFactory base,
            RecordMetadata metadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            int streamKeyIndex,
            Function keyFunction,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<Function> recordFunctions,
            CompiledFilter compiledFilter,
            MemoryCARW bindVarMemory,
            ObjList<Function> bindVarFunctions,
            Function filter,
            IntHashSet filterUsedColumnIndexes,
            ObjList<Function> perWorkerFilters,
            PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(metadata);
        this.base = base;
        this.recordFunctions = recordFunctions;
        this.workerCount = workerCount;
        try {
            frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    new AsyncStreamGroupByAtom(configuration, asm, base.getMetadata(), columnFilter, keyTypes, valueTypes, streamKeyIndex,
                            keyFunction, keyFunctions, perWorkerKeys, groupByFunctions, perWorkerFunctions,
                            compiledFilter, bindVarMemory, bindVarFunctions, filter, filterUsedColumnIndexes, perWorkerFilters, workerCount),
                    AsyncStreamGroupByRecordCursorFactory::aggregate,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY
            );
            cursor = new AsyncStreamGroupByRecordCursor(configuration, frameSequence.getAtom().getStateTypes(), recordFunctions,
                    frameSequence.getAtom().getCollectorMap());
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    @TestOnly
    public AsyncStreamGroupByAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameSequence.of(base, executionContext, collectSubSeq, ORDER_ASC);
        try {
            cursor.of(frameSequence, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean isRewindRecomputing() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(usesCompiledFilter() ? "Async JIT Stream Group By" : "Async Stream Group By");
        sink.meta("workers").val(workerCount);
        if (frameSequence.getAtom().getCollectorMap() != null) {
            sink.optAttr("segmentKey", frameSequence.getAtom().getKey(-1), true);
        }
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("keyFunctions", frameSequence.getAtom().getKeyFunctions(), true);
        sink.optAttr("values", frameSequence.getAtom().getFunctions(-1), true);
        final AsyncFilterContext filterContext = frameSequence.getAtom().getFilterContext();
        if (filterContext != null) {
            sink.optAttr("filter", filterContext.getFilter(-1), true);
        }
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        final AsyncFilterContext filterContext = frameSequence.getAtom().getFilterContext();
        return filterContext != null && filterContext.getCompiledFilter() != null;
    }

    @Override
    protected void _close() {
        Throwable th = Misc.freeBestEffort(null, cursor);
        cursor = null;
        th = Misc.freeBestEffort(th, frameSequence);
        frameSequence = null;
        th = Misc.freeBestEffort(th, base);
        base = null;
        th = Misc.freeObjListBestEffort(th, recordFunctions);
        recordFunctions = null;
        CairoException.rethrowCleanupFailure(th);
    }

    private static void aggregate(
            int workerId,
            PageFrameMemoryRecord record,
            PageFrameReduceTask task,
            SqlExecutionCircuitBreaker circuitBreaker,
            PageFrameSequence<?> stealingFrameSequence
    ) {
        final PageFrameSequence<AsyncStreamGroupByAtom> sequence = task.getFrameSequence(AsyncStreamGroupByAtom.class);
        final AsyncStreamGroupByAtom atom = sequence.getAtom();
        final int slot = atom.maybeAcquire(workerId, sequence == stealingFrameSequence, circuitBreaker);
        final AsyncFilterContext filterContext = atom.getFilterContext();
        try {
            final DirectLongList rows;
            if (filterContext == null) {
                record.init(task.populateFrameMemory());
                rows = null;
            } else {
                record = filterContext.filter(record, sequence.getPageFrameAddressCache(), slot, task.getFrameIndex(), task.getFrameRowCount());
                rows = filterContext.getFilteredRows(slot);
            }
            final long baseRowId = Rows.toRowID(task.getFrameIndex(), 0);
            final long rowCount = rows == null ? task.getFrameRowCount() : rows.size();
            final Function keyFunction = atom.getKey(slot);
            final boolean isBatch = !(record instanceof PageFrameFilteredMemoryRecord);
            if (atom.getCollectorMap() != null) {
                final StreamGroupByMap map = atom.getMap(slot);
                final RecordChain results = atom.getResultRecords(task);
                long previous = -1;
                long segmentCount = 0;
                for (long row = 0; row < rowCount; segmentCount++) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    record.setFilteredRowIndex(rows == null ? row : rows.get(row), row);
                    final long key = keyFunction.getLong(record);
                    final long end = findGroupEnd(record, rows, keyFunction, key, row, rowCount);
                    if (row == 0) {
                        task.setGroupByFirstKey(key);
                    }
                    if (row == 0 && end == rowCount) {
                        atom.acquireSpanMap(slot, key).aggregate(record, rows, row, end, baseRowId, isBatch, circuitBreaker);
                    } else {
                        map.clear(key);
                        map.aggregate(record, rows, row, end, baseRowId, isBatch, circuitBreaker);
                        previous = map.appendTo(results, previous, circuitBreaker);
                    }
                    task.setGroupByLastKey(key);
                    row = end;
                }
                task.setGroupBySegmentCount(segmentCount);
                results.toTop();
                return;
            }
            final FlyweightPackedMapValue value = atom.getValue(slot);
            final GroupByFunctionsUpdater updater = atom.getUpdater(slot);
            final ObjList<GroupByFunction> functions = atom.getFunctions(slot);
            final int[] batchColumns = atom.getBatchColumnIndexes();
            final int keyIndex = atom.getStateTypes().getColumnCount() - 1;
            final long stride = (value.getSizeInBytes() + 7) / 8;
            final DirectLongList results = task.getGroupByValues();
            for (long row = 0, groups = 0; row < rowCount; groups++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final long firstRow = rows == null ? row : rows.get(row);
                record.setFilteredRowIndex(firstRow, row);
                final long key = keyFunction.getLong(record);
                final long end = findGroupEnd(record, rows, keyFunction, key, row, rowCount);
                final boolean isContiguous = rows == null || rows.get(end - 1) - firstRow == end - row - 1;
                results.ensureCapacity(stride);
                value.of(results.getAddress() + groups * stride * Long.BYTES);
                updater.updateEmpty(value);
                GroupByUtils.setStreamGroupByKey(value, keyIndex, keyFunction.getType(), key);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final GroupByFunction function = functions.getQuick(i);
                    final int column = batchColumns[i];
                    final long pageAddress = isBatch && column >= 0 ? record.getPageAddress(column) : 0;
                    if (isBatch && isContiguous && column != AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE
                            && (column < 0 || pageAddress != 0)) {
                        final long address = column < 0 ? 0 : pageAddress
                                                              + firstRow * ColumnType.sizeOf(function.getComputeBatchArgType());
                        function.computeBatch(value, address, (int) (end - row), baseRowId + firstRow);
                    } else {
                        record.setFilteredRowIndex(firstRow, row);
                        function.computeFirst(value, record, baseRowId + firstRow);
                        for (long r = row + 1; r < end; r++) {
                            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                            final long rowIndex = rows == null ? r : rows.get(r);
                            record.setFilteredRowIndex(rowIndex, r);
                            function.computeNext(value, record, baseRowId + rowIndex);
                        }
                    }
                }
                results.setPos((groups + 1) * stride);
                row = end;
            }
        } finally {
            try {
                if (filterContext == null) {
                    task.releaseFrameMemory();
                } else {
                    filterContext.getMemoryPool(slot).releaseParquetBuffers();
                }
            } finally {
                atom.release(slot);
            }
        }
    }

    private static long findGroupEnd(PageFrameMemoryRecord record, DirectLongList rows, Function keyFunction, long key, long start, long end) {
        if (start + 1 == end) {
            return end;
        }
        long lastEqual = start;
        long probe = start + 1;
        long step = 1;
        while (true) {
            record.setFilteredRowIndex(rows == null ? probe : rows.get(probe), probe);
            if (keyFunction.getLong(record) != key) {
                break;
            }
            if (probe == end - 1) {
                return end;
            }
            lastEqual = probe;
            step *= 2;
            probe = Math.min(start + step, end - 1);
        }
        long lo = lastEqual + 1;
        long hi = probe;
        while (lo < hi) {
            final long mid = lo + (hi - lo) / 2;
            record.setFilteredRowIndex(rows == null ? mid : rows.get(mid), mid);
            if (keyFunction.getLong(record) == key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
