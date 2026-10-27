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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
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
            ArrayColumnTypes valueTypes,
            Function keyFunction,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<Function> recordFunctions,
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
                    new AsyncStreamGroupByAtom(configuration, asm, base.getMetadata(), valueTypes,
                            keyFunction, keyFunctions, perWorkerKeys, groupByFunctions, perWorkerFunctions, workerCount),
                    AsyncStreamGroupByRecordCursorFactory::aggregate,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY
            );
            cursor = new AsyncStreamGroupByRecordCursor(configuration, frameSequence.getAtom().getStateTypes(), recordFunctions);
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
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Stream Group By");
        sink.meta("workers").val(workerCount);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("keyFunctions", frameSequence.getAtom().getKeyFunctions(), true);
        sink.optAttr("values", frameSequence.getAtom().getFunctions(-1), true);
        sink.child(base);
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
        try {
            final PageFrameMemory frameMemory = task.populateFrameMemory();
            record.init(frameMemory);
            record.setRowIndex(0);
            final long baseRowId = record.getRowId();
            final long rowCount = task.getFrameRowCount();
            final Function keyFunction = atom.getKey(slot);
            final FlyweightPackedMapValue value = atom.getValue(slot);
            final GroupByFunctionsUpdater updater = atom.getUpdater(slot);
            final ObjList<GroupByFunction> functions = atom.getFunctions(slot);
            final int[] batchColumns = atom.getBatchColumnIndexes();
            final int keyIndex = atom.getStateTypes().getColumnCount() - 1;
            final long stride = (value.getSizeInBytes() + 7) / 8;
            final DirectLongList results = task.getGroupByValues();
            final boolean batch = !frameMemory.hasColumnTops() && !frameMemory.hasColumnTypeCasts();
            for (long row = 0, groups = 0; row < rowCount; groups++) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                record.setRowIndex(row);
                final long key = keyFunction.getLong(record);
                final long end = findGroupEnd(record, keyFunction, key, row, rowCount);
                results.ensureCapacity(stride);
                value.of(results.getAddress() + groups * stride * Long.BYTES);
                updater.updateEmpty(value);
                GroupByUtils.setStreamGroupByKey(value, keyIndex, keyFunction.getType(), key);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final GroupByFunction function = functions.getQuick(i);
                    final int column = batchColumns[i];
                    if (batch && column != AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE) {
                        final long address = column < 0 ? 0 : frameMemory.getPageAddress(column)
                                + row * ColumnType.sizeOf(function.getComputeBatchArgType());
                        function.computeBatch(value, address, (int) (end - row), baseRowId + row);
                    } else {
                        record.setRowIndex(row);
                        function.computeFirst(value, record, baseRowId + row);
                        for (long r = row + 1; r < end; r++) {
                            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                            record.setRowIndex(r);
                            function.computeNext(value, record, baseRowId + r);
                        }
                    }
                }
                results.setPos((groups + 1) * stride);
                row = end;
            }
        } finally {
            try {
                task.releaseFrameMemory();
            } finally {
                atom.release(slot);
            }
        }
    }

    private static long findGroupEnd(PageFrameMemoryRecord record, Function keyFunction, long key, long start, long end) {
        if (start + 1 == end) {
            return end;
        }
        long lastEqual = start;
        long probe = start + 1;
        long step = 1;
        while (true) {
            record.setRowIndex(probe);
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
            record.setRowIndex(mid);
            if (keyFunction.getLong(record) == key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
