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

package io.questdb.griffin.engine.table.aggr;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractAggregator implements Aggregator {

    public static void applyCompiledFilter(
            CompiledFilter compiledFilter,
            MemoryCARW bindVarMemory,
            ObjList<Function> bindVarFunctions,
            PageFrameReduceTask task
    ) {
        task.populateJitData();
        final DirectLongList data = task.getDataAddresses();
        final DirectLongList varSizeAux = task.getAuxAddresses();
        final DirectLongList rows = task.getFilteredRows();
        long hi = compiledFilter.call(
                data.getAddress(),
                data.size(),
                varSizeAux.getAddress(),
                bindVarMemory.getAddress(),
                bindVarFunctions.size(),
                rows.getAddress(),
                task.getFrameRowCount(),
                0
        );
        rows.setPos(hi);
    }

    public static void applyFilter(Function filter, DirectLongList rows, PageFrameMemoryRecord record, long frameRowCount) {
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }

    @Override
    public void aggregate(
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
            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (!fragment.isSharded()) {
                aggregateNonSharded(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
            } else {
                aggregateSharded(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.requestSharding(fragment);
        } finally {
            atom.release(slotId);
            task.releaseFrameMemory();
        }
    }

    @Override
    public void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getFilteredRows();
        final PageFrameSequence<AsyncGroupByAtom> frameSequence = task.getFrameSequence(AsyncGroupByAtom.class);

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        rows.clear();

        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncGroupByAtom atom = frameSequence.getAtom();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final AsyncGroupByAtom.MapFragment fragment = atom.getFragment(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledFilter();
        final Function filter = atom.getFilter(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        try {
            if (compiledFilter == null || frameSequence.getPageFrameAddressCache().hasColumnTops(task.getFrameIndex())) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            if (atom.isSharded()) {
                fragment.shard();
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (!fragment.isSharded()) {
                aggregateFilteredNonSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
            } else {
                aggregateFilteredSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.requestSharding(fragment);
        } finally {
            atom.release(slotId);
            task.releaseFrameMemory();
        }
    }

    protected abstract void aggregateFilteredNonSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    );

    protected abstract void aggregateFilteredSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    );

    protected abstract void aggregateNonSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    );

    protected abstract void aggregateSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            AsyncGroupByAtom.MapFragment fragment,
            RecordSink mapSink
    );
}
