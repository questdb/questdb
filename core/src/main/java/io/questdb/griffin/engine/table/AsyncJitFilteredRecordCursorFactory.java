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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncJitFilteredRecordCursorFactory extends AbstractAsyncFilteredRecordCursorFactory<AsyncJitFilteredRecordCursorFactory.AsyncJitFilterAtom> {
    private static final PageFrameReducer REDUCER = AsyncJitFilteredRecordCursorFactory::filter;

    /**
     * Takes the base, the filter resources and the LIMIT function, and frees all of them when it throws.
     */
    public AsyncJitFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull AsyncFilterResources filterResources,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            int workerCount,
            boolean enablePreTouch
    ) {
        super(
                engine,
                configuration,
                messageBus,
                base,
                filterResources,
                AsyncJitFilteredRecordCursorFactory::newAtom,
                REDUCER,
                reduceTaskFactory,
                limitLoFunction,
                limitLoPos,
                workerCount,
                enablePreTouch,
                "Async JIT Filter"
        );
        assert !(base instanceof FilteredRecordCursorFactory);
        assert !(base instanceof AsyncJitFilteredRecordCursorFactory);
    }

    @Override
    public boolean usesCompiledFilter() {
        return true;
    }

    private static void filter(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        final PageFrameSequence<AsyncJitFilterAtom> frameSequence = task.getFrameSequence(AsyncJitFilterAtom.class);
        final AsyncJitFilterAtom atom = frameSequence.getAtom();
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int filterId = atom.maybeAcquireFilter(workerId, owner, circuitBreaker);

        try {
            final boolean isParquetFrame = task.isParquetFrame();
            final boolean useLateMaterialization = atom.shouldUseLateMaterialization(filterId, isParquetFrame, task.isCountOnly());

            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
            } else {
                frameMemory = task.populateFrameMemory();
            }
            record.init(frameMemory);

            if (frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                // Use Java-based filter in case of a page frame with column tops
                // or type-cast columns (fixed→var conversion not supported in JIT).
                final Function filter = atom.getFilter(filterId);

                if (task.isCountOnly()) {
                    long count = 0;
                    for (long r = 0; r < frameRowCount; r++) {
                        record.setRowIndex(r);
                        if (filter.getBool(record)) {
                            count++;
                        }
                    }
                    task.setFilteredRowCount(count);
                } else { // normal filter task
                    for (long r = 0; r < frameRowCount; r++) {
                        record.setRowIndex(r);
                        if (filter.getBool(record)) {
                            rows.add(r);
                        }
                    }

                    if (isParquetFrame) {
                        atom.getSelectivityStats(filterId).update(rows.size(), frameRowCount);
                    }
                    if (useLateMaterialization && task.populateRemainingColumns(atom.getLateMaterializationSkipColumnIndexes(), rows, true)) {
                        record.init(frameMemory);
                    }
                    task.setFilteredRowCount(rows.size());
                }
                return;
            }

            // Use JIT-compiled filter.
            task.populateJitData();
            final DirectLongList dataAddresses = task.getDataAddresses();
            final DirectLongList auxAddresses = task.getAuxAddresses();

            if (task.isCountOnly()) {
                final long filteredRowCount = atom.compiledCountOnlyFilter.call(
                        dataAddresses.getAddress(),
                        dataAddresses.size(),
                        auxAddresses.getAddress(),
                        atom.getBindVarMemory().getAddress(),
                        atom.getBindVarFunctions().size(),
                        frameRowCount
                );
                task.setFilteredRowCount(filteredRowCount);
            } else { // normal filter task
                final long filteredRowCount = atom.getCompiledFilter().call(
                        dataAddresses.getAddress(),
                        dataAddresses.size(),
                        auxAddresses.getAddress(),
                        atom.getBindVarMemory().getAddress(),
                        atom.getBindVarFunctions().size(),
                        rows.getAddress(),
                        frameRowCount
                );

                rows.setPos(filteredRowCount);
                if (isParquetFrame) {
                    atom.getSelectivityStats(filterId).update(filteredRowCount, frameRowCount);
                }
                if (useLateMaterialization && task.populateRemainingColumns(atom.getLateMaterializationSkipColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                task.setFilteredRowCount(rows.size());

                // Pre-touch native columns, if asked.
                if (frameMemory.getFrameFormat() == PartitionFormat.NATIVE) {
                    atom.preTouchColumns(record, rows, frameRowCount);
                }
            }
        } finally {
            atom.releaseFilter(filterId);
        }
    }

    private static AsyncJitFilterAtom newAtom(
            CairoConfiguration configuration,
            AsyncFilterResources filterResources,
            IntList columnTypes,
            boolean enablePreTouch
    ) {
        final MemoryCARW bindVarMemory = Vm.getCARWInstance(
                configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(),
                MemoryTag.NATIVE_JIT
        );
        final CompiledFilter compiledFilter = filterResources.takeCompiledFilter();
        final CompiledCountOnlyFilter compiledCountOnlyFilter = filterResources.takeCompiledCountOnlyFilter();
        final Function filter = filterResources.takeFilter();
        final ObjList<Function> bindVarFunctions = filterResources.takeBindVarFunctions();
        final ObjList<Function> perWorkerFilters = filterResources.takePerWorkerFilters();
        try {
            return new AsyncJitFilterAtom(
                    configuration,
                    filter,
                    filterResources.getFilterUsedColumnIndexes(),
                    perWorkerFilters,
                    compiledFilter,
                    compiledCountOnlyFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    columnTypes,
                    enablePreTouch
            );
        } catch (Throwable th) {
            Misc.freeObjList(perWorkerFilters, th);
            Misc.free(bindVarMemory, th);
            Misc.free(compiledCountOnlyFilter, th);
            Misc.free(compiledFilter, th);
            Misc.free(filter, th);
            Misc.freeObjList(bindVarFunctions, th);
            throw th;
        }
    }

    public static class AsyncJitFilterAtom extends AsyncFilterAtom {
        final CompiledCountOnlyFilter compiledCountOnlyFilter;

        /**
         * Takes the filter resources, the count-only filter included, which {@link #close()} frees; frees none of
         * them when it throws.
         */
        public AsyncJitFilterAtom(
                CairoConfiguration configuration,
                Function filter,
                IntHashSet filterUsedColumnIndexes,
                ObjList<Function> perWorkerFilters,
                CompiledFilter compiledFilter,
                CompiledCountOnlyFilter compiledCountOnlyFilter,
                MemoryCARW bindVarMemory,
                ObjList<Function> bindVarFunctions,
                IntList columnTypes,
                boolean enablePreTouch
        ) {
            super(
                    configuration,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    columnTypes,
                    enablePreTouch
            );
            this.compiledCountOnlyFilter = compiledCountOnlyFilter;
        }

        @Override
        public Throwable close(@Nullable Throwable cleanupFailure) {
            return super.close(Misc.freeBestEffort(cleanupFailure, compiledCountOnlyFilter));
        }
    }
}
