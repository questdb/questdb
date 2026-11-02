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
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncFilteredRecordCursorFactory extends AbstractAsyncFilteredRecordCursorFactory<AsyncFilterAtom> {
    private static final PageFrameReducer REDUCER = AsyncFilteredRecordCursorFactory::filter;

    /**
     * Takes the base, the filter, its per-worker clones and the LIMIT function, and frees all of them when it throws.
     */
    public AsyncFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull Function filter,
            @NotNull IntHashSet filterUsedColumnIndexes,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
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
                new AsyncFilterResources(null, null, null, null, filter, filterUsedColumnIndexes, perWorkerFilters),
                AsyncFilteredRecordCursorFactory::newAtom,
                REDUCER,
                reduceTaskFactory,
                limitLoFunction,
                limitLoPos,
                workerCount,
                enablePreTouch,
                "Async Filter"
        );
        assert !(base instanceof AsyncFilteredRecordCursorFactory);
    }

    private static void filter(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        final AsyncFilterAtom atom = task.getFrameSequence(AsyncFilterAtom.class).getAtom();

        final boolean isParquetFrame = task.isParquetFrame();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int filterId = atom.maybeAcquireFilter(workerId, owner, circuitBreaker);
        // The slot is held from here on, so everything below belongs inside the try that releases
        // it: populateFrameMemory() navigates to the frame, which decodes parquet and can breach the
        // per-query memory limit. See PerWorkerLocks.acquireSlot().
        try {
            final boolean useLateMaterialization = atom.shouldUseLateMaterialization(filterId, isParquetFrame, task.isCountOnly());

            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
            } else {
                frameMemory = task.populateFrameMemory();
            }
            record.init(frameMemory);

            final DirectLongList rows = task.getFilteredRows();
            rows.clear();

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

                // Pre-touch native columns, if asked.
                if (frameMemory.getFrameFormat() == PartitionFormat.NATIVE) {
                    atom.preTouchColumns(record, rows, frameRowCount);
                }
            }
        } finally {
            atom.releaseFilter(filterId);
        }
    }

    private static AsyncFilterAtom newAtom(
            CairoConfiguration configuration,
            AsyncFilterResources filterResources,
            IntList columnTypes,
            boolean enablePreTouch
    ) {
        final Function filter = filterResources.takeFilter();
        final ObjList<Function> perWorkerFilters = filterResources.takePerWorkerFilters();
        try {
            return new AsyncFilterAtom(
                    configuration,
                    null,
                    null,
                    null,
                    filter,
                    filterResources.getFilterUsedColumnIndexes(),
                    perWorkerFilters,
                    columnTypes,
                    enablePreTouch
            );
        } catch (Throwable th) {
            Misc.freeObjList(perWorkerFilters, th);
            Misc.free(filter, th);
            throw th;
        }
    }
}
