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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * The filter a parallel operator applies to its page frames: the filter function and its per-worker clones, see
 * {@link ClonedFunction}, the JIT-compiled filter with the memory and the functions of its bind variables, the
 * columns the filter reads, and the selectivity statistics that decide late materialization, one per worker slot.
 * Owns all of them.
 * <p>
 * Without worker slots, the owner thread, every reducing worker and every work-stealing thread share one statistics
 * instance: a filter that hands out no slots has no per-thread identity to key them on, and {@link SelectivityStats}
 * tolerates concurrent updates.
 */
public class AsyncFilter implements QuietCloseable {
    private final @Nullable ObjList<Function> bindVarFunctions;
    private final @Nullable MemoryCARW bindVarMemory;
    private final @Nullable CompiledFilter compiledFilter;
    private final ClonedFunction filter = new ClonedFunction();
    private final @Nullable IntHashSet filterUsedColumnIndexes;
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    private final @Nullable ObjList<SelectivityStats> perWorkerSelectivityStats;

    /**
     * Takes the filter and bind variable resources of {@code resources}; a consumer of the filter of its page frames
     * reads no count-only filter.
     */
    public AsyncFilter(AsyncFilterResources resources, int slotCount) {
        this(
                resources.takeCompiledFilter(),
                resources.takeBindVarMemory(),
                resources.takeBindVarFunctions(),
                resources.takeFilter(),
                resources.getFilterUsedColumnIndexes(),
                resources.takePerWorkerFilters(),
                slotCount
        );
    }

    /**
     * Takes the given resources, which {@link #close()} frees, and keeps statistics of their own for
     * {@code slotCount} worker slots.
     */
    public AsyncFilter(
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int slotCount
    ) {
        this.compiledFilter = compiledFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;
        this.filter.setFunction(filter);
        this.filter.setPerWorkerFunctions(perWorkerFilters);
        this.filterUsedColumnIndexes = filterUsedColumnIndexes;
        if (slotCount > 0) {
            perWorkerSelectivityStats = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
            }
        } else {
            perWorkerSelectivityStats = null;
        }
    }

    @Override
    public void close() {
        CairoException.rethrowCleanupFailure(close(null));
    }

    /**
     * Frees the compiled filter, the bind variables, the filter and its clones, folding each failure into
     * {@code cleanupFailure}, which it returns.
     */
    public Throwable close(@Nullable Throwable cleanupFailure) {
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, compiledFilter);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, bindVarMemory);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, bindVarFunctions);
        return filter.close(cleanupFailure);
    }

    /**
     * Notifies the filter, its clones and the bind variable functions that the cursor has closed and resets the
     * statistics for the next query, folding each failure into {@code cleanupFailure}, which it returns.
     */
    public Throwable cursorClosed(@Nullable Throwable cleanupFailure) {
        cleanupFailure = filter.cursorClosed(cleanupFailure);
        cleanupFailure = Function.cursorClosed(cleanupFailure, bindVarFunctions);
        ownerSelectivityStats.clear();
        Misc.clearObjList(perWorkerSelectivityStats);
        return cleanupFailure;
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    /**
     * The filter function the worker in {@code slotId} evaluates, see {@link ClonedFunction#get}.
     */
    public Function getFilter(int slotId) {
        return filter.get(slotId);
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public SelectivityStats getSelectivityStats(int slotId) {
        if (slotId == -1 || perWorkerSelectivityStats == null) {
            return ownerSelectivityStats;
        }
        return perWorkerSelectivityStats.getQuick(slotId);
    }

    /**
     * Initializes the filter and its clones, see {@link ClonedFunction#init}, and the bind variable functions, whose
     * values it then copies to the bind variable memory.
     */
    public void initFilters(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filter.init(symbolTableSource, executionContext);
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            AsyncFilterUtils.prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    /**
     * Reports whether a task decodes only the filter columns of a frame before filtering it. Late materialization
     * pays off for a Parquet frame under a selective filter, and always for a task that only counts the rows that
     * pass.
     */
    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame, boolean isCountOnly) {
        if (!isParquetFrame || filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return isCountOnly || getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    public void toPlan(PlanSink sink) {
        sink.val(filter.getFunction());
    }

    /**
     * Rewinds the filter and its clones. A cursor that dispatches its page frames again after its own {@code toTop()}
     * calls this; one that re-reads what its first pass built does not.
     */
    public void toTop() {
        filter.toTop();
    }
}
