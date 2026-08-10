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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;


public class AsyncGroupByNotKeyedAtom implements StatefulAtom, PerWorkerLockOwner, Closeable, Reopenable, Plannable {
    // Sentinel for batch-ineligible functions.
    static final int BATCH_NOT_ELIGIBLE = Integer.MIN_VALUE;
    // Sentinel for batch-eligible no-arg functions (e.g. count(*)).
    static final int BATCH_NO_ARG = -1;
    private final int[] batchColumnIndexes;
    private final AsyncFilterContext filterCtx;
    private final boolean hasNonBatchFunctions;
    private final GroupByAllocator ownerAllocator;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final SimpleMapValue ownerMapValue;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<SimpleMapValue> perWorkerMapValues;
    // Per-query native memory tracker captured from SqlExecutionContext on init.
    // Null when no per-query limit applies. Workers and operator code feed it to
    // tracker-aware Unsafe overloads to charge allocations to the active workload.
    private MemoryTracker memoryTracker;

    public AsyncGroupByNotKeyedAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int @NotNull [] batchColumnIndexes,
            int valueCount,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;

        try {
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.batchColumnIndexes = batchColumnIndexes;
            boolean hasNonBatch = false;
            for (int idx : batchColumnIndexes) {
                if (idx == BATCH_NOT_ELIGIBLE) {
                    hasNonBatch = true;
                    break;
                }
            }
            this.hasNonBatchFunctions = hasNonBatch;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            this.filterCtx = new AsyncFilterContext(
                    configuration,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    ownerFilter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount,
                    workerCount,
                    0L,
                    0L
            );

            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            if (perWorkerGroupByFunctions != null) {
                perWorkerFunctionUpdaters = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                perWorkerFunctionUpdaters = null;
            }
            perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            ownerMapValue = new SimpleMapValue(valueCount);
            perWorkerMapValues = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerMapValues.extendAndSet(i, new SimpleMapValue(valueCount));
            }

            // Lazy variant (openOnInit=false): the chunk index is global-counter bookkeeping;
            // only the data chunks it hands out are charged to the per-query tracker.
            ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration, false);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                perWorkerAllocators = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    final GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration, false);
                    perWorkerAllocators.extendAndSet(i, workerAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            clear();
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        ownerFunctionUpdater.updateEmpty(ownerMapValue);
        ownerMapValue.setNew(true);
        for (int i = 0, n = perWorkerMapValues.size(); i < n; i++) {
            SimpleMapValue value = perWorkerMapValues.getQuick(i);
            ownerFunctionUpdater.updateEmpty(value);
            value.setNew(true);
        }
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                PerWorkerFunctionList.clear(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);
        filterCtx.clear();
        memoryTracker = null;
    }

    @Override
    public void close() {
        // clear() already freed the data chunks under the bound tracker (the index is on the
        // global counter), so close() has nothing tracked to free. Nulling is defensive: any
        // stray free hits the global counter and cannot underflow an already-recycled block.
        if (ownerAllocator != null) {
            ownerAllocator.setMemoryTracker(null);
        }
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                if (allocator != null) {
                    allocator.setMemoryTracker(null);
                }
            }
        }
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAllocator);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAllocators);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerMapValue);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerMapValues);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                final ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                perWorkerGroupByFunctions.setQuick(i, null);
                try {
                    PerWorkerFunctionList.close(functions);
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filterCtx);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public int[] getBatchColumnIndexes() {
        return batchColumnIndexes;
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctions == null) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    public SimpleMapValue getMapValue(int slotId) {
        if (slotId == -1) {
            return ownerMapValue;
        }
        return perWorkerMapValues.getQuick(slotId);
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    // Thread-unsafe, should be used by query owner thread only.
    public SimpleMapValue getOwnerMapValue() {
        return ownerMapValue;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    // Thread-unsafe, should be used by query owner thread only.
    public ObjList<SimpleMapValue> getPerWorkerMapValues() {
        return perWorkerMapValues;
    }

    public boolean hasNonBatchFunctions() {
        return hasNonBatchFunctions;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        filterCtx.initFilters(symbolTableSource, executionContext);

        // The owner group by functions initialize here, once per query execution; the cursor does
        // not re-initialize them. Donate the initialized owner state to the aligned per-worker
        // clones before they initialize. Stateful functions inside aggregate arguments, such as
        // cursor comparisons caching a scalar sub-query result, must run their expensive and
        // potentially nondeterministic initialization exactly once per query, not once per worker,
        // and every worker must observe the same state as the owner.
        Function.init(ownerGroupByFunctions, symbolTableSource, executionContext, null);
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(
                            perWorkerGroupByFunctions.getQuick(i),
                            ownerGroupByFunctions,
                            symbolTableSource,
                            executionContext
                    );
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use its own private filter, function updaters, etc. anytime.
            return -1;
        }
        // All other threads, e.g. worker or work stealing threads, must always acquire a lock
        // to use shared resources.
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // init() runs before reopen() (frameSequence.of() -> atom.init(), then cursor.of()
        // -> atom.reopen()), so memoryTracker is available here to bind on the allocators
        // before worker threads allocate any backing.
        ownerAllocator.setMemoryTracker(memoryTracker);
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                allocator.setMemoryTracker(memoryTracker);
                allocator.reopen();
            }
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        filterCtx.toPlan(sink);
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }
}
