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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

/**
 * Per-query state of the parallel row-preserving HORIZON JOIN.
 * <p>
 * Every worker slot, and the owner thread, gets its own slave time frame cursors, a
 * {@link HorizonJoinMatcher} and a horizon timestamp iterator, so each page frame task matches
 * its master rows independently. The owner thread uses its set both when it reduces its own tasks
 * and when it matches the tail of an oversized frame (see {@link #getMaxTaskRows()}). The atom also
 * holds one more time frame cursor per slave: the owner thread positions the output records on
 * it, so reading a matched slave row never disturbs the scan state of a matcher.
 */
public class AsyncHorizonJoinProjectionAtom implements StatefulAtom, PerWorkerLockOwner {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final IntHashSet filterUsedColumnIndexes;
    private final int masterTimestampIndex;
    private final long maxTaskRows;
    private final int offsetCount;
    private final ObjList<ConcurrentTimeFrameCursor> outputSlaveTimeFrameCursors;
    private final Function ownerFilter;
    private final AsyncHorizonTimestampIterator ownerHorizonIterator;
    private final HorizonJoinMatcher ownerMatcher;
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    // Per slave.
    private final ObjList<ConcurrentTimeFrameCursor> ownerSlaveTimeFrameCursors;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<AsyncHorizonTimestampIterator> perWorkerHorizonIterators;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<HorizonJoinMatcher> perWorkerMatchers;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;
    // Worker-major: the cursor of worker w and slave s sits at w * slaveCount + s.
    private final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    private final int slaveCount;
    private final long slotsPerRow;
    private final int workerCount;
    private MemoryTracker memoryTracker;

    public AsyncHorizonJoinProjectionAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            long @NotNull [] offsets,
            int masterTimestampIndex,
            long maxTaskRows,
            @NotNull AsyncHorizonJoinResources resources,
            int workerCount
    ) {
        // Adopt the filter resources first: the holder keeps whatever this constructor does not
        // take, and close() frees whatever it took, whichever allocation below fails.
        this.compiledFilter = resources.takeCompiledFilter();
        this.bindVarMemory = resources.takeBindVarMemory();
        this.bindVarFunctions = resources.takeBindVarFunctions();
        this.ownerFilter = resources.takeFilter();
        this.perWorkerFilters = resources.takePerWorkerFilters();
        this.filterUsedColumnIndexes = resources.getFilterUsedColumnIndexes();
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        this.slaveCount = slaveStates.size();
        this.offsetCount = offsets.length;
        this.slotsPerRow = (long) offsetCount * slaveCount;
        this.masterTimestampIndex = masterTimestampIndex;
        this.maxTaskRows = maxTaskRows;
        this.workerCount = workerCount;
        this.outputSlaveTimeFrameCursors = new ObjList<>(slaveCount);
        this.ownerSlaveTimeFrameCursors = new ObjList<>(slaveCount);
        this.perWorkerHorizonIterators = new ObjList<>(workerCount);
        this.perWorkerMatchers = new ObjList<>(workerCount);
        this.perWorkerSelectivityStats = new ObjList<>(workerCount);
        this.perWorkerSlaveTimeFrameCursors = new ObjList<>(workerCount * slaveCount);
        try {
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            this.ownerHorizonIterator = new AsyncHorizonTimestampIterator(offsets);
            this.ownerMatcher = new HorizonJoinMatcher(configuration, slaveStates, masterAsOfJoinMapSinkClasses, slaveAsOfJoinMapSinkClasses);
            for (int s = 0; s < slaveCount; s++) {
                ownerSlaveTimeFrameCursors.add(slaveStates.getQuick(s).getFactory().newTimeFrameCursor());
                outputSlaveTimeFrameCursors.add(slaveStates.getQuick(s).getFactory().newTimeFrameCursor());
            }
            for (int w = 0; w < workerCount; w++) {
                perWorkerHorizonIterators.add(new AsyncHorizonTimestampIterator(offsets));
                perWorkerMatchers.add(new HorizonJoinMatcher(configuration, slaveStates, masterAsOfJoinMapSinkClasses, slaveAsOfJoinMapSinkClasses));
                perWorkerSelectivityStats.add(new SelectivityStats());
                for (int s = 0; s < slaveCount; s++) {
                    perWorkerSlaveTimeFrameCursors.add(slaveStates.getQuick(s).getFactory().newTimeFrameCursor());
                }
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        // Runs from PageFrameSequence.reset(), after every task has finished. Keeps the objects
        // for the next query and releases everything they hold for this one.
        Throwable failure = Misc.freeObjListAndKeepObjectsBestEffort(null, ownerSlaveTimeFrameCursors);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, outputSlaveTimeFrameCursors);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerSlaveTimeFrameCursors);
        failure = Misc.clearBestEffort(failure, ownerMatcher);
        failure = Misc.clearObjListBestEffort(failure, perWorkerMatchers);
        ownerSelectivityStats.clear();
        Misc.clearObjList(perWorkerSelectivityStats);
        memoryTracker = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeBestEffort(null, compiledFilter);
        failure = Misc.freeBestEffort(failure, bindVarMemory);
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        failure = Misc.freeBestEffort(failure, ownerFilter);
        failure = Misc.freeObjListBestEffort(failure, perWorkerFilters);
        failure = Misc.freeObjListBestEffort(failure, ownerSlaveTimeFrameCursors);
        failure = Misc.freeObjListBestEffort(failure, outputSlaveTimeFrameCursors);
        failure = Misc.freeObjListBestEffort(failure, perWorkerSlaveTimeFrameCursors);
        failure = Misc.freeBestEffort(failure, ownerMatcher);
        failure = Misc.freeObjListBestEffort(failure, perWorkerMatchers);
        failure = Misc.freeBestEffort(failure, ownerHorizonIterator);
        failure = Misc.freeObjListBestEffort(failure, perWorkerHorizonIterators);
        CairoException.rethrowCleanupFailure(failure);
    }

    public @Nullable ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public @Nullable MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public @Nullable CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    public @Nullable Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return ownerFilter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public int getMasterTimestampIndex() {
        return masterTimestampIndex;
    }

    /**
     * Returns how many master rows of one page frame a task matches. The owner thread matches the
     * rest of a larger frame itself, in chunks of the same size, which keeps the per-task output of
     * {@code maxTaskRows * offsetCount * slaveCount} longs bounded even for a Parquet row group the
     * page frame cursor cannot split.
     */
    public long getMaxTaskRows() {
        return maxTaskRows;
    }

    public ConcurrentTimeFrameCursor getOutputSlaveTimeFrameCursor(int slaveIndex) {
        return outputSlaveTimeFrameCursors.getQuick(slaveIndex);
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    public SelectivityStats getSelectivityStats(int slotId) {
        if (slotId == -1) {
            return ownerSelectivityStats;
        }
        return perWorkerSelectivityStats.getQuick(slotId);
    }

    /**
     * Returns the number of longs a matched master row occupies: one slave row id per offset and
     * slave.
     */
    public long getSlotsPerRow() {
        return slotsPerRow;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        if (ownerFilter != null) {
            ownerFilter.init(symbolTableSource, executionContext);
        }
        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext, ownerFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    /**
     * Binds every time frame cursor of one slave to the shared frame state of the current query.
     * Must run after {@link ConcurrentTimeFrameState#of} and before any task matches rows.
     */
    public void initTimeFrameCursors(
            int slaveIndex,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor slavePageFrameCursor,
            ConcurrentTimeFrameState sharedState
    ) {
        // The matchers walk horizon timestamps in order, so MONOTONIC caps the decode buffers of a
        // Parquet slave to a quarter of the budget per pool. The output cursor revisits frames in
        // master row order, which jumps back and forth between the offsets of a row.
        initTimeFrameCursor(ownerSlaveTimeFrameCursors.getQuick(slaveIndex), slavePageFrameCursor, sharedState, ParquetDecodeHint.MONOTONIC);
        ownerMatcher.of(slaveIndex, ownerSlaveTimeFrameCursors.getQuick(slaveIndex), masterSymbolTableSource, slavePageFrameCursor, memoryTracker);
        initTimeFrameCursor(outputSlaveTimeFrameCursors.getQuick(slaveIndex), slavePageFrameCursor, sharedState, ParquetDecodeHint.SCATTERED);
        for (int w = 0; w < workerCount; w++) {
            final ConcurrentTimeFrameCursor cursor = perWorkerSlaveTimeFrameCursors.getQuick(w * slaveCount + slaveIndex);
            initTimeFrameCursor(cursor, slavePageFrameCursor, sharedState, ParquetDecodeHint.MONOTONIC);
            perWorkerMatchers.getQuick(w).of(slaveIndex, cursor, masterSymbolTableSource, slavePageFrameCursor, memoryTracker);
        }
    }

    public boolean isFiltered() {
        return ownerFilter != null;
    }

    /**
     * Matches the master rows at positions {@code [lo, hi)} of a page frame against every slave and
     * writes the slave row ids to {@code outAddress}: the row at position p and offset k starts at
     * long {@code ((p - lo) * offsetCount + k) * slaveCount}. A position is an index into
     * {@code filteredRows} when the master is filtered and a frame row index otherwise.
     * <p>
     * The tuples arrive in horizon timestamp order, so each slave is scanned forward once per call.
     */
    public void match(
            int slotId,
            PageFrameMemoryRecord masterRecord,
            long timestampAddress,
            @Nullable DirectLongList filteredRows,
            long lo,
            long hi,
            long outAddress,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final HorizonJoinMatcher matcher = slotId == -1 ? ownerMatcher : perWorkerMatchers.getQuick(slotId);
        final AsyncHorizonTimestampIterator iterator = slotId == -1 ? ownerHorizonIterator : perWorkerHorizonIterators.getQuick(slotId);
        if (filteredRows != null) {
            iterator.ofFiltered(timestampAddress, filteredRows, lo, hi);
        } else {
            iterator.of(timestampAddress, lo, hi - lo);
        }
        matcher.toTop();
        final boolean isKeyed = matcher.isKeyed();
        while (iterator.next()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final long rowIndex = iterator.getMasterRowIndex();
            final long position = filteredRows != null ? iterator.getMasterRowCompactIndex() : rowIndex;
            if (isKeyed) {
                masterRecord.setRowIndex(rowIndex);
            }
            final long slot = ((position - lo) * offsetCount + iterator.getOffsetIndex()) * slaveCount;
            matcher.match(iterator.getHorizonTimestamp(), masterRecord, outAddress + (slot << 3));
        }
    }

    /**
     * Acquires the slot whose matcher the calling thread uses. The owner thread reducing a task of
     * its own query uses the owner set without locking. On success, {@link #release(int)} must
     * follow.
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    /**
     * Reports whether a task should decode only the filter columns before filtering. Late
     * materialization pays off for a selective filter over a Parquet frame.
     */
    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame, boolean isCountOnly) {
        if (!isParquetFrame || filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return isCountOnly || getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    public void toTop() {
        if (ownerFilter != null) {
            ownerFilter.toTop();
        }
        if (perWorkerFilters != null) {
            for (int i = 0, n = perWorkerFilters.size(); i < n; i++) {
                perWorkerFilters.getQuick(i).toTop();
            }
        }
    }

    private static void initTimeFrameCursor(
            ConcurrentTimeFrameCursor cursor,
            TablePageFrameCursor slavePageFrameCursor,
            ConcurrentTimeFrameState sharedState,
            ParquetDecodeHint hint
    ) {
        cursor.of(sharedState, slavePageFrameCursor, cursor.getTimestampIndex());
        cursor.setParquetDecodeHint(hint);
    }
}
