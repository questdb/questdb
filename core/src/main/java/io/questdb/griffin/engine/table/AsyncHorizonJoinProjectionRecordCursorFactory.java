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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Parallel HORIZON JOIN without aggregation: one output row per master row and offset.
 * <p>
 * Workers reduce the master page frames. Each task filters its frame, walks the (row, offset)
 * pairs in horizon timestamp order and records the matched row id of every slave in the task's
 * row list, after the filtered row indexes. The owner thread collects the tasks in frame order
 * and emits the rows in master row order, one row per offset in offset order, reading the matched
 * slave rows through its own time frame cursors. The output is therefore ordered by the master's
 * designated timestamp, which the metadata keeps.
 * <p>
 * The per-task output grows with the product of rows, offsets and slaves, so a task matches at
 * most {@link AsyncHorizonJoinProjectionAtom#getMaxTaskRows()} rows. The factory sizes native
 * page frames to fit; the owner thread matches the rest of a larger (Parquet) frame itself.
 * <p>
 * The native frames shrink as the slots per master row, offsets times slaves, grow. The code
 * generator therefore picks this factory for at most {@link #MAX_SLOTS_PER_MASTER_ROW} slots and
 * the serial {@link HorizonJoinProjectionRecordCursorFactory} past that, unless the master lacks
 * the random access the serial factory needs, as a covering index scan does.
 */
public class AsyncHorizonJoinProjectionRecordCursorFactory extends AbstractRecordCursorFactory {
    /**
     * The most slots per master row, offsets times slaves, for which the code generator picks this
     * factory over a master with random access. A frame costs a reduce task and a page address
     * cache entry whether or not any of its rows pass the filter, and the factory cuts the master
     * into frames of the small frame budget divided by the slots. Within the cap, the frames stay
     * at 1/100 of that budget or larger, 1,000 rows at the default budget of 100,000, so such a
     * scan makes at most 100 times the frames of a small-frame scan over the same rows. A master
     * without random access, such as a covering index scan, keeps this factory above the cap, as
     * the class javadoc says, and its frames shrink further with the slots.
     */
    public static final int MAX_SLOTS_PER_MASTER_ROW = 100;
    private static final PageFrameReducer FILTER_AND_MATCH = AsyncHorizonJoinProjectionRecordCursorFactory::filterAndMatch;
    private static final PageFrameReducer MATCH = AsyncHorizonJoinProjectionRecordCursorFactory::match;
    private final SCSequence collectSubSeq = new SCSequence();
    private final MasterFrameSource masterFrameSource = new MasterFrameSource();
    private final int offsetCount;
    private final int pageFrameMaxRows;
    private final int pageFrameMinRows;
    private final int workerCount;
    private AsyncHorizonJoinProjectionRecordCursor cursor;
    private PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence;
    private JoinRecordMetadata horizonJoinMetadata;
    private RecordCursorFactory masterFactory;
    private AsyncHorizonJoinResources resources;
    private ObjList<RecordCursorFactory> slaveFactories;
    private ObjList<HorizonJoinSlaveState> slaveStates;

    public AsyncHorizonJoinProjectionRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull JoinRecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            long @NotNull [] offsets,
            int masterTimestampIndex,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull AsyncHorizonJoinResources resources,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(metadata);
        assert masterFactory.supportsPageFrameCursor();
        // Adopt every owned argument before anything can throw: _close() frees them on failure.
        this.horizonJoinMetadata = metadata;
        this.masterFactory = masterFactory;
        this.slaveStates = slaveStates;
        this.resources = resources;
        this.offsetCount = offsets.length;
        this.workerCount = workerCount;
        final int slaveCount = slaveStates.size();
        final long slotsPerRow = (long) offsetCount * slaveCount;
        // A task stores one long per matched (row, offset, slave), and the owner thread keeps up
        // to a queue's worth of finished tasks. Native page frames stay within the small frame
        // budget, the same one parallel window joins use. A task matches at most as many longs as
        // the row id list of a full-size filter frame holds, so a Parquet row group larger than
        // the small budget still reduces on a worker when its offsets are few.
        final long frameRows = Math.max(1, configuration.getSqlSmallPageFrameMaxRows() / slotsPerRow);
        this.pageFrameMaxRows = (int) frameRows;
        this.pageFrameMinRows = (int) Math.max(1, Math.min(configuration.getSqlSmallPageFrameMinRows(), frameRows));
        final long maxTaskRows = Math.max(1, configuration.getSqlPageFrameMaxRows() / slotsPerRow);
        try {
            this.slaveFactories = new ObjList<>(slaveCount);
            final AsyncHorizonJoinProjectionAtom atom = new AsyncHorizonJoinProjectionAtom(
                    configuration,
                    slaveStates,
                    masterAsOfJoinMapSinkClasses,
                    slaveAsOfJoinMapSinkClasses,
                    offsets,
                    masterTimestampIndex,
                    maxTaskRows,
                    resources,
                    workerCount
            );
            final boolean isFiltered = atom.isFiltered();
            // The frame sequence adopts the atom, freeing it on its own constructor failure.
            this.frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    isFiltered ? FILTER_AND_MATCH : MATCH,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_HORIZON_JOIN
            );
            // Transfer one state factory at a time. The destination owns an entry only after add()
            // succeeds; detach then makes the state suffix the sole rollback owner.
            for (int i = 0; i < slaveCount; i++) {
                final HorizonJoinSlaveState state = slaveStates.getQuick(i);
                slaveFactories.add(state.getFactory());
                state.detachFactory();
            }
            this.cursor = new AsyncHorizonJoinProjectionRecordCursor(
                    slaveFactories,
                    offsets,
                    masterTimestampIndex,
                    maxTaskRows,
                    columnSources,
                    columnIndexes,
                    isFiltered,
                    configuration.getSqlParallelFilterDispatchLimit()
            );
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * Returns whether a projection with the given numbers of offsets and slaves stays within
     * {@link #MAX_SLOTS_PER_MASTER_ROW}.
     */
    public static boolean isWithinSlotCap(int offsetCount, int slaveCount) {
        return (long) offsetCount * slaveCount <= MAX_SLOTS_PER_MASTER_ROW;
    }

    @Override
    @TestOnly
    public StatefulAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Consult the breaker at open, so a scan over an empty table still observes cancellation.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        final PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence = execute(executionContext);
        try {
            cursor.of(frameSequence, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        // Rows follow the master in ascending timestamp order, one row per offset.
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(usesCompiledFilter() ? "Async JIT Horizon Join Projection" : "Async Horizon Join Projection");
        sink.meta("workers").val(workerCount);
        sink.meta("offsets").val(offsetCount);
        final AsyncHorizonJoinProjectionAtom atom = frameSequence.getAtom();
        final Function filter = atom.getFilter(-1);
        if (filter != null) {
            sink.attr("filter").val(filter, masterFactory);
        }
        sink.child(masterFactory);
        for (int i = 0, n = slaveFactories.size(); i < n; i++) {
            sink.child(slaveFactories.getQuick(i));
        }
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    @Override
    public boolean usesExternalDataSource() {
        final RecordCursorFactory masterFactory = this.masterFactory;
        if (masterFactory != null && masterFactory.usesExternalDataSource()) {
            return true;
        }
        final ObjList<RecordCursorFactory> slaveFactories = this.slaveFactories;
        for (int i = 0, n = slaveFactories != null ? slaveFactories.size() : 0; i < n; i++) {
            final RecordCursorFactory slaveFactory = slaveFactories.getQuick(i);
            if (slaveFactory != null && slaveFactory.usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }

    private static void filterAndMatch(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinProjectionAtom.class);
        final AsyncHorizonJoinProjectionAtom atom = frameSequence.getAtom();
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        // populateFrameMemory() decodes the frame and can throw, so it sits inside the try that
        // releases the slot, see PerWorkerLocks.acquireSlot().
        try {
            final boolean isParquetFrame = task.isParquetFrame();
            final boolean isCountOnly = task.isCountOnly();
            final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame, isCountOnly);
            final PageFrameMemory frameMemory = useLateMaterialization
                    ? task.populateFrameMemory(atom.getFilterUsedColumnIndexes())
                    : task.populateFrameMemory();
            record.init(frameMemory);

            final CompiledFilter compiledFilter = atom.getCompiledFilter();
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                applyFilter(atom.getFilter(slotId), rows, record, frameRowCount);
            } else {
                applyCompiledFilter(
                        compiledFilter,
                        atom.getBindVarMemory(),
                        atom.getBindVarFunctions(),
                        frameMemory,
                        frameSequence.getPageFrameAddressCache(),
                        task.getDataAddresses(),
                        task.getAuxAddresses(),
                        rows,
                        frameRowCount
                );
            }

            final long filteredRowCount = rows.size();
            task.setFilteredRowCount(filteredRowCount);
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(filteredRowCount, frameRowCount);
            }
            if (filteredRowCount == 0 || isCountOnly) {
                return;
            }

            // Decode the remaining columns before the slots extend the list: the call reads the
            // filtered row indexes, which occupy the whole list until then.
            if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                record.init(frameMemory);
            }
            final long matchedRowCount = Math.min(filteredRowCount, atom.getMaxTaskRows());
            final long slotCount = matchedRowCount * atom.getSlotsPerRow();
            rows.ensureCapacity(slotCount);
            atom.match(
                    slotId,
                    record,
                    frameMemory.getPageAddress(atom.getMasterTimestampIndex()),
                    rows,
                    0,
                    matchedRowCount,
                    rows.getAddress() + (filteredRowCount << 3),
                    circuitBreaker
            );
            rows.skip(slotCount);
        } finally {
            atom.release(slotId);
        }
    }

    private static void match(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinProjectionAtom.class);
        final AsyncHorizonJoinProjectionAtom atom = frameSequence.getAtom();
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        task.setFilteredRowCount(frameRowCount);
        if (task.isCountOnly()) {
            // Without a filter every row counts; the owner thread never asks for this, but the
            // answer is cheap either way.
            return;
        }

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final PageFrameMemory frameMemory = task.populateFrameMemory();
            record.init(frameMemory);
            final long matchedRowCount = Math.min(frameRowCount, atom.getMaxTaskRows());
            final long slotCount = matchedRowCount * atom.getSlotsPerRow();
            rows.ensureCapacity(slotCount);
            atom.match(
                    slotId,
                    record,
                    frameMemory.getPageAddress(atom.getMasterTimestampIndex()),
                    null,
                    0,
                    matchedRowCount,
                    rows.getAddress(),
                    circuitBreaker
            );
            rows.skip(slotCount);
        } finally {
            atom.release(slotId);
        }
    }

    private PageFrameSequence<AsyncHorizonJoinProjectionAtom> execute(SqlExecutionContext executionContext) throws SqlException {
        // The frame sequence opens the master's page frame cursor through masterFrameSource,
        // which sizes the master frames.
        return frameSequence.of(masterFrameSource, executionContext, collectSubSeq, ORDER_ASC);
    }

    /**
     * Opens the master's page frame cursor with the frame sizes of this factory. The sizes sit on
     * the execution context for the duration of the master's cursor open only, and the method then
     * restores the sizes the context had on entry. The cursors the query opens afterwards scan
     * with those: the sub-queries of the stolen filter, which
     * {@link AsyncHorizonJoinProjectionAtom#init} opens right after this call, and the slaves. A
     * sub-query that the master's cursor open evaluates itself, such as one that bounds a runtime
     * timestamp interval, scans with the master's sizes.
     */
    private PageFrameCursor openMasterFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        final int contextMinRows = executionContext.getPageFrameMinRows();
        final int contextMaxRows = executionContext.getPageFrameMaxRows();
        try {
            executionContext.changePageFrameSizes(pageFrameMinRows, pageFrameMaxRows);
            return masterFactory.getPageFrameCursor(executionContext, order);
        } finally {
            executionContext.changePageFrameSizes(contextMinRows, contextMaxRows);
        }
    }

    @Override
    protected void _close() {
        final AsyncHorizonJoinProjectionRecordCursor cursor = this.cursor;
        this.cursor = null;
        final PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final ObjList<RecordCursorFactory> slaveFactories = this.slaveFactories;
        this.slaveFactories = null;
        final ObjList<HorizonJoinSlaveState> slaveStates = this.slaveStates;
        this.slaveStates = null;
        final JoinRecordMetadata horizonJoinMetadata = this.horizonJoinMetadata;
        this.horizonJoinMetadata = null;
        final AsyncHorizonJoinResources resources = this.resources;
        this.resources = null;

        // Free the cursor before the frame sequence: a cursor left half-open by a failed
        // getCursor() still references the sequence and resets it on close().
        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, frameSequence);
        failure = Misc.freeBestEffort(failure, masterFactory);
        failure = Misc.freeObjListBestEffort(failure, slaveFactories);
        failure = Misc.freeObjListBestEffort(failure, slaveStates);
        failure = Misc.freeBestEffort(failure, horizonJoinMetadata);
        failure = Misc.freeBestEffort(failure, resources);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The master as the frame sequence sees it: {@link PageFrameSequence#of} opens the page frame
     * cursor through this factory, which lets {@link #openMasterFrameCursor} size the frames.
     */
    private class MasterFrameSource implements RecordCursorFactory {

        @Override
        public RecordMetadata getMetadata() {
            return masterFactory.getMetadata();
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
            return openMasterFrameCursor(executionContext, order);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return masterFactory.recordCursorSupportsRandomAccess();
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }
    }
}
