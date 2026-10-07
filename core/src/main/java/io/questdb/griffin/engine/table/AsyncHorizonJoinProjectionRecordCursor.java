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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Owner-side cursor of {@link AsyncHorizonJoinProjectionRecordCursorFactory}. Collects the
 * reduced page frames in frame order and emits, for every master row that passed the filter, one
 * row per offset in offset order.
 */
class AsyncHorizonJoinProjectionRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncHorizonJoinProjectionRecordCursor.class);
    private final int defaultDispatchLimit;
    private final boolean isMasterFiltered;
    // Positioned on the output rows; reads the collected task's frame memory.
    private final PageFrameMemoryRecord masterRecord;
    private final int masterTimestampIndex;
    // Positioned on master rows while the owner thread matches the tail of an oversized frame.
    private final PageFrameMemoryRecord matchRecord;
    private final ObjList<Record> matchedSlaveRecords;
    private final long maxTaskRows;
    private final ObjList<Record> nullSlaveRecords;
    private final int offsetCount;
    private final long[] offsets;
    private final MultiHorizonJoinRecord record;
    private final int slaveCount;
    private final ObjList<RecordCursorFactory> slaveFactories;
    private final ObjList<TablePageFrameCursor> slaveFrameCursors;
    private final ObjList<SymbolTableSource> slaveSymbolTableSources;
    private final ObjList<ConcurrentTimeFrameState> slaveTimeFrameStates;
    private final long slotsPerRow;
    private final MultiHorizonJoinSymbolTableSource symbolTableSource;
    // Matches of the frame rows past the ones a task matched, see matchTailChunk().
    private final DirectLongList tailSlots;
    private boolean allFramesActive;
    // Address of the slave row ids of the rows [chunkLo, chunkHi) of the current frame.
    private long chunkAddress;
    private long chunkHi;
    private long chunkLo;
    private long cursor = -1;
    private int dispatchLimit;
    private SqlExecutionContext executionContext;
    // Filtered row indexes of the current frame, followed by the slave row ids of its first rows.
    private DirectLongList filteredRows;
    private int frameIndex;
    private int frameLimit;
    // Number of master rows of the current frame that passed the filter.
    private long frameRowCount;
    private PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence;
    private long frameTimestampAddress;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private long masterTimestamp;
    private int offsetPosition;
    // Position of the current master row within the rows of the frame that passed the filter.
    private long rowPosition;

    AsyncHorizonJoinProjectionRecordCursor(
            @NotNull ObjList<RecordCursorFactory> slaveFactories,
            long @NotNull [] offsets,
            int masterTimestampIndex,
            long maxTaskRows,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            boolean isMasterFiltered,
            int defaultDispatchLimit
    ) {
        this.slaveFactories = slaveFactories;
        this.slaveCount = slaveFactories.size();
        this.offsets = offsets;
        this.offsetCount = offsets.length;
        this.slotsPerRow = (long) offsetCount * slaveCount;
        this.masterTimestampIndex = masterTimestampIndex;
        this.maxTaskRows = maxTaskRows;
        this.isMasterFiltered = isMasterFiltered;
        this.defaultDispatchLimit = defaultDispatchLimit;
        this.matchedSlaveRecords = new ObjList<>(slaveCount);
        matchedSlaveRecords.setPos(slaveCount);
        this.nullSlaveRecords = new ObjList<>(slaveCount);
        this.slaveFrameCursors = new ObjList<>(slaveCount);
        slaveFrameCursors.setPos(slaveCount);
        this.slaveSymbolTableSources = new ObjList<>(slaveCount);
        slaveSymbolTableSources.setPos(slaveCount);
        this.slaveTimeFrameStates = new ObjList<>(slaveCount);
        this.symbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);
        this.record = new MultiHorizonJoinRecord(slaveCount);
        record.init(columnSources, columnIndexes);
        PageFrameMemoryRecord masterRecord = null;
        PageFrameMemoryRecord matchRecord = null;
        DirectLongList tailSlots = null;
        try {
            for (int s = 0; s < slaveCount; s++) {
                // Typed NULL records keep the column type semantics of an unmatched slave row: a
                // SYMBOL column reads VALUE_IS_NULL as its key, not INT_NULL.
                nullSlaveRecords.add(NullRecordFactory.getInstance(slaveFactories.getQuick(s).getMetadata()));
                slaveTimeFrameStates.add(new ConcurrentTimeFrameState());
            }
            masterRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            matchRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            tailSlots = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            Misc.freeObjList(slaveTimeFrameStates, th);
            Misc.free(masterRecord, th);
            Misc.free(matchRecord, th);
            Misc.free(tailSlots, th);
            throw th;
        }
        this.masterRecord = masterRecord;
        this.matchRecord = matchRecord;
        this.tailSlots = tailSlots;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isMasterFiltered) {
            calculateSizeNoFilter(counter);
            return;
        }
        if (frameIndex == -1) {
            fetchNextFrame(true);
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        }
        counter.add(remainingFrameRows());
        rowPosition = frameRowCount;
        offsetPosition = 0;
        collectCursor(false);

        while (frameIndex < frameLimit) {
            fetchNextFrame(true);
            if (frameRowCount > 0) {
                counter.add(frameRowCount * offsetCount);
                rowPosition = frameRowCount;
                collectCursor(false);
            }
            if (!allFramesActive) {
                throw buildInterruptionException();
            }
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    LOG.debug()
                            .$("closing [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(frameIndex)
                            .$(", frameCount=").$(frameLimit)
                            .$(", frameId=").$(frameSequence.getId())
                            .$(", cursor=").$(cursor)
                            .I$();
                    collectCursor(true);
                    if (frameLimit > -1) {
                        // The reader may stop ahead of the last frame, as it does under a LIMIT.
                        // A task matches up to maxTaskRows rows, so workers skip the published
                        // tasks they have not started instead of reducing them for nothing. The
                        // frame sequence resets the cancellation when the factory reopens it.
                        frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                        frameSequence.await();
                    }
                    // Clears the atom, which releases the time frame cursors bound to the states below.
                    frameSequence.reset();
                }
            } finally {
                // Free the shared resources only after the workers have finished.
                Misc.freeObjList(slaveFrameCursors);
                Misc.freeObjListAndKeepObjects(slaveTimeFrameStates);
                Misc.free(tailSlots);
                // The records cache symbol tables and array buffers. close() ends in clear(), so
                // they stay reusable when the factory reopens this cursor.
                Misc.free(masterRecord);
                Misc.free(matchRecord);
                frameSequence = null;
                isOpen = false;
            }
        }
    }

    @Override
    public void expectLimitedIteration() {
        // Only the limited light sorts call this, and only over a random-access base, which this
        // cursor is not, so no plan reaches it. A LIMIT over this factory compiles to
        // LimitRecordCursorFactory, whose cursor does not call it: dispatch stays unlimited, and
        // close() cancels the tasks that the reader leaves unstarted. The override caps the
        // in-flight tasks for a parent that announces a limited read.
        dispatchLimit = defaultDispatchLimit;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return symbolTableSource.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        buildSlaveTimeFrameCacheConditionally();
        if (frameIndex == -1) {
            fetchNextFrame(false);
        }
        while (true) {
            if (rowPosition < frameRowCount) {
                if (offsetPosition == 0) {
                    if (rowPosition == chunkHi) {
                        matchTailChunk();
                    }
                    masterRecord.setRowIndex(isMasterFiltered ? filteredRows.get(rowPosition) : rowPosition);
                    masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                }
                final long slotAddress = chunkAddress + ((((rowPosition - chunkLo) * offsetCount + offsetPosition) * slaveCount) << 3);
                final AsyncHorizonJoinProjectionAtom atom = frameSequence.getAtom();
                for (int s = 0; s < slaveCount; s++) {
                    final long slaveRowId = Unsafe.getLong(slotAddress + ((long) s << 3));
                    if (slaveRowId != Long.MIN_VALUE) {
                        final ConcurrentTimeFrameCursor slaveCursor = atom.getOutputSlaveTimeFrameCursor(s);
                        final Record slaveRecord = slaveCursor.getRecord();
                        slaveCursor.recordAt(slaveRecord, slaveRowId);
                        matchedSlaveRecords.setQuick(s, slaveRecord);
                    } else {
                        matchedSlaveRecords.setQuick(s, nullSlaveRecords.getQuick(s));
                    }
                }
                final long offset = offsets[offsetPosition];
                // Matching already added every offset to the master timestamp without overflow.
                record.of(masterRecord, offset, masterTimestamp + offset, matchedSlaveRecords);
                if (++offsetPosition == offsetCount) {
                    offsetPosition = 0;
                    rowPosition++;
                }
                return true;
            }

            // Release the current frame. There is no identity check here: it was done when
            // 'cursor' was assigned.
            collectCursor(false);
            if (frameIndex < frameLimit) {
                fetchNextFrame(false);
                if (frameRowCount > 0) {
                    continue;
                }
            }
            if (!allFramesActive) {
                throw buildInterruptionException();
            }
            return false;
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return symbolTableSource.newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        if (isMasterFiltered) {
            return -1;
        }
        prepareForDispatchConditionally();
        long rowCount = 0;
        for (int i = 0, n = frameSequence.getFrameCount(); i < n; i++) {
            rowCount += frameSequence.getFrameRowCount(i);
        }
        return rowCount * offsetCount;
    }

    @Override
    public void toTop() {
        collectCursor(false);
        frameSequence.toTop();
        frameSequence.getAtom().toTop();
        // Keep frameLimit: the frame sequence prepares for dispatch only once.
        frameIndex = -1;
        frameRowCount = 0;
        rowPosition = 0;
        offsetPosition = 0;
        allFramesActive = true;
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            final AsyncHorizonJoinProjectionAtom atom = frameSequence.getAtom();
            final SymbolTableSource masterSymbolTableSource = frameSequence.getSymbolTableSource();
            for (int s = 0; s < slaveCount; s++) {
                final TablePageFrameCursor slaveFrameCursor = slaveFrameCursors.getQuick(s);
                final ConcurrentTimeFrameState state = slaveTimeFrameStates.getQuick(s);
                state.of(
                        slaveFrameCursor,
                        slaveFactories.getQuick(s).getMetadata(),
                        slaveFrameCursor.getColumnMapping(),
                        slaveFrameCursor.isExternal(),
                        executionContext.getPageFrameMinRows(),
                        executionContext.getPageFrameMaxRows(),
                        executionContext.getSharedQueryWorkerCount(),
                        executionContext.getMemoryTracker()
                );
                atom.initTimeFrameCursors(s, masterSymbolTableSource, slaveFrameCursor, state);
            }
            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private CairoException buildInterruptionException() {
        return frameSequence.buildInterruptionException();
    }

    private void calculateSizeNoFilter(Counter counter) {
        if (frameIndex == -1) {
            prepareForDispatchConditionally();
            // Nothing was dispatched yet, so every frame still lies ahead.
            for (int i = 0, n = frameSequence.getFrameCount(); i < n; i++) {
                counter.add(frameSequence.getFrameRowCount(i) * offsetCount);
            }
        } else {
            counter.add(remainingFrameRows());
            for (int i = frameIndex + 1, n = frameSequence.getFrameCount(); i < n; i++) {
                counter.add(frameSequence.getFrameRowCount(i) * offsetCount);
            }
            // Discard what was published.
            collectCursor(true);
            frameSequence.await();
        }
        // Leave the cursor exhausted, so that a following hasNext() returns false.
        frameIndex = frameLimit;
        frameRowCount = 0;
        rowPosition = 0;
        offsetPosition = 0;
    }

    private void collectCursor(boolean forceCollect) {
        if (cursor > -1) {
            frameSequence.collect(cursor, forceCollect);
            // Clear 'cursor': the frame index moved on and the loop can exit for lack of
            // frames, so a stale value would collect the task twice.
            cursor = -1;
            // The records were initialized with the task's frame memory, which is now released.
            masterRecord.clear();
            matchRecord.clear();
            filteredRows = null;
        }
    }

    private void fetchNextFrame(boolean isCountOnly) {
        prepareForDispatchConditionally();
        try {
            do {
                cursor = frameSequence.next(dispatchLimit, isCountOnly);
                if (cursor > -1) {
                    final PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", frameId=").$(frameSequence.getId())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (task.hasError()) {
                        throw task.buildError();
                    }

                    allFramesActive &= frameSequence.isActive();
                    frameIndex = task.getFrameIndex();
                    frameRowCount = task.getFilteredRowCount();
                    rowPosition = 0;
                    offsetPosition = 0;
                    if (frameRowCount > 0 && frameSequence.isActive()) {
                        if (!isCountOnly) {
                            final PageFrameMemory frameMemory = task.getFrameMemory();
                            masterRecord.init(frameMemory);
                            matchRecord.init(frameMemory);
                            frameTimestampAddress = frameMemory.getPageAddress(masterTimestampIndex);
                            filteredRows = task.getFilteredRows();
                            // The task matched the first rows; their slave row ids follow the
                            // filtered row indexes.
                            chunkLo = 0;
                            chunkHi = Math.min(frameRowCount, maxTaskRows);
                            chunkAddress = filteredRows.getAddress() + (isMasterFiltered ? frameRowCount << 3 : 0);
                        }
                        break;
                    } else {
                        // Force reset the frame size if the frame sequence was cancelled or failed.
                        frameRowCount = 0;
                        collectCursor(false);
                    }
                } else if (cursor == -2) {
                    // No frames to reduce.
                    break;
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable th) {
            if (th instanceof CairoException ce) {
                if (ce.isInterruption() || ce.isCancellation()) {
                    LOG.error().$("horizon join error [ex=").$safe(ce.getFlyweightMessage()).I$();
                    throw buildInterruptionException();
                }
                LOG.error().$("horizon join error [ex=").$(th).I$();
                throw ce;
            }
            LOG.error().$("horizon join error [ex=").$(th).I$();
            // Preserve typed user-facing errors (ImplicitCastException / NumericException) raised
            // via task.buildError(), so the caller can recognise them.
            if (th instanceof ImplicitCastException || th instanceof NumericException) {
                throw (RuntimeException) th;
            }
            throw CairoException.nonCritical().put(th.getMessage());
        }
    }

    /**
     * Matches the next chunk of the current frame on the owner thread. A task matches at most
     * {@code maxTaskRows} rows of its frame, so that its output stays bounded; the page frame
     * cursor cannot split a Parquet row group, so such a frame can hold more rows than that.
     */
    private void matchTailChunk() {
        final long lo = chunkHi;
        final long hi = Math.min(frameRowCount, lo + maxTaskRows);
        final long slotCount = (hi - lo) * slotsPerRow;
        tailSlots.clear();
        tailSlots.ensureCapacity(slotCount);
        frameSequence.getAtom().match(
                -1,
                matchRecord,
                frameTimestampAddress,
                isMasterFiltered ? filteredRows : null,
                lo,
                hi,
                tailSlots.getAddress(),
                executionContext.getCircuitBreaker()
        );
        chunkLo = lo;
        chunkHi = hi;
        chunkAddress = tailSlots.getAddress();
    }

    private void prepareForDispatchConditionally() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }
    }

    // Output rows left in the current frame, including the offsets of the current master row.
    private long remainingFrameRows() {
        return rowPosition < frameRowCount ? (frameRowCount - rowPosition) * offsetCount - offsetPosition : 0;
    }

    void of(PageFrameSequence<AsyncHorizonJoinProjectionAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        // Assign before anything can throw so that close() can reset the sequence.
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;
        isOpen = true;
        // A zero-frame sequence keeps frameLimit at -1 after prepareForDispatch(), which the
        // fetch and close paths handle as "nothing dispatched".
        frameLimit = -1;
        frameIndex = -1;
        frameRowCount = 0;
        rowPosition = 0;
        offsetPosition = 0;
        chunkLo = 0;
        chunkHi = 0;
        filteredRows = null;
        allFramesActive = true;
        isSlaveTimeFrameCacheBuilt = false;
        dispatchLimit = Integer.MAX_VALUE;
        for (int s = 0; s < slaveCount; s++) {
            final TablePageFrameCursor slaveFrameCursor = (TablePageFrameCursor) slaveFactories.getQuick(s).getPageFrameCursor(executionContext, ORDER_ASC);
            slaveFrameCursors.setQuick(s, slaveFrameCursor);
            slaveSymbolTableSources.setQuick(s, slaveFrameCursor);
        }
        final SymbolTableSource masterSymbolTableSource = frameSequence.getSymbolTableSource();
        symbolTableSource.of(masterSymbolTableSource, slaveSymbolTableSources);
        masterRecord.of(masterSymbolTableSource);
        matchRecord.of(masterSymbolTableSource);
        tailSlots.setMemoryTracker(executionContext.getMemoryTracker());
    }
}
