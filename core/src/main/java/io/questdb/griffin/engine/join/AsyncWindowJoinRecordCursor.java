/*******************************************************************************
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.FlyweightMapValue;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

class AsyncWindowJoinRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncWindowJoinRecordCursor.class);
    private final int columnSplit;
    private final @Nullable IntList crossIndex;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord groupByRecord;
    private final boolean isMasterFiltered;
    private final PageFrameMemoryRecord masterRecord;
    private final Record record;
    private final RecordMetadata slaveMetadata;
    private final LongList slavePartitionCeilings = new LongList();
    private final LongList slavePartitionTimestamps = new LongList();
    private final PageFrameAddressCache slaveTimeFrameAddressCache;
    private final IntList slaveTimeFramePartitionIndexes = new IntList();
    private final LongList slaveTimeFrameRowCounts = new LongList();
    private boolean allFramesActive;
    private long cursor = -1;
    private SqlExecutionContext executionContext;
    private DirectLongList filteredRows;
    private int frameIndex;
    private int frameLimit;
    private long frameRowCount;
    private long frameRowIndex;
    private long frameValueOffset;
    private FlyweightMapValue groupByValue;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private PageFrameSequence<? extends AsyncWindowJoinAtom> masterFrameSequence;
    private TablePageFrameCursor slaveFrameCursor;
    private long valueSizeBytes;

    public AsyncWindowJoinRecordCursor(
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull RecordMetadata slaveMetadata,
            @Nullable IntList columnIndex,
            int columnSplit,
            boolean isMasterFiltered
    ) {
        this.groupByFunctions = groupByFunctions;
        this.slaveMetadata = slaveMetadata;
        this.columnSplit = columnSplit;
        this.isMasterFiltered = isMasterFiltered;
        this.slaveTimeFrameAddressCache = new PageFrameAddressCache();
        this.crossIndex = columnIndex;
        this.masterRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        this.groupByRecord = new VirtualRecord(groupByFunctions);
        final JoinRecord jr = new JoinRecord(columnSplit);
        jr.of(masterRecord, groupByRecord);
        if (columnIndex != null) {
            SelectedRecord sr = new SelectedRecord(columnIndex);
            sr.of(jr);
            this.record = sr;
        } else {
            this.record = jr;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        buildSlaveTimeFrameCacheConditionally();

        if (isMasterFiltered) {
            calculateSizeFiltered(circuitBreaker, counter);
        } else {
            calculateSizeNoFilter(counter);
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (masterFrameSequence != null) {
                    LOG.debug()
                            .$("closing [shard=").$(masterFrameSequence.getShard())
                            .$(", frameIndex=").$(frameIndex)
                            .$(", frameCount=").$(frameLimit)
                            .$(", frameId=").$(masterFrameSequence.getId())
                            .$(", cursor=").$(cursor)
                            .I$();

                    collectCursor(true);
                    if (frameLimit > -1) {
                        masterFrameSequence.await();
                    }
                    masterFrameSequence.reset();
                }
            } finally {
                // Free shared resources only after workers have finished
                Misc.free(slaveFrameCursor);
                Misc.free(slaveTimeFrameAddressCache);
                isOpen = false;
            }
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        if (crossIndex != null) {
            columnIndex = crossIndex.getQuick(columnIndex);
        }
        if (columnIndex < columnSplit) {
            return masterFrameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
        }
        return (SymbolTable) groupByFunctions.getQuick(columnIndex - columnSplit);
    }

    @Override
    public boolean hasNext() {
        buildSlaveTimeFrameCacheConditionally();

        if (isMasterFiltered) {
            return hasNextFiltered();
        }
        return hasNextNoFilter();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (crossIndex != null) {
            columnIndex = crossIndex.getQuick(columnIndex);
        }
        if (columnIndex < columnSplit) {
            return masterFrameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
        }
        return ((SymbolFunction) groupByFunctions.getQuick(columnIndex - columnSplit)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        collectCursor(false);
        masterFrameSequence.toTop();
        masterFrameSequence.getAtom().toTop();
        slaveFrameCursor.toTop();
        // Don't reset frameLimit here since its value is used to prepare frame sequence for dispatch only once.
        frameIndex = -1;
        frameRowIndex = -1;
        frameValueOffset = -1;
        frameRowCount = -1;
        allFramesActive = true;
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            final int frameCount = initializeSlaveTimeFrameCache();
            populatePartitionTimestamps(slaveFrameCursor, slavePartitionTimestamps, slavePartitionCeilings);
            initializeTimeFrameCursors(frameCount);
            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private void calculateSizeFiltered(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        final boolean oldSkipAggregation = masterFrameSequence.getAtom().isSkipAggregation();
        masterFrameSequence.getAtom().setSkipAggregation(true);
        try {
            if (frameIndex == -1) {
                fetchNextFrame();
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }

            // We have rows in the current frame we still need to dispatch
            if (frameRowIndex < frameRowCount) {
                counter.add(frameRowCount - frameRowIndex);
                frameRowIndex = frameRowCount;
            }

            // Release the previous queue item.
            // There is no identity check here because this check
            // had been done when 'cursor' was assigned.
            collectCursor(false);

            while (frameIndex < frameLimit) {
                fetchNextFrame();
                if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                    counter.add(frameRowCount - frameRowIndex);
                    frameRowIndex = frameRowCount;
                    collectCursor(false);
                }

                if (!allFramesActive) {
                    throwTimeoutException();
                }

                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }
        } finally {
            masterFrameSequence.getAtom().setSkipAggregation(oldSkipAggregation);
        }
    }

    private void calculateSizeNoFilter(RecordCursor.Counter counter) {
        if (frameLimit == -1) {
            // Count page frame sizes and call it a day.
            masterFrameSequence.prepareForDispatch();
            frameLimit = masterFrameSequence.getFrameCount() - 1;
            for (int i = 0, n = masterFrameSequence.getFrameCount(); i < n; i++) {
                counter.add(masterFrameSequence.getFrameRowCount(i));
            }
        } else {
            // cursor.hasNext() was called previously.
            // Check if we have something left in the current frame.
            if (frameRowIndex < frameRowCount) {
                counter.add(frameRowCount - frameRowIndex);
                frameRowIndex = frameRowCount;
            }

            // Count sizes of remaining page frames.
            for (int i = frameIndex + 1, n = masterFrameSequence.getFrameCount(); i < n; i++) {
                counter.add(masterFrameSequence.getFrameRowCount(i));
            }

            // Discard what was published.
            collectCursor(true);
            masterFrameSequence.await();
        }
    }

    private void collectCursor(boolean forceCollect) {
        if (cursor > -1) {
            masterFrameSequence.collect(cursor, forceCollect);
            // It is necessary to clear 'cursor' value
            // because we updated frameIndex and loop can exit due to lack of frames.
            // Non-update of 'cursor' could cause double-free.
            cursor = -1;
            // We also need to clear the record as it's initialized with the task's
            // page frame memory that is now closed.
            masterRecord.clear();
        }
    }

    private void fetchNextFrame() {
        if (frameLimit == -1) {
            masterFrameSequence.prepareForDispatch();
            frameLimit = masterFrameSequence.getFrameCount() - 1;
        }

        try {
            do {
                cursor = masterFrameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = masterFrameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(masterFrameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(masterFrameSequence.getFrameCount())
                            .$(", frameId=").$(masterFrameSequence.getId())
                            .$(", active=").$(masterFrameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();

                    if (task.hasError()) {
                        throw CairoException.nonCritical()
                                .position(task.getErrorMessagePosition())
                                .put(task.getErrorMsg())
                                .setCancellation(task.isCancelled())
                                .setInterruption(task.isCancelled())
                                .setOutOfMemory(task.isOutOfMemory());
                    }

                    allFramesActive &= masterFrameSequence.isActive();
                    filteredRows = task.getFilteredRows();
                    // if there is no master filter, this value is set to frame size
                    frameRowCount = task.getFilteredRowCount();
                    frameIndex = task.getFrameIndex();
                    frameRowIndex = 0;
                    frameValueOffset = isMasterFiltered ? frameRowCount * Long.BYTES : 0;
                    if (frameRowCount > 0 && masterFrameSequence.isActive()) {
                        masterRecord.init(task.getFrameMemory());
                        break;
                    } else {
                        // Force reset frame size if frameSequence was canceled or failed.
                        frameRowCount = 0;
                        collectCursor(false);
                    }
                } else if (cursor == -2) {
                    break; // No frames to filter
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable th) {
            if (th instanceof CairoException ce) {
                if (ce.isInterruption() || ce.isCancellation()) {
                    LOG.error().$("filter error [ex=").$safe(ce.getFlyweightMessage()).I$();
                    throwTimeoutException();
                } else {
                    LOG.error().$("filter error [ex=").$(th).I$();
                    throw ce;
                }
            }
            LOG.error().$("filter error [ex=").$(th).I$();
            throw CairoException.nonCritical().put(th.getMessage());
        }
    }

    private boolean hasNextFiltered() {
        // Check for the first hasNext call.
        if (frameIndex == -1) {
            fetchNextFrame();
        }

        // We have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            masterRecord.setRowIndex(filteredRows.get(frameRowIndex++));
            groupByValue.of(filteredRows.getAddress() + frameValueOffset);
            frameValueOffset += valueSizeBytes;
            return true;
        }

        // Release the previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned.
        collectCursor(false);

        // Do we have more frames?
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                masterRecord.setRowIndex(filteredRows.get(frameRowIndex++));
                groupByValue.of(filteredRows.getAddress() + frameValueOffset);
                frameValueOffset += valueSizeBytes;
                return true;
            }
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }
        return false;
    }

    private boolean hasNextNoFilter() {
        // Check for the first hasNext call.
        if (frameIndex == -1) {
            fetchNextFrame();
        }

        // We have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            masterRecord.setRowIndex(frameRowIndex++);
            groupByValue.of(filteredRows.getAddress() + frameValueOffset);
            frameValueOffset += valueSizeBytes;
            return true;
        }

        // Release the previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned.
        collectCursor(false);

        // Do we have more frames?
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                masterRecord.setRowIndex(frameRowIndex++);
                groupByValue.of(filteredRows.getAddress() + frameValueOffset);
                frameValueOffset += valueSizeBytes;
                return true;
            }
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }
        return false;
    }

    private int initializeSlaveTimeFrameCache() {
        slaveTimeFrameAddressCache.of(slaveMetadata, slaveFrameCursor.getColumnIndexes(), slaveFrameCursor.isExternal());
        slaveTimeFramePartitionIndexes.clear();
        slaveTimeFrameRowCounts.clear();

        int frameCount = 0;
        PageFrame frame;
        while ((frame = slaveFrameCursor.next()) != null) {
            slaveTimeFramePartitionIndexes.add(frame.getPartitionIndex());
            slaveTimeFrameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            slaveTimeFrameAddressCache.add(frameCount++, frame);
        }
        return frameCount;
    }

    private void initializeTimeFrameCursors(int frameCount) {
        try {
            masterFrameSequence.getAtom().initTimeFrameCursors(
                    executionContext,
                    masterFrameSequence.getSymbolTableSource(),
                    slaveFrameCursor,
                    slaveTimeFrameAddressCache,
                    slaveTimeFramePartitionIndexes,
                    slaveTimeFrameRowCounts,
                    slavePartitionTimestamps,
                    slavePartitionCeilings,
                    frameCount
            );
        } catch (SqlException e) {
            throw CairoException.nonCritical().put(e.getFlyweightMessage());
        }
    }

    private void throwTimeoutException() {
        if (masterFrameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(
            PageFrameSequence<? extends AsyncWindowJoinAtom> masterFrameSequence,
            TablePageFrameCursor slaveFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final AsyncWindowJoinAtom atom = masterFrameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.masterFrameSequence = masterFrameSequence;
        this.slaveFrameCursor = slaveFrameCursor;
        this.executionContext = executionContext;
        allFramesActive = true;
        isSlaveTimeFrameCacheBuilt = false;
        frameIndex = -1;
        frameLimit = -1;
        frameRowIndex = -1;
        frameValueOffset = -1;
        frameRowCount = -1;
        masterRecord.of(masterFrameSequence.getSymbolTableSource());
        valueSizeBytes = atom.getValueSizeBytes();
        groupByValue = atom.getOwnerGroupByValue();
        groupByRecord.of(groupByValue);
    }
}
