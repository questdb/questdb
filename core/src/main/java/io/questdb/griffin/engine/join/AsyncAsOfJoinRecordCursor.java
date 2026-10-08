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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameState;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Collects the joined page frames of {@link AsyncAsOfJoinRecordCursorFactory} in master order. A row
 * joins the master record of the frame with the slave row the frame's output names; blocks expose the
 * master columns from the frame's memory and the gathered slave columns from the output.
 */
class AsyncAsOfJoinRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncAsOfJoinRecordCursor.class);
    private final int columnSplit;
    private final boolean isMasterFiltered;
    private final RecordMetadata masterMetadata;
    private final PageFrameMemoryRecord masterRecord;
    private final OuterJoinRecord record;
    private final RecordCursorFactory slaveFactory;
    private final RecordMetadata slaveMetadata;
    private final Record slaveRecord;
    private final ConcurrentTimeFrameCursor slaveRowCursor;
    private final ConcurrentTimeFrameState slaveTimeFrameState;
    private boolean allFramesActive;
    private JoinBlock block;
    private long cursor = -1;
    private SqlExecutionContext executionContext;
    private DirectLongList filteredRows;
    private int frameIndex;
    private int frameLimit;
    private long frameRowCount;
    private long frameRowIndex;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private PageFrameSequence<AsyncAsOfJoinAtom> masterFrameSequence;
    private TablePageFrameCursor slaveFrameCursor;

    AsyncAsOfJoinRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata masterMetadata,
            @NotNull RecordCursorFactory slaveFactory,
            int columnSplit,
            boolean isMasterFiltered
    ) {
        try {
            this.isOpen = true;
            this.slaveTimeFrameState = new ConcurrentTimeFrameState();
            this.slaveFactory = slaveFactory;
            this.slaveMetadata = slaveFactory.getMetadata();
            this.masterMetadata = masterMetadata;
            this.columnSplit = columnSplit;
            this.isMasterFiltered = isMasterFiltered;
            this.masterRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            this.slaveRowCursor = slaveFactory.newTimeFrameCursor();
            this.slaveRecord = slaveRowCursor.getRecord();
            this.record = new OuterJoinRecord(columnSplit, NullRecordFactory.getInstance(slaveMetadata));
            record.of(masterRecord, slaveRecord);
            this.isOpen = false;
        } catch (Throwable th) {
            close();
            throw th;
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
                    collectCursor(true);
                    if (frameLimit > -1) {
                        masterFrameSequence.await();
                    }
                    masterFrameSequence.reset();
                }
            } finally {
                // free shared resources only after the workers have finished; the time frame
                // cursor and the state are reusable after close, so the next open rebinds them
                slaveFrameCursor = Misc.free(slaveFrameCursor);
                Misc.free(slaveRowCursor);
                Misc.free(slaveTimeFrameState);
                Misc.free(masterRecord);
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
        if (columnIndex < columnSplit) {
            return masterFrameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
        }
        return slaveFrameCursor.getSymbolTable(columnIndex - columnSplit);
    }

    @Override
    public boolean hasNext() {
        buildSlaveTimeFrameCacheConditionally();
        // Check for the first hasNext call.
        if (frameIndex == -1) {
            fetchNextFrame();
        }
        if (frameRowIndex < frameRowCount) {
            positionAt(frameRowIndex++);
            return true;
        }
        collectCursor(false);
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                positionAt(frameRowIndex++);
                return true;
            }
        }
        if (!allFramesActive) {
            throw buildInterruptionException();
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return masterFrameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
        }
        return slaveFrameCursor.newSymbolTable(columnIndex - columnSplit);
    }

    /**
     * The rest of the current frame's rows; when the current frame has no row left, the next frame
     * comes first, as {@link #hasNext()} would fetch it.
     */
    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        buildSlaveTimeFrameCacheConditionally();
        if (frameIndex == -1) {
            fetchNextFrame();
        }
        if (frameRowIndex >= frameRowCount) {
            collectCursor(false);
            if (frameIndex >= frameLimit) {
                return null;
            }
            fetchNextFrame();
            if (frameRowCount < 1 || frameRowIndex >= frameRowCount) {
                return null;
            }
        }
        if (block == null) {
            block = new JoinBlock();
        }
        block.of((int) Math.min(frameRowCount - frameRowIndex, maxRows));
        return block;
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
    public void skipRecordBlock(int rowCount) {
        assert frameRowIndex + rowCount <= frameRowCount;
        frameRowIndex += rowCount;
    }

    @Override
    public boolean supportsRecordBlocks() {
        return true;
    }

    @Override
    public void toTop() {
        collectCursor(false);
        masterFrameSequence.toTop();
        masterFrameSequence.getAtom().toTop();
        slaveFrameCursor.toTop();
        frameIndex = -1;
        frameRowIndex = -1;
        frameRowCount = -1;
        allFramesActive = true;
    }

    private CairoException buildInterruptionException() {
        return masterFrameSequence.buildInterruptionException();
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            slaveTimeFrameState.of(
                    slaveFrameCursor,
                    slaveMetadata,
                    slaveFrameCursor.getColumnMapping(),
                    slaveFrameCursor.isExternal(),
                    executionContext.getPageFrameMinRows(),
                    executionContext.getPageFrameMaxRows(),
                    executionContext.getSharedQueryWorkerCount(),
                    executionContext.getMemoryTracker()
            );
            try {
                masterFrameSequence.getAtom().initTimeFrameCursors(
                        executionContext,
                        masterFrameSequence.getSymbolTableSource(),
                        slaveFrameCursor,
                        slaveTimeFrameState
                );
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
            slaveRowCursor.of(slaveTimeFrameState, slaveFrameCursor, slaveRowCursor.getTimestampIndex());
            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private void calculateSizeFiltered(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        final AsyncAsOfJoinAtom atom = masterFrameSequence.getAtom();
        final boolean oldSkipJoin = atom.isSkipJoin();
        atom.setSkipJoin(true);
        try {
            if (frameIndex == -1) {
                fetchNextFrame();
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            if (frameRowIndex < frameRowCount) {
                counter.add(frameRowCount - frameRowIndex);
                frameRowIndex = frameRowCount;
            }
            collectCursor(false);
            while (frameIndex < frameLimit) {
                fetchNextFrame();
                if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                    counter.add(frameRowCount - frameRowIndex);
                    frameRowIndex = frameRowCount;
                    collectCursor(false);
                }
                if (!allFramesActive) {
                    throw buildInterruptionException();
                }
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
        } finally {
            atom.setSkipJoin(oldSkipJoin);
        }
    }

    private void calculateSizeNoFilter(RecordCursor.Counter counter) {
        if (frameLimit == -1) {
            masterFrameSequence.prepareForDispatch();
            frameLimit = masterFrameSequence.getFrameCount() - 1;
            for (int i = 0, n = masterFrameSequence.getFrameCount(); i < n; i++) {
                counter.add(masterFrameSequence.getFrameRowCount(i));
            }
        } else {
            if (frameRowIndex < frameRowCount) {
                counter.add(frameRowCount - frameRowIndex);
                frameRowIndex = frameRowCount;
            }
            for (int i = frameIndex + 1, n = masterFrameSequence.getFrameCount(); i < n; i++) {
                counter.add(masterFrameSequence.getFrameRowCount(i));
            }
            collectCursor(true);
            masterFrameSequence.await();
        }
        frameIndex = frameLimit;
        frameRowIndex = frameRowCount;
    }

    private void collectCursor(boolean forceCollect) {
        if (cursor > -1) {
            masterFrameSequence.collect(cursor, forceCollect);
            cursor = -1;
            masterRecord.clear();
        }
    }

    private void fetchNextFrame() {
        if (frameLimit == -1) {
            masterFrameSequence.prepareForDispatch();
            frameLimit = masterFrameSequence.getFrameCount() - 1;
        }
        try {
            // frames may all be reduced already: moving to the next one still observes a cancelled query
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
            do {
                cursor = masterFrameSequence.next();
                if (cursor > -1) {
                    final PageFrameReduceTask task = masterFrameSequence.getTask(cursor);
                    if (task.hasError()) {
                        throw task.buildError();
                    }
                    allFramesActive &= masterFrameSequence.isActive();
                    filteredRows = task.getFilteredRows();
                    frameRowCount = task.getFilteredRowCount();
                    frameIndex = task.getFrameIndex();
                    frameRowIndex = 0;
                    if (frameRowCount > 0 && masterFrameSequence.isActive()) {
                        masterRecord.init(task.getFrameMemory());
                        break;
                    } else {
                        frameRowCount = 0;
                        collectCursor(false);
                    }
                } else if (cursor == -2) {
                    break; // no frames
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable th) {
            if (th instanceof CairoException ce) {
                if (ce.isInterruption() || ce.isCancellation()) {
                    LOG.error().$("asof join error [ex=").$safe(ce.getFlyweightMessage()).I$();
                    throw buildInterruptionException();
                } else {
                    LOG.error().$("asof join error [ex=").$(th).I$();
                    throw ce;
                }
            }
            LOG.error().$("asof join error [ex=").$(th).I$();
            if (th instanceof ImplicitCastException || th instanceof NumericException) {
                throw (RuntimeException) th;
            }
            throw CairoException.nonCritical().put(th.getMessage());
        }
    }

    // master row of the frame at output position p, and its slave row
    private void positionAt(long p) {
        masterRecord.setRowIndex(isMasterFiltered ? filteredRows.get(p) : p);
        final long slaveRowId = Unsafe.getLong(slaveRowIdsAddress() + (p << 3));
        if (slaveRowId >= 0) {
            slaveRowCursor.recordAt(slaveRecord, slaveRowId);
            record.hasSlave(true);
        } else {
            record.hasSlave(false);
        }
    }

    private long slaveRowIdsAddress() {
        return filteredRows.getAddress() + (isMasterFiltered ? frameRowCount << 3 : 0);
    }

    void of(
            PageFrameSequence<AsyncAsOfJoinAtom> masterFrameSequence,
            int slaveOrder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final AsyncAsOfJoinAtom atom = masterFrameSequence.getAtom();
        this.masterFrameSequence = masterFrameSequence;
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.slaveFrameCursor = (TablePageFrameCursor) slaveFactory.getPageFrameCursor(executionContext, slaveOrder);
        this.executionContext = executionContext;
        allFramesActive = true;
        isSlaveTimeFrameCacheBuilt = false;
        frameIndex = -1;
        frameLimit = -1;
        frameRowIndex = -1;
        frameRowCount = -1;
        masterRecord.of(masterFrameSequence.getSymbolTableSource());
    }

    /**
     * The current frame's rows from {@code frameRowIndex}: master columns at the frame memory's
     * addresses (gathered by the master filter's row list when filtered), slave columns from the
     * values the reduce gathered, one 8-byte slot per row.
     */
    private class JoinBlock implements RecordBlock {
        private int rowCount;

        @Override
        public long getColumnAddress(int columnIndex) {
            if (columnIndex < columnSplit) {
                final int type = masterRecordColumnType(columnIndex);
                if (ColumnType.isVarSize(type)) {
                    return 0;
                }
                final long address = masterRecord.getPageAddress(columnIndex);
                if (address == 0 || isMasterFiltered) {
                    return address;
                }
                return address + frameRowIndex * ColumnType.sizeOf(type);
            }
            final int position = masterFrameSequence.getAtom().getGatherPosition(columnIndex - columnSplit);
            if (position < 0) {
                return 0;
            }
            return slaveRowIdsAddress() + ((frameRowCount * (1 + position) + frameRowIndex) << 3);
        }

        @Override
        public long getColumnRowIndexesAddress(int columnIndex) {
            if (columnIndex < columnSplit) {
                return getRowIndexesAddress();
            }
            return 0;
        }

        @Override
        public long getColumnStride(int columnIndex) {
            if (columnIndex < columnSplit) {
                return ColumnType.sizeOf(masterRecordColumnType(columnIndex));
            }
            return Long.BYTES;
        }

        @Override
        public Record getRecordAt(int row) {
            positionAt(frameRowIndex + row);
            return record;
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public long getRowIndexesAddress() {
            return isMasterFiltered ? filteredRows.getAddress() + (frameRowIndex << 3) : 0;
        }

        void of(int rowCount) {
            this.rowCount = rowCount;
        }
    }

    private int masterRecordColumnType(int columnIndex) {
        return masterMetadata.getColumnType(columnIndex);
    }
}
