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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class AsyncFilteredRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncFilteredRecordCursor.class);

    private final Function filter;
    // Used for random access: we may have to deserialize Parquet page frame.
    private final PageFrameMemoryPool frameMemoryPool;
    private final boolean hasDescendingOrder;
    private final PageFrameMemoryRecord record;
    private boolean allFramesActive;
    private long cursor = -1;
    private int frameIndex;
    private int frameLimit;
    private long frameRowCount;
    private long frameRowIndex;
    private PageFrameSequence<?> frameSequence;
    private boolean isOpen;
    // The OG rows remaining, used to reset the counter when re-running cursor from top().
    private long ogRowsRemaining;
    private PageFrameMemoryRecord recordB;
    private DirectLongList rows;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowsRemaining;

    public AsyncFilteredRecordCursor(@NotNull CairoConfiguration configuration, Function filter, int scanDirection) {
        this.filter = filter;
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
        record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        if (frameIndex == -1) {
            fetchNextFrame();
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }

        if (rowsRemaining < 1) {
            return;
        }

        // We have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            long frameRowsLeft = Math.min(frameRowCount - frameRowIndex, rowsRemaining);
            rowsRemaining -= frameRowsLeft;
            frameRowIndex += frameRowsLeft;
            counter.add(frameRowsLeft);
            if (rowsRemaining < 1) {
                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                return;
            }
        }

        // Release the previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned.
        collectCursor(false);

        while (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                long frameRowsLeft = Math.min(frameRowCount - frameRowIndex, rowsRemaining);
                rowsRemaining -= frameRowsLeft;
                frameRowIndex += frameRowsLeft;
                counter.add(frameRowsLeft);
                if (rowsRemaining < 1) {
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                    return;
                } else {
                    collectCursor(false);
                }
            }

            if (!allFramesActive) {
                throwTimeoutException();
            }

            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(frameMemoryPool);

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
                    frameSequence.await();
                }
                frameSequence.clear();
            }
        }
    }

    public void freeRecords() {
        Misc.free(record);
        Misc.free(recordB);
        Misc.free(frameMemoryPool);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        if (recordB != null) {
            return recordB;
        }
        recordB = new PageFrameMemoryRecord(record, PageFrameMemoryRecord.RECORD_B_LETTER);
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        // Check for the first hasNext call.
        if (frameIndex == -1) {
            fetchNextFrame();
        }

        // Check for already reached row limit.
        if (rowsRemaining < 0) {
            return false;
        }

        // We have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            record.setRowIndex(rows.get(rowIndex()));
            frameRowIndex++;
            return checkLimit();
        }

        // Release the previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned.
        collectCursor(false);

        // Do we have more frames?
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                record.setRowIndex(rows.get(rowIndex()));
                frameRowIndex++;
                return checkLimit();
            }
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void skipRows(Counter rowCount) throws DataUnavailableException {
        if (frameIndex == -1) {
            fetchNextFrame();
        }

        long rowCountLeft = Math.min(rowsRemaining, rowCount.get());

        // We have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            long frameRowsLeft = Math.min(frameRowCount - frameRowIndex, rowCountLeft);
            rowsRemaining -= frameRowsLeft;
            frameRowIndex += frameRowsLeft;
            rowCountLeft -= frameRowsLeft;
            rowCount.dec(frameRowsLeft);
            if (rowCountLeft == 0) {
                return;
            }
        }

        // Release the previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned.
        collectCursor(false);

        while (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                long frameRowsLeft = Math.min(frameRowCount - frameRowIndex, rowCountLeft);
                rowsRemaining -= frameRowsLeft;
                frameRowIndex += frameRowsLeft;
                rowCountLeft -= frameRowsLeft;
                rowCount.dec(frameRowsLeft);
                if (rowCountLeft == 0) {
                    return;
                }
            }

            collectCursor(false);

            if (!allFramesActive) {
                throwTimeoutException();
            }
        }
    }

    @Override
    public void toTop() {
        collectCursor(false);
        filter.toTop();
        frameSequence.toTop();
        rowsRemaining = ogRowsRemaining;
        // Don't reset frameLimit here since its value is used to prepare frame sequence for dispatch only once.
        frameIndex = -1;
        frameRowIndex = -1;
        frameRowCount = -1;
        allFramesActive = true;
    }

    private boolean checkLimit() {
        if (--rowsRemaining < 0) {
            frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
            return false;
        }
        return true;
    }

    private void collectCursor(boolean forceCollect) {
        if (cursor > -1) {
            frameSequence.collect(cursor, forceCollect);
            // It is necessary to clear 'cursor' value
            // because we updated frameIndex and loop can exit due to lack of frames.
            // Non-update of 'cursor' could cause double-free.
            cursor = -1;
            // We also need to clear the record as it's initialized with the task's
            // page frame memory that is now closed.
            record.clear();
        }
    }

    private void fetchNextFrame() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        try {
            do {
                cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", frameId=").$(frameSequence.getId())
                            .$(", active=").$(frameSequence.isActive())
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

                    allFramesActive &= frameSequence.isActive();
                    rows = task.getFilteredRows();
                    frameRowCount = rows.size();
                    frameIndex = task.getFrameIndex();
                    frameRowIndex = 0;
                    if (frameRowCount > 0 && frameSequence.isActive()) {
                        record.init(task.getFrameMemory());
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

    private long rowIndex() {
        return hasDescendingOrder ? (frameRowCount - frameRowIndex - 1) : frameRowIndex;
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(PageFrameSequence<?> frameSequence, long rowsRemaining) {
        isOpen = true;
        this.frameSequence = frameSequence;
        this.rowsRemaining = rowsRemaining;
        ogRowsRemaining = rowsRemaining;
        frameIndex = -1;
        frameLimit = -1;
        frameRowIndex = -1;
        frameRowCount = -1;
        allFramesActive = true;
        frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        record.of(frameSequence.getSymbolTableSource());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource());
        }
    }
}
