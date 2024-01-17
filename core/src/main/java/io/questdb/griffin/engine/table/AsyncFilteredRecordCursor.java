/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rows;

class AsyncFilteredRecordCursor implements RecordCursor {

    static final String exceptionMessage = "timeout, query aborted";
    private static final Log LOG = LogFactory.getLog(AsyncFilteredRecordCursor.class);
    private final Function filter;
    private final boolean hasDescendingOrder;
    private final PageAddressCacheRecord record;
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
    private PageAddressCacheRecord recordB;
    private DirectLongList rows;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowsRemaining;

    public AsyncFilteredRecordCursor(Function filter, int scanDirection) {
        this.filter = filter;
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
        record = new PageAddressCacheRecord();
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
                frameSequence.cancel();
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
                    frameSequence.cancel();
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
            LOG.debug()
                    .$("closing [shard=").$(frameSequence.getShard())
                    .$(", frameIndex=").$(frameIndex)
                    .$(", frameCount=").$(frameLimit)
                    .$(", frameId=").$(frameSequence.getId())
                    .$(", cursor=").$(cursor)
                    .I$();

            if (frameSequence != null) {
                collectCursor(true);
                if (frameLimit > -1) {
                    frameSequence.await();
                }
                frameSequence.clear();
            }
            isOpen = false;
        }
    }

    public void freeRecords() {
        Misc.free(record);
        Misc.free(recordB);
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
        recordB = new PageAddressCacheRecord(record);
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
    public void recordAt(Record record, long atRowId) {
        ((PageAddressCacheRecord) record).setFrameIndex(Rows.toPartitionIndex(atRowId));
        ((PageAddressCacheRecord) record).setRowIndex(Rows.toLocalRowID(atRowId));
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
        // Check if we at the top already and there is nothing to do.
        if (frameIndex == 0 && frameRowIndex == 0) {
            return;
        }
        collectCursor(false);
        filter.toTop();
        frameSequence.toTop();
        rowsRemaining = ogRowsRemaining;
        frameIndex = -1;
        allFramesActive = true;
    }

    private boolean checkLimit() {
        if (--rowsRemaining < 0) {
            frameSequence.cancel();
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
                        throw CairoException.nonCritical().put(task.getErrorMsg());
                    }

                    allFramesActive &= frameSequence.isActive();
                    rows = task.getFilteredRows();
                    frameRowCount = rows.size();
                    frameIndex = task.getFrameIndex();
                    frameRowIndex = 0;
                    if (frameRowCount > 0 && frameSequence.isActive()) {
                        record.setFrameIndex(task.getFrameIndex());
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
        } catch (Throwable e) {
            LOG.error().$("filter error [ex=").$(e).I$();
            if (e instanceof CairoException) {
                CairoException ce = (CairoException) e;
                if (ce.isInterruption()) {
                    throwTimeoutException();
                } else {
                    throw ce;
                }
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }
    }

    private long rowIndex() {
        return hasDescendingOrder ? (frameRowCount - frameRowIndex - 1) : frameRowIndex;
    }

    private void throwTimeoutException() {
        throw CairoException.nonCritical().put(exceptionMessage).setInterruption(true);
    }

    void of(PageFrameSequence<?> frameSequence, long rowsRemaining) {
        isOpen = true;
        this.frameSequence = frameSequence;
        frameIndex = -1;
        frameLimit = -1;
        ogRowsRemaining = rowsRemaining;
        this.rowsRemaining = rowsRemaining;
        allFramesActive = true;
        record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        }
    }
}
