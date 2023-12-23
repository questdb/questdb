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

/**
 * Used to handle the LIMIT -N clause with the descending timestamp order case. To do so, this cursor
 * accumulates the row ids in a buffer and only then starts the iteration. That's necessary to preserve
 * the timestamp-based order in the result set. The buffer is filled in from bottom to top.
 * <p>
 * Here is an illustration of the described problem:
 * <pre>
 * row iteration order    frames                      frame iteration order
 *         |            [ row 0   <- frame 0 start           /\
 *         |              row 1                              |
 *         |              row 2 ] <- frame 0 end             |
 *         |            [ row 3   <- frame 1 start           |
 *        \/              row 4 ] <- frame 1 end             |
 * </pre>
 */
class AsyncFilteredNegativeLimitRecordCursor implements RecordCursor {

    private static final Log LOG = LogFactory.getLog(AsyncFilteredNegativeLimitRecordCursor.class);

    private final boolean hasDescendingOrder;
    private final PageAddressCacheRecord record;
    private int frameIndex;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    private PageAddressCacheRecord recordB;
    private long rowCount;
    private long rowIndex;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowLimit;
    // Buffer used to accumulate all filtered row ids.
    private DirectLongList rows;

    public AsyncFilteredNegativeLimitRecordCursor(int scanDirection) {
        this.record = new PageAddressCacheRecord();
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
    }

    @Override
    public void close() {
        LOG.debug()
                .$("closing [shard=").$(frameSequence.getShard())
                .$(", frameCount=").$(frameLimit)
                .I$();

        if (frameLimit > -1) {
            frameSequence.await();
        }
        frameSequence.clear();
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
        // check for the first hasNext call
        if (frameIndex == -1) {
            fetchAllFrames();
        }
        if (rowIndex < rows.getCapacity()) {
            long rowId = rows.get(rowIndex);
            record.setRowIndex(Rows.toLocalRowID(rowId));
            record.setFrameIndex(Rows.toPartitionIndex(rowId));
            rowIndex++;
            return true;
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
        if (frameIndex == -1) {
            return -1;
        }
        return rowCount;
    }

    @Override
    public void toTop() {
        rowIndex = rows.getCapacity() - rowCount;
    }

    private void fetchAllFrames() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (task.hasError()) {
                        throw CairoException.nonCritical().put(task.getErrorMsg());
                    }

                    // Consider frame sequence status only if we haven't accumulated enough rows.
                    allFramesActive &= frameSequence.isActive() || rowCount >= rowLimit;
                    final DirectLongList frameRows = task.getFilteredRows();
                    final long frameRowCount = frameRows.size();
                    frameIndex = task.getFrameIndex();

                    if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isActive()) {
                        // Copy rows into the buffer.
                        if (hasDescendingOrder) {
                            for (long i = 0; i < frameRowCount && rowCount < rowLimit; i++, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        } else {
                            for (long i = frameRowCount - 1; i > -1 && rowCount < rowLimit; i--, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        }

                        if (rowCount >= rowLimit) {
                            frameSequence.cancel();
                        }
                    }

                    frameSequence.collect(cursor, false);
                } else if (cursor == -2) {
                    break; // No frames to filter.
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable e) {
            LOG.error().$("negative limit filter error [ex=").$(e).I$();
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

        if (!allFramesActive) {
            throwTimeoutException();
        }
    }

    private void throwTimeoutException() {
        throw CairoException.nonCritical().put(AsyncFilteredRecordCursor.exceptionMessage).setInterruption(true);
    }

    void of(PageFrameSequence<?> frameSequence, long rowLimit, DirectLongList negativeLimitRows) {
        this.frameSequence = frameSequence;
        frameIndex = -1;
        frameLimit = -1;
        this.rowLimit = rowLimit;
        rows = negativeLimitRows;
        rowIndex = negativeLimitRows.getCapacity();
        rowCount = 0;
        record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        }
    }
}
