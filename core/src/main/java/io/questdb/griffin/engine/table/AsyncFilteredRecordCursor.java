/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rows;

class AsyncFilteredRecordCursor implements RecordCursor {

    private static final Log LOG = LogFactory.getLog(AsyncFilteredRecordCursor.class);
    private static final String exceptionMessage = "timeout, query aborted";
    private final Function filter;
    private final boolean hasDescendingOrder;
    private final PageAddressCacheRecord record;
    private PageAddressCacheRecord recordB;
    private DirectLongList rows;
    private long cursor = -1;
    private long frameRowIndex;
    private long frameRowCount;
    private int frameIndex;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement
    private long rowsRemaining;
    // the OG rows remaining, used to reset the counter when re-running cursor from top();
    private long ogRowsRemaining;
    private boolean allFramesActive;
    private boolean isOpen;

    public AsyncFilteredRecordCursor(Function filter, boolean hasDescendingOrder) {
        this.filter = filter;
        this.hasDescendingOrder = hasDescendingOrder;
        this.record = new PageAddressCacheRecord();
    }

    @Override
    public void close() {
        if (isOpen) {
            LOG.debug()
                    .$("closing [shard=").$(frameSequence.getShard())
                    .$(", frameIndex=").$(frameIndex)
                    .$(", frameCount=").$(frameLimit)
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
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        // check for the first hasNext call
        if (frameIndex == -1 && frameLimit > -1) {
            fetchNextFrame();
        }

        // we have rows in the current frame we still need to dispatch
        if (frameRowIndex < frameRowCount) {
            record.setRowIndex(rows.get(rowIndex()));
            frameRowIndex++;
            return checkLimit();
        }

        // Release previous queue item.
        // There is no identity check here because this check
        // had been done when 'cursor' was assigned
        collectCursor(false);

        // do we have more frames?
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (frameRowCount > 0 && frameRowIndex < frameRowCount) {
                record.setRowIndex(rows.get(rowIndex()));
                frameRowIndex++;
                return checkLimit();
            }
        }

        if (!allFramesActive) {
            throw CairoException.nonCritical().put(exceptionMessage).setInterruption(true);
        }
        return false;
    }

    private long rowIndex() {
        return hasDescendingOrder ? (frameRowCount - frameRowIndex - 1) : frameRowIndex;
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
    public void recordAt(Record record, long atRowId) {
        ((PageAddressCacheRecord) record).setFrameIndex(Rows.toPartitionIndex(atRowId));
        ((PageAddressCacheRecord) record).setRowIndex(Rows.toLocalRowID(atRowId));
    }

    @Override
    public void toTop() {
        // check if we at the top already and there is nothing to do
        if (frameIndex == 0 && frameRowIndex == 0) {
            return;
        }
        filter.toTop();
        frameSequence.toTop();
        rowsRemaining = ogRowsRemaining;
        if (frameLimit > -1) {
            frameIndex = -1;
        }
        allFramesActive = true;
    }

    @Override
    public long size() {
        return -1;
    }

    private boolean checkLimit() {
        if (--rowsRemaining < 0) {
            frameSequence.cancel();
            return false;
        }
        return true;
    }

    private void fetchNextFrame() {
        try {
            do {
                this.cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    this.allFramesActive &= frameSequence.isActive();
                    this.rows = task.getRows();
                    this.frameRowCount = rows.size();
                    this.frameIndex = task.getFrameIndex();
                    this.frameRowIndex = 0;
                    if (this.frameRowCount > 0 && frameSequence.isActive()) {
                        record.setFrameIndex(task.getFrameIndex());
                        break;
                    } else {
                        this.frameRowCount = 0; // force reset frame size if frameSequence was canceled or failed
                        collectCursor(false);
                    }
                } else {
                    Os.pause();
                }
            } while (this.frameIndex < frameLimit);
        } catch (Throwable e) {
            LOG.critical().$("unexpected error [ex=").$(e).I$();
            throw CairoException.nonCritical().put(exceptionMessage).setInterruption(true);
        }
    }

    void of(PageFrameSequence<?> frameSequence, long rowsRemaining) throws SqlException {
        this.isOpen = true;
        this.frameSequence = frameSequence;
        this.frameIndex = -1;
        this.frameLimit = frameSequence.getFrameCount() - 1;
        this.ogRowsRemaining = rowsRemaining;
        this.rowsRemaining = rowsRemaining;
        this.allFramesActive = true;
        record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        }
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
}
