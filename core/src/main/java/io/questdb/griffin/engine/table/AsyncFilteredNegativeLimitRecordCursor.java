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

import io.questdb.cairo.sql.PageAddressCacheRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;

/**
 * Used to handle the LIMIT -N clause with the descending timestamp order case. To do so, this cursor
 * accumulates the row ids in a buffer and only then starts the iteration. That's necessary to preserve
 * the timestamp-based order in the result set. The buffer is filled in from bottom to top.
 *
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

    private final PageAddressCacheRecord record;
    private PageAddressCacheRecord recordB;
    private SCSequence collectSubSeq;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    // Buffer used to accumulate all filtered row ids.
    private DirectLongList rows;
    private long rowIndex;
    private long rowCount;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowLimit;

    public AsyncFilteredNegativeLimitRecordCursor() {
        this.record = new PageAddressCacheRecord();
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

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
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
        rowIndex = rows.getCapacity() - rowCount;
    }

    @Override
    public long size() {
        return rowCount;
    }

    private void fetchAllFrames() {
        int frameIndex = -1;
        do {
            long cursor = collectSubSeq.next();
            if (cursor > -1) {
                PageFrameReduceTask task = reduceQueue.get(cursor);
                PageFrameSequence<?> thatFrameSequence = task.getFrameSequence();
                if (thatFrameSequence == this.frameSequence) {
                    frameIndex = task.getFrameIndex();
                    handleTask(cursor, task);
                } else {
                    // not our task, nothing to collect
                    collectSubSeq.done(cursor);
                }
            } else {
                if (frameSequence.tryDispatch()) {
                    // We have dispatched something, so let's try to collect it.
                    continue;
                }
                if (frameSequence.isNothingDispatchedSince(frameIndex)) {
                    // We haven't dispatched anything, and we have collected everything
                    // that was dispatched previously. Use local task to avoid being
                    // blocked in case of full reduce queue.
                    PageFrameReduceTask task = frameSequence.reduceLocally();
                    frameIndex = task.getFrameIndex();
                    handleTask(cursor, task);
                }
            }
        } while (frameIndex < frameLimit);
    }

    private void handleTask(long cursor, PageFrameReduceTask task) {
        LOG.debug()
                .$("collected [shard=").$(frameSequence.getShard())
                .$(", frameIndex=").$(task.getFrameIndex())
                .$(", frameCount=").$(frameSequence.getFrameCount())
                .$(", valid=").$(frameSequence.isValid())
                .$(", cursor=").$(cursor)
                .I$();

        final DirectLongList frameRows = task.getRows();
        final long frameRowCount = frameRows.size();
        final int frameIndex = task.getFrameIndex();

        if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isValid()) {
            // Copy rows into the buffer.
            for (long i = frameRowCount - 1; i > -1 && rowCount < rowLimit; i--, rowCount++) {
                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
            }
        }
        // It is necessary to clear 'cursor' value
        // because we updated frameIndex and loop can exit due to lack of frames.
        // Non-update of 'cursor' could cause double-free.
        collectCursor(cursor);
    }

    void of(SCSequence collectSubSeq, PageFrameSequence<?> frameSequence, long rowLimit, DirectLongList negativeLimitRows) throws SqlException {
        this.collectSubSeq = collectSubSeq;
        this.frameSequence = frameSequence;
        this.reduceQueue = frameSequence.getPageFrameReduceQueue();
        this.frameLimit = frameSequence.getFrameCount() - 1;
        this.rowLimit = rowLimit;
        this.rows = negativeLimitRows;
        this.rowIndex = negativeLimitRows.getCapacity();
        this.rowCount = 0;
        record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        // when frameCount is 0 our collect sequence is not subscribed
        // we should not be attempting to fetch queue using it
        if (frameLimit > -1) {
            fetchAllFrames();
        }
    }

    private void collectCursor(long cursor) {
        if (cursor > -1) {
            reduceQueue.get(cursor).collected(false);
            collectSubSeq.done(cursor);
        } else {
            frameSequence.collectLocalTask();
        }
    }
}
