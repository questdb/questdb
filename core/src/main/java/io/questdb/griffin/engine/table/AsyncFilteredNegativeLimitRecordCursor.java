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
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
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
            long cursor = frameSequence.next();
            if (cursor > -1) {
                PageFrameReduceTask task = frameSequence.getTask(cursor);
                LOG.debug()
                        .$("collected [shard=").$(frameSequence.getShard())
                        .$(", frameIndex=").$(task.getFrameIndex())
                        .$(", frameCount=").$(frameSequence.getFrameCount())
                        .$(", active=").$(frameSequence.isActive())
                        .$(", cursor=").$(cursor)
                        .I$();

                final DirectLongList frameRows = task.getRows();
                final long frameRowCount = frameRows.size();
                frameIndex = task.getFrameIndex();

                if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isActive()) {
                    // Copy rows into the buffer.
                    for (long i = frameRowCount - 1; i > -1 && rowCount < rowLimit; i--, rowCount++) {
                        rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                    }

                    if (rowCount >= rowLimit) {
                        frameSequence.cancel();
                    }
                }

                frameSequence.collect(cursor, false);
            } else {
                Os.pause();
            }
        } while (frameIndex < frameLimit);
    }

    void of(PageFrameSequence<?> frameSequence, long rowLimit, DirectLongList negativeLimitRows) throws SqlException {
        this.frameSequence = frameSequence;
        this.frameLimit = frameSequence.getFrameCount() - 1;
        this.rowLimit = rowLimit;
        this.rows = negativeLimitRows;
        this.rowIndex = negativeLimitRows.getCapacity();
        this.rowCount = 0;
        record.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource(), frameSequence.getPageAddressCache());
        }
        // when frameCount is 0 our collect sequence is not subscribed
        // we should not be attempting to fetch queue using it
        if (frameLimit > -1) {
            fetchAllFrames();
        }
    }
}
