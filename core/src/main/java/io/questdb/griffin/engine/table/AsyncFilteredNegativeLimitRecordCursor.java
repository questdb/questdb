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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordFanOut;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.AdaptiveDispatchLimit;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * With a fan-out, the LIMIT counts the rows that the factories above return for the rows of the scan, see
 * RecordFanOut: the cursor skips the rows that return no row above, and stops once the rows it keeps return
 * the LIMIT rows above. The Limit above then drops the surplus of the earliest row it keeps. Rows that return
 * no row above, such as the empty arrays of UNNEST, can make it scan far, so it sizes the tasks in flight to
 * the rows above that the collected frames return, as the forward cursor does, see AdaptiveDispatchLimit.
 */
class AsyncFilteredNegativeLimitRecordCursor implements AsyncFilteredRecordCursorFactory.RecordFreer, RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncFilteredNegativeLimitRecordCursor.class);
    private final int dispatchLimit;
    // the tasks in flight of a scan with a fan-out, counted in the rows above the scan
    private final AdaptiveDispatchLimit fanOutDispatchLimit;
    // Used for random access: we may have to deserialize Parquet page frame.
    private final PageFrameMemoryPool frameMemoryPool;
    private final boolean hasDescendingOrder;
    private final PageFrameMemoryRecord record;
    // Counts the rows above the scan for the rows that the cursor keeps, or null when the LIMIT counts the
    // rows of the scan.
    private RecordFanOut fanOut;
    // The rows above the scan for the rows that the cursor keeps, see fanOut.
    private long fanOutRowCount;
    private int frameIndex;
    private int frameLimit;
    private PageFrameSequence<?> frameSequence;
    private boolean isOpen;
    private PageFrameMemoryRecord recordB;
    private long rowCount;
    private long rowIndex;
    // Artificial limit on remaining rows to be returned from this cursor.
    // It is typically copied from LIMIT clause on SQL statement.
    private long rowLimit;
    // Buffer used to accumulate all filtered row ids.
    private DirectLongList rows;

    public AsyncFilteredNegativeLimitRecordCursor(@NotNull CairoConfiguration configuration, int scanDirection) {
        // close() only frees these once isOpen (set in of()), so a ctor failure must free the
        // already-allocated natives here; build them into locals so the catch can release them.
        PageFrameMemoryRecord record = null;
        PageFrameMemoryPool frameMemoryPool = null;
        try {
            record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            frameMemoryPool = new PageFrameMemoryPool(configuration);
        } catch (Throwable th) {
            Misc.free(record);
            Misc.free(frameMemoryPool);
            throw th;
        }
        this.record = record;
        this.hasDescendingOrder = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
        this.frameMemoryPool = frameMemoryPool;
        this.dispatchLimit = configuration.getSqlParallelFilterDispatchLimit();
        this.fanOutDispatchLimit = new AdaptiveDispatchLimit(dispatchLimit);
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    LOG.debug()
                            .$("closing [shard=").$(frameSequence.getShard())
                            .$(", frameCount=").$(frameLimit)
                            .I$();

                    if (frameLimit > -1) {
                        frameSequence.await();
                    }
                    frameSequence.reset();
                }
            } finally {
                Misc.free(frameMemoryPool);
                isOpen = false;
            }
        }
    }

    @Override
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
        // check for the first hasNext call
        if (frameIndex == -1) {
            fetchAllFrames();
        }
        if (rowIndex < rows.getCapacity()) {
            long rowId = rows.get(rowIndex);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId));
            record.init(frameMemory);
            record.setRowIndex(Rows.toLocalRowID(rowId));
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
    public long preComputedStateSize() {
        return frameIndex;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        frameMemoryPool.setParquetDecodeHint(hint);
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

    private CairoException buildInterruptionException() {
        return frameSequence.buildInterruptionException();
    }

    private void collectFanOutRow(long frameRowIndex) {
        record.setRowIndex(frameRowIndex);
        final long rowFanOut = fanOut.getFanOut(record);
        if (rowFanOut > 0) {
            rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRowIndex));
            rowCount++;
            fanOutRowCount += rowFanOut;
        }
    }

    // Keeps the rows of the frame, from its last row in the order of the scan, that return rows above the scan,
    // until they return the LIMIT rows. Each row it keeps returns at least one row above, so it keeps at most
    // rowLimit rows, which the buffer holds. It reads the rows from the frame memory of the task, where the
    // worker decoded every column of the filtered rows, so it decodes no Parquet row group of its own.
    private void collectFanOutRows(PageFrameMemory frameMemory, DirectLongList frameRows, long frameRowCount) {
        record.init(frameMemory);
        if (hasDescendingOrder) {
            for (long i = 0; i < frameRowCount && fanOutRowCount < rowLimit; i++) {
                collectFanOutRow(frameRows.get(i));
            }
        } else {
            for (long i = frameRowCount - 1; i > -1 && fanOutRowCount < rowLimit; i--) {
                collectFanOutRow(frameRows.get(i));
            }
        }
    }

    private void fetchAllFrames() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next(fanOut != null ? fanOutDispatchLimit.get() : dispatchLimit, false);
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    // the frames that the cursor drains once it reached the LIMIT are not part of the read
                    final boolean isReading = !isRowLimitReached();
                    final long fanOutRowCountBefore = fanOutRowCount;
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();

                    if (task.hasError()) {
                        throw task.buildError();
                    }

                    // Consider frame sequence status only if we haven't accumulated enough rows.
                    allFramesActive &= frameSequence.isActive() || isRowLimitReached();
                    final DirectLongList frameRows = task.getFilteredRows();
                    final long frameRowCount = frameRows.size();
                    frameIndex = task.getFrameIndex();

                    if (frameRowCount > 0 && rowCount < rowLimit + 1 && frameSequence.isActive()) {
                        // Copy rows into the buffer.
                        if (fanOut != null) {
                            collectFanOutRows(task.getFrameMemory(), frameRows, frameRowCount);
                        } else if (hasDescendingOrder) {
                            for (long i = 0; i < frameRowCount && rowCount < rowLimit; i++, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        } else {
                            for (long i = frameRowCount - 1; i > -1 && rowCount < rowLimit; i--, rowCount++) {
                                rows.set(--rowIndex, Rows.toRowID(frameIndex, frameRows.get(i)));
                            }
                        }

                        if (isRowLimitReached()) {
                            frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                        }
                    }
                    if (fanOut != null && isReading) {
                        fanOutDispatchLimit.onFrameCollected(fanOutRowCount - fanOutRowCountBefore);
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
            if (e instanceof CairoException ce) {
                if (ce.isInterruption()) {
                    throw buildInterruptionException();
                } else {
                    throw ce;
                }
            }
            // Preserve typed user-facing errors (ImplicitCastException / NumericException)
            // raised via task.buildError() so the caller can recognise them.
            if (e instanceof ImplicitCastException || e instanceof NumericException) {
                throw (RuntimeException) e;
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }

        if (!allFramesActive) {
            throw buildInterruptionException();
        }
    }

    private boolean isRowLimitReached() {
        return (fanOut != null ? fanOutRowCount : rowCount) >= rowLimit;
    }

    // the tasks in flight that the cursor asks for when it fetches its next frame
    int getDispatchLimit() {
        return fanOut != null ? fanOutDispatchLimit.get() : dispatchLimit;
    }

    void of(
            PageFrameSequence<?> frameSequence,
            long rowLimit,
            DirectLongList negativeLimitRows,
            @Nullable RecordFanOut fanOut
    ) {
        this.isOpen = true;
        this.frameSequence = frameSequence;
        this.frameIndex = -1;
        this.frameLimit = -1;
        this.rowLimit = rowLimit;
        this.rows = negativeLimitRows;
        this.rowIndex = negativeLimitRows.getCapacity();
        this.rowCount = 0;
        this.fanOut = fanOut;
        this.fanOutRowCount = 0;
        fanOutDispatchLimit.of(rowLimit);
        frameMemoryPool.setMemoryTracker(frameSequence.getMemoryTracker());
        ((AsyncFilterAtom) frameSequence.getAtom()).setParentUsedColumns(null);
        frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        record.of(frameSequence.getSymbolTableSource());
        if (recordB != null) {
            recordB.of(frameSequence.getSymbolTableSource());
        }
    }
}
