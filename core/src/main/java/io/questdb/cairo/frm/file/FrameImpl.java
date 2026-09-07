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

package io.questdb.cairo.frm.file;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.std.IntList;
import io.questdb.cairo.frm.ColumnTopSink;
import io.questdb.cairo.frm.DeletedFrameColumn;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.frm.FrameColumnTypePool;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.setSinkForNativePartition;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_CONTIGUOUS_FILE;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_MEMORY;

public class FrameImpl implements Frame {
    private static final int MAX_OPEN_COLUMNS = 64;
    // A task slot an operation has no use for, matching TableWriter#IGNORE.
    private static final long IGNORE = -1L;
    private static final Log LOG = LogFactory.getLog(FrameImpl.class);
    private final FrameColumnPool columnPool;
    // Pre-bound so publishing a task allocates nothing.
    private final TableWriter.ColumnTaskHandler cthAppendColumnRef = this::cthAppendColumn;
    private final TableWriter.ColumnTaskHandler cthMergeColumnRef = this::cthMergeColumn;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger errorCount = new AtomicInteger();
    private boolean canWrite = false;
    private ReadOnlyObjList<? extends MemoryCR> columnsMemory;
    private ColumnTopSink columnTopSink;
    private final LongList columnTops = new LongList();
    private final ObjList<FrameColumn> source1Columns = new ObjList<>();
    private final ObjList<FrameColumn> source2Columns = new ObjList<>();
    private final ObjList<FrameColumn> targetColumns = new ObjList<>();
    // The rest of one operation: the values every column of it shares, which is why they are here rather
    // than in each task. Everything that varies per column travels in the task's own slots.
    private int commitMode;
    private long mergeIndexRows;
    private long upcomingTableTxn;
    private boolean create = false;
    private volatile Throwable error;
    // When set, a COVERING posting-indexed column is opened as a plain column, so the frame writes its data but adds no
    // index entries.
    private boolean deferCoveredIndexing = false;
    private ColumnVersionReader crv;
    private RecycleBin<FrameImpl> frameRecycleBin;
    private int frameType;
    private final MessageBus messageBus;
    private RecordMetadata metadata;
    private long offset = 0;
    private Path partitionPath = new Path();
    private long partitionTimestamp;
    private long rowCount;
    private long timestampIndexAddr;

    public FrameImpl(FrameColumnPool columnPool, @Nullable MessageBus messageBus) {
        this.columnPool = columnPool;
        this.messageBus = messageBus;
    }

    @Override
    public void appendColumns(Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode) {
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        execute(source, null, cthAppendColumnRef, sourceLo, sourceHi, IGNORE, IGNORE, IGNORE);
    }

    @Override
    public void close() {
        this.columnsMemory = null;
        this.columnTopSink = null;
        this.crv = null;
        // Scoped to the open that set it: this frame goes back to the recycle bin below, and the next
        // open gets it as a frame that indexes every column, whatever the previous one asked for.
        this.deferCoveredIndexing = false;
        if (frameRecycleBin != null && !frameRecycleBin.isClosed()) {
            frameRecycleBin.put(this);
        } else {
            free();
        }
    }

    @Override
    public int columnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public void commitColumnTops() {
        if (columnTopSink != null) {
            columnTopSink.commitColumnTops();
        }
    }

    @Override
    public void setDeferCoveredIndexing(boolean deferCoveredIndexing) {
        this.deferCoveredIndexing = deferCoveredIndexing;
    }

    @Override
    public FrameColumn createColumn(int columnIndex) {
        if (frameType == COLUMN_CONTIGUOUS_FILE) {
            return getContiguousFileFrameColumn(columnIndex);
        } else if (frameType == COLUMN_MEMORY) {
            return getMemoryFrameColumn(columnIndex);
        } else {
            throw CairoException.critical(0)
                    .put("unknown frame type [type=").put(frameType)
                    .put(", partitionPath=").put(partitionPath).put(']');
        }
    }

    public void createROFromMemoryColumns(ReadOnlyObjList<? extends MemoryCR> columns, TableWriterMetadata metadata, long size) {
        createROFromMemoryColumns(columns, metadata, size, 0);
    }

    /**
     * @param timestampIndexAddr the sorted timestamp index backing the designated timestamp column, or 0
     *                           when the frame's timestamp column carries its own timestamps
     */
    public void createROFromMemoryColumns(ReadOnlyObjList<? extends MemoryCR> columns, TableWriterMetadata metadata, long size, long timestampIndexAddr) {
        this.timestampIndexAddr = timestampIndexAddr;
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = null;
        this.rowCount = size;
        this.partitionTimestamp = Long.MIN_VALUE;
        this.partitionPath.of(partitionPath);
        this.canWrite = false;
        this.create = false;
        this.frameType = COLUMN_MEMORY;
        assert columns.size() == metadata.getColumnCount() * 2;
        this.columnsMemory = columns;
    }

    public void createRW(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionWriter cvw, long size) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvw;
        this.columnTopSink = null;
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = true;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    /**
     * Same as {@link #createRW(Path, long, RecordMetadata, ColumnVersionWriter, long)}, but column-top
     * updates go to {@code columnTopSink} instead of a {@code ColumnVersionWriter} - see {@link ColumnTopSink}.
     */
    public void createRW(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionReader cvr, ColumnTopSink columnTopSink, long size) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.columnTopSink = columnTopSink;
        columnTopSink.ofColumnCount(metadata.getColumnCount());
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = true;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public void mergeColumns(
            Frame source1,
            long source1Lo,
            long source1Hi,
            Frame source2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            long upcomingTableTxn,
            int commitMode
    ) {
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        // Five task slots against a merge's six bounds, so the row count travels as a field.
        this.mergeIndexRows = mergeIndexRows;
        execute(source1, source2, cthMergeColumnRef, source1Lo, source1Hi, source2Lo, source2Hi, mergeIndexAddr);
    }

    public void openRO(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionReader cvr, long partitionRowCount) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.rowCount = partitionRowCount;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = false;
        this.create = false;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    public void openRO(
            @Transient Path tablePath,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy,
            RecordMetadata metadata,
            ColumnVersionReader cvr,
            long partitionRowCount
    ) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.rowCount = partitionRowCount;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(tablePath);
        setSinkForNativePartition(
                this.partitionPath.slash(),
                metadata.getTimestampType(),
                partitionBy,
                partitionTimestamp,
                partitionNameTxn
        );
        this.canWrite = false;
        this.create = false;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    public void openRW(@Transient Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionWriter cvw, long size) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvw;
        this.columnTopSink = null;
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = false;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    /**
     * Opens a writable frame whose column-top updates go to {@code columnTopSink} instead of a
     * {@code ColumnVersionWriter} - see {@link ColumnTopSink}.
     */
    public void openRW(@Transient Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionReader cvr, ColumnTopSink columnTopSink, long size) {
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.columnTopSink = columnTopSink;
        columnTopSink.ofColumnCount(metadata.getColumnCount());
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = false;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    @Override
    public void publishColumnTops(ColumnTopSink sink) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            long colTop = columnTops.getQuick(i);
            // -1 (untouched, this frame has no sink and nothing wrote through it): nothing to record.
            if (colTop > -1) {
                sink.setColumnTop(i, colTop);
            }
        }
    }

    public void saveChanges(FrameColumn frameColumn) {
        if (!canWrite) {
            throw CairoException.critical(0).put("cannot save column top, partition frame is read-only [path=").put(partitionPath).put(']');
        }
        // Tracked internally whether or not there is an external sink: createColumn reads this list back for the NEXT
        // piece written to this frame, and only a tracked value stops it re-resolving the source directory's own.
        final int columnIndex = frameColumn.getColumnIndex();
        final long columnTop = Math.max(frameColumn.getColumnTop(), columnTops.getQuick(columnIndex));
        columnTops.setQuick(columnIndex, columnTop);
        if (columnTopSink != null) {
            columnTopSink.setColumnTop(columnIndex, columnTop);
        }
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    private void closeColumns(int columnLo, int columnHi) {
        for (int i = columnLo; i < columnHi; i++) {
            targetColumns.setQuick(i, Misc.free(targetColumns.getQuick(i)));
            source1Columns.setQuick(i, Misc.free(source1Columns.getQuick(i)));
            source2Columns.setQuick(i, Misc.free(source2Columns.getQuick(i)));
        }
    }

    /**
     * One column's share of {@link #appendColumns}.
     */
    private void cthAppendColumn(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long sourceLo,
            long sourceHi,
            long ignore2,
            long ignore3,
            long ignore4
    ) {
        if (errorCount.get() > 0) {
            // Another column already failed and the operation is going to be abandoned, so there is no
            // point writing more bytes into a partition nobody will publish.
            return;
        }
        try {
            final FrameColumn targetColumn = targetColumns.getQuick(columnIndex);
            targetColumn.setUpcomingTableTxn(upcomingTableTxn);
            // rowCount is this frame's own tail.
            FrameAlgebra.appendColumn(targetColumn, rowCount, source1Columns.getQuick(columnIndex), sourceLo, sourceHi, commitMode);
        } catch (Throwable th) {
            onError(columnIndex, th);
        }
    }

    /**
     * One column's share of {@link #mergeColumns}.
     */
    private void cthMergeColumn(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long source1Lo,
            long source1Hi,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr
    ) {
        if (errorCount.get() > 0) {
            return;
        }
        try {
            final FrameColumn targetColumn = targetColumns.getQuick(columnIndex);
            targetColumn.setUpcomingTableTxn(upcomingTableTxn);
            targetColumn.merge(
                    rowCount,
                    source1Columns.getQuick(columnIndex),
                    source1Lo,
                    source1Hi,
                    source2Columns.getQuick(columnIndex),
                    source2Lo,
                    source2Hi,
                    mergeIndexAddr,
                    mergeIndexRows,
                    commitMode
            );
        } catch (Throwable th) {
            onError(columnIndex, th);
        }
    }

    private void dispatchColumns(
            TableWriter.ColumnTaskHandler taskHandler,
            boolean isParallel,
            int columnLo,
            int columnHi,
            long long0,
            long long1,
            long long2,
            long long3,
            long long4
    ) {
        // Read here rather than in the tasks: metadata is this frame's shared, non-thread-safe state.
        final long timestampColumnIndex = metadata.getTimestampIndex();
        if (!isParallel) {
            for (int i = columnLo; i < columnHi; i++) {
                if (isLiveColumn(i)) {
                    taskHandler.run(i, source1Columns.getQuick(i).getColumnType(), timestampColumnIndex, long0, long1, long2, long3, long4);
                }
            }
            return;
        }

        final Sequence pubSeq = messageBus.getColumnTaskPubSeq();
        final RingQueue<ColumnTask> queue = messageBus.getColumnTaskQueue();
        doneLatch.reset();
        int queuedCount = 0;
        for (int i = columnLo; i < columnHi; i++) {
            if (!isLiveColumn(i)) {
                continue;
            }
            final long cursor = pubSeq.next();
            if (cursor > -1) {
                try {
                    // Only the column index and the bounds travel in the task: the open columns and the
                    // rest of the operation are this frame's own fields, and this frame owns the handler.
                    queue.get(cursor).of(
                            doneLatch,
                            i,
                            source1Columns.getQuick(i).getColumnType(),
                            timestampColumnIndex,
                            long0,
                            long1,
                            long2,
                            long3,
                            long4,
                            taskHandler
                    );
                } finally {
                    queuedCount++;
                    pubSeq.done(cursor);
                }
            } else {
                // Queue full. Run the column here rather than wait for room, the same way
                // TableWriter#dispatchColumnTasks does - and this is also what makes progress
                // guaranteed when nothing else is draining the queue.
                taskHandler.run(i, source1Columns.getQuick(i).getColumnType(), timestampColumnIndex, long0, long1, long2, long3, long4);
            }
        }
        // Work stealing: the calling thread runs whatever it can reach, including tasks other writers
        // published, until every task of THIS operation has counted down.
        TableWriter.consumeColumnTasks0(queue, queuedCount, messageBus.getColumnTaskSubSeq(), doneLatch);
    }

    private void execute(
            Frame source1,
            @Nullable Frame source2,
            TableWriter.ColumnTaskHandler taskHandler,
            long long0,
            long long1,
            long long2,
            long long3,
            long long4
    ) {
        final int columnCount = source1.columnCount();
        // A frame with a single column has nothing to spread, and without a bus there is nowhere to spread it to.
        final boolean isParallel = messageBus != null && columnCount > 1;
        errorCount.set(0);
        error = null;
        targetColumns.setAll(columnCount, null);
        source1Columns.setAll(columnCount, null);
        source2Columns.setAll(columnCount, null);
        try {
            final int batchSize = isParallel ? MAX_OPEN_COLUMNS : 1;
            for (int columnLo = 0; columnLo < columnCount; columnLo += batchSize) {
                final int columnHi = Math.min(columnLo + batchSize, columnCount);
                try {
                    openColumns(source1, source2, columnLo, columnHi);
                    dispatchColumns(taskHandler, isParallel, columnLo, columnHi, long0, long1, long2, long3, long4);
                    throwOnError();
                    for (int i = columnLo; i < columnHi; i++) {
                        if (isLiveColumn(i)) {
                            saveChanges(targetColumns.getQuick(i));
                        }
                    }
                } finally {
                    closeColumns(columnLo, columnHi);
                }
            }
        } finally {
            // Nothing is open by now - every batch closed its own - so this only drops the references.
            targetColumns.clear();
            source1Columns.clear();
            source2Columns.clear();
        }
    }

    private void free() {
        partitionPath = Misc.free(partitionPath);
    }

    private FrameColumn getContiguousFileFrameColumn(int columnIndex) {
        int columnType = metadata.getColumnType(columnIndex);
        if (columnType < 0) {
            return DeletedFrameColumn.INSTANCE;
        }
        boolean isIndexed = metadata.isColumnIndexed(columnIndex);
        int indexBlockCapacity = isIndexed ? metadata.getIndexValueBlockCapacity(columnIndex) : 0;
        byte indexType = metadata.getColumnIndexType(columnIndex);
        if (deferCoveredIndexing && isIndexed && IndexType.isPosting(indexType) && metadata instanceof TableWriterMetadata) {
            IntList coveringCols = ((TableWriterMetadata) metadata).getColumnMetadata(columnIndex).getCoveringColumnIndices();
            if (coveringCols != null && coveringCols.size() > 0) {
                // A zero capacity is what the column pool reads as "not indexed", so it hands back the
                // plain column and no index writer is opened at all.
                indexBlockCapacity = 0;
            }
        }
        // A tracked top (only ever set by this frame's own saveChanges, when it has no external sink) takes over from
        // crv entirely once present: it already reflects everything crv would resolve to PLUS every piece this frame.
        long columnTop = columnTops.getQuick(columnIndex);
        if (columnTop < 0) {
            int crvRecIndex = crv.getRecordIndex(partitionTimestamp, columnIndex);
            columnTop = crv.getColumnTopByIndexOrDefault(crvRecIndex, partitionTimestamp, columnIndex, rowCount);
        }
        long columnTxn = crv.getColumnNameTxn(partitionTimestamp, columnIndex);

        FrameColumnTypePool columnTypePool = columnPool.getPool(columnType);
        boolean createNew = columnTop >= rowCount || create;
        columnTop = Math.min(columnTop, rowCount);
        return columnTypePool.create(
                partitionPath,
                metadata.getColumnName(columnIndex),
                columnTxn,
                columnType,
                indexBlockCapacity,
                indexType,
                columnTop,
                columnIndex,
                createNew,
                canWrite
        );
    }

    private FrameColumn getMemoryFrameColumn(int columnIndex) {
        int columnType = metadata.getColumnType(columnIndex);
        if (columnType < 0) {
            return DeletedFrameColumn.INSTANCE;
        }
        FrameColumnTypePool columnTypePool = columnPool.getPool(columnType);
        FrameColumn column = columnTypePool.createFromMemoryColumn(
                columnIndex,
                columnType,
                rowCount,
                columnsMemory.get(TableWriter.getPrimaryColumnIndex(columnIndex)),
                columnsMemory.get(TableWriter.getSecondaryColumnIndex(columnIndex))
        );
        if (timestampIndexAddr != 0
                && columnIndex == metadata.getTimestampIndex()
                && column instanceof MemoryFixFrameColumn fixColumn) {
            fixColumn.ofTimestampIndex(timestampIndexAddr);
        }
        return column;
    }

    private boolean isLiveColumn(int columnIndex) {
        return source1Columns.getQuick(columnIndex).getColumnType() >= 0;
    }

    private void onError(int columnIndex, Throwable th) {
        LOG.error().$("frame column task failed [columnIndex=").$(columnIndex)
                .$(", error=").$(th)
                .I$();
        if (errorCount.getAndIncrement() == 0) {
            error = th;
        }
    }

    /**
     * Opens one batch of columns up front, on the calling thread - see {@link #execute} for why this cannot overlap
     * with the copy.
     */
    private void openColumns(Frame source1, @Nullable Frame source2, int columnLo, int columnHi) {
        for (int i = columnLo; i < columnHi; i++) {
            source1Columns.setQuick(i, source1.createColumn(i));
            if (!isLiveColumn(i)) {
                // A dropped column: neither the other source nor the target opens a file for it.
                continue;
            }
            if (source2 != null) {
                source2Columns.setQuick(i, source2.createColumn(i));
            }
            targetColumns.setQuick(i, createColumn(i));
        }
    }

    private void resetColumnTops(int columnCount) {
        columnTops.setPos(columnCount);
        columnTops.fill(0, columnCount, -1L);
    }

    private void throwOnError() {
        final Throwable th = error;
        if (th != null) {
            error = null;
            // Rethrown as it was raised, so a caller that already distinguishes a CairoException from a
            // CairoError - o3 failure handling does - keeps seeing what one column threw.
            if (th instanceof RuntimeException re) {
                throw re;
            }
            if (th instanceof Error err) {
                throw err;
            }
            throw CairoException.critical(0).put("frame column task failed [error=").put(th.getMessage()).put(']');
        }
    }

    void setRecycleBin(RecycleBin<FrameImpl> frameRecycleBin) {
        this.frameRecycleBin = frameRecycleBin;
    }
}
