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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
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
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.setSinkForNativePartition;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_CONTIGUOUS_FILE;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_MEMORY;

public class FrameImpl implements Frame {
    // addDataBytes keeps two longs per column: the var-size data bytes, then the leading NULL run (see
    // extendLeadingNullRun) of the sources it has been given so far.
    private static final int DATA_BYTES_STRIDE = 2;
    private static final int MAX_OPEN_COLUMNS = 64;
    // A task slot an operation has no use for, matching TableWriter#IGNORE.
    private static final long IGNORE = -1L;
    private static final Log LOG = LogFactory.getLog(FrameImpl.class);
    private final FrameColumnPool columnPool;
    // Pre-bound so publishing a task allocates nothing.
    private final TableWriter.ColumnTaskHandler cthAppendColumnRef = this::cthAppendColumn;
    private final TableWriter.ColumnTaskHandler cthMergeColumnRef = this::cthMergeColumn;
    private final TableWriter.ColumnTaskHandler cthReserveColumnRef = this::cthReserveColumn;
    private final TableWriter.ColumnTaskHandler cthSyncColumnRef = this::cthSyncColumn;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final FilesFacade ff;
    // The columns closed since the last sync() whose files still owe a blocking flush: sync() reaches their files by
    // name. Only a frame that does not keep its columns open closes a written column before its sync().
    private final IntHashSet fsyncPendingColumns = new IntHashSet();
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
    // Set while a reserve() runs against a second source that keeps its columns open: its column tasks map that
    // source's columns, in parallel, for every action of the plan to read through.
    private boolean isReserveMappingSource2;
    // The source rows a reserve() sizes var-size columns off; see Frame#reserve.
    private LongList reserveSource1Ranges;
    private LongList reserveSource2Ranges;
    private long upcomingTableTxn;
    private boolean create = false;
    private volatile Throwable error;
    // How far rowCount runs past the live rows: 0 for a PLAIN partition, and the dead space a COMPOSITE one's
    // pieces have moved off otherwise. Held as the gap rather than as the live count so that every append,
    // which lands live rows, advances both numbers by moving rowCount alone.
    private long deadRowCount = 0;
    // When set, a COVERING posting-indexed column is opened as a plain column, so the frame writes its data but adds no
    // index entries.
    private boolean deferCoveredIndexing = false;
    private ColumnVersionReader crv;
    private RecycleBin<FrameImpl> frameRecycleBin;
    private int frameType;
    // See setKeepColumnsOpen: the columns openColumn hands out stay here, open, until close().
    private boolean isKeepingColumnsOpen = false;
    // Whether an append or merge under CommitMode.SYNC ran since the last sync(): without one there is nothing to
    // flush, and sync() returns before looking at a single column.
    private boolean isSyncPending = false;
    private final ObjList<FrameColumn> keptColumns = new ObjList<>();
    private final MessageBus messageBus;
    private RecordMetadata metadata;
    private long offset = 0;
    private Path partitionPath = new Path();
    private long partitionTimestamp;
    private long rowCount;
    private long timestampIndexAddr;
    // The logical row window shift() points this frame at, [windowLo, windowHi). Long.MAX_VALUE: no window.
    private long windowHi = Long.MAX_VALUE;
    private long windowLo = 0;

    public FrameImpl(FrameColumnPool columnPool, @Nullable MessageBus messageBus, FilesFacade ff) {
        this.columnPool = columnPool;
        this.messageBus = messageBus;
        this.ff = ff;
    }

    @Override
    public void addDataBytes(LongList dataBytes, LongList ranges) {
        final int columnCount = metadata.getColumnCount();
        final int size = columnCount * DATA_BYTES_STRIDE;
        final int previousSize = dataBytes.size();
        if (previousSize < size) {
            dataBytes.setPos(size);
            dataBytes.fill(previousSize, size, 0);
        }
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            final int slot = columnIndex * DATA_BYTES_STRIDE;
            // Resolved rather than opened: a fixed-size column needs nothing else from its file here.
            final long columnTop = Math.min(resolveColumnTop(columnIndex), rowCount);
            dataBytes.setQuick(slot + 1, extendLeadingNullRun(dataBytes.getQuick(slot + 1), columnTop, ranges));
            if (ColumnType.isVarSize(columnType)) {
                final FrameColumn column = openColumn(columnIndex);
                try {
                    final long bytes = varDataBytes(column, ColumnType.getDriver(columnType), ranges);
                    dataBytes.setQuick(slot, dataBytes.getQuick(slot) + bytes);
                } finally {
                    releaseColumn(column);
                }
            }
        }
    }

    @Override
    public void appendColumns(Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode) {
        assert source.getWindowLo() <= sourceLo && sourceHi <= source.getWindowHi();
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        markWritten(commitMode);
        execute(source, null, cthAppendColumnRef, true, sourceLo, sourceHi, IGNORE, IGNORE, IGNORE);
    }

    @Override
    public void close() {
        // Everything openColumn kept open dies with the frame; the next open starts with nothing cached, no
        // window and per-operation columns, whatever the previous one asked for. So does any flush not synced by
        // now: it belongs to a write being abandoned.
        Misc.freeObjListAndClear(keptColumns);
        fsyncPendingColumns.clear();
        this.isSyncPending = false;
        this.isKeepingColumnsOpen = false;
        this.windowLo = 0;
        this.windowHi = Long.MAX_VALUE;
        // Scoped to the open that set it, the same way deferCoveredIndexing is: the next open gets a frame
        // whose rows are all live, whatever the previous one stated.
        this.deadRowCount = 0;
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
    public long getLiveRowCount() {
        return rowCount - deadRowCount;
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
    public long getWindowHi() {
        return windowHi;
    }

    @Override
    public long getWindowLo() {
        return windowLo;
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
        assert source1.getWindowLo() <= source1Lo && source1Hi <= source1.getWindowHi();
        assert source2.getWindowLo() <= source2Lo && source2Hi <= source2.getWindowHi();
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        markWritten(commitMode);
        // Five task slots against a merge's six bounds, so the row count travels as a field.
        this.mergeIndexRows = mergeIndexRows;
        execute(source1, source2, cthMergeColumnRef, true, source1Lo, source1Hi, source2Lo, source2Hi, mergeIndexAddr);
    }

    @Override
    public FrameColumn openColumn(int columnIndex) {
        FrameColumn column;
        if (isKeepingColumnsOpen) {
            column = keptColumns.getQuiet(columnIndex);
            if (column == null) {
                column = createColumn(columnIndex);
                keptColumns.extendAndSet(columnIndex, column);
            }
        } else {
            column = createColumn(columnIndex);
        }
        if (frameType == COLUMN_CONTIGUOUS_FILE && !canWrite) {
            // A kept column maps the whole extent on its first use, which is every row any window of this
            // frame can reach. A per-operation column maps only what its one operation asks for.
            column.setReadWindow(windowHi, isKeepingColumnsOpen ? rowCount : 0);
        }
        return column;
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
                // columnTops is this frame's own array, so it stays dense; a sink writes through to _cv,
                // which is keyed by the writer index.
                sink.setColumnTop(metadata.getWriterIndex(i), colTop);
            }
        }
    }

    @Override
    public void releaseColumn(FrameColumn column) {
        if (!isKeepingColumnsOpen) {
            closeColumn(column);
        }
    }

    /**
     * Points a kept-open read-only frame at the same partition again for another operation - the next commit's
     * merges, typically - without reopening or remapping anything: its columns and their mappings stay, and only
     * what the previous operation set is reset. Valid only while the partition's directory and column files are
     * the ones this frame opened; see CompositeFrameCache for what guards that.
     */
    public void reopenRO(RecordMetadata metadata, ColumnVersionReader cvr, long partitionRowCount) {
        assert frameType == COLUMN_CONTIGUOUS_FILE && !canWrite;
        // A column the previous open found EMPTY - its top at or above the extent of the time, so no file was
        // opened - may have data now: the plans since have written rows for it at the tail. Its top is the old
        // extent and it has nothing to map, so it goes, and the next openColumn resolves it afresh. A column that
        // has a file keeps its top for good: every write lands above it.
        for (int i = 0, n = keptColumns.size(); i < n; i++) {
            final FrameColumn column = keptColumns.getQuick(i);
            if (column != null && column != DeletedFrameColumn.INSTANCE && column.getPrimaryFd() == -1) {
                column.close();
                keptColumns.setQuick(i, null);
            }
        }
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.rowCount = partitionRowCount;
        this.deadRowCount = 0;
        this.windowLo = 0;
        this.windowHi = Long.MAX_VALUE;
    }

    /**
     * The writable counterpart of {@link #reopenRO}: points a kept-open writable frame at the same partition again
     * for another plan, with the plan's own column-version view and column-top sink, keeping every column file open.
     */
    public void reopenRW(RecordMetadata metadata, ColumnVersionReader cvr, ColumnTopSink columnTopSink, long size) {
        assert frameType == COLUMN_CONTIGUOUS_FILE && canWrite;
        this.metadata = metadata;
        resetColumnTops(metadata.getColumnCount());
        this.crv = cvr;
        this.columnTopSink = columnTopSink;
        columnTopSink.ofColumnCount(metadata.getColumnCount());
        this.rowCount = size;
        this.deadRowCount = 0;
        this.windowLo = 0;
        this.windowHi = Long.MAX_VALUE;
    }

    @Override
    public void reserve(long rowHi, Frame source1, LongList source1Ranges, @Nullable Frame source2, @Nullable LongList source2Ranges) {
        if (rowHi <= rowCount) {
            return;
        }
        this.reserveSource1Ranges = source1Ranges;
        this.reserveSource2Ranges = source2 != null ? source2Ranges : null;
        this.isReserveMappingSource2 = source2 instanceof FrameImpl f && f.isKeepingColumnsOpen;
        try {
            // No tops are saved: nothing is written, so no column's top moves.
            execute(source1, source2, cthReserveColumnRef, false, rowHi, IGNORE, IGNORE, IGNORE, IGNORE);
        } finally {
            this.reserveSource1Ranges = null;
            this.reserveSource2Ranges = null;
            this.isReserveMappingSource2 = false;
        }
    }

    @Override
    public void reserve(long rowHi, LongList dataBytes) {
        if (rowHi <= rowCount) {
            return;
        }
        assert canWrite;
        final int columnCount = metadata.getColumnCount();
        assert dataBytes.size() >= columnCount * DATA_BYTES_STRIDE;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType >= 0) {
                final FrameColumn column = openColumn(columnIndex);
                try {
                    final int slot = columnIndex * DATA_BYTES_STRIDE;
                    final long topRows = topAbsorbedRows(column, rowHi, leadingNullRows(dataBytes.getQuick(slot + 1)));
                    long bytes = dataBytes.getQuick(slot);
                    if (ColumnType.isVarSize(columnType)) {
                        bytes -= topRows * ColumnType.getDriver(columnType).getDataVectorMinEntrySize();
                    }
                    column.reserve(rowCount, rowHi - topRows, bytes, false);
                } finally {
                    releaseColumn(column);
                }
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
            columnTopSink.setColumnTop(metadata.getWriterIndex(columnIndex), columnTop);
        }
    }

    @Override
    public void setKeepColumnsOpen(boolean isKeepColumnsOpen) {
        // Bounded by the same budget execute() opens columns in, so a wide table keeps no more files open at
        // once than one batch of an operation already does: past it, every operation opens its own columns.
        final boolean isKeeping = isKeepColumnsOpen && metadata.getColumnCount() <= MAX_OPEN_COLUMNS;
        if (!isKeeping) {
            for (int i = 0, n = keptColumns.size(); i < n; i++) {
                closeColumn(keptColumns.getQuick(i));
            }
            keptColumns.clear();
        }
        this.isKeepingColumnsOpen = isKeeping;
    }

    @Override
    public void setLiveRowCount(long liveRowCount) {
        assert liveRowCount >= 0 && liveRowCount <= rowCount;
        this.deadRowCount = rowCount - liveRowCount;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public void shift(long rowLo, long rowHi) {
        assert 0 <= rowLo && rowLo <= rowHi && rowHi <= rowCount;
        this.windowLo = rowLo;
        this.windowHi = rowHi;
    }

    @Override
    public void sync() {
        if (!isSyncPending) {
            return;
        }
        final int keptCount = keptColumns.size();
        int pendingCount = 0;
        for (int i = 0; i < keptCount; i++) {
            final FrameColumn column = keptColumns.getQuick(i);
            if (column != null && column.isSyncPending()) {
                pendingCount++;
            }
        }
        if (pendingCount > 0) {
            syncKeptColumns(keptCount, pendingCount);
        }
        for (int i = 0, n = fsyncPendingColumns.size(); i < n; i++) {
            fsyncColumnFiles(fsyncPendingColumns.get(i));
        }
        fsyncPendingColumns.clear();
        isSyncPending = false;
    }

    /**
     * Extends a column's leading NULL run over the source rows of {@code ranges}, taken in the order the plan appends
     * them: a row under the source column's top lengthens the run, and the first row holding a value ends it. The run
     * is carried as its length while it is open and as that length's bitwise complement once it has ended - see
     * {@link #leadingNullRows}.
     */
    private static long extendLeadingNullRun(long run, long columnTop, LongList ranges) {
        for (int i = 0, n = ranges.size(); i < n && run >= 0; i += 2) {
            final long lo = ranges.getQuick(i);
            final long hi = ranges.getQuick(i + 1);
            if (lo >= hi) {
                continue;
            }
            if (lo < columnTop) {
                run += Math.min(hi, columnTop) - lo;
            }
            if (hi > columnTop) {
                return ~run;
            }
        }
        return run;
    }

    /**
     * The length of a run {@link #extendLeadingNullRun} carries, whether or not it has ended.
     */
    private static long leadingNullRows(long run) {
        return run < 0 ? ~run : run;
    }

    /**
     * The data bytes rows {@code [lo, hi)} of a var-size source column take, summed over every range. A row under
     * the column's top has no bytes of its own and is written as this type's NULL, which has a size too.
     */
    private static long varDataBytes(FrameColumn column, ColumnTypeDriver driver, LongList ranges) {
        long bytes = 0;
        for (int i = 0, n = ranges.size(); i < n; i += 2) {
            long lo = ranges.getQuick(i);
            final long hi = ranges.getQuick(i + 1);
            if (lo >= hi) {
                continue;
            }
            final long top = column.getColumnTop();
            if (lo < top) {
                bytes += (Math.min(hi, top) - lo) * driver.getDataVectorMinEntrySize();
                lo = top;
                if (lo >= hi) {
                    continue;
                }
            }
            // The aux vector is addressed from the column's row 0, which is the top's row.
            final long auxAddr = column.getContiguousAuxAddr(hi);
            bytes += driver.getDataVectorSize(auxAddr, lo - top, hi - 1 - top);
        }
        return bytes;
    }

    /**
     * Closes a column this frame handed out, noting first whether its files still owe a blocking flush, which
     * {@link #sync()} then does by name - the column's fds and mappings are gone by then.
     */
    private void closeColumn(FrameColumn column) {
        if (column != null && column.isSyncPending()) {
            fsyncPendingColumns.add(column.getColumnIndex());
        }
        Misc.free(column);
    }

    private void closeColumns(Frame source1, @Nullable Frame source2, int columnLo, int columnHi) {
        // Each column goes back to the frame that opened it, which closes it unless it keeps its columns open.
        for (int i = columnLo; i < columnHi; i++) {
            releaseColumn(targetColumns.getQuick(i));
            targetColumns.setQuick(i, null);
            source1.releaseColumn(source1Columns.getQuick(i));
            source1Columns.setQuick(i, null);
            if (source2 != null) {
                source2.releaseColumn(source2Columns.getQuick(i));
            }
            source2Columns.setQuick(i, null);
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
            // rowCount is this frame's own tail; getLiveRowCount() is how much of it rows still point at.
            FrameAlgebra.appendColumn(targetColumn, rowCount, getLiveRowCount(), source1Columns.getQuick(columnIndex), sourceLo, sourceHi, commitMode);
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

    /**
     * One column's share of {@link #sync()}.
     */
    private void cthSyncColumn(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long ignore0,
            long ignore1,
            long ignore2,
            long ignore3,
            long ignore4
    ) {
        if (errorCount.get() > 0) {
            return;
        }
        try {
            targetColumns.getQuick(columnIndex).sync();
        } catch (Throwable th) {
            onError(columnIndex, th);
        }
    }

    /**
     * One column's share of {@link #reserve}.
     */
    private void cthReserveColumn(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long rowHi,
            long ignore1,
            long ignore2,
            long ignore3,
            long ignore4
    ) {
        if (errorCount.get() > 0) {
            return;
        }
        try {
            final FrameColumn targetColumn = targetColumns.getQuick(columnIndex);
            final FrameColumn source1Column = source1Columns.getQuick(columnIndex);
            // Only a plan of appends from source1 alone runs its ranges in the order they are listed. A merge with
            // source2 can come first and end the target's top, so such a plan absorbs nothing up front.
            final long topRows = reserveSource2Ranges == null
                    ? topAbsorbedRows(targetColumn, rowHi, leadingNullRows(extendLeadingNullRun(0, source1Column.getColumnTop(), reserveSource1Ranges)))
                    : 0;
            long dataBytes = 0;
            if (ColumnType.isVarSize(columnType)) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                // The absorbed rows are leading rows under source1's top, each of which varDataBytes charges a NULL.
                dataBytes = varDataBytes(source1Column, driver, reserveSource1Ranges) - topRows * driver.getDataVectorMinEntrySize();
                if (reserveSource2Ranges != null) {
                    // Maps the source's column on the way, which is all the fixed-size branch below does.
                    dataBytes += varDataBytes(source2Columns.getQuick(columnIndex), driver, reserveSource2Ranges);
                }
            } else if (isReserveMappingSource2 && reserveSource2Ranges != null) {
                // A kept-open column maps the source's whole extent on its first use, so any row it holds will do.
                long hi = 0;
                for (int i = 1, n = reserveSource2Ranges.size(); i < n; i += 2) {
                    hi = Math.max(hi, reserveSource2Ranges.getQuick(i));
                }
                source2Columns.getQuick(columnIndex).getContiguousDataAddr(hi);
            }
            // One allocation and one (re)map per file of the target column, for every write of the plan.
            final int timestampIndex = metadata.getTimestampIndex();
            targetColumn.reserve(
                    rowCount,
                    rowHi - topRows,
                    dataBytes,
                    timestampIndex > -1 && metadata.isDedupKey(timestampIndex)
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
        // The last live column runs here rather than through the queue: this thread would otherwise only wait for it.
        int inlineColumn = columnHi - 1;
        while (inlineColumn >= columnLo && !isLiveColumn(inlineColumn)) {
            inlineColumn--;
        }
        for (int i = columnLo; i < columnHi; i++) {
            if (!isLiveColumn(i)) {
                continue;
            }
            final long cursor = i == inlineColumn ? -1 : pubSeq.next();
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
                // The inline column, or the queue is full. Run the column here rather than wait for room, the same
                // way TableWriter#dispatchColumnTasks does - and this is also what makes progress guaranteed when
                // nothing else is draining the queue.
                taskHandler.run(i, source1Columns.getQuick(i).getColumnType(), timestampColumnIndex, long0, long1, long2, long3, long4);
            }
        }
        // Work stealing: the calling thread runs whatever it can reach, including tasks other writers
        // published, until every task of THIS operation has counted down.
        TableWriter.consumeColumnTasks0(queue, queuedCount, messageBus.getColumnTaskSubSeq(), doneLatch);
    }

    /**
     * @param saveTops whether every live column's top is saved once its task is done - what every operation that
     *                 writes rows wants, and what one that writes none has no business doing
     */
    private void execute(
            Frame source1,
            @Nullable Frame source2,
            TableWriter.ColumnTaskHandler taskHandler,
            boolean saveTops,
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
                    if (saveTops) {
                        for (int i = columnLo; i < columnHi; i++) {
                            if (isLiveColumn(i)) {
                                saveChanges(targetColumns.getQuick(i));
                            }
                        }
                    }
                } finally {
                    closeColumns(source1, source2, columnLo, columnHi);
                }
            }
        } finally {
            // Every batch released its own columns by now, so this only drops the references.
            targetColumns.clear();
            source1Columns.clear();
            source2Columns.clear();
        }
    }

    private void free() {
        Misc.freeObjListAndClear(keptColumns);
        partitionPath = Misc.free(partitionPath);
    }

    /**
     * Flushes the files of a column {@link #closeColumn} closed with a flush still owed: opens each by name and
     * fsyncs it, which flushes every dirty page of the file, the ones written through a mapping since unmapped
     * included.
     */
    private void fsyncColumnFiles(int columnIndex) {
        final int columnType = metadata.getColumnType(columnIndex);
        if (columnType < 0) {
            return;
        }
        final CharSequence columnName = metadata.getColumnName(columnIndex);
        // Keyed by the writer index, see resolveColumnTop.
        final long columnNameTxn = crv.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final int plen = partitionPath.size();
        try {
            fsyncFile(TableUtils.dFile(partitionPath, columnName, columnNameTxn));
            if (ColumnType.isVarSize(columnType)) {
                partitionPath.trimTo(plen);
                fsyncFile(TableUtils.iFile(partitionPath, columnName, columnNameTxn));
            }
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    private void fsyncFile(LPSZ path) {
        final long fd = TableUtils.openRO(ff, path, LOG);
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
        }
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
        long columnTop = resolveColumnTop(columnIndex);
        // Keyed by the writer index, see resolveColumnTop.
        long columnTxn = crv.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));

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

    private void markWritten(int commitMode) {
        // ASYNC is treated as NOSYNC, see ContiguousFileFixFrameColumn#markWritten.
        if (commitMode == CommitMode.SYNC) {
            isSyncPending = true;
        }
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
            source1Columns.setQuick(i, source1.openColumn(i));
            if (!isLiveColumn(i)) {
                // A dropped column: neither the other source nor the target opens a file for it.
                continue;
            }
            if (source2 != null) {
                source2Columns.setQuick(i, source2.openColumn(i));
            }
            targetColumns.setQuick(i, openColumn(i));
        }
    }

    private void resetColumnTops(int columnCount) {
        columnTops.setPos(columnCount);
        columnTops.fill(0, columnCount, -1L);
    }

    /**
     * The top {@link #openColumn} would open column {@code columnIndex} with, before it is capped at the row count,
     * without opening the column. A tracked top (only ever set by this frame's own saveChanges) takes over from crv
     * entirely once present: it already reflects everything crv would resolve to PLUS every piece this frame wrote.
     */
    private long resolveColumnTop(int columnIndex) {
        if (frameType != COLUMN_CONTIGUOUS_FILE) {
            return 0;
        }
        final long columnTop = columnTops.getQuick(columnIndex);
        if (columnTop >= 0) {
            return columnTop;
        }
        // _cv records are keyed by the WRITER index. TableReaderMetadata is dense - it drops every retired
        // column - so an ALTER COLUMN TYPE or a DROP COLUMN makes the two index spaces diverge, and a dense
        // lookup then reads some other column's name txn and top. TableWriterMetadata's writer index is the
        // identity, so this is a no-op for the writer's own callers.
        final int writerIndex = metadata.getWriterIndex(columnIndex);
        final int crvRecIndex = crv.getRecordIndex(partitionTimestamp, writerIndex);
        return crv.getColumnTopByIndexOrDefault(crvRecIndex, partitionTimestamp, writerIndex, rowCount);
    }

    /**
     * Syncs the kept-open columns with a flush pending, spread over the column tasks the way a write is.
     */
    private void syncKeptColumns(int columnCount, int pendingCount) {
        errorCount.set(0);
        error = null;
        targetColumns.setAll(columnCount, null);
        // dispatchColumns reads which columns to run off source1Columns: a dropped column is skipped, and so is one
        // with nothing to flush.
        source1Columns.setAll(columnCount, DeletedFrameColumn.INSTANCE);
        try {
            for (int i = 0; i < columnCount; i++) {
                final FrameColumn column = keptColumns.getQuick(i);
                if (column != null && column.isSyncPending()) {
                    source1Columns.setQuick(i, column);
                    targetColumns.setQuick(i, column);
                }
            }
            dispatchColumns(cthSyncColumnRef, messageBus != null && pendingCount > 1, 0, columnCount, IGNORE, IGNORE, IGNORE, IGNORE, IGNORE);
            throwOnError();
        } finally {
            targetColumns.clear();
            source1Columns.clear();
        }
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

    /**
     * How many of the rows a plan appends below {@code rowHi} the target column takes into its top rather than its
     * files: the leading NULL run of the plan's sources, as long as the column reaches the plan with no data of its
     * own and every row of this frame is live - the two conditions {@link FrameAlgebra#appendColumn} raises a top on.
     */
    private long topAbsorbedRows(FrameColumn targetColumn, long rowHi, long leadingNullRows) {
        if (targetColumn.getColumnTop() == rowCount && deadRowCount == 0) {
            return Math.min(leadingNullRows, rowHi - rowCount);
        }
        return 0;
    }

    void setRecycleBin(RecycleBin<FrameImpl> frameRecycleBin) {
        this.frameRecycleBin = frameRecycleBin;
    }
}
