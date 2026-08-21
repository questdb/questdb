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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.frm.ColumnTopSink;
import io.questdb.cairo.frm.DeletedFrameColumn;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.frm.FrameColumnTypePool;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.setSinkForNativePartition;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_CONTIGUOUS_FILE;
import static io.questdb.cairo.frm.FrameColumn.COLUMN_MEMORY;

public class FrameImpl implements Frame {
    private final FrameColumnPool columnPool;
    private boolean canWrite = false;
    private ReadOnlyObjList<? extends MemoryCR> columnsMemory;
    private ColumnTopSink columnTopSink;
    /**
     * Per-column tracked top, index = column index, -1 = untouched this open. Populated only by
     * {@link #saveChanges} when this frame has no external {@link ColumnTopSink} - see
     * {@link #publishColumnTops}. Reset (not reallocated) on every open/create, since a pooled
     * {@code FrameImpl} outlives any one partition and a stale entry from a previous, unrelated use would
     * otherwise leak through {@link #getContiguousFileFrameColumn}.
     */
    private final LongList columnTops = new LongList();
    private boolean create = false;
    private ColumnVersionReader crv;
    private RecycleBin<FrameImpl> frameRecycleBin;
    private int frameType;
    private RecordMetadata metadata;
    private long offset = 0;
    private Path partitionPath = new Path();
    private long partitionTimestamp;
    private long rowCount;
    private long timestampIndexAddr;

    public FrameImpl(FrameColumnPool columnPool) {
        this.columnPool = columnPool;
    }

    @Override
    public void close() {
        this.columnsMemory = null;
        this.columnTopSink = null;
        this.crv = null;
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
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = false;
        this.frameType = COLUMN_CONTIGUOUS_FILE;
        this.timestampIndexAddr = 0;
    }

    @Override
    public void publishColumnTops(ColumnVersionWriter cvw) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            long colTop = columnTops.getQuick(i);
            // -1 (untouched, this frame has no sink and nothing wrote through it): nothing to record.
            // Anything else, INCLUDING colTop == rowCount ("every row was a free ride, no real byte
            // anywhere"), still goes through mergeColumnTop: a brand-new partition timestamp's own
            // chronological default can resolve to something other than what this frame just
            // determined (see ColumnVersionWriter#mergeColumnTop), and skipping here would leave that
            // wrong default in place instead of the value this frame actually computed.
            if (colTop > -1) {
                cvw.mergeColumnTop(partitionTimestamp, i, colTop);
            }
        }
    }

    public void saveChanges(FrameColumn frameColumn) {
        if (!canWrite) {
            throw CairoException.critical(0).put("cannot save column top, partition frame is read-only [path=").put(partitionPath).put(']');
        }
        if (columnTopSink != null) {
            columnTopSink.setColumnTop(frameColumn.getColumnIndex(), frameColumn.getColumnTop());
        } else {
            // No external sink: track internally instead, for publishColumnTops to push later. Never
            // regresses a column's tracked top - only up or unchanged, matching how a column's top can
            // only ever advance while this frame is written to (addTop grows it; once real bytes land
            // below it, it is fixed for good) - so a stale, smaller read can never overwrite a piece
            // that already advanced it further.
            int columnIndex = frameColumn.getColumnIndex();
            columnTops.setQuick(columnIndex, Math.max(frameColumn.getColumnTop(), columnTops.getQuick(columnIndex)));
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
        // A tracked top (only ever set by this frame's own saveChanges, when it has no external sink)
        // takes over from crv entirely once present: it already reflects everything crv would resolve to
        // PLUS every piece this frame has written since, and re-resolving from crv here would throw that
        // progress away and read the OLD directory's value instead of this frame's own current one.
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

    private void resetColumnTops(int columnCount) {
        columnTops.setPos(columnCount);
        columnTops.fill(0, columnCount, -1L);
    }

    void setRecycleBin(RecycleBin<FrameImpl> frameRecycleBin) {
        this.frameRecycleBin = frameRecycleBin;
    }
}
