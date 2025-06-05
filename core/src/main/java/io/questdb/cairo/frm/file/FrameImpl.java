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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.frm.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

public class FrameImpl implements Frame {
    private final FrameColumnPool columnPool;
    private boolean canWrite = false;
    private boolean create = false;
    private ColumnVersionReader crv;
    private RecycleBin<FrameImpl> frameRecycleBin;
    private RecordMetadata metadata;
    private long offset = 0;
    private Path partitionPath = new Path();
    private long partitionTimestamp;
    private long rowCount;

    public FrameImpl(FrameColumnPool columnPool) {
        this.columnPool = columnPool;
    }

    @Override
    public void close() {
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
        int columnType = metadata.getColumnType(columnIndex);
        if (columnType < 0) {
            return DeletedFrameColumn.INSTANCE;
        }
        boolean isIndexed = metadata.isColumnIndexed(columnIndex);
        int indexBlockCapacity = isIndexed ? metadata.getIndexValueBlockCapacity(columnIndex) : 0;
        int crvRecIndex = crv.getRecordIndex(partitionTimestamp, columnIndex);
        long columnTop = crv.getColumnTopByIndexOrDefault(crvRecIndex, partitionTimestamp, columnIndex, rowCount);
        long columnTxn = crv.getColumnNameTxn(partitionTimestamp, columnIndex);

        FrameColumnTypePool columnTypePool = canWrite ? columnPool.getPoolRW(columnType) : columnPool.getPoolRO(columnType);
        boolean createNew = columnTop >= rowCount || create;
        columnTop = Math.min(columnTop, rowCount);
        return columnTypePool.create(partitionPath, metadata.getColumnName(columnIndex), columnTxn, columnType, indexBlockCapacity, columnTop, columnIndex, createNew);
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    public void createRW(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionWriter cvw, long size) {
        this.metadata = metadata;
        this.crv = cvw;
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = true;
    }

    public void openRO(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionReader cvr, long partitionRowCount) {
        this.metadata = metadata;
        this.crv = cvr;
        this.rowCount = partitionRowCount;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = false;
        this.create = false;
    }

    public void openRW(Path partitionPath, long partitionTimestamp, RecordMetadata metadata, ColumnVersionWriter cvw, long size) {
        this.metadata = metadata;
        this.crv = cvw;
        this.rowCount = size;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionPath.of(partitionPath);
        this.canWrite = true;
        this.create = false;
    }

    public void saveChanges(FrameColumn frameColumn) {
        if (!canWrite) {
            throw CairoException.critical(0).put("cannot save column top, partition frame is read-only [path=").put(partitionPath).put(']');
        }
        ColumnVersionWriter cvw = (ColumnVersionWriter) crv;
        cvw.upsertColumnTop(partitionTimestamp, frameColumn.getColumnIndex(), frameColumn.getColumnTop());
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

    void setRecycleBin(RecycleBin<FrameImpl> frameRecycleBin) {
        this.frameRecycleBin = frameRecycleBin;
    }
}
