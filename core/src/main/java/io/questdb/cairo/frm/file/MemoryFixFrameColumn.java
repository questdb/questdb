/*******************************************************************************
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

import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.vm.api.MemoryCR;

public final class MemoryFixFrameColumn implements FrameColumn {
    // Introduce a flag to avoid double close, which will lead to very serious consequences.
    private boolean closed;
    private int columnIndex;
    private MemoryCR columnMemory;
    private int columnType;
    private RecycleBin<FrameColumn> recycleBin;
    private long rowCount;

    @Override
    public void addTop(long value) {
        throw new UnsupportedOperationException("Memory Column does not support column tops");
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (recycleBin != null && !recycleBin.isClosed()) {
                recycleBin.put(this);
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getColumnTop() {
        return 0;
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public long getContiguousAuxAddr(long rowHi) {
        return 0;
    }

    @Override
    public long getContiguousDataAddr(long rowHi) {
        if (rowHi > rowCount) {
            throw new IllegalArgumentException("rowHi exceeds rowCount");
        }
        return columnMemory.addressOf(0);
    }

    @Override
    public long getPrimaryFd() {
        return -1;
    }

    @Override
    public long getSecondaryFd() {
        return -1;
    }

    @Override
    public int getStorageType() {
        return COLUMN_MEMORY;
    }

    public void of(
            int columnIndex,
            int columnType,
            long rowCount,
            MemoryCR columnMemory
    ) {
        this.rowCount = rowCount;
        this.columnMemory = columnMemory;
        this.columnType = columnType;
        this.columnIndex = columnIndex;
        this.closed = false;
    }

    public void setRecycleBin(RecycleBin<FrameColumn> recycleBin) {
        assert this.recycleBin == null;
        this.recycleBin = recycleBin;
    }
}
