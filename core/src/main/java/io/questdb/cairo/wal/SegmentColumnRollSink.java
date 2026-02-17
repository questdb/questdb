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

package io.questdb.cairo.wal;

import io.questdb.griffin.ColumnConversionOffsetSink;
import io.questdb.std.LongList;

public class SegmentColumnRollSink implements ColumnConversionOffsetSink {
    private final int ENTRIES_PER_COLUMN = 6;
    private final LongList data = new LongList();
    private int baseIndex = -ENTRIES_PER_COLUMN;

    public void clear() {
        data.clear();
        baseIndex = -ENTRIES_PER_COLUMN;
    }

    public int count() {
        return data.size() / ENTRIES_PER_COLUMN;
    }

    public long getDestAuxFd(int columnIndex) {
        return data.get(columnIndex * ENTRIES_PER_COLUMN + 1);
    }

    public long getDestAuxSize(int columnIndex) {
        return data.get(columnIndex * ENTRIES_PER_COLUMN + 5);
    }

    public long getDestPrimaryFd(int columnIndex) {
        return data.get(columnIndex * ENTRIES_PER_COLUMN);
    }

    public long getDestPrimarySize(int columnIndex) {
        return data.get(columnIndex * ENTRIES_PER_COLUMN + 4);
    }

    public long getSrcAuxOffset(int c) {
        return data.get(c * ENTRIES_PER_COLUMN + 3);
    }

    public long getSrcPrimaryOffset(int columnIndex) {
        return data.get(columnIndex * ENTRIES_PER_COLUMN + 2);
    }

    public void nextColumn() {
        baseIndex += ENTRIES_PER_COLUMN;
        data.setPos(baseIndex + ENTRIES_PER_COLUMN);
        data.fill(baseIndex, baseIndex + ENTRIES_PER_COLUMN, -1);
    }

    public void setDestPrimaryFd(long fd) {
        data.extendAndSet(baseIndex, fd);
    }

    public void setDestSecondaryFd(long fd) {
        data.extendAndSet(baseIndex + 1, fd);
    }

    @Override
    public void setDestSizes(long primarySize, long auxSize) {
        data.extendAndSet(baseIndex + 4, primarySize);
        data.extendAndSet(baseIndex + 5, auxSize);
    }

    @Override
    public void setSrcOffsets(long primaryOffset, long auxOffset) {
        data.extendAndSet(baseIndex + 2, primaryOffset);
        data.extendAndSet(baseIndex + 3, auxOffset);
    }
}
