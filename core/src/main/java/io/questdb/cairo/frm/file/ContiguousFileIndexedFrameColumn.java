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

import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class ContiguousFileIndexedFrameColumn extends ContiguousFileFixFrameColumn {
    private final BitmapIndexWriter indexWriter;

    public ContiguousFileIndexedFrameColumn(CairoConfiguration configuration) {
        super(configuration);
        this.indexWriter = new BitmapIndexWriter(configuration);
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        super.append(appendOffsetRowCount, sourceColumn, sourceLo, sourceHi, commitMode);
        long fd = super.getPrimaryFd();
        int shl = ColumnType.pow2SizeOf(getColumnType());

        final long size = sourceHi - sourceLo;
        assert size >= 0;

        if (size > 0) {
            long mappedAddress = TableUtils.mapAppendColumnBuffer(ff, fd, (appendOffsetRowCount - getColumnTop()) << shl, size << shl, false, MEMORY_TAG);
            try {
                indexWriter.rollbackConditionally(appendOffsetRowCount);
                for (long i = 0; i < size; i++) {
                    indexWriter.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(mappedAddress + (i << shl))), appendOffsetRowCount + i);
                }
                indexWriter.setMaxValue(appendOffsetRowCount + size - 1);
                indexWriter.commit();
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, (appendOffsetRowCount - getColumnTop()) << shl, size << shl, MEMORY_TAG);
            }
        }
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        super.appendNulls(rowCount, sourceColumnTop, commitMode);
        indexWriter.rollbackConditionally(rowCount);
        for (long i = 0; i < sourceColumnTop; i++) {
            indexWriter.add(0, rowCount + i);
        }
        indexWriter.setMaxValue(rowCount + sourceColumnTop - 1);
        indexWriter.commit();
    }

    @Override
    public void close() {
        indexWriter.close();
        super.close();
    }

    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            int indexBlockCapacity,
            long columnTop,
            int columnIndex,
            boolean isEmpty
    ) {
        super.ofRW(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex);
        try {
            indexWriter.of(partitionPath, columnName, columnTxn, isEmpty ? indexBlockCapacity : 0);
        } catch (Throwable e) {
            // indexWriter has already closed
            super.close();
        }
    }

    @Override
    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            long columnTop,
            int columnIndex
    ) {
        // close to reuse
        closed = false;
        super.close();
        throw new UnsupportedOperationException();
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    private int keyCount(int key, long size, long mappedAddress) {
        int count = 0;
        for (long i = 0; i < size; i++) {
            if (TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(mappedAddress + (i << 2))) == key) {
                count++;
            }
        }
        return count;
    }
}
