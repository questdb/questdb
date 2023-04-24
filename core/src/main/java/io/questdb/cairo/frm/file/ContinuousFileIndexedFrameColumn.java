/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class ContinuousFileIndexedFrameColumn extends ContinuousFileFixFrameColumn {
    private final FilesFacade ff;
    private final BitmapIndexWriter indexWriter;

    public ContinuousFileIndexedFrameColumn(FilesFacade ff, long fileOpts, long keyAppendPageSize, long valueAppendPageSize) {
        super(ff, fileOpts);
        this.ff = ff;
        this.indexWriter = new BitmapIndexWriter(ff, keyAppendPageSize, valueAppendPageSize);
    }

    @Override
    public void append(long offset, FrameColumn sourceColumn, long sourceOffset, long count) {
        super.append(offset, sourceColumn, sourceOffset, count);
        int fd = super.getPrimaryFd();
        int shl = ColumnType.pow2SizeOf(getColumnType());

        count -= sourceColumn.getColumnTop();
        if (count > 0) {
            long mappedAddress = TableUtils.mapAppendColumnBuffer(ff, fd, (offset - getColumnTop()) << shl, count << shl, false, MEMORY_TAG);
            try {
                indexWriter.rollbackConditionally(offset);
                for (long i = 0; i < count; i++) {
                    indexWriter.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(mappedAddress + (i << shl))), i + offset);
                }
                indexWriter.setMaxValue(offset + count - 1);
            } finally {
                TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, (offset - getColumnTop()) << shl, count << shl, MEMORY_TAG);
            }
        }
    }

    @Override
    public void appendNulls(long offset, long count) {
        super.appendNulls(offset, count);
        indexWriter.rollbackConditionally(offset);
        for (long i = 0; i < count; i++) {
            indexWriter.add(0, offset + i);
        }
        indexWriter.setMaxValue(offset + count - 1);
    }

    @Override
    public void close() {
        indexWriter.close();
        super.close();
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
        throw new UnsupportedOperationException();
    }

    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            int indexBlockCapacity,
            long columnTop,
            int columnIndex
    ) {
        super.ofRW(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex);
        indexWriter.of(partitionPath, columnName, columnTxn, columnTop < 0 ? indexBlockCapacity : 0);
    }
}
