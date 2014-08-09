/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.index;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;

import java.nio.ByteBuffer;

public class IndexCursor implements Cursor {
    private int remainingBlockCount;
    private int remainingRowCount;
    private long rowBlockOffset;
    private ByteBuffer buffer;
    private int bufPos;
    private long size;
    //
    private String columnName;
    private int columnIndex = -1;
    private KVIndex index;
    private int key;


    public void configure(Partition partition) throws JournalException {
        if (columnIndex == -1) {
            columnIndex = partition.getJournal().getMetadata().getColumnIndex(columnName);
        }
        this.index = partition.getIndexForColumn(columnIndex);

        this.remainingBlockCount = 0;
        this.remainingRowCount = 0;

        if (key < 0) {
            return;
        }

        long keyOffset = index.getKeyOffset(key);
        if (keyOffset >= index.firstEntryOffset + index.keyBlockSize) {
            return;
        }

        ByteBuffer buf = index.kData.getBuffer(keyOffset, KVIndex.ENTRY_SIZE);
        this.rowBlockOffset = buf.getLong();
        this.size = buf.getLong();

        if (size == 0) {
            return;
        }

        this.remainingBlockCount = (int) (this.size / index.rowBlockLen) + 1;
        this.remainingRowCount = (int) (this.size % index.rowBlockLen);

        if (remainingRowCount == 0) {
            remainingBlockCount--;
            remainingRowCount = index.rowBlockLen;
        }

        this.buffer = index.rData.getBuffer(this.rowBlockOffset - index.rowBlockSize, index.rowBlockSize);
        this.bufPos = buffer.position();
    }

    public IndexCursor with(String columnName, int key) {
        this.columnName = columnName;
        this.key = key;
        return this;
    }

    public boolean hasNext() {
        return this.remainingRowCount > 0 || this.remainingBlockCount > 0;
    }

    public long next() {
        if (remainingRowCount == 0) {
            this.buffer = index.rData.getBuffer(rowBlockOffset - index.rowBlockSize, index.rowBlockSize);
            this.bufPos = buffer.position();
            this.remainingRowCount = index.rowBlockLen;
        }

        long result = this.buffer.getLong(this.bufPos + --this.remainingRowCount * 8);

        if (remainingRowCount == 0 && --this.remainingBlockCount > 0) {
            this.rowBlockOffset = this.buffer.getLong(this.bufPos + index.rowBlockLen * 8);
        }
        return result;
    }

    public long size() {
        return size;
    }
}
