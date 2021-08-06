/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.tasks;

import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public class TableWriterTask implements Closeable, CharSink {
    public static final int TSK_SLAVE_SYNC = 1;
    private final char[] doubleDigits = new char[21];
    private int type;
    private long tableId;
    private long data;
    private long dataSize;
    private long appendPtr;
    private long appendLim;

    public TableWriterTask(long size) {
        data = Unsafe.calloc(size);
        this.dataSize = size;
        this.appendPtr = data;
        this.appendLim = data + dataSize;
    }

    @Override
    public void close() {
        if (dataSize > 0) {
            Unsafe.free(data, dataSize);
            dataSize = 0;
            appendPtr = 0;
            appendLim = 0;
        }
    }

    public void fromSlaveSyncRequest(
            long tableId,
            long txMem,
            long txMemSize,
            long metaMem,
            long metaMemSize
    ) {
        long tskSize = this.dataSize;
        if (tskSize < txMemSize + metaMemSize + 16) {
            resize(txMemSize + metaMemSize + 16);
        }
        long p = this.data;
        Unsafe.getUnsafe().putLong(p, txMemSize);
        Vect.memcpy(txMem, p + 8, txMemSize);
        Unsafe.getUnsafe().putLong(p + txMemSize + 8, metaMemSize);
        Vect.memcpy(metaMem, p + txMemSize + 16, metaMemSize);
        this.type = TSK_SLAVE_SYNC;
        this.tableId = tableId;
    }

    public long getAppendOffset() {
        return appendPtr - data;
    }

    public long getData() {
        return data;
    }

    public long getDataSize() {
        return dataSize;
    }

    @Override
    public char[] getDoubleDigitsBuffer() {
        return doubleDigits;
    }

    @Override
    public CharSink put(char c) {
        if (appendPtr >= appendLim) {
            resize(dataSize * 2);
        }
        Unsafe.getUnsafe().putByte(appendPtr++, (byte) c);
        return this;
    }

    public long getTableId() {
        return tableId;
    }

    public int getType() {
        return type;
    }

    public void putAt(long offset, int value) {
        Unsafe.getUnsafe().putInt(data + offset, value);
    }

    public void resize(long size) {
        assert dataSize > 0;
        if (size > dataSize) {
            long appendOffset = getAppendOffset();
            data = Unsafe.realloc(data, dataSize, size);
            dataSize = size;
            appendPtr = data + appendOffset;
            appendLim = data + dataSize;
        }
    }

    public long skip(int byteCount) {
        if (appendPtr + byteCount - 1 >= appendLim) {
            resize(Math.max(dataSize * 2, (appendPtr - data) + byteCount));
        }
        final long offset = getAppendOffset();
        appendPtr += byteCount;
        return offset;
    }
}
