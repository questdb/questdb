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

import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;

public class TableWriterTask implements Closeable {
    public static final int TSK_SLAVE_SYNC = 1;
    public static final int TSK_ALTER_TABLE = 2;
    private int type;
    private long tableId;
    private String tableName;
    private long data;
    private long dataSize;
    private long appendPtr;
    private long appendLim;
    private long instance;
    private long sequence;
    private long ip;

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
            String tableName,
            long txMem,
            long txMemSize,
            long metaMem,
            long metaMemSize,
            long slaveIP,
            long sequence
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
        this.tableName = tableName;
        this.ip = slaveIP;
        this.sequence = sequence;
    }

    public void of(
            int type,
            long tableId,
            String tableName
    ) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.type = type;
        this.appendPtr = data;
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

    public long getInstance() {
        return instance;
    }

    public void setInstance(long instance) {
        this.instance = instance;
    }

    public long getIp() {
        return ip;
    }

    public void setIp(long ip) {
        this.ip = ip;
    }

    public long getSequence() {
        return sequence;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getType() {
        return type;
    }

    public void putStr(CharSequence value) {
        int len = value.length();
        ensureCapacity(len * 2 + 4);
        Unsafe.getUnsafe().putInt(appendPtr, len);
        Chars.copyStrChars(value, 0, len, appendPtr + 4);
        appendPtr += len * 2L + 4;
    }

    public void putLong(byte c) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(appendPtr++, c);
    }

    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(appendPtr, value);
        appendPtr += 4;
    }

    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(appendPtr, value);
        appendPtr += 2;
    }

    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(appendPtr, value);
        appendPtr += 8;
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

    private void ensureCapacity(int byteCount) {
        if (appendPtr + byteCount - 1 >= appendLim) {
            resize(Math.max(dataSize * 2, (appendPtr - data) + byteCount));
        }
    }
}
