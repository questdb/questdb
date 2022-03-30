/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.griffin.AsyncWriterCommand;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;

public class TableWriterTask implements Closeable {
    public static final int CMD_SLAVE_SYNC = 1;
    public static final int CMD_ALTER_TABLE = 2;
    public static final int CMD_UPDATE_TABLE = 3;

    public static final int TSK_BEGIN = 64;
    public static final int TSK_COMPLETE = 65;

    private AsyncWriterCommand cmd;
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

    public TableWriterTask(long data, long size) {
        this.data = data;
        this.dataSize = size;
        this.appendPtr = data;
        this.appendLim = data + dataSize;
    }

    @Override
    public void close() {
        if (dataSize > 0) {
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
        Vect.memcpy(p + 8, txMem, txMemSize);
        Unsafe.getUnsafe().putLong(p + txMemSize + 8, metaMemSize);
        Vect.memcpy(p + txMemSize + 16, metaMem, metaMemSize);
        this.type = CMD_SLAVE_SYNC;
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
        this.ip = 0L;
    }

    public AsyncWriterCommand getAsyncWriterCommand() {
        return cmd;
    }

    public void setAsyncWriterCommand(AsyncWriterCommand cmd) {
        this.cmd = cmd;
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
        final int byteLen = len * 2 + Integer.BYTES;
        ensureCapacity(byteLen);
        Unsafe.getUnsafe().putInt(appendPtr, len);
        Chars.copyStrChars(value, 0, len, appendPtr + 4);
        this.appendPtr += byteLen;
    }

    public void putByte(byte c) {
        ensureCapacity(Byte.BYTES);
        Unsafe.getUnsafe().putByte(appendPtr++, c);
    }

    public void putInt(int value) {
        ensureCapacity(Integer.BYTES);
        Unsafe.getUnsafe().putInt(appendPtr, value);
        appendPtr += Integer.BYTES;
    }

    public void putShort(short value) {
        ensureCapacity(Short.BYTES);
        Unsafe.getUnsafe().putShort(appendPtr, value);
        appendPtr += Short.BYTES;
    }

    public void putLong(long value) {
        ensureCapacity(Long.BYTES);
        Unsafe.getUnsafe().putLong(appendPtr, value);
        appendPtr += Long.BYTES;
    }

    private void resize(long size) {
        assert dataSize > 0;
        if (size > dataSize) {
            long appendOffset = appendPtr - data;
            data = Unsafe.realloc(data, dataSize, size, MemoryTag.NATIVE_REPL);
            dataSize = size;
            appendPtr = data + appendOffset;
            appendLim = data + dataSize;
        }
    }

    private void ensureCapacity(int byteCount) {
        if (appendPtr + byteCount > appendLim) {
            resize(Math.max(dataSize * 2, (appendPtr - data) + byteCount));
        }
    }
}
