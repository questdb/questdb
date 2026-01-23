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

package io.questdb.tasks;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.std.Chars;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public class TableWriterTask implements Closeable {
    public static final int CMD_ALTER_TABLE = 2;
    public static final int CMD_UNUSED = 1;
    public static final int CMD_UPDATE_TABLE = 3;

    public static final int TSK_BEGIN = 64;
    public static final int TSK_COMPLETE = 65;

    private final long data;
    private final long dataSize;
    private long appendLim;
    private long appendPtr;
    private AsyncWriterCommand cmd;
    private long instance;
    private long ip;
    private long tableId;
    private TableToken tableToken;
    private int type;

    public TableWriterTask(long data, long dataSize) {
        this.data = data;
        this.dataSize = dataSize;
        reset();
    }

    public static String getCommandName(int cmd) {
        return switch (cmd) {
            case CMD_ALTER_TABLE -> "ALTER TABLE";
            case CMD_UPDATE_TABLE -> "UPDATE TABLE";
            default -> "UNKNOWN COMMAND";
        };
    }

    @Override
    public void close() {
        appendPtr = 0;
        appendLim = 0;
    }

    public AsyncWriterCommand getAsyncWriterCommand() {
        return cmd;
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

    public long getIp() {
        return ip;
    }

    public long getTableId() {
        return tableId;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public int getType() {
        return type;
    }

    public void of(
            int type,
            long tableId,
            TableToken tableToken
    ) {
        this.tableId = tableId;
        this.tableToken = tableToken;
        this.type = type;
        this.appendPtr = data;
        this.ip = 0L;
    }

    public void putByte(byte c) {
        checkCapacity(Byte.BYTES);
        Unsafe.getUnsafe().putByte(appendPtr++, c);
    }

    public void putInt(int value) {
        checkCapacity(Integer.BYTES);
        Unsafe.getUnsafe().putInt(appendPtr, value);
        appendPtr += Integer.BYTES;
    }

    public void putLong(long value) {
        checkCapacity(Long.BYTES);
        Unsafe.getUnsafe().putLong(appendPtr, value);
        appendPtr += Long.BYTES;
    }

    public void putShort(short value) {
        checkCapacity(Short.BYTES);
        Unsafe.getUnsafe().putShort(appendPtr, value);
        appendPtr += Short.BYTES;
    }

    public void putStr(CharSequence value) {
        int len = value.length();
        final int byteLen = len * 2 + Integer.BYTES;
        checkCapacity(byteLen);
        Unsafe.getUnsafe().putInt(appendPtr, len);
        Chars.copyStrChars(value, 0, len, appendPtr + Integer.BYTES);
        appendPtr += byteLen;
    }

    public void setAsyncWriterCommand(AsyncWriterCommand cmd) {
        this.cmd = cmd;
    }

    public void setInstance(long instance) {
        this.instance = instance;
    }

    public void setIp(long ip) {
        this.ip = ip;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    private void checkCapacity(long byteCount) {
        if (appendPtr + byteCount > appendLim) {
            throw CairoException.critical(0).put("async command/event queue buffer overflow");
        }
    }

    private void reset() {
        appendPtr = data;
        appendLim = data + dataSize;
    }
}
