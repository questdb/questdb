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

package io.questdb.cairo.map;

import io.questdb.cairo.ContiguousVirtualMemory;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;

public class CompactMapRecord implements MapRecord {

    private final ContiguousVirtualMemory entries;
    private final long[] columnOffsets;
    private final CompactMapValue value;
    private long offset;
    private RecordCursor symbolTableResolver;
    private IntList symbolTableIndex;

    public CompactMapRecord(ContiguousVirtualMemory entries, long[] columnOffsets, CompactMapValue value) {
        this.entries = entries;
        this.columnOffsets = columnOffsets;
        this.value = value;
        this.value.linkRecord(this);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public CompactMapRecord clone() {
        return new CompactMapRecord(this.entries, this.columnOffsets, new CompactMapValue(this.entries, columnOffsets));
    }

    @Override
    public BinarySequence getBin(int col) {
        long o = getLong(col);
        if (o == -1L) {
            return null;
        }
        return entries.getBin(offset + o);
    }

    @Override
    public long getBinLen(int col) {
        long o = getLong(col);
        if (o == -1L) {
            return -1L;
        }
        return entries.getBinLen(offset + o);
    }

    @Override
    public boolean getBool(int col) {
        return entries.getBool(getColumnOffset(col));
    }

    @Override
    public byte getByte(int col) {
        return entries.getByte(getColumnOffset(col));
    }

    @Override
    public double getDouble(int col) {
        return entries.getDouble(getColumnOffset(col));
    }

    @Override
    public float getFloat(int col) {
        return entries.getFloat(getColumnOffset(col));
    }

    @Override
    public int getInt(int col) {
        return entries.getInt(getColumnOffset(col));
    }

    @Override
    public long getLong(int col) {
        return entries.getLong(getColumnOffset(col));
    }

    @Override
    public long getRowId() {
        return offset;
    }

    @Override
    public short getShort(int col) {
        return entries.getShort(getColumnOffset(col));
    }

    @Override
    public CharSequence getStr(int col) {
        long o = getLong(col);
        if (o == -1L) {
            return null;
        }
        return entries.getStr(offset + o);
    }

    @Override
    public CharSequence getStrB(int col) {
        long o = getLong(col);
        if (o == -1L) {
            return null;
        }
        return entries.getStr2(offset + o);
    }

    @Override
    public int getStrLen(int col) {
        long o = getLong(col);
        if (o == -1L) {
            return TableUtils.NULL_LEN;
        }
        return entries.getStrLen(offset + o);
    }

    @Override
    public CharSequence getSym(int col) {
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(col)).valueOf(getInt(col));
    }

    @Override
    public CharSequence getSymB(int col) {
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(col)).valueBOf(getInt(col));
    }

    @Override
    public MapValue getValue() {
        value.of(offset, false);
        return value;
    }

    @Override
    public void setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex) {
        this.symbolTableResolver = resolver;
        this.symbolTableIndex = symbolTableIndex;
    }

    private long getColumnOffset(int columnIndex) {
        return offset + columnOffsets[columnIndex];
    }

    long getNextRecordOffset() {
        return entries.getLong(offset + 1) + offset;
    }

    void of(long offset) {
        this.offset = offset;
    }
}
