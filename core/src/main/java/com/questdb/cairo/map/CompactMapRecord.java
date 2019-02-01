/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.TableUtils;
import com.questdb.cairo.VirtualMemory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.BinarySequence;
import com.questdb.std.IntList;
import com.questdb.std.Unsafe;

class CompactMapRecord implements MapRecord {

    private final VirtualMemory entries;
    private final long[] columnOffsets;
    private final CompactMapValue value;
    private long offset;
    private RecordCursor symbolTableResolver;
    private IntList symbolTableIndex;

    public CompactMapRecord(VirtualMemory entries, long[] columnOffsets, CompactMapValue value) {
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
        return symbolTableResolver.getSymbolTable(symbolTableIndex.getQuick(col)).value(getInt(col));
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
        return offset + Unsafe.arrayGet(columnOffsets, columnIndex);
    }

    long getNextRecordOffset() {
        return entries.getLong(offset + 1) + offset;
    }

    void of(long offset) {
        this.offset = offset;
    }
}
