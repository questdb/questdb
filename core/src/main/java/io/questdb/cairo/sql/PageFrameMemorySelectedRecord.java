/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

/**
 * Same as {@link PageFrameMemorySelectedRecord}, but with column index remapping.
 * <p>
 * Must be initialized with a {@link #init(PageFrameMemory)} call
 * for a given page frame before any use.
 */
public class PageFrameMemorySelectedRecord extends PageFrameMemoryRecord {
    private final IntList columnIndexes;

    public PageFrameMemorySelectedRecord(IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    @Override
    public BinarySequence getBin(int col) {
        return super.getBin(deferenceColumn(col));
    }

    @Override
    public long getBinLen(int col) {
        return super.getBinLen(deferenceColumn(col));
    }

    @Override
    public boolean getBool(int col) {
        return super.getBool(deferenceColumn(col));
    }

    @Override
    public byte getByte(int col) {
        return super.getByte(deferenceColumn(col));
    }

    @Override
    public char getChar(int col) {
        return super.getChar(deferenceColumn(col));
    }

    @Override
    public long getDate(int col) {
        return super.getDate(deferenceColumn(col));
    }

    @Override
    public double getDouble(int col) {
        return super.getDouble(deferenceColumn(col));
    }

    @Override
    public float getFloat(int col) {
        return super.getFloat(deferenceColumn(col));
    }

    @Override
    public byte getGeoByte(int col) {
        return super.getGeoByte(deferenceColumn(col));
    }

    @Override
    public int getGeoInt(int col) {
        return super.getGeoInt(deferenceColumn(col));
    }

    @Override
    public long getGeoLong(int col) {
        return super.getGeoLong(deferenceColumn(col));
    }

    @Override
    public short getGeoShort(int col) {
        return super.getGeoShort(deferenceColumn(col));
    }

    @Override
    public int getIPv4(int col) {
        return super.getIPv4(deferenceColumn(col));
    }

    @Override
    public int getInt(int col) {
        return super.getInt(deferenceColumn(col));
    }

    @Override
    public long getLong(int col) {
        return super.getLong(deferenceColumn(col));
    }

    @Override
    public long getLong128Hi(int col) {
        return super.getLong128Hi(deferenceColumn(col));
    }

    @Override
    public long getLong128Lo(int col) {
        return super.getLong128Lo(deferenceColumn(col));
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        super.getLong256(deferenceColumn(col), sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return super.getLong256A(deferenceColumn(col));
    }

    @Override
    public Long256 getLong256B(int col) {
        return super.getLong256B(deferenceColumn(col));
    }

    @Override
    public Record getRecord(int col) {
        return super.getRecord(deferenceColumn(col));
    }

    @Override
    public short getShort(int col) {
        return super.getShort(deferenceColumn(col));
    }

    @Override
    public CharSequence getStrA(int col) {
        return super.getStrA(deferenceColumn(col));
    }

    @Override
    public CharSequence getStrB(int col) {
        return super.getStrB(deferenceColumn(col));
    }

    @Override
    public int getStrLen(int col) {
        return super.getStrLen(deferenceColumn(col));
    }

    @Override
    public CharSequence getSymA(int col) {
        return super.getSymA(deferenceColumn(col));
    }

    @Override
    public CharSequence getSymB(int col) {
        return super.getSymB(deferenceColumn(col));
    }

    @Override
    public long getTimestamp(int col) {
        return super.getLong(deferenceColumn(col));
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return super.getVarcharA(deferenceColumn(col));
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return super.getVarcharB(deferenceColumn(col));
    }

    @Override
    public int getVarcharSize(int col) {
        return super.getVarcharSize(deferenceColumn(col));
    }

    private int deferenceColumn(int columnIndex) {
        return columnIndexes.getQuick(columnIndex);
    }
}
