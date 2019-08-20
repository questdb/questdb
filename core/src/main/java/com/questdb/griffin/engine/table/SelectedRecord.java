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

package com.questdb.griffin.engine.table;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.IntList;
import com.questdb.std.str.CharSink;

class SelectedRecord implements Record {
    private final IntList columnCrossIndex;
    private Record base;

    public SelectedRecord(IntList columnCrossIndex) {
        this.columnCrossIndex = columnCrossIndex;
    }

    @Override
    public BinarySequence getBin(int col) {
        return base.getBin(getColumnIndex(col));
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(getColumnIndex(col));
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(getColumnIndex(col));
    }

    @Override
    public byte getByte(int col) {
        return base.getByte(getColumnIndex(col));
    }

    @Override
    public long getDate(int col) {
        return base.getDate(getColumnIndex(col));
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(getColumnIndex(col));
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(getColumnIndex(col));
    }

    @Override
    public int getInt(int col) {
        return base.getInt(getColumnIndex(col));
    }

    @Override
    public long getLong(int col) {
        return base.getLong(getColumnIndex(col));
    }

    @Override
    public char getChar(int col) {
        return base.getChar(getColumnIndex(col));
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return base.getShort(getColumnIndex(col));
    }

    @Override
    public CharSequence getStr(int col) {
        return base.getStr(getColumnIndex(col));
    }

    @Override
    public void getStr(int col, CharSink sink) {
        base.getStr(getColumnIndex(col), sink);
    }

    @Override
    public CharSequence getStrB(int col) {
        return base.getStrB(getColumnIndex(col));
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(getColumnIndex(col));
    }

    @Override
    public CharSequence getSym(int col) {
        return base.getSym(getColumnIndex(col));
    }

    @Override
    public long getTimestamp(int col) {
        return base.getTimestamp(getColumnIndex(col));
    }

    Record getBaseRecord() {
        return base;
    }

    IntList getColumnCrossIndex() {
        return columnCrossIndex;
    }

    private int getColumnIndex(int columnIndex) {
        return columnCrossIndex.getQuick(columnIndex);
    }

    void of(Record record) {
        this.base = record;
    }
}
