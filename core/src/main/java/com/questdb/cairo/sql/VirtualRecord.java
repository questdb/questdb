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

package com.questdb.cairo.sql;

import com.questdb.std.BinarySequence;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;

public class VirtualRecord implements Record {
    private final ObjList<? extends Function> functions;
    private Record base;

    public VirtualRecord(ObjList<? extends Function> functions) {
        this.functions = functions;
    }

    public Record getBaseRecord() {
        return base;
    }

    @Override
    public BinarySequence getBin(int col) {
        return getFunction(col).getBin(base);
    }

    @Override
    public long getBinLen(int col) {
        return getFunction(col).getBinLen(base);
    }

    @Override
    public boolean getBool(int col) {
        return getFunction(col).getBool(base);
    }

    @Override
    public byte getByte(int col) {
        return getFunction(col).getByte(base);
    }

    @Override
    public long getDate(int col) {
        return getFunction(col).getDate(base);
    }

    @Override
    public double getDouble(int col) {
        return getFunction(col).getDouble(base);
    }

    @Override
    public float getFloat(int col) {
        return getFunction(col).getFloat(base);
    }

    @Override
    public int getInt(int col) {
        return getFunction(col).getInt(base);
    }

    @Override
    public long getLong(int col) {
        return getFunction(col).getLong(base);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return getFunction(col).getShort(base);
    }

    @Override
    public char getChar(int col) {
        return getFunction(col).getChar(base);
    }

    @Override
    public CharSequence getStr(int col) {
        return getFunction(col).getStr(base);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        getFunction(col).getStr(base, sink);
    }

    @Override
    public CharSequence getStrB(int col) {
        return getFunction(col).getStrB(base);
    }

    @Override
    public int getStrLen(int col) {
        return getFunction(col).getStrLen(base);
    }

    @Override
    public CharSequence getSym(int col) {
        return getFunction(col).getSymbol(base);
    }

    @Override
    public long getTimestamp(int col) {
        return getFunction(col).getTimestamp(base);
    }

    public ObjList<? extends Function> getFunctions() {
        return functions;
    }

    public void of(Record record) {
        this.base = record;
    }

    private Function getFunction(int columnIndex) {
        return functions.getQuick(columnIndex);
    }
}
