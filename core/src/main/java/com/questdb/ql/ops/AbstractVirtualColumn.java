/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.DirectInputStream;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.SymbolTable;
import com.questdb.store.VariableColumn;

import java.io.OutputStream;

public abstract class AbstractVirtualColumn implements VirtualColumn {
    private final int columnType;
    private final int position;
    private String name;

    protected AbstractVirtualColumn(int columnType, int position) {
        this.columnType = columnType;
        this.position = position;
    }

    @Override
    public byte get(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(Record rec, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(getFlyweightStr(rec));
    }

    @Override
    public int getStrLen(Record rec) {
        CharSequence cs = getFlyweightStr(rec);
        return cs == null ? VariableColumn.NULL_LEN : cs.length();
    }

    @Override
    public CharSequence getSym(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBucketCount() {
        return 0;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return null;
    }

    @Override
    public int getType() {
        return columnType;
    }

    @Override
    public boolean isIndexed() {
        return false;
    }

    protected static void typeError(int pos, int type) throws ParserException {
        throw QueryError.position(pos).$('\'').$(ColumnType.nameOf(type)).$("' type expected").$();
    }

    protected static void assertConstant(VirtualColumn arg) throws ParserException {
        if (!arg.isConstant()) {
            throw QueryError.$(arg.getPosition(), "Constant expected");
        }
    }
}
