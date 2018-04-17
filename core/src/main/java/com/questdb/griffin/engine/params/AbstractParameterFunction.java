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

package com.questdb.griffin.engine.params;


import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.griffin.Function;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public abstract class AbstractParameterFunction implements Function {
    private final int position;

    public AbstractParameterFunction(int position) {
        this.position = position;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        throw error(ColumnType.BINARY);
    }

    @Override
    public boolean getBool(Record rec) {
        throw error(ColumnType.BOOLEAN);
    }

    @Override
    public byte getByte(Record rec) {
        throw error(ColumnType.BYTE);
    }

    @Override
    public long getDate(Record rec) {
        throw error(ColumnType.DATE);
    }

    @Override
    public double getDouble(Record rec) {
        throw error(ColumnType.DOUBLE);
    }

    @Override
    public float getFloat(Record rec) {
        throw error(ColumnType.FLOAT);
    }

    @Override
    public int getInt(Record rec) {
        throw error(ColumnType.INT);
    }

    @Override
    public long getLong(Record rec) {
        throw error(ColumnType.LONG);
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public short getShort(Record rec) {
        throw error(ColumnType.SHORT);
    }

    @Override
    public CharSequence getStr(Record rec) {
        throw error(ColumnType.STRING);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        throw error(ColumnType.STRING);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        throw error(ColumnType.STRING);
    }

    @Override
    public int getStrLen(Record rec) {
        throw error(ColumnType.STRING);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        throw error(ColumnType.SYMBOL);
    }

    @Override
    public long getTimestamp(Record rec) {
        throw error(ColumnType.TIMESTAMP);
    }

    private ParameterException error(int requiredType) {
        return ParameterException.position(position).put("invalid parameter type [required=").put(ColumnType.nameOf(requiredType)).put(", actual=").put(ColumnType.nameOf(getType())).put(']');
    }
}
