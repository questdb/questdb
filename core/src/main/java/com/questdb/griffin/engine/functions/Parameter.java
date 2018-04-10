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

package com.questdb.griffin.engine.functions;

import com.questdb.cairo.sql.Record;
import com.questdb.griffin.Function;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public class Parameter implements Function {
    private final int position;
    private String name;

    public Parameter(int position) {
        this.position = position;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return null;
    }

    @Override
    public boolean getBool(Record rec) {
        return false;
    }

    @Override
    public byte getByte(Record rec) {
        return 0;
    }

    @Override
    public long getDate(Record rec) {
        return 0;
    }

    @Override
    public double getDouble(Record rec) {
        return 0;
    }

    @Override
    public float getFloat(Record rec) {
        return 0;
    }

    @Override
    public int getInt(Record rec) {
        return 0;
    }

    @Override
    public long getLong(Record rec) {
        return 0;
    }

    @Override
    public short getShort(Record rec) {
        return 0;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return null;
    }

    @Override
    public void getStr(Record rec, CharSink sink) {

    }

    @Override
    public CharSequence getStrB(Record rec) {
        return null;
    }

    @Override
    public int getStrLen(Record rec) {
        return 0;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public long getTimestamp(Record rec) {
        return 0;
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return null;
    }

    public void setName(String name) {
        this.name = name;
    }
}
