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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

class UnionRecord implements Record {
    private Record base;

    public void of(Record base) {
        this.base = base;
    }

    @Override
    public BinarySequence getBin(int col) {
        return base.getBin(col);
    }

    @Override
    public int getInt(int col) {
        return base.getInt(col);
    }

    @Override
    public long getLong(int col) {
        return base.getLong(col);
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return base.getByte(col);
    }

    @Override
    public long getDate(int col) {
        return base.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(col);
    }

    @Override
    public short getShort(int col) {
        return base.getShort(col);
    }

    @Override
    public char getChar(int col) {
        return base.getChar(col);
    }

    @Override
    public CharSequence getStr(int col) {
        return base.getStr(col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        base.getStr(col, sink);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        base.getLong256(col, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return base.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        return base.getLong256B(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        return base.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(col);
    }

    @Override
    public CharSequence getSym(int col) {
        return base.getSym(col);
    }

    @Override
    public long getTimestamp(int col) {
        return base.getTimestamp(col);
    }
}
