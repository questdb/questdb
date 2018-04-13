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
import com.questdb.common.ColumnType;
import com.questdb.griffin.Function;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public class Parameter implements Function {
    private final int position;
    private Function var;

    public Parameter(int position) {
        this.position = position;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return var.getBin(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return var.getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        return var.getByte(rec);
    }

    @Override
    public long getDate(Record rec) {
        return var.getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return var.getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return var.getFloat(rec);
    }

    @Override
    public int getInt(Record rec) {
        return var.getInt(rec);
    }

    public int getValueType() {
        if (var != null) {
            return var.getType();
        }
        return getType();
    }

    public void setInt(int value) {
        if (var instanceof IntVar) {
            ((IntVar) var).value = value;
        } else {
            var = new IntVar(position, value);
        }
    }

    public void setLong(long value) {
        if (var instanceof LongVar) {
            ((LongVar) var).value = value;
        } else {
            var = new LongVar(position, value);
        }

    }

    private static class IntVar extends IntFunction {
        private int value;

        public IntVar(int position, int value) {
            super(position);
            this.value = value;
        }

        @Override
        public int getInt(Record rec) {
            return value;
        }
    }

    private static class LongVar extends LongFunction {
        private long value;


        public LongVar(int position, long value) {
            super(position);
            this.value = value;
        }

        @Override
        public long getLong(Record rec) {
            return value;
        }
    }

    @Override
    public long getLong(Record rec) {
        return var.getLong(rec);
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public short getShort(Record rec) {
        return var.getShort(rec);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return var.getStr(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        var.getStr(rec, sink);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return var.getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        return var.getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return var.getSymbol(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return var.getTimestamp(rec);
    }

    @Override
    public int getType() {
        return ColumnType.PARAMETER;
    }

    @Override
    public boolean isConstant() {
        return false;
    }


}
