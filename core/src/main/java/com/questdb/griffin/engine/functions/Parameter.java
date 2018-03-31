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

import com.questdb.common.ColumnType;
import com.questdb.common.Record;
import com.questdb.ex.UndefinedParameterException;
import com.questdb.ex.WrongParameterTypeException;

public class Parameter extends AbstractFunction {

    private int valueType = ColumnType.PARAMETER;
    private long longValue;
    private double doubleValue;
    private String stringValue;
    private String name;

    public Parameter(int position) {
        super(ColumnType.PARAMETER, position);
    }

    @Override
    public byte get(Record rec) {
        switch (valueType) {
            case ColumnType.LONG:
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return (byte) longValue;
            default:
                throw wrongType(ColumnType.BYTE);
        }
    }

    @Override
    public double getDouble(Record rec) {
        switch (valueType) {
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
                return doubleValue;
            default:
                throw wrongType(ColumnType.DOUBLE);
        }
    }

    @Override
    public float getFloat(Record rec) {
        switch (valueType) {
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
                return (float) doubleValue;
            default:
                throw wrongType(ColumnType.DOUBLE);
        }
    }

    @Override
    public int getInt(Record rec) {
        switch (valueType) {
            case ColumnType.LONG:
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return (int) longValue;
            default:
                throw wrongType(ColumnType.INT);
        }
    }

    @Override
    public long getLong(Record rec) {
        switch (valueType) {
            case ColumnType.LONG:
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return longValue;
            default:
                throw wrongType(ColumnType.LONG);
        }
    }

    @Override
    public short getShort(Record rec) {
        switch (valueType) {
            case ColumnType.LONG:
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return (short) longValue;
            default:
                throw wrongType(ColumnType.SHORT);
        }
    }

    @Override
    public CharSequence getStr(Record rec) {
        switch (valueType) {
            case ColumnType.STRING:
                return stringValue;
            default:
                throw wrongType(ColumnType.STRING);
        }
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public void set(int value) {
        valueType = ColumnType.INT;
        longValue = value;
    }

    public void set(short value) {
        valueType = ColumnType.SHORT;
        longValue = value;
    }

    public void set(byte value) {
        valueType = ColumnType.BYTE;
        longValue = value;
    }

    public void set(double value) {
        valueType = ColumnType.DOUBLE;
        doubleValue = value;
    }

    public void set(float value) {
        valueType = ColumnType.FLOAT;
        doubleValue = value;
    }

    public void set(String value) {
        valueType = ColumnType.STRING;
        stringValue = value;
    }

    public void set(long value) {
        valueType = ColumnType.LONG;
        longValue = value;
    }

    public void setDate(long value) {
        valueType = ColumnType.DATE;
        longValue = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    private RuntimeException wrongType(int expected) {
        if (valueType == ColumnType.PARAMETER) {
            return new UndefinedParameterException(name);
        } else {
            return new WrongParameterTypeException(name, expected, valueType);
        }
    }
}
