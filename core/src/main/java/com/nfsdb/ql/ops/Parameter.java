/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.ops;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.std.CharSequenceObjHashMap;
import com.nfsdb.store.ColumnType;

public class Parameter extends AbstractVirtualColumn {

    private ColumnType valueType = ColumnType.PARAMETER;
    private long longValue;
    private double doubleValue;
    private String stringValue;

    private Parameter() {
        super(ColumnType.PARAMETER);
    }

    public static VirtualColumn getOrCreate(ExprNode node, CharSequenceObjHashMap<Parameter> parameterMap) {
        Parameter p = parameterMap.get(node.token);
        if (p == null) {
            parameterMap.put(node.token, p = new Parameter());
            p.setName(node.token);
        }
        return p;
    }

    @Override
    public byte get(Record rec) {
        switch (valueType) {
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return (byte) longValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'byte' got " + valueType);
        }
    }

    @Override
    public double getDouble(Record rec) {
        switch (valueType) {
            case DOUBLE:
            case FLOAT:
                return doubleValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'double' got " + valueType);
        }
    }

    @Override
    public float getFloat(Record rec) {
        switch (valueType) {
            case DOUBLE:
            case FLOAT:
                return (float) doubleValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'float' got " + valueType);
        }
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        switch (valueType) {
            case STRING:
                return stringValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'string' got " + valueType);
        }
    }

    @Override
    public int getInt(Record rec) {
        switch (valueType) {
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return (int) longValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'int' got " + valueType);
        }
    }

    @Override
    public long getLong(Record rec) {
        switch (valueType) {
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return longValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'long' got " + valueType);
        }
    }

    @Override
    public short getShort(Record rec) {
        switch (valueType) {
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return (short) longValue;
            default:
                throw new JournalRuntimeException("Parameter " + getName() + " is of incorrect type. Expected 'short' got " + valueType);
        }
    }

    @Override
    public CharSequence getStr(Record rec) {
        return getFlyweightStr(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(getFlyweightStr(rec));
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public void prepare(StorageFacade facade) {
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
}
