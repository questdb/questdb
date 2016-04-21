/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.questdb.ql.ops;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.io.sink.CharSink;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.model.ExprNode;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.store.ColumnType;

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
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr(rec);
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
