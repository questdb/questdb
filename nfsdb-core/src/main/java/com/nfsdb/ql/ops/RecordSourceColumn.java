/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.ql.ops;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

import java.io.OutputStream;

public class RecordSourceColumn extends AbstractVirtualColumn {
    private final int index;
    private SymbolTable symbolTable;

    public RecordSourceColumn(int index, ColumnType type) {
        super(type);
        this.index = index;
    }

    @Override
    public byte get(Record rec) {
        return rec.get(index);
    }

    @Override
    public void getBin(Record rec, OutputStream s) {
        rec.getBin(index, s);
    }

    @Override
    public DirectInputStream getBin(Record rec) {
        return rec.getBin(index);
    }

    @Override
    public boolean getBool(Record rec) {
        return rec.getBool(index);
    }

    @Override
    public long getDate(Record rec) {
        return rec.getDate(index);
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(index);
    }

    @Override
    public float getFloat(Record rec) {
        return rec.getFloat(index);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        switch (getType()) {
            case SYMBOL:
                return rec.getSym(index);
            default:
                return rec.getFlyweightStr(index);
        }
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(index);
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(index);
    }

    @Override
    public short getShort(Record rec) {
        return rec.getShort(index);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return rec.getStr(index);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        rec.getStr(index, sink);
    }

    @Override
    public String getSym(Record rec) {
        return rec.getSym(index);
    }

    @Override
    public SymbolTable getSymbolTable() {
        return symbolTable;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(SymFacade facade) {
        if (getType() == ColumnType.SYMBOL) {
            this.symbolTable = facade.getSymbolTable(index);
        }
    }
}
