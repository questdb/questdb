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
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.OutputStream;

abstract class AbstractVirtualColumn implements VirtualColumn {
    private final ColumnType type;
    private String name;

    AbstractVirtualColumn(ColumnType type) {
        this.type = type;
    }

    @Override
    public byte get(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public void getBin(Record rec, OutputStream s) {
        throw new NotImplementedException();
    }

    @Override
    public DirectInputStream getBin(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public boolean getBool(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public long getDate(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public double getDouble(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public float getFloat(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public int getInt(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public long getLong(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public short getShort(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public CharSequence getStr(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        throw new NotImplementedException();
    }

    @Override
    public String getSym(Record rec) {
        throw new NotImplementedException();
    }

    @Override
    public ColumnType getType() {
        return type;
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new NotImplementedException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
}
