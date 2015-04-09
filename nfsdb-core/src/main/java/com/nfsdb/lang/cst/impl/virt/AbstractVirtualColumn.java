/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.virt;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSourceState;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.OutputStream;

public abstract class AbstractVirtualColumn implements VirtualColumn {
    protected RecordSourceState state;
    private String name;
    private ColumnType type;

    public AbstractVirtualColumn(ColumnType type) {
        this.type = type;
    }

    @Override
    public void configure(RecordMetadata metadata, RecordSourceState state) {
        this.state = state;
    }

    @Override
    public byte get() {
        throw new NotImplementedException();
    }

    @Override
    public void getBin(OutputStream s) {
        throw new NotImplementedException();
    }

    @Override
    public DirectInputStream getBin() {
        throw new NotImplementedException();
    }

    @Override
    public boolean getBool() {
        throw new NotImplementedException();
    }

    @Override
    public long getDate() {
        throw new NotImplementedException();
    }

    @Override
    public double getDouble() {
        throw new NotImplementedException();
    }

    @Override
    public float getFloat() {
        throw new NotImplementedException();
    }

    @Override
    public CharSequence getFlyweightStr() {
        throw new NotImplementedException();
    }

    @Override
    public int getInt() {
        throw new NotImplementedException();
    }

    @Override
    public long getLong() {
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

    @Override
    public short getShort() {
        throw new NotImplementedException();
    }

    @Override
    public CharSequence getStr() {
        throw new NotImplementedException();
    }

    @Override
    public void getStr(CharSink sink) {
        throw new NotImplementedException();
    }

    @Override
    public String getSym() {
        throw new NotImplementedException();
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new NotImplementedException();
    }

    @Override
    public ColumnType getType() {
        return type;
    }

    protected void setType(ColumnType type) {
        this.type = type;
    }
}
