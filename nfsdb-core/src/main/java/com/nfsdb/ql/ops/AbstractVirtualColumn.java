/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

import java.io.OutputStream;

abstract class AbstractVirtualColumn implements VirtualColumn {
    private final ColumnType type;
    private String name;

    AbstractVirtualColumn(ColumnType type) {
        this.type = type;
    }

    @Override
    public byte get(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(Record rec, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBucketCount() {
        return 0;
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
    public SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnType getType() {
        return type;
    }

    @Override
    public boolean isIndexed() {
        return false;
    }
}
