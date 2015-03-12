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

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSourceState;
import com.nfsdb.storage.ColumnType;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

public abstract class AbstractVirtualColumn implements VirtualColumn {
    private final String name;
    private final ColumnType type;
    protected RecordSourceState state;
    protected RecordMetadata metadata;

    public AbstractVirtualColumn(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public void configure(RecordMetadata metadata, RecordSourceState state) {
        this.state = state;
        this.metadata = metadata;
    }

    @Override
    public byte get() {
        throw new NoSuchElementException();
    }

    @Override
    public void getBin(OutputStream s) {
        throw new NoSuchElementException();
    }

    @Override
    public InputStream getBin() {
        throw new NoSuchElementException();
    }

    @Override
    public boolean getBool() {
        throw new NoSuchElementException();
    }

    @Override
    public long getDate() {
        throw new NoSuchElementException();
    }

    @Override
    public double getDouble() {
        throw new NoSuchElementException();
    }

    @Override
    public float getFloat() {
        throw new NoSuchElementException();
    }

    @Override
    public int getInt() {
        throw new NoSuchElementException();
    }

    @Override
    public long getLong() {
        throw new NoSuchElementException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public short getShort() {
        throw new NoSuchElementException();
    }

    @Override
    public CharSequence getStr() {
        throw new NoSuchElementException();
    }

    @Override
    public void getStr(CharSink sink) {
        throw new NoSuchElementException();
    }

    @Override
    public String getSym() {
        throw new NoSuchElementException();
    }

    @Override
    public ColumnType getType() {
        return type;
    }
}
