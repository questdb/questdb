/*
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
 */

package com.nfsdb.ql.impl;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;

import java.io.OutputStream;

public abstract class AbstractRecord implements Record {

    protected final RecordMetadata metadata;

    protected AbstractRecord(RecordMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public byte get(String column) {
        return get(metadata.getColumnIndex(column));
    }

    @Override
    public void getBin(String column, OutputStream s) {
        getBin(metadata.getColumnIndex(column), s);
    }

    @Override
    public DirectInputStream getBin(String column) {
        return getBin(metadata.getColumnIndex(column));
    }

    @Override
    public boolean getBool(String column) {
        return getBool(metadata.getColumnIndex(column));
    }

    @Override
    public double getDouble(String column) {
        return getDouble(metadata.getColumnIndex(column));
    }

    @Override
    public float getFloat(String column) {
        return getFloat(metadata.getColumnIndex(column));
    }

    @Override
    public CharSequence getFlyweightStr(String column) {
        return getFlyweightStr(metadata.getColumnIndex(column));
    }

    @Override
    public int getInt(String column) {
        return getInt(metadata.getColumnIndex(column));
    }

    @Override
    public long getLong(String column) {
        return getLong(metadata.getColumnIndex(column));
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public CharSequence getStr(String column) {
        return getStr(metadata.getColumnIndex(column));
    }

    @Override
    public String getSym(String column) {
        return getSym(metadata.getColumnIndex(column));
    }
}
