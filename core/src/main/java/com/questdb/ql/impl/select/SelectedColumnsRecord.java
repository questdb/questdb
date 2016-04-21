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

package com.questdb.ql.impl.select;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
import com.questdb.ql.AbstractRecord;
import com.questdb.ql.Record;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;

import java.io.OutputStream;

public class SelectedColumnsRecord extends AbstractRecord {
    private final int reindex[];
    private Record base;

    public SelectedColumnsRecord(RecordMetadata metadata, @Transient ObjList<CharSequence> names) {
        super(metadata);
        int k = names.size();
        this.reindex = new int[k];

        for (int i = 0; i < k; i++) {
            reindex[i] = metadata.getColumnIndex(names.getQuick(i));
        }
    }

    @Override
    public byte get(int col) {
        return base.get(reindex[col]);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        base.getBin(reindex[col], s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return base.getBin(reindex[col]);
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(reindex[col]);
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(reindex[col]);
    }

    @Override
    public long getDate(int col) {
        return base.getDate(reindex[col]);
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(reindex[col]);
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(reindex[col]);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return base.getFlyweightStr(reindex[col]);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return base.getFlyweightStrB(reindex[col]);
    }

    @Override
    public int getInt(int col) {
        return base.getInt(reindex[col]);
    }

    @Override
    public long getLong(int col) {
        return base.getLong(reindex[col]);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return base.getShort(reindex[col]);
    }

    @Override
    public CharSequence getStr(int col) {
        return base.getStr(reindex[col]);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        base.getStr(reindex[col], sink);
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(reindex[col]);
    }

    @Override
    public String getSym(int col) {
        return base.getSym(reindex[col]);
    }

    public SelectedColumnsRecord of(Record base) {
        this.base = base;
        return this;
    }
}
