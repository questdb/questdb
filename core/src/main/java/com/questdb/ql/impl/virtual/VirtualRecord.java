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

package com.questdb.ql.impl.virtual;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
import com.questdb.ql.AbstractRecord;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;

import java.io.OutputStream;

class VirtualRecord extends AbstractRecord {
    private final int split;
    private final ObjList<VirtualColumn> virtualColumns;
    private Record base;

    VirtualRecord(RecordMetadata metadata, int split, ObjList<VirtualColumn> virtualColumns) {
        super(metadata);
        this.split = split;
        this.virtualColumns = virtualColumns;
    }

    @Override
    public byte get(int col) {
        return col < split ? base.get(col) : virtualColumns.get(col - split).get(base);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        if (col < split) {
            base.getBin(col, s);
        } else {
            virtualColumns.get(col - split).getBin(base, s);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        return col < split ? base.getBin(col) : virtualColumns.get(col - split).getBin(base);
    }

    @Override
    public long getBinLen(int col) {
        return col < split ? base.getBinLen(col) : virtualColumns.get(col - split).getBinLen(base);
    }

    @Override
    public boolean getBool(int col) {
        return col < split ? base.getBool(col) : virtualColumns.get(col - split).getBool(base);
    }

    @Override
    public long getDate(int col) {
        return col < split ? base.getDate(col) : virtualColumns.get(col - split).getDate(base);
    }

    @Override
    public double getDouble(int col) {
        return col < split ? base.getDouble(col) : virtualColumns.get(col - split).getDouble(base);
    }

    @Override
    public float getFloat(int col) {
        return col < split ? base.getFloat(col) : virtualColumns.get(col - split).getFloat(base);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return col < split ? base.getFlyweightStr(col) : virtualColumns.get(col - split).getFlyweightStr(base);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return col < split ? base.getFlyweightStrB(col) : virtualColumns.get(col - split).getFlyweightStrB(base);
    }

    @Override
    public int getInt(int col) {
        return col < split ? base.getInt(col) : virtualColumns.get(col - split).getInt(base);
    }

    @Override
    public long getLong(int col) {
        return col < split ? base.getLong(col) : virtualColumns.get(col - split).getLong(base);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return col < split ? base.getShort(col) : virtualColumns.get(col - split).getShort(base);
    }

    @Override
    public CharSequence getStr(int col) {
        return col < split ? base.getStr(col) : virtualColumns.get(col - split).getStr(base);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            base.getStr(col, sink);
        } else {
            virtualColumns.get(col - split).getStr(base, sink);
        }
    }

    @Override
    public int getStrLen(int col) {
        return col < split ? base.getStrLen(col) : virtualColumns.get(col - split).getStrLen(base);
    }

    @Override
    public String getSym(int col) {
        return col < split ? base.getSym(col) : virtualColumns.get(col - split).getSym(base);
    }

    public void prepare(StorageFacade facade) {
        for (int i = 0, n = virtualColumns.size(); i < n; i++) {
            virtualColumns.getQuick(i).prepare(facade);
        }
    }

    public void setBase(Record base) {
        this.base = base;
    }
}
