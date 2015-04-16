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

package com.nfsdb.ql.impl;

import com.nfsdb.Partition;
import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.storage.FixedColumn;

import java.io.OutputStream;

public class JournalRecord extends AbstractRecord {
    public Partition partition;
    public long rowid;

    public JournalRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        return ((FixedColumn) partition.getAbstractColumn(col)).getByte(rowid);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        partition.getBin(rowid, col, s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return partition.getBin(rowid, col);
    }

    @Override
    public boolean getBool(int col) {
        return partition.getBoolean(rowid, col);
    }

    @Override
    public long getDate(int col) {
        return partition.getLong(rowid, col);
    }

    @Override
    public double getDouble(int col) {
        return partition.getDouble(rowid, col);
    }

    @Override
    public float getFloat(int col) {
        return partition.getFloat(rowid, col);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return partition.getFlyweightStr(rowid, col);
    }

    @Override
    public int getInt(int col) {
        return partition.getInt(rowid, col);
    }

    @Override
    public long getLong(int col) {
        return partition.getLong(rowid, col);
    }

    @Override
    public long getRowId() {
        return rowid;
    }

    @Override
    public short getShort(int col) {
        return partition.getShort(rowid, col);
    }

    @Override
    public String getStr(int col) {
        return partition.getStr(rowid, col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        partition.getStr(rowid, col, sink);
    }

    @Override
    public String getSym(int col) {
        return partition.getSym(rowid, col);
    }
    
    @Override
    public String toString() {
        return "DataItem{" +
                "partition=" + partition +
                ", rowid=" + rowid +
                '}';
    }
}
