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

package com.nfsdb.journal.lang.cst;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.column.FixedColumn;

import java.io.InputStream;
import java.io.OutputStream;

public class JournalEntry extends AbstractDataRow {
    public Partition<Object> partition;
    public long rowid;
    public JournalEntry slave;

    @Override
    public int getColumnIndex(String column) {
        return partition.getJournal().getMetadata().getColumnIndex(column);
    }

    @Override
    public byte get(int col) {
        return ((FixedColumn) partition.getAbstractColumn(col)).getByte(rowid);
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
    public long getDate(int col) {
        return partition.getLong(rowid, col);
    }

    @Override
    public double getDouble(int col) {
        return partition.getDouble(rowid, col);
    }

    @Override
    public String getStr(int col) {
        return partition.getStr(rowid, col);
    }

    @Override
    public String getSym(int col) {
        return partition.getSym(rowid, col);
    }

    @Override
    public boolean getBool(int col) {
        return partition.getBoolean(rowid, col);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        partition.getBin(rowid, col, s);
    }

    @Override
    public short getShort(int col) {
        return partition.getShort(rowid, col);
    }

    @Override
    public InputStream getBin(int col) {
        return partition.getBin(rowid, col);
    }

    @Override
    public DataRow getSlave() {
        return slave;
    }

    @Override
    public int getColumnCount() {
        return partition.getJournal().getMetadata().getColumnCount();
    }

    @Override
    public ColumnType getColumnTypeInternal(int column) {
        return partition.getJournal().getMetadata().getColumnMetadata(column).type;
    }

    @Override
    public String toString() {
        return "DataItem{" +
                "partition=" + partition +
                ", rowid=" + rowid +
                ", slave=" + slave +
                '}';
    }
}
