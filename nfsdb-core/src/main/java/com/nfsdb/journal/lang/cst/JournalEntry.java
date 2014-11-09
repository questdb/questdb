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

package com.nfsdb.journal.lang.cst;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.FixedColumn;

import java.io.OutputStream;

public class JournalEntry {
    public Partition<Object> partition;
    public long rowid;
    public JournalEntry slave;

    public int getColumnIndex(String column) {
        return partition.getJournal().getMetadata().getColumnIndex(column);
    }

    public byte get(String column) {
        return get(getColumnIndex(column));
    }

    public byte get(int col) {
        return ((FixedColumn) partition.getAbstractColumn(col)).getByte(rowid);
    }

    public int getInt(String column) {
        return getInt(getColumnIndex(column));
    }

    public int getInt(int col) {
        return partition.getInt(rowid, col);
    }

    public long getLong(String column) {
        return getLong(getColumnIndex(column));
    }

    public long getLong(int col) {
        return partition.getLong(rowid, col);
    }

    public long getDate(int col) {
        return partition.getLong(rowid, col);
    }

    public double getDouble(String column) {
        return getDouble(getColumnIndex(column));
    }

    public double getDouble(int col) {
        return partition.getDouble(rowid, col);
    }

    public String getStr(String column) {
        return getStr(getColumnIndex(column));
    }

    public String getStr(int col) {
        return partition.getStr(rowid, col);
    }

    public String getSym(String column) {
        return getSym(getColumnIndex(column));
    }

    public String getSym(int col) {
        return partition.getSym(rowid, col);
    }

    public boolean getBool(String column) {
        return getBool(getColumnIndex(column));
    }

    public boolean getBool(int col) {
        return partition.getBoolean(rowid, col);
    }

    public void getBin(int col, OutputStream s) {
        partition.getBin(rowid, col, s);
    }

    public short getShort(int col) {
        return partition.getShort(rowid, col);
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
