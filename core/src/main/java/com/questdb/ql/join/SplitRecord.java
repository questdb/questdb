/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.join;

import com.questdb.std.DirectInputStream;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.store.Record;

import java.io.OutputStream;

public class SplitRecord implements Record {
    private final ObjList<Record> records = new ObjList<>();
    private final IntList indices = new IntList();

    SplitRecord(int countA, int countB, Record recordA, Record recordB) {
        addRecord(recordA, countA);
        addRecord(recordB, countB);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        getRec(col).getBin(idx(col), s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return getRec(col).getBin(idx(col));
    }

    @Override
    public long getBinLen(int col) {
        return getRec(col).getBinLen(idx(col));
    }

    @Override
    public boolean getBool(int col) {
        return getRec(col).getBool(idx(col));
    }

    @Override
    public byte getByte(int col) {
        return getRec(col).getByte(idx(col));
    }

    @Override
    public long getDate(int col) {
        return getRec(col).getDate(idx(col));
    }

    @Override
    public double getDouble(int col) {
        return getRec(col).getDouble(idx(col));
    }

    @Override
    public float getFloat(int col) {
        return getRec(col).getFloat(idx(col));
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return getRec(col).getFlyweightStr(idx(col));
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return getRec(col).getFlyweightStrB(idx(col));
    }

    @Override
    public int getInt(int col) {
        return getRec(col).getInt(idx(col));
    }

    @Override
    public long getLong(int col) {
        return getRec(col).getLong(idx(col));
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return getRec(col).getShort(idx(col));
    }

    @Override
    public int getStrLen(int col) {
        return getRec(col).getStrLen(idx(col));
    }

    @Override
    public CharSequence getSym(int col) {
        return getRec(col).getSym(idx(col));
    }

    public Record getRec(int col) {
        return records.getQuick(col);
    }

    private void addRecord(Record rec, int count) {
        if (rec instanceof SplitRecord) {
            ObjList<Record> theirRecords = ((SplitRecord) rec).records;
            for (int i = 0, n = theirRecords.size(); i < n; i++) {
                records.add(theirRecords.getQuick(i));
                indices.add(((SplitRecord) rec).indices.getQuick(i));
            }
        } else {
            for (int i = 0; i < count; i++) {
                records.add(rec);
                indices.add(i);
            }
        }
    }

    private int idx(int col) {
        return indices.getQuick(col);
    }
}
