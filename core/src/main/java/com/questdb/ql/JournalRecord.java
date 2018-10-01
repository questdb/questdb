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

package com.questdb.ql;

import com.questdb.std.DirectInputStream;
import com.questdb.std.Rows;
import com.questdb.store.Partition;
import com.questdb.store.Record;

import java.io.OutputStream;

public class JournalRecord implements Record {
    public Partition partition;
    public long rowid;
    public int partitionIndex = -1;

    @Override
    public void getBin(int col, OutputStream s) {
        partition.getBin(rowid, col, s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return partition.getBin(rowid, col);
    }

    @Override
    public long getBinLen(int col) {
        return partition.getBinLen(rowid, col);
    }

    @Override
    public boolean getBool(int col) {
        return partition.getBool(rowid, col);
    }

    @Override
    public byte getByte(int col) {
        return partition.fixCol(col).getByte(rowid);
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
    public CharSequence getFlyweightStrB(int col) {
        return partition.getFlyweightStrB(rowid, col);
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
        return Rows.toRowID(partitionIndex, rowid);
    }

    @Override
    public short getShort(int col) {
        return partition.getShort(rowid, col);
    }

    @Override
    public int getStrLen(int col) {
        return partition.getStrLen(rowid, col);
    }

    @Override
    public CharSequence getSym(int col) {
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
