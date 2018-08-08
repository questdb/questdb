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

package com.questdb.ql.select;

import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import com.questdb.std.Unsafe;
import com.questdb.store.Record;
import com.questdb.store.RecordMetadata;

import java.io.OutputStream;

public class SelectedColumnsRecord implements Record {
    private final int reindex[];
    private Record base;

    protected SelectedColumnsRecord(int[] reindex) {
        this.reindex = reindex;
    }

    public SelectedColumnsRecord(RecordMetadata metadata, @Transient ObjList<CharSequence> names) {
        int k = names.size();
        this.reindex = new int[k];

        for (int i = 0; i < k; i++) {
            reindex[i] = metadata.getColumnIndex(names.getQuick(i));
        }
    }

    public SelectedColumnsRecord copy() {
        return new SelectedColumnsRecord(reindex);
    }

    @Override
    public byte getByte(int col) {
        return base.getByte(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public void getBin(int col, OutputStream s) {
        base.getBin(Unsafe.arrayGet(reindex, col), s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return base.getBin(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public long getDate(int col) {
        return base.getDate(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return base.getFlyweightStr(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return base.getFlyweightStrB(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public int getInt(int col) {
        return base.getInt(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public long getLong(int col) {
        return base.getLong(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return base.getShort(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(Unsafe.arrayGet(reindex, col));
    }

    @Override
    public CharSequence getSym(int col) {
        return base.getSym(Unsafe.arrayGet(reindex, col));
    }

    public Record getBase() {
        return base;
    }

    public SelectedColumnsRecord of(Record base) {
        this.base = base;
        return this;
    }
}
