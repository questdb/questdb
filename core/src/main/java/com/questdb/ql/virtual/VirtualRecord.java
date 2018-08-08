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

package com.questdb.ql.virtual;

import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

import java.io.OutputStream;

class VirtualRecord implements Record {
    private final int split;
    private final ObjList<VirtualColumn> virtualColumns;
    private final Record base;

    VirtualRecord(int split, ObjList<VirtualColumn> virtualColumns, Record base) {
        this.split = split;
        this.virtualColumns = virtualColumns;
        this.base = base;
    }

    @Override
    public byte getByte(int col) {
        return col < split ? base.getByte(col) : getVc(col).get(base);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        if (col < split) {
            base.getBin(col, s);
        } else {
            getVc(col).getBin(base, s);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        return col < split ? base.getBin(col) : getVc(col).getBin(base);
    }

    @Override
    public long getBinLen(int col) {
        return col < split ? base.getBinLen(col) : getVc(col).getBinLen(base);
    }

    @Override
    public boolean getBool(int col) {
        return col < split ? base.getBool(col) : getVc(col).getBool(base);
    }

    @Override
    public long getDate(int col) {
        return col < split ? base.getDate(col) : getVc(col).getDate(base);
    }

    @Override
    public double getDouble(int col) {
        return col < split ? base.getDouble(col) : getVc(col).getDouble(base);
    }

    @Override
    public float getFloat(int col) {
        return col < split ? base.getFloat(col) : getVc(col).getFloat(base);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return col < split ? base.getFlyweightStr(col) : getVc(col).getFlyweightStr(base);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return col < split ? base.getFlyweightStrB(col) : getVc(col).getFlyweightStrB(base);
    }

    @Override
    public int getInt(int col) {
        return col < split ? base.getInt(col) : getVc(col).getInt(base);
    }

    @Override
    public long getLong(int col) {
        return col < split ? base.getLong(col) : getVc(col).getLong(base);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return col < split ? base.getShort(col) : getVc(col).getShort(base);
    }

    @Override
    public int getStrLen(int col) {
        return col < split ? base.getStrLen(col) : getVc(col).getStrLen(base);
    }

    @Override
    public CharSequence getSym(int col) {
        return col < split ? base.getSym(col) : getVc(col).getSym(base);
    }

    public Record getBase() {
        return base;
    }

    public void prepare(StorageFacade facade) {
        for (int i = 0, n = virtualColumns.size(); i < n; i++) {
            virtualColumns.getQuick(i).prepare(facade);
        }
    }

    VirtualRecord copy(Record base) {
        return new VirtualRecord(split, virtualColumns, base);
    }

    private VirtualColumn getVc(int col) {
        return virtualColumns.getQuick(col - split);
    }
}
