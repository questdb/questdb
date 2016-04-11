/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.virtual;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.std.ObjList;

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
