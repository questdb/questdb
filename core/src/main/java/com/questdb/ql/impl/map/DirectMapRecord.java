/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.ql.impl.map;

import com.questdb.ql.AbstractRecord;
import com.questdb.ql.StorageFacade;
import com.questdb.std.DirectInputStream;
import com.questdb.std.str.CharSink;

import java.io.OutputStream;

public class DirectMapRecord extends AbstractRecord {
    private final StorageFacade storageFacade;
    private DirectMapEntry entry;

    public DirectMapRecord(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    @Override
    public byte get(int col) {
        return entry.get(col);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int col) {
        return entry.getBool(col);
    }

    @Override
    public long getDate(int col) {
        return entry.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        return entry.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return entry.getFloat(col);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return entry.getFlyweightStr(col);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return entry.getFlyweightStrB(col);
    }

    @Override
    public int getInt(int col) {
        return entry.getInt(col);
    }

    @Override
    public long getLong(int col) {
        return entry.getLong(col);
    }

    @Override
    public long getRowId() {
        return entry.getRowId();
    }

    @Override
    public short getShort(int col) {
        return entry.getShort(col);
    }

    @Override
    public CharSequence getStr(int col) {
        return entry.getStr(col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        entry.getStr(col, sink);
    }

    @Override
    public int getStrLen(int col) {
        return entry.getStrLen(col);
    }

    @Override
    public String getSym(int col) {
        return storageFacade.getSymbolTable(col).value(entry.getInt(col));
    }

    public DirectMapRecord of(DirectMapEntry entry) {
        this.entry = entry;
        return this;
    }
}
