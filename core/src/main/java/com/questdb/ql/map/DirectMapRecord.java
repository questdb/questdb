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

package com.questdb.ql.map;

import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

public class DirectMapRecord implements Record {
    private final StorageFacade storageFacade;
    private DirectMapEntry entry;

    public DirectMapRecord(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    @Override
    public boolean getBool(int col) {
        return entry.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return entry.get(col);
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
    public int getStrLen(int col) {
        return entry.getStrLen(col);
    }

    @Override
    public CharSequence getSym(int col) {
        return storageFacade.getSymbolTable(col).value(entry.getInt(col));
    }

    public DirectMapRecord of(DirectMapEntry entry) {
        this.entry = entry;
        return this;
    }
}
