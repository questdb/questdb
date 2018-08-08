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

package com.questdb.ql.join.asof;

import com.questdb.std.Unsafe;
import com.questdb.store.Record;
import com.questdb.store.SymbolTable;

abstract class AbstractMemRecord implements Record {
    @Override
    public byte getByte(int col) {
        return Unsafe.getUnsafe().getByte(address(col));
    }

    @Override
    public boolean getBool(int col) {
        return Unsafe.getBool(address(col));
    }

    @Override
    public long getDate(int col) {
        return Unsafe.getUnsafe().getLong(address(col));
    }

    @Override
    public double getDouble(int col) {
        return Unsafe.getUnsafe().getDouble(address(col));
    }

    @Override
    public float getFloat(int col) {
        return Unsafe.getUnsafe().getFloat(address(col));
    }

    @Override
    public int getInt(int col) {
        return Unsafe.getUnsafe().getInt(address(col));
    }

    @Override
    public long getLong(int col) {
        return Unsafe.getUnsafe().getLong(address(col));
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return Unsafe.getUnsafe().getShort(address(col));
    }

    @Override
    public CharSequence getSym(int col) {
        return getSymbolTable(col).value(Unsafe.getUnsafe().getInt(address(col)));
    }

    protected abstract long address(int col);

    protected abstract SymbolTable getSymbolTable(int col);
}
