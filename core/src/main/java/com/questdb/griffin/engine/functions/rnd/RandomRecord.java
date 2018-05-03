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

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.ObjList;

public class RandomRecord implements Record {
    final ObjList<Function> functions;

    public RandomRecord(ObjList<Function> functions) {
        this.functions = functions;
    }

    @Override
    public BinarySequence getBin(int col) {
        return functions.getQuick(col).getBin(null);
    }

    @Override
    public boolean getBool(int col) {
        return functions.getQuick(col).getBool(null);
    }

    @Override
    public byte getByte(int col) {
        return functions.getQuick(col).getByte(null);
    }

    @Override
    public long getDate(int col) {
        return functions.getQuick(col).getDate(null);
    }

    @Override
    public double getDouble(int col) {
        return functions.getQuick(col).getDouble(null);
    }

    @Override
    public float getFloat(int col) {
        return functions.getQuick(col).getFloat(null);
    }

    @Override
    public CharSequence getStr(int col) {
        return functions.getQuick(col).getStr(null);
    }

    @Override
    public int getInt(int col) {
        return functions.getQuick(col).getInt(null);
    }

    @Override
    public long getLong(int col) {
        return functions.getQuick(col).getLong(null);
    }

    @Override
    public short getShort(int col) {
        return functions.getQuick(col).getShort(null);
    }

    @Override
    public CharSequence getSym(int col) {
        return functions.getQuick(col).getSymbol(null);
    }

    @Override
    public long getTimestamp(int col) {
        return functions.getQuick(col).getTimestamp(null);
    }
}
