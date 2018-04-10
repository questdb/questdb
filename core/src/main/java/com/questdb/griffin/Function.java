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

package com.questdb.griffin;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public interface Function {

    BinarySequence getBin(Record rec);

    boolean getBool(Record rec);

    byte getByte(Record rec);

    long getDate(Record rec);

    double getDouble(Record rec);

    float getFloat(Record rec);

    int getInt(Record rec);

    long getLong(Record rec);

    short getShort(Record rec);

    CharSequence getStr(Record rec);
    void getStr(Record rec, CharSink sink);

    CharSequence getStrB(Record rec);

    int getStrLen(Record rec);

    int getPosition();

    long getTimestamp(Record rec);

    int getType();

    default boolean isConstant() {
        return false;
    }

    CharSequence getSymbol(Record rec);
}
