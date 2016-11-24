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

package com.questdb.ql;

import com.questdb.std.DirectInputStream;
import com.questdb.std.str.CharSink;

import java.io.OutputStream;

public interface Record {

    String asString(int col);

    byte get(int col);

    void getBin(int col, OutputStream s);

    DirectInputStream getBin(int col);

    long getBinLen(int col);

    boolean getBool(int col);

    long getDate(int col);

    double getDouble(int col);

    float getFloat(int col);

    CharSequence getFlyweightStr(int col);

    CharSequence getFlyweightStrB(int col);

    int getInt(int col);

    long getLong(int col);

    long getRowId();

    short getShort(int col);

    void getStr(int col, CharSink sink);

    int getStrLen(int col);

    String getSym(int col);
}
