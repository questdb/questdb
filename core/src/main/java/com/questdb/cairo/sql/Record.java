/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo.sql;

import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public interface Record {

    default BinarySequence getBin(int col) {
        throw new UnsupportedOperationException();
    }

    default long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    default boolean getBool(int col) {
        throw new UnsupportedOperationException();
    }

    default byte getByte(int col) {
        throw new UnsupportedOperationException();
    }

    default long getDate(int col) {
        return getLong(col);
    }

    default double getDouble(int col) {
        throw new UnsupportedOperationException();
    }

    default float getFloat(int col) {
        throw new UnsupportedOperationException();
    }

    default int getInt(int col) {
        throw new UnsupportedOperationException();
    }

    default long getLong(int col) {
        throw new UnsupportedOperationException();
    }

    default long getRowId() {
        throw new UnsupportedOperationException();
    }

    default short getShort(int col) {
        throw new UnsupportedOperationException();
    }

    default char getChar(int col) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getStr(int col) {
        throw new UnsupportedOperationException();
    }

    default void getStr(int col, CharSink sink) {
        sink.put(getStr(col));
    }

    default CharSequence getStrB(int col) {
        throw new UnsupportedOperationException();
    }

    default int getStrLen(int col) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getSym(int col) {
        throw new UnsupportedOperationException();
    }

    default long getTimestamp(int col) {
        return getLong(col);
    }
}
