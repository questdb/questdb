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

package com.questdb.griffin.engine;

import com.questdb.common.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public interface Function {

    default byte get(Record rec) {
        throw new UnsupportedOperationException();
    }

    default BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    default boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    default double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    int getPosition();

    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getStr(Record rec) {
        throw new UnsupportedOperationException();
    }

    default void getStr(Record rec, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    default int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getSym(Record rec) {
        throw new UnsupportedOperationException();
    }

    int getType();

    default boolean isConstant() {
        return false;
    }
}
