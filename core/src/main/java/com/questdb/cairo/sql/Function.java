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

import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

import java.io.Closeable;

public interface Function extends Closeable {

    @Override
    default void close() {
    }

    BinarySequence getBin(Record rec);

    long getBinLen(Record rec);

    boolean getBool(Record rec);

    byte getByte(Record rec);

    long getDate(Record rec);

    double getDouble(Record rec);

    float getFloat(Record rec);

    int getInt(Record rec);

    long getLong(Record rec);

    RecordMetadata getMetadata();

    int getPosition();

    RecordCursorFactory getRecordCursorFactory();

    short getShort(Record rec);

    char getChar(Record rec);

    CharSequence getStr(Record rec);

    void getStr(Record rec, CharSink sink);

    CharSequence getStrB(Record rec);

    int getStrLen(Record rec);

    CharSequence getSymbol(Record rec);

    long getTimestamp(Record rec);

    int getType();

    default void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {
    }

    default boolean isConstant() {
        return false;
    }

    default void toTop() {
    }
}
