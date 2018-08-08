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

package com.questdb.ql.analytic;

import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordCursor;
import com.questdb.store.SymbolTable;

public interface AnalyticFunction {
    int STREAM = 1;
    int TWO_PASS = 2;
    int THREE_PASS = 3;

    void add(Record record);

    default byte get() {
        throw new UnsupportedOperationException();
    }

    default boolean getBool() {
        throw new UnsupportedOperationException();
    }

    default long getDate() {
        throw new UnsupportedOperationException();
    }

    default double getDouble() {
        throw new UnsupportedOperationException();
    }

    default float getFloat() {
        throw new UnsupportedOperationException();
    }

    default CharSequence getFlyweightStr() {
        throw new UnsupportedOperationException();
    }

    default CharSequence getFlyweightStrB() {
        throw new UnsupportedOperationException();
    }

    default int getInt() {
        throw new UnsupportedOperationException();
    }

    default long getLong() {
        throw new UnsupportedOperationException();
    }

    RecordColumnMetadata getMetadata();

    default short getShort() {
        throw new UnsupportedOperationException();
    }

    default void getStr(CharSink sink) {
        throw new UnsupportedOperationException();
    }

    default int getStrLen() {
        throw new UnsupportedOperationException();
    }

    default CharSequence getSym() {
        throw new UnsupportedOperationException();
    }

    default SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException();
    }

    int getType();

    void prepare(RecordCursor cursor);

    void prepareFor(Record record);

    void reset();

    void toTop();
}
