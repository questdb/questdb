/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.sql;

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public interface Record {

    CharSequenceFunction GET_STR = Record::getStr;

    CharSequenceFunction GET_SYM = Record::getSym;

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

    default char getChar(int col) {
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

    default void getLong256(int col, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    default Long256 getLong256A(int col) {
        throw new UnsupportedOperationException();
    }

    default Long256 getLong256B(int col) {
        throw new UnsupportedOperationException();
    }

    default BinarySequence getRawBytes(int col, int len) {
        throw new UnsupportedOperationException();
    }

    default long getRowId() {
        throw new UnsupportedOperationException();
    }

    default short getShort(int col) {
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

    @FunctionalInterface
    interface CharSequenceFunction {
        CharSequence get(Record record, int col);
    }
}
