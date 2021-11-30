/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.ThreadLocal;

public class SqlException extends Exception implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<SqlException> tlException = new ThreadLocal<>(SqlException::new);
    private final StringSink message = new StringSink();
    private int position;

    private SqlException() {
    }

    public static SqlException $(int position, CharSequence message) {
        return position(position).put(message);
    }

    public static SqlException ambiguousColumn(int position) {
        return position(position).put("Ambiguous column name");
    }

    public static SqlException inconvertibleTypes(int position, int fromType, CharSequence fromName, int toType, CharSequence toName) {
        return $(position, "inconvertible types: ")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(" [from=").put(fromName)
                .put(", to=").put(toName).put(']');
    }

    public static SqlException inconvertibleValue(int columnNumber, double value, int fromType, int toType) {
        return $(-1, "inconvertible value: ")
                .put(value)
                .put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']')
                .put(" in target column number: ")
                .put(columnNumber);
    }

    public static SqlException inconvertibleValue(int columnNumber, long value, int fromType, int toType) {
        return $(-1, "inconvertible value: ")
                .put(value)
                .put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']')
                .put(" in target column number: ")
                .put(columnNumber);
    }

    public static SqlException invalidColumn(int position, CharSequence column) {
        return position(position).put("Invalid column: ").put(column);
    }

    public static SqlException invalidDate(int position) {
        return position(position).put("Invalid date");
    }

    public static SqlException position(int position) {
        SqlException ex = tlException.get();
        ex.message.clear();
        ex.position = position;
        return ex;
    }

    public static SqlException unexpectedToken(int position, CharSequence token) {
        return position(position).put("unexpected token: ").put(token);
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + position + "] " + message;
    }

    public int getPosition() {
        return position;
    }

    public SqlException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public SqlException put(char c) {
        message.put(c);
        return this;
    }

    public SqlException put(int value) {
        message.put(value);
        return this;
    }

    public SqlException put(long value) {
        message.put(value);
        return this;
    }

    public SqlException put(double value) {
        message.put(value);
        return this;
    }

    public SqlException put(Sinkable sinkable) {
        message.put(sinkable);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(position).put("]: ").put(message);
    }
}
