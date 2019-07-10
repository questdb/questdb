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

package com.questdb.griffin;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class SqlException extends Exception implements Sinkable {
    private static final ThreadLocal<SqlException> tlException = new ThreadLocal<>(SqlException::new);
    private final StringSink message = new StringSink();
    private int position;

    public static SqlException $(int position, CharSequence message) {
        return position(position).put(message);
    }

    public static SqlException ambiguousColumn(int position) {
        return position(position).put("Ambiguous column name");
    }

    public static SqlException invalidColumn(int position, CharSequence column) {
        return position(position).put("Invalid column: ").put(column);
    }

    public static SqlException last() {
        return tlException.get();
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

    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + position + "] " + message.toString();
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

    public SqlException put(Sinkable sinkable) {
        message.put(sinkable);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(position).put("]: ").put(message);
    }
}
