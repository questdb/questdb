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

package com.questdb.cutlass.json;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class JsonException extends Exception implements Sinkable {
    private static final ThreadLocal<JsonException> tlException = new ThreadLocal<>(JsonException::new);
    private final StringSink message = new StringSink();
    private int position;

    public static JsonException $(int position, CharSequence message) {
        return position(position).put(message);
    }

    public static JsonException position(int position) {
        JsonException ex = tlException.get();
        ex.message.clear();
        ex.position = position;
        return ex;
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

    public JsonException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public JsonException put(char c) {
        message.put(c);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(position).put("]: ").put(message);
    }
}
