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

package io.questdb.cutlass.text;

import io.questdb.std.Sinkable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class TextException extends Exception implements Sinkable {
    private static final ThreadLocal<TextException> tlException = new ThreadLocal<>(TextException::new);
    private final StringSink message = new StringSink();

    public static TextException $(CharSequence message) {
        TextException te = tlException.get();
        StringSink sink = te.message;
        sink.clear();
        sink.put(message);
        return te;
    }

    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public TextException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public TextException put(char c) {
        message.put(c);
        return this;
    }

    public TextException put(double c) {
        message.put(c, 6);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put(message);
    }
}
