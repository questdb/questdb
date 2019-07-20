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

package com.questdb.griffin.engine.functions.bind;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class BindException extends RuntimeException implements Sinkable {
    private static final ThreadLocal<BindException> tlException = new ThreadLocal<>(BindException::new);
    private final StringSink message = new StringSink();

    public static BindException $(CharSequence message) {
        return init().put(message);
    }

    public static BindException init() {
        BindException ex = tlException.get();
        ex.message.clear();
        return ex;
    }

    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public BindException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public BindException put(char c) {
        message.put(c);
        return this;
    }

    public BindException put(int value) {
        message.put(value);
        return this;
    }

    public BindException put(long value) {
        message.put(value);
        return this;
    }

    public BindException put(Sinkable sinkable) {
        message.put(sinkable);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put(message);
    }
}
