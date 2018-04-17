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

package com.questdb.griffin.engine.params;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class ParameterException extends RuntimeException implements Sinkable {
    private static final ThreadLocal<ParameterException> tlException = new ThreadLocal<>(ParameterException::new);
    private final StringSink message = new StringSink();
    private int position;

    public static ParameterException position(int position) {
        ParameterException ex = tlException.get();
        ex.message.clear();
        ex.position = position;
        return ex;
    }

    @Override
    public String getMessage() {
        return "[" + position + "] " + message.toString();
    }

    public int getPosition() {
        return position;
    }

    public ParameterException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public ParameterException put(char c) {
        message.put(c);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(position).put("]: ").put(message);
    }
}
