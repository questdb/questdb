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

package com.questdb.network;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class NetworkError extends Error implements Sinkable {
    private static final ThreadLocal<NetworkError> tlException = new ThreadLocal<>(NetworkError::new);
    private final StringSink message = new StringSink();
    private int errno;

    public static NetworkError instance(int errno, CharSequence message) {
        NetworkError ex = tlException.get();
        ex.errno = errno;
        ex.message.clear();
        ex.message.put(message);
        return ex;
    }

    public static NetworkError instance(int errno) {
        NetworkError ex = tlException.get();
        ex.errno = errno;
        ex.message.clear();
        return ex;
    }

    public NetworkError couldNotBindSocket() {
        message.put("could not bind socket");
        return this;
    }

    public int getErrno() {
        return errno;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public NetworkError ip(int ipv4) {
        Net.appendIP4(message, ipv4);
        return this;
    }

    public NetworkError put(char c) {
        message.put(c);
        return this;
    }

    public NetworkError put(int value) {
        message.put(value);
        return this;
    }

    public NetworkError put(Throwable err) {
        message.put(err);
        return this;
    }

    public NetworkError put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("[errno=").put(errno).put("] ").put(message);
    }
}
