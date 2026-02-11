/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.network;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class NetworkError extends Error implements Sinkable, FlyweightMessageContainer {
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

    public NetworkError couldNotBindSocket(CharSequence who, int ipv4, int port) {
        return this.put("could not bind socket [who=").put(who).put(", bindTo=").ip(ipv4).put(':').put(port).put(']');
    }

    public int getErrno() {
        return errno;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + errno + "] " + message;
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

    public NetworkError put(long value) {
        message.put(value);
        return this;
    }

    public NetworkError put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("[errno=").put(errno).putAscii("] ").put(message);
    }
}
