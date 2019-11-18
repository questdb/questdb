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

package io.questdb.cairo;

import io.questdb.std.Sinkable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class CairoException extends RuntimeException implements Sinkable {
    private static final ThreadLocal<CairoException> tlException = new ThreadLocal<>(CairoException::new);
    private final StringSink message = new StringSink();
    private int errno;

    public static CairoException instance(int errno) {
        CairoException ex = tlException.get();
        ex.message.clear();
        ex.errno = errno;
        return ex;
    }

    @Override
    public String getMessage() {
        return "[" + errno + "] " + message.toString();
    }

    public CairoException put(long value) {
        message.put(value);
        return this;
    }

    public CairoException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public CairoException put(char c) {
        message.put(c);
        return this;
    }

    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(errno).put("]: ").put(message);
    }
}
