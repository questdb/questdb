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

package io.questdb.cutlass.text;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public class TextException extends RuntimeException implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<TextException> tlException = new ThreadLocal<>(TextException::new);
    private final StringSink message = new StringSink();

    public static TextException $(CharSequence message) {
        TextException te = tlException.get();
        StringSink sink = te.message;
        sink.clear();
        sink.put(message);
        return te;
    }

    @Override
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

    public TextException put(Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public TextException put(char c) {
        message.put(c);
        return this;
    }

    public TextException put(double c) {
        message.put(c);
        return this;
    }

    public TextException put(long c) {
        message.put(c);
        return this;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(message);
    }
}
