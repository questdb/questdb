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

package io.questdb.cutlass.http.ex;

import io.questdb.cutlass.http.HttpException;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NotEnoughLinesException extends HttpException implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<NotEnoughLinesException> tlException = new ThreadLocal<>(NotEnoughLinesException::new);
    private final StringSink message = new StringSink();

    public static NotEnoughLinesException $(CharSequence message) {
        NotEnoughLinesException te = tlException.get();
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

    public NotEnoughLinesException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public NotEnoughLinesException put(char c) {
        message.put(c);
        return this;
    }

    public NotEnoughLinesException put(long c) {
        message.put(c);
        return this;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(message);
    }
}
