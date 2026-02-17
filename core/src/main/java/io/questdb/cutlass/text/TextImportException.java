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

public class TextImportException extends RuntimeException implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<TextImportException> tlException = new ThreadLocal<>(TextImportException::new);
    private final StringSink message = new StringSink();
    private boolean cancelled;
    private byte phase;

    public static TextImportException instance(byte phase, CharSequence message) {
        return instance(phase, message, Integer.MIN_VALUE);
    }

    public static TextImportException instance(byte phase, CharSequence message, int errno) {
        TextImportException te = tlException.get();
        te.phase = phase;
        te.cancelled = false;
        StringSink sink = te.message;
        sink.clear();
        if (errno > Integer.MIN_VALUE) {
            sink.put('[').put(errno).put("] ");
        }
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

    public byte getPhase() {
        return phase;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public TextImportException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public TextImportException put(Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public TextImportException put(char c) {
        message.put(c);
        return this;
    }

    public TextImportException put(double c) {
        message.put(c);
        return this;
    }

    public TextImportException put(long c) {
        message.put(c);
        return this;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(message);
    }
}
