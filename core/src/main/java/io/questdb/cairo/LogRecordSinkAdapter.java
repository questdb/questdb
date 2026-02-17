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

package io.questdb.cairo;

import io.questdb.log.LogRecord;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LogRecordSinkAdapter implements Utf16Sink {

    private LogRecord line;

    public LogRecordSinkAdapter of(LogRecord line) {
        this.line = line;
        return this;
    }

    @Override
    public Utf16Sink put(@Nullable CharSequence cs) {
        line.$(cs);
        return this;
    }

    @Override
    public Utf16Sink put(char c) {
        line.$(c);
        return this;
    }

    @Override
    public Utf16Sink put(int value) {
        line.$(value);
        return this;
    }

    @Override
    public Utf16Sink put(long value) {
        line.$(value);
        return this;
    }

    @Override
    public Utf16Sink put(double value) {
        line.$(value);
        return this;
    }

    @Override
    public Utf16Sink put(boolean value) {
        line.$(value);
        return this;
    }

    @Override
    public Utf16Sink put(@Nullable Sinkable sinkable) {
        line.$(sinkable);
        return this;
    }

    @Override
    public Utf16Sink put(char @NotNull [] chars, int start, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf16Sink putISODate(long value) {
        line.$ts(value);
        return this;
    }

    @Override
    public Utf16Sink putISODateMillis(long value) {
        line.$ts(value * 1000);
        return this;
    }

    @Override
    public Utf16Sink putQuoted(@NotNull CharSequence cs) {
        line.$('\"').$(cs).I$();
        return this;
    }
}
