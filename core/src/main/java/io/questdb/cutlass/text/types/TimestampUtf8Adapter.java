/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8s;

public class TimestampUtf8Adapter extends TimestampAdapter {

    private final DirectUtf16Sink utf16Sink;

    public TimestampUtf8Adapter(DirectUtf16Sink utf16Sink) {
        this.utf16Sink = utf16Sink;
    }

    public TimestampUtf8Adapter of(DateFormat format, DateLocale locale, String pattern) {
        this.format = format;
        this.locale = locale;
        this.pattern = pattern;
        return this;
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value, DirectUtf16Sink utf16Sink, DirectUtf8Sink utf8Sink) throws Exception {
        utf16Sink.clear();
        if (!Utf8s.utf8ToUtf16EscConsecutiveQuotes(value.lo(), value.hi(), utf16Sink)) {
            throw Utf8Exception.INSTANCE;
        }
        row.putDate(column, format.parse(utf16Sink, locale));
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        write(row, column, value, utf16Sink, null);
    }
}
