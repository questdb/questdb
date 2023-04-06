/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cutlass.text.TextUtil;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;

public class TimestampUtf8Adapter extends TimestampAdapter {

    private final DirectCharSink utf8Sink;

    public TimestampUtf8Adapter(DirectCharSink utf8Sink) {
        this.utf8Sink = utf8Sink;
    }

    public TimestampUtf8Adapter of(DateFormat format, DateLocale locale) {
        this.format = format;
        this.locale = locale;
        return this;
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value, DirectCharSink utf8Sink) throws Exception {
        utf8Sink.clear();
        TextUtil.utf8ToUtf16EscConsecutiveQuotes(value.getLo(), value.getHi(), utf8Sink);
        row.putDate(column, format.parse(utf8Sink, locale));
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value) throws Exception {
        write(row, column, value, utf8Sink);
    }
}
