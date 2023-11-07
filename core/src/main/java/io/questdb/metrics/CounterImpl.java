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


package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.LongAdder;

public class CounterImpl implements Counter {
    private final LongAdder counter;
    private final CharSequence name;

    public CounterImpl(CharSequence name) {
        this.name = name;
        this.counter = new LongAdder();
    }

    @Override
    public void add(long value) {
        counter.add(value);
    }

    @Override
    public long getValue() {
        return counter.sum();
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        PrometheusFormatUtils.appendCounterType(name, sink);
        PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, counter.longValue());
        PrometheusFormatUtils.appendNewLine(sink);
    }
}
