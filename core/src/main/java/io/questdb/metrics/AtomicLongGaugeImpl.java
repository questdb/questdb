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

package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongGaugeImpl implements AtomicLongGauge {
    private final AtomicLong counter;
    private final CharSequence name;

    public AtomicLongGaugeImpl(CharSequence name) {
        this.name = name;
        this.counter = new AtomicLong();
    }

    @Override
    public void add(long value) {
        counter.addAndGet(value);
    }

    @Override
    public void dec() {
        counter.decrementAndGet();
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public long getValue() {
        return counter.get();
    }

    @Override
    public void inc() {
        counter.incrementAndGet();
    }

    @Override
    public long incrementAndGet() {
        return counter.incrementAndGet();
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, counter.longValue());
        PrometheusFormatUtils.appendNewLine(sink);
    }

    @Override
    public void setValue(long value) {
        counter.set(value);
    }

    private void appendMetricName(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(name);
    }

    private void appendType(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(name);
        sink.putAscii(" gauge\n");
    }
}
