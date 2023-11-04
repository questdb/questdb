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

import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

/**
 * Read-only gauge used to expose various stats.
 */
public class VirtualLongGauge implements LongGauge {

    private final CharSequence name;
    private final StatProvider provider;

    public VirtualLongGauge(CharSequence name, StatProvider statProvider) {
        this.name = name;
        this.provider = statProvider;
    }

    @Override
    public void add(long value) {
        // do nothing as this gauge is RO view of some stat
    }

    @Override
    public void dec() {
        // do nothing as this gauge is RO view of some stat
    }

    @Override
    public long getValue() {
        return provider.getValue();
    }

    @Override
    public void inc() {
        // do nothing as this gauge is RO view of some stat
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, getValue());
        PrometheusFormatUtils.appendNewLine(sink);
    }

    @Override
    public void setValue(long value) {
        // do nothing as this gauge is RO view of some stat
    }

    private void appendMetricName(CharSinkBase<?> sink) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(getName());
    }

    private void appendType(CharSinkBase<?> sink) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(getName());
        sink.putAscii(" gauge\n");
    }

    private CharSequence getName() {
        return this.name;
    }

    @FunctionalInterface
    public interface StatProvider {
        long getValue();
    }
}
