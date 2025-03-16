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

import io.questdb.cairo.ColumnType;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class DoubleGaugeImpl implements Target, DoubleGauge {
    private final CharSequence name;
    private volatile double value;

    public DoubleGaugeImpl(CharSequence name) {
        this.name = name;
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public void putName(StringSink sink) {
        appendMetricName(sink);
    }

    @Override
    public void putType(StringSink sink) {
        sink.put("gauge");
    }

    @Override
    public void putValueAsString(StringSink sink) {
        sink.put(value);
    }

    @Override
    public void putValueType(StringSink sink) {
        sink.put(ColumnType.nameOf(ColumnType.DOUBLE));
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
        PrometheusFormatUtils.appendNewLine(sink);
    }

    @Override
    public void setValue(double value) {
        this.value = value;
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
