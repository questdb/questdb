/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.str.CharSink;

import java.util.concurrent.atomic.LongAdder;

public class CounterWithOneLabelImpl implements CounterWithOneLabel {
    private final CharSequence name;
    private final CharSequence labelName0;
    private final CharSequence[] labelValues0;
    private final LongAdder[] counters;

    CounterWithOneLabelImpl(CharSequence name, CharSequence labelName0, CharSequence[] labelValues0) {
        this.name = name;
        this.labelName0 = labelName0;
        this.labelValues0 = labelValues0;
        this.counters = new LongAdder[labelValues0.length];
        for (int i = 0, n = labelValues0.length; i < n; i++) {
            counters[i] = new LongAdder();
        }
    }

    @Override
    public void inc(short label0) {
        counters[label0].increment();
    }

    @Override
    public void scrapeIntoPrometheus(CharSink sink) {
        PrometheusFormatUtils.appendCounterType(name, sink);
        for (int i = 0, n = counters.length; i < n; i++) {
            PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
            sink.put('{');
            PrometheusFormatUtils.appendLabel(sink, labelName0, labelValues0[i]);
            sink.put('}');
            PrometheusFormatUtils.appendSampleLineSuffix(sink, counters[i].longValue());
        }
        PrometheusFormatUtils.appendNewLine(sink);
    }
}
