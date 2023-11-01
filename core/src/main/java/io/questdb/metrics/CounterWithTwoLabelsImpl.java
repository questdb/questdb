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

import io.questdb.std.Numbers;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.LongAdder;

public class CounterWithTwoLabelsImpl implements CounterWithTwoLabels {
    private final LongAdder[] counters;
    private final CharSequence labelName0;
    private final CharSequence labelName1;
    private final CharSequence[] labelValues0;
    private final CharSequence[] labelValues1;
    private final CharSequence name;
    private final int shl;

    CounterWithTwoLabelsImpl(CharSequence name,
                             CharSequence labelName0, CharSequence[] labelValues0,
                             CharSequence labelName1, CharSequence[] labelValues1) {
        this.name = name;
        this.labelName0 = labelName0;
        this.labelName1 = labelName1;
        this.labelValues0 = labelValues0;
        this.labelValues1 = labelValues1;
        int labelValues0Capacity = Numbers.ceilPow2(labelValues0.length);
        this.shl = Numbers.msb(labelValues0Capacity);
        this.counters = new LongAdder[labelValues0Capacity * labelValues1.length];
        for (int i = 0, n = labelValues0.length; i < n; i++) {
            for (int j = 0, k = labelValues1.length; j < k; j++) {
                counters[(i << shl) + j] = new LongAdder();
            }
        }
    }

    @Override
    public void inc(short label0, short label1) {
        counters[(label0 << shl) + label1].increment();
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        PrometheusFormatUtils.appendCounterType(name, sink);
        for (int i = 0, n = labelValues0.length; i < n; i++) {
            for (int j = 0, k = labelValues1.length; j < k; j++) {
                PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
                sink.put('{');
                PrometheusFormatUtils.appendLabel(sink, labelName0, labelValues0[i]);
                sink.put(',');
                PrometheusFormatUtils.appendLabel(sink, labelName1, labelValues1[j]);
                sink.put('}');
                PrometheusFormatUtils.appendSampleLineSuffix(sink, counters[(i << shl) + j].longValue());
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);
    }
}
