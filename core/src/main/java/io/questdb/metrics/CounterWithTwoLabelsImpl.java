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

import io.questdb.griffin.engine.table.PrometheusMetricsRecordCursorFactory;
import io.questdb.griffin.engine.table.PrometheusMetricsRecordCursorFactory.PrometheusMetricsCursor.PrometheusMetricsRecord;
import io.questdb.std.Numbers;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.LongAdder;

public class CounterWithTwoLabelsImpl implements CounterWithTwoLabels {
    private final LongAdder[] counters;
    private final CharSequence labelName0;
    private final CharSequence labelName1;
    private final CharSequence[] labelValues0;
    private final CharSequence[] labelValues1;
    private final CharSequence name;

    CounterWithTwoLabelsImpl(
            CharSequence name,
            CharSequence labelName0,
            CharSequence[] labelValues0,
            CharSequence labelName1,
            CharSequence[] labelValues1
    ) {
        this.name = name;
        this.labelName0 = labelName0;
        this.labelName1 = labelName1;
        this.labelValues0 = labelValues0;
        this.labelValues1 = labelValues1;
        this.counters = new LongAdder[labelValues0.length * labelValues1.length];
        for (int i = 0, n = this.counters.length; i < n; i++) {
            counters[i] = new LongAdder();
        }
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public void inc(int label0, int label1) {
        counters[(label0 * labelValues0.length) + label1].increment();
    }

    @Override
    public int scrapeIntoRecord(PrometheusMetricsRecord record) {
        scrapeIntoRecord(record, 0);
        return counters.length;
    }

    @Override
    public void scrapeIntoRecord(PrometheusMetricsRecord record, int label) {
        int i1 = label / labelValues0.length;
        int i2 = label % labelValues0.length;
        record
                .setCounterName(getName())
                .setType("counter")
                .setValue(counters[label].longValue())
                .setKind("LONG")
                .setLabels(labelName0, labelValues0[i1], labelName1, labelValues1[i2]);
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
                PrometheusFormatUtils.appendSampleLineSuffix(sink, counters[(i * 1) + j].longValue());
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);
    }
}
