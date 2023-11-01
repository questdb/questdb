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

import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

public class MetricsRegistryImpl implements MetricsRegistry {
    private final ObjList<Scrapable> metrics = new ObjList<>();

    @Override
    public void addScrapable(Scrapable scrapable) {
        metrics.add(scrapable);
    }

    @Override
    public Counter newCounter(CharSequence name) {
        Counter counter = new CounterImpl(name);
        metrics.add(counter);
        return counter;
    }

    @Override
    public CounterWithOneLabel newCounter(CharSequence name, CharSequence labelName0, CharSequence[] labelValues0) {
        CounterWithOneLabel counter = new CounterWithOneLabelImpl(name, labelName0, labelValues0);
        metrics.add(counter);
        return counter;
    }

    @Override
    public CounterWithTwoLabels newCounter(CharSequence name,
                                           CharSequence labelName0, CharSequence[] labelValues0,
                                           CharSequence labelName1, CharSequence[] labelValues1) {
        CounterWithTwoLabels counter = new CounterWithTwoLabelsImpl(name, labelName0, labelValues0, labelName1, labelValues1);
        metrics.add(counter);
        return counter;
    }

    @Override
    public DoubleGauge newDoubleGauge(CharSequence name) {
        DoubleGaugeImpl gauge = new DoubleGaugeImpl(name);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public LongGauge newLongGauge(CharSequence name) {
        LongGauge gauge = new LongGaugeImpl(name);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public LongGauge newLongGauge(int memoryTag) {
        LongGauge gauge = new MemoryTagLongGauge(memoryTag);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public LongGauge newVirtualGauge(CharSequence _name, VirtualLongGauge.StatProvider provider) {
        VirtualLongGauge gauge = new VirtualLongGauge(_name, provider);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        for (int i = 0, n = metrics.size(); i < n; i++) {
            Scrapable metric = metrics.getQuick(i);
            metric.scrapeIntoPrometheus(sink);
        }
    }
}
