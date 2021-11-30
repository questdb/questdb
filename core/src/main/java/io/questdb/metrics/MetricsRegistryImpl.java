/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.str.CharSink;

public class MetricsRegistryImpl implements MetricsRegistry {
    private final ObjList<Scrapable> metrics = new ObjList<>();

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
    public Gauge newGauge(CharSequence name) {
        Gauge gauge = new GaugeImpl(name);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public Gauge newGauge(int memoryTag) {
        Gauge gauge = new MemoryTagGauge(memoryTag);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public Gauge newVirtualGauge(CharSequence _name, VirtualGauge.StatProvider provider) {
        VirtualGauge gauge = new VirtualGauge(_name, provider);
        metrics.add(gauge);
        return gauge;
    }

    @Override
    public void scrapeIntoPrometheus(CharSink sink) {
        for (int i = 0, n = metrics.size(); i < n; i++) {
            Scrapable metric = metrics.getQuick(i);
            metric.scrapeIntoPrometheus(sink);
        }
    }
}
