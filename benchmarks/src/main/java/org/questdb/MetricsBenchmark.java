/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package org.questdb;

import io.questdb.metrics.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetricsBenchmark {

    private static final short LABEL0 = 0;
    private static final short LABEL1 = 1;
    private static final MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
    private static final Counter counterWithoutLabels = metricsRegistry.newCounter("counter_without_labels");
    private static final CounterWithOneLabel counterWithOneLabel = metricsRegistry.newCounter("counter_with_one_label",
            "label0", new CharSequence[]{"A", "B", "C"}
    );
    private static final CounterWithTwoLabels counterWithTwoLabels = metricsRegistry.newCounter("counter_with_two_labels",
            "label0", new CharSequence[]{"A", "B", "C"},
            "label1", new CharSequence[]{"X", "Y", "Z"}
    );
    private static final LongGauge gauge = metricsRegistry.newLongGauge("gauge");
    private static final MetricsRegistry nullMetricsRegistry = new NullMetricsRegistry();
    private static final Counter nullCounter = nullMetricsRegistry.newCounter("null_counter");
    private static final LongGauge nullGauge = nullMetricsRegistry.newLongGauge("null_gauge");

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetricsBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .addProfiler("gc")
                .threads(4)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testCounterWithOneLabel() {
        counterWithOneLabel.inc(LABEL0);
    }

    @Benchmark
    public void testCounterWithTwoLabels() {
        counterWithTwoLabels.inc(LABEL0, LABEL1);
    }

    @Benchmark
    public void testCounterWithoutLabels() {
        counterWithoutLabels.inc();
    }

    @Benchmark
    public void testGauge() {
        gauge.inc();
        gauge.dec();
    }

    @Benchmark
    public void testNullCounter() {
        nullCounter.inc();
    }

    @Benchmark
    public void testNullGauge() {
        nullGauge.inc();
        nullGauge.dec();
    }
}
