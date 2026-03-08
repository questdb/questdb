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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PrometheusSimpleClientBenchmark {

    private static final Counter counterWithOneLabel = Counter.build()
            .name("counter_with_one_label")
            .help("counter")
            .labelNames("label0")
            .register();
    private static final Counter counterWithTwoLabels = Counter.build()
            .name("counter_with_two_labels")
            .help("counter")
            .labelNames("label0", "label1")
            .register();
    private static final Counter counterWithoutLabels = Counter.build()
            .name("counter_without_labels")
            .help("counter")
            .register();
    private static final Gauge gauge = Gauge.build()
            .name("gauge")
            .help("gauge")
            .register();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PrometheusSimpleClientBenchmark.class.getSimpleName())
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
        counterWithOneLabel.labels("label0").inc();
    }

    @Benchmark
    public void testCounterWithTwoLabels() {
        counterWithTwoLabels.labels("label0", "label1").inc();
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
}
