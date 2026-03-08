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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.common.TextFormat;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PrometheusSimpleClientScrapeBenchmark {

    private static final Counter counter = Counter.build()
            .name("counter")
            .help("counter")
            .register(CollectorRegistry.defaultRegistry);
    private static final Gauge gauge = Gauge.build()
            .name("gauge")
            .help("gauge")
            .register(CollectorRegistry.defaultRegistry);
    private static final Writer writer = new NullWriter();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PrometheusSimpleClientScrapeBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    @Group
    @GroupThreads(4)
    public void testCounter() {
        counter.inc();
    }

    @Benchmark
    @Group
    @GroupThreads(4)
    public void testGauge() {
        gauge.inc();
    }

    @Benchmark
    @Group
    @GroupThreads(1)
    public void testScrape() throws IOException {
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
    }

    private static class NullWriter extends Writer {

        @Override
        public Writer append(char c) {
            return this;
        }

        @Override
        public Writer append(CharSequence csq) {
            return this;
        }

        @Override
        public Writer append(CharSequence csq, int start, int end) {
            return this;
        }

        @Override
        public void close() {
        }

        @Override
        public void flush() {
        }

        @Override
        public void write(String str) {
        }

        @Override
        public void write(String str, int off, int len) {
        }

        @Override
        public void write(int c) {
        }

        @Override
        public void write(char[] cbuf, int off, int len) {
        }
    }
}
