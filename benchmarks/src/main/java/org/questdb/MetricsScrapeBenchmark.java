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

package org.questdb;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetricsScrapeBenchmark {

    private static final MetricsRegistry metricsRegistry = new MetricsRegistryImpl();
    private static final Counter counter = metricsRegistry.newCounter("counter");
    private static final LongGauge gauge = metricsRegistry.newLongGauge("gauge");
    private static final NullCharSink sink = new NullCharSink();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetricsScrapeBenchmark.class.getSimpleName())
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
    @GroupThreads()
    public void testScrape() {
        metricsRegistry.scrapeIntoPrometheus(sink);
    }

    private static class NullCharSink implements BorrowableUtf8Sink {

        @Override
        public NativeByteSink borrowDirectByteSink() {
            return new NativeByteSink() {
                @Override
                public void close() {

                }

                @Override
                public long ptr() {
                    return 0;
                }
            };
        }

        @Override
        public Utf8Sink put(long lo, long hi) {
            return this;
        }

        @Override
        public Utf8Sink put(Utf8Sequence us) {
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            return this;
        }
    }
}
