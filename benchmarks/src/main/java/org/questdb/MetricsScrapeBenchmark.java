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

package org.questdb;

import io.questdb.metrics.*;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;
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
    private static final Gauge gauge = metricsRegistry.newGauge("gauge");
    private static final CharSink sink = new NullCharSink();

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
    @GroupThreads()
    public void testScrape() {
        metricsRegistry.scrapeIntoPrometheus(sink);
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

    private static class NullCharSink implements CharSink {

        @Override
        public CharSink encodeUtf8(CharSequence cs) {
            return this;
        }

        @Override
        public CharSink encodeUtf8(CharSequence cs, int lo, int hi) {
            return this;
        }

        @Override
        public CharSink encodeUtf8AndQuote(CharSequence cs) {
            return this;
        }

        @Override
        public char[] getDoubleDigitsBuffer() {
            return new char[0];
        }

        @Override
        public CharSink put(char c) {
            return this;
        }

        @Override
        public CharSink putUtf8(char c) {
            return this;
        }

        @Override
        public CharSink put(int value) {
            return this;
        }

        @Override
        public CharSink put(long value) {
            return this;
        }

        @Override
        public CharSink put(float value, int scale) {
            return this;
        }

        @Override
        public CharSink put(double value) {
            return this;
        }

        @Override
        public CharSink put(double value, int scale) {
            return this;
        }

        @Override
        public CharSink put(boolean value) {
            return this;
        }

        @Override
        public CharSink put(Throwable e) {
            return this;
        }

        @Override
        public CharSink put(Sinkable sinkable) {
            return this;
        }

        @Override
        public CharSink putISODate(long value) {
            return this;
        }

        @Override
        public CharSink putISODateMillis(long value) {
            return this;
        }

        @Override
        public CharSink putSize(long value) {
            return this;
        }

        @Override
        public CharSink putQuoted(CharSequence cs) {
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            return this;
        }

        @Override
        public CharSink put(CharSequence cs) {
            return this;
        }

        @Override
        public int encodeSurrogate(char c, CharSequence in, int pos, int hi) {
            return 0;
        }
    }
}
