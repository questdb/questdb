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

import io.questdb.metrics.GCMetrics;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8StringSink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GCMetricsBenchmark {

    GCMetrics metrics = new GCMetrics();
    DirectUtf8Sink sink = new DirectUtf8Sink(1024 * 1024);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GCMetricsBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .addProfiler(GCProfiler.class)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testScrape() {
        sink.clear();
        metrics.scrapeIntoPrometheus(sink);
    }
}
