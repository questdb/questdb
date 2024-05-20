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

package org.questdb;

import io.questdb.metrics.CounterImpl;
import io.questdb.metrics.LongGaugeImpl;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@Threads(Threads.MAX)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConcurrentAssociativeCacheBenchmark {

    private final ConcurrentAssociativeCache<Integer> cacheNoMetrics;
    private final ConcurrentAssociativeCache<Integer> cacheWithMetrics;

    public ConcurrentAssociativeCacheBenchmark() {
        final int cpus = Runtime.getRuntime().availableProcessors();
        cacheNoMetrics = new ConcurrentAssociativeCache<>(4 * cpus, 4 * cpus);
        cacheWithMetrics = new ConcurrentAssociativeCache<>(
                4 * cpus,
                4 * cpus,
                new LongGaugeImpl("bench"),
                new CounterImpl("bench"),
                new CounterImpl("bench")
        );
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ConcurrentAssociativeCacheBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testNoMetrics_contented() {
        Integer v = cacheNoMetrics.poll("foobar");
        cacheNoMetrics.put("foobar", v != null ? v : 42);
    }

    @Benchmark
    public void testNoMetrics_uncontented(RndState rndState) {
        CharSequence k = rndState.rnd.nextChars(16);
        Integer v = cacheNoMetrics.poll(k);
        cacheNoMetrics.put(k, v != null ? v : 42);
    }

    @Benchmark
    public void testWithMetrics_contented() {
        Integer v = cacheWithMetrics.poll("foobar");
        cacheWithMetrics.put("foobar", v != null ? v : 42);
    }

    @Benchmark
    public void testWithMetrics_uncontented(RndState rndState) {
        CharSequence k = rndState.rnd.nextChars(16);
        Integer v = cacheWithMetrics.poll(k);
        cacheWithMetrics.put(k, v != null ? v : 42);
    }

    @State(Scope.Thread)
    public static class RndState {
        final Rnd rnd = new Rnd();
    }
}
