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

import io.questdb.metrics.Counter;
import io.questdb.metrics.CounterImpl;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.LongGaugeImpl;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.Rnd;
import io.questdb.std.SimpleAssociativeCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@Threads(Threads.MAX)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AssociativeCacheBenchmark {
    private static final int N_QUERIES = 50;
    private static final LongGauge cachedGauge = new LongGaugeImpl("bench");
    private static final Counter hitCounter = new CounterImpl("bench");
    private static final Counter missCounter = new CounterImpl("bench");
    private static final String[] queries = new String[N_QUERIES];
    private final ConcurrentAssociativeCache<Integer> cacheNoMetrics;
    private final ConcurrentAssociativeCache<Integer> cacheWithMetrics;

    public AssociativeCacheBenchmark() {
        final int cpus = Runtime.getRuntime().availableProcessors();
        cacheNoMetrics = new ConcurrentAssociativeCache<>(
                new DefaultConcurrentCacheConfiguration() {
                    @Override
                    public int getBlocks() {
                        return 8 * cpus;
                    }

                    @Override
                    public int getRows() {
                        return 2 * cpus;
                    }
                }
        );
        cacheWithMetrics = new ConcurrentAssociativeCache<>(
                new ConcurrentCacheConfiguration() {
                    @Override
                    public int getBlocks() {
                        return 8 * cpus;
                    }

                    @Override
                    public LongGauge getCachedGauge() {
                        return cachedGauge;
                    }

                    @Override
                    public Counter getHiCounter() {
                        return hitCounter;
                    }

                    @Override
                    public Counter getMissCounter() {
                        return missCounter;
                    }

                    @Override
                    public int getRows() {
                        return 2 * cpus;
                    }
                }
        );
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(AssociativeCacheBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testConcurrentNoMetrics_randomKeys(RndState rndState) {
        CharSequence k = queries[rndState.rnd.nextInt(N_QUERIES)];
        Integer v = cacheNoMetrics.poll(k);
        cacheNoMetrics.put(k, v != null ? v : 42);
    }

    @Benchmark
    public void testConcurrentNoMetrics_sameKey() {
        Integer v = cacheNoMetrics.poll(queries[0]);
        cacheNoMetrics.put(queries[0], v != null ? v : 42);
    }

    @Benchmark
    public void testConcurrentWithMetrics_randomKeys(RndState rndState) {
        CharSequence k = queries[rndState.rnd.nextInt(N_QUERIES)];
        Integer v = cacheWithMetrics.poll(k);
        cacheWithMetrics.put(k, v != null ? v : 42);
    }

    @Benchmark
    public void testConcurrentWithMetrics_sameKey() {
        Integer v = cacheWithMetrics.poll(queries[0]);
        cacheWithMetrics.put(queries[0], v != null ? v : 42);
    }

    @Benchmark
    public void testSimpleWithMetrics_randomKeys(RndState rndState) {
        CharSequence k = queries[rndState.rnd.nextInt(N_QUERIES)];
        Integer v = rndState.localCache.poll(k);
        rndState.localCache.put(k, v != null ? v : 42);
    }

    @Benchmark
    public void testSimpleWithMetrics_sameKey(RndState rndState) {
        Integer v = rndState.localCache.poll(queries[0]);
        rndState.localCache.put(queries[0], v != null ? v : 42);
    }

    @State(Scope.Thread)
    public static class RndState {
        final SimpleAssociativeCache<Integer> localCache;
        final Rnd rnd = new Rnd();

        public RndState() {
            final int cpus = Runtime.getRuntime().availableProcessors();
            this.localCache = new SimpleAssociativeCache<>(
                    8 * cpus,
                    2 * cpus,
                    cachedGauge,
                    hitCounter,
                    missCounter
            );
        }
    }

    static {
        Rnd rnd = new Rnd();
        for (int i = 0; i < queries.length; i++) {
            queries[i] = Chars.toString(rnd.nextChars(100));
        }
    }
}
