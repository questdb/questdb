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

import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class IntIntHashMapBenchmark {
    private static final double loadFactor = 0.5;
    private static final Rnd rnd = new Rnd();
    private final DirectIntIntHashMap directHashMap = new DirectIntIntHashMap(64, loadFactor, 0, 0, MemoryTag.NATIVE_DEFAULT);
    @Param({"5000", "50000", "500000", "5000000"})
    public int size;
    private IntIntHashMap hashMap = new IntIntHashMap(64, loadFactor);
    private HashMap<Integer, Integer> stdHashMap = new HashMap<>(64, (float) loadFactor);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntIntHashMapBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        hashMap = new IntIntHashMap(64, loadFactor);
        stdHashMap = new HashMap<>(64, (float) loadFactor);
        directHashMap.restoreInitialCapacity();
        rnd.reset();
    }

    @TearDown
    public void tearDown() {
        directHashMap.close();
    }

    @Benchmark
    public void testDirectHashMap() {
        int k = rnd.nextInt(size);
        directHashMap.put(k, k);
    }

    @Benchmark
    public void testHashMap() {
        int k = rnd.nextInt(size);
        hashMap.put(k, k);
    }

    @Benchmark
    public void testStdHashMap() {
        int k = rnd.nextInt(size);
        stdHashMap.putIfAbsent(k, k);
    }
}
