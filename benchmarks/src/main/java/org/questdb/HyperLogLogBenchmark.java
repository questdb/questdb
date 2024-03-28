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

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorArena;
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLog;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HyperLogLogBenchmark {
    private static final long N = 1_000_000;
    private static final GroupByAllocator allocator = new GroupByAllocatorArena(128 * 1024, Numbers.SIZE_1GB);
    private static final HyperLogLog hll = new HyperLogLog(14);
    private static final Rnd rnd = new Rnd();
    private static final GroupByLongHashSet set = new GroupByLongHashSet(16, 0.7, 0);
    private static long hllPtr = 0;
    private static long setPtr = 0;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HyperLogLogBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long baseline() {
        return rnd.nextLong(N);
    }

    @Setup(Level.Iteration)
    public void reset() {
        allocator.close();
        hll.setAllocator(allocator);
        hllPtr = 0;
        set.setAllocator(allocator);
        setPtr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testGroupByLongHashSet() {
        long value = rnd.nextLong(N);
        long index = set.of(setPtr).keyIndex(value);
        if (index >= 0) {
            set.addAt(index, value);
            setPtr = set.ptr();
        }
    }

    @Benchmark
    public void testHyperLogLog() {
        long value = rnd.nextLong(N);
        long hash = Hash.murmur3ToLong(value);
        hll.of(hllPtr).addAndComputeCardinalityFast(hash);
        hllPtr = hll.ptr();
    }
}
