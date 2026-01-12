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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;
import io.questdb.griffin.engine.groupby.GroupByLongLongHashMap;
import io.questdb.std.Numbers;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GroupByLongLongHashMapBenchmark {
    private static final GroupByAllocator allocator = new FastGroupByAllocator(128 * 1024, Numbers.SIZE_1GB);
    private static final int initialCapacity = 64;
    private static final double loadFactor = 0.5;
    private static final GroupByLongLongHashMap groupByLongLongHashMap = new GroupByLongLongHashMap(initialCapacity, loadFactor, 0, 0);
    private static final GroupByLongHashSet groupByLongHashSet = new GroupByLongHashSet(initialCapacity, loadFactor, 0);
    private static final Rnd rnd = new Rnd();
    private static long mapPtr = 0;
    private static long setPtr = 0;
    @Param({"5000", "50000", "500000", "5000000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLongLongHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        allocator.clear();
        groupByLongLongHashMap.setAllocator(allocator);
        groupByLongHashSet.setAllocator(allocator);
        mapPtr = 0;
        setPtr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testGroupByLongHashSet() {
        long value = rnd.nextLong(size);
        long index = groupByLongHashSet.of(setPtr).keyIndex(value);
        if (index >= 0) {
            groupByLongHashSet.addAt(index, value);
            setPtr = groupByLongHashSet.ptr();
        }
    }

    @Benchmark
    public void testGroupByLongLongHashMap() {
        long key = rnd.nextLong(size);
        long value = rnd.nextLong(size);
        groupByLongLongHashMap.of(mapPtr).put(key, value);
        mapPtr = groupByLongLongHashMap.ptr();
    }
}
