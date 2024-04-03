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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorArena;
import io.questdb.griffin.engine.groupby.GroupByLong128HashSet;
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
public class GroupByLong128HashSetBenchmark {
    private static final GroupByAllocator allocator = new GroupByAllocatorArena(128 * 1024, 4 * Numbers.SIZE_1GB);
    private static final double loadFactor = 0.7;
    private static final GroupByLong128HashSet groupByLong128HashSet = new GroupByLong128HashSet(64, loadFactor, 0);
    private static final int orderedMapPageSize = 1024 * 1024;
    private static final OrderedMap orderedMap = new OrderedMap(orderedMapPageSize, new SingleColumnType(ColumnType.UUID), null, 64, loadFactor, Integer.MAX_VALUE);
    private static final Rnd rnd = new Rnd();
    private static long ptr = 0;
    @Param({"2500", "25000", "250000", "2500000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLong128HashSetBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        orderedMap.clear();
        allocator.close();
        groupByLong128HashSet.setAllocator(allocator);
        ptr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testGroupByLong128HashSet() {
        long lo = rnd.nextLong(size);
        long hi = rnd.nextLong(size);
        long index = groupByLong128HashSet.of(ptr).keyIndex(lo, hi);
        if (index >= 0) {
            groupByLong128HashSet.addAt(index, lo, hi);
            ptr = groupByLong128HashSet.ptr();
        }
    }

    @Benchmark
    public void testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong128(rnd.nextLong(size), rnd.nextLong(size));
        key.createValue();
    }
}
