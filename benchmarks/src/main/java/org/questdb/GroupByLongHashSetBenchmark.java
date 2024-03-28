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
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;
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
public class GroupByLongHashSetBenchmark {
    private static final GroupByAllocator allocator = new GroupByAllocatorArena(128 * 1024, Numbers.SIZE_1GB);
    private static final double loadFactor = 0.7;
    private static final GroupByLongHashSet groupByLongHashSet = new GroupByLongHashSet(64, loadFactor, 0);
    private static final int orderedMapPageSize = 1024 * 1024;
    private static final OrderedMap orderedMap = new OrderedMap(orderedMapPageSize, new SingleColumnType(ColumnType.LONG), null, 64, loadFactor, Integer.MAX_VALUE);
    private static final Rnd rnd = new Rnd();
    private static long ptr = 0;
    @Param({"5000", "50000", "500000", "5000000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLongHashSetBenchmark.class.getSimpleName())
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
        groupByLongHashSet.setAllocator(allocator);
        ptr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testGroupByLongHashSet() {
        long value = rnd.nextLong(size);
        long index = groupByLongHashSet.of(ptr).keyIndex(value);
        if (index >= 0) {
            groupByLongHashSet.addAt(index, value);
            ptr = groupByLongHashSet.ptr();
        }
    }

    @Benchmark
    public void testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong(rnd.nextLong(size));
        key.createValue();
    }
}
