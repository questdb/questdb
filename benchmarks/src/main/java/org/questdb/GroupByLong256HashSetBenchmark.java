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
import io.questdb.griffin.engine.groupby.GroupByLong256HashSet;
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
public class GroupByLong256HashSetBenchmark {
    private static final GroupByAllocator allocator = new GroupByAllocatorArena(128 * 1024, 4 * Numbers.SIZE_1GB);
    private static final double loadFactor = 0.7;
    private static final GroupByLong256HashSet groupByLong256HashSet = new GroupByLong256HashSet(64, loadFactor, 0);
    private static final int orderedMapPageSize = 1024 * 1024;
    private static final OrderedMap orderedMap = new OrderedMap(orderedMapPageSize, new SingleColumnType(ColumnType.LONG256), null, 64, loadFactor, Integer.MAX_VALUE);
    private static final Rnd rnd = new Rnd();
    private static long ptr = 0;
    @Param({"1250", "12500", "125000", "1250000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLong256HashSetBenchmark.class.getSimpleName())
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
        groupByLong256HashSet.setAllocator(allocator);
        ptr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testGroupByLong256HashSet() {
        long l0 = rnd.nextLong(size);
        long l1 = rnd.nextLong(size);
        long l2 = rnd.nextLong(size);
        long l3 = rnd.nextLong(size);
        long index = groupByLong256HashSet.of(ptr).keyIndex(l0, l1, l2, l3);
        if (index >= 0) {
            groupByLong256HashSet.addAt(index, l0, l1, l2, l3);
            ptr = groupByLong256HashSet.ptr();
        }
    }

    @Benchmark
    public void testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong256(rnd.nextLong(size), rnd.nextLong(size), rnd.nextLong(size), rnd.nextLong(size));
        key.createValue();
    }
}
