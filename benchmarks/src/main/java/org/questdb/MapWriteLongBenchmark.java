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
import io.questdb.cairo.map.*;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MapWriteLongBenchmark {

    private static final double loadFactor = 0.7;
    private static final HashMap<Long, Long> hmap = new HashMap<>(64, (float) loadFactor);
    private static final OrderedMap orderedMap = new OrderedMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 64, loadFactor, Integer.MAX_VALUE);
    private static final Unordered16Map unordered16map = new Unordered16Map(new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 64, loadFactor, Integer.MAX_VALUE);
    private static final Unordered8Map unordered8map = new Unordered8Map(new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 64, loadFactor, Integer.MAX_VALUE);
    private final Rnd rnd = new Rnd();
    // aim for L1, L2, L3, RAM
    @Param({"5000", "50000", "500000", "5000000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapWriteLongBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        rnd.reset();

        hmap.clear();
        orderedMap.clear();
        unordered8map.clear();
        unordered16map.clear();
    }

    @Benchmark
    public void testHashMap() {
        hmap.put(rnd.nextLong(size), rnd.nextLong());
    }

    @Benchmark
    public void testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong(rnd.nextLong(size));
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }

    @Benchmark
    public void testUnordered16Map() {
        MapKey key = unordered16map.withKey();
        key.putLong(rnd.nextLong(size));
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }

    @Benchmark
    public void testUnordered8Map() {
        MapKey key = unordered8map.withKey();
        key.putLong(rnd.nextLong(size));
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }
}
