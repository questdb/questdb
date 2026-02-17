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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MapReadLongBenchmark {

    private static final double loadFactor = 0.7;
    private static final Rnd rnd = new Rnd();
    // aim for L1, L2, L3, RAM
    @Param({"5000", "50000", "500000", "5000000"})
    public int size;
    private HashMap<Long, Long> hmap;
    private OrderedMap orderedMap;
    private Unordered8Map unordered8map;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapReadLongBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        rnd.reset();

        Misc.free(orderedMap);
        Misc.free(unordered8map);

        hmap = new HashMap<>(size, (float) loadFactor);
        orderedMap = new OrderedMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), size, loadFactor, Integer.MAX_VALUE);
        unordered8map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), size, loadFactor, Integer.MAX_VALUE);

        for (long i = 0; i < size; i++) {
            MapKey key = orderedMap.withKey();
            key.putLong(i);
            MapValue value = key.createValue();
            value.putLong(0, i);

            MapKey key8 = unordered8map.withKey();
            key8.putLong(i);
            MapValue value8 = key8.createValue();
            value8.putLong(0, i);

            hmap.put(i, i);
        }
    }

    @Benchmark
    public Long testHashMap() {
        return hmap.get(rnd.nextLong(size));
    }

    @Benchmark
    public long testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong(rnd.nextLong(size));
        MapValue value = key.findValue();
        return value != null ? value.getLong(0) : 0;
    }

    @Benchmark
    public long testUnordered8Map() {
        MapKey key = unordered8map.withKey();
        key.putLong(rnd.nextLong(size));
        MapValue value = key.findValue();
        return value != null ? value.getLong(0) : 0;
    }
}
