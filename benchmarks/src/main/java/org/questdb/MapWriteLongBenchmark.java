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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MapWriteLongBenchmark {

    private static final int N = 1_000_000;
    private static final double loadFactor = 0.7;
    private static final OrderedMap fmap = new OrderedMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024);
    private static final HashMap<Long, Long> hmap = new HashMap<>(64, (float) loadFactor);
    private static final Unordered8Map u8map = new Unordered8Map(new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024);
    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapWriteLongBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long baseline() {
        return rnd.nextLong(N) + rnd.nextLong();
    }

    @Setup(Level.Iteration)
    public void reset() {
        fmap.clear();
        u8map.clear();
        hmap.clear();
        rnd.reset();
    }

    @Benchmark
    public void testFastMap() {
        MapKey key = fmap.withKey();
        key.putLong(rnd.nextLong(N));
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }

    @Benchmark
    public void testHashMap() {
        hmap.put(rnd.nextLong(N), rnd.nextLong());
    }

    @Benchmark
    public void testUnordered8Map() {
        MapKey key = u8map.withKey();
        key.putLong(rnd.nextLong(N));
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }
}
