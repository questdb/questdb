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
import io.questdb.cairo.map.CompactMap;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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
public class FastMapReadLongBenchmark {

    private static final int N = 5_000_000;
    private static final double loadFactor = 0.7;
    private static final HashMap<Long, Long> hmap = new HashMap<>(N, (float) loadFactor);
    private static final FastMap fmap = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024);
    private static final CompactMap cmap = new CompactMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024, Integer.MAX_VALUE);
    private static final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FastMapReadLongBenchmark.class.getSimpleName())
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
        System.out.print(" [q=" + cmap.size() + ", l=" + fmap.size() + ", cap=" + cmap.getKeyCapacity() + "] ");
    }

    @Benchmark
    public MapValue testCompactMap() {
        MapKey key = cmap.withKey();
        key.putLong(rnd.nextLong(N));
        return key.findValue();
    }

    @Benchmark
    public MapValue testFastMap() {
        MapKey key = fmap.withKey();
        key.putLong(rnd.nextLong(N));
        return key.findValue();
    }

    @Benchmark
    public Long testHashMap() {
        return hmap.get(rnd.nextLong(N));
    }

    static {
        for (int i = 0; i < N; i++) {
            MapKey key = cmap.withKey();
            key.putLong(i);
            MapValue value = key.createValue();
            value.putLong(0, i);
        }

        for (int i = 0; i < N; i++) {
            MapKey key = fmap.withKey();
            key.putLong(i);
            MapValue values = key.createValue();
            values.putLong(0, i);
        }

        for (long i = 0; i < N; i++) {
            hmap.put(i, i);
        }
    }
}
