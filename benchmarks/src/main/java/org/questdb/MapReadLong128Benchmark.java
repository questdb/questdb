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
import io.questdb.cairo.map.Unordered16Map;
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
public class MapReadLong128Benchmark {

    private static final int N = 5_000_000;
    private static final double loadFactor = 0.7;
    private static final OrderedMap orderedMap = new OrderedMap(1024 * 1024, new SingleColumnType(ColumnType.LONG128), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024);
    private static final Rnd rnd = new Rnd();
    private static final Unordered16Map u16map = new Unordered16Map(new SingleColumnType(ColumnType.LONG128), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapReadLong128Benchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long baseline() {
        return rnd.nextLong(N) + rnd.nextLong(N);
    }

    @Benchmark
    public MapValue testOrderedMap() {
        MapKey key = orderedMap.withKey();
        key.putLong128(rnd.nextLong(N), rnd.nextLong(N));
        return key.findValue();
    }

    @Benchmark
    public MapValue testUnordered16Map() {
        MapKey key = u16map.withKey();
        key.putLong128(rnd.nextLong(N), rnd.nextLong(N));
        return key.findValue();
    }

    static {
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                MapKey key = orderedMap.withKey();
                key.putLong128(i, j);
                MapValue value = key.createValue();
                value.putLong(0, i);

                key = u16map.withKey();
                key.putLong128(i, j);
                value = key.createValue();
                value.putLong(0, i);
            }
        }
    }
}
