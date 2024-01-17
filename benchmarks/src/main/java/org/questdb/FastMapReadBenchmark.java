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
import io.questdb.std.str.StringSink;
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
public class FastMapReadBenchmark {

    private static final int N = 5_000_000;
    private static final double loadFactor = 0.7;
    private static final HashMap<String, Long> hmap = new HashMap<>(N, (float) loadFactor);
    private static final FastMap fmap = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024);
    private static final CompactMap qmap = new CompactMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), N, loadFactor, 1024, Integer.MAX_VALUE);
    private static final Rnd rnd = new Rnd();
    private static final StringSink sink = new StringSink();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FastMapReadBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public int baseline() {
        return rnd.nextInt(N);
    }

    @Setup(Level.Iteration)
    public void reset() {
        System.out.print(" [q=" + qmap.size() + ", l=" + fmap.size() + ", cap=" + qmap.getKeyCapacity() + "] ");
    }

    @Benchmark
    public MapValue testCompactMap() {
        MapKey key = qmap.withKey();
        sink.clear();
        sink.put(rnd.nextInt(N));
        key.putStr(sink);
        return key.findValue();
    }

    @Benchmark
    public MapValue testFastMap() {
        MapKey key = fmap.withKey();
        sink.clear();
        sink.put(rnd.nextInt(N));
        key.putStr(sink);
        return key.findValue();
    }

    @Benchmark
    public Long testHashMap() {
        return hmap.get(String.valueOf(rnd.nextInt(N)));
    }

    static {
        for (int i = 0; i < N; i++) {
            MapKey key = qmap.withKey();
            key.putStr(String.valueOf(i));
            MapValue value = key.createValue();
            value.putLong(0, i);
        }

        for (int i = 0; i < N; i++) {
            MapKey key = fmap.withKey();
            key.putStr(String.valueOf(i));
            MapValue values = key.createValue();
            values.putLong(0, i);
        }

        for (int i = 0; i < N; i++) {
            hmap.put(String.valueOf(i), (long) i);
        }
    }
}
