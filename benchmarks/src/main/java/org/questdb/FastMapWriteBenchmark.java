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
public class FastMapWriteBenchmark {

    private static final int M = 25;
    private static final double loadFactor = 0.7;
    private static final HashMap<String, Long> hmap = new HashMap<>(64, (float) loadFactor);
    private static final FastMap fmap = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024);
    private static final CompactMap qmap = new CompactMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024, Integer.MAX_VALUE);
    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FastMapWriteBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public CharSequence baseline() {
        return rnd.nextChars(M);
    }

    @Setup(Level.Iteration)
    public void reset() {
        System.out.print(" [q=" + qmap.size() + ", l=" + fmap.size() + ", cap=" + qmap.getKeyCapacity() + "] ");
        fmap.clear();
        qmap.clear();
        rnd.reset();
    }

    @Benchmark
    public void testCompactMap() {
        MapKey key = qmap.withKey();
        key.putStr(rnd.nextChars(M));
        MapValue value = key.createValue();
        value.putLong(0, 20);
    }

    @Benchmark
    public void testFastMap() {
        MapKey key = fmap.withKey();
        key.putStr(rnd.nextChars(M));
        MapValue values = key.createValue();
        values.putLong(0, 20);
    }

    @Benchmark
    public void testHashMap() {
        hmap.put(rnd.nextChars(M).toString(), 20L);
    }
}
