/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSequenceZ;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark aims to reproduce scenario of a table with large number of
 * columns that differ in suffix only.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CharSequenceIntHashMapBenchmark {

    private static final int N = 100;
    private static final double loadFactor = 0.5;
    private final CharSequenceZ[] keys = new CharSequenceZ[N];
    private final CharSequenceIntHashMap map = new CharSequenceIntHashMap(16, loadFactor, CharSequenceIntHashMap.NO_ENTRY_VALUE);
    private final HashMap<String, Integer> hmap = new HashMap<>(16, (float) loadFactor);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CharSequenceIntHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < N; i++) {
            keys[i] = new CharSequenceZ("a_very_looooooong_column_name" + i);
            map.put(keys[i], i);
            hmap.put(keys[i].toString(), i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Misc.free(keys);
    }

    @Benchmark
    public int testCharSequenceIntHashMap() {
        int sum = 0;
        for (int i = 0; i < N; i++) {
            sum += map.get(keys[i]);
        }
        return sum;
    }

    @Benchmark
    public int testHashMap() {
        int sum = 0;
        for (int i = 0; i < N; i++) {
            sum += hmap.get(keys[i].toString());
        }
        return sum;
    }
}
