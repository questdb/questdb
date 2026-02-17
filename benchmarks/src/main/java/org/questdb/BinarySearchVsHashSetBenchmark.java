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

import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
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

import java.util.concurrent.TimeUnit;


/**
 * Tests the relative speed and scaling of a binary search over a long list, versus a hash set lookup.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BinarySearchVsHashSetBenchmark {
    private final LongList longList = new LongList();
    private final LongHashSet longSet = new LongHashSet();
    @Param({"1", "2", "3", "5", "8", "10", "100", "200", "1000", "10000", "100000"})
    public int size;
    Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BinarySearchVsHashSetBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void binarySearchAlwaysPresent() {
        longList.binarySearch(rnd.nextLong(size), Vect.BIN_SEARCH_SCAN_UP);
    }

    @Benchmark
    public void binarySearchNeverPresent() {
        longList.binarySearch(rnd.nextLong(), Vect.BIN_SEARCH_SCAN_UP);
    }

    @Benchmark
    public void hashSetAlwaysPresent() {
        longSet.contains(rnd.nextLong(size));
    }

    @Benchmark
    public void hashSetNeverPresent() {
        longSet.contains(rnd.nextLong());
    }

    @Setup(Level.Iteration)
    public void setup() {
        rnd.reset();
        longList.clear();
        longSet.clear();

        for (int i = 0; i < size; i++) {
            longList.add(i);
            longSet.add(i);
        }
    }
}
