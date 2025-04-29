/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.filter.CuckooFilter;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
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

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CuckooFilterReadBenchmark {

    private static final int SCALE_FACTOR = 10;
    private static final GroupByAllocator allocator = new FastGroupByAllocator(128 * 1024, Numbers.SIZE_1GB);
    public static long countFalse = 0;
    public static long countTrue = 0;
    public static boolean failed = false;
    private static CuckooFilter filter = null;
    private final Rnd rnd = new Rnd();
    // aim for L1, L2, L3, RAM
    @Param({"33554432"/*"5000", "50000", "500000", "5000000"*/})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CuckooFilterReadBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(0)
//                .forks(1)
                .build();

        new Runner(opt).run();
    }

    // for 2^25 keys
    // 4 bits = 21% false positive
    // 8 bits = 1.39% false positive <-- should be default,
    // 12 bits = 0.08% false positive
    @Setup(Level.Iteration)
    public void reset() {
        rnd.reset();
        if (filter == null) {
            filter = new CuckooFilter((int) Math.pow(2, 25), 4).of(0, allocator);
        } else {
            double ratio = (double) countTrue / ((double) countTrue + (double) countFalse);
            double falsePositiveRate = (ratio - (1.0 / SCALE_FACTOR)) * 100;
            System.out.println("CuckooFilter { " +
                    "fillFactor=" + filter.getFillFactor() +
                    ", allocated=" + filter.getAllocSize()
                    + ", falsePositiveRate=" + falsePositiveRate + "%");
            filter.clear();
        }
        countTrue = 0;
        countFalse = 0;
        failed = false;
        for (int i = 0; i < size; i++) {
            filter.insert(i);
        }
    }

    @Benchmark
    public void testCuckooFilterRead() {
        boolean b = filter.maybeContains(rnd.nextLong(size * SCALE_FACTOR));
        if (b) {
            countTrue++;
        } else {
            countFalse++;
        }
    }
}

// 1 in 10 should be positive on avg
// 32371145 vs 69288834 negative

