/*+*****************************************************************************
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
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Isolates index sorting in the array-valued exact percentile aggregates. A caller can bind
 * percentiles in descending order, yielding descending discrete selection indices; the continuous
 * variant also sorts indices gathered from a hash set. The old insertion sort then shifts O(k^2)
 * entries per group; the current Arrays.sort call is O(k log k).
 * Both methods include the same input copy and return the sorted array for JMH to consume.
 * This measures the index sort, not the full SQL query or value selection.
 * <p>
 * Run: java -jar benchmarks/target/benchmarks.jar PercentileIndexSortBenchmark -f 1 -wi 2 -i 3
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PercentileIndexSortBenchmark {
    @Param({"16", "1024", "8192", "32768"})
    public int percentileCount;

    @Param({"DESCENDING", "SHUFFLED"})
    public String order;

    private int[] indicesToSort;

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                .include(PercentileIndexSortBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build()).run();
    }

    @Benchmark
    public int[] arraysSort() {
        int[] indices = indicesToSort.clone();
        Arrays.sort(indices, 0, percentileCount);
        return indices;
    }

    @Benchmark
    public int[] insertionSortBeforeFix() {
        int[] indices = indicesToSort.clone();
        for (int i = 1; i < percentileCount; i++) {
            int key = indices[i];
            int j = i - 1;
            while (j >= 0 && indices[j] > key) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
        return indices;
    }

    @Setup(Level.Trial)
    public void setup() {
        indicesToSort = new int[percentileCount];
        for (int i = 0; i < percentileCount; i++) {
            indicesToSort[i] = percentileCount - i - 1;
        }
        if ("SHUFFLED".equals(order)) {
            Random random = new Random(42);
            for (int i = percentileCount - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                int value = indicesToSort[i];
                indicesToSort[i] = indicesToSort[j];
                indicesToSort[j] = value;
            }
        } else if (!"DESCENDING".equals(order)) {
            throw new IllegalArgumentException(order);
        }
        int[] reference = indicesToSort.clone();
        Arrays.sort(reference);
        int[] previous = insertionSortBeforeFix();
        if (!Arrays.equals(reference, previous)) {
            throw new AssertionError("the before/after index sorts disagree");
        }
    }
}
