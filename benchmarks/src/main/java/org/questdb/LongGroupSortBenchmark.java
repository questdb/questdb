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

import io.questdb.std.LongGroupSort;
import io.questdb.std.LongList;
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

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link LongGroupSort#quickSort(int, LongList, int, int)} over the input
 * shapes that matter for its interval-list callers: ascending / descending runs (the
 * fast-paths), random distinct keys, shuffled duplicate-heavy keys (fat-pivot
 * three-way partitioning) and the organ-pipe pattern (sampled-pivot robustness).
 * <p>
 * Sorting mutates its input, so every invocation refills the list from a pristine
 * array; {@code baselineFill} measures that refill alone and should be subtracted
 * from {@code sort} to isolate the sort cost.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LongGroupSortBenchmark {

    @Param({"16", "96", "1024", "65536"})
    public int groupCount;

    @Param({"sorted", "reverse", "random", "dup4", "organPipe"})
    public String shape;

    private LongList list;
    private long[] src;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LongGroupSortBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long baselineFill() {
        fill();
        return list.getQuick(0);
    }

    @Setup
    public void setup() {
        Rnd rnd = new Rnd();
        long[] keys = new long[groupCount];
        for (int i = 0; i < groupCount; i++) {
            switch (shape) {
                case "sorted":
                    keys[i] = i;
                    break;
                case "reverse":
                    keys[i] = groupCount - i;
                    break;
                case "random":
                    keys[i] = rnd.nextLong();
                    break;
                case "dup4":
                    keys[i] = rnd.nextInt(4);
                    break;
                case "organPipe":
                    keys[i] = i < groupCount / 2 ? i : groupCount - i;
                    break;
                default:
                    throw new IllegalArgumentException(shape);
            }
        }
        src = new long[2 * groupCount];
        for (int i = 0; i < groupCount; i++) {
            src[2 * i] = keys[i];
            src[2 * i + 1] = keys[i] + 1;
        }
        list = new LongList(2 * groupCount + 128);
    }

    @Benchmark
    public long sort() {
        fill();
        LongGroupSort.quickSort(2, list, 0, groupCount);
        return list.getQuick(2 * groupCount - 2);
    }

    private void fill() {
        list.clear();
        for (int i = 0, m = 2 * groupCount; i < m; i++) {
            list.add(src[i]);
        }
    }
}
