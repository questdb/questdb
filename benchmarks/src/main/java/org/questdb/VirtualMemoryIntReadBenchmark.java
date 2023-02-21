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

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.MemoryPARWImpl;
import io.questdb.std.MemoryTag;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VirtualMemoryIntReadBenchmark {
    private static final MemoryCARWImpl mem1 = new MemoryCARWImpl(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    private static final MemoryPARWImpl mem2 = new MemoryPARWImpl(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VirtualMemoryIntReadBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        mem1.clear();
        mem2.clear();
        long o = 0;
        for (int i = 0; i < 10000; i++) {
            mem1.putInt(o, i);
            mem2.putInt(o, i);
            o += 4;
        }
    }

    @Benchmark
    public long testIntContiguous() {
        long sum = 0;
        long o = 0;
        for (int i = 0; i < 10000; i++) {
            sum += mem1.getInt(o);
            o += 4;
        }
        return sum;
    }

    @Benchmark
    public long testIntLegacy() {
        long sum = 0;
        long o = 0;
        for (int i = 0; i < 10000; i++) {
            sum += mem2.getInt(o);
            o += 4;
        }
        return sum;
    }
}
