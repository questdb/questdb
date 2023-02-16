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

import io.questdb.std.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CountLongBenchmark {
    private final static long memSize = 16 * 1024;
    private final static int longCount = (int) (memSize / Long.BYTES);
    private long[] longArr;
    private long mem;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CountLongBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        longArr = new long[longCount];
        final Rnd rnd = new Rnd();
        long p = mem;
        for (int i = 0; i < longCount; i++) {
            long l = rnd.nextLong();
            byte b = rnd.nextByte();
            if (Math.abs(b) % 10 == 0) {
                l = Numbers.LONG_NaN;
            }

            Unsafe.getUnsafe().putLong(p, l);
            longArr[i] = l;
            p += Long.BYTES;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public long testJavaHeapCount() {
        long result = 0;
        for (int i = 0; i < longCount; i++) {
            result += longArr[i] == Numbers.LONG_NaN ? 0 : 1;
        }
        return result;
    }

    @Benchmark
    public long testJavaNativeCount() {
        long result = 0;
        for (int i = 0; i < longCount; i++) {
            result += Unsafe.getUnsafe().getLong(mem + i * Long.BYTES) != Numbers.LONG_NaN ? 1 : 0;
        }
        return result;
    }

    @Benchmark
    public long testNativeCount() {
        return Vect.countLong(mem, longCount);
    }
}
