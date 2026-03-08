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
public class CountIntBenchmark {
    private final static long memSize = 16 * 1024;
    private final static int intCount = (int) (memSize / Integer.BYTES);
    private int[] intArr;
    private long mem;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CountIntBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        intArr = new int[intCount];
        final Rnd rnd = new Rnd();
        long p = mem;
        for (int i = 0; i < intCount; i++) {
            int val = rnd.nextInt();
            byte b = rnd.nextByte();
            if (Math.abs(b) % 10 == 0) {
                val = Numbers.INT_NULL;
            }

            Unsafe.getUnsafe().putInt(p, val);
            intArr[i] = val;
            p += Integer.BYTES;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public long testJavaHeapCount() {
        long result = 0;
        for (int i = 0; i < intCount; i++) {
            result += intArr[i] == Numbers.INT_NULL ? 0 : 1;
        }
        return result;
    }

    @Benchmark
    public long testJavaNativeCount() {
        long result = 0;
        for (int i = 0; i < intCount; i++) {
            result += Unsafe.getUnsafe().getInt(mem + i * Integer.BYTES) != Numbers.INT_NULL ? 1 : 0;
        }
        return result;
    }

    @Benchmark
    public long testNativeCount() {
        return Vect.countInt(mem, intCount);
    }
}
