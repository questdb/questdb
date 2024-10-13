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
public class SumShortBenchmark {
    private final static long memSize = 1_000_000 * Short.BYTES;
    private final static int shortCount = (int) (memSize / Short.BYTES);
    private long mem;
    private short[] sarr;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SumShortBenchmark.class.getSimpleName())
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
        sarr = new short[shortCount];
        final Rnd rnd = new Rnd();
        long p = mem;
        for (int i = 0; i < shortCount; i++) {
            short s = rnd.nextShort();
            Unsafe.getUnsafe().putShort(p, s);
            sarr[i] = s;
            p += Short.BYTES;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public double testJavaHeapSum() {
        double result = 0.0;
        for (int i = 0; i < shortCount; i++) {
            result += sarr[i];
        }
        return result;
    }

    @Benchmark
    public double testJavaNativeSum() {
        double result = 0.0;
        for (int i = 0; i < shortCount; i++) {
            result += Unsafe.getUnsafe().getShort(mem + i * Short.BYTES);
        }
        return result;
    }

    @Benchmark
    public double testNativeSum() {
        return Vect.sumShort(mem, shortCount);
    }
}
