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

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SumDoubleBenchmark {
    private final static long memSize = 1024 * 16;
    private final static int doubleCount = (int) (memSize / Double.BYTES);
    private long countAddr;
    private long doubleAddr;
    private double[] doubleArray;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SumDoubleBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        doubleAddr = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        countAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        doubleArray = new double[doubleCount];
        final Rnd rnd = new Rnd();
        long p = doubleAddr;
        for (int i = 0; i < doubleCount; i++) {
            double d = rnd.nextDouble();
            Unsafe.getUnsafe().putDouble(p, d);
            doubleArray[i] = d;
            p += Double.BYTES;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(doubleAddr, memSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(countAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public double testJavaHeapSum() {
        double result = 0.0;
        for (int i = 0; i < doubleCount; i++) {
            result += doubleArray[i];
        }
        return result;
    }

    @Benchmark
    public double testJavaNativeSum() {
        double result = 0.0;
        for (int i = 0; i < doubleCount; i++) {
            result += Unsafe.getUnsafe().getDouble(doubleAddr + i * 8);
        }
        return result;
    }

    @Benchmark
    public double testVectMaxDouble() {
        return Vect.maxDouble(doubleAddr, doubleCount);
    }

    @Benchmark
    public double testVectMinDouble() {
        return Vect.minDouble(doubleAddr, doubleCount);
    }

    @Benchmark
    public double testVectSumDouble() {
        return Vect.sumDouble(doubleAddr, doubleCount);
    }

    @Benchmark
    public double testVectSumDoubleAcc() {
        return Vect.sumDoubleAcc(doubleAddr, doubleCount, countAddr);
    }
}
