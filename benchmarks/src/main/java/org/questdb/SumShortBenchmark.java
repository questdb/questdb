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
public class SumShortBenchmark {
    private final static long memSize = 1024 * 16;
    private final static int shortCount = (int) (memSize / Short.BYTES);
    private long shortAddr;
    private short[] shortArray;

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
        shortAddr = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        shortArray = new short[shortCount];
        final Rnd rnd = new Rnd();
        long p = shortAddr;
        for (int i = 0; i < shortCount; i++) {
            short s = rnd.nextShort();
            Unsafe.getUnsafe().putShort(p, s);
            shortArray[i] = s;
            p += Short.BYTES;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(shortAddr, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public double testJavaHeapSum() {
        double result = 0.0;
        for (int i = 0; i < shortCount; i++) {
            result += shortArray[i];
        }
        return result;
    }

    @Benchmark
    public double testJavaNativeSum() {
        double result = 0.0;
        for (int i = 0; i < shortCount; i++) {
            result += Unsafe.getUnsafe().getShort(shortAddr + i * Short.BYTES);
        }
        return result;
    }

    @Benchmark
    public int testVectMaxShort() {
        return Vect.maxShort(shortAddr, shortCount);
    }

    @Benchmark
    public int testVectMinShort() {
        return Vect.minShort(shortAddr, shortCount);
    }

    @Benchmark
    public double testVectSumShort() {
        return Vect.sumShort(shortAddr, shortCount);
    }
}
