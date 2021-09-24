/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MemCopyBenchmark {
    private final static long len = 1024 * 1024;
    private static final long mem1 = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
    private static final long mem2 = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
    private static final byte[] a1 = new byte[(int) len];
    private static final byte[] a2 = new byte[(int) len];

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemCopyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        Os.init();
    }

    @Benchmark
    public void testAMemCpy() {
        Vect.memcpy(mem1, mem2, len);
    }

    @Benchmark
    public void testAMemSet() {
        Vect.memset(mem1, len, 0);
    }

    @Benchmark
    public void testArrayCopy() {
        System.arraycopy(a1, 0, a2, 0, (int) len);
    }

    @Benchmark
    public void testJavaCopyMemory() {
        Unsafe.getUnsafe().copyMemory(mem1, mem2, len);
    }

    @Benchmark
    public void testJavaSetMemory() {
        Unsafe.getUnsafe().setMemory(mem1, len, (byte) 0);
    }

    @Benchmark
    public void testVanillaLoop() {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(mem2 + i, Unsafe.getUnsafe().getByte(mem1 + i));
        }
    }
}
