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
public class MemEqBenchmark {
    private final static long len = 128;
    private static final long mem1 = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
    private static final long mem2 = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemEqBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void reset() {
        Os.init();
        for (int i = 0; i < len; i += Integer.BYTES) {
            Unsafe.getUnsafe().putInt(mem1 + i, i);
            Unsafe.getUnsafe().putInt(mem2 + i, i);
        }
    }

    @Benchmark
    public boolean testAMemCmp() {
        return Vect.memeq(mem1, mem2, len);
    }

    @Benchmark
    public boolean testVanillaLoop() {
        return memeqVanilla(mem1, mem2, len);
    }

    private static boolean memeqVanilla(long a, long b, long len) {
        int i = 0;
        for (; i + 7 < len; i += 8) {
            if (Unsafe.getUnsafe().getLong(a + i) != Unsafe.getUnsafe().getLong(b + i)) {
                return false;
            }
        }
        if (i + 3 < len) {
            if (Unsafe.getUnsafe().getInt(a + i) != Unsafe.getUnsafe().getInt(b + i)) {
                return false;
            }
            i += 4;
        }
        for (; i < len; i++) {
            if (Unsafe.getUnsafe().getByte(a + i) != Unsafe.getUnsafe().getByte(b + i)) {
                return false;
            }
        }
        return true;
    }
}
