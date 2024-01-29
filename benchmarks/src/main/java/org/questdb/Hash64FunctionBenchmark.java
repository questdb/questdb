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

import io.questdb.std.Crc32CFunction;
import io.questdb.std.Hash64MemFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Hash64FunctionBenchmark {
    private final Crc32CFunction crc32c = new Crc32CFunction();
    @Param({"16", "22", "32", "38", "64", "72", "128", "134", "1024", "1034"})
    public long len;
    private long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Hash64FunctionBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(mem + i, (byte) i);
        }
    }

    @Benchmark
    public long testCrc32c() {
        return crc32c.hash(mem, len);
    }

    @Benchmark
    public long testHashMem32() {
        return Hash64MemFunction.INSTANCE.hash(mem, len);
    }
}
