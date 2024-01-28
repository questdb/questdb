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

import io.questdb.std.Crc32Function;
import io.questdb.std.HashMem32Function;
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
public class HashFunctionBenchmark {
    private final Crc32Function crc32c = new Crc32Function();
    @Param({"16", "22", "32", "38", "64", "72", "128", "134", "1024", "1034"})
    public long len;
    private long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HashFunctionBenchmark.class.getSimpleName())
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
    public int testCrc32c() {
        return crc32c.hash(mem, len);
    }

    @Benchmark
    public int testHashMem32() {
        return HashMem32Function.INSTANCE.hash(mem, len);
    }
}
