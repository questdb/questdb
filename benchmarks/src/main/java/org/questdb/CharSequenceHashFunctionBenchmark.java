/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cutlass.line.tcp.DirectByteCharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CharSequenceHashFunctionBenchmark {

    private final DirectByteCharSequence charSequence = new DirectByteCharSequence();
    @Param({"16", "64", "256"})
    private int len;
    private long ptr;
    private String str;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CharSequenceHashFunctionBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setUp() {
        ptr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        charSequence.of(ptr, ptr + len);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, (byte) 'a');
            sb.append('a');
        }
        str = sb.toString();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        ptr = Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public int testStandardDirectByteCharSequence() {
        return Chars.hashCode(charSequence);
    }

    @Benchmark
    public long testXXHashDirectByteCharSequence() {
        return Hash.xxHash64(charSequence);
    }

    // this hash function is not called on the hot path; it's included for reference
    @Benchmark
    public long testXXHashString() {
        return DirectByteCharSequenceIntHashMap.xxHash64(str);
    }
}
