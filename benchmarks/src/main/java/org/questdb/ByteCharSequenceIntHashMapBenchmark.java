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

import io.questdb.std.ByteCharSequenceIntHashMap;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.ByteCharSequence;
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
public class ByteCharSequenceIntHashMapBenchmark {

    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private ByteCharSequenceIntHashMap directMap = new ByteCharSequenceIntHashMap();
    @Param({"7", "15", "31", "63"})
    private int len;
    @Param({"16", "256"})
    private int n;
    private CharSequenceIntHashMap nonDirectMap = new CharSequenceIntHashMap();
    private long ptr;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteCharSequenceIntHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setUp() {
        ptr = Unsafe.malloc((long) n * len, MemoryTag.NATIVE_DEFAULT);
        dbcs.of(ptr, ptr + len);
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                char ch = (char) ('a' + i + j);
                Unsafe.getUnsafe().putByte(ptr + (long) i * len + j, (byte) ch);
                sb.append(ch);
            }
            nonDirectMap.put(sb.toString(), 42);
            directMap.put(ByteCharSequence.newInstance(dbcs), 42);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        ptr = Unsafe.free(ptr, (long) n * len, MemoryTag.NATIVE_DEFAULT);
        directMap = new ByteCharSequenceIntHashMap();
        nonDirectMap = new CharSequenceIntHashMap();
    }

    @Benchmark
    public int testDirectMap() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            dbcs.of(ptr + (long) i * len, ptr + (long) (i + 1) * len);
            sum += directMap.get(dbcs);
        }
        return sum;
    }

    @Benchmark
    public long testNonDirectMap() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            dbcs.of(ptr + (long) i * len, ptr + (long) (i + 1) * len);
            sum += nonDirectMap.get(dbcs);
        }
        return sum;
    }
}
