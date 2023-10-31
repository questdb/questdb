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

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.str.FlyweightDirectCharSink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Utf8StringIntHashMapBenchmark {

    private final FlyweightDirectCharSink dcs = new FlyweightDirectCharSink();
    private final DirectUtf8String dus = new DirectUtf8String();
    private Utf8StringIntHashMap directMap = new Utf8StringIntHashMap();
    @Param({"7", "15", "31", "63"})
    private int len;
    @Param({"16", "256"})
    private int n;
    private CharSequenceIntHashMap nonDirectMap = new CharSequenceIntHashMap();
    private long ptr;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Utf8StringIntHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setUp() {
        ptr = Unsafe.malloc((long) n * len, MemoryTag.NATIVE_DEFAULT);
        dus.of(ptr, ptr + len);
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                char ch = (char) ('a' + i + j);
                Unsafe.getUnsafe().putByte(ptr + (long) i * len + j, (byte) ch);
                sb.append(ch);
            }
            nonDirectMap.put(sb.toString(), 42);
            directMap.put(Utf8String.newInstance(dus), 42);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        ptr = Unsafe.free(ptr, (long) n * len, MemoryTag.NATIVE_DEFAULT);
        directMap = new Utf8StringIntHashMap();
        nonDirectMap = new CharSequenceIntHashMap();
    }

    @Benchmark
    public int testDirectMap() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            dus.of(ptr + (long) i * len, ptr + (long) (i + 1) * len);
            sum += directMap.get(dus);
        }
        return sum;
    }

    @Benchmark
    public long testNonDirectMap() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            dcs.of(ptr + (long) i * len, ptr + (long) (i + 1) * len);
            sum += nonDirectMap.get(dcs);
        }
        return sum;
    }
}
