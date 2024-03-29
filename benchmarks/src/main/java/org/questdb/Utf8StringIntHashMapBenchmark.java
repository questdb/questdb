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
import io.questdb.std.Misc;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.str.*;
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

    private final DirectString utf16String = new DirectString();
    private final DirectUtf8String utf8String = new DirectUtf8String();
    @Param({"16", "256", "1024"})
    private int n;
    @Param({"5", "7", "31", "63"})
    private int prefixLength;
    private long[] utf16Addresses;
    private CharSequenceIntHashMap utf16Map = new CharSequenceIntHashMap();
    private DirectUtf16Sink utf16Sink;
    private long[] utf8Addresses;
    private Utf8StringIntHashMap utf8Map = new Utf8StringIntHashMap();
    private DirectUtf8Sink utf8Sink;

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
        utf8Sink = new DirectUtf8Sink(2 * n * prefixLength);
        utf16Sink = new DirectUtf16Sink(4 * n * prefixLength);
        utf8Addresses = new long[n + 1];
        utf16Addresses = new long[n + 1];
        utf8Addresses[0] = utf8Sink.ptr();
        utf16Addresses[0] = utf16Sink.ptr();
        for (int i = 0; i < n; i++) {
            // The keys have identical prefix, but different suffixes.
            for (int j = 0; j < prefixLength; j++) {
                utf8Sink.put('a');
                utf16Sink.put('a');
            }
            utf8Sink.put(i);
            utf16Sink.put(i);

            utf8Map.put(Utf8String.newInstance(utf8Sink), 42);
            utf16Map.put(utf16Sink.toString(), 42);

            utf8Addresses[i + 1] = utf8Sink.ptr() + utf8Sink.size();
            utf16Addresses[i + 1] = utf16Sink.ptr() + utf16Sink.size();
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        utf8Sink = Misc.free(utf8Sink);
        utf16Sink = Misc.free(utf16Sink);
        utf8Map = new Utf8StringIntHashMap();
        utf16Map = new CharSequenceIntHashMap();
    }

    @Benchmark
    public long testUtf16Map() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            sum += utf16Map.get(utf16String.of(utf16Addresses[i], utf16Addresses[i + 1]));
        }
        return sum;
    }

    @Benchmark
    public int testUtf8Map() {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            sum += utf8Map.get(utf8String.of(utf8Addresses[i], utf8Addresses[i + 1]));
        }
        return sum;
    }
}
