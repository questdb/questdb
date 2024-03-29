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

import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class StringHashFunctionBenchmark {

    private final DirectString utf16String = new DirectString();
    private final DirectUtf8String utf8String = new DirectUtf8String();
    @Param({"7", "15", "31", "63", "1024"})
    private int len;
    private DirectUtf16Sink utf16Sink;
    private DirectUtf8Sink utf8Sink;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StringHashFunctionBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setUp() {
        utf8Sink = new DirectUtf8Sink(len);
        utf16Sink = new DirectUtf16Sink(2 * len);
        for (int i = 0; i < len; i++) {
            utf8Sink.put('a');
            utf16Sink.put('a');
        }
        utf8String.of(utf8Sink.ptr(), utf8Sink.ptr() + utf8Sink.size());
        utf16String.of(utf16Sink.ptr(), utf16Sink.ptr() + utf16Sink.size());
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        utf8Sink = Misc.free(utf8Sink);
        utf16Sink = Misc.free(utf16Sink);
    }

    @Benchmark
    public long testHashUtf8() {
        return Hash.hashUtf8(utf8String);
    }

    @Benchmark
    public int testStandardHashCharSequence() {
        return Chars.hashCode(utf16String);
    }
}
