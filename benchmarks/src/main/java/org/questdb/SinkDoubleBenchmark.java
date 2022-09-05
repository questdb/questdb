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

import io.questdb.std.*;
import io.questdb.std.str.DirectUnboundedByteSink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SinkDoubleBenchmark {
    private final static long memSize = 1024 * 16;
    private final static double d = 78899.9;
    private final static long l = 2298989898L;
    private final static DirectUnboundedByteSink flyweight = new DirectUnboundedByteSink();
    private long mem;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SinkDoubleBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public long testSinkDouble() {
        flyweight.of(mem);
        Numbers.append(flyweight, d);
        return flyweight.getAddress();
    }

    @Benchmark
    public long testSinkLong() {
        flyweight.of(mem);
        Numbers.append(flyweight, l);
        return flyweight.getAddress();
    }

    @Benchmark
    public long testToStringDouble() {
        return Chars.asciiStrCpy(Double.toString(d), mem);
    }

    @Benchmark
    public long testToStringLong() {
        return Chars.asciiStrCpy(Long.toString(l), mem);
    }
}
