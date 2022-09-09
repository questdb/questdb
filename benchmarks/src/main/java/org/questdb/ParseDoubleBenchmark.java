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
import io.questdb.std.str.DirectByteCharSequence;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ParseDoubleBenchmark {
    private final static long memSize = 1024 * 16;
    private final static String d = "78899.9";
    private final static DirectByteCharSequence flyweight = new DirectByteCharSequence();
    private long mem;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParseDoubleBenchmark.class.getSimpleName())
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
        Chars.asciiStrCpy(d, d.length(), mem);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public double testCharSequenceParse() throws NumericException {
        return Numbers.parseDouble(flyweight.of(mem, mem + d.length()));
    }

    @Benchmark
    public double testStringParse() {
        final byte[] b = new byte[d.length()];
        for (int i = 0, n = b.length; i < n; i++) {
            b[i] = Unsafe.getUnsafe().getByte(mem + i);
        }
        return Double.parseDouble(new String(b));
    }
}
