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

import io.questdb.cairo.CairoKeywords;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CairoKeywordBenchmark {

    private final long memSize = "abcde.detached".length() + 1;
    private final StringSink sink = new StringSink();
    private final StringSink sink2 = new StringSink();
    private long mem;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CairoKeywordBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        Utf8s.strCpyAscii("wal.detached", mem);
        Unsafe.getUnsafe().putByte(mem + memSize - 1, (byte) 0);
        sink2.clear();
        Utf8s.utf8ToUtf16(mem, mem + memSize, sink2);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Benchmark
    public boolean testConventional() {
        sink.clear();
        Utf8s.utf8ToUtf16(mem, mem + memSize, sink);
        return Chars.endsWith(sink, ".detached");
    }

    @Benchmark
    public boolean testFastPath() {
        return CairoKeywords.isDetachedDirMarker(mem);
    }

    @Benchmark
    public boolean testFullFatWal() {
        return CairoKeywords.isWal(mem);
    }

    @Benchmark
    public boolean testHalfConventional() {
        return Chars.endsWith(sink2, ".detached");
    }

    @Benchmark
    public boolean testHalfWal() {
        return Chars.startsWith(sink2, "wal");
    }
}
