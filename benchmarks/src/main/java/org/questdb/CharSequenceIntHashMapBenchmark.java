/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.DirectCharSequenceIntHashHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CharSequenceIntHashMapBenchmark {
    @Param({"8", "16", "16384"})
    public int charSequenceLength;

    private CharSequenceIntHashMap charSequenceIntHashMap;
    private DirectCharSequenceIntHashHashMap directCharSequenceIntHashHashMap;

    private CharSequence charSequence;
    private CharSequence preInsertedCharSequence;

    @Setup(Level.Iteration)
    public void setUp() {
        charSequenceIntHashMap = new CharSequenceIntHashMap();
        directCharSequenceIntHashHashMap = new DirectCharSequenceIntHashHashMap();
        charSequence = createCharSequence('1');
        preInsertedCharSequence = createCharSequence('2');
        charSequenceIntHashMap.put(preInsertedCharSequence, 2);
        directCharSequenceIntHashHashMap.put(preInsertedCharSequence, 2);
    }

    private CharSequence createCharSequence(char c) {
        long mem = Unsafe.malloc(charSequenceLength * 2, MemoryTag.NATIVE_DEFAULT);
        DirectUtf8String actualString = new DirectUtf8String();
        actualString.of(mem, mem + (charSequenceLength * 2));
        for (int i = 0; i < charSequenceLength; i++) {
            Unsafe.getUnsafe().putChar(mem + (i * 2), c);
        }
        return actualString.asAsciiCharSequence();
    }

    @TearDown
    public void tearDown() {
        directCharSequenceIntHashHashMap.close();
    }

    @Benchmark
    public boolean testHeapMapPut() {
        return charSequenceIntHashMap.put(charSequence, 1);
    }

    @Benchmark
    public boolean testOffHeapMapPut() {
        return directCharSequenceIntHashHashMap.put(charSequence, 1);
    }

    @Benchmark
    public CharSequence testHeapMapGet() {
        var index = charSequenceIntHashMap.keyIndex(preInsertedCharSequence);
        return charSequenceIntHashMap.keyAt(index);
    }

    @Benchmark
    public CharSequence testOffHeapMapGet() {
        var index = directCharSequenceIntHashHashMap.keyIndex(preInsertedCharSequence);
        return directCharSequenceIntHashHashMap.keyAt(index);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CharSequenceIntHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
