/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.MemoryPARWImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VirtualMemoryStrReadBenchmark {
    private static final MemoryPARWImpl mem1 = new MemoryPARWImpl(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    private static final MemoryPARWImpl mem2 = new MemoryPARWImpl(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    private static final MemoryCARWImpl mem3 = new MemoryCARWImpl(1024 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    private static final MemoryCARWImpl mem4 = new MemoryCARWImpl(1024 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    private static final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VirtualMemoryStrReadBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        mem1.jumpTo(0);
        mem2.jumpTo(0);
        mem3.jumpTo(0);
        mem4.jumpTo(0);

        for (int i = 0; i < 100; i++) {
            CharSequence s = rnd.nextChars(rnd.nextInt() % 4);
            mem2.putLong(mem1.putStr(s));
            mem4.putLong(mem3.putStr(s));
        }
    }

    @Benchmark
    public long testGetStrContiguous() {
        long sum = 0;
        for (int i = 0; i < 100; i++) {
            long o = mem4.getLong(i * Long.BYTES);
            CharSequence s = mem3.getStr(o);
            int l = s.length();
            assert l > 0 && l < 4;
            for (int k = 0; k < l; k++) {
                sum += s.charAt(k);
            }
        }
        return sum;
    }

    @Benchmark
    public long testGetStrLegacy() {
        long sum = 0;
        for (int i = 0; i < 100; i++) {
            long o = mem2.getLong(i * Long.BYTES);
            CharSequence s = mem1.getStr(o);
            int l = s.length();
            for (int k = 0; k < l; k++) {
                sum += s.charAt(k);
            }
        }
        return sum;
    }
}
