/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package org.questdb;

import com.questdb.cairo.VirtualMemory;
import com.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class VirtualMemoryBenchmark {


    private static final VirtualMemory mem1 = new VirtualMemory(1024 * 1024);
    private static final VirtualMemory mem2 = new VirtualMemory(1024 * 1024);
    private static final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VirtualMemoryBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        mem2.jumpTo(0);
    }

    @Benchmark
    public void testInternalSequence() {
        for (int i = 0; i < 10000; i++) {
            mem2.putInt(i);
        }
    }

    @Benchmark
    public void testInternalSequenceStr() {
        for (int i = 0; i < 10000; i++) {
            CharSequence cs = rnd.nextChars(rnd.nextInt() % 4);
            mem2.putStr(cs);
        }
    }

    @Benchmark
    public void testExternalSequence() {
        long o = 0;
        for (int i = 0; i < 10000; i++) {
            mem1.putInt(o, i);
            o += 4;
        }
    }

    @Benchmark
    public void testExternalSequenceStr() {
        long o = 0;
        for (int i = 0; i < 10000; i++) {
            CharSequence cs = rnd.nextChars(rnd.nextInt() % 4);
            mem2.putStr(o, cs);
            o += cs.length() * 2 + 4;
        }
    }


}
