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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DoubleBenchmark {

    private static final long EXP_BIT_MASK = 0x7FF0000000000000L;
    private static final double x = Double.NaN;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DoubleBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public boolean testBitFiddle() {
        return (Double.doubleToLongBits(x) & EXP_BIT_MASK) == EXP_BIT_MASK;
    }

    @Benchmark
    public boolean testIsFinite() {
        return Double.isFinite(x);
    }

    @Benchmark
    public double testStraight() {
        return x;
    }

    @Benchmark
    public long testToLong() {
        return Double.doubleToLongBits(x);
    }
}
