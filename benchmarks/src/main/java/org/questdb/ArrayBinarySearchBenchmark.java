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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.api.MemoryA;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ArrayBinarySearchBenchmark {
    @Param({"50", "100", "10000", "1000000"})
    public int size;

    DirectArray array;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ArrayBinarySearchBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void countDouble(Blackhole bh) {
        bh.consume(array.flatView().countDouble(0, size));
    }

    @Benchmark
    public void searchExistingEnd(Blackhole bh) {
        bh.consume(array.binarySearchDoubleValue1DArray((size - 1) * 3.0, true));
    }

    @Benchmark
    public void searchExistingMiddle(Blackhole bh) {
        bh.consume(array.binarySearchDoubleValue1DArray(size / 2 * 3.0, true));
    }

    @Benchmark
    public void searchExistingStart(Blackhole bh) {
        bh.consume(array.binarySearchDoubleValue1DArray(0.0, true));
    }

    @Benchmark
    public void searchMissingValue(Blackhole bh) {
        bh.consume(array.binarySearchDoubleValue1DArray(10.0, true));
    }

    @Setup(Level.Trial)
    public void setup() {
        array = new DirectArray();
        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        array.setDimLen(0, size);
        array.applyShape();
        MemoryA mem = array.startMemoryA();
        for (int i = 0; i < size; i++) {
            mem.putDouble(i * 3);
        }
    }

    @Benchmark
    public void sumDouble(Blackhole bh) {
        bh.consume(array.flatView().sumDouble(0, size));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        array.close();
    }
}
