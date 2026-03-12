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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.groupby.vect.AvgDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumLongVectorAggregateFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
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

import static io.questdb.griffin.SqlCodeGenerator.GKK_VANILLA_INT;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VectorizedGroupByMicroBenchmark {
    private static final long FRAME_ROW_COUNT = 1L << 20;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(VectorizedGroupByMicroBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long keyedIntSumLong(SumLongState state) {
        if (!state.function.aggregate(state.pRosti, state.keyAddress, state.valueAddress, FRAME_ROW_COUNT)) {
            throw new IllegalStateException("native keyedIntSumLong aggregation failed");
        }
        if (!state.function.wrapUp(state.pRosti)) {
            throw new IllegalStateException("native keyedIntSumLong wrap-up failed");
        }
        return Rosti.getSize(state.pRosti);
    }

    @Benchmark
    public long keyedIntAvgDouble(AvgDoubleState state) {
        if (!state.function.aggregate(state.pRosti, state.keyAddress, state.valueAddress, FRAME_ROW_COUNT)) {
            throw new IllegalStateException("native keyedIntSumDouble aggregation failed");
        }
        if (!state.function.wrapUp(state.pRosti)) {
            throw new IllegalStateException("native keyedIntAvgDouble wrap-up failed");
        }
        return Rosti.getSize(state.pRosti);
    }

    @State(Scope.Thread)
    public abstract static class BaseState {
        @Param({"4096", "65536"})
        public int distinctKeys;

        protected long keyAddress;
        protected long pRosti;

        @Setup(Level.Trial)
        public void setUpBase() {
            Os.init();
            if (Integer.bitCount(distinctKeys) != 1) {
                throw new IllegalArgumentException("distinctKeys must be a power of two");
            }
            keyAddress = Unsafe.malloc(FRAME_ROW_COUNT * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            final int mask = distinctKeys - 1;
            for (int i = 0; i < FRAME_ROW_COUNT; i++) {
                Unsafe.getUnsafe().putInt(keyAddress + (long) i * Integer.BYTES, mix32(i) & mask);
            }
        }

        @Setup(Level.Invocation)
        public void reset() {
            Rosti.clear(pRosti);
            resetFunction();
        }

        @TearDown(Level.Trial)
        public void tearDownBase() {
            if (pRosti != 0) {
                Rosti.free(pRosti);
                pRosti = 0;
            }
            keyAddress = Unsafe.free(keyAddress, FRAME_ROW_COUNT * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            closeFunction();
        }

        protected final long allocateRosti(ArrayColumnTypes columnTypes) {
            final long rosti = Rosti.alloc(columnTypes, distinctKeys * 2L);
            if (rosti == 0) {
                throw new IllegalStateException("could not allocate rosti");
            }
            // Single-key vectorized GROUP BY uses an INT key in the first slot.
            Unsafe.getUnsafe().putInt(Rosti.getInitialValueSlot(rosti, 0), Numbers.INT_NULL);
            return rosti;
        }

        protected abstract void closeFunction();

        protected abstract void resetFunction();

        private static int mix32(int value) {
            int x = value * 0x9E3779B9;
            x ^= x >>> 16;
            x *= 0x85EBCA6B;
            x ^= x >>> 13;
            return x;
        }
    }

    @State(Scope.Thread)
    public static class SumLongState extends BaseState {
        private SumLongVectorAggregateFunction function;
        private long valueAddress;

        @Setup(Level.Trial)
        public void setUp() {
            setUpBase();

            valueAddress = Unsafe.malloc(FRAME_ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < FRAME_ROW_COUNT; i++) {
                Unsafe.getUnsafe().putLong(valueAddress + (long) i * Long.BYTES, 31L * i - 7L);
            }

            function = new SumLongVectorAggregateFunction(GKK_VANILLA_INT, 1, -1, 1);
            final ArrayColumnTypes columnTypes = new ArrayColumnTypes().add(ColumnType.INT);
            function.pushValueTypes(columnTypes);
            pRosti = allocateRosti(columnTypes);
            function.initRosti(pRosti);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            valueAddress = Unsafe.free(valueAddress, FRAME_ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            tearDownBase();
        }

        @Override
        protected void closeFunction() {
            if (function != null) {
                function.close();
                function = null;
            }
        }

        @Override
        protected void resetFunction() {
            function.clear();
        }
    }

    @State(Scope.Thread)
    public static class AvgDoubleState extends BaseState {
        private AvgDoubleVectorAggregateFunction function;
        private long valueAddress;

        @Setup(Level.Trial)
        public void setUp() {
            setUpBase();

            valueAddress = Unsafe.malloc(FRAME_ROW_COUNT * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < FRAME_ROW_COUNT; i++) {
                Unsafe.getUnsafe().putDouble(valueAddress + (long) i * Double.BYTES, mixDouble(i));
            }

            function = new AvgDoubleVectorAggregateFunction(GKK_VANILLA_INT, 1, -1, 1);
            final ArrayColumnTypes columnTypes = new ArrayColumnTypes().add(ColumnType.INT);
            function.pushValueTypes(columnTypes);
            pRosti = allocateRosti(columnTypes);
            function.initRosti(pRosti);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            valueAddress = Unsafe.free(valueAddress, FRAME_ROW_COUNT * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            tearDownBase();
        }

        @Override
        protected void closeFunction() {
            if (function != null) {
                function.close();
                function = null;
            }
        }

        @Override
        protected void resetFunction() {
            function.clear();
        }

        private static double mixDouble(int value) {
            long x = 0x9E3779B97F4A7C15L * (value + 1L);
            x ^= x >>> 33;
            x *= 0xFF51AFD7ED558CCDL;
            x ^= x >>> 33;
            x *= 0xC4CEB9FE1A85EC53L;
            x ^= x >>> 33;
            return ((x >>> 11) * 0x1.0p-53) * 1024.0 - 512.0;
        }
    }
}
