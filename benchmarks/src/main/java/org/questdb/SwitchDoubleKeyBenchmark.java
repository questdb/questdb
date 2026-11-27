/*+*****************************************************************************
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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * The cost per row of {@code CASE d WHEN k1 THEN 0 WHEN k2 THEN 1 ... ELSE -1 END} on a DOUBLE or
 * FLOAT operand, for a number of keys and for rows that match the first key, the last key, a key
 * picked at random, no key, or are NULL.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class SwitchDoubleKeyBenchmark {
    private static final int ROWS = 1024;
    private final DoubleRecord record = new DoubleRecord();
    private final double[] values = new double[ROWS];
    @Param({"1", "2", "4", "5", "8", "16", "200", "1000"})
    public int keyCount;
    @Param({"first", "last", "random", "none", "null"})
    public String rows;
    @Param({"double", "float"})
    public String type;
    private Function function;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SwitchDoubleKeyBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public long caseOnDouble() {
        long sum = 0;
        for (int i = 0; i < ROWS; i++) {
            record.value = values[i];
            sum += function.getInt(record);
        }
        return sum;
    }

    @Setup
    public void setup() throws SqlException {
        // keys 0, 1.5, 3.0, ..., exact in FLOAT too; a row that matches no key lies halfway between
        // two of them
        final ObjList<Function> args = new ObjList<>();
        final IntList positions = new IntList();
        final boolean isFloat = type.equals("float");
        args.add(isFloat ? new FloatFunction() {
            @Override
            public float getFloat(Record rec) {
                return (float) rec.getDouble(0);
            }
        } : new DoubleFunction() {
            @Override
            public double getDouble(Record rec) {
                return rec.getDouble(0);
            }
        });
        positions.add(0);
        for (int i = 0; i < keyCount; i++) {
            args.add(isFloat ? new FloatConstant(i * 1.5f) : new DoubleConstant(i * 1.5));
            args.add(IntConstant.newInstance(i));
            positions.add(0);
            positions.add(0);
        }
        args.add(IntConstant.newInstance(-1));
        positions.add(0);
        function = new SwitchFunctionFactory().newInstance(0, args, positions, null, null);

        final Rnd rnd = new Rnd();
        for (int i = 0; i < ROWS; i++) {
            values[i] = switch (rows) {
                case "first" -> 0;
                case "last" -> (keyCount - 1) * 1.5;
                case "random" -> rnd.nextInt(keyCount) * 1.5;
                case "none" -> rnd.nextInt(keyCount) * 1.5 + 0.75;
                default -> Double.NaN;
            };
        }
    }

    private static class DoubleRecord implements Record {
        private double value;

        @Override
        public double getDouble(int col) {
            return value;
        }
    }
}
