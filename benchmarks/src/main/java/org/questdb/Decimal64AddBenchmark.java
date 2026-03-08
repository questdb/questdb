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

import io.questdb.std.Decimal64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class Decimal64AddBenchmark {

    private BigDecimal bigDecimalAddend1;
    private BigDecimal bigDecimalAddend2;
    private Decimal64 decimal64Addend1;
    private Decimal64 decimal64Addend2;
    private Decimal64 decimal64Result;
    private MathContext mathContext;
    @SuppressWarnings("unused")
    @Param({"SIMPLE", "LARGE_NUMBERS", "SMALL_NUMBERS", "DIFFERENT_SCALES", "SAME_MAGNITUDE", "NEGATIVE_POSITIVE", "NEAR_CANCEL"})
    private String scenario;


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Decimal64AddBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public BigDecimal bigDecimalAdd() {
        return bigDecimalAddend1.add(bigDecimalAddend2);
    }

    @Benchmark
    public BigDecimal bigDecimalAddWithContext() {
        return bigDecimalAddend1.add(bigDecimalAddend2, mathContext);
    }

    @Benchmark
    public Decimal64 decimal64Add() {
        decimal64Result.copyFrom(decimal64Addend1);
        decimal64Result.add(decimal64Addend2);
        return decimal64Result;
    }

    @Setup
    public void setup() {
        decimal64Result = new Decimal64();
        mathContext = new MathContext(18, RoundingMode.HALF_UP);


        switch (scenario) {
            case "SIMPLE":
                // Simple addition: 123.456 + 789.123
                decimal64Addend1 = Decimal64.fromLong(123456L, 3);
                decimal64Addend2 = Decimal64.fromLong(789123L, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("789.123");
                break;

            case "LARGE_NUMBERS":
                // Large number addition: 12345678.123 + 9876543.456
                decimal64Addend1 = Decimal64.fromLong(12345678123L, 3);
                decimal64Addend2 = Decimal64.fromLong(9876543456L, 3);
                bigDecimalAddend1 = new BigDecimal("12345678.123");
                bigDecimalAddend2 = new BigDecimal("9876543.456");
                break;

            case "SMALL_NUMBERS":
                // Small number addition: 0.00123 + 0.00456
                decimal64Addend1 = Decimal64.fromLong(123L, 5);
                decimal64Addend2 = Decimal64.fromLong(456L, 5);
                bigDecimalAddend1 = new BigDecimal("0.00123");
                bigDecimalAddend2 = new BigDecimal("0.00456");
                break;

            case "DIFFERENT_SCALES":
                // Different scales: 123.456 + 7.890123456
                decimal64Addend1 = Decimal64.fromLong(123456L, 3);
                decimal64Addend2 = Decimal64.fromLong(7890123456L, 9);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("7.890123456");
                break;

            case "SAME_MAGNITUDE":
                // Same magnitude: 123.456 + 123.789
                decimal64Addend1 = Decimal64.fromLong(123456L, 3);
                decimal64Addend2 = Decimal64.fromLong(123789L, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("123.789");
                break;

            case "NEGATIVE_POSITIVE":
                // Negative + positive: -123.456 + 789.123
                decimal64Addend1 = Decimal64.fromLong(-123456L, 3);
                decimal64Addend2 = Decimal64.fromLong(789123L, 3);
                bigDecimalAddend1 = new BigDecimal("-123.456");
                bigDecimalAddend2 = new BigDecimal("789.123");
                break;

            case "NEAR_CANCEL":
                // Near canceling addition: 123.456 + (-123.455) = 0.001
                decimal64Addend1 = Decimal64.fromLong(123456L, 3);
                decimal64Addend2 = Decimal64.fromLong(-123455L, 3);
                bigDecimalAddend1 = new BigDecimal("123.456");
                bigDecimalAddend2 = new BigDecimal("-123.455");
                break;
        }
    }
}